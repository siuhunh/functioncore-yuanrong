/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package diskusage monitors the usage of directories
package diskusage

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
)

const (
	diskCheckTimeOut        = 10 * time.Second
	userInodesCheckInterval = 50 * time.Millisecond
)

// Monitor monitors the usage of a directory
type Monitor struct {
	watcher        *fsnotify.Watcher
	cache          *cache
	stopCh         chan struct{}
	rootDir        string
	ignores        []string
	processHooks   []ProcessHook
	cachedSize     int64
	diffOnce       sync.Once
	firstSize      int64
	firstSizeLock  sync.Mutex
	diffSize       int64
	mountPath      []string
	inodeSize      int64
	firstInode     int64
	inodeCheckFlag int64
	closeOnce      sync.Once
}

// Opt contains options to Monitor
type Opt func(m *Monitor) error

// ProcessHook is invoked when an file change event happens
type ProcessHook func(fsnotify.Event)

// Ignore creates an option to ask the Monitor to ignore some directories
func Ignore(dirs ...string) Opt {
	return func(m *Monitor) error {
		absDirs := make([]string, len(dirs))
		for i, dir := range dirs {
			absDir, err := filepath.Abs(dir)
			if err != nil {
				log.GetLogger().Errorf("failed to absolute dir %s, %s", dir, err.Error())
				return err
			}
			absDirs[i] = absDir
		}
		m.ignores = absDirs
		log.GetLogger().Infof("ignores dir: %v", m.ignores)
		return nil
	}
}

// AddProcessHooks creates an option to add ProcessHook to the Monitor
func AddProcessHooks(hooks ...ProcessHook) Opt {
	return func(m *Monitor) error {
		m.processHooks = append(m.processHooks, hooks...)
		return nil
	}
}

func newMonitor(rootDir string, opts ...Opt) (*Monitor, error) {
	rootDir, err := filepath.Abs(rootDir)
	if err != nil {
		log.GetLogger().Errorf("failed to absolute rootDir %s, %s", rootDir, err.Error())
		return nil, err
	}

	// make sure rootDir exists
	_, err = os.Lstat(rootDir)
	if err != nil {
		log.GetLogger().Errorf("failed to stat dir %s, %s", rootDir, err.Error())
		return nil, err
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.GetLogger().Errorf("failed to create watcher for dir %s, %s", rootDir, err.Error())
		return nil, err
	}

	monitor := &Monitor{
		watcher: watcher,
		cache:   &cache{rootDir: rootDir},
		stopCh:  make(chan struct{}),
		rootDir: rootDir,
	}
	for _, opt := range opts {
		if err := opt(monitor); err != nil {
			return nil, err
		}
	}
	return monitor, nil
}

// New creates a new Monitor
func New(rootDir string, opts ...Opt) (*Monitor, error) {
	monitor, err := newMonitor(rootDir, opts...)
	if err != nil {
		log.GetLogger().Errorf("failed to new monitor", err.Error())
		return nil, err
	}
	go monitor.start(true)
	if monitor.inodeCheckFlag != 0 {
		log.GetLogger().Infof("check inode begin")
		go monitor.inodeCheck()
	}
	return monitor, nil
}

// NewRuntimeMonitor creates a new runtime Monitor
func NewRuntimeMonitor(monitorDir []string, stopChan <-chan struct{}, opts ...Opt) (*Monitor, error) {
	if len(monitorDir) == 0 {
		return nil, errors.New("failed to new monitor, monitor dir is empty")
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.GetLogger().Errorf("failed to create watcher, %s", err.Error())
		return nil, err
	}

	monitor := &Monitor{
		watcher: watcher,
		stopCh:  make(chan struct{}),
	}
	for _, opt := range opts {
		if err := opt(monitor); err != nil {
			return nil, err
		}
	}

	for _, dir := range monitorDir {
		rootDir, err := filepath.Abs(dir)
		if err != nil {
			log.GetLogger().Errorf("failed to absolute rootDir %s, %s", rootDir, err.Error())
			return nil, err
		}
		// make sure rootDir exists
		_, err = os.Lstat(rootDir)
		if err != nil {
			log.GetLogger().Warnf("failed to stat dir %s, no such file or dir", rootDir)
			return nil, err
		}
		if err := monitor.watcher.Add(rootDir); err != nil {
			log.GetLogger().Errorf("failed to watch dir %s, err: %s", rootDir, err.Error())
			return nil, err
		}
	}

	go monitor.startRuntimeMonitor(stopChan)
	return monitor, nil
}

// NewCustomImageMonitor creates a new custom image disk Monitor
func NewCustomImageMonitor(rootDir string, opts ...Opt) (*Monitor, error) {
	monitor, err := newMonitor(rootDir, opts...)
	if err != nil {
		log.GetLogger().Errorf("failed to new monitor", err.Error())
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), diskCheckTimeOut)
	defer cancel()

	monitor.firstWithTimeOut(ctx)
	go monitor.start(false)
	return monitor, nil
}

func (m *Monitor) firstScan(path string, stopScan chan struct{}) {
	var isTimeout bool
	err := filepath.Walk(path, func(walkPath string, fi os.FileInfo, err error) error {
		select {
		case <-stopScan:
			isTimeout = true
			log.GetLogger().Warnf("first scan timeout, walking stopped, path: %s", path)
			return errors.New("walking stopped")
		default:
			if err != nil {
				return nil
			}
			for _, ignore := range m.ignores {
				if strings.HasPrefix(walkPath, ignore) {
					return nil
				}
			}
			log.GetLogger().Debugf("firstScan current mount path: %s, dir %t, name %s",
				walkPath, fi.IsDir(), fi.Name())
			if fi.IsDir() {
				if err := m.watcher.Add(walkPath); err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
			}
			m.cache.add(walkPath, fi)
			return nil
		}
	})
	if err != nil && err.Error() != "walking stopped" {
		log.GetLogger().Warnf("failed to walk path: %s, %s", path, err.Error())
	}
	if isTimeout {
		m.cacheUsageMax()
	} else {
		m.cacheUsage()
	}
}

func (m *Monitor) firstWithTimeOut(context context.Context) {
	finishCh := make(chan struct{})
	stopScan := make(chan struct{})
	go func() {
		m.firstInodeUsageWithCustomContainer(stopScan)
		m.firstScan(m.rootDir, stopScan)
		log.GetLogger().Infof("cacheUsage finished, the value of firstSize is %d", m.FirstSize())
		close(finishCh)
	}()

	select {
	case <-context.Done():
		close(stopScan)
		<-finishCh
		log.GetLogger().Infof("disk usage check timeout, set firstSize as 10G")
	case _, ok := <-finishCh:
		if !ok {
			log.GetLogger().Infof("disk usage check finished")
		}
	}
}

func (m *Monitor) firstScanWithNonCustomImageFunction() {
	m.firstInodeUsage()
	m.recursiveWatch(m.rootDir)
	m.cacheUsage()
}

func (m *Monitor) start(isNonCustomImageFunc bool) {
	if isNonCustomImageFunc {
		m.firstScanWithNonCustomImageFunction()
	}
	log.GetLogger().Debugf("usage of %s is ready", m.rootDir)
	for {
		select {
		case _, ok := <-m.stopCh:
			if !ok {
				_ = m.watcher.Close()
			}
			return
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}
			if ok := m.processEvent(event); !ok {
				return
			}
		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			log.GetLogger().Errorf("watch error for dir %s, %s", m.rootDir, err.Error())
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				m.reinitialize()
			}
		}
	}
}

func (m *Monitor) startRuntimeMonitor(stopChan <-chan struct{}) {
	log.GetLogger().Debugf("monitor of %s is ready", m.rootDir)
	if stopChan == nil {
		log.GetLogger().Error("failed to start runtime monitor, stopChan is nil")
		return
	}
	for {
		select {
		case event, ok := <-m.watcher.Events:
			if !ok {
				return
			}
			for _, hook := range m.processHooks {
				hook(event)
			}
		case err, ok := <-m.watcher.Errors:
			if !ok {
				return
			}
			log.GetLogger().Errorf("watch error for dir %s, %s", m.rootDir, err.Error())
			if errors.Is(err, fsnotify.ErrEventOverflow) {
				m.reinitialize()
			}
		case _, ok := <-m.stopCh:
			if !ok {
				_ = m.watcher.Close()
			}
			return
		case _, ok := <-stopChan:
			if !ok {
				_ = m.watcher.Close()
			}
			log.GetLogger().Warnf("received termination signal, exit runtime monitor")
			return
		}
	}
}

func (m *Monitor) reinitialize() {
	log.GetLogger().Infof("reinitializing dir %s", m.rootDir)

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.GetLogger().Errorf("failed to reinitialize for dir %s, %s", m.rootDir, err.Error())
		return
	}

	_ = m.watcher.Close()
	m.watcher = watcher
	m.cache = &cache{rootDir: m.rootDir}

	m.recursiveWatch(m.rootDir)
	m.cacheUsage()
}

func (m *Monitor) processEvent(event fsnotify.Event) bool {
	log.GetLogger().Debugf("got event for dir %s: %s, ignores %v", m.rootDir, event, m.ignores)
	for _, ignore := range m.ignores {
		if strings.HasPrefix(event.Name, ignore) {
			return true
		}
	}
	for _, hook := range m.processHooks {
		hook(event)
	}

	fi, err := os.Lstat(event.Name)
	if err == nil {
		if fi.IsDir() && event.Op&fsnotify.Create > 0 {
			m.recursiveWatch(event.Name)
		}

		if !fi.IsDir() && (event.Op&fsnotify.Write > 0 || event.Op&fsnotify.Create > 0) {
			m.cache.add(event.Name, fi)
		}
	}

	if event.Op&fsnotify.Remove > 0 || event.Op&fsnotify.Rename > 0 {
		if event.Name == m.rootDir {
			log.GetLogger().Infof("root dir %s removed. No need to monitor", m.rootDir)
			return false
		}

		m.cache.remove(event.Name, func(path string, f file) error {
			if _, ok := f.(*directory); ok {
				m.watcher.Remove(path)
			}
			return nil
		})
	}

	m.cacheUsage()

	return true
}

func (m *Monitor) cacheUsage() {
	m.firstSizeLock.Lock()
	defer m.firstSizeLock.Unlock()

	size := m.cache.size()
	m.diffOnce.Do(func() {
		m.firstSize = size
	})
	atomic.StoreInt64(&m.cachedSize, size)
	atomic.StoreInt64(&m.diffSize, size-m.firstSize)
}

func (m *Monitor) cacheUsageMax() {
	m.firstSizeLock.Lock()
	defer m.firstSizeLock.Unlock()
	size := m.cache.size()
	if m.firstSize == 0 {
		m.diffOnce.Do(func() {
			m.firstSize = constants.ExpansionUserDataMaxSize
		})
	}
	atomic.StoreInt64(&m.cachedSize, size)
}

func (m *Monitor) countInodes() int64 {
	count := int64(0)
	err := filepath.WalkDir(m.rootDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		for _, ignore := range m.ignores {
			if strings.HasPrefix(path, ignore) {
				return nil
			}
		}
		count++
		return nil
	})
	if err != nil {
		log.GetLogger().Warnf("failed to walk path: %s when get inode, %s", m.rootDir, err.Error())
	}
	return count
}

func (m *Monitor) getFirstCountInodes(stopScan chan struct{}) int64 {
	var isTimeout bool
	count := int64(0)
	err := filepath.WalkDir(m.rootDir, func(walkPath string, entry fs.DirEntry, err error) error {
		select {
		case <-stopScan:
			isTimeout = true
			log.GetLogger().Warnf("first scan inode timeout, walking stopped, path: %s", walkPath)
			return errors.New("walking stopped")
		default:
			if err != nil {
				return nil
			}
			for _, ignore := range m.ignores {
				if strings.HasPrefix(walkPath, ignore) {
					return nil
				}
			}
			count++
			return nil
		}
	})
	if err != nil && err.Error() != "walking stopped" {
		log.GetLogger().Warnf("failed to walk path: %s when get inode, %s", m.rootDir, err.Error())
		return constants.ExpansionUserInodesMaxSize
	}
	if isTimeout {
		return constants.ExpansionUserInodesMaxSize
	}
	return count
}

func (m *Monitor) inodeCheck() {
	t := time.NewTicker(userInodesCheckInterval)
	for {
		select {
		case <-t.C:
			m.curInodeUsage()
		case _, ok := <-m.stopCh:
			if !ok {
				t.Stop()
			}
			log.GetLogger().Infof("stop the task of inode usage update")
			t.Stop()
			return
		}
	}
}

func (m *Monitor) recursiveWatch(path string) {
	err := filepath.Walk(path, func(walkPath string, fi os.FileInfo, err error) error {
		if err != nil {
			// This file is probably deleted/renamed at the same time. Ignore and continue.
			return nil
		}
		for _, ignore := range m.ignores {
			if strings.HasPrefix(walkPath, ignore) {
				return nil
			}
		}
		log.GetLogger().Debugf("recursiveWatch current mount path: %s, dir %t, name %s",
			walkPath, fi.IsDir(), fi.Name())
		if fi.IsDir() {
			if err := m.watcher.Add(walkPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}

		m.cache.add(walkPath, fi)

		return nil
	})
	if err != nil {
		log.GetLogger().Warnf("failed to walk path: %s, %s", path, err.Error())
	}
}
func (m *Monitor) firstInodeUsage() {
	inode := m.countInodes()
	m.firstInode = inode
}

func (m *Monitor) firstInodeUsageWithCustomContainer(stopCh chan struct{}) {
	inode := m.getFirstCountInodes(stopCh)
	m.firstInode = inode
	log.GetLogger().Infof("in first scanning, current inode num %d", inode)
}

func (m *Monitor) curInodeUsage() {
	inode := m.countInodes()
	atomic.StoreInt64(&m.inodeSize, inode)
}

// SetCheckInode -
func (m *Monitor) SetCheckInode() {
	atomic.StoreInt64(&m.inodeCheckFlag, 1)
}

// Size returns a cached size of the monitored directory
func (m *Monitor) Size() int64 {
	return atomic.LoadInt64(&m.cachedSize)
}

// CurrentInode returns the current inodes of the monitored directory
func (m *Monitor) CurrentInode() int64 {
	return atomic.LoadInt64(&m.inodeSize)
}

// FirstSize returns a cached first size of the monitored directory
func (m *Monitor) FirstSize() int64 {
	return atomic.LoadInt64(&m.firstSize)
}

// FirstInode returns a cached first inodes of the monitored directory
func (m *Monitor) FirstInode() int64 {
	return atomic.LoadInt64(&m.firstInode)
}

// Diff returns the difference between the first monitored size and the current size.
func (m *Monitor) Diff() int64 {
	return atomic.LoadInt64(&m.diffSize)
}

// Close stops the monitor
func (m *Monitor) Close() {
	if m.stopCh != nil {
		m.closeOnce.Do(func() {
			close(m.stopCh)
		})
		m.stopCh = nil
	}
}
