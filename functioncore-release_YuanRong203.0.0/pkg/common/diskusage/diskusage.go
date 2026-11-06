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
	watcher          *fsnotify.Watcher
	cache            *cache
	stopCh           chan struct{}
	rootDir          string
	ignores          []string
	processHooks     []ProcessHook
	cachedSize       int64
	diffOnce         sync.Once
	firstSize        int64
	firstSizeLock    sync.Mutex
	diffSize         int64
	mountPath        []string
	inodeSize        int64
	firstInode       int64
	closeOnce        sync.Once
	needReinitialize int64
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
		case _, ok := <-m.stopCh:
			if !ok {
				_ = m.watcher.Close()
			}
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
	m.reinitializeWatcher()
	return true
}

func (m *Monitor) reinitializeWatcher() {
	if atomic.LoadInt64(&m.needReinitialize) == 1 {
		m.reinitialize()
	}
	atomic.StoreInt64(&m.needReinitialize, 0)
}

func (m *Monitor) cacheUsage() {
	m.firstSizeLock.Lock()
	defer m.firstSizeLock.Unlock()

	size := m.cache.size()
	num := m.cache.num()
	m.diffOnce.Do(func() {
		m.firstSize = size
		m.firstInode = num
	})
	atomic.StoreInt64(&m.cachedSize, size)
	atomic.StoreInt64(&m.diffSize, size-m.firstSize)
	atomic.StoreInt64(&m.inodeSize, num)
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

// Reinitialize -
func (m *Monitor) Reinitialize() {
	atomic.StoreInt64(&m.needReinitialize, 1)
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
