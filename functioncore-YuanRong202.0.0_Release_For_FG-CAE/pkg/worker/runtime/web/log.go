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

package web

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/utils"
	"functioncore/pkg/worker/websocket"
)

const (
	syncStream                    = "sync"
	containerdSTDLogSpaceSplitNum = 3
)

var errWatcherStopped = errors.New("watcher is stopped")

func collectRuntimeContainerLog(conf *config.Configuration) *RuntimeContainerLogger {
	runtimeContainerID := conf.RuntimeContainerID
	var logFileName string
	var logFilePath string
	isContainerd := engine.IsContainerd()
	if isContainerd {
		logFileName = "0.log"
		stdLogDirName := fmt.Sprintf("%s_%s_%s",
			os.Getenv("POD_NAMESPACE"), os.Getenv("PodName"), os.Getenv("POD_ID"))
		logFilePath = filepath.Join(engine.CtrStdLogPath, stdLogDirName, "runtime", logFileName)
	} else {
		logFileName = runtimeContainerID + "-json.log"
		logFilePath = filepath.Join("/var/lib/docker/containers/", runtimeContainerID, logFileName)
	}
	processCB := func(msg RuntimeContainerLog) {
		logger, err := functionlog.GetFunctionLogger(conf)
		if err != nil {
			log.GetLogger().Warnf("failed to get function logger, %s", err.Error())
			return
		}
		if logger == nil {
			log.GetLogger().Warnf("failed to get function logger")
			return
		}

		logger.WriteStdLog(msg.log, msg.t.UTC().Format(functionlog.NanoLogLayout),
			runtime.PodApplier == constants.WorkerManagerApplier,
			msg.t.UTC().Format(functionlog.NanoLogLayout))
	}
	logger, err := NewRuntimeStdLogger(logFilePath, processCB)
	if err != nil {
		log.GetLogger().Errorf("failed to collect runtime container log: %s", err.Error())
		runtime.CloseExitChannel()
	}

	// receive syncPoint, add syncPoint to logger's syncPoints and record log
	go logger.receiveSyncPoint()
	go func() {
		if err := logger.Run(); err != nil {
			log.GetLogger().Errorf("failed to collect runtime container log: %s", err.Error())
			runtime.CloseExitChannel()
		}
	}()

	return logger
}

// RuntimeContainerLogProcessCB -
type RuntimeContainerLogProcessCB func(RuntimeContainerLog)

// RuntimeContainerLog -
type RuntimeContainerLog struct {
	log   string
	level string
	t     time.Time
}

type dockerLog struct {
	Log    string `json:"log"`
	Stream string `json:"stream"`
	Time   string `json:"time"`
}

type syncPoint struct {
	t    time.Time
	done chan struct{}
}

// RuntimeContainerLogger -
type RuntimeContainerLogger struct {
	t            *tail
	syncPointCh  chan *syncPoint
	syncPoints   []*syncPoint
	logFilePath  string
	processCB    RuntimeContainerLogProcessCB
	tick         time.Time
	isContainerd bool
}

// NewRuntimeStdLogger -
func NewRuntimeStdLogger(logFilePath string, processCB RuntimeContainerLogProcessCB) (*RuntimeContainerLogger, error) {
	isContainerd := strings.Contains(logFilePath, "containerd")
	logger := &RuntimeContainerLogger{
		syncPointCh:  make(chan *syncPoint),
		processCB:    processCB,
		logFilePath:  logFilePath,
		isContainerd: isContainerd,
	}

	err := tailFile(logger)
	if err != nil {
		log.GetLogger().Errorf("failed to start tailing file: %s, %s", logFilePath, err.Error())
		return nil, err
	}

	return logger, nil
}

func (l *RuntimeContainerLogger) receiveSyncPoint() {
	if l.t == nil {
		log.GetLogger().Errorf("tail is nil")
		return
	}
	log.GetLogger().Infof("start to receive sync point for %s", l.logFilePath)
	for {
		select {
		case syncPoint, ok := <-l.syncPointCh:
			if !ok {
				log.GetLogger().Errorf("sync times chan closed unexpectedly")
				return
			}
			l.tick = time.Now()
			l.addSyncPoint(syncPoint)
			l.notifySyncPoints()
		}
	}
}

// Run -
func (l *RuntimeContainerLogger) Run() error {
	if l.t == nil {
		log.GetLogger().Errorf("tail is nil")
		return errors.New("tail is nil")
	}

	log.GetLogger().Infof("start tailing file: %s", l.logFilePath)
	timeOfNoSync := time.NewTicker(time.Second)
	defer timeOfNoSync.Stop()
	for {
		select {
		case line, ok := <-l.t.Lines:
			if !ok {
				log.GetLogger().Errorf("tail lines chan closed unexpectedly")
				return errors.New("tail lines chan closed unexpectedly")
			}
			// it will send a line log to rdispatcher by websocket when open remote debug
			if utils.GetEnableRemoteDebug() {
				log.GetLogger().Debugf("it will send message to rdispatcher with %s", line)
				// no block
				websocket.SendMsgToRDispatcher(line)
			}
			l.processLine(line)
			l.notifySyncPoints()
		case err, ok := <-l.t.Errors:
			if !ok || err == nil {
				log.GetLogger().Errorf("tail error chan closed unexpectedly")
				return errors.New("tail error chan closed unexpectedly")
			}
			log.GetLogger().Errorf("failed to tail file: %s, %s", l.logFilePath, err.Error())
			return err
		case <-timeOfNoSync.C:
			l.tick = time.Now()
			l.notifySyncPoints()
		}
	}
}

// syncTo waits until logger has processed to "t"
func (l *RuntimeContainerLogger) syncTo(t time.Time) error {
	// Wait for docker log to catch up.
	time.Sleep(1 * time.Millisecond)

	content, err := makeDockerLog("", syncStream, t.UTC())
	if err != nil {
		log.GetLogger().Errorf("failed to make docker log: %s", err.Error())
		return err
	}
	if err := appendFile(l.logFilePath, content); err != nil {
		log.GetLogger().Errorf("failed to append file: %s, %s", l.logFilePath, err.Error())
		return err
	}

	syncPoint := &syncPoint{
		t:    t,
		done: make(chan struct{}),
	}

	l.syncPointCh <- syncPoint

	<-syncPoint.done

	return nil
}

func (l *RuntimeContainerLogger) addSyncPoint(syncPoint *syncPoint) {
	if !syncPoint.t.After(l.tick) {
		close(syncPoint.done)
	} else {
		l.syncPoints = append(l.syncPoints, syncPoint)
		sort.Slice(l.syncPoints, func(i, j int) bool {
			if i < 0 || i >= len(l.syncPoints) || j < 0 || j >= len(l.syncPoints) {
				return false
			}
			return l.syncPoints[i].t.Before(l.syncPoints[j].t)
		})
	}
}

func (l *RuntimeContainerLogger) notifySyncPoints() {
	idx := -1
	for i, syncPoint := range l.syncPoints {
		if syncPoint.t.After(l.tick) {
			break
		}
		idx = i
	}
	if idx != -1 {
		for i := 0; i < idx+1; i++ {
			close(l.syncPoints[i].done)
		}
		l.syncPoints = l.syncPoints[idx+1:]
	}
}

func parseContainerdLog(line string, msg *dockerLog) error {
	if err := json.Unmarshal([]byte(line), msg); err == nil {
		// json format line in containerd is sync log appended by worker
		return nil
	}
	parts := strings.SplitN(line, " ", containerdSTDLogSpaceSplitNum)
	if len(parts) < containerdSTDLogSpaceSplitNum {
		return fmt.Errorf("invalid containerd log format, line parts less then 3")
	}
	logContentIndex := strings.Index(parts[2], "F ")
	if logContentIndex == -1 {
		return fmt.Errorf("invalid containerd log format, missing 'F' separator")
	}
	logContent := parts[2][logContentIndex+2:]
	msg.Time = parts[0]
	msg.Stream = parts[1]
	msg.Log = logContent
	return nil
}
func (l *RuntimeContainerLogger) processLine(line string) {
	msg := dockerLog{}
	if !l.isContainerd {
		if err := json.Unmarshal([]byte(line), &msg); err != nil {
			log.GetLogger().Warnf("failed to parse docker log: %s, %s", line, err.Error())
			return
		}
	} else {
		if err := parseContainerdLog(line, &msg); err != nil {
			log.GetLogger().Warnf("failed to parse containerd log: %s, %s", line, err.Error())
			return
		}
	}

	log.GetLogger().Debugf("process line: %+v", msg)

	var (
		level  string
		isSync bool
	)
	switch msg.Stream {
	case "stderr":
		level = worker.FuncLogLevelWarn
	case syncStream:
		isSync = true
	default:
		level = worker.FuncLogLevelInfo
	}

	t, err := time.Parse(time.RFC3339Nano, msg.Time)
	if err != nil {
		log.GetLogger().Warnf("failed to parse time: %s to RFC3339Nano, %s", msg.Time, err.Error())
		return
	}

	if !isSync {
		l.processCB(RuntimeContainerLog{log: msg.Log, level: level, t: t})
	}

	if t.After(l.tick) {
		l.tick = t
	}
}

type tail struct {
	file           *os.File
	reader         *bufio.Reader
	watcher        *watcher
	changeNotifier *changeNotifier
	Lines          chan string
	Errors         chan error
	fileName       string
}

func tailFile(logger *RuntimeContainerLogger) error {
	t := &tail{
		Lines:    make(chan string),
		Errors:   make(chan error),
		fileName: logger.logFilePath,
	}

	var err error
	t.watcher, err = newWatcher()
	if err != nil {
		return err
	}

	go t.sync(logger)
	logger.t = t
	return nil
}

func (t *tail) sync(logger *RuntimeContainerLogger) {
	defer t.clean()

	if err := t.reopen(); err != nil {
		log.GetLogger().Errorf("failed to wait open: %s", err.Error())
		t.Errors <- err
		return
	}
	t.reader = bufio.NewReader(t.file)

	var (
		offset int64
		err    error
	)
	for {
		offset, err = t.tell()
		if err != nil {
			log.GetLogger().Errorf("failed to tell file: %s, %s", t.fileName, err.Error())
			t.Errors <- err
			return
		}

		line, err := t.readLine()
		if err != nil && errors.Is(err, io.EOF) {
			if err := t.handleReadLineEOF(logger, offset, line); err != nil {
				log.GetLogger().Errorf("failed to handle eof for file: %s, %s", t.fileName, err.Error())
				t.Errors <- err
				return
			}
			continue
		}
		if err != nil {
			log.GetLogger().Errorf("failed to read file: %s, %s", t.fileName, err.Error())
			t.Errors <- err
			return
		}
		t.Lines <- line
	}
}

func (t *tail) clean() {
	if t.watcher != nil {
		t.watcher.close()
	}
	if t.file != nil {
		if err := t.file.Close(); err != nil {
			log.GetLogger().Warnf("failed to close file: %s, %s", t.fileName, err.Error())
		}
	}
}

func (t *tail) tell() (int64, error) {
	offset, err := t.file.Seek(0, os.SEEK_CUR)
	if err != nil {
		return 0, err
	}
	offset -= int64(t.reader.Buffered())
	return offset, nil
}

func (t *tail) readLine() (string, error) {
	line, err := t.reader.ReadString('\n')
	if err != nil {
		return line, err
	}
	line = strings.TrimRight(line, "\n")
	return line, nil
}

func (t *tail) handleReadLineEOF(logger *RuntimeContainerLogger, offset int64, line string) error {
	if line != "" {
		if err := t.seekTo(offset, 0); err != nil {
			log.GetLogger().Errorf("failed to seek to offset: %s, %s", offset, err.Error())
			return err
		}
	}

	if t.changeNotifier == nil {
		pos, err := t.file.Seek(0, os.SEEK_CUR)
		if err != nil {
			log.GetLogger().Errorf("failed to seek to current position: %s", err.Error())
			return err
		}
		changeNotifier, err := t.watcher.watch(t.fileName, pos)
		if err != nil {
			log.GetLogger().Errorf("failed to watch file: %s, %s", t.fileName, err.Error())
			return err
		}
		t.changeNotifier = changeNotifier
	}

	reopen := func() error {
		if err := t.reopen(); err != nil {
			log.GetLogger().Errorf("failed to reopen: %s", err.Error())
			return err
		}
		t.reader = bufio.NewReader(t.file)
		return nil
	}

	select {
	case <-t.changeNotifier.modifyCh:
		return nil
	case <-t.changeNotifier.deleteCh:
		log.GetLogger().Infof("reopen a deleted file...")
		return reopen()
	case <-t.changeNotifier.truncateCh:
		log.GetLogger().Infof("reopen a truncated file...")
		close(t.changeNotifier.closeCh)
		<-t.changeNotifier.closeDone
		go logger.syncTo(time.Now())
		return reopen()
	case err, ok := <-t.changeNotifier.errCh:
		if !ok || err == nil {
			log.GetLogger().Errorf("change notifier error chan closed unexpectedly")
			return errors.New("change notifier error chan closed unexpectedly")
		}
		return err
	}
}

func (t *tail) seekTo(offset int64, whence int) error {
	_, err := t.file.Seek(offset, whence)
	if err != nil {
		return err
	}
	t.reader.Reset(t.file)
	return nil
}

func (t *tail) reopen() error {
	if t.file != nil {
		if err := t.file.Close(); err != nil {
			log.GetLogger().Warnf("failed to close file: %s, %s", t.fileName, err.Error())
		}
	}
	t.changeNotifier = nil

	var err error
	for {
		t.file, err = os.Open(t.fileName)
		if err == nil {
			break
		}
		if !os.IsNotExist(err) {
			return fmt.Errorf("unable to open file %s: %s", t.fileName, err.Error())
		}
		if err := t.watcher.waitUntilCreate(t.fileName); err != nil {
			if errors.Is(err, errWatcherStopped) {
				return err
			}
			return fmt.Errorf("unable to wait creation of file: %s, %s", t.fileName, err.Error())
		}
	}
	return nil
}

type watcher struct {
	watcher *fsnotify.Watcher
	stopCh  chan struct{}
}

func newWatcher() (*watcher, error) {
	wa, err := fsnotify.NewWatcher()
	if err != nil {
		log.GetLogger().Errorf("failed to new fsnotify watcher: %s", err.Error())
		return nil, err
	}

	w := &watcher{
		watcher: wa,
		stopCh:  make(chan struct{}),
	}

	return w, nil
}

func (w *watcher) close() {
	close(w.stopCh)
	if err := w.watcher.Close(); err != nil {
		log.GetLogger().Warnf("failed to close watcher: %s", err.Error())
	}
}

func (w *watcher) waitUntilCreate(fileName string) error {
	dir := filepath.Dir(fileName)
	if err := w.watcher.Add(dir); err != nil {
		log.GetLogger().Errorf("failed to watch dir: %s, %s", dir, err.Error())
		return err
	}
	defer func() {
		if err := w.watcher.Remove(dir); err != nil {
			log.GetLogger().Warnf("failed to remove watching dir: %s, %s", dir, err.Error())
		}
	}()

	_, err := os.Stat(fileName)
	if err == nil {
		return nil // file already exists.
	}
	if !os.IsNotExist(err) {
		log.GetLogger().Errorf("failed to stat file: %s, %s", fileName, err.Error())
		return err
	}

	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				log.GetLogger().Errorf("watcher event chan closed unexpectedly")
				return errors.New("inotify watcher event chan closed unexpectedly")
			}

			same, err := isSameFileName(fileName, event.Name)
			if err != nil {
				log.GetLogger().Errorf("failed to check is same file name for file %s and %s",
					fileName, event.Name, err.Error())
				return err
			}
			if same {
				log.GetLogger().Infof("file %s is created", fileName)
				return nil
			}
		case err, ok := <-w.watcher.Errors:
			if !ok || err == nil {
				log.GetLogger().Errorf("watcher errors chan closed unexpectedly")
				return errors.New("inotify watcher errors chan closed unexpectedly")
			}
			log.GetLogger().Errorf("failed to watch file, watcher returns error: %s", err.Error())
			return err
		case <-w.stopCh:
			return errWatcherStopped
		}
	}
}

func (w *watcher) watch(fileName string, pos int64) (*changeNotifier, error) {
	if err := w.watcher.Add(fileName); err != nil {
		log.GetLogger().Errorf("failed to watch file: %s, %s", fileName, err.Error())
		return nil, err
	}

	c := newChangeNotifier()
	go func() {
		for {
			if !w.handle(c, fileName, &pos) {
				return
			}
		}
	}()

	return c, nil
}

func (w *watcher) handle(c *changeNotifier, fileName string, size *int64) bool {
	prevSize := *size

	select {
	case event, ok := <-w.watcher.Events:
		if !ok {
			log.GetLogger().Errorf("watcher event chan closed unexpectedly")
			c.errCh <- errors.New("watcher event chan closed unexpectedly")
			return false
		}

		switch {
		case event.Op&fsnotify.Remove > 0 || event.Op&fsnotify.Rename > 0:
			w.handleDelete(c, fileName)
			return false
		case event.Op&fsnotify.Chmod > 0 || event.Op&fsnotify.Write > 0:
			fi, err := os.Stat(fileName)
			if err != nil && os.IsNotExist(err) {
				w.handleDelete(c, fileName)
				return false
			}
			if err != nil {
				log.GetLogger().Errorf("failed to stat file: %s, %s", fileName, err.Error())
				c.errCh <- fmt.Errorf("failed to stat file: %s, %s", fileName, err.Error())
				return false
			}
			*size = fi.Size()
			// if file size is less or prevSize == *size == 0, we think file is truncate
			if (prevSize > *size) || (prevSize == 0 && *size == 0) {
				w.notify(c.truncateCh)
			} else {
				w.notify(c.modifyCh)
			}
		default:
			// nothing to do
		}
	case err, ok := <-w.watcher.Errors:
		if !ok || err == nil {
			log.GetLogger().Errorf("watcher errors chan closed unexpectedly")
			c.errCh <- errors.New("watcher errors chan closed unexpectedly")
			return false
		}
		log.GetLogger().Errorf("failed to watch file, watcher returns error: %s", err.Error())
		c.errCh <- fmt.Errorf("failed to watch file, watcher returns error: %s", err.Error())
		return false
	case <-w.stopCh:
		return false
	case <-c.closeCh:
		close(c.closeDone)
		return false
	}
	return true
}

func (w *watcher) handleDelete(c *changeNotifier, fileName string) {
	if err := w.watcher.Remove(fileName); err != nil {
		log.GetLogger().Warnf("failed to remove watching file: %s, %s", fileName, err.Error())
	}
	w.notify(c.deleteCh)
}

func (w *watcher) notify(ch chan struct{}) {
	if ch == nil {
		log.GetLogger().Warnf("nil chan")
		return
	}

	select {
	case ch <- struct{}{}:
	default:
	}
}

type changeNotifier struct {
	modifyCh   chan struct{}
	deleteCh   chan struct{}
	truncateCh chan struct{}
	errCh      chan error
	closeCh    chan struct{}
	closeDone  chan struct{}
}

func newChangeNotifier() *changeNotifier {
	return &changeNotifier{
		modifyCh:   make(chan struct{}, 1),
		deleteCh:   make(chan struct{}, 1),
		truncateCh: make(chan struct{}, 1),
		errCh:      make(chan error),
		closeCh:    make(chan struct{}),
		closeDone:  make(chan struct{}),
	}
}

func isSameFileName(a, b string) (bool, error) {
	var err error
	a, err = filepath.Abs(a)
	if err != nil {
		return false, err
	}
	b, err = filepath.Abs(b)
	if err != nil {
		return false, err
	}
	return a == b, nil
}

func makeDockerLog(content, stream string, t time.Time) ([]byte, error) {
	strTime := t.Format(time.RFC3339Nano)
	d := &dockerLog{
		Log:    content,
		Stream: stream,
		Time:   strTime,
	}
	b, err := json.Marshal(d)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal docker log: %v %s", d, err.Error())
		return nil, err
	}
	return append(b, '\n'), nil
}

func appendFile(fileName string, b []byte) error {
	fi, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer fi.Close()

	if _, err := fi.Write(b); err != nil {
		return err
	}

	return nil
}
