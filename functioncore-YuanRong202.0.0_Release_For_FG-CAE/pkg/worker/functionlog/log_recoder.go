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

package functionlog

import (
	"bytes"
	"container/list"
	"encoding/base64"
	"encoding/binary"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/utils"
)

const (
	// user log will be deleted by cffagent for follow reasons:
	// 1.irregular log file name
	// 2.unchanged for 12 hours when log is not empty
	// 3.unchanged for 6 hours when log is empty
	timeModInterval = 5 * time.Hour
	timeModRetry    = 3
)

var lrRange = invokectx.DefaultMetrics.RegisterRange(func() invokectx.Poolable { return NewLogRecorder() })

// LogRecorderBox -
var LogRecorderBox = make([]*LogRecorder, 0, 2)

func getLogRecorder(ic invokectx.InvokeCtx, f func(invokectx.InvokeCtx) invokectx.Poolable) *LogRecorder {
	logRecorderIf := f(ic)
	logRecorder, ok := logRecorderIf.(*LogRecorder)
	if !ok {
		log.GetLogger().Errorf("failed to assert logRecorder")
		return nil
	}
	return logRecorder
}

func rangeLogRecorder(f func(invokectx.InvokeCtx, *LogRecorder) bool) bool {
	exist := lrRange.Range(func(ic invokectx.InvokeCtx, logRecorderIf invokectx.Poolable) bool {
		logRecorder, ok := logRecorderIf.(*LogRecorder)
		if !ok {
			log.GetLogger().Errorf("failed to assert logRecorder")
			return false
		}
		return f(ic, logRecorder)
	})
	return exist
}

// RangeLoggerWithTraceID -
func (f *FunctionLogger) RangeLoggerWithTraceID(funcLog *FunctionLog, traceID string) {
	exist := rangeLogRecorder(func(ic invokectx.InvokeCtx, logRecorder *LogRecorder) bool {
		if traceID == logRecorder.traceID {
			if logRecorder.Stage() != constants.InvokeStage && logRecorder.Stage() != constants.ColdStartStage {
				logRecorder.WriteLimit(funcLog)
				return false
			}
			logRecorder.RealTimeWrite(funcLog)
			return false
		}
		return true
	})
	if !exist {
		f.WriteDefaultLogWithTraceID(funcLog, traceID)
	}
}

// LogHeaderFooterGenerator -
type LogHeaderFooterGenerator interface {
	GenerateLogHeader(*FunctionLog)
	GenerateLogFooter(*FunctionLog)
}

// FixSizeRecorder tries to maintain a number of logs with the total size just over limitSize. When we write some new
// logs to the recorder, old logs will be dropped.
type FixSizeRecorder struct {
	logs      *list.List
	guessSize func(funcLog *FunctionLog) int
	dropped   func(funcLog *FunctionLog, logTooLarge bool)
	size      int
	limitSize int
}

// NewFixSizeRecorder -
func NewFixSizeRecorder(
	limitSize int,
	guessSize func(funcLog *FunctionLog) int,
	dropped func(funcLog *FunctionLog, logTooLarge bool)) *FixSizeRecorder {
	return &FixSizeRecorder{
		guessSize: guessSize,
		dropped:   dropped,
		limitSize: limitSize,
	}
}

// Reset resets the recorder to its initial state
func (r *FixSizeRecorder) Reset() {
	if r.logs != nil {
		r.logs.Init()
	}
	r.size = 0
}

// Write -
func (r *FixSizeRecorder) Write(funcLog *FunctionLog) {
	if r.logs == nil {
		r.logs = list.New()
	}

	if r.size > r.limitSize {
		r.processOverLimitSize(r.limitSize, true)
	}

	size := r.guessSize(funcLog)
	if size > r.limitSize { // special case: the funcLog is too large
		r.Range(func(fl *FunctionLog) {
			r.dropped(fl, false)
		})
		r.logs = nil
		r.size = 0
		r.dropped(funcLog, true)
	} else {
		r.logs.PushBack(funcLog)
		r.size += size
	}
}

func (r *FixSizeRecorder) processUserLargeLog(funcLog *FunctionLog) {
	r.dropped(funcLog, true)
	r.processOverLimitSize(r.limitSize, false)
	r.logs.PushBack(funcLog.DeepCopy())
	r.size += r.guessSize(funcLog)
	return
}

// RealTimeWrite -
func (r *FixSizeRecorder) RealTimeWrite(funcLog *FunctionLog) {
	if r.logs == nil {
		r.logs = list.New()
	}
	if r.size > r.limitSize {
		r.processOverLimitSize(r.limitSize, false)
	}
	size := r.guessSize(funcLog)
	if size > r.limitSize {
		r.processUserLargeLog(funcLog)
		return
	}
	r.logs.PushBack(funcLog.DeepCopy())
	r.size += size
	r.dropped(funcLog, false)
}

// Range -
func (r *FixSizeRecorder) Range(fn func(*FunctionLog)) {
	if r.logs == nil {
		return
	}

	for e := r.logs.Front(); e != nil; e = e.Next() {
		funcLog, ok := e.Value.(*FunctionLog)
		if !ok {
			continue
		}

		fn(funcLog)
	}
}

func (r *FixSizeRecorder) processOverLimitSize(limitSize int, writeModeFlag bool) {
	for {
		elem := r.logs.Front()
		if elem == nil {
			break
		}
		funcLog, ok := elem.Value.(*FunctionLog)
		if !ok {
			break
		}

		size := r.guessSize(funcLog)
		if r.size-size < limitSize {
			break
		}
		r.size -= size
		if writeModeFlag {
			r.dropped(funcLog, false)
		}
		r.logs.Remove(elem)
	}
}

// WriteLimit is to save limitSize log
func (r *FixSizeRecorder) WriteLimit(funcLog *FunctionLog, limitSize int) {
	if r.logs == nil {
		r.logs = list.New()
	}
	if r.size > limitSize {
		r.processOverLimitSize(limitSize, false)
	}

	size := r.guessSize(funcLog)
	r.logs.PushBack(funcLog)
	r.size += size
}

// LogRecorder record user log of function
type LogRecorder struct {
	f         *FunctionLogger
	header    *FunctionLog
	footer    *FunctionLog
	logs      *FixSizeRecorder
	logsMux   sync.Mutex
	generator LogHeaderFooterGenerator
	idx       int
	base      []byte
	syncCh    chan struct{}

	invokeID               [8]byte // uint64 bytes
	traceID                string
	stage                  string
	logLevel               string
	livedataID             string
	logOption              string
	separatedWithLineBreak bool
}

type internalFunctionLog struct {
	*FunctionLog
	FunctionName string
	TraceID      string
	LivedataID   string
	Stage        string
	Index        int
}

// FunctionLog is format of function logs which are flushed
type FunctionLog struct {
	Message         *bytes.Buffer
	Time            string
	NanoTime        string
	Level           string
	ErrorType       string
	IsFinishedLog   bool
	IsStdLog        bool
	ReportSummary   bool
	CPUUsed         float64
	MemoryUsed      float64
	Duration        float64
	StorageUsed     float64
	TraceID         string
	Stage           string
	IsStart         bool
	IsFinish        bool
	LogType         string
	Summary         string
	ErrorCode       int32
	LiveDataTraceID string
}

// NewFunctionLog -
func NewFunctionLog() *FunctionLog {
	return &FunctionLog{Message: new(bytes.Buffer)}
}

// Reset -
func (l *FunctionLog) Reset() {
	l.Time = ""
	l.Level = ""
	l.Message.Reset()
	l.ErrorType = ""
	l.IsFinishedLog = false
	l.IsStdLog = false
	l.ReportSummary = false
	l.CPUUsed = 0
	l.MemoryUsed = 0
	l.Duration = 0
	l.StorageUsed = 0
}

// DeepCopy -
func (l *FunctionLog) DeepCopy() *FunctionLog {
	x := *l // copy
	oldBuf := l.Message.Bytes()
	newBuf := make([]byte, len(oldBuf))
	copy(newBuf, oldBuf)
	x.Message = bytes.NewBuffer(newBuf)
	return &x
}

// NewLogRecorder -
func NewLogRecorder() *LogRecorder {
	r := &LogRecorder{}
	r.logs = NewFixSizeRecorder(maxTailLogSizeKB, r.guessSize, r.handleDropped)
	return r
}

// Reset resets the recorder to its initial state
func (r *LogRecorder) Reset() {
	r.f = nil
	r.header = nil
	r.footer = nil
	r.logs.Reset()
	r.generator = nil
	r.idx = 0
	r.base = nil
	r.syncCh = nil
	binary.BigEndian.PutUint64(r.invokeID[:], 0)
	r.traceID = ""
	r.stage = ""
	r.logLevel = ""
	r.livedataID = ""
	r.logOption = ""
	r.separatedWithLineBreak = false
}

func (r *LogRecorder) handleDropped(funcLog *FunctionLog, logTooLarge bool) {
	if r.idx == 0 {
		if header := r.getHeader(); header != nil {
			// Header can still be used in 'MarshalAll'. Pass in a copy to prevent releasing the old header
			// to the sync.Pool.
			r.f.write(r.generateInternalFunctionLog(header.DeepCopy(), 0))
		}
		r.idx++
	}

	// when new drop happens, we no longer need the previous base
	if r.base != nil {
		r.base = nil
	}

	if logTooLarge {
		res := r.formatLog(funcLog)
		if len(res) > maxTailLogSizeKB {
			res = res[len(res)-maxTailLogSizeKB:]
		}
		r.base = make([]byte, len(res))
		copy(r.base, res)
	}

	r.f.write(r.generateInternalFunctionLog(funcLog, r.idx))
	r.idx++
}

// guessSize returns the guessed size of bytes when calling 'formatLog' for the same 'functionLog'. The guessed size
// may not be accurate.
func (r *LogRecorder) guessSize(functionLog *FunctionLog) int {
	if functionLog.IsStdLog {
		return functionLog.Message.Len()
	}
	// 1 is the ' ' in between
	return len(functionLog.Time) + 1 + len(r.traceID) + 1 + len(r.logLevel) + 1 + functionLog.Message.Len()
}

func (r *LogRecorder) formatLog(functionLog *FunctionLog) []byte {
	// user std log without additional information
	if functionLog.IsStdLog {
		return functionLog.Message.Bytes()
	}
	level := functionLog.Level
	if level == "WARNING" {
		level = worker.FuncLogLevelWarn
	}

	var b bytes.Buffer
	b.Grow(len(functionLog.Time) + 1 + len(r.traceID) + 1 + len(level) + 1 + functionLog.Message.Len())
	b.WriteString(functionLog.Time)
	b.WriteString(" ")
	b.WriteString(r.traceID)
	b.WriteString(" ")
	b.WriteString(level)
	b.WriteString(" ")
	b.Write(functionLog.Message.Bytes())

	return b.Bytes()
}

// StartSync -
func (r *LogRecorder) StartSync() {
	r.syncCh = make(chan struct{})
}

// FinishSync -
func (r *LogRecorder) FinishSync() {
	close(r.syncCh)
}

// MarshalAll -
func (r *LogRecorder) MarshalAll() string {
	if r.syncCh != nil {
		<-r.syncCh
	}

	r.logsMux.Lock()

	var buffer bytes.Buffer
	if r.base != nil {
		buffer.Write(r.base)
		if r.separatedWithLineBreak {
			buffer.WriteString("\n")
		}
	}
	r.logs.Range(func(piece *FunctionLog) {
		buffer.Write(r.formatLog(piece))
		if r.separatedWithLineBreak {
			buffer.WriteString("\n")
		}
	})

	res := buffer.String()
	if len(res) > maxTailLogSizeKB {
		res = res[len(res)-maxTailLogSizeKB:]
	}

	buffer.Reset()
	if header := r.getHeader(); header != nil {
		buffer.Write(header.Message.Bytes())
	}
	buffer.WriteString("\n")
	buffer.WriteString(res)

	if footer := r.getFooter(); footer != nil {
		buffer.Write(footer.Message.Bytes())
	}

	encoded := base64.StdEncoding.EncodeToString(buffer.Bytes())
	r.logsMux.Unlock()
	return encoded
}

// Finish finalizes the logRecorder. This MUST be called.
func (r *LogRecorder) Finish(ic invokectx.InvokeCtx) {
	if r.syncCh != nil {
		<-r.syncCh
	}

	r.logsMux.Lock()

	if r.idx == 0 {
		if header := r.getHeader(); header != nil {
			r.f.write(r.generateInternalFunctionLog(header, 0))
		}
		r.idx++
	}
	r.logs.Range(func(piece *FunctionLog) {
		r.f.write(r.generateInternalFunctionLog(piece, r.idx))
		r.idx++
	})
	if footer := r.getFooter(); footer != nil {
		r.f.write(r.generateInternalFunctionLog(footer, -1))
	}

	r.logsMux.Unlock()

	getLogRecorder(ic, lrRange.Unmark)
}

// FinishHeader -
func (r *LogRecorder) FinishHeader(ic invokectx.InvokeCtx) {
	if r.syncCh != nil {
		<-r.syncCh
	}

	r.logsMux.Lock()

	if r.idx == 0 {
		if header := r.getHeader(); header != nil {
			r.f.write(r.generateInternalFunctionLog(header.DeepCopy(), 0))
		}
		r.idx++
	}
	r.logsMux.Unlock()
}

// FinishFooter -
func (r *LogRecorder) FinishFooter(ic invokectx.InvokeCtx) {
	if r.syncCh != nil {
		<-r.syncCh
	}
	r.logsMux.Lock()
	if footer := r.getFooter(); footer != nil {
		r.f.write(r.generateInternalFunctionLog(footer, -1))
	}

	r.logsMux.Unlock()
	getLogRecorder(ic, lrRange.Unmark)
}

// FinishCopy is to copy load and init log to mem in normal instance
func (r *LogRecorder) FinishCopy(ic invokectx.InvokeCtx) {
	r.logsMux.Lock()
	logRecorderCopy := &LogRecorder{}
	logRecorderCopy.f = r.f
	logRecorderCopy.header = r.header
	logRecorderCopy.footer = r.footer

	logs := &FixSizeRecorder{}
	logs.logs = r.logs.logs
	// old logs need to be nil, because Reset() canceled
	r.logs.logs = nil
	logs.dropped = r.logs.dropped
	logs.guessSize = r.logs.guessSize
	logs.limitSize = r.logs.limitSize
	logs.size = r.logs.size

	logRecorderCopy.logs = logs
	logRecorderCopy.generator = r.generator
	logRecorderCopy.idx = r.idx
	logRecorderCopy.base = r.base
	logRecorderCopy.syncCh = r.syncCh
	logRecorderCopy.invokeID = r.invokeID
	logRecorderCopy.traceID = r.traceID
	logRecorderCopy.stage = r.stage
	logRecorderCopy.logLevel = r.logLevel
	logRecorderCopy.livedataID = r.livedataID
	logRecorderCopy.logOption = r.logOption
	logRecorderCopy.separatedWithLineBreak = r.separatedWithLineBreak
	LogRecorderBox = append(LogRecorderBox, logRecorderCopy)
	r.logsMux.Unlock()
	getLogRecorder(ic, lrRange.Unmark)
}

// FinishSave is to save load and init log to disk in normal instance
func (r *LogRecorder) FinishSave() {
	r.logsMux.Lock()
	if r.idx == 0 {
		if header := r.getHeader(); header != nil {
			r.f.write(r.generateInternalFunctionLog(header, 0))
		}
		r.idx++
	}
	r.logs.Range(func(piece *FunctionLog) {
		r.f.write(r.generateInternalFunctionLog(piece, r.idx))
		r.idx++
	})
	if footer := r.getFooter(); footer != nil {
		footer.IsFinishedLog = false
		r.f.write(r.generateInternalFunctionLog(footer, -1))
	}

	r.logsMux.Unlock()
}

// ReplaceTraceID is to replace load and init log in normal instance
func (r *LogRecorder) ReplaceTraceID(traceID, livedataID string) {
	r.logsMux.Lock()
	r.traceID = traceID
	r.livedataID = livedataID

	r.logsMux.Unlock()
}

func (r *LogRecorder) generateInternalFunctionLog(functionLog *FunctionLog, idx int) *internalFunctionLog {
	return &internalFunctionLog{
		FunctionLog:  functionLog,
		FunctionName: utils.LocalFuncURN.Name,
		TraceID:      r.traceID,
		LivedataID:   r.livedataID,
		Stage:        r.stage,
		Index:        idx,
	}
}

// SetHeader set logger header
func (r *LogRecorder) SetHeader(header *FunctionLog) {
	r.logsMux.Lock()
	r.header = header
	r.generator.GenerateLogHeader(header)
	r.logsMux.Unlock()
}

// SetFooter set logger footer
func (r *LogRecorder) SetFooter(footer *FunctionLog, summaryInfo LogHeaderFooterGenerator) {
	r.logsMux.Lock()
	r.footer = footer
	r.generator = summaryInfo
	r.generator.GenerateLogFooter(footer)
	r.logsMux.Unlock()
	return
}

func (r *LogRecorder) getHeader() *FunctionLog {
	if r.header != nil {
		return r.header
	}
	if r.generator != nil {
		r.header = r.f.AcquireLog()
		r.generator.GenerateLogHeader(r.header)
	}
	return r.header
}

func (r *LogRecorder) getFooter() *FunctionLog {
	if r.footer != nil {
		return r.footer
	}

	if r.generator != nil {
		r.footer = r.f.AcquireLog()
		r.generator.GenerateLogFooter(r.footer)
	}
	return r.footer
}

// Write log to logRecorder. This can be called from a different goroutine
func (r *LogRecorder) Write(funcLog *FunctionLog) {
	r.logsMux.Lock()
	r.logs.Write(funcLog)
	r.logsMux.Unlock()
}

// WriteLimit write limited log to logRecorder. not reserved to disk
func (r *LogRecorder) WriteLimit(funcLog *FunctionLog) {
	r.logsMux.Lock()
	r.logs.WriteLimit(funcLog, maxInitLogSizeMB)
	r.logsMux.Unlock()
}

// RealTimeWrite log to logRecorder. This can be called from a different goroutine
func (r *LogRecorder) RealTimeWrite(funcLog *FunctionLog) {
	r.logsMux.Lock()
	r.logs.RealTimeWrite(funcLog)
	r.logsMux.Unlock()
}

// LogOption -
func (r *LogRecorder) LogOption() string {
	return r.logOption
}

// InvokeID -
func (r *LogRecorder) InvokeID() []byte {
	return r.invokeID[:]
}

// TraceID -
func (r *LogRecorder) TraceID() string {
	return r.traceID
}

// Stage -
func (r *LogRecorder) Stage() string {
	return r.stage
}

// Generator -
func (r *LogRecorder) Generator() LogHeaderFooterGenerator {
	return r.generator
}
