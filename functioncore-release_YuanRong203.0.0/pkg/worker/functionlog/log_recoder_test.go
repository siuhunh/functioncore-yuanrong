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
	"io"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger"
	loggerConfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/utils"
	mockUtils "functioncore/test/utils"
)

const logDir = "/home/sn/log"

func TestMain(m *testing.M) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(loggerConfig.GetCoreInfoFromEnv, func() (loggerConfig.CoreInfo, error) { return loggerConfig.GetDefaultCoreInfo(), nil })
	p.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
		return &os.File{}, nil
	})
	p.ApplyFunc(logger.CreateSink, func(coreInfo loggerConfig.CoreInfo) (io.Writer, error) {
		return io.Discard, nil
	})

	GetFunctionLogger(&config.Configuration{StartArgs: config.StartArgs{
		LogDir:   logDir,
		IsLoaded: false,
	}})

	p.Reset()
	m.Run()
	createLoggerOnce = sync.Once{}
	os.RemoveAll(logDir)
}

func TestLogRecorder_Fusion(t *testing.T) {
	logger, err := GetFunctionLogger(&config.Configuration{
		StartArgs: config.StartArgs{LogDir: logDir},
	})
	if err != nil {
		t.Fatalf("failed to get function logger, capability:fusion, %s", err.Error())
	}
	logger.SetLogLevel(worker.FuncLogLevelWarn)
	traceID := "traceID"
	metrics := invokectx.DefaultMetrics
	ic := metrics.AcquireInvokeCtx()
	defer invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
	logRecorder := logger.NewLogRecorder(ic, traceID, constants.InvokeStage, WithoutLineBreak(), WithLogOptionTail())
	if logger.GetLogRecorder(ic) == nil {
		t.Error("failed to get log recorder by invokeID")
	}
	logRecorder.Write(&FunctionLog{
		Level:   "INFO",
		Message: bytes.NewBufferString("message"),
	})

	logger.WriteStdLog("std logs", "my time", false, "time")
	logRecorder.Finish(ic)
}

func TestGetLTSLoggerMessage(m *testing.T) {
	functionLog := &internalFunctionLog{FunctionLog: &FunctionLog{}}
	functionLog.Index = -1
	functionLog.Time = ""
	functionLog.TraceID = ""
	functionLog.Message = new(bytes.Buffer)
	msg := getLTSLoggerMessage(functionLog)
	assert.Equal(m, len(msg), 0)

	functionLog = &internalFunctionLog{FunctionLog: &FunctionLog{}}
	functionLog.Index = 1
	functionLog.Time = "test time"
	functionLog.TraceID = "test 1"
	functionLog.Message = bytes.NewBufferString("time test message")
	msg = getLTSLoggerMessage(functionLog)
	assert.NotEqual(m, len(msg), 0)

	functionLog = &internalFunctionLog{FunctionLog: &FunctionLog{}}
	functionLog.Index = constants.DefaultFuncLogIndex
	functionLog.Time = "test time"
	functionLog.TraceID = "test 2"
	functionLog.Message = bytes.NewBufferString("time test message")
	str := functionLog.Time + " " + "time test message"
	msg = getLTSLoggerMessage(functionLog)
	assert.Equal(m, str, msg)
}

func TestFormatLog(m *testing.T) {
	r := &LogRecorder{}

	t := r.formatLog(&FunctionLog{
		Time:    "time",
		Level:   "WARNING",
		Message: new(bytes.Buffer),
	})
	assert.Equal(m, "time  WARN ", string(t))
}

func TestMarshalAll(t *testing.T) {
	convey.Convey("Test MarshalAll", t, func() {
		convey.Convey("Test no log", func() {
			r := &LogRecorder{
				f:                      &FunctionLogger{},
				separatedWithLineBreak: true,
			}

			limitSize := maxTailLogSizeKB
			droppedNum := 0
			r.logs = NewFixSizeRecorder(
				limitSize,
				r.guessSize,
				func(funcLog *FunctionLog, logTooLarge bool) {
					droppedNum++
					r.handleDropped(funcLog, logTooLarge)
				},
			)

			r.MarshalAll()

			var buffer bytes.Buffer
			r.logs.Range(func(piece *FunctionLog) {
				buffer.Write(r.formatLog(piece))
				if r.separatedWithLineBreak {
					buffer.WriteString("\n")
				}
			})

			assert.Equal(t, 0, droppedNum)
			assert.Equal(t, 0, len(buffer.Bytes()))
			assert.Equal(t, 0, r.idx)

			metrics := invokectx.DefaultMetrics
			r.Finish(metrics.AcquireInvokeCtx())

			assert.Equal(t, 1, r.idx)
		})
		convey.Convey("Test too many logs", func() {
			r := &LogRecorder{
				f:                      &FunctionLogger{},
				separatedWithLineBreak: true,
			}

			limitSize := maxTailLogSizeKB
			droppedNum := 0
			r.logs = NewFixSizeRecorder(
				limitSize,
				r.guessSize,
				func(funcLog *FunctionLog, logTooLarge bool) {
					droppedNum++
					r.handleDropped(funcLog, logTooLarge)
				},
			)

			writeNum := 150
			for idx := 0; idx < writeNum; idx++ {
				r.Write(&FunctionLog{
					Message: bytes.NewBufferString("Function log test"),
				})
			}
			r.MarshalAll()

			var buffer bytes.Buffer
			r.logs.Range(func(piece *FunctionLog) {
				buffer.Write(r.formatLog(piece))
				if r.separatedWithLineBreak {
					buffer.WriteString("\n")
				}
			})

			assert.True(t, droppedNum > 0)
			assert.True(t, len(buffer.Bytes()) > limitSize)
			assert.Equal(t, droppedNum+1, r.idx)

			metrics := invokectx.DefaultMetrics
			r.Finish(metrics.AcquireInvokeCtx())

			assert.Equal(t, writeNum+1, r.idx)
		})
		convey.Convey("Test logs", func() {
			r := &LogRecorder{
				f:                      &FunctionLogger{},
				separatedWithLineBreak: true,
			}

			limitSize := maxTailLogSizeKB
			droppedNum := 0
			r.logs = NewFixSizeRecorder(
				limitSize,
				r.guessSize,
				func(funcLog *FunctionLog, logTooLarge bool) {
					droppedNum++
					r.handleDropped(funcLog, logTooLarge)
				},
			)

			writeNum := 20
			for idx := 0; idx < writeNum; idx++ {
				r.Write(&FunctionLog{
					Message: bytes.NewBufferString("Function log test"),
				})
			}
			r.MarshalAll()

			var buffer bytes.Buffer
			r.logs.Range(func(piece *FunctionLog) {
				buffer.Write(r.formatLog(piece))
				if r.separatedWithLineBreak {
					buffer.WriteString("\n")
				}
			})

			assert.Equal(t, 0, droppedNum)
			assert.True(t, len(buffer.Bytes()) < limitSize)
			assert.Equal(t, 0, r.idx)

			metrics := invokectx.DefaultMetrics
			r.Finish(metrics.AcquireInvokeCtx())

			assert.Equal(t, writeNum+1, r.idx)
		})
	})
}

func TestMarshalAll2(t *testing.T) {
	convey.Convey("Test logTooLarge", t, func() {
		r := &LogRecorder{
			f:                      &FunctionLogger{},
			separatedWithLineBreak: true,
		}

		limitSize := maxTailLogSizeKB
		droppedNum := 0
		r.logs = NewFixSizeRecorder(
			limitSize,
			r.guessSize,
			func(funcLog *FunctionLog, logTooLarge bool) {
				droppedNum++
				r.handleDropped(funcLog, logTooLarge)
			},
		)

		msg := strings.Repeat("a", maxTailLogSizeKB+1)
		r.Write(&FunctionLog{
			Message: bytes.NewBufferString(msg),
		})
		r.Write(&FunctionLog{
			Message: bytes.NewBufferString("Function log test"),
		})
		r.MarshalAll()

		assert.NotNil(t, r.base)

		var buffer bytes.Buffer
		r.logs.Range(func(piece *FunctionLog) {
			buffer.Write(r.formatLog(piece))
			if r.separatedWithLineBreak {
				buffer.WriteString("\n")
			}
		})

		assert.Equal(t, 1, droppedNum)
		assert.True(t, len(buffer.Bytes()) != 0)
		assert.True(t, len(buffer.Bytes()) < limitSize)
		assert.Equal(t, 2, r.idx)

		metrics := invokectx.DefaultMetrics
		r.Finish(metrics.AcquireInvokeCtx())

		assert.Equal(t, 3, r.idx)
	})
	convey.Convey("Test logTooLarge then drop", t, func() {
		r := &LogRecorder{
			f:                      &FunctionLogger{},
			separatedWithLineBreak: true,
		}

		limitSize := maxTailLogSizeKB
		droppedNum := 0
		r.logs = NewFixSizeRecorder(
			limitSize,
			r.guessSize,
			func(funcLog *FunctionLog, logTooLarge bool) {
				droppedNum++
				r.handleDropped(funcLog, logTooLarge)
			},
		)

		msg := strings.Repeat("a", maxTailLogSizeKB+1)
		r.Write(&FunctionLog{
			Message: bytes.NewBufferString(msg),
		})

		writeNum := 150
		for idx := 0; idx < writeNum; idx++ {
			r.Write(&FunctionLog{
				Message: bytes.NewBufferString("Function log test"),
			})
		}
		r.MarshalAll()

		assert.Nil(t, r.base)

		var buffer bytes.Buffer
		r.logs.Range(func(piece *FunctionLog) {
			buffer.Write(r.formatLog(piece))
			if r.separatedWithLineBreak {
				buffer.WriteString("\n")
			}
		})

		assert.True(t, droppedNum > 0)
		assert.True(t, len(buffer.Bytes()) > limitSize)
		assert.Equal(t, droppedNum+1, r.idx)

		metrics := invokectx.DefaultMetrics
		r.Finish(metrics.AcquireInvokeCtx())

		assert.Equal(t, writeNum+2, r.idx)

		r.Finish(metrics.AcquireInvokeCtx())
	})
	convey.Convey("Test drop then logTooLarge", t, func() {
		r := &LogRecorder{
			f:                      &FunctionLogger{},
			separatedWithLineBreak: true,
		}

		limitSize := maxTailLogSizeKB
		droppedNum := 0
		r.logs = NewFixSizeRecorder(
			limitSize,
			r.guessSize,
			func(funcLog *FunctionLog, logTooLarge bool) {
				droppedNum++
				r.handleDropped(funcLog, logTooLarge)
			},
		)

		writeNum := 150
		for idx := 0; idx < writeNum; idx++ {
			r.Write(&FunctionLog{
				Message: bytes.NewBufferString("Function log test"),
			})
		}
		msg := strings.Repeat("a", maxTailLogSizeKB+1)
		r.Write(&FunctionLog{
			Message: bytes.NewBufferString(msg),
		})
		r.MarshalAll()

		assert.NotNil(t, r.base)

		var buffer bytes.Buffer
		r.logs.Range(func(piece *FunctionLog) {
			buffer.Write(r.formatLog(piece))
			if r.separatedWithLineBreak {
				buffer.WriteString("\n")
			}
		})

		assert.True(t, droppedNum > 0)
		assert.Equal(t, 0, len(buffer.Bytes()))
		assert.Equal(t, droppedNum+1, r.idx)

		metrics := invokectx.DefaultMetrics
		r.Finish(metrics.AcquireInvokeCtx())

		assert.Equal(t, droppedNum+1, r.idx)
	})
}

type fakeGenerator struct {
	header *FunctionLog
}

func (f *fakeGenerator) GenerateLogHeader(header *FunctionLog) { f.header = header }
func (*fakeGenerator) GenerateLogFooter(*FunctionLog)          {}

func TestLogPool(t *testing.T) {
	generator := &fakeGenerator{}
	r := &LogRecorder{
		f:                      NewFunctionLogger(zap.NewNop(), "", ""),
		separatedWithLineBreak: true,
		generator:              generator,
	}
	r.handleDropped(r.f.AcquireLog(), false)
	assert.True(t, r.header == generator.header)
	assert.True(t, r.f.AcquireLog() != r.header) // header should not be released
}

func TestFunctionLogDeepCopy(t *testing.T) {
	l := NewFunctionLog()
	l.Message.WriteString("abc")
	l.ErrorType = "xxx"
	l.IsFinishedLog = true
	l.IsStdLog = true
	l.Level = "info"

	x := l.DeepCopy()
	assert.True(t, l != x)
	assert.Equal(t, l, x)
}

func TestFixSizeRecorder_WriteLimit(t *testing.T) {
	r := &LogRecorder{}
	case2Logs := list.New()
	case2Logs.PushBack(&FunctionLog{
		Level:   "INFO",
		Message: bytes.NewBufferString("abc"),
	})
	type fields struct {
		logs      *list.List
		guessSize func(funcLog *FunctionLog) int
		dropped   func(funcLog *FunctionLog, logTooLarge bool)
		size      int
		limitSize int
	}
	type args struct {
		funcLog   *FunctionLog
		limitSize int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1 write less than limitSize", fields{logs: nil, guessSize: r.guessSize, limitSize: 20},
			args{funcLog: &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString("abc"),
			}, limitSize: 10}},
		{"case2 write more than limitSize", fields{logs: case2Logs, guessSize: r.guessSize, size: 20,
			limitSize: 20}, args{funcLog: &FunctionLog{
			Level:   "INFO",
			Message: bytes.NewBufferString("abc"),
		}, limitSize: 10}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &FixSizeRecorder{
				logs:      tt.fields.logs,
				guessSize: tt.fields.guessSize,
				dropped:   tt.fields.dropped,
				size:      tt.fields.size,
				limitSize: tt.fields.limitSize,
			}
			r.WriteLimit(tt.args.funcLog, tt.args.limitSize)
			if r.size > 20 {
				t.Errorf("write limit error, size is: %d", r.size)
			}
		})

	}
}

func TestLogRecorder_WriteLimit(t *testing.T) {
	functionLog := &FunctionLog{
		Level:   "INFO",
		Message: bytes.NewBufferString(strings.Repeat("a", 90*1024)),
	}
	r := NewLogRecorder()
	for i := 0; i < 256; i++ {
		r.WriteLimit(functionLog)
	}
	if r.logs.size > maxInitLogSizeMB+90*1024*2 {
		t.Errorf("write limit error, size is: %d", r.logs.size)
	}
}

func TestLogRecorder_RealTimeWrite(t *testing.T) {
	convey.Convey("TestLogRecorder_RealTimeWrite", t, func() {
		convey.Convey("small log", func() {
			functionLog := &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString(strings.Repeat("a", 2*1024)),
			}
			p := gomonkey.NewPatches()
			p.ApplyFunc(NewFixSizeRecorder, func(limitSize int,
				guessSize func(funcLog *FunctionLog) int,
				dropped func(funcLog *FunctionLog, logTooLarge bool)) *FixSizeRecorder {
				return &FixSizeRecorder{
					nil,
					func(funcLog *FunctionLog) int {
						return 2 * 1024
					},
					func(funcLog *FunctionLog, logTooLarge bool) {
						return
					},
					0,
					10 * 1024}
			})
			defer p.Reset()
			r := NewLogRecorder()
			for i := 0; i < 4; i++ {
				r.RealTimeWrite(functionLog)
			}
			convey.So(r.logs.logs.Len(), convey.ShouldEqual, 4)
		})

		convey.Convey("big log", func() {
			p := gomonkey.NewPatches()
			p.ApplyFunc(NewFixSizeRecorder, func(limitSize int,
				guessSize func(funcLog *FunctionLog) int,
				dropped func(funcLog *FunctionLog, logTooLarge bool)) *FixSizeRecorder {
				return &FixSizeRecorder{
					nil,
					func(funcLog *FunctionLog) int {
						return 12 * 1024
					},
					func(funcLog *FunctionLog, logTooLarge bool) {
						return
					},
					0,
					10 * 1024}
			})
			defer p.Reset()
			r := NewLogRecorder()
			largeLog := &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString(strings.Repeat("a", 12*1024)),
			}
			r.RealTimeWrite(largeLog)
			convey.So(r.logs.logs.Len(), convey.ShouldEqual, 1)
		})

		convey.Convey("Exceeds the limits.", func() {
			p := gomonkey.NewPatches()
			p.ApplyFunc(NewFixSizeRecorder, func(limitSize int,
				guessSize func(funcLog *FunctionLog) int,
				dropped func(funcLog *FunctionLog, logTooLarge bool)) *FixSizeRecorder {
				return &FixSizeRecorder{
					nil,
					func(funcLog *FunctionLog) int {
						return 2 * 1024
					},
					func(funcLog *FunctionLog, logTooLarge bool) {
						return
					},
					0,
					10 * 1024}
			})
			defer p.Reset()
			r := NewLogRecorder()
			largeLog := &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString(strings.Repeat("a", 1*1024)),
			}
			for i := 0; i < 10; i++ {
				r.RealTimeWrite(largeLog)
			}
			convey.So(r.logs.logs.Len(), convey.ShouldEqual, 6)
		})
		convey.Convey("extreme big log", func() {
			p := gomonkey.NewPatches()
			p.ApplyFunc(NewFixSizeRecorder, func(limitSize int,
				guessSize func(funcLog *FunctionLog) int,
				dropped func(funcLog *FunctionLog, logTooLarge bool)) *FixSizeRecorder {
				return &FixSizeRecorder{
					nil,
					func(funcLog *FunctionLog) int {
						return 12 * 1024
					},
					func(funcLog *FunctionLog, logTooLarge bool) {
						return
					},
					0,
					10 * 1024}
			})
			defer p.Reset()
			r := NewLogRecorder()
			largeALog := &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString(strings.Repeat("a", 12*1024)),
			}
			largeBLog := &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString(strings.Repeat("b", 12*1024)),
			}
			largeCLog := &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString(strings.Repeat("c", 12*1024)),
			}
			largeDLog := &FunctionLog{
				Level:   "INFO",
				Message: bytes.NewBufferString(strings.Repeat("d", 12*1024)),
			}
			r.RealTimeWrite(largeALog)
			r.RealTimeWrite(largeBLog)
			r.RealTimeWrite(largeCLog)
			r.RealTimeWrite(largeDLog)
			convey.So(r.logs.logs.Len(), convey.ShouldEqual, 2)
		})
	})
}

type fakeSummary struct {
}

func (f *fakeSummary) GenerateLogHeader(funcLog *FunctionLog) {
	return
}
func (f *fakeSummary) GenerateLogFooter(funcLog *FunctionLog) {
	return
}

func TestLogRecorder_SetHeaderAndFooter(t *testing.T) {
	convey.Convey("TestLogRecorder_SetHeaderAndFooter", t, func() {
		convey.Convey("set header and footer", func() {
			summary := &fakeSummary{}
			r := &LogRecorder{
				f:                      &FunctionLogger{},
				separatedWithLineBreak: true,
				generator:              summary,
			}
			header := &FunctionLog{
				TraceID: "start-header",
			}
			footer := &FunctionLog{
				TraceID: "start-footer",
			}
			r.SetHeader(header)
			r.SetFooter(footer, summary)
			curHeader := r.getHeader()
			curFooter := r.getFooter()
			convey.So(curHeader.TraceID, convey.ShouldEqual, header.TraceID)
			convey.So(curFooter.TraceID, convey.ShouldEqual, footer.TraceID)
		})
		convey.Convey("set header and footer, enableLogPath is true", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.EnableLogPath, func() bool {
					return true
				}),
			})
			defer patches.ResetAll()
			summary := &fakeSummary{}
			r := &LogRecorder{
				f:                      &FunctionLogger{},
				separatedWithLineBreak: true,
				generator:              summary,
			}
			header := &FunctionLog{
				TraceID: "start-header",
			}
			footer := &FunctionLog{
				TraceID: "start-footer",
			}
			r.SetHeader(header)
			r.SetFooter(footer, summary)
			curHeader := r.getHeader()
			curFooter := r.getFooter()
			convey.So(curHeader.TraceID, convey.ShouldEqual, header.TraceID)
			convey.So(curFooter.TraceID, convey.ShouldEqual, footer.TraceID)
		})
	})
}

func TestLogRecorder_ClearSync(t *testing.T) {
	r := &LogRecorder{}
	r.StartSync()
	assert.NotNil(t, r.syncCh)
	r.ClearSync()
	assert.Nil(t, r.syncCh)
}