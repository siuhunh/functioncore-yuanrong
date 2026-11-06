/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io/fs"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/logger"
	loggerConfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/utils"
)

func TestGetFunctionLogger(t *testing.T) {
	_, err := GetFunctionLogger(&config.Configuration{StartArgs: config.StartArgs{
		LogDir:   "/home/sn/logs",
		IsLoaded: false,
	}})
	assert.Nil(t, err)
}

func TestRefreshFileModTime(t *testing.T) {
	convey.Convey("Test RefreshFileModTime", t, func() {
		convey.Convey("Test 1", func() {
			f := &FunctionLogger{
				logAbsPath: "patch",
			}
			ch := make(chan struct{})
			close(ch)
			f.RefreshFileModTime(ch)

			p := gomonkey.NewPatches()
			p.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				ch := make(chan time.Time, 1)
				ch <- time.Time{}
				return &time.Ticker{C: ch}
			})
			defer p.Reset()
			f.RefreshFileModTime(ch)
			f.refreshWithRetry()
		})
	})
	convey.Convey("Test RefreshFileModTime err", t, func() {
		convey.Convey("Test 2", func() {
			var stopCh chan struct{}
			f := &FunctionLogger{}
			f.RefreshFileModTime(stopCh)
		})
	})
}

func TestExtractLogTankService(t *testing.T) {
	l := &logTankService{}
	l.extractLogTankService("groupID;", "streamID;")
	l.extractLogTankService("ID", "ID")
}

func Test_refreshWithRetry(t *testing.T) {
	convey.Convey("Test refreshWithRetry", t, func() {
		convey.Convey("Test 1", func() {
			var testMsg string
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Chtimes, func(name string, atime time.Time, mtime time.Time) error {
					testMsg = "os Chtimes return nil"
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			f := &FunctionLogger{}
			f.refreshWithRetry()
			convey.So(testMsg, convey.ShouldEqual, "os Chtimes return nil")
		})
	})
}

func TestSync(t *testing.T) {
	Sync(nil)
}

func Test_concurrentPrintWithTraceID(t *testing.T) {
	convey.Convey("Test concurrentPrintWithTraceID", t, func() {
		funcLogger, _ := GetFunctionLogger(&config.Configuration{StartArgs: config.StartArgs{
			LogDir:   "/home/sn/logs",
			IsLoaded: false,
		}})
		exec := ""
		p := gomonkey.NewPatches()
		p.ApplyMethod(reflect.TypeOf(funcLogger), "RangeLoggerWithTraceID",
			func(_ *FunctionLogger, funcLog *FunctionLog, traceID string) {
				exec = "range"
				return
			})

		defer p.Reset()
		convey.Convey("Non-standard printing", func() {
			logStr := "test"
			exec = ""
			funcLogger.concurrentPrintWithTraceID(logStr, "2022-12-10 10:15:12.125", "2022-12-10 10:15:12.125")
			convey.So(exec, convey.ShouldEqual, "")
		})

		convey.Convey("Non-standard timestamp printing", func() {
			logStr := "2022-12-10T10:15:12 1d5e992b-28f3-4c13-b268-e6a975550e9e log-test"
			exec = ""
			funcLogger.concurrentPrintWithTraceID(logStr, "2022-12-10 10:15:12.125", "2022-12-10 10:15:12.125")
			convey.So(exec, convey.ShouldEqual, "")
		})

		convey.Convey("standard printing 1", func() {
			logStr := "2022-12-10 10:15:12.125 1d5e992b-28f3-4c13-b268-e6a975550e9e log-test"
			exec = ""
			funcLogger.concurrentPrintWithTraceID(logStr, "2022-12-10 10:15:12.125", "2022-12-10 10:15:12.125")
			convey.So(exec, convey.ShouldEqual, "range")
		})

		convey.Convey("standard printing 2", func() {
			logStr := "2022-12-10 10:15:12 1d5e992b-28f3-4c13-b268-e6a975550e9e log-test"
			exec = ""
			funcLogger.concurrentPrintWithTraceID(logStr, "2022-12-10 10:15:12.125", "2022-12-10 10:15:12.125")
			convey.So(exec, convey.ShouldEqual, "range")
		})
	})
}

func Test_GetLogRecorderFromInvokeID(t *testing.T) {
	convey.Convey("Test GetLogRecorderFromInvokeID", t, func() {
		convey.Convey("The width exceeds the value of uint64.", func() {
			f := &FunctionLogger{
				logAbsPath: "patch",
			}
			value := "test"
			LogRecorder, flag := f.GetLogRecorderFromInvokeID([]byte(value))
			convey.So(LogRecorder, convey.ShouldBeNil)
			convey.So(flag, convey.ShouldBeFalse)
		})

		convey.Convey("failed to get invoke ctx", func() {
			f := &FunctionLogger{
				logAbsPath: "patch",
			}
			value := bytes.NewBufferString(strings.Repeat("a", 1024))
			LogRecorder, flag := f.GetLogRecorderFromInvokeID(value.Bytes())
			convey.So(LogRecorder, convey.ShouldBeNil)
			convey.So(flag, convey.ShouldBeFalse)
		})
	})
}

type fakeFile struct{}

func (fakeFile) Name() string       { return "" }
func (fakeFile) Size() int64        { return 100 }
func (fakeFile) Mode() fs.FileMode  { return 0755 }
func (fakeFile) ModTime() time.Time { return time.Time{} }
func (fakeFile) IsDir() bool        { return false }
func (fakeFile) Sys() interface{}   { return nil }

func Test_newLTSFunctionLogger(t *testing.T) {
	convey.Convey("Test newLTSFunctionLogger", t, func() {
		convey.Convey("get logNameTemplate", func() {
			cfg := &config.Configuration{
				StartArgs: config.StartArgs{
					LogDir: "/home/sn/log/user/12345678901234561234567890123456/",
				},
			}
			logConfigs := loggerConfig.CoreInfo{
				FilePath: "/home/sn/log",
				Level:    "INFO",
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(utils.LocalFuncURN, urnutils.BaseURN{
					TenantID: "12345678901234561234567890123456",
					Name:     "0-absserviceid-absFunc-Name",
				}),
				gomonkey.ApplyFunc(loggerConfig.GetCoreInfoFromEnv,
					func() (loggerConfig.CoreInfo, error) {
						return logConfigs, nil
					}),
				gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
					return &os.File{}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&os.File{}), "Stat", func(*os.File) (os.FileInfo, error) {
					return fakeFile{}, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			funcLogger, err := newLTSFunctionLogger(cfg)
			convey.So(err, convey.ShouldBeNil)
			convey.So(funcLogger, convey.ShouldNotBeNil)
			log.GetLogger().Infof("logAbsPath: %s", funcLogger.logAbsPath)
			log.GetLogger().Infof("logNameTemplate: %s", funcLogger.logNameTemplate)
			path := logger.GetLogName(funcLogger.logNameTemplate)
			convey.So(len(path), convey.ShouldNotEqual, 0)
			log.GetLogger().Infof("path: %s", path)
		})
		convey.Convey("test lts custom tag", func() {
			isSetCustomTagSuccess := false
			cfg := &config.Configuration{
				StartArgs: config.StartArgs{
					LogDir: "/home/sn/log/user/12345678901234561234567890123456/",
				},
				RuntimeConfig: config.RuntimeConfig{
					LtsCustomTag: map[string]string{"ltsKey": "ltsValue"},
				},
			}
			logConfigs := loggerConfig.CoreInfo{
				FilePath: "/home/sn/log",
				Level:    "INFO",
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(utils.LocalFuncURN, urnutils.BaseURN{
					TenantID: "12345678901234561234567890123456",
					Name:     "0-absserviceid-absFunc-Name",
				}),
				gomonkey.ApplyFunc(loggerConfig.GetCoreInfoFromEnv,
					func() (loggerConfig.CoreInfo, error) {
						return logConfigs, nil
					}),
				gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
					return &os.File{}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&os.File{}), "Stat", func(*os.File) (os.FileInfo, error) {
					return fakeFile{}, nil
				}),
				gomonkey.ApplyFunc(commonUtils.ValidateFilePath, func(path string) error {
					return nil
				}),
				gomonkey.ApplyFunc((*zap.Logger).With, func(logger *zap.Logger, fields ...zap.Field) *zap.Logger {
					for _, field := range fields {
						if field.Type == zapcore.ObjectMarshalerType {
							isSetCustomTagSuccess = true
						}
					}
					return logger
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			_, _ = newLTSFunctionLogger(cfg)
			convey.So(isSetCustomTagSuccess, convey.ShouldBeTrue)
		})
	})
}

func Test_FuncLogger(t *testing.T) {
	convey.Convey("Test FuncLogger", t, func() {
		convey.Convey("AcquireLog and ReleaseLog", func() {
			funcLogger := NewFunctionLogger(nil, "", "")
			log := funcLogger.AcquireLog()
			log.TraceID = "test1"
			tmpLog := log
			funcLogger.ReleaseLog(log)
			newLog := funcLogger.AcquireLog()
			newLog.TraceID = "test2"
			convey.So(tmpLog.TraceID, convey.ShouldEqual, "test2")
		})

		convey.Convey("AcquireLog and ReleaseLog copy log", func() {
			funcLogger := NewFunctionLogger(nil, "", "")
			log := funcLogger.AcquireLog()
			log.TraceID = "test1"
			traceID := log.TraceID
			funcLogger.ReleaseLog(log)
			newLog := funcLogger.AcquireLog()
			newLog.TraceID = "test2"
			convey.So(traceID, convey.ShouldEqual, "test1")
		})
	})
}
