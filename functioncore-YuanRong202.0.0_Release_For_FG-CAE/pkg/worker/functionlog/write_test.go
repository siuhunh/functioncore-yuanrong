/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/worker"
	"functioncore/test/utils"
)

func Test_getLTSLoggerMessage(t *testing.T) {
	convey.Convey("Test_getLTSLoggerMessage", t, func() {
		convey.Convey("log is StdLog", func() {
			funcLog := internalFunctionLog{
				FunctionLog: &FunctionLog{
					IsStdLog: true,
					Message:  bytes.NewBuffer([]byte("test1")),
				},
				FunctionName: "test",
				TraceID:      "trace_id",
				LivedataID:   "livedata_id",
				Stage:        "invoke",
				Index:        0,
			}
			data := getLTSLoggerMessage(&funcLog)
			assert.Equal(t, data, "test1")
		})
		convey.Convey("log is not StdLog, Index > 0", func() {
			funcLog := internalFunctionLog{
				FunctionLog: &FunctionLog{
					IsStdLog: false,
					Level:    worker.FuncLogLevelInfo,
					Message:  bytes.NewBuffer([]byte("test2")),
				},
				FunctionName: "test",
				TraceID:      "trace_id",
				LivedataID:   "livedata_id",
				Stage:        "invoke",
				Index:        2,
			}
			data := getLTSLoggerMessage(&funcLog)
			if !strings.Contains(data, worker.FuncLogLevelInfo) {
				t.Errorf("'%s' does not contain '%s'", data, worker.FuncLogLevelInfo)
			}
		})
		convey.Convey("log is not StdLog, Index = -2", func() {
			funcLog := internalFunctionLog{
				FunctionLog: &FunctionLog{
					IsStdLog: false,
					Level:    worker.FuncLogLevelInfo,
					Message:  bytes.NewBuffer([]byte("test2")),
				},
				FunctionName: "test",
				TraceID:      "trace_id",
				LivedataID:   "livedata_id",
				Stage:        "invoke",
				Index:        -2,
			}
			data := getLTSLoggerMessage(&funcLog)
			if strings.Contains(data, worker.FuncLogLevelInfo) {
				t.Errorf("'%s' does not contain '%s'", data, worker.FuncLogLevelInfo)
			}
		})
	})
}

func Test_writeWithFNil(t *testing.T) {
	convey.Convey("test write with f is nil", t, func() {
		convey.Convey("case1: f is nil", func() {
			callCount := 0
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(getLTSLoggerMessage, func(_ *internalFunctionLog) string {
					callCount++
					return ""
				}),
			})
			defer patches.ResetAll()
			var f *FunctionLogger
			f.write(nil)
			convey.So(callCount, convey.ShouldEqual, 0)
		})
	})
}
