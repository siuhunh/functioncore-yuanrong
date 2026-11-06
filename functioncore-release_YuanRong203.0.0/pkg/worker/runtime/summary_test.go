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

package runtime

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/utils"
)

func TestNewSummary(t *testing.T) {
	testCases := []struct {
		name          string
		cfg           *config.Configuration
		recorder      *functionlog.LogRecorder
		expectSummary *Summary
	}{
		{
			name: "CodeSha256 length greater than 32",
			cfg: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					CodeSha256: "123456789012345678901234567890123",
					CodeSha512: "123456789012345678901234567890123",
					ResourceMetaData: config.ResourceMetaData{
						Mem: 100,
					},
				},
			},
			recorder: functionlog.NewLogRecorder(),
			expectSummary: &Summary{
				SummaryInfo: snerror.SummaryInfo{
					MemorySize: 100,
					FuncDigest: "1234567890123456789012345678901",
				},
			},
		},
		{
			name: "CodeSha256 length less than 32",
			cfg: &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{
					CodeSha256: "123456789012345678901234567890",
					CodeSha512: "123456789012345678901234567890",
					ResourceMetaData: config.ResourceMetaData{
						Mem: 100,
					},
				},
			},
			recorder: functionlog.NewLogRecorder(),
			expectSummary: &Summary{
				SummaryInfo: snerror.SummaryInfo{
					MemorySize: 100,
					FuncDigest: "123456789012345678901234567890",
				},
			},
		},
	}

	for _, tt := range testCases {
		convey.Convey(tt.name, t, func() {
			actualSummary := NewSummarySpan("", "", tt.cfg, time.Duration(0))
			convey.So(actualSummary.SummaryInfo, convey.ShouldResemble, tt.expectSummary.SummaryInfo)
		})
	}
}

func TestGenerateLogHeader(t *testing.T) {
	convey.Convey("TestGenerateLogHeader", t, func() {
		cfg := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				CodeSha256: "123456789012345678901234567890123",
				CodeSha512: "123456789012345678901234567890123",
				ResourceMetaData: config.ResourceMetaData{
					Mem: 100,
				},
			},
		}

		i := NewSummarySpan(constants.InvokeStage, "1234", cfg, time.Duration(0))
		timeStr := "2025-02-27 12:00:00.123"
		var err error
		i.startTime, err = time.Parse(functionlog.FuncLogTimeFormat, timeStr)
		if err != nil {
			assert.Error(t, err, "failed to Parse time")
			return
		}

		actualLogHeader := functionlog.NewFunctionLog()
		i.GenerateLogHeader(actualLogHeader)
		expectLogHeader := &functionlog.FunctionLog{
			NanoTime: i.startTime.Format(functionlog.NanoLogLayout),
			Time:     i.startTime.Format(functionlog.LogTimeFormat),
			Level:    "INFO",
			Message: bytes.NewBufferString(fmt.Sprintf("%s Start %s request '%s', version: %s",
				i.startTime.Format(functionlog.LogTimeFormat), i.stage, i.traceID,
				utils.LocalFuncURN.Version)),
		}

		convey.So(actualLogHeader, convey.ShouldResemble, expectLogHeader)
		convey.So(actualLogHeader.Time, convey.ShouldEqual, "2025-02-27 12:00:00.123Z")
	})
}

func TestSummary_Finish(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do",
		func(_ *sync.Once, f func()) {
			f()
		})
	patch.ApplyGlobalVar(&PodApplier, "")
	defer func() {
		patch.Reset()
	}()
	s := &Summary{
		timeout: 3 * time.Second,
		stage:   constants.InvokeStage,
	}
	testCases := []struct {
		name                              string
		subTime                           time.Duration
		initializerDuration               time.Duration
		startupDuration                   time.Duration
		summary                           *Summary
		expectDuration                    float64
		expectBillingDuration             int
		expectFinishInvokeBillingDuration int
	}{
		{
			name:                              "invoke timeout",
			subTime:                           4 * time.Second,
			summary:                           s,
			expectDuration:                    3000,
			expectBillingDuration:             3000,
			expectFinishInvokeBillingDuration: 3000,
		},
		{
			name:                              "invoke with initializerDuration",
			subTime:                           11 * time.Millisecond,
			initializerDuration:               31 * time.Millisecond,
			summary:                           s,
			expectDuration:                    42.000,
			expectBillingDuration:             42,
			expectFinishInvokeBillingDuration: 11,
		},
		{
			name:                              "invoke with startupDuration",
			subTime:                           10 * time.Millisecond,
			startupDuration:                   40 * time.Millisecond,
			summary:                           s,
			expectDuration:                    50.000,
			expectBillingDuration:             50,
			expectFinishInvokeBillingDuration: 10,
		},
		{
			name:                              "invoke timeout with startupDuration",
			subTime:                           11 * time.Millisecond,
			startupDuration:                   2990 * time.Millisecond,
			summary:                           s,
			expectDuration:                    3000.000,
			expectBillingDuration:             3000,
			expectFinishInvokeBillingDuration: 11,
		},
		{
			name:                              "invoke with startupDuration And initializerDuration",
			subTime:                           10 * time.Millisecond,
			startupDuration:                   40 * time.Millisecond,
			initializerDuration:               30 * time.Millisecond,
			summary:                           s,
			expectDuration:                    80.000,
			expectBillingDuration:             80,
			expectFinishInvokeBillingDuration: 10,
		},
	}
	for _, tt := range testCases {
		convey.Convey(tt.name, t, func() {
			ExtensionDuration = 0
			RestoreDuration = 0
			defer func() {
				InitializerDuration = 0
				StartupDuration = 0
			}()
			s.startTime = time.Now().Add(-tt.subTime)
			s.endTime = s.startTime.Add(s.timeout)
			if tt.initializerDuration > 0 {
				InitializerDuration = tt.initializerDuration
			}
			if tt.startupDuration > 0 {
				StartupDuration = tt.startupDuration
			}
			tt.summary.Finish(nil)
			convey.So(tt.summary.SummaryInfo.Duration, convey.ShouldBeBetweenOrEqual, tt.expectDuration, tt.expectDuration+0.1)
			convey.So(tt.summary.SummaryInfo.BillingDuration, convey.ShouldEqual, tt.expectBillingDuration)
			billingDurationLog := fmt.Sprintf("billing duration: %dms", tt.expectFinishInvokeBillingDuration)
			if tt.initializerDuration > 0 {
				// InitializerDuration is not included in invoke LoggerFooter.
				billingDurationLog = fmt.Sprintf("billing duration: %dms", tt.subTime.Milliseconds())
			}
			functionLog := functionlog.NewFunctionLog()
			tt.summary.GenerateLogFooter(functionLog)
			convey.So(functionLog.Message.String(), convey.ShouldContainSubstring, billingDurationLog)
		})
	}
}

func TestSummary_FinishError(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do",
		func(_ *sync.Once, f func()) {
			f()
		})
	patch.ApplyGlobalVar(&PodApplier, "")
	defer func() {
		patch.Reset()
	}()
	s := &Summary{
		timeout: 3 * time.Second,
		stage:   constants.InvokeStage,
	}
	s.Finish(&Error{
		Code:    snerror.RuntimeMemoryExceedLimit,
		Message: snerror.ErrText(snerror.RuntimeMemoryExceedLimit),
	})
	logs := functionlog.NewFunctionLog()
	s.GenerateLogFooter(logs)
	assert.NotEmpty(t, logs.ErrorType)
}

func Test_generateLoggerFooter(t *testing.T) {
	_ = os.Setenv("PodName", "pool6-4000-16000-java1.8-67f66bbdbc-l7m2h")
	endTime := time.Now().UTC()
	duration := 10.01
	billingDuration := 100
	memoryUsed := 100.1
	cpuUsed := 0.123
	billingMemory := 16000
	storageUsed := 1.23
	traceID := "123"
	s := &Summary{
		endTime: endTime,
		SummaryInfo: snerror.SummaryInfo{
			MemoryUsed:  memoryUsed,
			CPUUsed:     cpuUsed,
			StorageUsed: storageUsed,
		},
		footerDuration:        duration,
		footerBillingDuration: billingDuration,
		traceID:               traceID,
	}

	testCases := []struct {
		name            string
		summary         *Summary
		error           *Error
		stage           string
		expectLogFooter *functionlog.FunctionLog
		hcsMode         bool
	}{
		{
			name:    "invoke success",
			summary: s,
			stage:   constants.InvokeStage,
			error:   nil,
			expectLogFooter: &functionlog.FunctionLog{
				NanoTime:      endTime.Format(functionlog.NanoLogLayout),
				Time:          endTime.Format(functionlog.FuncLogTimeFormat),
				IsFinishedLog: true,
				Level:         "INFO",
				Message: bytes.NewBufferString(fmt.Sprintf("%s Finish %s request '%s', duration: %.3fms, "+
					"billing duration: %dms, memory used: %.3fMB, billing memory: %dMB, cpu used: %.3fU, storage used: %.3fMB",
					endTime.Format(functionlog.FuncLogTimeFormat),
					constants.InvokeStage,
					traceID,
					duration,
					billingDuration,
					memoryUsed,
					billingMemory,
					cpuUsed,
					storageUsed)),
				ErrorType:     "",
				ReportSummary: true,
				CPUUsed:       cpuUsed,
				MemoryUsed:    memoryUsed,
				Duration:      duration,
				StorageUsed:   storageUsed,
			},
		},
		{
			name:    "invoke success by hcs",
			summary: s,
			stage:   constants.InvokeStage,
			error:   nil,
			expectLogFooter: &functionlog.FunctionLog{
				NanoTime:      endTime.Format(functionlog.NanoLogLayout),
				Time:          endTime.Format(functionlog.FuncLogTimeFormat),
				IsFinishedLog: true,
				Level:         "INFO",
				Message: bytes.NewBufferString(fmt.Sprintf("%s Finish %s request '%s', duration: %.3fms, "+
					"memory used: %.3fMB, cpu used: %.3fU, storage used: %.3fMB",
					endTime.Format(functionlog.FuncLogTimeFormat),
					constants.InvokeStage,
					traceID,
					duration,
					memoryUsed,
					cpuUsed,
					storageUsed)),
				ErrorType:     "",
				ReportSummary: true,
				CPUUsed:       cpuUsed,
				MemoryUsed:    memoryUsed,
				Duration:      duration,
				StorageUsed:   storageUsed,
			},
			hcsMode: true,
		},
		{
			name:    "init success",
			summary: s,
			stage:   constants.InitializeStage,
			error:   nil,
			expectLogFooter: &functionlog.FunctionLog{
				NanoTime:      endTime.Format(functionlog.NanoLogLayout),
				Time:          endTime.Format(functionlog.FuncLogTimeFormat),
				IsFinishedLog: true,
				Level:         "INFO",
				Message: bytes.NewBufferString(fmt.Sprintf("%s Finish %s request '%s', duration: %.3fms, "+
					"memory used: %.3fMB, storage used: %.3fMB.",
					endTime.Format(functionlog.FuncLogTimeFormat),
					constants.InitializeStage,
					traceID,
					duration,
					memoryUsed,
					storageUsed)),
				ErrorType:     "",
				ReportSummary: true,
				CPUUsed:       cpuUsed,
				MemoryUsed:    memoryUsed,
				Duration:      duration,
				StorageUsed:   storageUsed,
			},
		},
		{
			name:    "invoke timeout",
			summary: s,
			stage:   constants.InvokeStage,
			error: &Error{
				Code: snerror.RuntimeInvokeTimeoutCode,
			},
			expectLogFooter: &functionlog.FunctionLog{
				NanoTime:      endTime.Format(functionlog.NanoLogLayout),
				Time:          endTime.Format(functionlog.FuncLogTimeFormat),
				IsFinishedLog: true,
				Level:         "ERROR",
				Message: bytes.NewBufferString(fmt.Sprintf("%s Finish %s request '%s'(%s), duration: %.3fms, "+
					"billing duration: %dms, memory used: %.3fMB, billing memory: %dMB, cpu used: %.3fU, storage used: %.3fMB",
					endTime.Format(functionlog.FuncLogTimeFormat),
					constants.InvokeStage,
					traceID,
					"invoke Timeout",
					duration,
					billingDuration,
					memoryUsed,
					billingMemory,
					cpuUsed,
					storageUsed)),
				ErrorType:     "TimeOut",
				ReportSummary: true,
				CPUUsed:       cpuUsed,
				MemoryUsed:    memoryUsed,
				Duration:      duration,
				StorageUsed:   storageUsed,
			},
		},
	}
	for _, tt := range testCases {
		convey.Convey(tt.name, t, func() {
			hcsMode = tt.hcsMode
			tt.summary.stage = tt.stage
			tt.summary.err = tt.error
			actualLoggerFooter := functionlog.NewFunctionLog()
			tt.summary.GenerateLogFooter(actualLoggerFooter)
			convey.So(actualLoggerFooter, convey.ShouldResemble, tt.expectLogFooter)
			hcsMode = false
		})
	}
}

func Test_getErrorInfo(t *testing.T) {
	err1 := &Error{
		Code:    snerror.RuntimeLoadTimeoutCode,
		Message: "aaa",
	}
	err2 := &Error{
		Code:    snerror.RuntimeProcessExit,
		Message: "aaa",
	}
	err3 := &Error{
		Code:    snerror.DiskUsageExceed,
		Message: "aaa",
	}
	err4 := &Error{
		Code:    snerror.RequestCanceled,
		Message: "aaa",
	}
	err5 := &Error{
		Code:    snerror.RuntimeUnHealthy,
		Message: "aaa",
	}
	err6 := &Error{
		Code:    snerror.RuntimeMemoryExceedLimit,
		Message: "aaa",
	}
	err7 := &Error{}

	type args struct {
		err   *Error
		stage string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case1",
			args: args{
				err:   err1,
				stage: "aaa",
			},
			want: fmt.Sprintf("%s Timeout", "aaa"),
		},
		{
			name: "case2",
			args: args{
				err:   err2,
				stage: "aaa",
			},
			want: fmt.Sprintf("%s Failed:RuntimeProcessExited", "aaa"),
		},
		{
			name: "case3",
			args: args{
				err:   err3,
				stage: "aaa",
			},
			want: fmt.Sprintf("%s Failed:DiskUsageExceedLimit", "aaa"),
		},
		{
			name: "case4",
			args: args{
				err:   err4,
				stage: "aaa",
			},
			want: fmt.Sprintf("%s Failed:RequestCanceled", "aaa"),
		},
		{
			name: "case5",
			args: args{
				err:   err5,
				stage: "aaa",
			},
			want: fmt.Sprintf("%s Failed:RuntimeUnHealthy", "aaa"),
		},
		{
			name: "case6",
			args: args{
				err:   err6,
				stage: "aaa",
			},
			want: fmt.Sprintf("%s Failed:RuntimeMemoryExceedLimit", "aaa"),
		},
		{
			name: "case7",
			args: args{
				err:   err7,
				stage: "aaa",
			},
			want: fmt.Sprintf("%s Failed", "aaa"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getErrorInfo(tt.args.err, tt.args.stage); got != tt.want {
				t.Errorf("getErrorInfo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getErrorType(t *testing.T) {
	err1 := &Error{
		Code:    snerror.RuntimeLoadTimeoutCode,
		Message: "aaa",
	}
	err2 := &Error{
		Code:    snerror.DiskUsageExceed,
		Message: "aaa",
	}
	err3 := &Error{
		Code:    snerror.RequestCanceled,
		Message: "aaa",
	}
	err4 := &Error{
		Code:    snerror.RuntimeUnHealthy,
		Message: "aaa",
	}
	err5 := &Error{
		Code:    snerror.RuntimeMemoryExceedLimit,
		Message: "aaa",
	}
	err6 := &Error{
		Code:    snerror.RuntimeProcessExit,
		Message: "aaa",
	}
	type args struct {
		err *Error
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "case1",
			args: args{
				err: err1,
			},
			want: "TimeOut",
		},
		{
			name: "case2",
			args: args{
				err: err2,
			},
			want: "DiskTmpUsageOutOfRange",
		},
		{
			name: "case3",
			args: args{
				err: err3,
			},
			want: "RequestCanceled",
		},
		{
			name: "case4",
			args: args{
				err: err4,
			},
			want: "RuntimeUnHealthy",
		},
		{
			name: "case5",
			args: args{
				err: err5,
			},
			want: "MemoryExceedLimit",
		},
		{
			name: "case6",
			args: args{
				err: err6,
			},
			want: "RunningError",
		},
		{
			name: "case7",
			args: args{
				err: nil,
			},
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getErrorType(tt.args.err); got != tt.want {
				t.Errorf("getErrorType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_CalculateBillingDuration(t *testing.T) {
	type args struct {
		duration time.Duration
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			name: "case1",
			args: args{
				duration: 1000001 * time.Nanosecond,
			},
			want: time.Duration(1) * meteringGranularity * time.Microsecond,
		},
		{
			name: "case2",
			args: args{
				duration: 1001001 * time.Nanosecond,
			},
			want: time.Duration(2) * meteringGranularity * time.Microsecond,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CalculateBillingDuration(tt.args.duration); got != tt.want {
				t.Errorf("calculateBillingDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isFirstLoadAndInitOrInvokeFinish(t *testing.T) {
	convey.Convey("test isFirstLoadAndInitOrInvokeFinish", t, func() {
		cfg := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				CodeSha256: "123456789012345678901234567890123",
				CodeSha512: "123456789012345678901234567890123",
				ResourceMetaData: config.ResourceMetaData{
					Mem: 100,
				},
			},
		}

		i := NewSummarySpan(constants.InvokeStage, "1234", cfg, time.Duration(0))
		i.startTime = time.Now().UTC()
		flag := i.isFirstLoadAndInitOrInvokeFinish()
		convey.So(flag, convey.ShouldBeTrue)

		i = NewSummarySpan(constants.InitializeStage, "1234", cfg, time.Duration(0))
		i.startTime = time.Now().UTC()
		flag = i.isFirstLoadAndInitOrInvokeFinish()
		convey.So(flag, convey.ShouldBeTrue)

		i = NewSummarySpan(constants.LoadStage, "1234", cfg, time.Duration(0))
		i.startTime = time.Now().UTC()
		flag = i.isFirstLoadAndInitOrInvokeFinish()
		convey.So(flag, convey.ShouldBeTrue)

		i = NewSummarySpan(constants.LoadStage, "1234", cfg, time.Duration(0))
		i.startTime = time.Now().UTC()
		flag = i.isFirstLoadAndInitOrInvokeFinish()
		convey.So(flag, convey.ShouldBeFalse)

		atomic.StoreUint32(&extensionFlag, 0)
		i = NewSummarySpan(constants.ExtensionStage, "1234", cfg, time.Duration(0))
		i.startTime = time.Now().UTC()
		flag = i.isFirstLoadAndInitOrInvokeFinish()
		convey.So(flag, convey.ShouldBeTrue)
	})
}

func Test_GetTraceID(t *testing.T) {
	summary := &Summary{traceID: "traceID"}
	t.Run("test get traceID", func(t *testing.T) {
		if got := summary.GetTraceID(); got != "traceID" {
			t.Errorf("GetTraceID() = %v, want %v", got, "traceID")
		}
	})
}

func Test_GenerateSummaryInfo(t *testing.T) {
	convey.Convey("test GenerateSummaryInfo", t, func() {
		cfg := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				CodeSha256: "123456789012345678901234567890123",
				CodeSha512: "123456789012345678901234567890123",
				ResourceMetaData: config.ResourceMetaData{
					Mem: 100,
				},
			},
		}
		summaryInfo := GenerateSummaryInfo(cfg, 1, 1, nil)
		convey.So(summaryInfo.MemorySize, convey.ShouldEqual, 100)
	})
}

func Test_SummaryInfo(t *testing.T) {
	convey.Convey("test GenerateSummaryInfo", t, func() {
		RecodeSummary("test1", &Summary{
			traceID: "test1",
		})
		RecodeSummary("test2", &Summary{
			traceID: "test2",
		})
		DeleteSummary("test1")
		summaryInfo := GetSummary("test2")
		convey.So(summaryInfo.traceID, convey.ShouldEqual, "test2")
	})
}
