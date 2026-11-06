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
	"bufio"
	"bytes"
	"container/list"
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/functionbus/bus/tcpbustool"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/worker/config"
	logger "functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	workerutils "functioncore/pkg/worker/utils"
	"functioncore/test/utils"
)

func TestRuntimeMetricsNoRequesting(t *testing.T) {
	testMetrics := Metrics{
		tracer: newFutureTracer(),
	}
	testMetrics.IdleCheckSuicide()
	memoryUsed := 123.45
	testMetrics.UpdateMemoryUsed(memoryUsed)
	if testMetrics.GetMemoryUsed(nil) != memoryUsed {
		t.Errorf("update memory-used when no requesting failed")
	}

	testMetrics.IncreaseRequestCount("0")
	testMetrics.DecreaseRequestCount("0")
	if testMetrics.lastMemoryUsed != memoryUsed {
		t.Errorf("update and reset memory-used when no requesting failed")
	}
	if testMetrics.GetMemoryUsed(nil) != memoryUsed {
		t.Errorf("update and reset memory-used when no requesting failed")
	}
	err := snerror.New(4205, "oom test")
	if testMetrics.GetMemoryUsed(err) != memoryUsed {
		t.Errorf("update and reset memory-used when oom happened failed")
	}
}

func TestRuntimeMetricsOOM(t *testing.T) {
	convey.Convey("GetMemoryUsed case: OOM", t, func() {

		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc((*atomic.Value).Load, func(_ *atomic.Value) (res any) {
				return float64(1)
			}),
			gomonkey.ApplyFunc(snerror.IsOOMErrorCode, func(_ error) bool {
				return true
			}),
		})
		defer patches.ResetAll()
		testMetrics := Metrics{
			tracer:        newFutureTracer(),
			oomMemoryUsed: atomic.Value{},
			memoryUsed:    atomic.Value{},
		}
		testMetrics.IdleCheckSuicide()
		memoryUsed := float64(1)
		testMetrics.UpdateMemoryUsed(memoryUsed)
		if testMetrics.GetMemoryUsed(nil) != memoryUsed {
			t.Errorf("update memory-used when no requesting failed")
		}
	})
}

func TestRuntimeMetricsRequesting(t *testing.T) {
	testMetrics := Metrics{
		tracer: newFutureTracer(),
	}
	testMetrics.isFunctionLoaded = true
	memoryUsed := 123.45
	testMetrics.IncreaseRequestCount("1")
	testMetrics.UpdateMemoryUsed(memoryUsed)
	if testMetrics.GetMemoryUsed(nil) != memoryUsed {
		t.Errorf("update memory-used when requesting failed")
	}

	// update higher memory-used, expect to cover lower one
	memoryUsedHigher := 200.00
	testMetrics.UpdateMemoryUsed(memoryUsedHigher)
	if testMetrics.GetMemoryUsed(nil) != memoryUsedHigher {
		t.Errorf("update higher memory-used when requesting failed, ret: %v, expect: %v",
			testMetrics.GetMemoryUsed(nil), memoryUsedHigher)
	}

	// last request finished, update lower memory-used, export to cover older one
	testMetrics.DecreaseRequestCount("1")
	memoryUsedLower := 100.00
	testMetrics.IncreaseRequestCount("2")
	testMetrics.UpdateMemoryUsed(memoryUsedLower)
	if testMetrics.GetMemoryUsed(nil) != memoryUsedLower {
		t.Errorf("reset and update memory-used failed, ret: %v, expect: %v",
			RunMetrics.GetMemoryUsed(nil), memoryUsedLower)
	}
	testMetrics.DecreaseRequestCount("2")
	memoryUsed = 50.1
	testMetrics.UpdateMemoryUsed(memoryUsed)

	memoryUsed = 100.1
	testMetrics.UpdateMemoryUsed(memoryUsed)

	if testMetrics.GetMemoryUsed(nil) != memoryUsed {
		t.Errorf("failed to update lastMemoryUsed")
	}

	testMetrics.isFunctionLoaded = false
	testMetrics.memoryUsed = atomic.Value{}
	memoryUsed = 11.1
	testMetrics.UpdateMemoryUsed(memoryUsed)
	if testMetrics.GetMemoryUsed(nil) != memoryUsed {
		t.Errorf("failed to update lastMemoryUsed")
	}

}

func BenchmarkRequestCount(b *testing.B) {
	testMetrics := Metrics{
		tracer: newFutureTracer(),
	}
	var idGen uint64
	pool := &sync.Pool{
		New: func() interface{} {
			return make([]byte, 8)
		},
	}
	b.SetParallelism(200)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := atomic.AddUint64(&idGen, 1)
			b := pool.Get().([]byte)
			binary.BigEndian.PutUint64(b, id)
			futureID := *(*string)(unsafe.Pointer(&b))
			testMetrics.IncreaseRequestCount(futureID)
			testMetrics.DecreaseRequestCount(futureID)
			pool.Put(b)
		}
	})
}

func TestMetrics_GetRequestFinishTime(t *testing.T) {
	time1 := time.Now()
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastMemoryUsed    float64
		memoryUsed        atomic.Value
		requestFinishTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{
			name: "case1",
			fields: fields{
				requestFinishTime: time1,
			},
			want: time1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				lastMemoryUsed:    tt.fields.lastMemoryUsed,
				memoryUsed:        tt.fields.memoryUsed,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			if got := m.GetRequestFinishTime(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRequestFinishTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetrics_GetRecoverProperty(t *testing.T) {
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastMemoryUsed    float64
		memoryUsed        atomic.Value
		requestFinishTime time.Time
	}
	tests := []struct {
		name   string
		fields fields
		want   common.RecoverProperty
	}{
		{
			name: "case1",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{
						"aaa": {
							records: list.New(),
							m:       map[string]*list.Element{},
						},
					},
					mux: sync.Mutex{},
				},
			},
			want: common.RecoverProperty{
				Requests: map[string]*common.RecoverTraceRecord{
					"aaa": {
						Records: []string{},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				lastMemoryUsed:    tt.fields.lastMemoryUsed,
				memoryUsed:        tt.fields.memoryUsed,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			if got := m.GetRecoverProperty(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRecoverProperty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetrics_AddTraceRecord(t *testing.T) {
	list1 := &list.List{}
	caller := "aaa"
	callee := "bbb"
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastMemoryUsed    float64
		memoryUsed        atomic.Value
		requestFinishTime time.Time
	}
	type args struct {
		futureID string
		callee   string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
	}{
		{
			name: "case1",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{
						caller: {
							records: &list.List{},
							m:       map[string]*list.Element{},
						},
					},
					mux: sync.Mutex{},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(list1), "PushBack",
						func(l *list.List, v interface{}) *list.Element {
							return &list.Element{}
						}),
				})
				return patches
			},
		},
		{
			name: "case2",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{
						caller: {
							records: &list.List{},
							m: map[string]*list.Element{
								callee: {},
							},
						},
					},
					mux: sync.Mutex{},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				lastMemoryUsed:    tt.fields.lastMemoryUsed,
				memoryUsed:        tt.fields.memoryUsed,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			m.AddTraceRecord(caller, callee)
			assert.NotNil(t, m.tracer.traces[caller].m[callee])
			patches.ResetAll()
		})
	}
}

func TestMetrics_RemoveTraceRecord(t *testing.T) {
	caller := "aaa"
	callee := "bbb"
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastMemoryUsed    float64
		memoryUsed        atomic.Value
		requestFinishTime time.Time
	}
	type args struct {
		futureID string
		callee   string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{
						caller: {
							records: &list.List{},
							m: map[string]*list.Element{
								callee: {},
							},
						},
					},
					mux: sync.Mutex{},
				},
			},
		},
		{
			name: "case2",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{
						caller: {
							records: &list.List{},
							m:       map[string]*list.Element{},
						},
					},
					mux: sync.Mutex{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				lastMemoryUsed:    tt.fields.lastMemoryUsed,
				memoryUsed:        tt.fields.memoryUsed,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			m.RemoveTraceRecord(caller, callee)
			assert.NotNil(t, m.tracer.traces[caller])
		})
	}
}

func TestMetrics_Cancel(t *testing.T) {
	caller := "aaa"
	futureID := "bbb"
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastMemoryUsed    float64
		memoryUsed        atomic.Value
		requestFinishTime time.Time
	}
	type args struct {
		futureID string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
		want1  bool
	}{
		{
			name: "case1",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{
						caller: {
							records: &list.List{},
							m:       map[string]*list.Element{},
						},
					},
					mux: sync.Mutex{},
				},
			},
			args: args{
				futureID: futureID,
			},
			want:  nil,
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				lastMemoryUsed:    tt.fields.lastMemoryUsed,
				memoryUsed:        tt.fields.memoryUsed,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			got, got1 := m.Cancel(tt.args.futureID)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Cancel() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Cancel() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestMetrics_IsCanceled(t *testing.T) {
	caller := "aaa"
	futureID := "bbb"
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastMemoryUsed    float64
		memoryUsed        atomic.Value
		requestFinishTime time.Time
	}
	type args struct {
		futureID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		wantErr bool
	}{
		{
			name: "case1",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{
						caller: {
							records: &list.List{},
							m:       map[string]*list.Element{},
						},
					},
					mux: sync.Mutex{},
				},
			},
			args: args{
				futureID: futureID,
			},
			want:    false,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				lastMemoryUsed:    tt.fields.lastMemoryUsed,
				memoryUsed:        tt.fields.memoryUsed,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			got, err := m.IsCanceled(tt.args.futureID)
			if (err != nil) != tt.wantErr {
				t.Errorf("IsCanceled() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("IsCanceled() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetrics_UpdateMemoryUsed(t *testing.T) {
	var v atomic.Value
	one := float64(1)
	v.Store(one)
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastMemoryUsed    float64
		memoryUsed        atomic.Value
		requestFinishTime time.Time
		setMemoryUsed     float64
		handler           func()
	}
	type args struct {
		memory float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{},
					mux:    sync.Mutex{},
				},
				memoryUsed:     v,
				lastMemoryUsed: float64(0),
				setMemoryUsed:  float64(1),
				handler:        func() {},
			},
			args: args{},
		},
		{
			name: "case isFunctionLoaded",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{},
					mux:    sync.Mutex{},
				},
				memoryUsed:     v,
				lastMemoryUsed: float64(0),
				setMemoryUsed:  float64(2),
				handler: func() {
					RunMetrics.SetFunctionLoaded()
				},
			},
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				lastMemoryUsed:    tt.fields.lastMemoryUsed,
				memoryUsed:        tt.fields.memoryUsed,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			tt.fields.handler()
			m.UpdateMemoryUsed(tt.fields.setMemoryUsed)
			assert.Equal(t, tt.fields.setMemoryUsed, m.lastMemoryUsed)
		})
	}
}

func TestMetrics_UpdateCPUUsed(t *testing.T) {
	var v atomic.Value
	lastCPU := CPUUsed{
		TotalTimestampNs: 1,
		CPUUsedNs:        10000000,
	}
	setCPU := CPUUsed{
		TotalTimestampNs: 2,
		CPUUsedNs:        20000000,
	}
	one := float64(1)
	v.Store(one)
	type fields struct {
		tracer            *futureTracer
		suicideTimer      *time.Timer
		suicideTimerMux   sync.Mutex
		lastCPUUsed       CPUUsed
		avgCPUUsed        atomic.Value
		maxCPUUsed        atomic.Value
		setCPUUsed        CPUUsed
		requestFinishTime time.Time
		handler           func()
	}
	type args struct {
		memory float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{},
					mux:    sync.Mutex{},
				},
				avgCPUUsed:  v,
				lastCPUUsed: lastCPU,
				maxCPUUsed:  v,
				setCPUUsed:  setCPU,
				handler:     func() {},
			},
			args: args{},
		},
		{
			name: "case2 empty lastCPU",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{},
					mux:    sync.Mutex{},
				},
				avgCPUUsed: v,
				maxCPUUsed: v,
				setCPUUsed: setCPU,
				handler:    func() {},
			},
			args: args{},
		},
		{
			name: "case isFunctionLoaded",
			fields: fields{
				tracer: &futureTracer{
					traces: map[string]*trace{},
					mux:    sync.Mutex{},
				},
				avgCPUUsed:  v,
				lastCPUUsed: lastCPU,
				maxCPUUsed:  v,
				setCPUUsed:  setCPU,
				handler: func() {
					RunMetrics.SetFunctionLoaded()
				},
			},
			args: args{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := gomonkey.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
				return int64(300), int64(128)
			})
			defer patches.Reset()
			m := &Metrics{
				tracer:            tt.fields.tracer,
				suicideTimer:      tt.fields.suicideTimer,
				suicideTimerMux:   tt.fields.suicideTimerMux,
				requestFinishTime: tt.fields.requestFinishTime,
			}
			tt.fields.handler()
			m.UpdateCPUUsed(tt.fields.setCPUUsed)
			assert.Equal(t, tt.fields.setCPUUsed, m.cpuUsed.Load().(CPUUsed))
		})
	}
}

func Test_max(t *testing.T) {
	var maxData = max(1.0, 5.0, 2.2)
	if maxData != 5.0 {
		t.Errorf("failed to get max float")
	}
}

func Test_RealTimeWriteNoPanic(t *testing.T) {
	convey.Convey("Test_WriteLog_No_Panic", t, func() {
		convey.Convey("no panic", func() {
			ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
			summarySpan := NewSummarySpan(constants.InvokeStage, "test", &config.Configuration{}, 3*time.Second)
			functionLog := logger.NewFunctionLogger(nil, "", "")
			logRecorder := functionLog.NewLogRecorder(ic, "test", constants.InitializeStage,
				logger.WithLogHeaderFooterGenerator(summarySpan),
				logger.WithLogOptionTail(),
			)
			go func() {
				time.Sleep(time.Millisecond * 100)
				functionLog.WriteStdLog("std logs", "my time", false, "time")
			}()
			logRecorder.Finish(ic)
			invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
			time.Sleep(3 * time.Second)
		})
	})
}

func Test_WriteStdLog(t *testing.T) {
	convey.Convey("Test_WriteStdLog", t, func() {
		convey.Convey("write stdLog with Standard Output", func() {
			var rangeFlag bool
			funcLogger, _ := logger.GetFunctionLogger(&config.Configuration{StartArgs: config.StartArgs{
				LogDir:   "/home/sn/logs",
				IsLoaded: false,
			}})
			patches := gomonkey.ApplyMethod(reflect.TypeOf(funcLogger), "RangeLoggerWithTraceID",
				func(_ *logger.FunctionLogger, funcLog *logger.FunctionLog, traceID string) {
					rangeFlag = true
					return
				})
			defer patches.Reset()
			ic := invokectx.DefaultMetrics.AcquireInvokeCtx()
			traceID := "5b16959d-0180-462b-8d81-5b2df0d267a5"
			summarySpan := NewSummarySpan(constants.InvokeStage, traceID, &config.Configuration{}, 3*time.Second)
			functionLog := logger.NewFunctionLogger(nil, "", "")
			logRecorder := functionLog.NewLogRecorder(ic, "test", constants.InitializeStage,
				logger.WithLogHeaderFooterGenerator(summarySpan),
				logger.WithLogOptionTail(),
			)
			logStr := "2024-08-19 07:34:05.957 " + traceID + " test"
			functionLog.WriteStdLog(logStr, "my time", false, "time")
			logRecorder.Finish(ic)
			invokectx.DefaultMetrics.ReleaseInvokeCtx(ic)
			assert.Equal(t, true, rangeFlag)
		})
	})
}

func Test_CalAvgCPUUsed(t *testing.T) {
	convey.Convey("Test_CalAvgCPUUsed", t, func() {
		convey.Convey("TotalTimestampNs < 1", func() {
			m := &Metrics{}
			cpuUsed := CPUUsed{TotalTimestampNs: 0}
			result := m.CalAvgCPUUsed(cpuUsed)
			assert.Equal(t, float64(0), result)
		})

		convey.Convey("current.TotalTimestampNs-pre.TotalTimestampNs < 1", func() {
			m := &Metrics{}
			pre := CPUUsed{TotalTimestampNs: 0}
			current := CPUUsed{TotalTimestampNs: 0}
			m.cpuUsed.Store(current)
			result := m.CalAvgCPUUsed(pre)
			assert.Equal(t, float64(0), result)
		})

		convey.Convey("cpuValue <= maxCPU", func() {
			m := &Metrics{}
			pre := CPUUsed{TotalTimestampNs: 1, CPUUsedNs: 10000000}
			current := CPUUsed{TotalTimestampNs: 2, CPUUsedNs: 20000000}
			numCPU := runtime.NumCPU()
			patch := gomonkey.ApplyGlobalVar(&numCPU, 1)
			patch.ApplyGlobalVar(&maxCPU, float64(1))
			defer patch.Reset()
			m.cpuUsed.Store(current)
			result := m.CalAvgCPUUsed(pre)
			assert.Equal(t, float64(1), result)
		})

		convey.Convey("cpuValue > maxCPU", func() {
			m := &Metrics{}
			pre := CPUUsed{TotalTimestampNs: 1, CPUUsedNs: 10000000}
			current := CPUUsed{TotalTimestampNs: 2, CPUUsedNs: 20000000}
			numCPU := runtime.NumCPU()
			patch := gomonkey.ApplyGlobalVar(&numCPU, 1)
			patch.ApplyGlobalVar(&maxCPU, float64(0))
			defer patch.Reset()
			m.cpuUsed.Store(current)
			result := m.CalAvgCPUUsed(pre)
			assert.Equal(t, float64(0), result)
		})
	})
}

func TestReportResourceUsage(t *testing.T) {
	type args struct {
		tickStr string
	}
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		args        args
	}{
		{"test can't get env", func() utils.PatchSlice {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "!" })})
			return patches
		}, args{}},
		{
			"test get env success but goroutine return",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "1" })})
				return patches
			},
			args{tickStr: ""},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		ReportResourceUsage(tt.args.tickStr)
		patches.ResetAll()
	}
}

func Test_reportResourceUsage(t *testing.T) {
	type args struct {
		interval time.Duration
		podName  string
	}
	tests := []struct {
		name    string
		patches utils.PatchesFunc
		args    args
	}{
		{
			"test report resource but stop chan is nil", func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(
					utils.PatchSlice{gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} { return nil })})
				return patches
			},
			args{},
		},
		{
			"test report resource success", func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} {
					stopChan := make(chan struct{})
					go func() {
						time.Sleep(10 * time.Millisecond)
						stopChan <- struct{}{}
					}()
					return stopChan
				})})
				return patches
			},
			args{interval: time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patches()
			RegisterResourceUsageCallBack(func(resourceUsage *common.ResourceUsage, reportTimeout time.Duration) error {
				return nil
			})
			reportResourceUsage(tt.args.interval, tt.args.podName)
			patches.ResetAll()
		})
	}
}

func TestMetrics_UpdateCaeMetrics(t *testing.T) {
	m := &Metrics{}
	caeMetrics := monitor.CaeMetrics{
		ReceiveBytes:         100,
		ReceivePackage:       200,
		TransmitBytes:        300,
		TransmitPackage:      400,
		FileSystemReadBytes:  500,
		FileSystemWriteBytes: 600,
		TimestampMs:          700,
	}
	m.UpdateCaeMetrics(caeMetrics)
	caeMetricsOfGet := m.GetCurrentCaeMetrics()
	assert.Equal(t, caeMetrics, caeMetricsOfGet, "GetCurrentCaeMetrics")
}

func Test_reportCaeMetrics(t *testing.T) {
	type args struct {
		interval time.Duration
		podName  string
	}
	tests := []struct {
		name    string
		patches utils.PatchesFunc
		args    args
	}{
		{
			"test report resource but stop chan is nil", func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(
					utils.PatchSlice{gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} { return nil })})
				return patches
			},
			args{},
		},
		{
			"test report resource success", func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} {
					stopChan := make(chan struct{})
					go func() {
						time.Sleep(10 * time.Millisecond)
						stopChan <- struct{}{}
					}()
					return stopChan
				})})
				return patches
			},
			args{interval: time.Millisecond},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patches()
			RegisterReportCaeMetricsCallBack(func(resourceUsage *common.CaeMetrics, reportTimeout time.Duration) error {
				return nil
			})
			reportCaeMetrics(tt.args.interval, tt.args.podName, false)
			patches.ResetAll()
		})
	}
}

func TestMetrics_CalAvgCaeMetricsData(t *testing.T) {
	m := Metrics{}
	m.UpdateCaeMetrics(monitor.CaeMetrics{
		ReceiveBytes:         2,
		ReceivePackage:       3,
		TransmitBytes:        4,
		TransmitPackage:      5,
		FileSystemReadBytes:  6,
		FileSystemWriteBytes: 7,
		TimestampMs:          8000,
	})

	avgCaeMetrics := m.CalAvgCaeMetricsData(monitor.CaeMetrics{
		ReceiveBytes:         1,
		ReceivePackage:       2,
		TransmitBytes:        3,
		TransmitPackage:      4,
		FileSystemReadBytes:  5,
		FileSystemWriteBytes: 6,
		TimestampMs:          7000,
	})

	if avgCaeMetrics.ReceiveBytesPerSecond != 1 || avgCaeMetrics.ReceivePackagePerSecond != 1 ||
		avgCaeMetrics.TransmitBytesPerSecond != 1 || avgCaeMetrics.TransmitPackagePerSecond != 1 ||
		avgCaeMetrics.FileSystemWriteBytesPerSecond != 1 || avgCaeMetrics.FileSystemReadBytesPerSecond != 1 {
		t.Errorf("failed to test CalAvgCaeMetricsData")
	}
}

func TestMetrics_InitRuntimeMetrics(t *testing.T) {
	convey.Convey("TestMetrics_InitRuntimeMetrics", t, func() {
		convey.Convey("successful", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Create, func(name string) (*os.File, error) {
					return &os.File{}, nil
				}),
				gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitRuntimeMetrics(&config.Configuration{})
			time.Sleep(10 * time.Millisecond)
			assert.Equal(t, nil, err)
		})
		convey.Convey("failure", func() {
			patch := gomonkey.ApplyFunc(os.Create, func(name string) (*os.File, error) {
				return &os.File{}, errors.New("test")
			})
			defer patch.Reset()
			err := InitRuntimeMetrics(&config.Configuration{})
			assert.NotNil(t, err)
		})
	})
}

func TestMetrics_updateRuntimeMetrics(t *testing.T) {
	convey.Convey("TestMetrics_updateRuntimeMetrics", t, func() {
		convey.Convey("successful", func() {
			runtimeMetrics := &runtimeMetricsMgr{
				runtimeMetrics: &runtimeMetrics{},
			}
			runtimeMetrics.updateRuntimeMetrics(1.1, 2.2, CPUUsed{3, 3})
			assert.Equal(t, 1.1, runtimeMetrics.runtimeMetrics.memoryUsed)
			assert.Equal(t, 2.2, runtimeMetrics.runtimeMetrics.storageUsed)
		})
	})
}

func TestMetrics_encodeRuntimeMetrics(t *testing.T) {
	convey.Convey("TestMetrics_encodeRuntimeMetrics", t, func() {
		convey.Convey("successful", func() {
			runtimeMetrics := &runtimeMetricsMgr{
				runtimeMetrics: &runtimeMetrics{},
			}
			data := runtimeMetrics.encodeRuntimeMetrics(new(bytes.Buffer))
			assert.NotEmpty(t, data)
		})
	})
}

func TestMetrics_reportRuntimeMetrics(t *testing.T) {
	convey.Convey("TestMetrics_reportRuntimeMetrics", t, func() {
		runtimeMetrics := &runtimeMetricsMgr{
			runtimeMetrics: &runtimeMetrics{},
			metricsPool: &sync.Pool{New: func() interface{} {
				return &bytes.Buffer{}
			}},
			file: &os.File{},
		}
		convey.Convey("successful", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} {
					stopChan := make(chan struct{})
					go func() {
						time.Sleep(50 * time.Millisecond)
						stopChan <- struct{}{}
					}()
					return stopChan
				}),
				gomonkey.ApplyFunc((*os.File).Seek, func(c *os.File, offset int64, whence int) (ret int64, err error) {
					return 0, nil
				}),
				gomonkey.ApplyFunc((*os.File).Close, func(c *os.File) (err error) {
					return nil
				}),
				gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					ch := make(chan time.Time, 1)
					ch <- time.Time{}
					return &time.Ticker{C: ch}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&bufio.Writer{}), "Flush",
					func(_ *bufio.Writer) error {
						return nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			runtimeMetrics.reportRuntimeMetrics()
		})
		convey.Convey("failure", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} {
					stopChan := make(chan struct{})
					go func() {
						time.Sleep(50 * time.Millisecond)
						stopChan <- struct{}{}
					}()
					return stopChan
				}),
				gomonkey.ApplyFunc((*os.File).Seek, func(c *os.File, offset int64, whence int) (ret int64, err error) {
					return 0, errors.New("test")
				}),
				gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					ch := make(chan time.Time, 1)
					ch <- time.Time{}
					return &time.Ticker{C: ch}
				}),
				gomonkey.ApplyFunc((*os.File).Close, func(c *os.File) (err error) {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&bufio.Writer{}), "Flush",
					func(_ *bufio.Writer) error {
						return nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			runtimeMetrics.reportRuntimeMetrics()
		})
	})
}

func TestMetrics_UpdateCustomReportMetrics(t *testing.T) {
	metric := tcpbustool.CustomReportMetric{
		Val:              "test",
		CustomReportTime: time.Now().Unix(),
	}
	m := &Metrics{}

	m.UpdateCustomReportMetrics(metric)
}

func TestMetrics_GetAndClearCustomReportMetrics(t *testing.T) {
	m := &Metrics{}
	customReportMetrics := m.GetAndClearCustomReportMetrics()
	if customReportMetrics == nil {
		t.Error("TestMetrics_GetAndClearCustomReportMetrics fail")
	}
}
