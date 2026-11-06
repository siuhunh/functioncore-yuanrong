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

package invoke

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/runtime"
)

func TestServer_ReportConcurrentResourceTimeWindows(t *testing.T) {
	runtime.ConResource = &runtime.ConcurrentResource{}
	s := &Server{
		conf:       &config.Configuration{},
		bus:        &tcpbus.TCPBus{},
		billingLog: &functionlog.BillingLogger{},
	}
	Convey("Test ReportConcurrentResourceTimeWindows", t, func() {
		Convey("pod is create by minInstance with idle mode", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.ReservedInstanceIdleMode
				return
			})

			patch.ApplyGlobalVar(&runtime.PodApplier, constants.WorkerManagerApplier)
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.ReservedInstanceIdleMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond), constants.InvokeStage)
			err := s.ReportConcurrentResourceTimeWindows("", "")
			So(err, ShouldBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 0)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 0)
		})

		Convey("pod is create by minInstance with non-idle mode", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.ReservedInstanceNonIdleMode
				return
			})

			patch.ApplyGlobalVar(&runtime.PodApplier, constants.WorkerManagerApplier)
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.ReservedInstanceNonIdleMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
				runtime.ConResource.ClearReportedTimeWindows()
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond), constants.InvokeStage)
			err := s.ReportConcurrentResourceTimeWindows("", "")
			So(err, ShouldBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 0)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 1)
		})
		Convey("no timeWindow", func() {
			patch := ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.NonReservedInstanceMode
				return
			})
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.NonReservedInstanceMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
			}()
			err := s.ReportConcurrentResourceTimeWindows("", "")
			So(err, ShouldBeNil)
		})
		Convey("report success", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.NonReservedInstanceMode
				return
			})
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.NonReservedInstanceMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond), constants.InvokeStage)
			runtime.ConResource.AppendTimeWindows(startTime.Add(2*time.Millisecond),
				startTime.Add(3*time.Millisecond), constants.InvokeStage)
			err := s.ReportConcurrentResourceTimeWindows("", "")
			So(err, ShouldBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 0)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 0)
		})
		Convey("failed to report", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return nil, errors.New("failed to report")
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.NonReservedInstanceMode
				return
			})
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.NonReservedInstanceMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
				runtime.ConResource.RemoveReportedTimeWindows(2)
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond),
				constants.InvokeStage)
			runtime.ConResource.AppendTimeWindows(startTime.Add(2*time.Millisecond),
				startTime.Add(3*time.Millisecond), constants.InvokeStage)
			err := s.ReportConcurrentResourceTimeWindows("", "")
			So(err, ShouldNotBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 1)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 0)
		})
	})
}

func TestServer_gatherTimeWindows(t *testing.T) {
	tmpTime := time.Date(2019, 13, 15, 23, 90, 12, 04, time.UTC)
	testCases := []struct {
		name                      string
		timeWindows               []runtime.TimeWindow
		gatheredTimeWindows       []runtime.TimeWindow
		expectGatheredTimeWindows []runtime.TimeWindow
	}{
		{
			name: "gather two time windows(InvokeStage)",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
				{
					StartTime:       3,
					EndTime:         4,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         4,
					BillingDuration: 2,
					BusyDuration:    2,
					IdleDuration:    maxBillingDuration - 2,
					Stage:           constants.InvokeStage,
				},
			},
		},
		{
			name: "gather two time windows when len(gatheredTimeWindows) = 1",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
				{
					StartTime:       3,
					EndTime:         4,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
			},
			gatheredTimeWindows: []runtime.TimeWindow{{}},
			expectGatheredTimeWindows: []runtime.TimeWindow{{},
				{
					StartTime:       1,
					EndTime:         4,
					BillingDuration: 2,
					BusyDuration:    2,
					IdleDuration:    maxBillingDuration - 2,
					Stage:           constants.InvokeStage,
				},
			},
		},
		{
			name: "gather a timeWindow, but the BillingDuration > maxBillingDuration",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       0,
					EndTime:         1,
					BillingDuration: maxBillingDuration + 1,
					Stage:           constants.InvokeStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       0,
					EndTime:         1,
					BillingDuration: maxBillingDuration,
					BusyDuration:    maxBillingDuration,
					IdleDuration:    0,
					Stage:           constants.InvokeStage,
				},
			},
		},
		{
			name: "gather two timeWindow, but the totalBillingDuration > maxBillingDuration",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: maxBillingDuration - 1,
					Stage:           constants.InvokeStage,
				},
				{
					StartTime:       3,
					EndTime:         4,
					BillingDuration: 2,
					Stage:           constants.InvokeStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         4,
					BillingDuration: maxBillingDuration,
					BusyDuration:    maxBillingDuration,
					IdleDuration:    0,
					Stage:           constants.InvokeStage,
				},
			},
		},
		{
			name: "gather five timeWindow: load、initialize、extension、invoke、invoke",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       0,
					EndTime:         1,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.LoadStage,
				},
				{
					StartTime:       2,
					EndTime:         3,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.ExtensionStage,
				},
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.InitializeStage,
				},
				{
					StartTime:       3,
					EndTime:         4,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
				{
					StartTime:       5,
					EndTime:         6,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       0,
					EndTime:         1,
					BillingDuration: 1,
					BusyDuration:    1,
					IdleDuration:    0,
					Stage:           constants.LoadStage,
				},
				{
					StartTime:       2,
					EndTime:         3,
					BillingDuration: 1,
					BusyDuration:    1,
					IdleDuration:    0,
					Stage:           constants.ExtensionStage,
				},
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					IdleDuration:    0,
					Stage:           constants.InitializeStage,
				},
				{
					StartTime:       3,
					EndTime:         6,
					BillingDuration: 2,
					BusyDuration:    2,
					IdleDuration:    maxBillingDuration - 2,
					Stage:           constants.InvokeStage,
				},
			},
		},
		{
			name: "gather one timeWindow(InitializeStage)",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.InitializeStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.InitializeStage,
				},
			},
		},
		{
			name: "gather one timeWindow(LoadStage)",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.LoadStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.LoadStage,
				},
			},
		},
		{
			name: "gather one timeWindow(ExtensionStage)",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.ExtensionStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.ExtensionStage,
				},
			},
		},
	}
	for _, tt := range testCases {
		Convey(tt.name, t, func() {
			defer func() {
				gatheredTimeWindows = nil
			}()
			gatheredTimeWindows = tt.gatheredTimeWindows
			allDuration := getAllDurationWithPeriod(tmpTime, tmpTime.Add(300*time.Second))
			gatherTimeWindows(tt.timeWindows, allDuration)
			So(gatheredTimeWindows, ShouldResemble, tt.expectGatheredTimeWindows)
		})
	}
}

func TestServer_ReportPreStopInvokeTimeWindows(t *testing.T) {
	runtime.ConResource = &runtime.ConcurrentResource{}
	s := &Server{
		conf:       &config.Configuration{},
		bus:        &tcpbus.TCPBus{},
		billingLog: &functionlog.BillingLogger{},
	}
	Convey("Test ReportPreStopInvokeTimeWindows", t, func() {
		Convey("pod is create by minInstance with idle mode", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.ReservedInstanceIdleMode
				return
			})

			patch.ApplyGlobalVar(&runtime.PodApplier, constants.WorkerManagerApplier)
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.ReservedInstanceIdleMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond), constants.InvokeStage)
			err := s.ReportPreStopInvokeTimeWindows("", "")
			So(err, ShouldBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 0)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 0)
		})

		Convey("pod is create by minInstance with non-idle mode", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.ReservedInstanceNonIdleMode
				return
			})

			patch.ApplyGlobalVar(&runtime.PodApplier, constants.WorkerManagerApplier)
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.ReservedInstanceNonIdleMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
				runtime.ConResource.ClearReportedTimeWindows()
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond), constants.InvokeStage)
			err := s.ReportPreStopInvokeTimeWindows("", "")
			So(err, ShouldBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 0)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 1)
		})
		Convey("no timeWindow", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.NonReservedInstanceMode
				return
			})
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.NonReservedInstanceMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
			}()
			err := s.ReportPreStopInvokeTimeWindows("", "")
			So(err, ShouldBeNil)
		})
		Convey("report success", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.NonReservedInstanceMode
				return
			})
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.NonReservedInstanceMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond), constants.InvokeStage)
			runtime.ConResource.AppendTimeWindows(startTime.Add(2*time.Millisecond),
				startTime.Add(3*time.Millisecond), constants.InvokeStage)
			err := s.ReportPreStopInvokeTimeWindows("", "")
			So(err, ShouldBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 0)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 1)
		})
		Convey("failed to report", func() {
			patch := ApplyMethod(reflect.TypeOf(s.bus), "ReportRuntimeMetrics",
				func(_ *tcpbus.TCPBus, ctx context.Context,
					_ *common.RuntimeMetricsProperty) (*commontypes.TCPResponse, error) {
					return nil, errors.New("failed to report")
				})
			patch.ApplyMethod(reflect.TypeOf(s.billingLog), "WriteLog",
				func(_ *functionlog.BillingLogger, billingType, billingMessage string) {
					return
				})
			patch.ApplyFunc(runtime.SetReservedInstanceModeInCurrentTimeWindow, func() {
				runtime.CurrentIdleMode = constants.NonReservedInstanceMode
				return
			})
			patch.ApplyGlobalVar(&runtime.CurrentIdleMode, constants.NonReservedInstanceMode)
			defer func() {
				patch.Reset()
				gatheredTimeWindows = nil
				runtime.ConResource.RemoveReportedTimeWindows(2)
			}()
			startTime := time.Now()
			runtime.ConResource.AppendTimeWindows(startTime, startTime.Add(1*time.Millisecond),
				constants.InvokeStage)
			runtime.ConResource.AppendTimeWindows(startTime.Add(2*time.Millisecond),
				startTime.Add(3*time.Millisecond), constants.InvokeStage)
			err := s.ReportPreStopInvokeTimeWindows("", "")
			So(err, ShouldNotBeNil)
			So(len(gatheredTimeWindows), ShouldEqual, 1)
			So(len(runtime.ConResource.GetTimeWindows(startTime, startTime.Add(1*time.Millisecond))),
				ShouldEqual, 2)
		})
	})
}

func TestServer_gatherPreStopTimeWindows(t *testing.T) {
	tmpTime := time.Date(2019, 13, 15, 23, 90, 12, 04, time.UTC)
	testCases := []struct {
		name                      string
		timeWindows               []runtime.TimeWindow
		gatheredTimeWindows       []runtime.TimeWindow
		expectGatheredTimeWindows []runtime.TimeWindow
	}{
		{
			name: "gather two time windows(InvokeStage)",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
				{
					StartTime:       3,
					EndTime:         4,
					BillingDuration: 1,
					Stage:           constants.InvokeStage,
				},
			},
			expectGatheredTimeWindows: nil,
		},
		{
			name: "gather one time windows when len(gatheredTimeWindows) = 1",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       3,
					EndTime:         4,
					BillingDuration: 1,
					Stage:           constants.PreStopStage,
				},
			},
			gatheredTimeWindows: []runtime.TimeWindow{{}},
			expectGatheredTimeWindows: []runtime.TimeWindow{{},
				{
					StartTime:       3,
					EndTime:         4,
					BillingDuration: 1,
					Stage:           constants.PreStopStage,
				},
			},
		},
		{
			name: "gather one timeWindow(PreStopStage)",
			timeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.PreStopStage,
				},
			},
			expectGatheredTimeWindows: []runtime.TimeWindow{
				{
					StartTime:       1,
					EndTime:         2,
					BillingDuration: 1,
					BusyDuration:    1,
					Stage:           constants.PreStopStage,
				},
			},
		},
	}
	for _, tt := range testCases {
		Convey(tt.name, t, func() {
			defer func() {
				gatheredTimeWindows = nil
			}()
			gatheredTimeWindows = tt.gatheredTimeWindows
			allDuration := getAllDurationWithPeriod(tmpTime, tmpTime.Add(300*time.Second))
			gatherPreStopTimeWindows(tt.timeWindows, allDuration)
			So(gatheredTimeWindows, ShouldResemble, tt.expectGatheredTimeWindows)
		})
	}
}
