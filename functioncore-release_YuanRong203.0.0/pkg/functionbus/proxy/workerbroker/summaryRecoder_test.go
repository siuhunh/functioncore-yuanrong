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

package workerbroker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/functionbus/common"
)

func Test_SummaryRecorder(t *testing.T) {
	runtimeMetrics := common.RuntimeMetricsProperty{
		FunctionURN:      "absPrefix:absZone:absBusinessID:absTenantID:absProductID:absName:latest",
		PodName:          "pool22-300-128-fusion-595d598b8c-c2btg",
		MetricsName:      constants.MetricsUsedResourceTimeWindow,
		GPUMem:           0,
		EphemeralStorage: 512,
	}

	broker := &workerBroker{
		workerID:            "defaultaz-#-pool22-300-128-fusion-595d598b8c-c2btg",
		funcKeyWithResource: "c53626012ba84727b938ca8bf03108ef/0@default@java-test/latest/300-12",
	}
	Convey("Test SummaryRecorder", t, func() {
		Convey("invalid stage", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			summaryRecorder.StartBilling("", 1, 1)
			summaryRecorder.StopBilling("", 1)
			summaryRecorder.mergeAndReportBillData()
			So(summaryRecorder.runtimeMetrics.MetricsValue, ShouldEqual, "")
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})
		Convey("init stage with concurrency = 1", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 1, 1)
			time.Sleep(10 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 1)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 0)
			So(summaryRecorder.runtimeMetrics.MetricsValue, ShouldEqual, "")
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("init stage with concurrency > 1", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(10 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 100)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 1)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InitializeStage)
			So(summaryRecorder.reportSummary[0].BusyDuration, ShouldEqual, 10)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("init stage with concurrency > 1, no stop", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(10 * time.Millisecond)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 1)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InitializeStage)
			So(summaryRecorder.reportSummary[0].BusyDuration, ShouldEqual, 10)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke stage with concurrency = 1", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InvokeStage, 1, 1)
			time.Sleep(10 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 1)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 0)
			So(summaryRecorder.runtimeMetrics.MetricsValue, ShouldEqual, "")
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke stage with concurrency > 1", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 1)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].BusyDuration, 90, 110)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].StartTime, time.Now().Nanosecond()/1000000-120, time.Now().Nanosecond()/1000000-80)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].EndTime, time.Now().Nanosecond()/1000000-20, time.Now().Nanosecond()/1000000+20)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke and init stage with concurrency = 1", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 1, 1)
			time.Sleep(20 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 1)
			summaryRecorder.StartBilling(constants.InvokeStage, 1, 1)
			time.Sleep(10 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 1)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 0)
			So(summaryRecorder.runtimeMetrics.MetricsValue, ShouldEqual, "")
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke and init stage with concurrency > 1", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(200 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 100)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 2)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InitializeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].BusyDuration, 200, 220)
			So(summaryRecorder.reportSummary[1].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[1].BusyDuration, 100, 120)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke and init stage with concurrency > 1, multiple requests overlap case 1", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(200 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 100)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 2)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InitializeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].BusyDuration, 200, 220)
			So(summaryRecorder.reportSummary[1].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[1].BusyDuration, 400, 420)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke and init stage with concurrency > 1, multiple requests overlap successful case 2", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(200 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 100)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 2)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InitializeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].BusyDuration, 200, 220)
			So(summaryRecorder.reportSummary[1].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[1].BusyDuration, 250, 270)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke stage with concurrency > 1, multiple requests overlap case 3", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			patch := ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("test") })
			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(200 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 100)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 2)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InitializeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].BusyDuration, 200, 220)
			So(summaryRecorder.reportSummary[1].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[1].BusyDuration, 250, 270)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})
		Convey("invoke stage with concurrency > 1, multiple requests overlap case 4", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			var mockOK bool
			patch := ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "RPush",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					return &redis.IntCmd{}
				})
			ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					if mockOK {
						return 0, nil
					}
					return 0, errors.New("test")
				})
			ApplyFunc(redisclient.WriteToDisk,
				func(redisCache redisclient.RedisCache) error {
					if mockOK {
						return nil
					}
					return errors.New("error")
				})

			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(100 * time.Millisecond)
			mockOK = true
			summaryRecorder.mergeAndReportBillData()
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(150 * time.Millisecond)
			currentSummary := []TimeWindow{}
			err := json.Unmarshal([]byte(summaryRecorder.runtimeMetrics.MetricsValue), &currentSummary)
			So(err, ShouldBeNil)
			So(len(currentSummary), ShouldEqual, 1)
			ShouldBeBetweenOrEqual(currentSummary[0].BusyDuration, 200, 220)
			mockOK = false
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 1)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].BusyDuration, 150, 170)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})
		Convey("invoke stage with concurrency > 1, multiple requests overlap case 5", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			var mockOK bool
			patch := ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "RPush",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					return &redis.IntCmd{}
				})
			ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					if mockOK {
						return 0, nil
					}
					return 0, errors.New("test")
				})
			ApplyFunc(redisclient.WriteToDisk,
				func(redisCache redisclient.RedisCache) error {
					if mockOK {
						return nil
					}
					return errors.New("error")
				})

			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			mockOK = true
			summaryRecorder.mergeAndReportBillData()
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			currentSummary := []TimeWindow{}
			err := json.Unmarshal([]byte(summaryRecorder.runtimeMetrics.MetricsValue), &currentSummary)
			So(err, ShouldBeNil)
			So(len(currentSummary), ShouldEqual, 1)
			ShouldBeBetweenOrEqual(currentSummary[0].BusyDuration, 200, 220)
			mockOK = false
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 1)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[0].BusyDuration, 150, 170)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})

		Convey("invoke stage with concurrency > 1, multiple requests overlap case 6", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			var mockOK bool
			patch := ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "RPush",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					return &redis.IntCmd{}
				})
			ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					if mockOK {
						return 0, nil
					}
					return 0, errors.New("test")
				})
			ApplyFunc(redisclient.WriteToDisk,
				func(redisCache redisclient.RedisCache) error {
					if mockOK {
						return nil
					}
					return errors.New("error")
				})

			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			mockOK = false
			summaryRecorder.mergeAndReportBillData()
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			currentSummary := []TimeWindow{}
			err := json.Unmarshal([]byte(summaryRecorder.runtimeMetrics.MetricsValue), &currentSummary)
			So(err, ShouldBeNil)
			So(len(currentSummary), ShouldEqual, 1)
			ShouldBeBetweenOrEqual(currentSummary[0].BusyDuration, 200, 220)
			mockOK = false
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 2)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(currentSummary[0].BusyDuration, 200, 220)
			So(summaryRecorder.reportSummary[1].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[1].BusyDuration, 150, 170)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})
		Convey("invoke stage with concurrency > 1, multiple requests overlap case 7", func() {
			stopCh := make(chan struct{})
			summaryRecorder := newSummaryRecorder(broker, runtimeMetrics, stopCh)
			var mockOK bool
			patch := ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "RPush",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					return &redis.IntCmd{}
				})
			ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					if mockOK {
						return 0, nil
					}
					return 0, errors.New("test")
				})
			ApplyFunc(redisclient.WriteToDisk,
				func(redisCache redisclient.RedisCache) error {
					if mockOK {
						return nil
					}
					return errors.New("error")
				})

			defer func() {
				patch.Reset()
			}()
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 100)
			summaryRecorder.StartBilling(constants.InitializeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InitializeStage, 100)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			time.Sleep(100 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			mockOK = false
			summaryRecorder.mergeAndReportBillData()
			summaryRecorder.StartBilling(constants.InvokeStage, 100, 1)
			currentSummary := []TimeWindow{}
			err := json.Unmarshal([]byte(summaryRecorder.runtimeMetrics.MetricsValue), &currentSummary)
			So(err, ShouldBeNil)
			So(len(currentSummary), ShouldEqual, 2)
			ShouldBeBetweenOrEqual(currentSummary[0].BusyDuration, 200, 220)
			ShouldBeBetweenOrEqual(currentSummary[1].BusyDuration, 200, 220)
			mockOK = false
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.StopBilling(constants.InvokeStage, 100)
			time.Sleep(150 * time.Millisecond)
			summaryRecorder.mergeAndReportBillData()
			So(len(summaryRecorder.reportSummary), ShouldEqual, 3)
			So(summaryRecorder.reportSummary[0].Stage, ShouldEqual, constants.InitializeStage)
			ShouldBeBetweenOrEqual(currentSummary[0].BusyDuration, 200, 220)
			So(summaryRecorder.reportSummary[1].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(currentSummary[1].BusyDuration, 200, 220)
			So(summaryRecorder.reportSummary[2].Stage, ShouldEqual, constants.InvokeStage)
			ShouldBeBetweenOrEqual(summaryRecorder.reportSummary[2].BusyDuration, 150, 170)
			close(stopCh)
			time.Sleep(10 * time.Millisecond)
		})
	})
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
			name: "case1",
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
