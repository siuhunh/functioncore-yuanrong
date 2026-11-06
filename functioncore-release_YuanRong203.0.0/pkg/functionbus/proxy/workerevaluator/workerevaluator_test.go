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

package workerevaluator

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/panjf2000/ants/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	workerID = "testA-instance-001"
)

var (
	stopChan         chan struct{}
	mockTaskPool     *ants.Pool = nil
	patches          testutils.PatchSlice
	requestChan      chan *http.Request
	backupTimeWindow *common.WorkerMetricsTimeWindow
	responseChan     chan interface{}
)

func mockClientDo(c *http.Client, _ *http.Request) (*http.Response, error) {
	rsp := action.GetWorkerSuccessResponse{
		Worker: &action.Worker{},
		Instance: &types.Instance{
			InstanceID: workerID,
			DeployedIP: "127.0.0.1",
		},
		Code: 150200,
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		log.GetLogger().Errorf("Marshal failed")
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, nil
}

func setup() {
	stopChan = make(chan struct{}, 1)
	client := &http.Client{}
	patches = testutils.InitPatchSlice()
	defer patches.ResetAll()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", mockClientDo),
	})
	CreateInstance(stopChan)
}

func tearDown() {
	close(stopChan)
}

func TestGetInstance(t *testing.T) {
	setup()
	defer tearDown()

	if GetInstance() == nil {
		t.Errorf("Get worker evaluator instance failed")
		return
	}
}

func TestWorkerEvaluator_oneFunctionOneInstance(t *testing.T) {
	setup()
	defer tearDown()

	workerID := "testA-instance-001"
	functionKey := "testA"

	mrd := CreateMockRequestDispatcher(functionKey)
	GetInstance().Observe(functionKey, mrd)
	GetInstance().getOrAddWorkerMetrics("testA-instance-001", WithFunctionKeyWithResource("testA"),
		WithNodeIP("127.0.0.1"), WithLocalWorker(false))

	MockBatchAddlatency(workerID, 256)
	time.Sleep(100 * time.Millisecond)

	mrd.Lock()
	if mrd.finishedRequests != 256 {
		t.Error("Observer recv request count failed")
	}
	mrd.Unlock()
}

func MockBatchAddlatency(workerID string, count int) {
	for i := 0; i < count; i++ {
		GetMockTaskPool().Submit(func() {
			GetInstance().RecordRequestEnd(workerID, time.Millisecond)
		})
	}
}

func GetMockTaskPool() *ants.Pool {
	if mockTaskPool == nil {
		var err error
		mockTaskPool, err = ants.NewPool(1000, ants.WithPreAlloc(true))
		if err != nil {
			log.GetLogger().Error("new mock task pool failed")
			return nil
		}
	}
	return mockTaskPool
}

func MockClientDo(c *http.Client, req *http.Request) (*http.Response, error) {
	requestChan <- req
	rsp := action.GetWorkerSuccessResponse{
		Worker: &action.Worker{},
		Instance: &types.Instance{
			InstanceID: workerID,
			DeployedIP: "127.0.0.1",
		},
		Code: 150200,
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		log.GetLogger().Errorf("Marshal failed")
		return nil, err
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, nil
}

func collectSetup() {

	requestChan = make(chan *http.Request, 1)
	patches = testutils.InitPatchSlice()

	client := &http.Client{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.ParseFuncKey, func(funcKey string) (string, string, string) {
			return "test1", "test1", "test1"
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(isStatefulFunc, func(metrics *WorkerMetrics) bool {
			return false
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(isMicroServiceFunc, func(metrics *WorkerMetrics) bool {
			return false
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(isWebSocketFunc, func(metrics *WorkerMetrics) bool {
			return false
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return client
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", MockClientDo),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURL, func() string {
			return ""
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {
		}),
	})
	backupTimeWindow = common.FConfig.WorkerMetricsTimeWindow
	common.FConfig.WorkerMetricsTimeWindow = &common.WorkerMetricsTimeWindow{
		TimeWindowLength: 1,
		NumberThreshold:  1,
	}
	stopChan = make(chan struct{}, 1)
	CreateInstance(stopChan)
}

func collectTearDown() {
	close(stopChan)
	close(requestChan)
	patches.ResetAll()
	common.FConfig.WorkerMetricsTimeWindow = backupTimeWindow
}

// check bus report worker metrics valid
// input:worker metrics
// output worker report metrics
func TestPeriodCollectMetrics(t *testing.T) {
	convey.Convey("TestPeriodCollectMetrics", t, func() {
		collectSetup()
		defer collectTearDown()

		metrics := GetInstance().getOrAddWorkerMetrics(workerID, WithFunctionKeyWithResource("testA"),
			WithNodeIP("127.0.0.1"), WithLocalWorker(false))
		convey.So(metrics, convey.ShouldNotBeNil)

		GetInstance().RecordRequestStart("error_worker_id")
		GetInstance().RecordRequestStart(workerID)
		time.Sleep(time.Duration(1) * time.Second)
		GetInstance().RecordRequestEnd(workerID, time.Duration(1)*time.Second)
		req := <-requestChan
		convey.So(req, convey.ShouldNotBeNil)

		body, err := ioutil.ReadAll(req.Body)
		convey.So(err, convey.ShouldBeNil)

		var message map[string]json.RawMessage
		var reportMetrics []workermanager.WorkerView
		err = json.Unmarshal(body, &message)
		convey.So(err, convey.ShouldBeNil)
		var viewType string
		err = json.Unmarshal(message[workermanager.ViewType], &viewType)
		convey.So(err, convey.ShouldBeNil)
		convey.So(viewType, convey.ShouldEqual, workermanager.WorkerMetric)
		err = json.Unmarshal(message[workermanager.ViewObject], &reportMetrics)
		convey.So(len(reportMetrics), convey.ShouldBeGreaterThan, 0)
		convey.So(reportMetrics[0].Metrics[0].UsedTimeSlice, convey.ShouldEqual, 1)
	})
}

func MockMarshal(v interface{}) ([]byte, error) {
	responseChan <- v
	return nil, errors.New("")
}

func syncAbandonedSetup() {
	stopChan = make(chan struct{}, 1)
	responseChan = make(chan interface{}, 1)
	CreateInstance(stopChan)

	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(json.Marshal, MockMarshal),
	})
}

func syncAbandonedTearDown() {
	close(stopChan)
	close(responseChan)
	patches.ResetAll()
}

func TestSyncAbandoned(t *testing.T) {
	convey.Convey("TestSyncAbandoned", t, func() {
		convey.Convey("TestSyncAbandoned success", func() {
			CreateInstance(nil)
			syncAbandonedSetup()
			response := &pb.CallResponse{
				Labels: map[string]string{
					constants.HeaderRequestID: "1234",
				},
				ErrorCode: proxyerrors.StateOKCode,
			}
			Client = &metricsClient{
				c: &redisclient.Client{},
			}
			SyncAbandoned(response)
			rsp := <-responseChan
			convey.So(rsp, convey.ShouldNotBeNil)
			realRsp, ok := rsp.(*asyncResponse)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realRsp.StatusCode, convey.ShouldEqual, http.StatusOK)
			syncAbandonedTearDown()
		})

		convey.Convey("TestSyncAbandonedByPosixCallResult", func() {
			CreateInstance(nil)
			syncAbandonedSetup()

			result := &pb.PosixCallResult{
				Labels: map[string]string{
					constants.HeaderRequestID: "1234",
				},
				ErrorCode: proxyerrors.StateOKCode,
			}
			Client = &metricsClient{
				c: &redisclient.Client{},
			}
			SyncAbandonedByPosixCallResult(result)
			rsp := <-responseChan
			convey.So(rsp, convey.ShouldNotBeNil)
			realRsp, ok := rsp.(*asyncResponse)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realRsp.StatusCode, convey.ShouldEqual, http.StatusOK)
			syncAbandonedTearDown()
		})

		convey.Convey("TestSyncAbandonedByPosixCallResult with StateAcceptedCode", func() {
			CreateInstance(nil)
			syncAbandonedSetup()

			result := &pb.PosixCallResult{
				ErrorCode: proxyerrors.StateAcceptedCode,
			}
			Client = &metricsClient{
				c: &redisclient.Client{},
			}
			SyncAbandonedByPosixCallResult(result)
			var rsp interface{}
			select {
			case rsp = <-responseChan:
			case <-time.After(100 * time.Millisecond):
				fmt.Println("Timeout: No request received within 100ms")
			}
			convey.So(rsp, convey.ShouldBeNil)
			syncAbandonedTearDown()
		})

		convey.Convey("TestSyncAbandonedByPosixCallResult with MaxStoreResponseSize", func() {
			CreateInstance(nil)
			syncAbandonedSetup()
			defer syncAbandonedTearDown()
			size := 200 * 1024 // 100K
			rawData := make([]byte, size)

			result := &pb.PosixCallResult{
				Labels: map[string]string{
					constants.HeaderRequestID: "1234",
				},
				ErrorCode: proxyerrors.StateOKCode,
				RawData:   rawData,
			}
			Client = &metricsClient{
				c: &redisclient.Client{},
			}
			SyncAbandonedByPosixCallResult(result)
			rsp := <-responseChan
			convey.So(rsp, convey.ShouldNotBeNil)
			realRsp, ok := rsp.(*asyncResponse)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(realRsp.StatusCode, convey.ShouldEqual, http.StatusOK)
		})
	})
}

func Test_isDisableReportFunc(t *testing.T) {
	funcList := []string{"funcName"}
	common.FConfig.AutoScalerConfig = &common.AutoScalerConfig{
		DisAbleReportFunctionList: funcList,
	}
	type args struct {
		funcName string
	}
	var a args
	a.funcName = "funcName"
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDisableReportFunc(tt.args.funcName); got != tt.want {
				t.Errorf("isDisableReportFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsStatefulFunc(t *testing.T) {
	convey.Convey("Test i stateful func true", t, func() {
		collectSetup()
		defer collectTearDown()
		metrics := GetInstance().getOrAddWorkerMetrics(workerID, WithFunctionKeyWithResource("testA"),
			WithNodeIP("127.0.0.1"), WithLocalWorker(false))
		res := isStatefulFunc(metrics)
		convey.So(res, convey.ShouldBeFalse)
	})
}

func TestIsMicroServiceFunc(t *testing.T) {
	convey.Convey("Test i microservice func true", t, func() {
		collectSetup()
		defer collectTearDown()
		metrics := GetInstance().getOrAddWorkerMetrics(workerID, WithFunctionKeyWithResource("testA"),
			WithNodeIP("127.0.0.1"), WithLocalWorker(false))
		res := isMicroServiceFunc(metrics)
		convey.So(res, convey.ShouldBeFalse)
	})
}

func Test_isWebSocketFunc1(t *testing.T) {
	mockFuncKey := "mockFuncKey"
	tests := []struct {
		name        string
		want        bool
		patchesFunc testutils.PatchesFunc
		metrics     *WorkerMetrics
	}{
		{
			name:    "case 1 is ws func",
			metrics: &WorkerMetrics{},
			want:    true,
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetFuncKeyFromFuncKeyWithResource, func(string) string { return mockFuncKey }),
					gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionFuncMetaData, func(
						_ *servicediscovery.Registry, _ string) (servicediscovery.FuncMetaData, error) {
						return servicediscovery.FuncMetaData{
							BusinessType: constants.BusinessTypeWebSocket,
						}, nil
					}),
				})
				return patches
			},
		}, {
			name:    "case 2 get meta info error",
			metrics: &WorkerMetrics{},
			want:    true,
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetFuncKeyFromFuncKeyWithResource, func(string) string { return mockFuncKey }),
					gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionFuncMetaData, func(
						_ *servicediscovery.Registry, _ string) (servicediscovery.FuncMetaData, error) {
						return servicediscovery.FuncMetaData{}, errors.New("get meta error")
					}),
				})
				return patches
			},
		}, {
			name:    "case 3 get meta info error",
			metrics: &WorkerMetrics{},
			want:    false,
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetFuncKeyFromFuncKeyWithResource, func(string) string { return mockFuncKey }),
					gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionFuncMetaData, func(
						_ *servicediscovery.Registry, _ string) (servicediscovery.FuncMetaData, error) {
						return servicediscovery.FuncMetaData{
							BusinessType: "",
						}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			res := isWebSocketFunc(tt.metrics)
			if res != tt.want {
				t.Errorf("isWebSocketFunc test failed, actual: %t, expected: %t", isWebSocketFunc(tt.metrics), tt.want)
			}
			p.ResetAll()
		})
	}
}

func Test_isMicroServiceFunc(t *testing.T) {
	metrics := &WorkerMetrics{}
	convey.Convey("test isMicroServiceFunc, GetFunctionFuncMetaData error", t, func() {
		patch := testutils.InitPatchSlice()
		patch.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionFuncMetaData, func(_ *servicediscovery.Registry, functionKey string) (servicediscovery.FuncMetaData, error) {
				return servicediscovery.FuncMetaData{}, errors.New("this is error")
			}),
		})
		defer patch.ResetAll()
		res := isMicroServiceFunc(metrics)
		if !res {
			t.Errorf("this is error")
		}
	})

	convey.Convey("test isMicroServiceFunc, GetFunctionFuncMetaData pass, BusinessType: constants.BusinessTypeCAE", t, func() {
		patch := testutils.InitPatchSlice()
		patch.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionFuncMetaData, func(_ *servicediscovery.Registry, functionKey string) (servicediscovery.FuncMetaData, error) {
				return servicediscovery.FuncMetaData{BusinessType: constants.BusinessTypeCAE}, nil
			}),
		})
		defer patch.ResetAll()
		res := isMicroServiceFunc(metrics)
		if !res {
			t.Errorf("this is error")
		}
	})

	convey.Convey("test isMicroServiceFunc, GetFunctionFuncMetaData error", t, func() {
		patch := testutils.InitPatchSlice()
		patch.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetFunctionFuncMetaData, func(_ *servicediscovery.Registry, functionKey string) (servicediscovery.FuncMetaData, error) {
				return servicediscovery.FuncMetaData{BusinessType: ""}, nil
			}),
		})
		defer patch.ResetAll()
		res := isMicroServiceFunc(metrics)
		if res {
			t.Errorf("this is error")
		}
	})
}

func TestProcessAsyncResult(t *testing.T) {
	convey.Convey("TestProcessAsyncResult success", t, func() {
		CreateInstance(nil)
		syncAbandonedSetup()
		Client = &metricsClient{
			c: &redisclient.Client{},
		}
		ProcessAsyncResult(proxyerrors.StateOKCode, make(map[string]string), []byte{},
			"", "defaultaz-#-pool24-600-512-fusion-64bdd8757b-tqdf7")
		rsp := <-responseChan
		convey.So(rsp, convey.ShouldNotBeNil)
		realRsp, ok := rsp.(*asyncResponse)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(realRsp.StatusCode, convey.ShouldEqual, http.StatusOK)
		syncAbandonedTearDown()
	})

}
