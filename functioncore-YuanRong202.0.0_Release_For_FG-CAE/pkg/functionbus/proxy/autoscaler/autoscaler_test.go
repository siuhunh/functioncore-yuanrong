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

package autoscaler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/dataaggregation"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/autoscaler/algorithm"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher/queue"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

func patchedSetup(p *Patches) {
	common.FConfig.HTTPSConfig = &commontls.InternalHTTPSConfig{}

	p.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
		return &http.Client{Timeout: 600000}
	})
	p.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("")
	})
	p.ApplyFunc((*workerbroker.Manager).CreateWorkerBroker, func(mgr *workerbroker.Manager, config *workerbroker.BrokerConfig) error {
		return nil
	})

	p.ApplyFunc(utils.GetWorkerManagerBaseURL, func() string {
		return ""
	})

	p.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {})

}

func TestQueueLengthChanged(t *testing.T) {
	p := NewPatches()
	patchedSetup(p)
	defer patches.ResetAll()

	stopCh := make(chan struct{})
	q := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
	scale := &defaultAutoScaler{
		scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
			time.Hour, 0, scaleLatencyWindowCapacity),
		AutoScalerConfig:  InitAutoScalerConfig(),
		scalingWorkersNum: 0,
		ScaleInfo: algorithm.ScaleInfo{
			CurrentConcurrencyPerWorker: 100,
		},
		requestQue:          q,
		funcKeyWithResource: funcKeyWithResource,
	}
	{
		scale.QueueLengthChanged(1, 0, common.TraceInfo{}, 0, queue.BackPressuredOption{})
		num := scale.scalingWorkersNum
		assert.Equal(t, 1, num)
		scale.scalingWorkersNum = 0
	}
	{
		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		q.Push(req)
		scale.addToBackPressureInstance(&types.Instance{InstanceID: "instanceA", OwnerIP: "OwnerA"})
		scale.QueueLengthChanged(1, 0, common.TraceInfo{}, 0, queue.BackPressuredOption{})
		num := scale.scalingWorkersNum
		assert.Equal(t, 0, num)
		resp := <-req.Response
		assert.Equal(t, resp.ErrorCode, uint32(constants.BackpressureCode))
		assert.Equal(t, resp.InstanceID, "instanceA")
		assert.Equal(t, resp.NodeID, "OwnerA")
		scale.backPressureInstance = BackPressureInstance{}
	}
	{
		// scale when long time no scaling and no scheduled
		scale.lastScalingTime = time.Now().Add(-10 * time.Second)
		scale.WorkerTPS = 100
		scale.ScalingLatencyMs = 1000
		scale.UsedWorkerNum = 1
		scale.lastScaleCalculationTime = time.Now().Add(-2 * time.Second)
		scale.QueueLengthChanged(1, 1, common.TraceInfo{}, 0, queue.BackPressuredOption{})
		num := scale.scalingWorkersNum
		assert.Equal(t, 0, num)

		scale.lastScaleCalculationTime = time.Now().Add(-2 * time.Second)
		scale.QueueLengthChanged(1, 1, common.TraceInfo{}, 10*time.Second, queue.BackPressuredOption{})
		num = scale.scalingWorkersNum
		assert.Equal(t, 1, num)
	}
}

func TestAutoScalerConfigHotLoad(t *testing.T) {
	convey.Convey("TestAutoScalerConfigHotLoad", t, func() {
		scaler := &defaultAutoScaler{
			AutoScalerConfig: InitAutoScalerConfig(),
		}
		common.FConfig.AutoScalerConfig.MaxUserTolerateDelayMs = 1001
		common.FConfig.AutoScalerConfig.BurstScaleLimitPerFunction = 11
		convey.So(scaler.MaxUserTolerateDelayMs, convey.ShouldEqual, 1001)
		convey.So(scaler.BurstScaleLimitPerFunction, convey.ShouldEqual, 11)
	})
}

func TestQueueLengthZero(t *testing.T) {
	p := NewPatches()
	patchedSetup(p)
	defer patches.ResetAll()

	scale := &defaultAutoScaler{
		scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
			time.Hour, 0, scaleLatencyWindowCapacity),
		AutoScalerConfig:  InitAutoScalerConfig(),
		scalingWorkersNum: 0,
		ScaleInfo: algorithm.ScaleInfo{
			CurrentConcurrencyPerWorker: 100,
		},
	}
	scale.QueueLengthChanged(0, 0, common.TraceInfo{}, 0, queue.BackPressuredOption{IsBackPressured: true})
	assert.Equal(t, false, scale.burst)
}

func TestScaleUpGotInnerUserErr(t *testing.T) {
	data := "{\"httpsConfig\":{\"httpsEnable\":true},\"localAuth\":{\"localAuthEnable\":true}}"
	err := json.Unmarshal([]byte(data), common.FConfig)
	if err != nil {
		fmt.Println(err)
		return
	}
	worker := &action.Worker{}
	instance := &types.Instance{
		IP:           "127.0.0.1",
		Port:         "8080",
		InstanceID:   "instanceID",
		DeployedIP:   "127.0.0.1",
		DeployedNode: "127.0.0.1",
		TenantID:     "tenantID",
		Version:      "latest",
		IsReserved:   false,
	}
	patches := NewPatches()
	patches.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
		return "auth", "10000000"
	})
	patches.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
		return &http.Client{Timeout: 600000}
	})
	patches.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
		scaleDownResp := action.GetWorkerSuccessResponse{
			Code:     snerror.InnerRuntimeInitTimeoutCode,
			Worker:   worker,
			Instance: instance,
			Message:  "runtime initialization timed out after 60s",
		}
		content, err := json.Marshal(scaleDownResp)
		if err != nil {
			return nil, errors.New("request error")
		}
		res := &http.Response{
			Body:       ioutil.NopCloser(bytes.NewReader(content)),
			StatusCode: http.StatusOK,
		}
		return res, nil
	})
	patches.ApplyFunc((*workerbroker.Manager).CreateWorkerBroker, func(mgr *workerbroker.Manager, config *workerbroker.BrokerConfig) error {
		return nil
	})
	defer patches.Reset()
	nodeIP := internal.SelfNodeIP
	internal.SelfNodeIP = "127.0.0.1"
	stopCh := make(chan struct{})
	close(stopCh)

	tests := []struct {
		name          string
		usedWorkerNum int
		expectRsp     bool
	}{
		{
			name:          "used worker num 0",
			usedWorkerNum: 0,
			expectRsp:     true,
		},
		{
			name:          "used worker num 1",
			usedWorkerNum: 1,
			expectRsp:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)

			req := &types.DispatcherRequest{
				Request:  &types.TCPRequest{},
				Response: make(chan *types.TCPResponse, 1),
			}

			requestQueue.Push(req)
			scale := &defaultAutoScaler{
				requestQue: requestQueue,
				scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
					time.Hour, 0, scaleLatencyWindowCapacity),
				AutoScalerConfig:    InitAutoScalerConfig(),
				scalingWorkersNum:   1,
				funcKeyWithResource: "tenantID/hello/latest/400-256",
				ScaleInfo: algorithm.ScaleInfo{
					CurrentConcurrencyPerWorker: 100,
					UsedWorkerNum:               tt.usedWorkerNum,
				},
			}
			ins := scale.scaleUp(common.TraceInfo{}, scale.CurrentConcurrencyPerWorker, scale.scalingWorkersNum, false)
			assert.NotNil(t, ins)

			select {
			case resp := <-req.Response:
				if !tt.expectRsp {
					assert.Fail(t, "expected no response", tt.name)
				} else {
					assert.Equal(t, uint32(snerror.RuntimeInitTimeoutCode), resp.ErrorCode)
					assert.True(t, strings.HasPrefix(resp.ErrorMessage, "runtime initialization timed out after"))
				}
			default:
				if tt.expectRsp {
					assert.Fail(t, "no response", tt.name)
				}
			}
		})
	}
	internal.SelfNodeIP = nodeIP
}

func TestScaleUp(t *testing.T) {
	data := "{\"httpsConfig\":{\"httpsEnable\":true},\"localAuth\":{\"localAuthEnable\":true}}"
	err := json.Unmarshal([]byte(data), common.FConfig)
	if err != nil {
		fmt.Println(err)
		return
	}
	worker := &action.Worker{}
	instance := &types.Instance{
		IP:           "127.0.0.1",
		Port:         "8080",
		InstanceID:   "instanceID",
		DeployedIP:   "127.0.0.1",
		DeployedNode: "127.0.0.1",
		TenantID:     "tenantID",
		Version:      "latest",
		IsReserved:   false,
	}
	convey.Convey("TestScaleUp", t, func() {
		convey.Convey("success", func() {
			patches := NewPatches()
			patches.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "auth", "10000000"
			})
			patches.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return &http.Client{Timeout: 600000}
			})
			patches.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
				scaleDownResp := action.GetWorkerSuccessResponse{
					Code:     150200,
					Worker:   worker,
					Instance: instance,
					Message:  "success",
				}
				content, err := json.Marshal(scaleDownResp)
				if err != nil {
					return nil, errors.New("request error")
				}
				res := &http.Response{
					Body:       ioutil.NopCloser(bytes.NewReader(content)),
					StatusCode: http.StatusOK,
				}
				return res, nil
			})
			patches.ApplyFunc((*workerbroker.Manager).CreateWorkerBroker, func(mgr *workerbroker.Manager, config *workerbroker.BrokerConfig) error {
				return nil
			})
			defer patches.Reset()
			nodeIP := internal.SelfNodeIP
			internal.SelfNodeIP = "127.0.0.1"
			stopCh := make(chan struct{})
			requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
			scale := &defaultAutoScaler{
				requestQue: requestQueue,
				scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
					time.Hour, 0, scaleLatencyWindowCapacity),
				AutoScalerConfig:    InitAutoScalerConfig(),
				scalingWorkersNum:   1,
				funcKeyWithResource: "tenantID/hello/latest/400-256",
				ScaleInfo: algorithm.ScaleInfo{
					CurrentConcurrencyPerWorker: 100,
					UsedWorkerNum:               1,
				},
			}
			ins := scale.scaleUp(common.TraceInfo{}, scale.CurrentConcurrencyPerWorker, scale.scalingWorkersNum, false)
			if ins == nil {
				scale.Lock()
				scale.scalingWorkersNum--
				scale.Unlock()
				return
			}
			err = scale.workerScaledUp(ins)
			scale.Lock()
			scale.scalingWorkersNum--
			scale.Unlock()
			convey.So(err, convey.ShouldBeNil)
			internal.SelfNodeIP = nodeIP
		})
		convey.Convey("fail", func() {
			patches := NewPatches()
			patches.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "auth", "10000000"
			})
			patches.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return &http.Client{Timeout: 600000}
			})
			patches.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
				return nil, errors.New("request error")
			})
			defer patches.Reset()
			stopCh := make(chan struct{})
			requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
			scale := &defaultAutoScaler{
				requestQue: requestQueue,
				scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
					time.Hour, 0, scaleLatencyWindowCapacity),
				AutoScalerConfig:    InitAutoScalerConfig(),
				scalingWorkersNum:   1,
				funcKeyWithResource: "tenantID/hello/latest/400-256",
			}
			ins := scale.scaleUp(common.TraceInfo{}, scale.CurrentConcurrencyPerWorker, scale.scalingWorkersNum, false)
			if ins == nil {
				scale.Lock()
				scale.scalingWorkersNum--
				scale.Unlock()
				return
			}
			scale.workerScaledUp(ins)
			scale.Lock()
			scale.scalingWorkersNum--
			scale.Unlock()
		})
		convey.Convey("scaling number: scale failed", func() {
			patches := NewPatches()
			patches.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "auth", "10000000"
			})
			patches.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return &http.Client{Timeout: 600000}
			})
			patches.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
				return nil, errors.New("request error")
			})
			defer patches.Reset()
			stopCh := make(chan struct{})
			requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
			scale := &defaultAutoScaler{
				requestQue: requestQueue,
				scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
					time.Hour, 0, scaleLatencyWindowCapacity),
				AutoScalerConfig:    InitAutoScalerConfig(),
				scalingWorkersNum:   1,
				funcKeyWithResource: "tenantID/hello/latest/400-256",
			}
			// scale up failed
			scale.retryToScaleUp(common.TraceInfo{})
			convey.So(scale.scalingWorkersNum, convey.ShouldEqual, 0)
		})
		convey.Convey("scaling number: scale successfully but create worker failed", func() {
			broken := false
			patches := NewPatches()
			patches.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "auth", "10000000"
			})
			patches.ApplyMethod(reflect.TypeOf(workerbroker.GetInstance()), "CreateWorkerBroker", func(_ *workerbroker.Manager, _ *workerbroker.BrokerConfig) error {
				return errors.New("failed to create broker")
			})
			patches.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return &http.Client{Timeout: 600000}
			})
			patches.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
				scaleDownResp := action.GetWorkerSuccessResponse{
					Code:     150200,
					Worker:   worker,
					Instance: instance,
					Message:  "success",
				}
				content, err := json.Marshal(scaleDownResp)
				if err != nil {
					return nil, errors.New("request error")
				}
				res := &http.Response{
					Body:       ioutil.NopCloser(bytes.NewReader(content)),
					StatusCode: http.StatusOK,
				}
				return res, nil
			})
			patches.ApplyFunc(action.ScaleDown, func(instanceID, functionKey string, isBrokenConnection bool) (bool, uint32, string) {
				broken = isBrokenConnection
				return false, 0, ""
			})
			defer patches.Reset()
			notifyRemote := common.NotifyRemote
			common.NotifyRemote = func(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error {
				return errors.New("error")
			}
			stopCh := make(chan struct{})
			requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
			scale := &defaultAutoScaler{
				requestQue: requestQueue,
				scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
					time.Hour, 0, scaleLatencyWindowCapacity),
				AutoScalerConfig:    InitAutoScalerConfig(),
				scalingWorkersNum:   1,
				funcKeyWithResource: "tenantID/hello/latest/400-256",
			}
			nodeIP := internal.SelfNodeIP
			internal.SelfNodeIP = "127.0.0.1"
			defer func() {
				internal.SelfNodeIP = nodeIP
			}()
			// scale up failed
			scale.retryToScaleUp(common.TraceInfo{})
			convey.So(scale.scalingWorkersNum, convey.ShouldEqual, 0)
			// scale up failed should force scale down the instance
			convey.So(broken, convey.ShouldEqual, true)
			common.NotifyRemote = notifyRemote
		})
		convey.Convey("scaling number: scale successfully but create remote worker failed", func() {
			patches := NewPatches()
			patches.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
				return "auth", "10000000"
			})
			patches.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return &http.Client{Timeout: 600000}
			})
			patches.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
				scaleDownResp := action.GetWorkerSuccessResponse{
					Code:     150200,
					Worker:   worker,
					Instance: instance,
					Message:  "success",
				}
				content, err := json.Marshal(scaleDownResp)
				if err != nil {
					return nil, errors.New("request error")
				}
				res := &http.Response{
					Body:       ioutil.NopCloser(bytes.NewReader(content)),
					StatusCode: http.StatusOK,
				}
				return res, nil
			})
			patches.ApplyFunc(action.ScaleDown, func(instanceID, functionKey string, isBrokenConnection bool) (bool, uint32, string) {
				return false, 0, ""
			})
			defer patches.Reset()
			notifyRemote := common.NotifyRemote
			common.NotifyRemote = func(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error {
				return errors.New("error")
			}
			stopCh := make(chan struct{})
			requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
			scale := &defaultAutoScaler{
				requestQue: requestQueue,
				scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
					time.Hour, 0, scaleLatencyWindowCapacity),
				AutoScalerConfig:    InitAutoScalerConfig(),
				scalingWorkersNum:   1,
				funcKeyWithResource: "tenantID/hello/latest/400-256",
			}
			// scale up failed
			scale.retryToScaleUp(common.TraceInfo{})
			convey.So(scale.scalingWorkersNum, convey.ShouldEqual, 0)
			common.NotifyRemote = notifyRemote
		})
	})
}

func TestCalculateScaleDown(t *testing.T) {
	stopCh := make(chan struct{})
	requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
	meta := servicediscovery.FunctionMetaInfo{}
	meta.InstanceMetaData.ConcurrentNum = 1
	scale := NewDefaultAutoScaler("tenantID/funcName/funcVersion", requestQueue, meta)
	scale.SetFuncMeta(meta)
	scale.UpdateUsedWorkerNum(1)
	scale.(*defaultAutoScaler).AutoScalerConfig = InitAutoScalerConfig()
	num := scale.CalculateScaleDown(0, 0)
	assert.Equal(t, 1, num)
	close(stopCh)
}

func TestCalculateScaleUpNum(t *testing.T) {
	{
		scale := &defaultAutoScaler{
			scalingWorkersNum: 0,
			scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
				time.Hour, 0, scaleLatencyWindowCapacity),
			AutoScalerConfig: InitAutoScalerConfig(),
			ScaleInfo: algorithm.ScaleInfo{CurrentConcurrencyPerWorker: 100,
				RequestLatencyMs: 100,
				WorkerTPS:        10,
				PendingRequests:  1,
			},
		}
		num := scale.calculateScaleUpNum(true)
		assert.Equal(t, 1, num)
	}
	{
		scale := &defaultAutoScaler{
			scalingWorkersNum: 0,
			scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
				time.Hour, 0, scaleLatencyWindowCapacity),
			AutoScalerConfig: InitAutoScalerConfig(),
			ScaleInfo: algorithm.ScaleInfo{CurrentConcurrencyPerWorker: 100,
				RequestLatencyMs: 2,
				WorkerTPS:        50000,
				PendingRequests:  1,
			},
		}
		num := scale.calculateScaleUpNum(false)
		assert.Equal(t, 1, num)
	}
	{
		scale := &defaultAutoScaler{
			scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
				time.Hour, 0, scaleLatencyWindowCapacity),
			AutoScalerConfig:  InitAutoScalerConfig(),
			scalingWorkersNum: 0,
			ScaleInfo: algorithm.ScaleInfo{
				CurrentConcurrencyPerWorker: 100,
				PendingRequests:             1,
			},
		}
		num := scale.calculateScaleUpNum(true)
		assert.Equal(t, 1, num)
	}
	{
		scale := &defaultAutoScaler{
			scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
				time.Hour, 0, scaleLatencyWindowCapacity),
			AutoScalerConfig:  InitAutoScalerConfig(),
			scalingWorkersNum: 0,
			ScaleInfo: algorithm.ScaleInfo{
				CurrentConcurrencyPerWorker: 100,
				RequestLatencyMs:            1000,
				WorkerTPS:                   100,
				RPS:                         120,
				PendingRequests:             200,
			},
		}
		scale.IgnorePendingRequests = true
		num := scale.calculateScaleUpNum(true)
		assert.Equal(t, 2, num)
	}
	{
		scale := &defaultAutoScaler{
			scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
				time.Hour, 0, scaleLatencyWindowCapacity),
			AutoScalerConfig:  InitAutoScalerConfig(),
			scalingWorkersNum: 0,
			ScaleInfo: algorithm.ScaleInfo{
				CurrentConcurrencyPerWorker: 600,
				RequestLatencyMs:            1000,
				WorkerTPS:                   60,
				RPS:                         60,
				PendingRequests:             136,
			},
		}
		num := scale.calculateScaleUpNum(true)
		assert.Equal(t, 1, num)
	}
}

func TestBackPressure(t *testing.T) {
	scale := &defaultAutoScaler{
		scaleLatencyAggregation: dataaggregation.NewDataAggregation(dataaggregation.AVERAGE,
			time.Hour, 0, scaleLatencyWindowCapacity),
		AutoScalerConfig:  InitAutoScalerConfig(),
		scalingWorkersNum: 0,
		ScaleInfo: algorithm.ScaleInfo{
			CurrentConcurrencyPerWorker: 100,
			UsedWorkerNum:               0,
		},
	}
	scale.addToBackPressureInstance(&types.Instance{InstanceID: "instanceA", OwnerIP: "OwnerA"})
	assert.Equal(t, true, scale.checkBackPressure())

	scale.backPressureInstance.timestamp = time.Now().Add(-2 * time.Second)
	assert.Equal(t, false, scale.checkBackPressure())

	scale.backPressureInstance.timestamp = time.Now()
	scale.scalingWorkersNum = 1
	assert.Equal(t, false, scale.checkBackPressure())

	scale.backPressureInstance.timestamp = time.Now()
	scale.scalingWorkersNum = 0
	scale.UsedWorkerNum = 1
	assert.Equal(t, true, scale.checkBackPressure())
}

func TestDecodeFromErrMSG(t *testing.T) {
	errCode := uint32(1)
	errMsg := "test"
	_, _, result3 := snerror.DecodeFromErrMSG(errCode, errMsg)
	assert.Equal(t, "test", result3)

	errCode = snerror.RuntimeInitTimeoutCode
	errMsg = "test"
	_, _, result4 := snerror.DecodeFromErrMSG(errCode, errMsg)
	assert.Equal(t, "test", result4)
}

func Test_defaultAutoScaler_breakBalanceCalculate(t *testing.T) {
	oneMinuteAgo, _ := time.ParseDuration("-60s")
	oneSecondAgo, _ := time.ParseDuration("-1s")
	type fields struct {
		AutoScalerConfig               *common.AutoScalerConfig
		requestQue                     *queue.RequestQueue
		scaleLatencyAggregation        dataaggregation.DataAggregation
		functionKey                    string
		funcKeyWithResource            string
		scalingWorkersNum              int
		backPressureInstance           BackPressureInstance
		burst                          bool
		lastScaleCalculationTime       time.Time
		lastScalingTime                time.Time
		ScaleInfo                      algorithm.ScaleInfo
		Mutex                          sync.Mutex
		firstBreakBalanceCalculateTime time.Time
		breakBalanceCalculateCount     int
	}
	type args struct {
		queueLength int
		now         time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"test firstBreakBalanceCalculateTime is halfMinuteAgo",
			fields{
				firstBreakBalanceCalculateTime: time.Now().Add(oneMinuteAgo),
			},
			args{
				now: time.Now(),
			},
		},
		{
			"test firstBreakBalanceCalculateTime is not halfMinuteAgo",
			fields{
				AutoScalerConfig: &common.AutoScalerConfig{
					BreakBalanceQueueLengthLimit: 20,
				},
				firstBreakBalanceCalculateTime: time.Now().Add(oneSecondAgo),
			},
			args{
				now: time.Now(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			das := &defaultAutoScaler{
				AutoScalerConfig:               tt.fields.AutoScalerConfig,
				requestQue:                     tt.fields.requestQue,
				scaleLatencyAggregation:        tt.fields.scaleLatencyAggregation,
				functionKey:                    tt.fields.functionKey,
				funcKeyWithResource:            tt.fields.funcKeyWithResource,
				scalingWorkersNum:              tt.fields.scalingWorkersNum,
				backPressureInstance:           tt.fields.backPressureInstance,
				burst:                          tt.fields.burst,
				lastScaleCalculationTime:       tt.fields.lastScaleCalculationTime,
				lastScalingTime:                tt.fields.lastScalingTime,
				ScaleInfo:                      tt.fields.ScaleInfo,
				Mutex:                          tt.fields.Mutex,
				firstBreakBalanceCalculateTime: tt.fields.firstBreakBalanceCalculateTime,
				breakBalanceCalculateCount:     tt.fields.breakBalanceCalculateCount,
			}
			das.breakBalanceCalculate(tt.args.queueLength, tt.args.now)
		})
	}
}

func Test_defaultAutoScaler_refreshBreakBalanceCalculate(t *testing.T) {
	type fields struct {
		AutoScalerConfig               *common.AutoScalerConfig
		requestQue                     *queue.RequestQueue
		scaleLatencyAggregation        dataaggregation.DataAggregation
		functionKey                    string
		funcKeyWithResource            string
		scalingWorkersNum              int
		backPressureInstance           BackPressureInstance
		burst                          bool
		lastScaleCalculationTime       time.Time
		lastScalingTime                time.Time
		ScaleInfo                      algorithm.ScaleInfo
		Mutex                          sync.Mutex
		firstBreakBalanceCalculateTime time.Time
		breakBalanceCalculateCount     int
	}
	type args struct {
		now time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test refresh break balance calculate",
			fields{},
			args{
				now: time.Now(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			das := &defaultAutoScaler{
				AutoScalerConfig:               tt.fields.AutoScalerConfig,
				requestQue:                     tt.fields.requestQue,
				scaleLatencyAggregation:        tt.fields.scaleLatencyAggregation,
				functionKey:                    tt.fields.functionKey,
				funcKeyWithResource:            tt.fields.funcKeyWithResource,
				scalingWorkersNum:              tt.fields.scalingWorkersNum,
				backPressureInstance:           tt.fields.backPressureInstance,
				burst:                          tt.fields.burst,
				lastScaleCalculationTime:       tt.fields.lastScaleCalculationTime,
				lastScalingTime:                tt.fields.lastScalingTime,
				ScaleInfo:                      tt.fields.ScaleInfo,
				Mutex:                          tt.fields.Mutex,
				firstBreakBalanceCalculateTime: tt.fields.firstBreakBalanceCalculateTime,
				breakBalanceCalculateCount:     tt.fields.breakBalanceCalculateCount,
			}
			das.refreshBreakBalanceCalculate(tt.args.now)
		})
	}
}

func Test_defaultAutoScaler_needBreakBalanceScaleUp(t *testing.T) {
	oneMinuteAgo, _ := time.ParseDuration("-60s")
	oneSecondAgo, _ := time.ParseDuration("-1s")
	type fields struct {
		AutoScalerConfig               *common.AutoScalerConfig
		requestQue                     *queue.RequestQueue
		scaleLatencyAggregation        dataaggregation.DataAggregation
		functionKey                    string
		funcKeyWithResource            string
		scalingWorkersNum              int
		backPressureInstance           BackPressureInstance
		burst                          bool
		lastScaleCalculationTime       time.Time
		lastScalingTime                time.Time
		ScaleInfo                      algorithm.ScaleInfo
		Mutex                          sync.Mutex
		firstBreakBalanceCalculateTime time.Time
		breakBalanceCalculateCount     int
	}
	type args struct {
		queuedTime time.Duration
		now        time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			"test need break balance scale up by queueTime",
			fields{
				firstBreakBalanceCalculateTime: time.Now().Add(oneSecondAgo),
				AutoScalerConfig: &common.AutoScalerConfig{
					MaxEvaluatedLatencyMs: 2,
				},
			},
			args{
				queuedTime: 10 * time.Second,
				now:        time.Now(),
			},
			true,
		},
		{
			"test need break balance scale up by break calculate",
			fields{
				firstBreakBalanceCalculateTime: time.Now().Add(oneMinuteAgo),
				breakBalanceCalculateCount:     25,
				AutoScalerConfig: &common.AutoScalerConfig{
					MaxEvaluatedLatencyMs: 2,
				},
			},
			args{
				queuedTime: time.Millisecond,
				now:        time.Now(),
			},
			true,
		},
		{
			"test not need break balance",
			fields{
				firstBreakBalanceCalculateTime: time.Now().Add(oneMinuteAgo),
				breakBalanceCalculateCount:     0,
				AutoScalerConfig: &common.AutoScalerConfig{
					MaxEvaluatedLatencyMs: 2,
				},
			},
			args{
				queuedTime: time.Millisecond,
				now:        time.Now(),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			das := &defaultAutoScaler{
				AutoScalerConfig:               tt.fields.AutoScalerConfig,
				requestQue:                     tt.fields.requestQue,
				scaleLatencyAggregation:        tt.fields.scaleLatencyAggregation,
				functionKey:                    tt.fields.functionKey,
				funcKeyWithResource:            tt.fields.funcKeyWithResource,
				scalingWorkersNum:              tt.fields.scalingWorkersNum,
				backPressureInstance:           tt.fields.backPressureInstance,
				burst:                          tt.fields.burst,
				lastScaleCalculationTime:       tt.fields.lastScaleCalculationTime,
				lastScalingTime:                tt.fields.lastScalingTime,
				ScaleInfo:                      tt.fields.ScaleInfo,
				Mutex:                          tt.fields.Mutex,
				firstBreakBalanceCalculateTime: tt.fields.firstBreakBalanceCalculateTime,
				breakBalanceCalculateCount:     tt.fields.breakBalanceCalculateCount,
			}
			assert.Equalf(t, tt.want, das.needBreakBalanceScaleUp(tt.args.queuedTime, tt.args.now), "needBreakBalanceScaleUp(%v, %v)", tt.args.queuedTime, tt.args.now)
		})
	}
}

func Test_getScaleTimeout(t *testing.T) {
	tests := []struct {
		name     string
		meta     servicediscovery.FunctionMetaInfo
		expected time.Duration
	}{
		{
			name:     "empty meta",
			expected: defaultScaleTimeout,
		},
		{
			name:     "custom container",
			meta:     servicediscovery.FunctionMetaInfo{FuncMetaData: servicediscovery.FuncMetaData{Timeout: 10, IsCustomContainer: true}},
			expected: defaultScaleTimeout,
		},
		{
			name: "normal func",
			meta: servicediscovery.FunctionMetaInfo{
				FuncMetaData:     servicediscovery.FuncMetaData{Timeout: 30},
				ExtendedMetaData: servicediscovery.ExtendedMetaData{Initializer: servicediscovery.Initializer{InitializerTimeout: 15}},
			},
			expected: (30 + 15 + scaleDelay) * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, getScaleTimeout(tt.meta), tt.name)
		})
	}
}

func TestQueueLengthChanged_MicroserviceAutoScaler(t *testing.T) {
	EnableMicroserviceAutoScaler = true
	p := NewPatches()
	patchedSetup(p)
	defer patches.ResetAll()

	stopCh := make(chan struct{})
	q := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
	autoScaler := &microserviceAutoScaler{
		scaler: &defaultAutoScaler{
			AutoScalerConfig:  InitAutoScalerConfig(),
			scalingWorkersNum: 0,
			ScaleInfo: algorithm.ScaleInfo{
				CurrentConcurrencyPerWorker: 100,
			},
			requestQue:          q,
			funcKeyWithResource: funcKeyWithResource,
		},
	}
	{
		autoScaler.QueueLengthChanged(1, 0, common.TraceInfo{}, 0, queue.BackPressuredOption{})
		num := autoScaler.scaler.scalingWorkersNum
		assert.Equal(t, 1, num)
		autoScaler.scaler.scalingWorkersNum = 0
	}
	{
		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		q.Push(req)
		autoScaler.scaler.addToBackPressureInstance(&types.Instance{InstanceID: "instanceA", OwnerIP: "OwnerA"})
		autoScaler.QueueLengthChanged(1, 0, common.TraceInfo{}, 0, queue.BackPressuredOption{})
		num := autoScaler.scaler.scalingWorkersNum
		assert.Equal(t, 0, num)
		resp := <-req.Response
		assert.Equal(t, resp.ErrorCode, uint32(constants.BackpressureCode))
		assert.Equal(t, resp.InstanceID, "instanceA")
		assert.Equal(t, resp.NodeID, "OwnerA")
		autoScaler.scaler.backPressureInstance = BackPressureInstance{}
	}
	{
		// scale when long time no scaling and no scheduled
		autoScaler.scaler.lastScalingTime = time.Now().Add(-10 * time.Second)
		autoScaler.scaler.WorkerTPS = 100
		autoScaler.scaler.ScalingLatencyMs = 1000
		autoScaler.scaler.UsedWorkerNum = 1
		autoScaler.scaler.lastScaleCalculationTime = time.Now().Add(-2 * time.Second)
		autoScaler.QueueLengthChanged(1, 1, common.TraceInfo{}, 0, queue.BackPressuredOption{})
		num := autoScaler.scaler.scalingWorkersNum
		assert.Equal(t, 0, num)

		autoScaler.scaler.lastScaleCalculationTime = time.Now().Add(-2 * time.Second)
		autoScaler.QueueLengthChanged(1, 1, common.TraceInfo{}, 10*time.Second, queue.BackPressuredOption{})
		num = autoScaler.scaler.scalingWorkersNum
		assert.Equal(t, 0, num)
	}
}

func TestSetFuncMeta_MicroserviceAutoScaler(t *testing.T) {
	autoScaler := &microserviceAutoScaler{
		scaler: &defaultAutoScaler{},
	}
	metaData := servicediscovery.FunctionMetaInfo{
		FuncMetaData: servicediscovery.FuncMetaData{Timeout: 0},
	}
	autoScaler.SetFuncMeta(metaData)
	assert.Equal(t, defaultScaleTimeout, autoScaler.scaler.scaleTimeout)
}

func TestCheckUsedWorker_DefaultAutoScaler(t *testing.T) {
	das := &defaultAutoScaler{}
	res := das.CheckUsedWorker()
	assert.Equal(t, false, res)
	das.UpdateUsedWorkerNum(10)
	res = das.CheckUsedWorker()
	assert.Equal(t, true, res)
}

func TestReset_DefaultAutoScaler(t *testing.T) {
	das := &defaultAutoScaler{}
	das.lastScaleCalculationTime = time.Now()
	das.Reset()
	assert.Equal(t, time.Time{}, das.lastScaleCalculationTime)
}

func TestCheckUsedWorker_MicroServiceAutoScaler(t *testing.T) {
	mas := &microserviceAutoScaler{
		scaler: &defaultAutoScaler{},
	}
	res := mas.CheckUsedWorker()
	assert.Equal(t, false, res)
	mas.UpdateUsedWorkerNum(10)
	res = mas.CheckUsedWorker()
	assert.Equal(t, false, res)
	assert.Equal(t, 0, mas.CalculateScaleDown(1, 2))
}

func TestMicroServiceAutoScaler_CalculateScaleDown(t *testing.T) {
	stopCh := make(chan struct{})
	requestQueue := queue.NewRequestQueue("fifo", internal.RequestQueueTimeout, stopCh)
	meta := servicediscovery.FunctionMetaInfo{}
	meta.InstanceMetaData.ConcurrentNum = 1
	scale := NewMicroServiceAutoScaler("tenantID/funcName/funcVersion", requestQueue, meta)
	scale.SetFuncMeta(meta)
	scale.UpdateUsedWorkerNum(1)
	num := scale.CalculateScaleDown(0, 0)
	assert.Equal(t, 1, num)
	close(stopCh)
}
