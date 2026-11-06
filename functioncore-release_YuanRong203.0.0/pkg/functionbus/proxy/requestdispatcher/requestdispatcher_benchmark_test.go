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

// Package requestdispatcher contains request lb, autoscaler and so on
package requestdispatcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/rand"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
)

const (
	functionKey    = "d0bxxxxa60/0@lxy@lxy_long_run_960s/latest/-"
	instNamePrefix = "defaultaz-#-pool22-300-128-fusion"
)

func BenchmarkRequests10ConcurrencyPerWorker(b *testing.B) {
	wg := new(sync.WaitGroup)

	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "auth", "10000000"
		}),
		gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return &http.Client{Timeout: 600000}
		}),
		gomonkey.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response, error) {
			duration := time.Duration(rand.Intn(15)) * time.Second
			b.Logf("scale instance costs: %v", duration)
			print("scale instance costs: ")
			println(duration)
			time.Sleep(duration)
			scaleResp := action.GetWorkerSuccessResponse{
				Code:    150200,
				Worker:  &action.Worker{},
				Message: "success",
				Instance: &types.Instance{IP: "127.0.0.1", Port: "10000", DeployedIP: internal.SelfNodeIP,
					InstanceID: instNamePrefix + uuid.New().String(), OwnerIP: internal.SelfNodeIP},
			}
			content, err := json.Marshal(scaleResp)
			if err != nil {
				return nil, errors.New("request error")
			}
			res := &http.Response{
				Body:       ioutil.NopCloser(bytes.NewReader(content)),
				StatusCode: http.StatusOK,
			}
			return res, nil
		}),
		gomonkey.ApplyFunc((*workerbroker.Manager).CreateWorkerBroker, func(mgr *workerbroker.Manager, cfg *workerbroker.BrokerConfig) error {
			workerevaluator.GetInstance().AddWorker(cfg.WorkerID,
				workerevaluator.WithFunctionKeyWithResource(cfg.FuncKeyWithResource),
				workerevaluator.WithNodeIP(cfg.RemoteIP),
				workerevaluator.WithLocalWorker(cfg.IsMinInstance),
				workerevaluator.WithRecoveredRequestNum(0),
				workerevaluator.WithStateID(""),
			)
			return nil
		}),
		gomonkey.ApplyFunc((*workerbroker.Manager).Process, func(mgr *workerbroker.Manager, workerID string, request *types.DispatcherRequest) {
			go func() {
				duration := time.Duration(10) * time.Millisecond
				time.Sleep(duration)
				workerevaluator.GetInstance().RecordRequestEnd(workerID, duration)
				request.Response <- &types.TCPResponse{}
				wg.Done()
			}()
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	metadata := servicediscovery.FunctionMetaInfo{}
	metadata.InstanceMetaData = servicediscovery.InstanceMetaData{
		ConcurrentNum: 10,
		IdleMode:      false,
	}
	dispatcher := CreateRequestDispatcher(functionKey, metadata, false)
	dispatcher.Start()

	config := &workerbroker.BrokerConfig{
		FuncKeyWithResource: functionKey,
		WorkerID:            "defaultaz-#-pool22-300-128-fusion-674d66499d-sh662",
		WorkerAddr:          "127.0.0.1",
		RemoteIP:            internal.SelfNodeIP,
		IsMinInstance:       true,
		StateID:             "",
	}
	workerbroker.GetInstance().CreateWorkerBroker(config)

	startTime := time.Now()

	b.Run("group1", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			handleRequest(wg, dispatcher, pb, b)
		})
	})

	wg.Wait()
	b.Logf("all requests finished cost: %v", time.Since(startTime))
	print("all request finished cost: ")
	println(time.Since(startTime))
	dispatcher.Stop()
	b.Log("Stop dispatcher success")
}

func setupRequest() *types.DispatcherRequest {
	internalReq := &types.TCPRequest{
		LogType:         "",
		RawData:         nil,
		RequestID:       0,
		UserData:        map[string]string{},
		Priority:        0,
		DirectRequest:   true,
		CanBackpressure: false,
		CPU:             "",
		Memory:          "",
		Concurrency:     10,
		TraceCtx:        nil,
		RetryFlag:       false,
		PersistResponse: false,
		StateID:         "",
	}
	return &types.DispatcherRequest{
		Request:     internalReq,
		Response:    make(chan *types.TCPResponse, 1),
		Position:    types.Position{},
		FunctionKey: functionKey,
		TraceID:     uuid.New().String(),
		NodeLabel:   "",
		FutureID:    "",
		Canceled:    false,
		WorkerID:    "",
	}
}
func handleRequest(wg *sync.WaitGroup, rd *RequestDispatcher, pb *testing.PB, b *testing.B) {
	for pb.Next() {
		wg.Add(1)
		req := setupRequest()
		//		b.Logf("request started for traceID: %s", req.TraceID)
		rd.Request(req)
	}
}
