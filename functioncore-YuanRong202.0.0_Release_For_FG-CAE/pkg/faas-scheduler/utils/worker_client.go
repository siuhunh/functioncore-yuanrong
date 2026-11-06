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

// Package utils worker http client for faas scheduler
package utils

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/faas-scheduler/config"
)

const (
	workerClientCallAttempts = 1
	workerClientDialTimeout  = 10 * time.Second
	// worker maximum generalization timeout 33 seconds + function maximum initialization timeout 300 seconds
	workerClientReadTimeout  = 333 * time.Second
	defaultWorkerMetricsTick = 60
	signAppID                = "faas-scheduler"
	buildWebSocketConPath    = "/wss_connect"
	countWebSocketConPath    = "/wss_count"
	countWebSocketConTimeout = 10 * time.Second
)

var (
	workerFhttpClientOnce sync.Once
	workerFhttpClient     *fasthttp.Client
)

func getOrCreateWorkerClient() *fasthttp.Client {
	workerFhttpClientOnce.Do(func() {
		workerFhttpClient = &fasthttp.Client{
			ReadTimeout:               workerClientReadTimeout,
			MaxIdemponentCallAttempts: workerClientCallAttempts,
			Dial: func(addr string) (conn net.Conn, e error) {
				return fasthttp.DialTimeout(addr, workerClientDialTimeout)
			},
		}
		if workerFhttpClient != nil && config.FConfig.HTTPSConfig.HTTPSEnable {
			tlsConfig := *tls.GetClientTLSConfig()
			tlsConfig.NextProtos = []string{"http/1.1"}
			workerFhttpClient.TLSConfig = &tlsConfig
		}
	})
	return workerFhttpClient
}

func initRequestToWorker(podIP, podPort, requestPath string, reqBody []byte) *fasthttp.Request {
	// create a new request to worker
	req := fasthttp.AcquireRequest()
	// update path and scheme
	host := podIP + ":" + podPort
	url := "//" + host + requestPath
	req.URI().Update(url)
	if config.FConfig.HTTPSConfig.HTTPSEnable {
		req.URI().SetScheme("https")
	} else {
		req.URI().SetScheme("http")
	}
	req.Header.SetHost(host)
	authorization, timestamp := localauth.SignLocally(
		config.FConfig.LocalAuth.AKey,
		config.FConfig.LocalAuth.SKey,
		signAppID,
		config.FConfig.LocalAuth.Duration,
	)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.Authorization, authorization)
	req.Header.Set(constants.HeaderMetricsTick, strconv.Itoa(defaultWorkerMetricsTick))
	req.SetBody(reqBody)
	return req
}

// SendBuildWebSocketConRequestToWorker send request to worker from faas-scheduler
func SendBuildWebSocketConRequestToWorker(podIP, podPort, traceID string,
	reqBody []byte, scalingOutTimeout time.Duration) error {
	req := initRequestToWorker(podIP, podPort, buildWebSocketConPath, reqBody)
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, traceID)
	req.Header.SetConnectionClose()
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(response)
	log.GetLogger().Infof("%s | podIP: %s | uri: %s | start to send websocket connection building request "+
		"to worker", traceID, podIP, req.URI().String())
	workerClient := getOrCreateWorkerClient()
	err := workerClient.DoTimeout(req, response, scalingOutTimeout)
	if err != nil {
		log.GetLogger().Errorf("failed to send request of building websocket connection "+
			"request to worker,  error %s", err.Error())
		return err
	}
	snErr := &snerror.BadResponse{}
	if response.Body() != nil && len(response.Body()) != 0 {
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker")
			return errors.New("failed to unmarshal the response body (%s) from worker")
		}
	}
	if response.StatusCode() != fasthttp.StatusOK {
		log.GetLogger().Errorf("failed to send request of building websocket connection request to worker,  "+
			"Status Code %d, errCode: %d, errMessage: %s", response.StatusCode(), snErr.Code, snErr.Message)
		return snerror.New(snErr.Code, snErr.Message)
	}
	log.GetLogger().Infof("%s | podIP: %s | uri: %s | statusCode: %d | succeed to send websocket connection "+
		"building request to worker", traceID, podIP, req.URI().String(), response.StatusCode())
	return nil
}

// PrepareBuildWebSocketConRequestBody prepare the request body between faas-scheduler and worker
// when establishing the websocket connection between worker and rdispatcher
func PrepareBuildWebSocketConRequestBody(req *schedulerapi.AcquireInstanceReq) ([]byte, error) {
	funcKeyWithoutRes, _, _ := urnutils.GetFuncKeyAndResFromFuncKeyWithResource(req.FuncKeyWithRes)
	reqObj := schedulerapi.CreateWebSocketConnectionReq{
		FunctionKey:     funcKeyWithoutRes,
		WebSocketConfig: req.WebSocketInsAcquireConfig,
	}
	reqBody, err := json.Marshal(reqObj)
	if err != nil {
		return nil, err
	}
	return reqBody, nil
}

// SendCountWebSocketConRequestToWorker send request to worker from faas-scheduler to query connection status
func SendCountWebSocketConRequestToWorker(podIP, podPort, traceID string) (uint32, uint32, error) {
	req := initRequestToWorker(podIP, podPort, countWebSocketConPath, []byte(""))
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, traceID)
	req.Header.SetConnectionClose()
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(response)
	log.GetLogger().Infof("%s | podIP: %s | uri: %s | start to send websocket connection counting request "+
		"to worker", traceID, podIP, req.URI().String())
	workerClient := getOrCreateWorkerClient()
	err := workerClient.DoTimeout(req, response, countWebSocketConTimeout)
	if err != nil {
		log.GetLogger().Errorf("failed to send request of counting websocket connection "+
			"request to worker,  error %s", err.Error())
		return 0, 0, err
	}
	if response.StatusCode() != fasthttp.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("traceID: %s | failed to unmarshal the response body from worker", traceID)
			return 0, 0, errors.New("failed to unmarshal the response body from worker when counting connections")
		}
		log.GetLogger().Errorf("failed to send request of counting websocket connection request to worker,  "+
			"Status Code %d, errCode: %d, errMessage: %s", response.StatusCode(), snErr.Code, snErr.Message)
		return 0, 0, errors.New(snErr.Message)
	}
	resp := &schedulerapi.WorkerWssCountResponse{}
	err = json.Unmarshal(response.Body(), resp)
	if err != nil {
		log.GetLogger().Errorf("traceID: %s | failed to unmarshal the response body from worker", traceID)
		return 0, 0, errors.New("failed to unmarshal the response body from worker when counting connections")
	}
	log.GetLogger().Infof("%s | podIP: %s | uri: %s | total: %d | ws: %d | succeed to send websocket "+
		"connection counting request to worker", traceID, podIP, req.URI().String(), resp.TotalConnNum, resp.WssConnNum)
	return resp.TotalConnNum, resp.WssConnNum, nil
}
