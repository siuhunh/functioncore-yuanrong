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

// Package vpa busproxy change the vpa
package vpa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commonutil "functioncore/pkg/common/utils"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

const (
	busProxyResourceURI = "/vpa/create"

	defaultRetryTimes = 3
	maxSleepTime      = 10
)

// ClientOption -
type ClientOption func(*Client)

// Client defines client basic info
type Client struct {
	client         *http.Client
	requestTimeout time.Duration
}

// WithRequestTimeout -
func WithRequestTimeout(requestTimeout time.Duration) ClientOption {
	return func(c *Client) {
		c.requestTimeout = requestTimeout
	}
}

func newHTTPRequest(opts ...ClientOption) *Client {
	var c = &Client{
		client: utils.GetBusProxyClient(),
	}
	for _, opt := range opts {
		opt(c)
	}

	return c
}

// SendFuncVpaReqToBus define the basic req from worker-manager to bux proxy
func SendFuncVpaReqToBus(action string, pod *corev1.Pod, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) snerror.SNError {
	if !utils.IsVpaFeatureEnabled() {
		return nil
	}

	containerInfo := &vpa.ContainerInfo{}
	for _, v := range pod.Status.ContainerStatuses {
		if v.Name == utils.WorkerContainerName {
			containerInfo.Worker = getTrimPrefixContainerID(v.ContainerID)
		} else if v.Name == utils.RuntimeContainerName {
			containerInfo.Runtime = getTrimPrefixContainerID(v.ContainerID)
		}
	}

	runtimePoolResource, workerPoolResource := getRuntimeAndWorkerPoolResource(pod)
	resource, err := utils.GetPoolResource(reqInfo.CPU, reqInfo.Memory, service.Spec.Configuration.BusinessType)
	if err != nil {
		log.GetLogger().Warnf("failed to get pool resource, traceID: %s, cpu: %d, mem: %d, error: %s",
			reqInfo.TraceID, reqInfo.CPU, reqInfo.Memory, err.Error())
		return snerror.New(snerror.VPAWorkerManagerError, "failed to get pool resource")
	}
	runtimeInitResource, workerInitResource := getRuntimeAndWorkerInitResource(resource)

	funcNameWithVersion := reqInfo.FunctionName
	if !isFuncNameContainFuncVersion(funcNameWithVersion) && reqInfo.FunctionVersion != "" {
		funcNameWithVersion = fmt.Sprintf("%s:%s", funcNameWithVersion, reqInfo.FunctionVersion)
	}

	funcReqSource := &vpa.FuncInstanceInfo{
		Action:              action,
		FuncName:            funcNameWithVersion,
		BusinessType:        service.Spec.Configuration.BusinessType,
		PodName:             constants.DefaultAZ + "-#-" + pod.Name,
		PodUID:              string(pod.ObjectMeta.UID),
		ContainerIDs:        containerInfo,
		Resource:            runtimeInitResource,
		RuntimeInitResource: runtimeInitResource,
		WorkerInitResource:  workerInitResource,
		RuntimePoolResource: runtimePoolResource,
		WorkerPoolResource:  workerPoolResource,
		HostIP:              pod.Status.HostIP,
	}

	log.GetLogger().Infof("begin to deal func vpa req, action: %s, funcName: %s, host: %s, isCaeFunc: %t, podName: %s,"+
		" traceId: %s", action, funcNameWithVersion, pod.Status.HostIP, commonutil.IsCAEFunc(funcReqSource.BusinessType),
		pod.Name, reqInfo.TraceID)
	req := newHTTPRequest()
	return req.dealFuncVpaReq(funcReqSource, service, reqInfo.TraceID)
}

func getRuntimeAndWorkerPoolResource(pod *corev1.Pod) (runtimePoolResource, workerPoolResource *vpa.ResourceConfig) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == utils.RuntimeContainerName {
			runtimePoolResource = getResourceConfigAsInt64(pod.Spec.Containers[i].Resources)
		} else if pod.Spec.Containers[i].Name == utils.WorkerContainerName {
			workerPoolResource = getResourceConfigAsInt64(pod.Spec.Containers[i].Resources)
		}
	}
	return runtimePoolResource, workerPoolResource
}

func getResourceConfigAsInt64(resource corev1.ResourceRequirements) *vpa.ResourceConfig {
	return &vpa.ResourceConfig{
		CPULimit:      resource.Limits.Cpu().MilliValue(),
		CPURequest:    resource.Requests.Cpu().MilliValue(),
		MemoryLimit:   resource.Limits.Memory().Value(),
		MemoryRequest: resource.Requests.Memory().Value(),
	}
}

func getRuntimeAndWorkerInitResource(resource utils.PoolResource) (runtimeInitResource,
	workerInitResource *vpa.ResourceConfig) {
	runtimeInitResource = &vpa.ResourceConfig{
		CPURequest:    resource.Runtime.RequestCPU,
		CPULimit:      resource.Runtime.LimitCPU,
		MemoryRequest: resource.Runtime.RequestMem << utils.MiToByteExponent,
		MemoryLimit:   resource.Runtime.LimitMem << utils.MiToByteExponent,
	}

	workerInitResource = &vpa.ResourceConfig{
		CPURequest:    resource.Worker.RequestCPU,
		CPULimit:      resource.Worker.LimitCPU,
		MemoryRequest: resource.Worker.RequestMem << utils.MiToByteExponent,
		MemoryLimit:   resource.Worker.LimitMem << utils.MiToByteExponent,
	}
	return runtimeInitResource, workerInitResource
}

func getTrimPrefixContainerID(withPrefixID string) string {
	containerID := strings.TrimPrefix(withPrefixID, "docker://")
	containerID = strings.TrimPrefix(containerID, "containerd://")
	return containerID
}

func (c *Client) dealFuncVpaReq(requestInfo *vpa.FuncInstanceInfo, service *faasfunction.FunctionService,
	traceID string) snerror.SNError {
	config.InitFuncInstResource.LoadOrStore(requestInfo.FuncName, config.Resource{
		CPU:    requestInfo.Resource.CPURequest,
		Memory: requestInfo.Resource.MemoryRequest,
	})

	var err snerror.SNError
	if requestInfo.Action == constants.VPACreate {
		for i := 0; i < defaultRetryTimes; i++ {
			err = c.doFuncVpaReq(requestInfo, traceID)
			if err == nil {
				return nil
			}
		}
		log.GetLogger().Errorf("failed to send func resource to bus in create phase with retry %d times, "+
			"traceID: %s, error: %s", defaultRetryTimes, traceID, err.Error())
		return snerror.New(err.Code(), "failed deal to communicate with busproxy")
	}

	if requestInfo.Action == constants.VPAReady {
		go c.retryInformerBusProxyUntilSuccess(requestInfo, service, traceID)
	}

	return nil
}

func (c *Client) retryInformerBusProxyUntilSuccess(requestInfo *vpa.FuncInstanceInfo,
	service *faasfunction.FunctionService, traceID string) {
	retryTimes := 0

	for {
		err := c.doFuncVpaReq(requestInfo, traceID)
		if err == nil {
			log.GetLogger().Infof("success to send func: %s resource to bus in ready phase, traceID: %s",
				requestInfo.FuncName, traceID)
			return
		}
		log.GetLogger().Errorf("failed to send func: %s resource to bus in ready phase with retry %d times, "+
			"traceID: %s, error: %s", requestInfo.FuncName, retryTimes, traceID, err.Error())
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		sleepTime := r.Intn(maxSleepTime)
		time.Sleep(time.Duration(sleepTime) * time.Second)
		retryTimes++

		if retryTimes >= defaultRetryTimes {
			// after three retries failed, check whether the function instance is deleted
			podSuffixName := strings.TrimPrefix(requestInfo.PodName, constants.DefaultAZ+"-#-")
			_, found := cache.WorkerCache.GetWorkerInstance(service.ID, podSuffixName)
			if !found {
				log.GetLogger().Warnf("get funcSvcId by cache: %s is empty, it may be deleted, podName: %s, "+
					"traceID: %s", service.ID, requestInfo.PodName, traceID)
				break
			}

			log.GetLogger().Debugf("get func: %s is success, but commouncation with bus failed, traceID: %s",
				requestInfo.FuncName, traceID)
		}
	}
}

func (c *Client) doFuncVpaReq(requestInfo *vpa.FuncInstanceInfo, traceID string) snerror.SNError {
	request, err := makeFuncResourceRequest(context.TODO(), requestInfo, traceID)
	if err != nil {
		log.GetLogger().Errorf("failed to get func: %s resource, traceID: %s, error: %s", requestInfo.FuncName,
			traceID, err.Error())
		return snerror.New(snerror.VPAWorkerManagerError, err.Error())
	}

	resp, err := c.client.Do(request)
	if err != nil {
		log.GetLogger().Errorf("failed to send funcName: %s request to worker manager, traceID: %s, error: %s,",
			requestInfo.FuncName, traceID, err.Error())
		return snerror.New(snerror.VPACommunicationError, err.Error())
	}

	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to get func %s response body, traceID: %s, error: %s",
			requestInfo.FuncName, traceID, err.Error())
		return snerror.New(snerror.VPACommunicationError, err.Error())
	}
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode == http.StatusInternalServerError {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal func: %s response body, traceID: %s, error: %s",
				requestInfo.FuncName, traceID, err.Error())
			return snerror.New(snerror.VPACommunicationError, err.Error())
		}
		return snerror.New(snerror.VPACommunicationError, snErr.Message)
	}
	log.GetLogger().Errorf("funcName: %s receive bus proxy error statusCode: %d, traceID: %s, error body: %s",
		requestInfo.FuncName, resp.StatusCode, traceID, respBody)
	return snerror.New(snerror.VPACommunicationError, "failed to receive bus proxy response")
}

func makeFuncResourceRequest(ctx context.Context, requestInfo *vpa.FuncInstanceInfo, traceID string) (
	*http.Request, error) {
	requestURI := utils.GetBusProxyBaseURL(requestInfo.HostIP) + busProxyResourceURI
	data, err := json.Marshal(requestInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal funcInfoReq, traceID: %s, error: %s", traceID, err.Error())
		return nil, err
	}
	log.GetLogger().Debugf("send func request to busproxy with body: %s, traceID: %s", string(data), traceID)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("action failed when make vpa req, traceID: %s, err %s", traceID, err.Error())
		return nil, err
	}
	req.Header.Set(constants.ContentType, constants.ApplicationJSON)
	req.Header.Set(constants.HeaderTraceID, traceID)
	authConfig := config.GetLocalAuth()
	if authConfig == nil {
		return req, nil
	}
	authorization, timestamp := localauth.SignLocally(authConfig.AKey, authConfig.SKey, utils.AppID, authConfig.Duration)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.Authorization, authorization)

	log.GetLogger().Infof("succeeded to sign the authorization of function: %s, podName: %s, traceID: %s",
		requestInfo.FuncName, requestInfo.PodName, traceID)
	return req, nil
}

func isFuncNameContainFuncVersion(funcName string) bool {
	for i := len(funcName) - 1; i >= 0; i-- {
		if funcName[i] == ':' {
			return true
		}
		if funcName[i] == '@' {
			return false
		}
	}
	return false
}
