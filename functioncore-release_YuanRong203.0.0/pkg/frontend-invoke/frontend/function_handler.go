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

// Package frontend deal with function handler
package frontend

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/contentsignaturecheck"
	"functioncore/pkg/common/httputils"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/snerror"
	commonStream "functioncore/pkg/common/stream"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/metrics"
	"functioncore/pkg/frontend-invoke/model"
	status "functioncore/pkg/frontend-invoke/statuscode"
	"functioncore/pkg/frontend-invoke/stream"
	"functioncore/pkg/frontend-invoke/trafficlimit"
	"functioncore/pkg/frontend-invoke/util"
)

var (
	graceExit = false
	clusterID = os.Getenv("CLUSTER_ID")
	// ErrHeavyLoad is error when frontend is under heavy memory pressure
	ErrHeavyLoad = errors.New("http server is under heavy load")

	busErrorCodes     = []int{210401, 210500, 211406, 211408, 211410, 213429, 214500, 214503}
	workerErrorPrefix = "1619"

	podName = os.Getenv(constants.HostNameEnvKey)
	nodeIP  = os.Getenv("NODE_IP")
)

const (
	// the minimum length split of authValue
	minLenOfSplit = 2
	base          = 10
	bitSize       = 64

	apigEventBodyPath                  = "path"
	apigEventBodyQueryStringParameters = "queryStringParameters"

	maxTryReleaseNum = 3
	tryReleaseTime   = 100 * time.Millisecond

	instanceInex = 1
)

type innerCode int

// String convert innerCode to error type string
func (ic innerCode) String() string {
	if ic < 0 {
		return "invalidCode"
	}
	if ic == 0 {
		return "sysOK"
	}
	if ic < snerror.UserErrorMax {
		return "usrErr"
	}
	return "sysErr"
}

type traceInfo struct {
	CallInstance string
	CallNode     string
	busName      string
	totalCost    time.Duration
	frontendCost time.Duration
	busCost      time.Duration
	workerCost   time.Duration
}

func invokeFilter(c *gin.Context, start time.Time) *model.FunctionRequestInfo {
	traceID := utils.InitTraceID(c.Request)
	reqInfo := &model.FunctionRequestInfo{TraceID: traceID, StartTime: start}
	bodySizeErr := util.PermittedRequest(c.Request)
	if bodySizeErr != nil {
		setLogAndSendResponse(status.InternalError, bodySizeErr.Code(), bodySizeErr, reqInfo, c.Writer)
		log.GetLogger().Errorf("the size of request body is out of range, traceID:%s", reqInfo.TraceID)
		return nil
	}

	err := processRequest(c, reqInfo)
	if err != nil {
		setLogAndSendResponse(status.BadRequest, status.BadRequest, err, reqInfo, c.Writer)
		log.GetLogger().Errorf("err when process http request, traceID %s: %s", reqInfo.TraceID, err.Error())
		return nil
	}
	if limitTraffic(reqInfo, c.Writer) {
		log.GetLogger().Errorf("reach traffic limit, traceID: %s", reqInfo.TraceID)
		return nil
	}
	return reqInfo
}

// InvokeHandler invoke handler
func InvokeHandler(c *gin.Context) {
	start := time.Now()
	reqInfo := invokeFilter(c, start)
	if reqInfo == nil {
		return
	}
	memoryWant := uint64(float64(getRequestContentLength(c.Request)) *
		constant.FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator)
	if !monitor.IsAllowByMemory(reqInfo.URN, memoryWant, reqInfo.TraceID) {
		log.GetLogger().Errorf("http server is under heavy load, traceID: %s", reqInfo.TraceID)
		setLogAndSendResponse(status.HeavyLoadCode, status.InternalError, ErrHeavyLoad, reqInfo, c.Writer)
		return
	}
	defer monitor.GetMemInstance().ReleaseFunctionMem(reqInfo.URN, memoryWant)
	if requestAuthCheck(c.Request, c.Writer, reqInfo) {
		return
	}
	if !util.IsHTTPStream(c.Request) {
		commonInvokeHandler(c, reqInfo, start)
		return
	}
	httpStreamInvokeHandler(c, reqInfo, start)
	return
}

func httpStreamInvokeHandler(c *gin.Context, reqInfo *model.FunctionRequestInfo, start time.Time) {
	log.GetLogger().Infof("the request of %s is a http stream request", reqInfo.TraceID)
	if !constant.FConfig.DataSystem.StreamEnable {
		errStr := fmt.Sprintf("not enable to support http stream request %s", reqInfo.TraceID)
		log.GetLogger().Warn(errStr)
		setLogAndSendResponse(status.InternalError, status.HTTPStreamNOTEnableError,
			errInter, reqInfo, c.Writer)
		return
	}
	if err := processHTTPStreamRequest(c, reqInfo); err != nil {
		return
	}
	commonInvokeHandler(c, reqInfo, start)
}

func commonInvokeHandler(c *gin.Context, reqInfo *model.FunctionRequestInfo, start time.Time) {
	req := fhttp.AcquireRequest()
	defer fhttp.ReleaseRequest(req)
	readBodyStart := time.Now()
	if err := transferHTTPRequestToFastHTTP(req, c.Request); err != nil {
		log.GetLogger().Errorf("transferHttpRequestToFastHTTP failed, traceID: %s, funcName: %s, err: %s",
			reqInfo.TraceID, reqInfo.Name, err.Error())
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	readBodyTotal := time.Since(readBodyStart)
	resp := fhttp.AcquireResponse()
	defer fhttp.ReleaseResponse(resp)
	insResource, isCAEFunc := getInsResource(reqInfo, req)
	if isCAEFunc {
		if err := startListenFrontendResponseStream(reqInfo.TenantID); err != nil {
			log.GetLogger().Errorf("failed to listen frontend response stream, tenantID: %s err: %s",
				reqInfo.TenantID, err.Error())
			alarm.GetClient().ReportAlarm(alarm.StreamClient, fmt.Sprintf("frontend failed to "+
				"listen frontend response stream, tenantID=%s, error msg=%s", reqInfo.TenantID, err.Error()))
		}
	}
	setHeader(reqInfo, req, reqInfo.URN, insResource)
	if err := websocketFilter(reqInfo, req, insResource); err != nil {
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	responseStopChan := RegisterResponse(c.Writer, reqInfo)
	req.Header.Set(constants.HeaderResponseStreamName, reqInfo.ResponseStreamName)
	defer ReleaseResponse(reqInfo.ResponseStreamName)
	if err := functionlb.FunctionLB.DoRequest(reqInfo, req, resp, insResource); err != nil {
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	writeRspStart := time.Now()
	processResp(reqInfo, resp, c.Writer, responseStopChan)
	writeRspCost := time.Since(writeRspStart)
	checkContentSignature(c, req, resp, reqInfo)
	traceInfo := getTraceInfo(start, reqInfo.AllBusCost, resp)
	fillAfterInvokeHeader(c, traceInfo)
	setLogAndPublishMetrics(c, reqInfo, traceInfo, readBodyTotal, writeRspCost)
}

func processResp(reqInfo *model.FunctionRequestInfo, resp *fhttp.Response,
	writer gin.ResponseWriter, stopChan *StreamStopChan) {
	if stopChan == nil || !CheckIsResponseStream(reqInfo.ResponseStreamName) {
		requestStreamErrorCode := int(reqInfo.GetRequestStreamErrorCode())
		if requestStreamErrorCode != 0 {
			setLogAndSendResponse(status.InternalError, requestStreamErrorCode, errInter, reqInfo, writer)
			return
		}
		transferFastHTTPResponseToHTTP(reqInfo, resp, writer)
		return
	}
	// wait until the response stream is written.
	if _, ok := <-stopChan.C; !ok {
		log.GetLogger().Infof("received response stream stop signal of request %s, return", reqInfo.TraceID)
	}
}

func checkContentSignature(c *gin.Context, req *fhttp.Request,
	resp *fhttp.Response, reqInfo *model.FunctionRequestInfo) {
	if string(resp.Header.Peek(constants.HeaderInnerCode)) == strconv.Itoa(snerror.ContentInconsistentErrorCode) {
		contentsignaturecheck.CheckContentSignature(c.Request.Header.Get(constants.HeaderContentSignature),
			contentsignaturecheck.GetContentSignature(req.Body()), reqInfo.TraceID, "frontend")
	}
}

func getRequestContentLength(request *http.Request) int64 {
	contentLength := request.ContentLength
	if util.IsHTTPStream(request) && contentLength > httpStreamContentLength {
		contentLength = httpStreamContentLength
	}
	return contentLength
}

func fillAfterInvokeHeader(c *gin.Context, traceInfo *traceInfo) {
	if graceExit {
		c.Writer.Header().Set("Connection", "close")
	}
	c.Writer.Header().Add(constants.HeaderCallNode, traceInfo.CallNode)
	c.Writer.Header().Add(constants.HeaderCallInstance, traceInfo.CallInstance)
}

func addMetricsHeader(w http.ResponseWriter, reqInfo *model.FunctionRequestInfo, resp *fhttp.Response) {
	if reqInfo.InnerCode == 0 {
		return
	}
	w.Header().Add(constants.HeaderErrorCode, strconv.Itoa(reqInfo.InnerCode))
	callNode := string(resp.Header.Peek(constants.HeaderCallNode))
	busName := string(resp.Header.Peek(constants.HeaderPodName))
	callInstance := string(resp.Header.Peek(constants.HeaderCallInstance))
	strs := strings.Split(string(resp.Header.Peek(constants.HeaderCallInstance)), workerInstanceAZSep)
	if len(strs) == wantedLenOfSplit {
		callInstance = strs[instanceInex]
	}
	if (reqInfo.InnerCode < snerror.UserErrorMax) ||
		strings.HasPrefix(strconv.Itoa(reqInfo.InnerCode), workerErrorPrefix) {
		w.Header().Add(constants.HeaderErrorNode, callNode)
		w.Header().Add(constants.HeaderErrorPod, callInstance)
		return
	}
	if utils.ContainsKey(busErrorCodes, reqInfo.InnerCode) {
		w.Header().Add(constants.HeaderErrorNode, callNode)
		w.Header().Add(constants.HeaderErrorPod, busName)
	}
}

// WebSocketInstanceCreateHandler create websocket instance
func WebSocketInstanceCreateHandler(c *gin.Context) {
	start := time.Now()
	reqInfo := invokeFilter(c, start)
	if reqInfo == nil {
		return
	}
	memoryWant := uint64(float64(c.Request.ContentLength) *
		constant.FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator)
	if !monitor.IsAllowByMemory(reqInfo.URN, memoryWant, reqInfo.TraceID) {
		log.GetLogger().Errorf("http server is under heavy load, traceID: %s", reqInfo.TraceID)
		setLogAndSendResponse(status.HeavyLoadCode, status.InternalError, ErrHeavyLoad, reqInfo, c.Writer)
		return
	}
	defer monitor.GetMemInstance().ReleaseFunctionMem(reqInfo.URN, memoryWant)
	if requestAuthCheck(c.Request, c.Writer, reqInfo) {
		return
	}
	req := fhttp.AcquireRequest()
	defer fhttp.ReleaseRequest(req)
	readBodyStart := time.Now()
	if err := transferHTTPRequestToFastHTTP(req, c.Request); err != nil {
		log.GetLogger().Errorf(" trace id: %s | func name: %s | err: %s | transferHttpRequestToFastHTTP failed",
			reqInfo.TraceID, reqInfo.Name, err.Error())
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	readBodyTotal := time.Since(readBodyStart)
	resp := fhttp.AcquireResponse()
	defer fhttp.ReleaseResponse(resp)
	err := faasSchedulerAdapter(req, reqInfo, resp)
	if err != nil {
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	writeRspStart := time.Now()
	transferFastHTTPResponseToHTTP(reqInfo, resp, c.Writer)
	c.Writer.Header().Add(constants.HeaderTraceID, reqInfo.TraceID)
	writeRspCost := time.Since(writeRspStart)
	if string(resp.Header.Peek(constants.HeaderInnerCode)) == strconv.Itoa(snerror.ContentInconsistentErrorCode) {
		contentsignaturecheck.CheckContentSignature(c.Request.Header.Get(constants.HeaderContentSignature),
			contentsignaturecheck.GetContentSignature(req.Body()), reqInfo.TraceID, "frontend")
	}
	if graceExit {
		c.Writer.Header().Set("Connection", "close")
	}
	traceInfo := getTraceInfo(start, reqInfo.AllBusCost, resp)
	c.Writer.Header().Add(constants.HeaderCallNode, traceInfo.CallNode)
	c.Writer.Header().Add(constants.HeaderCallInstance, traceInfo.CallInstance)
	setLogAndPublishMetrics(c, reqInfo, traceInfo, readBodyTotal, writeRspCost)
	return
}

// WebSocketInstanceDeleteHandler delete websocket instance
func WebSocketInstanceDeleteHandler(c *gin.Context) {
	start := time.Now()
	reqInfo := invokeFilter(c, start)
	if reqInfo == nil {
		return
	}
	memoryWant := uint64(float64(c.Request.ContentLength) *
		constant.FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator)
	defer monitor.GetMemInstance().ReleaseFunctionMem(reqInfo.URN, memoryWant)
	if !monitor.IsAllowByMemory(reqInfo.URN, memoryWant, reqInfo.TraceID) {
		log.GetLogger().Errorf("http server is under heavy load, traceID: %s", reqInfo.TraceID)
		setLogAndSendResponse(status.HeavyLoadCode, status.InternalError, ErrHeavyLoad, reqInfo, c.Writer)
		return
	}
	if requestAuthCheck(c.Request, c.Writer, reqInfo) {
		return
	}
	req := fhttp.AcquireRequest()
	defer fhttp.ReleaseRequest(req)
	if err := transferHTTPRequestToFastHTTP(req, c.Request); err != nil {
		log.GetLogger().Errorf("transferHttpRequestToFastHTTP failed, traceID: %s, funcName: %s, err: %s",
			reqInfo.TraceID, reqInfo.Name, err.Error())
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	releaseInstanceInfo := &schedulerapi.ReleaseInstanceReqFromRdispatcher{}
	err := json.Unmarshal(req.Body(), releaseInstanceInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to Unmarshal releaseInstanceInfo, error: %s", err.Error())
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	insResource, _ := getInsResource(reqInfo, req)
	setHeader(reqInfo, req, reqInfo.URN, insResource)
	funcKey := urnutils.GetFuncKey(reqInfo.TenantID, reqInfo.Name, reqInfo.Version)
	funcKeyWithRes := urnutils.GetFuncKeyWithResource(funcKey, insResource.CPU, insResource.Memory)
	log.GetLogger().Infof("funcKeyWithRes: %s | functionInstanceKey: %s | processorID: %d | start to release "+
		"websocket instance", funcKey, releaseInstanceInfo.InstanceKey, releaseInstanceInfo.ProcessorID)
	err = doReleaseReq(funcKeyWithRes, reqInfo, releaseInstanceInfo)
	if err != nil {
		setLogAndSendResponse(status.InternalError, status.InternalError, err, reqInfo, c.Writer)
		return
	}
	log.GetLogger().Infof("%s | %s | %s | %.3f | %d | invokeEndReleasingWebsocketConn",
		urnutils.ExtractFuncName(reqInfo.Name), urnutils.Anonymize(reqInfo.TenantID), reqInfo.TraceID,
		utils.ConvertSecondToMillisecond(time.Since(start).Seconds()), c.Request.ContentLength)
}

func getStreamInstanceHandler(c *gin.Context) {
	if authCheck(c.Request) != nil {
		c.Writer.WriteHeader(http.StatusUnauthorized)
		return
	}
	instance, err := stream.GetRandomStreamsAddr()
	if err != nil {
		log.GetLogger().Errorf("failed to get stream nodes, err: %s", err.Error())
		c.Writer.WriteHeader(http.StatusInternalServerError)
		_, err := c.Writer.Write([]byte(err.Error()))
		if err != nil {
			log.GetLogger().Errorf("failed to write rsp err %s", err.Error())
		}
		return
	}
	c.Writer.Header().Add(constants.HeaderStreamAddr, instance)
	c.Writer.WriteHeader(http.StatusOK)
}

func streamInvokeHandler(c *gin.Context) {
	if authCheck(c.Request) != nil {
		c.Writer.WriteHeader(http.StatusUnauthorized)
		return
	}
	requestID := c.Request.Header.Get(constants.HeaderStreamRunID)
	plainURN := c.Param(functionURNParam)
	functionInfo, err := urnutils.GetFunctionInfo(plainURN)
	if err != nil {
		c.Writer.WriteHeader(http.StatusBadRequest)
		log.GetLogger().Errorf("err when process http request, traceID %s: %s", requestID, err.Error())
		return
	}
	token := c.Request.Header.Get(constants.HeaderStreamToken)
	streamHandlerAddr := c.Request.Header.Get(constants.HeaderStreamAddr)
	if requestID == "" || functionInfo.TenantID == "" || token == "" || streamHandlerAddr == "" {
		c.Writer.WriteHeader(http.StatusBadRequest)
		log.GetLogger().Errorf("invalid request param, traceID %s", requestID)
		return
	}
	streamTransPort := func() string {
		if constant.FConfig.StreamConfig.StreamTransport == "" {
			return constants.StreamTransPort
		}
		return constant.FConfig.StreamConfig.StreamTransport
	}

	ctx, cancel := context.WithTimeout(context.Background(), commonStream.DialTimeout)
	defer cancel()
	option := &commonStream.ClientOption{
		Writer:            c.Writer,
		DialCtx:           ctx,
		CliCtx:            c.Request.Context(),
		Token:             token,
		ProjectID:         functionInfo.TenantID,
		RequestID:         requestID,
		StreamHandlerAddr: streamHandlerAddr,
		StreamTransPort:   streamTransPort(),
	}
	if abort := commonStream.ReceiveStream(option); !abort {
		c.AbortWithStatus(http.StatusInternalServerError)
	}
}

func instancesRecoverHandler(c *gin.Context) {
	if authCheck(c.Request) != nil {
		c.Writer.WriteHeader(http.StatusUnauthorized)
		return
	}
	instancesRes := util.GetInstancesRecord()
	data, err := json.Marshal(instancesRes)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal response data: %s", err.Error())
		c.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	c.Writer.WriteHeader(http.StatusOK)
	_, err = c.Writer.Write(data)
	if err != nil {
		log.GetLogger().Warnf("failed to write response body: %s", err.Error())
	}
}

func doReleaseReq(funcKeyWithRes string,
	reqInfo *model.FunctionRequestInfo, releaseInstanceInfo *schedulerapi.ReleaseInstanceReqFromRdispatcher) error {
	scheduler := remotescheduler.GetInstance().GetScheduler(funcKeyWithRes)
	if scheduler == nil {
		log.GetLogger().Errorf("traceID: %s | funcKeyWithRes: %s | failed to get scheduler",
			reqInfo.TraceID, funcKeyWithRes)
		return errors.New("no faas-scheduler can be found")
	}
	releaseReq := &schedulerapi.ReleaseInstanceReq{
		TraceID:        reqInfo.TraceID,
		FuncKeyWithRes: funcKeyWithRes,
		InstanceKey:    releaseInstanceInfo.InstanceKey,
		ProcessorID:    releaseInstanceInfo.ProcessorID,
		IsWebSocketReq: true,
	}
	return releaseInstance(scheduler, releaseReq)
}

func releaseInstance(scheduler common.RPCClient, releaseReq *schedulerapi.ReleaseInstanceReq) error {
	var err error
	for i := 0; i < maxTryReleaseNum; i++ {
		err = scheduler.ReleaseInstance(releaseReq)
		if err == nil {
			return nil
		}

		time.Sleep(tryReleaseTime)
	}
	log.GetLogger().Errorf("traceID: %s | failed to release websocket instance for %d times, error: %s",
		releaseReq.TraceID, maxTryReleaseNum, err.Error())
	return err
}

func faasSchedulerAdapter(req *fhttp.Request, reqInfo *model.FunctionRequestInfo, resp *fhttp.Response) error {
	rdispatcherInfo := &schedulerapi.WebSocketInsAcquireConfig{}
	err := json.Unmarshal(req.Body(), rdispatcherInfo)
	if err != nil {
		log.GetLogger().Errorf("trace id: %s | failed to decode rdispatcher req body", reqInfo.TraceID)
		return err
	}
	insResource, _ := getInsResource(reqInfo, req)
	setHeader(reqInfo, req, reqInfo.URN, insResource)
	funcKey := urnutils.GetFuncKey(reqInfo.TenantID, reqInfo.Name, reqInfo.Version)
	funcKeyWithRes := urnutils.GetFuncKeyWithResource(funcKey, insResource.CPU, insResource.Memory)
	funcTimeout := getRequestDeadline(reqInfo)
	metaInfo, ok := metadata.LoadMetaData(reqInfo.Name, reqInfo.TenantID, reqInfo.Version)
	if !ok {
		log.GetLogger().Errorf("traceID: %s | failed to get funcMeta", reqInfo.TraceID)
		return errors.New("failed to get funcMeta")
	}
	if !utils.IsWebSocketFunc(metaInfo.FuncMetaDataParts.BusinessType, metaInfo.FuncMetaDataParts.EnableRemoteDebug,
		metaInfo.ExtendedMetaData.EnableWebsocket) {
		log.GetLogger().Errorf("traceID: %s | funcKeyWithRes: %s | is not enable websocket",
			reqInfo.TraceID, funcKeyWithRes)
		return snerror.New(snerror.WebSocketNotEnableError, snerror.ErrText(snerror.WebSocketNotEnableError))
	}
	scheduler := remotescheduler.GetInstance().GetScheduler(funcKeyWithRes)
	if scheduler == nil {
		log.GetLogger().Errorf("traceID: %s | funcKeyWithRes: %s | failed to get scheduler",
			reqInfo.TraceID, funcKeyWithRes)
		return errors.New("scheduler not found")
	}
	log.GetLogger().Infof("%s | funcKeyWithRes: %s | timeout: %s | start to acquire function instance",
		reqInfo.TraceID, funcKeyWithRes, funcTimeout.String())
	faasSchedulerReq := makeFaasSchedulerReq(reqInfo, rdispatcherInfo, funcKeyWithRes, funcTimeout, metaInfo)
	faasSchedulerResp, err := acquireInstance(scheduler, faasSchedulerReq)
	if err != nil {
		log.GetLogger().Errorf("frontend acquires instance error %s from faas scheduler", err.Error())
		Instance().ReleaseWebsocketWatcher(reqInfo.TraceID)
		return err
	}
	faasSchedulerResponseHandler(faasSchedulerResp, funcKeyWithRes, resp, reqInfo)
	return nil
}

// used for mock scheduler AcquireInstance function
func acquireInstance(
	scheduler common.RPCClient, req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	return scheduler.AcquireInstance(req)
}

func makeFaasSchedulerReq(reqInfo *model.FunctionRequestInfo, rdispatcherInfo *schedulerapi.WebSocketInsAcquireConfig,
	funcKeyWithRes string, funcTimeout time.Duration,
	funcMeta *metadata.FunctionMetaData) *schedulerapi.AcquireInstanceReq {
	targetIP := rdispatcherInfo.TargetIP
	targetPort := rdispatcherInfo.TargetPort
	if funcMeta.ExtendedMetaData.EnableWebsocket {
		targetIP = frontendPodIP
		targetPort = frontendWSPort
		webSocketWatcher := Instance().GetOrAddWebsocketWatcher(reqInfo.TraceID)
		webSocketWatcher.upstreamIP = rdispatcherInfo.TargetIP
		webSocketWatcher.upstreamPort = rdispatcherInfo.TargetPort
		funcKey := urnutils.GetFuncKey(reqInfo.TenantID, reqInfo.Name, reqInfo.Version)
		webSocketWatcher.FuncKey = funcKey
	}
	return &schedulerapi.AcquireInstanceReq{
		TraceID:        reqInfo.TraceID,
		FuncKeyWithRes: funcKeyWithRes,
		ReqType:        schedulerapi.WebSocketRequest,
		WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{
			TargetIP:   targetIP,
			TargetPort: targetPort,
			Path:       generateWsPath(rdispatcherInfo),
		},
		Timeout: funcTimeout.String(),
	}
}

func generateWsPath(rdispatcherInfo *schedulerapi.WebSocketInsAcquireConfig) string {
	wsPath := ""
	if rdispatcherInfo.RequestBody == nil {
		return wsPath
	}
	if path, ok := rdispatcherInfo.RequestBody[apigEventBodyPath]; ok {
		if strPath, ok := path.(string); ok {
			wsPath = strPath
		}
	}
	if queryStringParameters, ok := rdispatcherInfo.RequestBody[apigEventBodyQueryStringParameters]; ok {
		if parameters, ok := queryStringParameters.(map[string]interface{}); ok {
			if len(parameters) > 0 {
				queryString := generateQueryString(parameters)
				wsPath += "?" + queryString
			}
		}
	}
	return wsPath
}

func generateQueryString(parameters map[string]interface{}) string {
	queryURL := &url.URL{}
	queries := queryURL.Query()
	httputils.SetQuery(&queries, parameters)
	return queries.Encode()
}

func faasSchedulerResponseHandler(faasSchedulerResp *schedulerapi.AcquireInstanceRsp, funcKeyWithRes string,
	resp *fhttp.Response, reqInfo *model.FunctionRequestInfo) {
	if faasSchedulerResp.Error.ErrorCode != 0 {
		Instance().ReleaseWebsocketWatcher(reqInfo.TraceID)
		log.GetLogger().Errorf("funcKeyWithRes: %s | errCode: %d | errorMsg: %s | failed to acquire function "+
			"instance, traceID: %s", funcKeyWithRes, faasSchedulerResp.Error.ErrorCode,
			faasSchedulerResp.Error.ErrorMessage, reqInfo.TraceID)
		resp.SetStatusCode(http.StatusInternalServerError)
		resp.Header.Set(constants.HeaderInnerCode, strconv.FormatUint(uint64(faasSchedulerResp.Error.ErrorCode), base))
		respBody, err := json.Marshal(faasSchedulerResp.Error)
		if err != nil {
			log.GetLogger().Errorf("failed to marshal websocket error response, funcKeyWithRes: %s", funcKeyWithRes)
			resp.SetBody(generateWebsocketInternalError())
			return
		}
		resp.SetBody(respBody)
		return
	}
	log.GetLogger().Infof("%s | funcKeyWithRes: %s | instanceKey: %s | succeed to process websocket "+
		"acquiring request", reqInfo.TraceID, funcKeyWithRes, faasSchedulerResp.InstanceKey)
	respBody, err := json.Marshal(faasSchedulerResp)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal websocket response, funcKeyWithRes: %s, traceID: %s",
			funcKeyWithRes, reqInfo.TraceID)
		resp.SetBody(generateWebsocketInternalError())
		return
	}
	resp.SetBody(respBody)
}

func generateWebsocketInternalError() []byte {
	websocketInternalErrorJSON, err := json.Marshal(snerror.New(snerror.WebSocketRequestInternalError,
		snerror.ErrText(snerror.WebSocketRequestInternalError)))
	if err != nil {
		log.GetLogger().Errorf("failed to marshal websocket internal error")
		return make([]byte, 0)
	}
	return websocketInternalErrorJSON
}

func setLogAndPublishMetrics(c *gin.Context, reqInfo *model.FunctionRequestInfo,
	traceInfo *traceInfo, readBodyTotal time.Duration, writeRspCost time.Duration) {
	// funcName|tenantID|traceID|cost|frontendCost:busCost:workerCost:readBodyCost|contentLength|nodeIP|workerPod|
	// innerCode|ErrMsg
	log.GetLogger().Infof("%s | %s | %s | %.3f | %.3f:%.3f:%.3f:%.3f:%.3f | %d | %s | %s | innerCode %d | %s | "+
		"invokeEndFrontend", urnutils.ExtractFuncName(reqInfo.Name), reqInfo.TenantID,
		reqInfo.TraceID, utils.ConvertSecondToMillisecond(traceInfo.totalCost.Seconds()),
		utils.ConvertSecondToMillisecond(traceInfo.frontendCost.Seconds()),
		utils.ConvertSecondToMillisecond(traceInfo.busCost.Seconds()),
		utils.ConvertSecondToMillisecond(traceInfo.workerCost.Seconds()),
		utils.ConvertSecondToMillisecond(readBodyTotal.Seconds()),
		utils.ConvertSecondToMillisecond(writeRspCost.Seconds()), c.Request.ContentLength, traceInfo.CallNode,
		urnutils.ExtractPodName(traceInfo.CallInstance), reqInfo.InnerCode, innerCode(reqInfo.InnerCode).String())

	metrics.PublishMetrics(metrics.RequestStatistics{ErrorFlag: reqInfo.InnerCode != 0,
		TotalDelay: traceInfo.totalCost.Seconds(), FrontendDelay: traceInfo.frontendCost.Seconds(),
		BusDelay: traceInfo.busCost.Seconds(), RuntimeDelay: traceInfo.workerCost.Seconds()})
}

func getTraceInfo(start time.Time, allBusCost time.Duration, resp *fhttp.Response) *traceInfo {
	traceInfo := &traceInfo{}
	traceInfo.CallNode = string(resp.Header.Peek(constants.HeaderCallNode))
	traceInfo.CallInstance = string(resp.Header.Peek(constants.HeaderCallInstance))

	traceInfo.totalCost = time.Since(start)
	traceInfo.frontendCost = traceInfo.totalCost - allBusCost
	workerCost := string(resp.Header.Peek(constants.HeaderWorkerCost))
	if len(workerCost) != 0 {
		v, err := strconv.ParseInt(workerCost, base, bitSize)
		if err != nil {
			log.GetLogger().Errorf("failed to ParseInt, workerCost %s", workerCost)
		} else {
			traceInfo.workerCost = time.Duration(v) * time.Nanosecond
		}
	}
	traceInfo.busCost = allBusCost - traceInfo.workerCost
	traceInfo.busName = string(resp.Header.Peek(constants.HeaderPodName))
	return traceInfo
}

func transferHTTPRequestToFastHTTP(req *fhttp.Request, r *http.Request) error {
	for key := range r.Header {
		req.Header.Set(key, r.Header.Get(key))
	}
	var err error
	if r.Header.Get(constants.HeaderRequestStreamName) != "" && r.Header.Get(constants.HeaderStreamAPIGEvent) != "" {
		streamEvent := r.Header.Get(constants.HeaderStreamAPIGEvent)
		body := []byte(streamEvent)
		req.Header.Set(constants.ContentType, constants.ApplicationJSON)
		req.Header.Set(constants.ContentLength, strconv.Itoa(len(body)))
		req.SetBody(body)
	} else {
		err = req.ReadBody(bufio.NewReader(r.Body), int(r.ContentLength),
			constant.FConfig.HTTPConfig.MaxRequestBodySize)
		closeErr := r.Body.Close()
		if closeErr != nil {
			log.GetLogger().Warnf("body close failed %s", closeErr.Error())
		}
	}
	return err
}

func transferFastHTTPResponseToHTTP(reqInfo *model.FunctionRequestInfo, resp *fhttp.Response, w http.ResponseWriter) {
	w.Header().Set(constants.ContentType, constants.ApplicationJSON)
	if reqInfo.InnerCode == status.HeavyLoadCode {
		w.Header().Set(constants.HeaderInnerCode, strconv.Itoa(status.InternalError))
	} else {
		w.Header().Set(constants.HeaderInnerCode, string(resp.Header.Peek(constants.HeaderInnerCode)))
	}
	w.Header().Add(constants.HeaderLogResult, string(resp.Header.Peek(constants.HeaderLogResult)))
	w.Header().Add(constants.HeaderInvokeSummary, string(resp.Header.Peek(constants.HeaderInvokeSummary)))
	w.Header().Add(constants.HeaderBillingDuration, string(resp.Header.Peek(constants.HeaderBillingDuration)))
	w.Header().Add(constants.HeaderExecuteNode, string(resp.Header.Peek(constants.HeaderCallNode)))
	if len(string(resp.Header.Peek(constants.HeaderCffHTTPResponse))) != 0 {
		w.Header().Add(constants.HeaderCffHTTPResponse, string(resp.Header.Peek(constants.HeaderCffHTTPResponse)))
	}
	addMetricsHeader(w, reqInfo, resp)
	w.WriteHeader(resp.StatusCode())
	err := resp.BodyWriteTo(w)
	if err != nil {
		log.GetLogger().Errorf("failed to write the response, traceID: %s, error: %s", reqInfo.TraceID, err.Error())
	}
}

func setLogAndSendResponse(internalCode int, errorCode int, err error, reqInfo *model.FunctionRequestInfo,
	w http.ResponseWriter) {
	if strings.Compare(functionlb.ErrNoNodeLabelUnavailable.Error(), err.Error()) == 0 {
		errorCode = status.LabelUnavailable
	}
	snErr, ok := err.(snerror.SNError)
	if ok {
		if snErr.Code() == snerror.NoSuchInstanceName {
			errorCode = snerror.NoSuchInstanceName
		}
		if snErr.Code() == snerror.WebSocketNotEnableError {
			errorCode = snerror.WebSocketNotEnableError
		}
	}
	sendResponseAndWriteLog(status.Code(internalCode), errorCode, err.Error(), reqInfo, w)
}

// limitTraffic runs traffic limitation for tenant
func limitTraffic(reqInfo *model.FunctionRequestInfo, w http.ResponseWriter) bool {
	if err := trafficlimit.TenantTrafficLimit(reqInfo.TenantID); err != nil {
		sendResponseAndWriteLog(http.StatusInternalServerError, status.TrafficLimitEffective, err.Error(), reqInfo, w)
		log.GetLogger().Warn(err.Error())
		return true
	}
	return false
}

// requestAuthCheck check auth for request
func requestAuthCheck(r *http.Request, w http.ResponseWriter, reqInfo *model.FunctionRequestInfo) bool {
	err := authCheck(r)
	if err != nil {
		sendResponseAndWriteLog(http.StatusUnauthorized, status.UnAuthorized, status.Message(status.BadRequest),
			reqInfo, w)
		log.GetLogger().Errorf("failed to authenticate request, traceID %s: %s", reqInfo.TraceID, err.Error())
		return true
	}
	return false
}

// getURNWithVersion initializes the URN and the version
func getURNWithVersion(version string, plainURN string) (string, string) {
	var newURN string
	if version == "" {
		version = "latest"

		newURN = plainURN + urnutils.URNSep + version
	} else {
		newURN = plainURN
	}
	return newURN, version
}

func processRequest(c *gin.Context, funcReqInfo *model.FunctionRequestInfo) error {
	plainURN := c.Param(functionURNParam)
	anonymizeURN := urnutils.AnonymizeTenantURN(plainURN)

	log.GetLogger().Debugf("request URN is coming: %s, alias: %s traceID: %s",
		anonymizeURN, c.Request.Header.Get(constants.HeaderInvokeAlias), funcReqInfo.TraceID)

	functionInfo, err := urnutils.GetFunctionInfo(plainURN)
	if err != nil {
		return err
	}
	if err = functionInfo.Valid(); err != nil {
		log.GetLogger().Errorf("invalid function name,err is %s", err)
		return err
	}
	if functionInfo.BusinessID == "" || functionInfo.TenantID == "" || functionInfo.Name == "" {
		return fmt.Errorf("wrong function name %s", plainURN)
	}

	urn, version := getURNWithVersion(functionInfo.Version, plainURN)

	appID, err := extractAK(c.Request)
	if err != nil {
		log.GetLogger().Warnf("failed to extract ak from authorization %s", err)
	}

	funcReqInfo.URN = urn
	funcReqInfo.AnonymizeURN = anonymizeURN
	funcReqInfo.BusinessID = functionInfo.BusinessID
	funcReqInfo.TenantID = functionInfo.TenantID
	funcReqInfo.Name = functionInfo.Name
	funcReqInfo.Version = version
	funcReqInfo.Alias = functionInfo.GetAliasForFuncBranch()
	funcReqInfo.AppID = appID
	processStateID(funcReqInfo, c)

	return nil
}

func processStateID(funcReqInfo *model.FunctionRequestInfo, c *gin.Context) {
	stateKey, stateID := c.Request.Header.Get(constants.HeaderStateKey), c.Request.Header.Get(constants.HeaderStateID)
	if stateKey == "" && stateID == "" {
		return
	}
	if stateID != "" {
		funcReqInfo.StateID = stateID
		return
	}
	service, funcName, err := urnutils.GetFunctionNameAndServiceName(funcReqInfo.Name)
	if err != nil {
		return
	}
	funcReqInfo.StateKey = stateKey
	stateID = utils.GenStateIDByKey(funcReqInfo.TenantID, "0@"+service, funcName, stateKey)
	funcReqInfo.StateID = stateID
	c.Request.Header.Set(constants.HeaderStateID, stateID)
}

func sendResponseAndWriteLog(code, iCode int, message string, reqInfo *model.FunctionRequestInfo,
	w http.ResponseWriter) {
	data, err := json.Marshal(InvokeErrorResponse{
		Code:    iCode,
		Message: message,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal response data: %s", err)
		return
	}
	if w != nil && w.Header() != nil {
		w.Header().Add(constants.HeaderErrorCode, strconv.Itoa(iCode))
		w.Header().Add(constants.HeaderErrorNode, nodeIP)
		w.Header().Add(constants.HeaderErrorPod, podName)
	}
	w.WriteHeader(code)
	_, err = w.Write(data)
	if err != nil {
		log.GetLogger().Warnf("failed to write response body: %s", err)
	}
	log.GetLogger().Infof("%s | %s | %s | %.3f | innerCode %d | %s | invokeEndFrontend",
		urnutils.ExtractFuncName(reqInfo.Name), reqInfo.TenantID, reqInfo.TraceID,
		utils.ConvertSecondToMillisecond(time.Since(reqInfo.StartTime).Seconds()),
		iCode, innerCode(iCode).String())
	metrics.PublishMetrics(metrics.RequestStatistics{ErrorFlag: true})
}

func authCheck(r *http.Request) error {
	if !constant.FConfig.AuthenticationEnable {
		return nil
	}
	requestSign := r.Header.Get(constants.HeaderAuthorization)
	timestamp := r.Header.Get(constants.HeaderAuthTimestamp)
	err := localauth.AuthCheckLocally(constant.FConfig.LocalAuth.AKey, constant.FConfig.LocalAuth.SKey,
		requestSign, timestamp, constant.FConfig.LocalAuth.Duration)
	if err != nil {
		log.GetLogger().Errorf("failed to check authorization of URL locally, error: %s", err.Error())
		return err
	}
	return nil
}

func extractAK(r *http.Request) (string, error) {
	authValue := r.Header.Get(constants.HeaderAuthorization)

	if len(authValue) == 0 {
		return "", nil
	}

	strs1 := strings.Split(authValue, appIDSplit)

	if len(strs1) < minLenOfSplit {
		return "", fmt.Errorf("error parse authorization appid= %s", "*****")
	}

	strs2 := strings.Split(strs1[1], commaSplit)

	return strs2[0], nil
}

func websocketFilter(reqInfo *model.FunctionRequestInfo, req *fhttp.Request, insResource types.InstanceResource) error {
	metaInfo, ok := metadata.LoadMetaData(reqInfo.Name, reqInfo.TenantID, reqInfo.Version)
	if !ok {
		log.GetLogger().Warnf("failed to get businessType info")
		return nil
	}
	reqInfo.BusinessType = metaInfo.FuncMetaDataParts.BusinessType
	reqInfo.EnableRemoteDebug = metaInfo.FuncMetaDataParts.EnableRemoteDebug
	if utils.IsCAEFunc(reqInfo.BusinessType) && !checkCAENeedAcquireInstance(metaInfo) {
		return nil
	}
	reqInfo.EnableWebSocket = metaInfo.ExtendedMetaData.EnableWebsocket
	if !utils.IsWebSocketFunc(reqInfo.BusinessType, reqInfo.EnableRemoteDebug, reqInfo.EnableWebSocket) {
		return nil
	}
	log.GetLogger().Infof("websocket function: %s, do filter with business type %s, remote debug %t, enable websocket %t",
		reqInfo.Name, reqInfo.BusinessType, reqInfo.EnableRemoteDebug, reqInfo.EnableWebSocket)
	funcKey := urnutils.GetFuncKey(reqInfo.TenantID, reqInfo.Name, reqInfo.Version)
	funcKeyWithRes := urnutils.GetFuncKeyWithResource(funcKey, insResource.CPU, insResource.Memory)
	funcTimeout := getRequestDeadline(reqInfo)
	scheduler := remotescheduler.GetInstance().GetScheduler(funcKeyWithRes)
	if scheduler == nil {
		return fmt.Errorf("traceID: %s, failed to get scheduler", reqInfo.TraceID)
	}
	log.GetLogger().Infof("traceID: %s |funcKeyWithRes: %s |timeout: %s | websocket func "+
		"start to acquire function instance", reqInfo.TraceID, funcKeyWithRes, funcTimeout.String())
	schedulerReq := &schedulerapi.AcquireInstanceReq{
		TraceID:        reqInfo.TraceID,
		FuncKeyWithRes: funcKeyWithRes,
		Timeout:        funcTimeout.String(),
		ReqType:        schedulerapi.WebsocketHTTPRequest,
	}
	rsp, err := acquireInstance(scheduler, schedulerReq)
	if err != nil {
		log.GetLogger().Errorf("frontend acquires instance error %s from faas scheduler", err.Error())
		return err
	}
	if rsp.Error.ErrorCode != 0 {
		return fmt.Errorf("traceID: %s |funcKeyWithRes: %s | errorCode: %d | errorMessage: %s | "+
			"failed to acquire function instance", reqInfo.TraceID, funcKeyWithRes,
			rsp.Error.ErrorCode, rsp.Error.ErrorMessage)
	}
	req.Header.Set(constants.HeaderDemandWssInstance, rsp.InstanceKey)
	reqInfo.WSReqInfo = newWSRequestInfo(funcKeyWithRes, rsp)
	log.GetLogger().Infof("traceID: %s |instanceKey is %s, DemandNodeIP: %s, DemandNodePort: %s, ProcessorID: %s",
		reqInfo.TraceID, rsp.InstanceKey, rsp.NodeIP, rsp.NodePort, rsp.ProcessorID)
	return nil
}

func checkCAENeedAcquireInstance(info *metadata.FunctionMetaData) bool {
	if info.FuncMetaDataParts.EnableRemoteDebug {
		return true
	}
	return false
}

func newWSRequestInfo(funcKeyWithRes string, rsp *schedulerapi.AcquireInstanceRsp) model.WSRequestInfo {
	return model.WSRequestInfo{
		FuncKeyWithRes: funcKeyWithRes,
		InstanceKey:    rsp.InstanceKey,
		ProcessorID:    rsp.ProcessorID,
		LeaseVersion:   rsp.LeaseVersion,
		DemandNodeIP:   rsp.NodeIP,
		DemandNodePort: rsp.NodePort,
	}
}
