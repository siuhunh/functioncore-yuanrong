/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package fasthttpserver implement server of fasthttp
package fasthttpserver

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/contentsignaturecheck"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/snerror"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/tracer"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/invokemetric"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/vpa"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

const (
	defaultHealthCheckTimeout = 3 * time.Second
	remoteCallDialTimeout     = 10 * time.Second
	// RemoteCallReadTimeout remote call read timeout
	RemoteCallReadTimeout     = 1260 * time.Second
	remoteCallWriteTimeout    = 10 * time.Second
	remoteCallAttemptTimes    = 1
	defaultReadBufferSize     = 32 * 1024
	defaultServerTimeout      = 1300 * time.Second
	defaultMaxRequestBodySize = 6 * 1024 * 1024
	workerManagerPort         = "58866"
	base                      = 10
	bitSize                   = 64
	statusOk                  = 200200
	defaultTaskReqMapLen      = 4
	trueValueStr              = "true"
)

// FastServer is server of fasthttp
type FastServer struct {
	listenAddr       string
	requestHandler   internal.HandleReq
	workerManagerURI string
}

type costInfo struct {
	totalCost    time.Duration
	busCost      time.Duration
	scheduleCost time.Duration
	workerCost   time.Duration
}

// CreateFastServer creates fasthttp server
func CreateFastServer(addr string, requestHandler internal.HandleReq) *FastServer {
	return &FastServer{
		listenAddr:     addr,
		requestHandler: requestHandler,
	}
}

// Start starts server
func (f *FastServer) Start() error {
	scheme := commontls.GetURLScheme(common.FConfig.HTTPSConfig.HTTPSEnable)
	systemNameSpace := os.Getenv(constants.SystemNameSpaceEnvKey)
	if systemNameSpace == "" {
		systemNameSpace = "default"
	}
	f.workerManagerURI = fmt.Sprintf("%s://worker-manager.%s.svc.cluster.local:%s/componenthealth", scheme,
		systemNameSpace, workerManagerPort)
	maxRequestBodySize := common.FConfig.MaxRequestBodySize
	if maxRequestBodySize == 0 {
		maxRequestBodySize = defaultMaxRequestBodySize
	}
	log.GetLogger().Infof("maxRequestBodySize is: %d", maxRequestBodySize)
	go func() {
		server := &fasthttp.Server{
			Handler:            f.routerHandler,
			ReadBufferSize:     defaultReadBufferSize,
			ReadTimeout:        defaultServerTimeout,
			WriteTimeout:       defaultServerTimeout,
			MaxRequestBodySize: maxRequestBodySize,
		}
		if common.FConfig.HTTPSConfig != nil && common.FConfig.HTTPSConfig.HTTPSEnable {
			log.GetLogger().Infof("start to listen the https request on addr: %s", f.listenAddr)
			err := commontls.FastHTTPListenAndServeTLS(f.listenAddr, server)
			if err != nil {
				log.GetLogger().Errorf("failed to start the HTTPS server: %s", err.Error())
				return
			}
			return
		}
		log.GetLogger().Infof("start to listen the http request on addr: %s", f.listenAddr)
		err := server.ListenAndServe(f.listenAddr)
		if err != nil {
			log.GetLogger().Errorf("failed to listen the http request on addr %s: %s", f.listenAddr, err)
			return
		}
	}()
	go func() {
		if common.FConfig == nil || common.FConfig.VPAConfig == nil || common.FConfig.VPAConfig.Enable == 0 {
			return
		}
		server := &fasthttp.Server{
			Handler:            f.routerVPAHandler,
			ReadBufferSize:     defaultReadBufferSize,
			ReadTimeout:        defaultServerTimeout,
			WriteTimeout:       defaultServerTimeout,
			MaxRequestBodySize: maxRequestBodySize,
		}
		sloAgentListenAddr := internal.SelfPodIP + ":" + constants.VPAHTTPPort
		log.GetLogger().Infof("start to listen the http request on addr for slo agent: %s", sloAgentListenAddr)
		err := server.ListenAndServe(sloAgentListenAddr)
		if err != nil {
			log.GetLogger().Errorf("failed to listen the http request on addr %s: %s", sloAgentListenAddr, err.Error())
			return
		}
	}()
	return nil
}

func (f *FastServer) routerHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/invoke":
		tracer.WrapFastHTTPHandler(f.handleReq)(ctx)
	case "/componenthealth":
		f.componentHealth(ctx)
	case "/heartbeat":
		f.handleHeartbeat(ctx)
	case "/vpa/create":
		f.handleVPACreate(ctx)
	default:
		ctx.Error("not found", fasthttp.StatusNotFound)
	}
}

func (f *FastServer) routerVPAHandler(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/vpa/recover":
		f.handleVPARecover(ctx)
	default:
		ctx.Error("not found", fasthttp.StatusNotFound)
	}
}

func (f *FastServer) handleVPARecover(ctx *fasthttp.RequestCtx) {
	if common.FConfig == nil || common.FConfig.VPAConfig == nil || common.FConfig.VPAConfig.Enable == 0 {
		ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
		return
	}
	traceID := string(ctx.Request.Header.Peek(constants.HeaderTraceID))
	err := vpa.RecoverFunctionInstanceMonitorInSLOAgent(ctx, traceID)
	if err != nil {
		writeErrResponse(ctx, uint32(err.Code()), err.Error())
		log.GetLogger().Errorf("failed to handle SLO agent recovery request, traceID: %s", traceID)
		return
	}
	ctx.Response.SetStatusCode(http.StatusOK)
	log.GetLogger().Infof("succeed to recover current function instance monitors in SLO agent")
}

func (f *FastServer) handleVPACreate(ctx *fasthttp.RequestCtx) {
	if common.FConfig == nil || common.FConfig.VPAConfig == nil || common.FConfig.VPAConfig.Enable == 0 {
		ctx.Response.SetStatusCode(fasthttp.StatusNotFound)
		return
	}
	traceID := string(ctx.Request.Header.Peek(constants.HeaderTraceID))
	if !auth(ctx) {
		f.authCheckFailed(ctx)
		log.GetLogger().Errorf("failed to auth request when handle vpa creation, traceID: %s", traceID)
		return
	}
	err := vpa.RegisterFunctionInstanceMonitorInSLOAgent(ctx.Request.Body(), traceID)
	if err != nil {
		writeErrResponse(ctx, uint32(err.Code()), err.Error())
		log.GetLogger().Errorf("failed to handle function instance monitor creation request, traceID: %s, "+
			"errorCode: %d, errorMessage: %s", traceID, err.Code(), err.Error())
		return
	}
	log.GetLogger().Debugf("succeed to create vpa monitor, traceID: %s", traceID)
	ctx.Response.SetStatusCode(http.StatusOK)
}

func (f *FastServer) handleHeartbeat(ctx *fasthttp.RequestCtx) {
	if !auth(ctx) {
		f.authCheckFailed(ctx)
		return
	}
	ctx.Response.SetStatusCode(statusOk)
}

func (f *FastServer) authCheckFailed(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(http.StatusUnauthorized)
	_, err := ctx.WriteString("failed to check proxy authorization")
	if err != nil {
		log.GetLogger().Errorf("failed to write rsp err %s", err.Error())
	}
	log.GetLogger().Errorf("failed to check request auth error")
}

func (f *FastServer) handleReq(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	traceID := string(ctx.Request.Header.Peek(constants.HeaderTraceID))
	if !auth(ctx) {
		writeErrResponse(ctx, proxyerrors.AuthErrCode, proxyerrors.AuthErr)
		log.GetLogger().Errorf("failed to auth request: traceID %s", traceID)
		return
	}

	req, dst, err := buildInternalReq(ctx, traceID)
	if err != nil {
		writeErrResponse(ctx, proxyerrors.InternalErrCode, proxyerrors.InternalErr)
		log.GetLogger().Errorf("failed to build internal request: %s, traceID %s", err, traceID)
		return
	}
	rawData := req.RawData
	defer extraMapPool.Put(req.Extra)

	contentLength := uint64(ctx.Request.Header.ContentLength())
	memoryWant := uint64(float64(contentLength) * common.FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator)
	urn := string(ctx.Request.Header.Peek(constants.HeaderInvokeURN))
	if !monitor.DistinguishMemAllowByStateful(urn, memoryWant, traceID, req.IsStateFul) {
		go invokemetric.SaveRejectCount(urnutils.GetFuncKey(dst.TenantID, dst.FunctionName, dst.FunctionVersion),
			req.DirectRequest)
		log.GetLogger().Errorf("failed to check used mem of system, traceID %s", traceID)
		writeErrResponse(ctx, proxyerrors.HeavyLoadCode, proxyerrors.HeavyLoadErr)
		return
	}
	defer monitor.GetMemInstance().ReleaseFunctionMem(urn, memoryWant)

	resp := f.requestHandler(traceID, req, dst)
	// request accepted and response later
	if resp.ErrorCode == proxyerrors.StateAcceptedCode {
		ctx.SetStatusCode(http.StatusAccepted)
		if _, err = ctx.WriteString(resp.ErrorMessage); err != nil {
			log.GetLogger().Errorf("failed to write response: %s, traceID %s", err.Error(), traceID)
		}
		return
	}

	addResponseHeader(&ctx.Response.Header, resp)

	if resp.ErrorCode == snerror.ContentInconsistentErrorCode {
		contentsignaturecheck.CheckContentSignature(req.Extra[constants.HeaderContentSignature],
			contentsignaturecheck.GetContentSignature(rawData), traceID, "busproxy")
	}

	setCtx(start, resp, ctx, dst, traceID)
}

func setCtx(start time.Time, resp *commontypes.TCPResponse, ctx *fasthttp.RequestCtx, dst *common.FunctionProperty,
	traceID string) {
	stateCode := internal.ConvertHTTPResponseStatusCode(resp.ErrorCode)
	body, err := getRes(resp)
	if err != nil {
		stateCode = http.StatusInternalServerError
	}

	cost := calculateCost(start, resp)
	addTraceHeader(&ctx.Response.Header, resp, cost)

	ctx.SetStatusCode(stateCode)
	if _, err = ctx.Write(body); err != nil {
		log.GetLogger().Errorf("failed to write response: %s, traceID %s", err.Error(), traceID)
	}

	// funcName|tenantID|traceID|cost|busCost:scheduleCost:workerCost
	log.GetLogger().Infof("%s | %s | %s | %.3f | %.3f:%.3f:%.3f | invokeEndBus",
		urnutils.ExtractFuncName(dst.FunctionName), urnutils.Anonymize(dst.TenantID), traceID,
		commonUtils.ConvertSecondToMillisecond(cost.totalCost.Seconds()),
		commonUtils.ConvertSecondToMillisecond(cost.busCost.Seconds()),
		commonUtils.ConvertSecondToMillisecond(cost.scheduleCost.Seconds()),
		commonUtils.ConvertSecondToMillisecond(cost.workerCost.Seconds()))
}

func calculateCost(start time.Time, rsp *commontypes.TCPResponse) *costInfo {
	cost := &costInfo{}
	cost.totalCost = time.Since(start)
	cost.busCost = cost.totalCost - rsp.ActualCallCost
	cost.workerCost = rsp.ActualCallCost
	if !rsp.ActualCallStartTime.IsZero() {
		cost.scheduleCost = rsp.ActualCallStartTime.Sub(start)
	}
	return cost
}

var podName = os.Getenv(constants.HostNameEnvKey)

func addResponseHeader(responseHeader *fasthttp.ResponseHeader, resp *commontypes.TCPResponse) {
	// transfer log to frontend
	responseHeader.Set(constants.HeaderLogResult, resp.Logs)
	responseHeader.Set(constants.HeaderInvokeSummary, resp.Summary)
	responseHeader.Set(constants.HeaderBillingDuration, resp.Labels[constants.HeaderBillingDuration])
	responseHeader.Set(constants.HeaderInnerCode, strconv.Itoa(int(resp.ErrorCode)))
	if resp.CffHTTPResponse {
		responseHeader.Set(constants.HeaderCffHTTPResponse, trueValueStr)
	}
	responseHeader.Set(constants.HeaderPodName, podName)
	if resp.NodeID != "" {
		responseHeader.Set(constants.HeaderBackPressureNodeID, resp.NodeID)
		responseHeader.Set(constants.HeaderBackPressureInstanceID, resp.InstanceID)
	}
}

func addTraceHeader(responseHeader *fasthttp.ResponseHeader, resp *commontypes.TCPResponse, cost *costInfo) {
	workCost := strconv.FormatInt(int64(cost.workerCost), base)
	responseHeader.Set(constants.HeaderWorkerCost, workCost)
	responseHeader.Set(constants.HeaderCallInstance, resp.CallInstanceID)
	responseHeader.Set(constants.HeaderCallNode, resp.CallNode)
}

// getRes marshal response to json if request is error
func getRes(rsp *commontypes.TCPResponse) ([]byte, error) {
	if rsp.ErrorCode == proxyerrors.StateOKCode {
		return rsp.RawData, nil
	}
	return internal.ConvertErrorToHTTPResponseBody(rsp.ErrorCode, rsp.ErrorMessage), nil
}

var trueBytes = []byte("true")

func buildInternalReq(ctx *fasthttp.RequestCtx, traceID string) (*commontypes.TCPRequest,
	*common.FunctionProperty, error) {
	urn := string(ctx.Request.Header.Peek(constants.HeaderInvokeURN))
	demandInstance := string(ctx.Request.Header.Peek(constants.HeaderDemandWssInstance))
	dst, err := utils.BuildFunctionProperty(urn, demandInstance, traceID)
	if err != nil {
		return nil, nil, err
	}

	request := &commontypes.TCPRequest{
		LogType:            string(ctx.Request.Header.Peek(constants.HeaderLogType)),
		RawData:            ctx.Request.Body(),
		DirectRequest:      true,
		CPU:                string(ctx.Request.Header.Peek(constants.HeaderCPUSize)),
		Memory:             string(ctx.Request.Header.Peek(constants.HeaderMemorySize)),
		Priority:           utils.GetPriority(string(ctx.Request.Header.Peek(constants.PriorityHeader))),
		BackpressureNodeID: string(ctx.Request.Header.Peek(constants.HeaderBackPressureNodeID)),
	}
	ctx.Request.ResetBody()

	if bytes.Equal(ctx.Request.Header.Peek(constants.HeaderBackpressure), trueBytes) {
		request.CanBackpressure = true
	}
	if bytes.Equal(ctx.Request.Header.Peek(constants.HeaderRetryFlag), trueBytes) {
		request.RetryFlag = true
	}
	if bytes.Equal(ctx.Request.Header.Peek(constants.HeaderPersistResponse), trueBytes) {
		request.PersistResponse = true
	}
	if demandInstance != "" {
		request.BusinessType = constants.BusinessTypeWebSocket
	}

	if numsStr := string(ctx.Request.Header.Peek(constants.HeaderBackpressureNums)); numsStr != "" {
		if backPressureNums, err := strconv.ParseUint(numsStr, base, bitSize); err == nil {
			request.BackPressureNums = backPressureNums
		}
	}

	request.Extra = getExtraHeaders(ctx)

	if request.Extra[constants.HeaderStateID] != "" {
		parseTaskReq(request, dst)
	}

	return request, dst, nil
}

func parseTaskReq(req *commontypes.TCPRequest, dst *common.FunctionProperty) {
	dst.InstanceID = internal.HostName
	service, funcName, _ := urnutils.GetFunctionNameAndServiceName(dst.FunctionName)
	dst.FunctionName = funcName
	userData := make(map[string]string, defaultTaskReqMapLen)
	userData[constants.SNServiceIDFlag] = "0@" + service
	userData[constants.FrontendCallFlag] = "Frontend"
	userData[constants.SrcTenantFlag] = dst.TenantID
	userData[constants.StateIDFlag] = req.Extra[constants.HeaderStateID]
	req.UserData = userData
	req.IsStateFul = true
}

var extraMapPool = mapPool{
	Pool: sync.Pool{New: func() interface{} {
		return make(map[string]string, constants.ExtraHeaderMapSize)
	}},
}

type mapPool struct {
	sync.Pool
}

// Get gets a map from pool
func (mp *mapPool) Get() map[string]string {
	ret, ok := mp.Pool.Get().(map[string]string)
	if !ok {
		return make(map[string]string, constants.ExtraHeaderMapSize)
	}
	return ret
}

// Put puts map back to pool
func (mp *mapPool) Put(extra map[string]string) {
	if extra == nil {
		return
	}
	for k := range extra {
		extra[k] = ""
	}
	mp.Pool.Put(extra)
}

func getExtraHeaders(ctx *fasthttp.RequestCtx) map[string]string {
	extra := extraMapPool.Get()
	extra[constants.HeaderRequestID] = string(ctx.Request.Header.Peek(constants.HeaderRequestID))
	extra[constants.HeaderAccessKey] = string(ctx.Request.Header.Peek(constants.HeaderAccessKey))
	extra[constants.HeaderSecretKey] = string(ctx.Request.Header.Peek(constants.HeaderSecretKey))
	extra[constants.HeaderSecurityAccessKey] = string(ctx.Request.Header.Peek(constants.HeaderSecurityAccessKey))
	extra[constants.HeaderSecuritySecretKey] = string(ctx.Request.Header.Peek(constants.HeaderSecuritySecretKey))
	extra[constants.HeaderAuthToken] = string(ctx.Request.Header.Peek(constants.HeaderAuthToken))
	extra[constants.HeaderSecurityToken] = string(ctx.Request.Header.Peek(constants.HeaderSecurityToken))
	extra[constants.HeaderInvokeSource] = string(ctx.Request.Header.Peek(constants.HeaderInvokeSource))
	extra[constants.HeaderDomainName] = string(ctx.Request.Header.Peek(constants.HeaderDomainName))
	extra[constants.HeaderLiveDataTraceID] = string(ctx.Request.Header.Peek(constants.HeaderLiveDataTraceID))
	extra[constants.HeaderInvokeAlias] = string(ctx.Request.Header.Peek(constants.HeaderInvokeAlias))
	extra[constants.HeaderOriginVersionTag] = string(ctx.Request.Header.Peek(constants.HeaderOriginVersionTag))
	extra[constants.HeaderStreamEnable] = string(ctx.Request.Header.Peek(constants.HeaderStreamEnable))
	extra[constants.HeaderStreamAddr] = string(ctx.Request.Header.Peek(constants.HeaderStreamAddr))
	extra[constants.HeaderStreamRunID] = string(ctx.Request.Header.Peek(constants.HeaderStreamRunID))
	extra[constants.HeaderStreamToken] = string(ctx.Request.Header.Peek(constants.HeaderStreamToken))
	extra[constants.HeaderStateID] = string(ctx.Request.Header.Peek(constants.HeaderStateID))
	extra[constants.HeaderWorkflowID] = string(ctx.Request.Header.Peek(constants.HeaderWorkflowID))
	extra[constants.HeaderWorkflowRunID] = string(ctx.Request.Header.Peek(constants.HeaderWorkflowRunID))
	extra[constants.HeaderWorkflowStateID] = string(ctx.Request.Header.Peek(constants.HeaderWorkflowStateID))
	extra[constants.HeaderContentSignature] = string(ctx.Request.Header.Peek(constants.HeaderContentSignature))
	extra[constants.HeaderTraceParent] = string(ctx.Request.Header.Peek(constants.HeaderTraceParent))
	extra[constants.HeaderRequestStreamName] = string(ctx.Request.Header.Peek(constants.HeaderRequestStreamName))
	extra[constants.HeaderResponseStreamName] = string(ctx.Request.Header.Peek(constants.HeaderResponseStreamName))
	extra[constants.HeaderFrontendResponseStreamName] =
		string(ctx.Request.Header.Peek(constants.HeaderFrontendResponseStreamName))
	putExtraMapIntoExtra(extra, ctx.Request.Header.Peek(constants.ExtraMap))
	return extra
}

func putExtraMapIntoExtra(extra map[string]string, peekExtraMapJSON []byte) {
	if len(peekExtraMapJSON) == 0 {
		return
	}
	if len(extra) == 0 {
		extra = make(map[string]string, len(peekExtraMapJSON))
	}
	extraMap := make(map[string]string)
	err := json.Unmarshal(peekExtraMapJSON, &extraMap)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal extra map json, %s", err.Error())
		return
	}
	for k, v := range extraMap {
		if extra[k] != "" {
			extra[k+"-user"] = v
			continue
		}
		extra[k] = v
	}
}

func writeErrResponse(ctx *fasthttp.RequestCtx, code uint32, message string) {
	ctx.Response.Header.Set(constants.HeaderInnerCode, strconv.Itoa(int(code)))
	ctx.Response.Header.Set(constants.HeaderPodName, podName)
	ctx.SetStatusCode(http.StatusInternalServerError)
	if _, err := ctx.Write(internal.ConvertErrorToHTTPResponseBody(code, message)); err != nil {
		log.GetLogger().Errorf("failed to write response: %s", err.Error())
	}
}

func auth(ctx *fasthttp.RequestCtx) bool {
	if !common.FConfig.AuthenticationEnable {
		return true
	}

	requestSign := string(ctx.Request.Header.Peek(constants.HeaderAuthorization))
	timestamp := string(ctx.Request.Header.Peek(constants.HeaderAuthTimestamp))
	err := localauth.AuthCheckLocally(common.FConfig.LocalAuth.AKey, common.FConfig.LocalAuth.SKey, requestSign,
		timestamp, common.FConfig.LocalAuth.Duration)
	if err != nil {
		log.GetLogger().Errorf("failed to check authorization of URL locally, error: %s", err.Error())
		return false
	}
	return true
}

func (f *FastServer) componentHealth(ctx *fasthttp.RequestCtx) {
	if !auth(ctx) {
		f.authCheckFailed(ctx)
		return
	}

	c := getFastHTTPClient()
	ctx.Request.SetHost("")
	ctx.Request.SetRequestURI(f.workerManagerURI)
	ctx.Request.Header.SetMethod(http.MethodGet)

	err := c.DoTimeout(&ctx.Request, &ctx.Response, defaultHealthCheckTimeout)
	if err != nil {
		msg := fmt.Sprintf("failed to request instanceManager error %s ", err.Error())
		log.GetLogger().Errorf("%s", msg)
		ctx.SetStatusCode(http.StatusInternalServerError)
		_, err = ctx.WriteString(msg)
		if err != nil {
			log.GetLogger().Errorf("failed to write rsp err %s", err.Error())
		}
		return
	}
	if ctx.Response.Header.StatusCode() == http.StatusInternalServerError {
		msg := fmt.Sprintf("failed to request instanceManager, status code is %d ", http.StatusInternalServerError)
		log.GetLogger().Errorf("%s", msg)
		ctx.SetStatusCode(http.StatusInternalServerError)
		_, err = ctx.WriteString(msg)
		if err != nil {
			log.GetLogger().Errorf("failed to write rsp err %s", err.Error())
		}
		return
	}
}

var (
	clientOnce     sync.Once
	fastClient     *fasthttp.Client
	httpClientOnce sync.Once
	fastHTTPClient *fasthttp.Client
)

func getFastHTTPClient() *fasthttp.Client {
	clientOnce.Do(func() {
		var tlsConfig *tls.Config
		if common.FConfig.HTTPSConfig.HTTPSEnable {
			newCfg := *commontls.GetClientTLSConfig()
			newCfg.NextProtos = []string{"http/1.1"}
			tlsConfig = &newCfg
		}
		fastClient = &fasthttp.Client{
			ReadTimeout:  defaultHealthCheckTimeout,
			WriteTimeout: defaultHealthCheckTimeout,
			TLSConfig:    tlsConfig,
			Dial: func(addr string) (net.Conn, error) {
				return fasthttp.DialTimeout(addr, defaultHealthCheckTimeout)
			},
		}
	})
	return fastClient
}

// GetFastHTTPClient get fast http client
func GetFastHTTPClient() *fasthttp.Client {
	httpClientOnce.Do(func() {
		var tlsConfig *tls.Config
		if common.FConfig.HTTPSConfig.HTTPSEnable {
			newCfg := *commontls.GetClientTLSConfig()
			newCfg.NextProtos = []string{"http/1.1"}
			tlsConfig = &newCfg
		}
		fastHTTPClient = &fasthttp.Client{
			MaxIdemponentCallAttempts: remoteCallAttemptTimes,
			ReadTimeout:               RemoteCallReadTimeout,
			WriteTimeout:              remoteCallWriteTimeout,
			TLSConfig:                 tlsConfig,
			Dial: func(addr string) (net.Conn, error) {
				return fasthttp.DialTimeout(addr, remoteCallDialTimeout)
			},
		}
	})
	return fastHTTPClient
}
