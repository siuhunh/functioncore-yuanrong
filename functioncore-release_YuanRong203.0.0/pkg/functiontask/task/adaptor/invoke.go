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

package adaptor

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/core-sdk/config"
	"functioncore/pkg/functionbus/proxy/fasthttpserver"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/task/util"
)

var clusterID = os.Getenv(constants.ClusterIDKey)

const retryTimes = 3

func (a *Adaptor) calleeInvoke(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetInvokeTaskReq()
	log.GetLogger().ForDebugf("calleeInvoke, serviceID: %s, stateID: %s, traceID: %s, futureID: %s", taskCtx.ServiceID(),
		req.GetStateID(), taskCtx.TraceID(), req.GetFutureID())

	resp := req.Callee(taskCtx)
	taskCtx.SetResult(resp)
}

func (a *Adaptor) invoke(taskCtx taskcontext.Context, args *FuncArgs) {
	req := new(invoke.Request)
	req.RequestPb = *args.GetInvokeReq()
	log.GetLogger().ForDebugf("the req after proccess is request to function: %s, traceId: %s",
		req.FuncName, taskCtx.TraceID())
	taskCtx.SetReqIDList(
		common.AppendReqID(taskCtx.ReqIDList(), req.FuncName, req.Version, req.CountOfFuncName),
	)
	funcName := args.GetInvokeReq().FuncName
	needForward, hosts := judgeForwardToOtherCluster(taskCtx, args.GetInvokeReq())
	if needForward {
		log.GetLogger().ForDebugf("request should forward to %#v for %s, traceId: %s",
			hosts, funcName, taskCtx.TraceID())
		resp := forwardReq(taskCtx, args, req, hosts)
		taskCtx.SetResult(resp)
		return
	}

	version := args.GetInvokeReq().Version
	if args.GetInvokeReq().StateID == "" {
		realFuncName, ok := util.GetRealFuncName(funcName, version, taskCtx.ServiceID(), taskCtx.TenantID())
		if !ok {
			log.GetLogger().Errorf("failed to get real funcName, funcName: %s, version: %s", funcName, version)
			processNoSuchFunctionError(taskCtx)
			return
		}
		if !servicediscovery.GetRegistry().IsFunctionExist(taskCtx.TenantID(), taskCtx.ServiceID(), realFuncName,
			urnutils.DefaultVersion) {
			log.GetLogger().Errorf("function does not exist, realFuncName: %s", realFuncName)
			processNoSuchFunctionError(taskCtx)
			return
		}
	}

	if taskCtx.Source() == common.SourceRuntimeToLocalFunctionTask {
		resp := req.Caller(taskCtx)
		taskCtx.SetResult(resp)
		return
	}

	if taskCtx.Source() == common.SourceRuntimeToCentralFunctionTask {
		resp := invoke.Invoke(taskCtx, req)
		taskCtx.SetResult(resp)
	}
}

func processNoSuchFunctionError(taskCtx taskcontext.Context) {
	taskCtx.SetResult(&invoke.Response{
		Code:    snerror.NoSuchFunction,
		Message: snerror.ErrText(snerror.NoSuchFunction),
	})
}

func forwardReq(taskCtx taskcontext.Context, args *FuncArgs, req *invoke.Request, hosts []string) *invoke.Response {
	sncFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: taskCtx.ServiceID(),
		FuncName:  req.FuncName,
		Version:   req.Version,
		StateID:   req.StateID,
		ReqIDList: taskCtx.ReqIDList(),
	})
	log.GetLogger().Debugf("invoke, funcName: %s, stateID: %s, traceID: %s, futureID: %s, createStateQueue: %t",
		req.GetFuncName(), req.GetStateID(), taskCtx.TraceID(), sncFuture.ID, req.GetCreateState())
	taskCtx.FutureStore().Add(sncFuture)

	go func() {
		resp := doForwardReq(taskCtx, args.GetInvokeReq(), hosts)
		sncFuture.SetResponse(resp)
	}()

	resp := &invoke.Response{
		Code:     common.EOK,
		FutureID: sncFuture.ID,
		StateID:  req.StateID,
	}
	return resp
}

func doForwardReq(taskCtx taskcontext.Context, invokeReq *invoke.RequestPb, hosts []string) *common.Response {
	payload := invokeReq.Payload
	funcVersionURN := getFuncVersionURN(taskCtx, invokeReq)
	path := "serverless/v1/functions" + "/" + funcVersionURN + "/invocations"
	headers := buildInvokeHeaders(taskCtx)
	request := fasthttp.AcquireRequest()
	response := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(request)
	defer fasthttp.ReleaseResponse(response)
	setRequestHeaders(request, headers)
	jsonErr := setExtraMapHeader(request, invokeReq.ExtraMap)
	if jsonErr != nil {
		return &common.Response{
			Code:    snerror.InternalError,
			Message: jsonErr.Error(),
		}
	}
	buildInvokeRequest(request, path, payload)
	err := doRequest(hosts, request, response)
	if err != nil {
		return &common.Response{Code: snerror.InternalError, Message: err.Error()}
	}
	innerCode, err := strconv.Atoi(string(response.Header.Peek(constants.HeaderInnerCode)))
	if err != nil {
		return &common.Response{
			Code:    snerror.InternalError,
			Message: err.Error(),
		}
	}
	resp := &common.Response{
		Code:    int32(innerCode),
		Result:  string(response.Body()),
		StateID: invokeReq.StateID,
		Log:     string(response.Header.Peek(constants.HeaderLogResult)),
	}
	if innerCode != common.EOK {
		errResp := &types.HttpResponse{}
		err := json.Unmarshal(response.Body(), errResp)
		if err != nil {
			return &common.Response{
				Code:    snerror.InternalError,
				Message: err.Error(),
			}
		}
		resp.Message = errResp.Message
		resp.Code = int32(errResp.Code)
	}
	return resp
}

func doRequest(hosts []string, request *fasthttp.Request, response *fasthttp.Response) error {
	var err error
	count := 0
	for _, host := range hosts {
		count++
		endpoint := fmt.Sprintf("%s:%s", host, "31220")
		request.URI().SetHost(endpoint)
		err = fasthttpserver.GetFastHTTPClient().DoTimeout(request, response, fasthttpserver.RemoteCallReadTimeout)
		if err == nil || count == retryTimes {
			break
		}
	}
	return err
}

func judgeForwardToOtherCluster(taskCtx taskcontext.Context, invokeReq *invoke.RequestPb) (bool, []string) {
	if invokeReq.StateID != "" {
		return false, []string{}
	}
	registry := servicediscovery.GetRegistry()
	funcURN := getFuncURN(taskCtx, invokeReq)
	clusters := registry.GetAvailableClusters(funcURN)
	if len(clusters) == 0 {
		return false, []string{}
	}
	for _, cluster := range clusters {
		if cluster == clusterID {
			return false, []string{}
		}
	}
	if len(clusters) == 1 && len(registry.ClusterFrontends[clusters[0]]) > 0 {
		return true, registry.ClusterFrontends[clusters[0]]
	}
	hosts := make([]string, 0)
	for _, cluster := range clusters {
		registry.ClusterFrontendsLock.RLock()
		frontends := registry.ClusterFrontends[cluster]
		registry.ClusterFrontendsLock.RUnlock()
		if len(frontends) == 0 {
			continue
		}
		hosts = append(hosts, frontends[0])
	}
	if len(hosts) > 0 {
		return true, hosts
	}
	log.GetLogger().Errorf("func:%s need forward to other cluster, but no available frontend found", funcURN)
	return false, []string{}
}

func getFuncURN(taskCtx taskcontext.Context, invokeReq *invoke.RequestPb) string {
	funcVersionURN := getFuncVersionURN(taskCtx, invokeReq)
	parts := strings.Split(funcVersionURN, ":")
	parts = parts[0 : len(parts)-1]
	return strings.Join(parts, ":")
}

func getFuncVersionURN(taskCtx taskcontext.Context, invokeReq *invoke.RequestPb) string {
	tenantID := taskCtx.TenantID()
	serviceID := taskCtx.ServiceID()
	fullFuncName := fmt.Sprintf("%s/%s@%s/%s", tenantID, serviceID, invokeReq.FuncName, invokeReq.Version)
	funcMeta, err := servicediscovery.GetRegistry().GetFunctionMetadata(fullFuncName)
	if err != nil {
		return ""
	}
	return funcMeta.FuncMetaData.FunctionVersionURN
}

func buildInvokeHeaders(taskCtx taskcontext.Context) map[string]string {
	headers := make(map[string]string)
	headers[constants.HeaderTraceID] = taskCtx.TraceID()
	authorization, timestamp := localauth.SignLocally(config.FConfigs.LocalAuth.AKey,
		config.FConfigs.LocalAuth.SKey, constants.FunctionTask, config.FConfigs.LocalAuth.Duration)
	headers[constants.HeaderAuthTimestamp] = timestamp
	headers[constants.HeaderAuthorization] = authorization
	headers[constants.ContentType] = "application/json"
	return headers
}

func setRequestHeaders(request *fasthttp.Request, headers map[string]string) {
	for key, value := range headers {
		request.Header.Set(key, value)
	}
}

func setExtraMapHeader(request *fasthttp.Request, extraMap map[string]string) error {
	if extraMap == nil {
		return nil
	}
	extraMapJSON, err := json.Marshal(extraMap)
	if err != nil {
		return err
	}
	request.Header.Set(constants.ExtraMap, string(extraMapJSON))
	return nil
}

func buildInvokeRequest(request *fasthttp.Request, path string, payload string) {
	request.Header.SetMethod(fasthttp.MethodPost)
	request.URI().DisablePathNormalizing = true
	request.URI().SetPath(path)
	if config.FConfigs.HTTPSConfig.HTTPSEnable {
		request.URI().SetScheme("https")
	}
	request.SetBody([]byte(payload))
}
