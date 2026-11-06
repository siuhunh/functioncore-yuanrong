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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/bridgetypes"
	commutils "functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	commonUtils "functioncore/pkg/core-sdk/common"
	"functioncore/pkg/core-sdk/sdk"
	coretypes "functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/servicediscovery"
	busUtils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/api/filter"
	"functioncore/pkg/functiontask/task/api/getinstance"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/api/waitfuture"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/taskcontext"
	taskutil "functioncore/pkg/functiontask/task/util"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

const (
	mapInitSize            = 16
	statusOKCode           = 150200
	workerManagerDeployURL = "/worker-manager/v1/functions/sn:cn:yrk:%s:function:%s:%s/worker"
)

type deployParam struct {
	FuncName        string // deployed function name
	Applier         string // applier instance id
	DeployNode      string // deploy node id
	Business        string // business
	TenantID        string // tenant id
	Version         string // version
	OwnerIP         string // owner IP
	TraceID         string // trace id
	TriggerFlag     string // This is a trigger request flag.
	StateID         string
	OwnedWorkerView types.WorkersView // proxy owned worker view
	CPU             int64
	Memory          int64
	Creator         string
	CreateOptions   *types.StateCreateOptions `json:"createOptions"`
}

type getWorkerSuccessResponse struct {
	Worker   *worker         `json:"worker"`
	Instance *types.Instance `json:"instance"`
	Code     int             `json:"code"`
	Message  string          `json:"message"`
}

type worker struct {
	Instances       []*types.Instance `json:"instances"`
	FunctionName    string            `json:"functionname"`
	FunctionVersion string            `json:"functionversion"`
	Tenant          string            `json:"tenant"`
	Business        string            `json:"business"`
}

func (a *Adaptor) wait(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetWaitReq()

	var deadlockChecker deadlock.Checker
	if taskCtx.Source() == common.SourceRuntimeToLocalFunctionTask {
		deadlockChecker = a.getDeadlockChecker(taskCtx, req.StateID, false)
	} else {
		deadlockChecker = a.getDeadlockChecker(taskCtx, req.StateID, true)
	}
	request := &waitfuture.TransformedRequest{
		FutureIDs: req.FutureIDs,
		WaitNum:   req.WaitNum,
		Timeout:   time.Duration(int64(req.Timeout) * int64(time.Millisecond)),
		StateID:   req.StateID,
		State:     req.State,
	}
	resp := waitfuture.Wait(taskCtx, request, deadlockChecker)
	taskCtx.SetResult(resp)
}

func (a *Adaptor) terminate(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetTerminateReq()
	if taskCtx.Source() == common.SourceRuntimeToLocalFunctionTask ||
		taskCtx.Source() == common.SourceRuntimeToCentralFunctionTask {
		taskCtx.SetReqIDList(
			common.AppendReqID(taskCtx.ReqIDList(), common.TerminateFuncName, "", req.CountOfFuncName),
		)
	}
	switch taskCtx.Source() {
	case common.SourceFunctionTaskToFunctionTask, common.SourceRuntimeToCentralFunctionTask:
		resp := req.Terminate(taskCtx)
		taskCtx.SetResult(resp)
	case common.SourceRuntimeToLocalFunctionTask:
		resp := req.Caller(taskCtx)
		taskCtx.SetResult(resp)
	default:
		log.GetLogger().Warnf("invalid source: %d", taskCtx.Source())
		return
	}
}

// setTimout adapts to setTTL interface which provided by core-sdk.
func (a *Adaptor) setTimeout(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetSetTimeoutReq()

	if taskCtx.Source() == common.SourceRuntimeToLocalFunctionTask ||
		taskCtx.Source() == common.SourceRuntimeToCentralFunctionTask {
		taskCtx.SetReqIDList(
			common.AppendReqID(taskCtx.ReqIDList(), common.SetTimeoutFuncName, "", req.CountOfFuncName),
		)
	}
	switch taskCtx.Source() {
	case common.SourceFunctionTaskToFunctionTask, common.SourceRuntimeToCentralFunctionTask:
		resp := req.SetTimeout(taskCtx)
		taskCtx.SetResult(resp)
	case common.SourceRuntimeToLocalFunctionTask:
		resp := req.Caller(taskCtx)
		taskCtx.SetResult(resp)
	default:
		log.GetLogger().Warnf("invalid source: %d", taskCtx.Source())
		return
	}
}

func (a *Adaptor) readState(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetReadReq()
	if req.ByKey {
		req.StateID = utils.GenStateIDByKey(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName, req.Key)
	}
	switch taskCtx.Source() {
	case common.SourceFunctionTaskToFunctionTask, common.SourceRuntimeToCentralFunctionTask:
		resp := req.ReadState(taskCtx)
		taskCtx.SetResult(resp)
	case common.SourceRuntimeToLocalFunctionTask:
		resp := req.Caller(taskCtx)
		taskCtx.SetResult(resp)
	default:
		log.GetLogger().Warnf("invalid source: %d", taskCtx.Source())
		return
	}
}

func (a *Adaptor) saveState(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetSaveReq()
	switch taskCtx.Source() {
	case common.SourceFunctionTaskToFunctionTask, common.SourceRuntimeToCentralFunctionTask:
		resp := req.SaveState(taskCtx)
		taskCtx.SetResult(resp)
	case common.SourceRuntimeToLocalFunctionTask:
		resp := req.Caller(taskCtx)
		taskCtx.SetResult(resp)
	default:
		log.GetLogger().Warnf("invalid source: %d", taskCtx.Source())
		return
	}
}

func (a *Adaptor) getInstance(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetGetInstanceReq()
	taskCtx.SetStateKey(req.Key)
	taskCtx.SetIsGetInstanceScenario(true)
	stateID := utils.GenStateIDByKey(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName, req.Key)
	exist, err := isStateExist(taskCtx, req.FuncName, stateID)
	if err != nil {
		taskCtx.SetResult(&filter.Response{
			Code:    int32(err.Code()),
			Message: err.Error(),
		})
		return
	}
	if !exist {
		taskCtx.SetResult(&getinstance.Response{
			Code:    snerror.NoSuchInstanceName,
			Message: fmt.Sprintf(snerror.ErrText(snerror.NoSuchInstanceName), req.Key, stateID),
		})
		return
	}
	taskCtx.SetResult(&getinstance.Response{
		Code:    common.EOK,
		StateID: stateID,
	})
}
func isNewStateKeyScenario(taskCtx taskcontext.Context, stateID string, stateKey string) bool {
	isNewStateKeyScenario := taskCtx.IsNewStateKeyScenario()
	log.GetLogger().Debugf("gen stateID by key, serviceID: %s, stateID: %s, stateKey: %s, traceID: %s,"+
		"isNewStateKeyScenario: %t", taskCtx.ServiceID(), stateID, stateKey, taskCtx.TraceID(),
		isNewStateKeyScenario)
	if !isNewStateKeyScenario {
		taskCtx.SetResult(&filter.Response{
			Code:     common.EOK,
			StateID:  stateID,
			StateKey: stateKey,
		})
		return false
	}
	return true
}

func processCreateStateError(taskCtx taskcontext.Context, err common.TaskError, req *filter.RequestPb, stateID string,
	metaData *metadata.Metadata) {
	if len(req.PodName) != 0 {
		req.StateID = stateID
		doRelease(taskCtx, req)
		err = common.NewTaskError(commonUtils.InternalErrCode, err.Error())
	}
	taskContextSetError(taskCtx, metaData, err)
}

// The filter method is used to create state information, such as state queues, state instances, and state ID.
func (a *Adaptor) filter(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetFilterReq()
	if !isStatefulFunctionCheck(taskCtx, args) {
		return
	}
	if isReqPodNameAndStateKeyEmpty(taskCtx, req) {
		return
	}
	stateID := req.StateID
	metaData := buildFilterMetadata(taskCtx, req, stateID)
	if isReqStateIDExists(taskCtx, stateID, metaData, req) {
		return
	}
	stateKey, isReturn := a.getStateKey(taskCtx, req, metaData)
	log.GetLogger().Debugf("functionTask getStateKey end, stateKey:%s, stateID:%s, isReturn:%t, traceId:%s",
		stateKey, stateID, isReturn, taskCtx.TraceID())
	if isReturn {
		return
	}
	req.Key = stateKey
	metaData.StateKey = stateKey
	stateID = utils.GenStateIDByKey(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName, req.Key)
	metaData.StateID = stateID
	if ok := checkStateIsNotExistDuringCreatingState(taskCtx, stateID, req); !ok {
		return
	}
	taskCtx.SetCreator(req.Creator)
	taskCtx.SetStateCreateOptions(metaData.CreateOptions)
	createStateID, err := util.CreateState(taskCtx, req.FuncName, stateID)
	if err != nil {
		processCreateStateError(taskCtx, err, req, stateID, metaData)
		return
	}
	scaleUp(context.Background(), taskCtx, metaData)
	if !taskCtx.IsSameNode(createStateID) {
		req.StateID = createStateID
		resp := req.Caller(taskCtx)
		taskCtx.SetResult(resp)
		return
	}
	createStateQueue(taskCtx, metaData, req)
}

func buildFilterMetadata(taskCtx taskcontext.Context, req *filter.RequestPb, stateID string) *metadata.Metadata {
	metaData := &metadata.Metadata{
		TenantID:     taskCtx.TenantID(),
		ServiceID:    taskCtx.ServiceID(),
		FunctionName: req.FuncName,
		StateID:      stateID,
		StateKey:     req.Key,
		Creator:      req.Creator,
	}
	if req.Options != nil {
		metaData.CreateOptions = &types.StateCreateOptions{
			CPU:    req.Options.Cpu,
			Memory: req.Options.Memory,
		}
	}
	return metaData
}

func scaleUp(ctx context.Context, taskCtx taskcontext.Context, scaleUpParam *metadata.Metadata) {
	request, err := makeScaleUpRequest(ctx, taskCtx.TraceID(), scaleUpParam)
	if err != nil {
		log.GetLogger().Warnf("function-task make request to worker manager error: %s,"+
			"functionKey: %s, stateID: %s, traceID: %s",
			err.Error(), scaleUpParam.FunctionName, scaleUpParam.StateID, taskCtx.TraceID())
		return
	}
	resp, err := busUtils.GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Warnf("function-task send request to worker manager error: %s,"+
			"functionKey: %s, stateID: %s, traceID: %s",
			err.Error(), scaleUpParam.FunctionName, scaleUpParam.StateID, taskCtx.TraceID())
		return
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Warnf("function-task failed to get response body: %s, functionKey: %s, stateID: %s, traceID: %s",
			err.Error(), scaleUpParam.FunctionName, scaleUpParam.StateID, taskCtx.TraceID())
		return
	}
	scaleUpResp, err := handleScaleUpResponse(taskCtx, resp, respBody, scaleUpParam)
	if err != nil {
		log.GetLogger().Warnf("Scale up function failed, functionKey: %s, stateID: %s, traceID: %s, error: %s",
			scaleUpParam.FunctionName, scaleUpParam.StateID, taskCtx.TraceID(), err.Error())
		return
	}
	log.GetLogger().Infof("deploy functionKey %s instance: %v successfully, stateID: %s, traceID: %s",
		scaleUpParam.FunctionName, scaleUpResp.Instance, scaleUpParam.StateID, taskCtx.TraceID())
}

func makeScaleUpRequest(ctx context.Context, traceID string, scaleUpParam *metadata.Metadata) (*http.Request, error) {
	tenantID := scaleUpParam.TenantID
	functionName, functionVersion := taskutil.ParseFuncName(scaleUpParam.ServiceID, scaleUpParam.FunctionName)
	log.GetLogger().Infof("prepare make scale up request, traceID: %s, tenantID: %s, "+
		"functionName: %s, functionVersion: %s", traceID, tenantID, functionName, functionVersion)
	requestURI := wmhealth.GetWorkerManagerBaseURL() +
		fmt.Sprintf(workerManagerDeployURL, tenantID, functionName, functionVersion)

	param := deployParam{
		TraceID:         traceID,
		FuncName:        functionName,
		Applier:         os.Getenv(constants.HostNameEnvKey),
		DeployNode:      os.Getenv("NODE_ID"),
		Business:        "yrk",
		TenantID:        tenantID,
		Version:         functionVersion,
		OwnerIP:         os.Getenv(constants.HostIPEnvKey),
		StateID:         scaleUpParam.StateID,
		OwnedWorkerView: types.WorkersView{OwnedNum: 0, CurrentWorkerNum: 0, ScalingWorkersNum: 1},
		CPU:             0,
		Memory:          0,
		Creator:         scaleUpParam.Creator,
		CreateOptions:   scaleUpParam.CreateOptions,
	}
	if scaleUpParam.CreateOptions != nil {
		param.CPU = scaleUpParam.CreateOptions.CPU
		param.Memory = scaleUpParam.CreateOptions.Memory
	}
	data, err := json.Marshal(param)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal function-task scaleUp request: %s", err.Error())
		return nil, err
	}

	request, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("action failed when make function-task scaleUp request, err %s", err.Error())
		return nil, err
	}
	busUtils.FillInWorkerManagerRequestHeaders(request)
	request.Header.Set(constants.HeaderForceDeploy, strconv.FormatBool(false))
	return request, nil
}

func handleScaleUpResponse(taskCtx taskcontext.Context, resp *http.Response, respBody []byte,
	scaleUpParam *metadata.Metadata) (getWorkerSuccessResponse, error) {
	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err := json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Warnf("function-task failed to unmarshal scaleUp response: %s, functionKey: %s, traceID: %s",
				err, scaleUpParam.FunctionName, taskCtx.TraceID())
			return getWorkerSuccessResponse{}, err
		}
		return getWorkerSuccessResponse{}, errors.New(snErr.Message)
	}

	scaleUpResp := getWorkerSuccessResponse{}
	if err := json.Unmarshal(respBody, &scaleUpResp); err != nil {
		log.GetLogger().Warnf("failed to unmarshal scaleUp response: %s, functionKey: %s, traceID: %s",
			err, scaleUpParam.FunctionName, taskCtx.TraceID())
		return getWorkerSuccessResponse{}, err
	}

	if scaleUpResp.Code != statusOKCode || scaleUpResp.Worker == nil {
		log.GetLogger().Warnf("deploy response code %d, message: %s, functionKey: %s, traceID: %s", scaleUpResp.Code,
			scaleUpResp.Message, scaleUpParam.FunctionName, taskCtx.TraceID())
		return getWorkerSuccessResponse{}, errors.New(scaleUpResp.Message)
	}
	return scaleUpResp, nil
}

func doRelease(taskCtx taskcontext.Context, req *filter.RequestPb) {
	clientInstance := bridgetypes.ClientInstance{
		PodName:       req.PodName,
		AssignStateID: req.StateID,
		FuncInfo: &bridgetypes.BridgeFuncInfo{
			TenantID:       taskCtx.TenantID(),
			ServiceID:      taskCtx.ServiceID(),
			BridgeFuncName: req.FuncName,
		},
	}
	_, err := util.Release(taskCtx, req.FuncName, clientInstance)
	if err != nil {
		log.GetLogger().Errorf("release, serviceID: %s, funcName: %s, traceID: %s, code: %d, err: %s",
			taskCtx.ServiceID(), req.GetFuncName(), taskCtx.TraceID(), err.Code(), err.Error())
	} else {
		log.GetLogger().Debugf("release, serviceID: %s, funcName: %s, traceID: %s", taskCtx.ServiceID(),
			req.GetFuncName(), taskCtx.TraceID())
	}
}

func isReqStateIDExists(taskCtx taskcontext.Context, stateID string, metaData *metadata.Metadata,
	req *filter.RequestPb) bool {
	if len(stateID) != 0 {
		createStateQueue(taskCtx, metaData, req)
		return true
	}
	return false
}

func isReqPodNameAndStateKeyEmpty(taskCtx taskcontext.Context, req *filter.RequestPb) bool {
	if len(req.Key) == 0 && len(req.PodName) == 0 {
		log.GetLogger().Debugf("req pod name and state key is empty, traceId:%s", taskCtx.TraceID())
		taskCtx.SetResult(&filter.Response{
			Code:     common.EOK,
			StateID:  "",
			StateKey: "",
		})
		return true
	}
	return false
}

func (a *Adaptor) getStateKey(taskCtx taskcontext.Context, req *filter.RequestPb, metaData *metadata.Metadata) (string,
	bool) {
	var stateKey string
	var retErr common.TaskError
	isReuseStateKey := false
	log.GetLogger().Debugf("getStateKey info, tenantId:%s, serviceId:%s, funcName:%s, key:%s",
		taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName, req.Key)
	if len(req.Key) == 0 && len(req.PodName) != 0 {
		clientInstance := bridgetypes.ClientInstance{
			PodName: req.PodName,
			FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID:       taskCtx.TenantID(),
				ServiceID:      taskCtx.ServiceID(),
				BridgeFuncName: req.FuncName,
			},
		}

		if stateKey, retErr = util.GetFuncStateKey(taskCtx, req.FuncName, clientInstance); retErr != nil {
			taskContextSetError(taskCtx, metaData, retErr)
			return "", true
		}
		req.Key = stateKey
		stateID := utils.GenStateIDByKey(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName, req.Key)
		isReuseStateKey = !isNewStateKeyScenario(taskCtx, stateID, stateKey)
	} else if len(req.Key) != 0 && len(req.PodName) != 0 {
		stateID := utils.GenStateIDByKey(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName, req.Key)
		clientInstance := bridgetypes.ClientInstance{
			PodName:        req.PodName,
			AssignStateID:  stateID,
			AssignStateKey: req.Key,
			FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID:       taskCtx.TenantID(),
				ServiceID:      taskCtx.ServiceID(),
				BridgeFuncName: req.FuncName,
			},
		}

		if _, retErr = util.GetFuncStateKey(taskCtx, req.FuncName, clientInstance); retErr != nil {
			taskContextSetError(taskCtx, metaData, retErr)
			return "", true
		}
		isReuseStateKey = !isNewStateKeyScenario(taskCtx, stateID, req.Key)
	}
	return req.Key, false || isReuseStateKey
}

// If the state exists or err occurs, false is returned. If the state does not exist, true is returned.
func checkStateIsNotExistDuringCreatingState(taskCtx taskcontext.Context, stateID string, req *filter.RequestPb) bool {
	if exist, err := isStateExist(taskCtx, req.FuncName, stateID); exist || err != nil {
		if err != nil {
			log.GetLogger().Errorf("isStateExist error during creating state, traceID: %s, funcName: %s, stateID: %s,"+
				"code: %d, err: %s", taskCtx.TraceID(), req.GetFuncName(), stateID, err.Code(), err.Error())
			taskCtx.SetResult(&filter.Response{
				Code:    int32(err.Code()),
				Message: err.Error(),
			})
			return false
		}
		if exist && len(req.PodName) == 0 {
			taskCtx.SetResult(&filter.Response{
				Code:    snerror.InstanceNameExist,
				Message: snerror.ErrText(snerror.InstanceNameExist),
			})
			return false
		}

		taskCtx.SetResult(&filter.Response{
			Code:     0,
			StateKey: req.Key,
			StateID:  stateID,
		})
		return false
	}
	return true
}

func createStateQueue(taskCtx taskcontext.Context, metaData *metadata.Metadata, req *filter.RequestPb) {
	log.GetLogger().Infof("createStateQueue start, traceID: %s, stateID: %s", taskCtx.TraceID(), metaData.StateID)
	taskCtx.StateQueueMap().CheckAndClearOldStateQueue(metaData)
	err := taskCtx.StateQueueMap().CreateStateQueue(metaData)
	if err != nil {
		taskContextSetError(taskCtx, metaData, err)
		return
	}

	snErr := taskutil.SaveStateInRedis(taskCtx.TenantID(), taskCtx.ServiceID(),
		metaData.FunctionName, metaData.StateID, metaData.StateKey)

	if snErr != nil {
		if len(req.PodName) != 0 {
			// only service bridge need do Release
			doRelease(taskCtx, req)
		}
		log.GetLogger().Errorf("createStateQueue error, traceID: %s, stateID: %s, code: %d, err: %s", taskCtx.TraceID(),
			metaData.StateID, snErr.Code(), snErr.Error())
		taskCtx.SetResult(&filter.Response{
			Code:    int32(snErr.Code()),
			Message: snErr.Error(),
		})
		return
	}
	taskCtx.SetResult(&filter.Response{
		Code:     common.EOK,
		StateID:  metaData.StateID,
		StateKey: metaData.StateKey,
	})
	log.GetLogger().Infof("createStateQueue end, traceID: %s, stateID: %s", taskCtx.TraceID(), metaData.StateID)
}

func isStateExist(taskCtx taskcontext.Context, funcName, stateID string) (bool, snerror.SNError) {
	stateHashKey := metadata.GeneStateHashKey(taskCtx.TenantID(), taskCtx.ServiceID(), funcName)
	log.GetLogger().Debugf("isStateExist, traceID: %s, funcName: %s, stateHashKey: %s, stateID: %s"+
		", getInstanceScenario: %d", taskCtx.TraceID(), funcName, stateHashKey, stateID,
		taskCtx.IsGetInstanceScenario())
	existInRedis, err := stateclient.HExists(stateHashKey, stateID)
	log.GetLogger().Debugf("Get from redis finish, "+
		"traceID: %s, funcName: %s, stateHashKey: %s, stateID: %s, existInRedis: %v",
		taskCtx.TraceID(), funcName, stateHashKey, stateID, existInRedis)
	if err != nil && !taskCtx.IsGetInstanceScenario() {
		log.GetLogger().Errorf("isStateExist Error, traceID: %s, funcName: %s, stateID: %s, code: %d, err: %s",
			taskCtx.TraceID(), funcName, stateID, err.Code(), err.Error())
		return false, err
	}
	// compared with redis record and etcd local cache record, if not the same, double check by meta etcd
	existInETCD := CheckCacheConsistencyAndRepair(taskCtx, existInRedis, funcName, stateID)
	log.GetLogger().Infof("isStateExist finish,"+
		"traceID: %s, funcName: %s, stateHashKey: %s, stateID: %s, existInRedis: %v, existInETCD: %v",
		taskCtx.TraceID(), funcName, stateHashKey, stateID, existInRedis, existInETCD)
	// fix the redis record
	if existInRedis != existInETCD {
		if !existInRedis {
			recoverStateHashKey(stateHashKey, stateID, taskCtx)
		} else {
			rollbackStateInfoInRedis(taskCtx, funcName, stateID)
		}
	}
	return existInETCD, nil
}

// CheckCacheConsistencyAndRepair -
func CheckCacheConsistencyAndRepair(taskCtx taskcontext.Context, existInRedis bool,
	funcName, stateID string) bool {
	var isStateExistInETCD bool
	// according to the latest stateful structure for flow calculation, we thought ETCD is right
	// if we find existInRedis not equal to existInETCD (local cache), query in MetaETCD and return existInRealETCD
	existInETCD := servicediscovery.GetRegistry().IsStateInsExist(stateID)
	isStateExistInETCD = existInETCD
	log.GetLogger().Infof("get IsStateInsExist from local cache finish,"+
		"traceID: %s, funcName: %s, stateID: %s, existInRedis: %v, existInETCD: %v",
		taskCtx.TraceID(), funcName, stateID, existInRedis, existInETCD)
	if existInRedis != existInETCD {
		existInRealETCD, realValueETCD, err := isExistRealInETCD(stateID)
		if err != nil {
			log.GetLogger().Warnf("get state: %s from etcd error: %s, set existInRealETCD as: %v",
				stateID, err.Error(), existInRealETCD)
			return existInRealETCD
		}
		log.GetLogger().Infof("double check state isExist from etcd finish,"+
			"traceID: %s, funcName: %s, stateID: %s, existInRedis: %v, existInRealETCD: %v",
			taskCtx.TraceID(), funcName, stateID, existInRedis, existInRealETCD)
		stateRecoveryConfig := coretypes.StateRecoveryConfig{
			CreateStateConfig: coretypes.CreateStateConfig{
				TenantID:      taskCtx.TenantID(),
				SnServiceID:   taskCtx.ServiceID(),
				ProxyFunction: coreconfig.NameOfFunctionTask,
				TraceID:       taskCtx.TraceID(),
			},
			FuncName:         funcName,
			StateID:          stateID,
			RealValue:        realValueETCD,
			LocalExistInETCD: existInETCD,
			RealExistInETCD:  existInRealETCD,
		}
		servicediscovery.GetRegistry().StateRecovery(stateRecoveryConfig)
		isStateExistInETCD = existInRealETCD
	}
	return isStateExistInETCD
}

func isExistRealInETCD(stateID string) (bool, string, error) {
	// if not exist in local storage need search in etcd
	// because the etcd information may not be modified to the local host in a timely manner through the way of listen
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient.Client == nil {
		return false, "", errors.New("failed to init state ETCD client")
	}
	etcdPath := taskutil.GetStateETCDPath(stateID)
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	values, err := stateClient.GetValues(defaultEtcdCtx, etcdPath)

	if err != nil {
		return false, "", err
	}

	if len(values) < 1 {
		return false, "", nil
	}

	newStates := make(map[string]string, mapInitSize)
	err = json.Unmarshal([]byte(values[0]), &newStates)
	if err != nil {
		log.GetLogger().Warnf("isExistRealInETCD failed to unmarshal data: %s", err.Error())
		return false, "", err
	}

	if val, ok := newStates[stateID]; ok {
		return true, val, nil
	}

	return false, "", nil
}

func isStatefulFunctionCheck(taskCtx taskcontext.Context, args *FuncArgs) bool {
	req := args.GetFilterReq()
	if !servicediscovery.GetRegistry().IsFunctionExist(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName,
		commutils.DefaultVersion) {
		taskCtx.SetResult(&filter.Response{
			Code:    snerror.NoSuchFunction,
			Message: snerror.ErrText(snerror.NoSuchFunction),
		})
		return false
	}
	// the stateless function returns an empty stateID.
	if !servicediscovery.GetRegistry().IsStatefulFunction(taskCtx.TenantID(), taskCtx.ServiceID(), req.FuncName,
		commutils.DefaultVersion) {
		if len(req.Key) != 0 {
			taskCtx.SetResult(&filter.Response{
				Code:    snerror.CreateStateByStatelessFunction,
				Message: snerror.ErrText(snerror.CreateStateByStatelessFunction),
			})
			return false
		}
		taskCtx.SetResult(&filter.Response{
			Code:     common.EOK,
			StateID:  "",
			StateKey: "",
		})
		return false
	}
	return true
}

func (a *Adaptor) createStateQueue(taskCtx taskcontext.Context, args *FuncArgs) {
	req := args.GetInvokeTaskCreateStateReq()
	resp := req.CreateStateQueue(taskCtx)
	taskCtx.SetResult(resp)
}

func taskContextSetError(taskCtx taskcontext.Context, metaData *metadata.Metadata, err common.TaskError) {
	log.GetLogger().Errorf("createStateQueue, traceID: %s, stateID: %s, code: %d, err: %s", taskCtx.TraceID(),
		metaData.StateID, err.Code(), err.Error())
	taskCtx.SetResult(&filter.Response{
		Code:    err.Code(),
		Message: err.Error(),
	})
}

func recoverStateHashKey(stateHashKey, stateID string, taskCtx taskcontext.Context) {
	_, err := stateclient.HSetNX(stateHashKey, stateID, taskCtx.GetStateKey())
	if err != nil {
		log.GetLogger().Warnf("HSet stateHashKey: %s field stateID: %s into redis failed: %s",
			stateHashKey, stateID, err.Error())
		return
	}
	log.GetLogger().Infof("success to recover stateHashKey: %s into redis with stateID: %s",
		stateHashKey, stateID)
	return
}

func rollbackStateInfoInRedis(taskCtx taskcontext.Context, funcName, stateID string) {
	var err error
	stateHashKey := metadata.GeneStateHashKey(taskCtx.TenantID(), taskCtx.ServiceID(), funcName)
	err = taskutil.DeleteStateInRedis(taskCtx.TenantID(), taskCtx.ServiceID(), funcName, stateID)
	if err != nil {
		log.GetLogger().Warnf("synchro state: %s rollback, delete stateHashKey: %s from redis failed: %s",
			stateID, stateHashKey, err.Error())
	}
	if config.Conf.PersistentState {
		stateValueKey := metadata.Gen(taskCtx.TenantID(), taskCtx.ServiceID(), metadata.StateIDPrefix, stateID)
		err = sdk.DeleteState(stateValueKey)
		if err != nil {
			log.GetLogger().Warnf("synchro state: %s rollback, delete stateKey: %s from redis failed: %s",
				stateID, stateValueKey, err.Error())
		}
	}
	return
}
