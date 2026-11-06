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

// Package sdk implements function core sdk
package sdk

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/core-sdk/common"
	"functioncore/pkg/core-sdk/communication"
	configs "functioncore/pkg/core-sdk/config"
	"functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functionbus/bus"
	buscommon "functioncore/pkg/functionbus/common"
	commonutils "functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy"
	"functioncore/pkg/functionbus/proxy/channelbusconn"
	"functioncore/pkg/functionbus/proxy/ratelimit"
	"functioncore/pkg/functionbus/proxy/statedispatcher"
	"functioncore/pkg/functionbus/utils"
	sc "functioncore/pkg/functionstate/client"
	"functioncore/pkg/functiontask/task/stateclient"
)

var (
	realAdaptor      AbsAdaptor
	etcdHandler      buscommon.ETCDConnectHandler
	channelBus       bus.CommunicationBus
	registerInfo     RegisterInfo
	stateClient      *sc.StateClient
	snServiceIDMatch = regexp.MustCompile(snServiceIDReg)
)

const (
	maxStateLength       = 8 * 1024 * 1024
	maxRequestLength     = 18 * 1024 * 1024
	maxCallTimeOut       = 100 * 24 * 3600 * time.Second
	minCallTimeout       = 5 * time.Second
	minTTL               = 60            // seconds
	maxTTL               = math.MaxInt32 // seconds
	defaultConns         = 8
	zeroTenantID         = "0"
	snServiceIDReg       = "^[a-zA-Z0-9]{1,16}$"
	siteOfSNID           = 1
	lengthOfSNID         = 2
	maxHTTPRetryAttempts = 6
	defaultRetryDelay    = 3
	logFileName          = "coresdk"
	taskFuncName         = "function-task"
	requestLayer         = "requestLayer"
	megabyte             = 1024 * 1024
)

// MsgConfig keeps the configuration of message
type MsgConfig struct {
	Timeout           time.Duration    // Single request timeout
	FuncCallID        uint64           // msg request/response id（not used）
	SrcFuncTenant     string           // caller(task) manage it
	DestFuncTenant    string           // tenant ID of destination function
	FuncVersion       string           // function version, default $latest
	NodeID            string           // not used
	SN                string           // not used
	LogType           string           // LogType in HookArgs
	SNServiceID       string           // sn service id
	TraceID           string           // traceid
	FutureID          string           // futureID of invocation
	RequestAttributes map[string]int32 // request attributes from fdk
	InstanceID        string           // used for invoke specific instance
	SrcFuncUrn        string           // funcName of caller
	SrcPodName        string           // podName of caller
	ExtraMap          map[string]string
}

// Result keeps the result of Call()
type Result struct {
	Reply       interface{}
	ErrCode     uint32
	ErrMsg      string
	DcCallStack []byte
	Log         string
}

// HookArgs keeps args of hook
type HookArgs struct {
	FuncArgs        []byte // Call() request in worker
	SrcTenant       string
	DstTenant       string
	StateID         string
	LogType         string
	StateKey        string // for trigger state call
	FunctionVersion string // for trigger state call
	ExternalRequest bool   // for trigger state call
	SNServiceID     string
	TraceID         string
	InstanceID      string
	SrcFuncUrn      string
	SrcPodName      string
}

// AbsAdaptor -
type AbsAdaptor func(string, uint64, *HookArgs) error

// SaveOption -
type SaveOption struct {
	TTL int `json:"seconds"`
}

// StateClientConfig keeps config for state client initialization
type StateClientConfig struct {
	ServerAddr             string
	ServerMode             string
	EnableTLS              bool
	Password               string
	AKey                   string
	SKey                   string
	ExpiredCb              func(string) // stateID
	GetRealTimeStateServer func() (string, redisclient.TimeoutConf, error)
}

// RegisterInfo keeps register information for function
type RegisterInfo struct {
	FunctionName       string
	FunctionVersion    string
	FunctionInstanceID string
	Tenant             string
	IP                 string
}

// NodeInfo keeps the information return to task
type NodeInfo struct {
	NodeIP string
	NodeID string
}

// FunctionCheck used as a callback function for the task to check function from the proxy interface
var FunctionCheck = &proxy.Verification{
	GetStatefulFunctionConcurrentNum: func(tenantID, functionName, serviceID string) (int, error) {
		return 1, nil
	},
}

// StateQueueMetadataChannel used to store stateful func metadata info
var StateQueueMetadataChannel chan commonutils.StatefulFuncMetaData

// Init bus and ondemand
func Init(stopCh <-chan struct{}) error {
	err := log.InitRunLog(logFileName, true)
	if err != nil {
		log.GetLogger().Errorf("init logger error: %s", err.Error())
		return err
	}
	err = configs.GetConfig()
	if err != nil {
		return err
	}
	log.GetLogger().Debugf("start to init sdk, functionName:%s, functionInstanceID:%s",
		registerInfo.FunctionName, registerInfo.FunctionInstanceID)
	config := proxy.BusProxyConfig{
		CallbackETCDHandler: etcdHandler,
	}
	var regInfo = channelbusconn.RegisterInfo{
		FuncName:        registerInfo.FunctionName,
		NodeID:          registerInfo.FunctionInstanceID,
		FunctionVersion: registerInfo.FunctionVersion,
		Tenant:          registerInfo.Tenant,
		WorkerInstance:  channelbusconn.WorkerInstance{},
	}
	if err = proxy.InitBusProxy(config, stopCh, regInfo, FunctionCheck); err != nil {
		log.GetLogger().Errorf("failed to init bus proxy, error: %s", err.Error())
		return err
	}

	// create channel bus that connect with proxy
	channelBusOpt := &buscommon.ChannelBusOption{IsProxy: false}
	channelBus, err = bus.NewBus(buscommon.ChannelBusType, buscommon.BusOption{NewBusOpt: channelBusOpt})
	if err != nil {
		log.GetLogger().Errorf("failed to new bus, error: %s", err.Error())
		return err
	}
	if channelBus == nil {
		return errors.New("channel bus is nil")
	}
	// channel starts to receive message
	channelBus.StartRecvMessage()

	if err = channelBus.RegisterBus(buscommon.FunctionProperty{
		TenantID:        registerInfo.Tenant,
		FunctionName:    registerInfo.FunctionName,
		FunctionVersion: registerInfo.FunctionVersion,
		InstanceID:      registerInfo.FunctionInstanceID,
	}); err != nil {
		log.GetLogger().Errorf("failed to register bus client, error: %s", err.Error())
		return err
	}

	StateQueueMetadataChannel = statedispatcher.GetStateQueueMetadataChannel()
	log.GetLogger().Debugf("finished sdk initiation")
	return nil
}

// RegEtcdConnStatusHandler register etcd connection fail handler
func RegEtcdConnStatusHandler(handler func(bool)) {
	etcdHandler = handler
}

func checkParameter(destFuncName string, args []byte, config MsgConfig) (uint32, string) {
	// check the format of destFuncName
	if !(common.FuncNameMatch.MatchString(destFuncName) || common.FuncNameAliasMatch.MatchString(destFuncName)) {
		return common.FuncNameInvalidErrCode, common.FuncNameInvalidErrMsg
	}

	// check the len of args
	if len(args) > maxRequestLength {
		return common.ArgsLengthInvalidErrCode, common.ArgsLengthInvalidErrMsg
	}

	// check timeout
	if config.Timeout < minCallTimeout || config.Timeout > maxCallTimeOut {
		return common.TimeoutInvalidErrCode, common.TimeoutInvalidErrMsg
	}
	return 0, ""
}

func makeResult(errCode uint32, errMsg string, funcName string, funcCallID uint64) Result {
	if errMsg != "" || errCode != uint32(0) {
		log.GetLogger().Errorf("failed to call function, error:%v %s, funcName:%s, funcCallID:%d", errCode,
			errMsg, funcName, funcCallID)
	}
	return Result{
		ErrCode: errCode,
		ErrMsg:  errMsg,
	}
}

func getPriority(requestAttributes map[string]int32, key string) int {
	if requestAttributes == nil {
		return 0
	}

	if value, ok := requestAttributes[key]; ok {
		return int(value)
	}

	return 0
}

// CallOnDemand call function on specified node
func CallOnDemand(args types.CallArgs, nodeID, stateID string, raw []byte, config MsgConfig) Result {
	log.GetLogger().ForDebugf("call on demand function %s on node %s, stateID %s, traceID: %s, isInvokeAll: %t",
		args.FuncName, nodeID, stateID, config.TraceID, args.IsInvokeAll)
	config.NodeID = nodeID
	return Call(args.FuncName, stateID, raw, config, args.IsInvokeAll)
}

// Call function
func Call(destFuncName string, stateID string, args []byte, config MsgConfig, isInvokeAll bool) Result {
	log.GetLogger().Debugf("started to call sdk, call:%s, stateID:%s, funcCallID:%d, srcTenant:%s, "+
		"destTenant:%s, RequestAttributes: %v, isInvokeAll: %t", destFuncName, stateID, config.FuncCallID,
		config.SrcFuncTenant, config.DestFuncTenant, config.RequestAttributes, isInvokeAll)
	errCode, errMsg := checkParameter(destFuncName, args, config)
	if errCode != 0 {
		log.GetLogger().Warnf("failed to call check parameter, error: %d", errCode)
		return makeResult(errCode, errMsg, destFuncName, config.FuncCallID)
	}
	// if the send call is not a function task
	if config.DestFuncTenant != zeroTenantID && config.SNServiceID != "" {
		destFuncName = config.SNServiceID + urnutils.Separator + destFuncName
		log.GetLogger().Debugf("caller is not a function task, destFuncName:%s", destFuncName)
	}
	// check stateID
	if destFuncName == registerInfo.FunctionName && stateID == "" {
		return makeResult(common.StateIDEmptyErrCode, common.StateIDEmptyErrMsg, destFuncName, config.FuncCallID)
	}
	if config.FuncVersion != "$latest" && config.FuncVersion != "" {
		destFuncName = destFuncName + ":" + config.FuncVersion
	}

	if destFuncName != taskFuncName && !ratelimit.GetInvokeLimiter().Take(config.TraceID) {
		return makeResult(common.InvokeRateLimitErrCode, common.InvokeRateLimitErrMsg,
			destFuncName, config.FuncCallID)
	}

	return callByBus(destFuncName, stateID, args, config, isInvokeAll)
}

func callByBus(destFuncName string, stateID string, args []byte, config MsgConfig, isInvokeAll bool) Result {
	dst := &buscommon.FunctionProperty{
		TenantID:        config.DestFuncTenant,
		FunctionName:    destFuncName,
		FunctionVersion: "",
		StateID:         stateID,
		FutureID:        config.FutureID,
		InstanceID:      "from-coresdk",
	}
	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, config.TraceID)
	ctx, cancel := context.WithTimeout(ctx, config.Timeout)
	defer cancel()

	userData := map[string]string{
		constants.SNServiceIDFlag: config.SNServiceID,
		constants.SrcTenantFlag:   config.SrcFuncTenant,
		constants.SrcFuncUrnFlag:  config.SrcFuncUrn,
		constants.SrcPodNameFlag:  config.SrcPodName,
	}
	if channelBus == nil {
		log.GetLogger().Errorf("channelBus is nil")
		return makeResult(common.InternalErrCode, common.InternalErrMsg, destFuncName, config.FuncCallID)
	}
	isStateful := checkStateful(destFuncName, stateID, config)
	// get cpu and memory if needed
	memory := getPriority(config.RequestAttributes, common.RequestAttributesMemory)
	requestMemory := strconv.Itoa(memory)
	log.GetLogger().Debugf("requestMemory is %s", requestMemory)
	tcpRequest := &commontypes.TCPRequest{
		LogType:     config.LogType,
		RawData:     args,
		RequestID:   config.FuncCallID,
		UserData:    userData,
		Priority:    getPriority(config.RequestAttributes, common.RequestAttributesLayer),
		IsStateFul:  isStateful,
		InstanceID:  config.InstanceID,
		IsInvokeAll: isInvokeAll,
		ExtraMap:    config.ExtraMap,
	}
	if requestMemory != "0" {
		tcpRequest.CPU = "0"
		tcpRequest.Memory = requestMemory
	}
	busRsp, err := channelBus.Call(ctx, dst, tcpRequest, false)
	if err != nil {
		log.GetLogger().Errorf("channelBus call error %s", err.Error())
		return makeResult(common.InternalErrCode, err.Error(), destFuncName, config.FuncCallID)
	}
	rsp := Result{
		ErrCode:     busRsp.ErrorCode,
		ErrMsg:      busRsp.ErrorMessage,
		DcCallStack: busRsp.DcCallStack,
		Log:         busRsp.Logs,
		Reply:       busRsp.RawData,
	}
	log.GetLogger().Debugf("finished calling sdk, destFuncName:%s, funcCallID:%d, errCode:%d", destFuncName,
		config.FuncCallID, rsp.ErrCode)
	return rsp
}

// SetResult set result of Call
func SetResult(invokeID uint64, errCode int32, result []byte, logs string) ([]byte, error) {
	log.GetLogger().Debugf("start sdk SetResult, funcCallID:%d, errCode:%d", invokeID, errCode)
	if channelBus == nil {
		return result, errors.New("channelBus is nil")
	}

	maxRespLength := maxRequestLength
	maxInvokeRespBodySize := atomic.LoadInt64(&commonutils.FConfig.MaxInvokeRespBodySize)
	if maxInvokeRespBodySize != 0 {
		maxRespLength = int(maxInvokeRespBodySize) * megabyte
	}

	if len(result) > maxRespLength {
		log.GetLogger().Warnf("set result error: result is too large than %d, length: %d, funcCallID: %d",
			commonutils.FConfig.MaxInvokeRespBodySize*megabyte, len(result), invokeID)
		// return result is too large
		errCode = int32(common.ArgsLengthInvalidErrCode)
		result = []byte(common.ArgsLengthInvalidErrMsg)
	}

	err := channelBus.SetResult(constructCallResponse(invokeID, errCode, result, logs))
	if err != nil {
		log.GetLogger().Warnf("set result error %s", err.Error())
		return result, err
	}
	log.GetLogger().Debugf("finish sdk SetResult, funcCallID: %d", invokeID)
	return result, nil
}

func constructCallResponse(requestID uint64, errCode int32, message []byte, logs string) *commontypes.TCPResponse {
	if errCode != 0 {
		return &commontypes.TCPResponse{
			ErrorCode:    uint32(errCode),
			ErrorMessage: string(message),
			Logs:         logs,
			RequestID:    requestID,
		}
	}
	return &commontypes.TCPResponse{
		ErrorCode: uint32(errCode),
		RawData:   message,
		Logs:      logs,
		RequestID: requestID,
	}
}

// RegMsgHandler handle Bus server register
func RegMsgHandler(adaptor AbsAdaptor) error {
	if adaptor == nil {
		return errors.New("adaptor is nil")
	}
	if channelBus == nil {
		return errors.New("please call functioncore RegFuncInfo first")
	}
	realAdaptor = adaptor
	channelBus.RegisterCallback(BusAdaptor)
	log.GetLogger().Debugf("finish sdk callback register")
	return nil
}

// BusAdaptor actual callBack of bus
func BusAdaptor(ctx context.Context, dst *buscommon.FunctionProperty, req *commontypes.TCPRequest) error {
	traceID := utils.ContextValueString(ctx, constants.HeaderTraceID)
	log.GetLogger().Debugf("start sdk bus adaptor process funcCallID:%d, traceID:%s", req.RequestID, traceID)
	if req.UserData == nil {
		log.GetLogger().Errorf("user data is empty, traceID: %s", traceID)
		return errors.New("user data is empty")
	}
	snServiceID := req.UserData[constants.SNServiceIDFlag]
	if snServiceID == "" {
		log.GetLogger().Warnf("SN ServiceID got from user data is empty")
	}
	isExternal := false
	if req.UserData[constants.FrontendCallFlag] != "" {
		isExternal = true
	}
	srcTenant := req.UserData[constants.SrcTenantFlag]
	stateKey := req.UserData[constants.StateKeyFlag]
	stateID := req.UserData[constants.StateIDFlag]
	srcFuncName := req.UserData[constants.SrcFuncUrnFlag]
	srcPodName := req.UserData[constants.SrcPodNameFlag]
	if stateID != "" {
		dst.StateID = stateID
	}
	functionName := dst.FunctionName
	hookArgs := &HookArgs{
		FuncArgs:        req.RawData,
		SrcTenant:       srcTenant,
		DstTenant:       dst.TenantID,
		StateID:         dst.StateID,
		LogType:         req.LogType,
		FunctionVersion: dst.FunctionVersion,
		ExternalRequest: isExternal,
		SNServiceID:     snServiceID,
		TraceID:         traceID,
		StateKey:        stateKey,
		InstanceID:      req.InstanceID,
		SrcFuncUrn:      srcFuncName,
		SrcPodName:      srcPodName,
	}
	startTime := time.Now()
	log.GetLogger().ForDebugf("start sdk adaptor process, funcCallID:%v, StateKey:%s, StateID:%s, SrcTenant:%s,"+
		" traceId: %s", req.RequestID, stateKey, dst.StateID, srcTenant, traceID)
	err := realAdaptor(functionName, req.RequestID, hookArgs)
	if err != nil {
		log.GetLogger().Errorf("sdk adaptor process failed %s, funcCallID:%d", err.Error(), req.RequestID)
		return err
	}
	log.GetLogger().ForDebugf("finished sdk adaptor process, funcCallID:%d, traceID:%s, cost time: %s",
		req.RequestID, traceID, time.Since(startTime))
	return nil
}

// RegFuncInfo register function information and create bus client
func RegFuncInfo(info RegisterInfo) error {
	// check the format of funcName
	if !(common.FuncNameMatch.MatchString(info.FunctionName) || common.FuncNameAliasMatch.MatchString(
		info.FunctionName)) {
		log.GetLogger().Errorf("register function info error:funcName is invalid, funcName is %s",
			info.FunctionName)
		return fmt.Errorf("funcName is invalid, funcName is %s", info.FunctionName)
	}
	registerInfo = info
	if info.FunctionVersion == "" {
		registerInfo.FunctionVersion = "$latest"
	} else {
		registerInfo.FunctionVersion = info.FunctionVersion
	}

	log.GetLogger().Infof("receive RegFuncInfo funcName:%s, funcInstanceID:%s", info.FunctionName,
		info.FunctionInstanceID)
	return nil
}

// InitStateClient init State
func InitStateClient(config StateClientConfig, stopCh <-chan struct{}) error {
	log.GetLogger().Debugf("start sdk InitStateClient, server addr:%s", config.ServerAddr)
	sConfig := sc.StateClientConfig{}
	sConfig.ServerAddr = config.ServerAddr
	sConfig.ServerMode = config.ServerMode
	sConfig.EnableTLS = config.EnableTLS
	sConfig.ConcurrentConns = defaultConns
	sConfig.ExpiredCb = config.ExpiredCb
	sConfig.Password = config.Password
	sConfig.AKey = config.AKey
	sConfig.SKey = config.SKey
	sConfig.GetRealTimeStateServer = config.GetRealTimeStateServer
	client, err := sc.CreateStateClient(sConfig, stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to init state client, error: %s", err.Error())
		return err
	}
	stateClient = client
	stateclient.StateCli = client
	log.GetLogger().Debugf("finish initiating StateClient, server addr:%s", config.ServerAddr)
	return nil
}

// SetCmdAndArg represents cmd and args for set.
type SetCmdAndArg struct {
	Key   string
	Value string
	TTL   int
}

// SaveStateAndResult save state and result.
func SaveStateAndResult(stateCmd, resultCmd SetCmdAndArg) snerror.SNError {
	if stateClient == nil {
		log.GetLogger().Errorf("failed to get state, error: stateClient is nil")
		return snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	err := stateClient.Transaction(sc.DoArg{
		"SET", stateCmd.Key, stateCmd.Value, "ex", stateCmd.TTL,
	}, sc.DoArg{
		"SET", resultCmd.Key, resultCmd.Value, "ex", resultCmd.TTL,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to SaveStateAndResult, error: %s", err.Error())
		return snerror.New(common.StateClientErrCode, err.Error())
	}
	sc.SubscribeExpiredKey(stateCmd.Key, stateCmd.TTL)
	return nil
}

// SetKeyWithTTL set key with ttl
func SetKeyWithTTL(key, value string, ttl int) snerror.SNError {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	if err := stateClient.SetKeyWithTTL(key, value, ttl); err != nil {
		log.GetLogger().Errorf("set key with ttl error, key: %s, error: %s", key, err.Error())
		return snerror.New(common.StateClientErrCode, err.Error())
	}

	return nil
}

// SaveState save state
func SaveState(stateID string, state string, option *SaveOption) snerror.SNError {
	// check the length of state
	if len(state) > maxStateLength {
		log.GetLogger().Errorf("failed to save state, error: state is too large", zap.String("stateID", stateID))
		return snerror.New(common.StateClientErrCode, "state is too large")
	}

	sOption := &sc.SaveOption{}
	if option.TTL < minTTL || option.TTL > maxTTL {
		log.GetLogger().Errorf("failed to save state, error: TTL is invalid, it should be in range "+
			"[60, MaxInt32]", zap.String("stateID", stateID))
		return snerror.New(common.InternalErrCode, "TTL is invalid, it should be in range [60, MaxInt32]")
	}
	sOption.TTL = option.TTL
	if stateClient == nil {
		log.GetLogger().Errorf("save state error: stateClient is nil", zap.String("stateID", stateID))
		return snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	err := stateClient.SaveState(stateID, state, sOption)
	if err != nil {
		log.GetLogger().Errorf("save state error: %s, stateID: %s", err.Error(), stateID)
		return snerror.New(common.StateClientErrCode, "save state error")
	}
	return nil
}

// ResetExpiredTTL reset expired ttl of id
func ResetExpiredTTL(id string, ttlInt int) snerror.SNError {
	log.GetLogger().Debugf("start sdk ResetExpiredTTL, stateID: %s", id)

	if ttlInt < minTTL || ttlInt > maxTTL {
		log.GetLogger().Errorf("failed to reset expired ttl, error: TTL is invalid, it should be in range " +
			"[60, MaxInt32]")
		return snerror.New(common.InternalErrCode, "TTL is invalid, it should be in range [60, MaxInt32]")
	}

	if stateClient == nil {
		log.GetLogger().Errorf("failed to reset expired ttl, error: state client is nil")
		return snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}

	if err := stateClient.ResetExpiredTTL(id, ttlInt); err != nil {
		log.GetLogger().Errorf("failed to reset expired ttl, error: %s", err.Error())
		return snerror.New(common.StateClientErrCode, err.Error())
	}
	log.GetLogger().Debugf("finish resetExpiredTTL, stateID: %s", id)
	return nil
}

// ScanState scan state info
func ScanState(pattern string) (map[string]string, snerror.SNError) {
	log.GetLogger().Debugf("scan state info, pattern: %s", pattern)
	if stateClient == nil {
		log.GetLogger().Errorf("ScanState error: stateClient is nil")
		return nil, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	scanRes, err := stateClient.HGetAll(pattern)
	if err != nil {
		return nil, snerror.New(common.StateClientErrCode, err.Error())
	}
	return scanRes, nil
}

// IsNotExist check if error is not exist error
func IsNotExist(err error) bool {
	return sc.IsNotExist(err)
}

// GetState get state
func GetState(stateID string) (string, snerror.SNError) {
	log.GetLogger().Debugf("start sdk GetState, stateID: %s", stateID)
	if stateClient == nil {
		log.GetLogger().Errorf("failed to get stateID %s, error: stateClient is nil", stateID)
		return "", snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	res, err := stateClient.RestoreState(stateID)
	if err != nil {
		log.GetLogger().Errorf("failed to get state, stateID: %s, error: %s", stateID, err.Error())
		return "", snerror.New(common.StateClientErrCode, err.Error())
	}
	log.GetLogger().Debugf("finish getting state, stateID: %s", stateID)
	return res, nil
}

// CreateState create state
func CreateState(funcName, stateID string, config types.CreateStateConfig) (string, string, snerror.SNError) {
	log.GetLogger().Debugf("start to create state, funcName: %s, stateID: %s, traceID: %s",
		funcName, stateID, config.TraceID)
	// check the format of funcName
	if !(common.FuncNameMatch.MatchString(funcName) || common.FuncNameAliasMatch.MatchString(funcName)) {
		log.GetLogger().Errorf("create state error: funcName is invalid, funcName is %s, stateID: %s", funcName, stateID)
		return "", "", snerror.New(common.InternalErrCode, "funcName invalid")
	}

	// combine function name with sn service id
	funcName = urnutils.NewComplexFuncName(config.SnServiceID, funcName).String()

	receivedStateID, receivedNodeID, err := communication.CreateStateByHTTP(funcName, stateID, config)
	if err != nil {
		return "", "", err
	}
	log.GetLogger().Debugf("finished creating state, funcName: %s, stateID: %s, traceID: %s",
		funcName, receivedStateID, config.TraceID)
	return receivedStateID, receivedNodeID, nil
}

// DeleteState delete state
func DeleteState(stateID string) snerror.SNError {
	log.GetLogger().Debugf("start sdk DeleteState, stateID: %s", stateID)
	if stateClient == nil {
		log.GetLogger().Errorf("delete state error: stateClient is nil", zap.String("stateID", stateID))
		return snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	err := stateClient.DeleteState(stateID)
	if err != nil {
		log.GetLogger().Errorf("delete state error: %s, stateID: %s", err.Error(), stateID)
		return snerror.New(common.StateClientErrCode, err.Error())
	}
	log.GetLogger().Debugf("finished deleting state, stateID: %s", stateID)
	return nil
}

// SAdd add key and value by state client
func SAdd(key string, value string) (int, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return 0, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.SAdd(key, value)
	if err != nil {
		log.GetLogger().Errorf("state client sadd method exec error. key: %s, error: %s", key, err)
		return 0, snerror.New(common.StateClientErrCode, err.Error())
	}

	return result, nil
}

// SRem remove key and value by state client
func SRem(key string, value string) (int, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return 0, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.SRem(key, value)
	if err != nil {
		log.GetLogger().Errorf("state client srem method exec error. key: %s, error: %s", key, err)
		return 0, snerror.New(common.StateClientErrCode, err.Error())
	}

	return result, nil
}

// SMembers list all values of key by state client
func SMembers(key string) ([]string, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return nil, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.SMembers(key)
	if err != nil {
		log.GetLogger().Errorf("state client smembers method exec error. key: %s, error: %s", key, err)
		return nil, snerror.New(common.StateClientErrCode, err.Error())
	}

	return result, nil
}

// SIsMember if key and value is exists in set return true
func SIsMember(key string, value string) (bool, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return false, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.SIsMember(key, value)
	if err != nil {
		return false, snerror.New(common.StateClientErrCode, err.Error())
	}
	return result, nil
}

// SCard get nums of key's value by state client
func SCard(key string) (int, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return 0, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.SCard(key)
	if err != nil {
		log.GetLogger().Errorf("state client SCard method exec error. key: %s, error: %s", key, err)
		return 0, snerror.New(common.StateClientErrCode, err.Error())
	}

	return result, nil
}

// Exists return true if key is exists
func Exists(key string) (bool, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return false, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.Exists(key)
	if err != nil {
		log.GetLogger().Errorf("state client exists method exec error. key: %s, error: %s", key, err)
		return false, snerror.New(common.StateClientErrCode, err.Error())
	}

	return result, nil
}

// DeleteStateMapping delete state mapping
func DeleteStateMapping(stateID string, tenantID string, proxyFunction string) snerror.SNError {
	log.GetLogger().Debugf("start to delete StateMapping, stateID: %s, tenantID: %s, proxyFunction: %s",
		stateID, tenantID, proxyFunction)
	var err snerror.SNError
	for i := 0; i < maxHTTPRetryAttempts; i++ {
		err = communication.DeleteStateByHTTP(stateID, tenantID, proxyFunction)
		if err == nil {
			break
		}
		time.Sleep(defaultRetryDelay * time.Second)
		log.GetLogger().Warnf("failed to delete state with stateID: %s, this is %v time to retry", stateID, i+1)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to delete state mapping, error (http response is error): %s,"+
			" stateID: %s", err.Error(), stateID)
		return err
	}
	log.GetLogger().Debugf("finished deleting StateMapping, stateID: %s", stateID)
	return nil
}

// HSetNX set the value of the hash table field when the field does not exist by state client
func HSetNX(key, filed, value string) (bool, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return false, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.HSetNX(key, filed, value)
	if err != nil {
		log.GetLogger().Errorf("state client HSetNX method exec error. key: %s, error: %s", key, err)
		return false, snerror.New(common.StateClientErrCode, err.Error())
	}
	return result, nil
}

// HDel remove fields from hash by state client
func HDel(key string, fields ...string) (int, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return 0, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.HDel(key, fields...)
	if err != nil {
		log.GetLogger().Errorf("state client HDel method exec error. key: %s, field %s, error: %s", key, fields, err)
		return 0, snerror.New(common.StateClientErrCode, err.Error())
	}
	return result, nil
}

// HGetAll get hash map of key by state client
func HGetAll(key string) (map[string]string, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return nil, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.HGetAll(key)
	if err != nil {
		log.GetLogger().Errorf("state client HGetAll method exec error. key: %s, error: %s", key, err)
		return nil, snerror.New(common.StateClientErrCode, err.Error())
	}
	return result, nil
}

// HExists if filed exists in hash return true
func HExists(key string, field string) (bool, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return false, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.HExists(key, field)
	if err != nil {
		log.GetLogger().Errorf("state client HExists method exec error. key: %s, field: %s, error: %s", key, field, err)
		return false, snerror.New(common.StateClientErrCode, err.Error())
	}
	return result, nil
}

// HLen get nums of key's fileds by state client
func HLen(key string) (int, snerror.SNError) {
	if stateClient == nil {
		log.GetLogger().Error("state client is nil")
		return 0, snerror.New(common.StateClientErrCode, common.StateClientErrMsg)
	}
	result, err := stateClient.HLen(key)
	if err != nil {
		log.GetLogger().Errorf("state client HLen method exec error. key: %s, error: %s", key, err)
		return 0, snerror.New(common.StateClientErrCode, err.Error())
	}
	return result, nil
}

// checkStateful check this request is stateful or not
func checkStateful(destFuncName string, stateID string, config MsgConfig) bool {
	var isStateful bool
	if destFuncName == taskFuncName || stateID != "" || FunctionCheck.IsStatefulFunction(
		config.DestFuncTenant, config.SNServiceID, destFuncName, config.FuncVersion) {
		isStateful = true
	}
	return isStateful
}
