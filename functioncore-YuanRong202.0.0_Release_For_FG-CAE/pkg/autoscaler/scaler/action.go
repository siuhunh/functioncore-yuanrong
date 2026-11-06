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

// Package scaler -
package scaler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/types/functiontypes"
)

// ActionType is type of action
type ActionType int

// enums of action type
const (
	ActionPull ActionType = iota
	ActionReduce

	defaultScaleTimeout = 15 * time.Minute
	// scaleDelay 33s(origin worker deploy) and 13s(vpc)
	scaleDelay             = 46
	workerManagerDeployURL = "/worker-manager/v1/microservices/sn:cn:yrk:%s:function:%s:%s/worker"
	workerManagerDeleteURL = "/worker-manager/v1/microservices/sn:cn:yrk:%s:function:%s:%s/worker"
	statusOKCode           = 150200
	scaleDownTimeout       = 30 * time.Second
)

var (
	lock = &sync.RWMutex{}
	// LastScaleTime last time to do scale down
	LastScaleTime int64
	// scale up error of reach max instance
	errReachMaxInstance = errors.New("this cluster reach max instance")
	// scale up error of reach on demand instance
	errReachMaxOnDemandInstance = errors.New("this cluster reach on demand max instance")
)

// DeployParam Deploy function param
type DeployParam struct {
	FuncName    string // deployed function name
	Applier     string // applier instance id
	DeployNode  string // deploy node id
	Business    string // business
	TenantID    string // tenant id
	Version     string // version
	OwnerIP     string // owner IP
	TraceID     string // trace id
	TriggerFlag string // This is a trigger request flag.
	StateID     string
	CPU         int
	Memory      int
	ExpectedNum int `json:"expected_num"`
}

// DeleteParam Delete function param
type DeleteParam struct {
	InstanceID         string `json:"instance_id"`
	FuncName           string `json:"function_name"`
	FuncVersion        string `json:"function_version"`
	BusinessID         string `json:"business_id"`
	TenantID           string `json:"tenant_id"`
	Applier            string `json:"applier"`
	IsBrokenConnection bool   `json:"broken_connection_status"`
	ExpectedNum        int    `json:"expected_num"`
}

// DeleteWorkerResponse is response to delete worker
type DeleteWorkerResponse struct {
	Code     int    `json:"code"`
	Reserved bool   `json:"reserved"`
	Message  string `json:"message"`
}

// Action describes action of pulling up reversed instance or reducing some instances
type Action struct {
	Type ActionType
	// reversed instance or pull
	NeedPull      int64
	NeedReduce    int64
	IdleMode      bool
	TotalExpected int64
}

func setLastScaleTime() {
	lock.Lock()
	LastScaleTime = time.Now().Unix()
	lock.Unlock()
}

func checkLastScaleTime() bool {
	now := time.Now().Unix()
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config file for downscaleStabilization")
		return false
	}
	if LastScaleTime != 0 && (now-LastScaleTime <= cfg.DownscaleStabilization) {
		return false
	}
	return true
}

// DoActions -
func DoActions(actions map[string]Action) {
	for functionSvcID, action := range actions {
		if action.Type == ActionPull {
			scaleUpCluster := clustermgr.GetClusterMgr().GetAvailableClusterForScaleUp()
			if scaleUpCluster.ClusterID == "" {
				log.GetLogger().Errorf("failed to get available cluster to scale up for %s", functionSvcID)
				return
			}
			expectedNumByCluster := getExpectedNumByCluster(functionSvcID, scaleUpCluster, action)
			go ScaleUp(functionSvcID, action.NeedPull, expectedNumByCluster, scaleUpCluster.ActiveNodes.IP)
		} else if action.Type == ActionReduce {
			if !checkLastScaleTime() {
				log.GetLogger().Infof("skip scale down, waiting cooling")
				continue
			}

			scaleDownCluster := clustermgr.GetClusterMgr().
				GetAvailableClusterForScaleDown(cache.GetInstance().GetFuncInstanceCurrentCountEveryCluster(functionSvcID))
			if scaleDownCluster.ClusterID == "" {
				log.GetLogger().Errorf("failed to get available cluster to scale down for %s", functionSvcID)
				return
			}
			expectedNumByCluster := getExpectedNumByCluster(functionSvcID, scaleDownCluster, action)
			// invoke except num lower than zero
			// only exist in this cluster has not enough instances to reduce
			// it will be retried in next round to handle not enough instance to reduce
			needReduceNum := action.NeedReduce
			if expectedNumByCluster < 0 {
				log.GetLogger().Warnf("not enough instance can reduce in this cluster, reduce: %d,"+
					" totalWantReduce: %d, funcSvcID: %s, cluster: %s", needReduceNum, action.NeedReduce,
					functionSvcID, scaleDownCluster.ClusterID)

				needReduceNum += expectedNumByCluster
				expectedNumByCluster = 0
			}
			go ScaleDown(functionSvcID, action.NeedReduce, expectedNumByCluster, scaleDownCluster.ActiveNodes.IP)
			setLastScaleTime()
		} else {
			continue
		}
	}
}

func getExpectedNumByCluster(functionSvcID string, usedCluster clustermgr.Cluster, action Action) int64 {
	var expectedNumByCluster int64
	curInsByCluster := cache.GetInstance().GetFuncInstanceCurrentCountInCluster(functionSvcID,
		usedCluster.ClusterID)
	if action.Type == ActionPull {
		expectedNumByCluster = curInsByCluster + action.NeedPull
	} else if action.Type == ActionReduce {
		expectedNumByCluster = curInsByCluster - action.NeedReduce
	}
	return expectedNumByCluster
}

// ScaleDown -
func ScaleDown(functionSvcID string, reduceWorkerNum int64, expectedNumByCluster int64, workerManagerIP string) (int,
	int) {
	var wg sync.WaitGroup
	var failedNums int32
	var succeedNum int32
	startTime := time.Now()
	reduceFunc := func() {
		defer wg.Done()

		isReversed, err := ScaleDownInstance(functionSvcID, expectedNumByCluster, workerManagerIP)
		if err != nil {
			log.GetLogger().Errorf("failed to scale down function %s, err: %s", functionSvcID, err.Error())
			atomic.AddInt32(&failedNums, 1)
			return
		}

		// if is reversed, need try to other cluster
		if isReversed {
			success := handleScaleDownRetry(workerManagerIP, functionSvcID, expectedNumByCluster)
			if !success {
				log.GetLogger().Errorf("failed to scale down retry function %s", functionSvcID)
				atomic.AddInt32(&failedNums, 1)
				return
			}
		}

		err = cache.GetInstance().DecFuncInstanceCurrentCount(functionSvcID)
		if err != nil {
			log.GetLogger().Errorf("failed to dec func instance, err: %s", err.Error())
			atomic.AddInt32(&failedNums, 1)
			return
		}

		atomic.AddInt32(&succeedNum, 1)
	}

	for i := 0; i < int(reduceWorkerNum); i++ {
		wg.Add(1)
		go reduceFunc()
	}
	wg.Wait()

	log.GetLogger().Infof("finished to scale down microservice instances with failed nums: %d succeed nums: %d "+
		"reduceWorkerNum is %d , usedCluster: %s, expectedNumByCluster is %d for func:  %s cost time: %s",
		atomic.LoadInt32(&failedNums), atomic.LoadInt32(&succeedNum),
		reduceWorkerNum, workerManagerIP, expectedNumByCluster, functionSvcID, time.Since(startTime).String())
	return int(atomic.LoadInt32(&failedNums)), int(atomic.LoadInt32(&succeedNum))
}

func handleScaleDownRetry(workerManagerIP string, functionSvcID string, expectedNumByCluster int64) bool {
	log.GetLogger().Warnf("function %s need to retry scale down because only left reversed instance! IP: %s,"+
		"expectedNumByCluster: %d", functionSvcID, workerManagerIP, expectedNumByCluster)

	clusterMap := clustermgr.GetClusterMgr().GetAllAvailableCluster()
	clusterInstanceCount := cache.GetInstance().GetFuncInstanceCurrentCountEveryCluster(functionSvcID)

	usedIP := map[string]bool{}
	usedIP[workerManagerIP] = true

	// use another cluster to scale up
	for _, cluster := range clusterMap {
		used, ok := usedIP[cluster.ActiveNodes.IP]
		if ok && used == true {
			continue
		}

		if clusterInstanceCount[cluster.ClusterID] <= 0 {
			continue
		}

		isReversed, err := ScaleDownInstance(functionSvcID, expectedNumByCluster, cluster.ActiveNodes.IP)
		usedIP[cluster.ActiveNodes.IP] = true
		if err == nil && !isReversed {
			log.GetLogger().Infof("function %s retry scale down end! Last worker manager scale up IP: %s",
				functionSvcID, cluster.ActiveNodes.IP)
			return true
		}
		log.GetLogger().Errorf("function %s need to retry scale down failed! IP: %s, err: %+v, isReversed: %t",
			functionSvcID, cluster.ActiveNodes.IP, err, isReversed)
	}
	return false
}

// ScaleDownInstance -
func ScaleDownInstance(functionSvcID string, expected int64, workerManagerIP string) (bool, error) {
	traceID := uuid.New().String()
	log.GetLogger().Infof("start to scale down by functionSvcID: %s, traceID: %s", functionSvcID, traceID)
	ctx, cancel := context.WithTimeout(utils.GetContext(), scaleDownTimeout)
	defer cancel()
	request, err := makeScaleDownReq(ctx, functionSvcID, expected, workerManagerIP)
	if err != nil {
		log.GetLogger().Errorf("failed to make scaleDown request to worker manager error: %s, functionSvcID: %s",
			err.Error(), functionSvcID)
		return false, err
	}
	utils.FillInWorkerManagerRequestHeaders(request)
	resp, err := utils.GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Errorf("failed to send scaleDown request to worker manager error: %s, functionSvcID: %s",
			err.Error(), functionSvcID)
		return false, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Warnf("failed to unmarshal scaleDowm response: %s, functionSvcID: %s", err,
				functionSvcID)
			return false, err
		}
		return false, fmt.Errorf(snErr.Message)
	}

	scaleDownResp := DeleteWorkerResponse{}
	if err = json.Unmarshal(respBody, &scaleDownResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal scaleDown response: %s, functionSvcID: %s", err,
			functionSvcID)
		return false, err
	}

	if scaleDownResp.Code != http.StatusOK {
		log.GetLogger().Warnf("scale down functionSvcID %s response code %d, response message %s",
			functionSvcID, scaleDownResp.Code, scaleDownResp.Message)
		if scaleDownResp.Code == 0 {
			return false, err
		}
		return false, fmt.Errorf(scaleDownResp.Message)
	}
	log.GetLogger().Infof("scale down functionSvcID %s instanceID %s and reserved result is %t",
		functionSvcID, scaleDownResp.Reserved)
	return scaleDownResp.Reserved, nil
}

func makeScaleDownReq(ctx context.Context, functionSvcID string, expected int64, workerManagerIP string) (*http.Request,
	error) {
	functionEtcdInfo, ok := cache.GetInstance().GetFunctionMeta(functionSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get functionEtcdInfo by functionSvcID: %s", functionSvcID)
		return nil, errors.New("failed to get functionEtcdInfo by functionSvcID: %s" + functionSvcID)
	}
	functionInfo := utils.GetFunctionInfoByURN(functionEtcdInfo.FuncMetaData.VersionURN)
	requestURI := utils.GetWorkerManagerBaseURLByIP(workerManagerIP) +
		fmt.Sprintf(workerManagerDeployURL, functionInfo.Tenant, functionInfo.FuncName, functionInfo.Version)
	param := DeleteParam{
		FuncName:    functionInfo.FuncName,
		FuncVersion: functionInfo.Version,
		BusinessID:  utils.InternalScaleBusinessID,
		TenantID:    functionInfo.Tenant,
		Applier:     "ASBRes",
		ExpectedNum: int(expected),
	}
	data, err := json.Marshal(param)
	if err != nil {
		log.GetLogger().Errorf("marshal error when make scaleUp request")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("error when make scaleUp request: %s", err.Error())
		return nil, err
	}
	return request, nil
}

// ScaleUp -
func ScaleUp(functionSvcID string, scalingWorkersNum int64, expectedNumByCluster int64, workerManagerIP string) (int,
	int) {
	var wg sync.WaitGroup
	var failedNums int32
	var succeedNum int32
	startTime := time.Now()
	log.GetLogger().Infof("begin to pull up microservice instances with scalingWorkersNum %d for func: %s",
		scalingWorkersNum, functionSvcID)
	pullUpFunc := func() {
		_, err := ScaleUpInstance(functionSvcID, expectedNumByCluster, workerManagerIP)
		if needRetryToOtherCluter(err) {
			err = handleScaleUpRetry(workerManagerIP, err, functionSvcID, expectedNumByCluster)
		}
		if err != nil {
			atomic.AddInt32(&failedNums, 1)
		} else {
			err := cache.GetInstance().IncFuncInstanceCurrentCount(functionSvcID)
			if err != nil {
				atomic.AddInt32(&failedNums, 1)
			} else {
				atomic.AddInt32(&succeedNum, 1)
			}
		}
		wg.Done()
	}

	for i := 0; i < int(scalingWorkersNum); i++ {
		wg.Add(1)
		go pullUpFunc()
	}
	wg.Wait()

	log.GetLogger().Infof("finished to scale up microservice instances with failed nums: %d succeed nums: %d "+
		"scalingWorkersNum is %d , usedCluster: %s, expectedNumByCluster is %d for func:  %s cost time: %s",
		atomic.LoadInt32(&failedNums), atomic.LoadInt32(&succeedNum),
		scalingWorkersNum, workerManagerIP, expectedNumByCluster, functionSvcID, time.Since(startTime).String())
	return int(atomic.LoadInt32(&failedNums)), int(atomic.LoadInt32(&succeedNum))
}

func handleScaleUpRetry(workerManagerIP string, err error,
	functionSvcID string, expectedNumByCluster int64) error {
	log.GetLogger().Warnf("Function %s need to retry scale up.Worker manager scale up failed! IP: %s",
		functionSvcID, workerManagerIP)
	clusterMap := clustermgr.GetClusterMgr().GetAllAvailableCluster()
	usedIP := map[string]bool{}
	usedIP[workerManagerIP] = true
	// use another cluster to scale up
	for _, cluster := range clusterMap {
		used, ok := usedIP[cluster.ActiveNodes.IP]
		if ok && used == true {
			continue
		}
		_, err = ScaleUpInstance(functionSvcID, expectedNumByCluster, cluster.ActiveNodes.IP)
		usedIP[cluster.ActiveNodes.IP] = true
		if !needRetryToOtherCluter(err) {
			log.GetLogger().Infof("Function %s retry scale up end! Last worker manager scale up IP: %s",
				functionSvcID, cluster.ActiveNodes.IP)
			break
		}
		log.GetLogger().Errorf("Function %s need to retry scale up.Worker manager scale up failed! IP: %s",
			functionSvcID,
			cluster.ActiveNodes.IP)
	}
	return err
}

func needRetryToOtherCluter(err error) bool {
	if err == nil {
		return false
	}

	// when autoScaler scale up request timeout or reach max instance
	// need retry to other cluster
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errReachMaxInstance) ||
		errors.Is(err, errReachMaxOnDemandInstance) {
		return true
	}

	return false
}

// ScaleUpInstance -
func ScaleUpInstance(functionSvcID string, expectedNum int64, workerManagerIP string) (*types.Instance, error) {
	currentNum := cache.GetInstance().GetFuncInstanceCurrentCount(functionSvcID)
	traceID := uuid.New().String()
	log.GetLogger().Infof("start to scale up by functionSvcID: %s, current number: %d, traceID: %s",
		functionSvcID, currentNum, traceID)
	functionEtcdInfo, ok := cache.GetInstance().GetFunctionMeta(functionSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get functionEtcdInfo by functionSvcID: %s", functionSvcID)
		return nil, errors.New("failed to get functionEtcdInfo by functionSvcID: %s" + functionSvcID)
	}
	timeOut := getScaleTimeOut(functionEtcdInfo)
	ctx, cancel := context.WithTimeout(utils.GetContext(), timeOut)
	defer cancel()
	request, err := makeScaleUpReq(ctx, functionSvcID, traceID, expectedNum, workerManagerIP)
	if err != nil {
		log.GetLogger().Errorf("failed to get functionEtcdInfo by functionSvcID: %s", functionSvcID)
		return nil, err
	}

	log.GetLogger().Debugf("succeeded to sign the authorization of functionSvcID: %s, traceID: %s", functionSvcID, traceID)
	resp, err := utils.GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Errorf("send request to worker manager error: %s, functionSvcID: %s, traceID: %s",
			err.Error(), functionSvcID, traceID)
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("failed to get response body: %s, functionSvcID: %s, traceID: %s", err.Error(),
			functionSvcID, traceID)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal scaleUp error response: %s, functionSvcID: %s, traceID: %s",
				err, functionSvcID, traceID)
			return nil, err
		}
		return nil, fmt.Errorf(snErr.Message)
	}

	scaleUpResp, instance, err := handleResp(functionSvcID, respBody, traceID)
	if err != nil {
		return instance, err
	}

	log.GetLogger().Infof("deploy functionSvcID %s instance: %v traceID: %s successfully",
		functionSvcID, scaleUpResp.Instance, traceID)
	return scaleUpResp.Instance, nil
}

func handleResp(functionSvcID string, respBody []byte, traceID string) (config.GetWorkerSuccessResponse,
	*types.Instance, error) {
	scaleUpResp := config.GetWorkerSuccessResponse{}
	if err := json.Unmarshal(respBody, &scaleUpResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal scaleUp response: %s, functionSvcID: %s, traceID: %s",
			err, functionSvcID, traceID)
		return config.GetWorkerSuccessResponse{}, nil, err
	}

	if scaleUpResp.Code == snerror.ReachMaxInstancesCode {
		log.GetLogger().Warnf("deploy response this cluster reach max instance, functionSvcID: %s, traceID: %s",
			functionSvcID, traceID)
		return config.GetWorkerSuccessResponse{}, nil, errReachMaxInstance
	}

	if scaleUpResp.Code == snerror.ReachMaxOnDemandInstancesPerTenant {
		log.GetLogger().Warnf("deploy response this cluster reach max on demand instance, functionSvcID: %s, traceID: %s",
			functionSvcID, traceID)
		return config.GetWorkerSuccessResponse{}, nil, errReachMaxOnDemandInstance
	}

	if scaleUpResp.Code != statusOKCode || scaleUpResp.Worker == nil {
		log.GetLogger().Warnf("deploy response code %d, functionSvcID: %s, traceID: %s", scaleUpResp.Code,
			functionSvcID, traceID)
		return config.GetWorkerSuccessResponse{}, nil, fmt.Errorf(scaleUpResp.Message)
	}
	return scaleUpResp, nil, nil
}

func makeScaleUpReq(ctx context.Context, functionSvcID string, traceID string, expectedNum int64,
	workerManagerIP string) (*http.Request, error) {
	nodeID := os.Getenv("NODE_ID")
	functionEtcdInfo, ok := cache.GetInstance().GetFunctionMeta(functionSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get functionEtcdInfo by functionSvcID: %s", functionSvcID)
		return nil, errors.New("failed to get functionEtcdInfo by functionSvcID: %s" + functionSvcID)
	}
	scaleUpParam := DeployParam{
		Applier:     constants.ASBResApplier,
		DeployNode:  nodeID,
		Business:    utils.InternalScaleBusinessID,
		TraceID:     traceID,
		ExpectedNum: int(expectedNum),
	}

	data, err := json.Marshal(scaleUpParam)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal scaleUp request: %s", err.Error())
		return nil, err
	}
	functionInfo := utils.GetFunctionInfoByURN(functionEtcdInfo.FuncMetaData.VersionURN)
	requestURI := utils.GetWorkerManagerBaseURLByIP(workerManagerIP) +
		fmt.Sprintf(workerManagerDeployURL, functionInfo.Tenant, functionInfo.FuncName, functionInfo.Version)
	request, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("action failed when make scaleUp request, traceID: %s, err %s",
			scaleUpParam.TraceID, err.Error())
		return nil, err
	}
	utils.FillInWorkerManagerRequestHeaders(request)
	request.Header.Set(constants.HeaderTraceID, scaleUpParam.TraceID)
	request.Header.Set(constants.HeaderForceDeploy, "true")
	return request, nil
}

func getScaleTimeOut(functionEtcdInfo *functiontypes.FunctionEtcdInfo) time.Duration {
	var timeOut = defaultScaleTimeout
	if functionEtcdInfo.FuncMetaData.Timeout == 0 {
		timeOut = defaultScaleTimeout
	} else {
		timeOut = time.Duration(scaleDelay+functionEtcdInfo.FuncMetaData.Timeout+
			functionEtcdInfo.ExtendedMetaData.Initializer.InitializerTimeout) * time.Second
	}
	return timeOut
}
