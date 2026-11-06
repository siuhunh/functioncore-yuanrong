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

// Package faasfunction receive from bus proxy and send to worker
package faasfunction

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"

	apiServerOptions "functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	logconfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tracer"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/cache/willdeleteworker"
	"functioncore/pkg/worker-manager/checkpoint"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource/etcdevents"
	"functioncore/pkg/worker-manager/iamwatcher"
	interfacelog "functioncore/pkg/worker-manager/log"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/storage/meta"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
	"functioncore/pkg/worker-manager/vpcmanager"
)

// FunctionAPI define faaSFunction api
type FunctionAPI struct {
	Options         *apiServerOptions.APIServerOptions
	podFetcher      *podfetcher.PodFetcher
	interfaceLog    *interfacelog.InterfaceLog
	VpcHandler      *vpcmanager.VpcHandler
	kubeClient      kubernetes.Interface
	Name            string
	Path            string
	delInsPendingCh map[string]chan struct{}
}

// GetWorkerSuccessResponse define
type GetWorkerSuccessResponse struct {
	Worker   *Worker               `json:"worker"`
	Instance *commontypes.Instance `json:"instance"`
	Code     int                   `json:"code"`
	Message  string                `json:"message"`
}

// Worker define a worker
type Worker struct {
	Instances       []*commontypes.Instance `json:"instances"`
	FunctionName    string                  `json:"functionname"`
	FunctionVersion string                  `json:"functionversion"`
	Tenant          string                  `json:"tenant"`
	Business        string                  `json:"business"`
}

// GetWorkerErrorResponse define a response
type GetWorkerErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// DeleteWorkerRequest is request to delete worker
type DeleteWorkerRequest struct {
	InstanceID         string `json:"instance_id"`
	FuncName           string `json:"function_name"`
	FuncVersion        string `json:"function_version"`
	TenantID           string `json:"tenant_id"`
	BusinessID         string `json:"business_id"`
	Applier            string `json:"applier"`
	IsBrokenConnection bool   `json:"broken_connection_status"`
	ExpectedNum        int    `json:"expected_num"`
	IgnoreApplier      bool   `json:"ignoreApplier"`
}

// DeleteWorkerResponse is response to delete worker
type DeleteWorkerResponse struct {
	Code     int    `json:"code"`
	Reserved bool   `json:"reserved"`
	Message  string `json:"message"`
}

// VPAResponse is response to vpa pause pod
type VPAResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ReportMetricsResponse is response to report metrics
type ReportMetricsResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// SuccessResponseInfo define the Response success info
type SuccessResponseInfo struct {
	rspWriter    http.ResponseWriter
	functionMeta meta.Storage
	functionName string
	header       string
	traceID      string
	stateID      string
	workerID     string
	isAlias      bool
	// for OnDemand instance
	deployedNode string
	instanceID   string
	host         string
	port         string
}

// CREvent cr event in queue
type CREvent struct {
	pod             *corev1.Pod
	requestInfo     *config.RequestInfo
	functionService *faasfunction.FunctionService
}

type willDeleteCheckTask struct {
	funcName       string
	funcSvcID      string
	timeout        int
	workerInstance v1alpha1.WorkerInstance
	closeCh        chan interface{}
	funcAPI        *FunctionAPI
}

func (w *willDeleteCheckTask) run() {
	timer := time.NewTimer(time.Second * time.Duration(w.timeout))
	defer timer.Stop()
	defer willDeleteTasks.Delete(w.workerInstance.PodName)
	select {
	case <-w.closeCh:
		log.GetLogger().Infof("will delete check for %s has been canceled", w.workerInstance.PodName)
	case <-timer.C:
		traceID := uuid.New().String()
		log.GetLogger().Warnf("will delete check timeout for %s, clear instance and etcd, traceID:%s",
			w.workerInstance.PodName, traceID)
		cache.WorkerCache.RemoveStateIDFromFuncCache(w.funcSvcID, w.workerInstance.StateID)
		w.funcAPI.handleUpdateAgencyInfoToWorker(w.funcSvcID, w.workerInstance.PodName, w.workerInstance, traceID)
		w.funcAPI.podFetcher.DeletePodByWorkerInstance(w.funcSvcID, utils.FunctionNameSpace(), traceID, w.workerInstance)
		_ = w.funcAPI.VpcHandler.IdleDeletePatServiceList(w.workerInstance.PodName)
		if _, ok := config.InitFuncInstResource.Load(w.funcName); ok {
			config.InitFuncInstResource.Delete(w.funcName)
		}
		willdeleteworker.GetCache().DeleteWillDeleteWorkerInstance(w.funcSvcID, w.workerInstance.InstanceID)
	}
}

func (w *willDeleteCheckTask) cancel() {
	willDeleteTasks.Delete(w.workerInstance.PodName)
	close(w.closeCh)
}

var (
	clusterID           = os.Getenv("CLUSTER_ID")
	willDeleteTasks     = sync.Map{}
	willDeleteTasksLock = sync.Mutex{}
)

const (
	// initPendChSize is the init size of pending request channel
	initPendChSize = 1000
	// strConvMinDigitValue define strconv for digit values >= 10.
	strConvMinDigitValue = 10
	singleFlightKeyTTL   = 10 * time.Second
	// cae request timeout 900s, gap 20s
	willDeleteCheckTimeout = 920
)

// NewFaaSFunctionAPI new faas function api
func NewFaaSFunctionAPI(apiVersionPath string, options *apiServerOptions.APIServerOptions,
	stopCh <-chan struct{}) (*FunctionAPI, error) {
	podFetcher := podfetcher.GetPodFetcher()
	if podFetcher == nil {
		log.GetLogger().Errorf("failed to get podFetcher")
		return nil, errors.New("failed to get podFetcher")
	}

	interfaceLog := &interfacelog.InterfaceLog{}
	vpcController, err := vpcmanager.GetVPCController()
	if err != nil {
		return nil, err
	}
	utils.WatcherUserAgency(stopCh)
	if utils.IsIAMTokenFeatureEnable() {
		go iamwatcher.RunIAMTokenWatcher(stopCh)
	}
	clusterstatusutil.InitClusterStatusCache()
	mgt := FunctionAPI{
		Name:            "faas-function",
		Path:            apiVersionPath + "/functions",
		Options:         options,
		kubeClient:      podFetcher.KubeClient,
		podFetcher:      podFetcher,
		interfaceLog:    interfaceLog,
		VpcHandler:      vpcController,
		delInsPendingCh: make(map[string]chan struct{}, initPendChSize),
	}
	return &mgt, nil
}

func (m *FunctionAPI) describeFunctionInternal(w http.ResponseWriter, requestInfo *config.RequestInfo) {
	funcSvcID := utils.CrName(requestInfo.BusinessID, requestInfo.TenantID,
		requestInfo.FunctionName, requestInfo.FunctionVersion)
	// Trigger expansion and delete the workInstance with the willDelete label and delete function instance
	workerInstances := willdeleteworker.GetCache().GetWillDeleteWorkerInstancesAndDelete(funcSvcID)
	if len(workerInstances) != 0 {
		for _, instance := range workerInstances {
			instance.WillDelete = false
			etcdhandler.PutInstanceToETCD(context.Background(), instance)
			log.GetLogger().Infof("%s worker's willDelete tag is set to false, applier: %s, funcSvc: %s",
				instance.InstanceID, instance.Applier, funcSvcID)
		}
	}

	log.GetLogger().Infof("describeFunc | %s | %s | cpu %d | mem %d | ownedWorker %d | curWorker %d | "+
		"scalingWorker %d | %s | %s", urnutils.ExtractFuncName(requestInfo.FunctionName), requestInfo.TraceID,
		requestInfo.CPU, requestInfo.Memory, requestInfo.OwnedWorkerView.OwnedNum,
		requestInfo.OwnedWorkerView.CurrentWorkerNum, requestInfo.OwnedWorkerView.ScalingWorkersNum, funcSvcID,
		requestInfo.StateID)

	functionService := m.getFunctionService(funcSvcID, requestInfo)

	if functionService == nil {
		log.GetLogger().Errorf("failed to get function meta from cache for %s, traceID: %s",
			funcSvcID, requestInfo.TraceID)
		err := refreshSilentFunc(requestInfo, funcSvcID)
		m.errorResponse(requestInfo, w, err, "")
		return
	}
	runtimeType := commonUtils.GetRuntimeLanguage(functionService.Spec.Configuration.Runtime.Language)
	if !utils.IsRegisterRuntimeType(runtimeType) {
		log.GetLogger().Errorf("unsupported runtime type %s for %s, traceID: %s",
			runtimeType, funcSvcID, requestInfo.TraceID)
		m.errorResponse(requestInfo, w, snerror.New(snerror.UnsupportedRuntimeType,
			fmt.Sprintf(snerror.ErrText(snerror.UnsupportedRuntimeType), runtimeType)), "")
		return
	}
	// only effect in  1.update version and update snapshot 2.rollback
	multiSnapshotConfig, err := m.getMultiSnapshotConfig()
	if err == nil && multiSnapshotConfig.Enable && len(functionService.Spec.SnapshotDeployment.SnapshotVersions) != 0 {
		m.refreshFuncMetaData(requestInfo, functionService)
	}
	m.updateSnapshotCriuOpts(functionService)
	if (requestInfo.CPU == 0 || requestInfo.Memory == 0) && requestInfo.CreateOptions == nil {
		requestInfo.CPU = functionService.Spec.Configuration.Resource.CPU
		requestInfo.Memory = functionService.Spec.Configuration.Resource.Mem
	}
	log.GetLogger().Debugf("start to generalized pod, functionName: %s, traceID: %s",
		requestInfo.FunctionName, requestInfo.TraceID)
	requestInfo, instance, err := m.executePod(requestInfo, functionService, w)
	if err != nil {
		return
	}

	m.successResponse(w, requestInfo, functionService, instance)
	return
}

func (m *FunctionAPI) getFunctionService(funcSvcID string,
	requestInfo *config.RequestInfo) *faasfunction.FunctionService {
	functionService := cache.WorkerCache.GetFuncMeta(funcSvcID)

	// If the functionService hasn't been cached（maybe caused by network latency
	// or worker-manager high load）, do an active query
	if functionService == nil {
		log.GetLogger().Warnf("The funcSvcID:%s hasn't been cached, do an active query, traceId: %s",
			funcSvcID, requestInfo.TraceID)
		cache.UpdateFuncCache(requestInfo.FunctionName, requestInfo.BusinessID,
			requestInfo.TenantID, requestInfo.FunctionVersion)
		functionService = cache.WorkerCache.GetFuncMeta(funcSvcID)
		m.podFetcher.CreateNewContext(funcSvcID)
	}
	return functionService
}

func (m *FunctionAPI) refreshFuncMetaData(requestInfo *config.RequestInfo, service *faasfunction.FunctionService) {
	start := time.Now()
	log.GetLogger().Infof("begin to refreshFuncMetaData")
	runtimeManagerTag := os.Getenv("RUNTIME_MANAGER_TAG")
	for _, v := range service.Spec.SnapshotDeployment.SnapshotVersions {
		if !strings.Contains(runtimeManagerTag, v.SnapshotImageTag) {
			log.GetLogger().Infof("SnapshotImageTag is %s, runtimeManagerTag is %s",
				v.SnapshotImageTag, runtimeManagerTag)
			continue
		}
		// checkpoint fail
		if v.ErrCode == utils.StatusInternalServerError {
			continue
		}
		// set new snapshot info in ETCD and workerCache
		service.Spec.SnapshotDeployment.ObjectID = v.ObjectID
		service.Spec.SnapshotDeployment.SnapshotSha256 = v.SnapshotSha256
		service.Spec.SnapshotDeployment.SnapshotSize = v.SnapshotSize
		log.GetLogger().Infof("Set new snapshot info. Refresh function is %s, traceId is %s cost %s,  objectId is %s, "+
			"snapshotSha256 is %s, snapshotSize is %s.",
			requestInfo.FunctionName, requestInfo.TraceID, time.Since(start), v.ObjectID, v.SnapshotSha256, v.SnapshotSize)
	}

}

func (m *FunctionAPI) getMultiSnapshotConfig() (config.MultiSnapshotConfig, error) {
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return config.MultiSnapshotConfig{}, err
	}
	return conf.PoolMgrConfig.MultiSnapshotConfig, err
}

func refreshSilentFunc(requestInfo *config.RequestInfo, funcSvcID string) error {
	key := fmt.Sprintf(constants.SilentFuncKey, requestInfo.TenantID, requestInfo.FunctionName,
		requestInfo.FunctionVersion)
	_, err := cache.SingleFlight.Do(funcSvcID, func() (interface{}, error) {
		metaClient := etcd3.GetMetaEtcdClient()
		if metaClient.Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client")
			return nil, snerror.New(snerror.RefreshSilentFunc, snerror.ErrText(snerror.RefreshSilentFunc))
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		getResp, err := metaClient.GetResponse(defaultEtcdCtx, etcd3.HandleCellPrefix(key))
		if err != nil {
			time.AfterFunc(singleFlightKeyTTL, func() {
				cache.SingleFlight.Remove(key)
			})
			log.GetLogger().Errorf("failed to query from etcd through key %s, err: %s", key, err.Error())
			return nil, snerror.New(utils.StatusQueryETCDError, "failed to query from etcd")
		}
		if len(getResp.Kvs) == 0 {
			log.GetLogger().Errorf("failed to get function meta from etcd by key: %s", key)
			return nil, snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
		}
		if err := utils.CheckMetaInfoValid(getResp.Kvs[0].Value); err != nil {
			log.GetLogger().Warnf("silent func meta is invalid, key: %s, err: %s", key, err.Error())
			return nil, snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
		}
		etcdevents.AwaitReadyChanDone()
		for _, kv := range getResp.Kvs {
			etcdevents.ProcessSilentEvent(&etcd3.Event{
				Type:      etcd3.PUT,
				Key:       strings.TrimPrefix(key, constants.SilentFuncKeyPrefix),
				Value:     kv.Value,
				PrevValue: nil,
				Rev:       kv.ModRevision,
				Source:    etcd3.Meta,
			})
		}
		return nil, snerror.New(snerror.RefreshSilentFunc, snerror.ErrText(snerror.RefreshSilentFunc))
	})
	return err
}

func (m *FunctionAPI) dealErrorWithType(err error, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService, w http.ResponseWriter) error {
	if err == utils.ErrReachMaxOnDemandInstanceNumPerTenant {
		maxOnDemandInsNum, _ := cache.TenantCache.GetMaxInsNum(requestInfo.TenantID)
		log.GetLogger().Warnf("deal error with reach max on-demand instance num per tenant: %d with traceID: %s",
			maxOnDemandInsNum, requestInfo.TraceID)
		message := fmt.Sprintf("tenant %s reach max on-demand instance num %d",
			requestInfo.TenantID, maxOnDemandInsNum)
		m.scaleLimitResponse(requestInfo, w, functionService, message, utils.ReachMaxOnDemandInstancesPerTenant)
	} else if err == utils.ErrReachMaxInstanceNum {
		maxScale := cache.WorkerCache.GetMaxScale(functionService.ID)
		if maxScale == 0 {
			log.GetLogger().Warnf("deal error with function disabled, traceID: %s", requestInfo.TraceID)
			message := fmt.Sprintf("function %s is disabled", functionService.ID)
			m.scaleLimitResponse(requestInfo, w, functionService, message, utils.FunctionIsDisabled)
		} else {
			log.GetLogger().Warnf("deal error with reach max instance num: %d with traceID: %s",
				maxScale, requestInfo.TraceID)
			message := fmt.Sprintf("function %s reach max instance num %d", functionService.ID, maxScale)
			m.scaleLimitResponse(requestInfo, w, functionService, message, utils.StatusReachMaxInstances)
		}
	} else if err == utils.ErrCancelGeneralizePod {
		log.GetLogger().Warnf("cancel generalize pod while action is not finished with traceID: %s",
			requestInfo.TraceID)
		err = snerror.New(snerror.CancelGeneralizePod, utils.ErrCancelGeneralizePod.Error())
		m.errorResponse(requestInfo, w, err, fmt.Sprintf("cancel generalize pod while action is not finished, "+
			"traceID %s: %s", requestInfo.TraceID, err.Error()))
	} else if err == utils.ErrVIPOverload {
		monitoring.ReportFailureFee(requestInfo, err)
		err = snerror.New(utils.VIPClusterOverload, utils.ErrVIPOverload.Error())
		m.errorResponse(requestInfo, w, err, fmt.Sprintf("active worker %s error, traceID %s: %s",
			requestInfo.WorkerID, requestInfo.TraceID, err.Error()))
	} else if err == utils.ErrNoNodesCanScheduler {
		log.GetLogger().Debugf("err no nodes can scheduler, traceID: %s", requestInfo.TraceID)
		monitoring.ReportFailureFee(requestInfo, err)
		err = snerror.New(utils.StatusClusterOverload, utils.ErrNoNodesCanScheduler.Error())
		m.errorResponse(requestInfo, w, err, fmt.Sprintf("active worker %s error, traceID %s: %s",
			requestInfo.WorkerID, requestInfo.TraceID, err.Error()))
	} else if err == utils.ErrInsufficientLicense {
		log.GetLogger().Warnf("exceeds the CPU license threshold, please expand the hcs license specifications.")
		err = snerror.New(utils.StatusInsufficientLicense, utils.ErrInsufficientLicense.Error())
		m.errorResponse(requestInfo, w, err, "exceeds the CPU license threshold")
	} else {
		monitoring.ReportFailureFee(requestInfo, err)
		// if error is not found, errCode==tools.StatusNotFound otherwise errCode==tools.StatusInternalServerError
		m.errorResponse(requestInfo, w, err, fmt.Sprintf("active worker %s error, traceID %s: %s",
			requestInfo.WorkerID, requestInfo.TraceID, err.Error()))
	}
	return err
}

func (m *FunctionAPI) executePod(requestInfo *config.RequestInfo, functionService *faasfunction.FunctionService,
	w http.ResponseWriter) (*config.RequestInfo, *commontypes.Instance, error) {
	// cpu memory is default and proxy owned worker num is 0 and function is stateless
	// use an existing instance or wait a reserved instance to deal with request
	if isDefaultResource(requestInfo, functionService) && requestInfo.StateID == "" &&
		!commonUtils.IsWebSocketFunc(functionService.Spec.Configuration.BusinessType, false,
			functionService.Spec.Configuration.EnableWebsocket) {
		ctx, ok := m.podFetcher.GetContext(functionService.ID)
		if !ok {
			err := snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
			return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
		}
		workerIns, err := cache.WorkerCache.GetOrWaitReservedIns(ctx, functionService.ID, requestInfo)
		if err != nil {
			log.GetLogger().Errorf("failed to get or wait reserved instance: %s, traceID: %s",
				err.Error(), requestInfo.TraceID)
			return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
		}
		if workerIns != nil {
			return requestInfo, utils.BuildInstanceByWorkerIns(workerIns), nil
		}
	}
	var (
		pod *corev1.Pod
		err error
	)
	for i := 0; i < 3; i++ {
		// scale up 1 instance to deal with request
		pod, err = m.podFetcher.WithContext(m.podFetcher.GeneralizePod, requestInfo, functionService)
		if utils.IsNetErrorAndNotTimeout(err) {
			log.GetLogger().Warnf("retry generalize pod when got net err %s traceID %s tryTime %d",
				err.Error(), requestInfo.TraceID, i)
			continue
		}
		break
	}
	if pod == nil && err == nil {
		err = fmt.Errorf("pod is nil with function name %s", requestInfo.FunctionName)
	}
	if err != nil {
		if !isNoSpaceErr(err, functionService) {
			return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
		}
		return m.handlerNoSpaceOfCustomContainers(err, requestInfo, functionService, w)
	}
	log.GetLogger().Infof("succeed to scale up 1 instance to deal with request for funcName: %s, traceID: %s, stateID: %s",
		requestInfo.FunctionName, requestInfo.TraceID, requestInfo.StateID)

	if cache.GetCustomRetryAlarmHelper().TryClearAlarmIfExist(functionService.ID) {
		alarm.GetClient().ClearAlarm(alarm.CustomImageSchedulerFailed)
	}

	return requestInfo,
		utils.BuildInstanceByPod(pod, requestInfo, functionService.Spec.Configuration.BusinessType), nil
}

// if no space error, need retry to pull up instance
func (m *FunctionAPI) handlerNoSpaceOfCustomContainers(err error, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService, w http.ResponseWriter) (
	*config.RequestInfo, *commontypes.Instance, error) {
	config, configErr := config.GetConfig()
	if configErr != nil {
		log.GetLogger().Errorf("get config error: %s", err.Error())
		return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
	}
	if config == nil {
		log.GetLogger().Errorf("config is nil")
		return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
	}

	var pod *corev1.Pod

	retryTimes := int64(0)

	if config.RetryTimeWhenNoSpace <= 0 {
		return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
	}

	for retryTimes < config.RetryTimeWhenNoSpace {
		retryTimes++
		pod, err = m.podFetcher.WithContext(m.podFetcher.GeneralizePod, requestInfo, functionService)
		if pod == nil && err == nil {
			err = fmt.Errorf("pod is nil with function name %s", requestInfo.FunctionName)
		}

		if err != nil {
			if isNoSpaceErr(err, functionService) {
				continue
			}

			return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
		}

		// means succeed to get pod
		if cache.GetCustomRetryAlarmHelper().TryClearAlarmIfExist(functionService.ID) {
			alarm.GetClient().ClearAlarm(alarm.CustomImageSchedulerFailed)
		}
		return requestInfo,
			utils.BuildInstanceByPod(pod, requestInfo, functionService.Spec.Configuration.BusinessType), nil
	}

	log.GetLogger().Debugf(fmt.Sprintf("retry over %d times, "+
		"custom image can't get node scheduler, funcSvcID is %s", config.RetryTimeWhenNoSpace, functionService.ID))

	alarm.GetClient().ReportAlarm(alarm.CustomImageSchedulerFailed, fmt.Sprintf("retry over %d times, "+
		"custom image can't get node scheduler, funcSvcID is %s", config.RetryTimeWhenNoSpace, functionService.ID))
	cache.GetCustomRetryAlarmHelper().AddAlarm(functionService.ID)

	return nil, nil, m.dealErrorWithType(err, requestInfo, functionService, w)
}

func isNoSpaceErr(err error, functionService *faasfunction.FunctionService) bool {
	if !commonUtils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) &&
		!utils.IsCustomContainerSpec(&functionService.Spec) {
		return false
	}

	if snErr, ok := err.(snerror.SNError); ok {
		errorCode := snErr.Code()
		errorMessage := snErr.Error()
		if errorCode == snerror.ClusterOverloadCode && strings.Contains(errorMessage, common.FailedToPullUpImageOfNoSpace) {
			return true
		}
	}
	return false
}

func isDefaultResource(requestInfo *config.RequestInfo, functionService *faasfunction.FunctionService) bool {
	return requestInfo.CPU == functionService.Spec.Configuration.Resource.CPU &&
		requestInfo.Memory == functionService.Spec.Configuration.Resource.Mem &&
		requestInfo.Applier != constants.ASBResApplier
}

func (m *FunctionAPI) parseFuncMetaBaseInfo(c *gin.Context) (*workermanager.FuncMetaBaseInfo, error) {
	baseInfoReq := &workermanager.FuncMetaBaseInfo{}
	return baseInfoReq, c.ShouldBindJSON(baseInfoReq)
}

// parse http request to internal RequestInfo
func (m *FunctionAPI) parseHTTPRequest(c *gin.Context) *config.RequestInfo {
	traceID := utils.GetTraceID(c.Request)
	// start to active
	urn, err := utils.GetMetaName(c)
	if err != nil {
		describe := fmt.Sprintf("the query does not have function urn with error %s, traceID %s",
			err.Error(), traceID)
		m.WriteLog(nil, utils.StatusBadRequest, utils.ErrTrans[utils.StatusBadRequest], describe)
		log.GetLogger().Errorf(describe)
		m.errorResponse(nil, c.Writer, err, describe)
		return nil
	}
	filter := utils.NewFilter()

	filter.Filters[meta.FilterKeyNamespace] = utils.Tenant2Namespace(c)
	functionInfo := utils.GetFunctionInfoByURN(urn)
	reqConf := config.RequestInfo{}
	res, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		msg := "failed to read request body"
		log.GetLogger().Errorf("%s", msg)
		m.errorResponse(nil, c.Writer, err, msg)
		return nil
	}
	err = json.Unmarshal(res, &reqConf)
	if err != nil {
		msg := "failed to unmarshal request info"
		log.GetLogger().Errorf("%s", msg)
		m.errorResponse(nil, c.Writer, err, msg)
		return nil
	}
	reqConf.URN = urn
	reqConf.FunctionName = functionInfo.FuncName
	reqConf.BusinessID = functionInfo.Business
	reqConf.TenantID = functionInfo.Tenant
	reqConf.FunctionVersion = functionInfo.Version
	reqConf.WorkerID = utils.CrNameByURN(urn)
	reqConf.CallType = utils.GetType(c.Request)
	reqConf.TraceID = traceID

	reqConf.Namespace = utils.Tenant2Namespace(c)
	reqConf.StartTime = time.Now()
	reqConf.TriggerFlag = utils.GetTriggerFlag(c.Request)
	reqConf.ForceDeploy = utils.GetForceDeploy(c.Request)
	reqConf.NodeLabel = utils.GetNodeLabel(c.Request)
	reqConf.TraceParent = utils.GetTraceParent(c.Request)

	return &reqConf
}

func (m *FunctionAPI) scaleDownFunction(c *gin.Context) {
	m.deleteFunctionInstance(c.Writer, c.Request)
}

// scaleUpFunction scale up function
func (m *FunctionAPI) scaleUpFunction(c *gin.Context) {
	requestInfo := m.parseHTTPRequest(c)
	if requestInfo == nil {
		log.GetLogger().Errorf("failed to parse describe function request, return directly")
		return
	}
	log.GetLogger().Debugf("scaled up function, parse deploy request success, functionName: %s,"+
		" deployedNode: %s, nodeLabel: %s, applierIP: %s, forceDeploy: %s, workerID: %s, traceID: %s, expectedNum: %d"+
		" Applier: %s\",", requestInfo.FunctionName, requestInfo.DeployNode, requestInfo.NodeLabel, requestInfo.OwnerIP,
		requestInfo.ForceDeploy, requestInfo.WorkerID, requestInfo.TraceID, requestInfo.ExpectedNum,
		requestInfo.Applier)

	// If callType isn't "active", refuse request
	if requestInfo.CallType != utils.RequestCallType {
		describe := fmt.Sprintf("callType is not active, traceID %s", requestInfo.TraceID)
		m.errorResponse(requestInfo, c.Writer, utils.ErrCallType, describe)
		log.GetLogger().Errorf("failed to describe function %s with error callType is not active, traceID %s",
			requestInfo.FunctionName, requestInfo.TraceID)
		return
	}
	cache.WorkerCache.SetFuncExpectedNum(utils.CrNameByURN(requestInfo.URN), requestInfo.ExpectedNum)

	m.describeFunctionInternal(c.Writer, requestInfo)
	return
}

// describeFunction describe function
func (m *FunctionAPI) describeFunction(c *gin.Context) {
	requestInfo := m.parseHTTPRequest(c)
	if requestInfo == nil {
		log.GetLogger().Errorf("failed to parse describe function request, return directly")
		return
	}

	// If callType isn't "active", refuse request
	if requestInfo.CallType != utils.RequestCallType {
		describe := fmt.Sprintf("callType is not active, traceID %s", requestInfo.TraceID)
		m.errorResponse(requestInfo, c.Writer, utils.ErrCallType, describe)
		log.GetLogger().Errorf("failed to describe function %s with error callType is not active, traceID %s",
			requestInfo.FunctionName, requestInfo.TraceID)
		return
	}

	m.describeFunctionInternal(c.Writer, requestInfo)
	return
}

// getFuncMetaInfo -
func (m *FunctionAPI) getFuncMetaInfo(c *gin.Context) {
	funcBaseInfoResp := &workermanager.FuncMetaBaseInfoResp{}
	baseInfoReq, err := m.parseFuncMetaBaseInfo(c)
	if err != nil {
		log.GetLogger().Warnf("parseFuncMetaBaseInfo err:%s", err.Error())
		funcBaseInfoResp.ErrMsg = fmt.Sprintf("getFuncMetaInfo err:%s", err.Error())
		m.writeResp(funcBaseInfoResp, c)
		return
	}
	funcMetaInfo, err := cache.GetFuncMetaInfo(*baseInfoReq)
	if err != nil {
		log.GetLogger().Warnf("GetFuncMetaInfo of %#v err:%s", baseInfoReq, err.Error())
		funcBaseInfoResp.ErrMsg = fmt.Sprintf("getFuncMetaInfo err:%s", err.Error())
	} else {
		funcBaseInfoResp.FuncMetaInfo = *funcMetaInfo
	}
	m.writeResp(funcBaseInfoResp, c)
	return
}

func (m *FunctionAPI) writeResp(baseInfoResp *workermanager.FuncMetaBaseInfoResp, c *gin.Context) {
	c.JSON(http.StatusOK, baseInfoResp)
}

func (m *FunctionAPI) parseDeleteRequest(w http.ResponseWriter, r *http.Request) (*DeleteWorkerRequest, error) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	request := &DeleteWorkerRequest{}
	err = json.Unmarshal(data, request)
	if err != nil {
		return nil, err
	}

	return request, nil
}

func (m *FunctionAPI) markWorkerInstanceWillDelete(w http.ResponseWriter,
	workerInstance v1alpha1.WorkerInstance) error {
	key := utils.GetWorkerKeyByInstance(workerInstance)
	log.GetLogger().Infof("mark WorkerInstance of %s will deleted", key)
	workerInfo, err := etcdhandler.QueryETCDWithRetry(context.Background(), key)
	if err != nil {
		log.GetLogger().Warnf("failed to query etcd with key:%s, :%s", key, err.Error())
		m.setDeleteResponse(http.StatusInternalServerError, false, err.Error(), w)
		return err
	}
	workerIns := &v1alpha1.WorkerInstance{}
	err = json.Unmarshal([]byte(workerInfo), workerIns)
	if err != nil {
		errMsg := fmt.Sprintf("failed to unmarshal WorkerInstance, key:%s", key)
		log.GetLogger().Warnf(errMsg)
		m.setDeleteResponse(http.StatusInternalServerError, false, errMsg, w)
		return err
	}
	// @see utils.GetWorkerKeyByInstance
	workerIns.BusinessID = workerInstance.BusinessID
	workerIns.TenantID = workerInstance.TenantID
	workerIns.FuncName = workerInstance.FuncName
	workerIns.Version = workerInstance.Version
	workerIns.InstanceID = workerInstance.InstanceID
	workerIns.WillDelete = true
	err = etcdhandler.PutInstanceToETCDWithErr(context.Background(), *workerIns, etcdhandler.PutOperation)
	if err != nil {
		errMsg := fmt.Sprintf("failed to update WorkerInstance, key:%s, reason:%s", key, err.Error())
		log.GetLogger().Warnf(errMsg)
		m.setDeleteResponse(http.StatusInternalServerError, false, errMsg, w)
	} else {
		okMsg := fmt.Sprintf("mark worker %s instance will delete success", key)
		log.GetLogger().Infof(okMsg)
		m.setDeleteResponse(http.StatusOK, false, okMsg, w)
	}
	return err
}

func runWillDeleteTask(workerInstance v1alpha1.WorkerInstance, funcAPI *FunctionAPI, funcSvcID, funcName string) {
	willDeleteTasksLock.Lock()
	defer willDeleteTasksLock.Unlock()
	deleteCheckTaskIf, ok := willDeleteTasks.LoadOrStore(workerInstance.PodName, &willDeleteCheckTask{
		funcName:       funcName,
		funcSvcID:      funcSvcID,
		funcAPI:        funcAPI,
		timeout:        willDeleteCheckTimeout,
		workerInstance: workerInstance,
		closeCh:        make(chan interface{}),
	})
	if !ok {
		deleteCheckTask, ok := deleteCheckTaskIf.(*willDeleteCheckTask)
		if ok {
			go deleteCheckTask.run()
		}
	}
}

func cancelWillDeleteTask(podName string) {
	willDeleteTasksLock.Lock()
	defer willDeleteTasksLock.Unlock()
	deleteCheckTaskIf, ok := willDeleteTasks.Load(podName)
	if ok {
		deleteCheckTask, ok := deleteCheckTaskIf.(*willDeleteCheckTask)
		if ok {
			deleteCheckTask.cancel()
		}
	}
}

func (m *FunctionAPI) deleteFunctionInstance(w http.ResponseWriter, r *http.Request) {
	var reserved bool
	startTime := time.Now()
	traceID := utils.GetTraceID(r)
	request, err := m.parseDeleteRequest(w, r)
	if err != nil {
		log.GetLogger().Errorf("failed to parse delete request with error: %s", err.Error())
		m.setDeleteResponse(http.StatusInternalServerError, false, err.Error(), w)
		return
	}
	podfetcher.CancelWillDeleteTask(request.InstanceID)
	podName := utils.GetPodNameFromInstanceID(request.InstanceID)
	funcSvcID := utils.CrName(request.BusinessID, request.TenantID, request.FuncName, request.FuncVersion)
	workerInstance, found := m.getWorkInstanceByApplier(request, podName, funcSvcID)
	log.GetLogger().Infof("trying to delete worker with podName %s/%s by %s for:%s with traceID:%s, IgnoreApplier:%t",
		workerInstance.PodName, podName, request.Applier, funcSvcID, traceID, request.IgnoreApplier)
	functionService := cache.WorkerCache.GetFuncMeta(funcSvcID)
	// @See DTS2024082314358
	if found && workerInstance.Applier != constants.WorkerManagerApplier &&
		request.Applier == constants.ASBResApplier && functionService != nil &&
		commonUtils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) {
		if err = m.markWorkerInstanceWillDelete(w, workerInstance); err == nil {
			runWillDeleteTask(workerInstance, m, funcSvcID, request.FuncName)
			return
		}
	}
	cancelWillDeleteTask(podName)
	if !found {
		// this means not found the pod can scale down
		// it happened when only reversed instance or no instance
		// this situation autoScaler need retry to other cluster
		if podName == "" && request.Applier == constants.ASBResApplier {
			log.GetLogger().Infof("can delete pod not found for: %s", funcSvcID)
			m.setDeleteResponse(http.StatusOK, true, "can delete pod not found", w)
			return
		}

		log.GetLogger().Warnf("failed to get worker instance %s, %s", funcSvcID, podName)
		pod, err := m.podFetcher.GetPodByName(podName, utils.FunctionNameSpace())
		if utils.IsPodNotFound(err) {
			log.GetLogger().Infof("pod: %s is already deleted for: %s", podName, funcSvcID)
			deleteInstanceFromETCD(workerInstance, request)
			m.setDeleteResponse(http.StatusOK, false, "delete worker instance successfully", w)
			return
		}
		if err != nil {
			log.GetLogger().Errorf("failed to get pod %s, %s", podName, err.Error())
			m.setDeleteResponse(http.StatusInternalServerError, false, err.Error(), w)
			return
		}
		workerInstance = utils.MakeWorkerInsInfo(pod)
	}
	if !request.IgnoreApplier {
		if workerInstance.Applier == constants.WorkerManagerApplier && !request.IsBrokenConnection {
			log.GetLogger().Infof("no need to delete reserved instance: %s for: %s", podName, funcSvcID)
			m.setDeleteResponse(http.StatusOK, true, "no need to delete reserved instance", w)
			return
		} else if workerInstance.Applier == constants.WorkerManagerApplier && request.IsBrokenConnection {
			reserved = true
			log.GetLogger().Infof("isBrokenConnection %t, need to delete reserved instance: %s for: %s",
				request.IsBrokenConnection, podName, funcSvcID)
		}
	}
	cache.WorkerCache.RemoveStateIDFromFuncCache(funcSvcID, workerInstance.StateID)
	m.handleUpdateAgencyInfoToWorker(funcSvcID, podName, workerInstance, traceID)
	m.podFetcher.DeletePodByWorkerInstance(funcSvcID, utils.FunctionNameSpace(), traceID, workerInstance)
	_ = m.VpcHandler.IdleDeletePatServiceList(podName)
	instanceNum, _ := cache.WorkerCache.GetInstanceNum(funcSvcID)
	log.GetLogger().Infof("succeed to delete function instance: %s for: %s, cost time: %s, "+
		"currentNum: %d, IsBrokenConnection %t", workerInstance.InstanceID, request.FuncName,
		time.Since(startTime).String(), instanceNum, request.IsBrokenConnection)
	if !reserved {
		if _, ok := config.InitFuncInstResource.Load(request.FuncName); ok {
			config.InitFuncInstResource.Delete(request.FuncName)
		}
	}
	m.setDeleteResponse(http.StatusOK, reserved, "delete worker instance successfully", w)
	// clear will delete worker cached data
	willdeleteworker.GetCache().DeleteWillDeleteWorkerInstance(funcSvcID, workerInstance.InstanceID)
	m.deletePodWithLog(request, traceID, startTime)
}

func (m *FunctionAPI) getWorkInstanceByApplier(request *DeleteWorkerRequest, podName string,
	funcSvcID string) (v1alpha1.WorkerInstance, bool) {
	var workerInstance v1alpha1.WorkerInstance
	var found bool
	if request.Applier == constants.ASBResApplier && podName == "" {
		cache.WorkerCache.SetFuncExpectedNum(funcSvcID, request.ExpectedNum)
		workerInstance, found = cache.WorkerCache.GetWorkerInstanceToDeleteByASBRes(funcSvcID)
	} else {
		workerInstance, found = cache.WorkerCache.GetWorkerInstance(funcSvcID, podName)
		functionService := cache.WorkerCache.GetFuncMeta(funcSvcID)
		if functionService != nil && commonUtils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) {
			cache.WorkerCache.DecreaseFuncExpectedNum(funcSvcID)
		}
		if !found && functionService != nil {
			workerInstance.BusinessType = functionService.Spec.Configuration.BusinessType
		}
	}
	return workerInstance, found
}

func deleteInstanceFromETCD(workerInstance v1alpha1.WorkerInstance, request *DeleteWorkerRequest) {
	if request == nil {
		return
	}
	workerInstance.BusinessID = request.BusinessID
	workerInstance.TenantID = request.TenantID
	workerInstance.FuncName = request.FuncName
	workerInstance.Version = request.FuncVersion
	workerInstance.InstanceID = request.InstanceID
	etcdhandler.DeleteInstanceFromETCD(context.Background(), workerInstance)
}

func (m *FunctionAPI) deletePodWithLog(request *DeleteWorkerRequest, traceID string, startTime time.Time) {
	if request == nil {
		log.GetLogger().Warnf("request is nil")
		return
	}
	describe := fmt.Sprintf("delete successfully, cost time is %s", time.Now().Sub(startTime).String())
	podRequest := &config.RequestInfo{
		BusinessID:      request.BusinessID,
		FunctionName:    request.FuncName,
		FunctionVersion: request.FuncVersion,
		TraceID:         traceID,
	}
	m.WriteLog(podRequest, 0, 0, describe)
}

func (m *FunctionAPI) handleUpdateAgencyInfoToWorker(funcSvcID string, podName string,
	workerInstance v1alpha1.WorkerInstance, traceID string) {
	functionService := cache.WorkerCache.GetFuncMeta(funcSvcID)
	if functionService != nil {
		if len(cache.WorkerCache.GetFunctionPreStopHandler(funcSvcID)) != 0 {
			workerID := constants.DefaultAZ + "-#-" + podName
			m.podFetcher.SendUpdateAgencyInfoRequestToWorker(workerInstance, traceID, workerID, functionService, podName)
		}
	} else {
		log.GetLogger().Warnf("failed to get functionService %s, %s, %s", funcSvcID, podName, traceID)
	}
}

func (m *FunctionAPI) reportMetrics(w http.ResponseWriter, r *http.Request) {
	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to get body: %s", err.Error())
		m.setMetricsResponse(http.StatusInternalServerError, err.Error(), w)
		return
	}

	startTime := time.Now()

	var message map[string]json.RawMessage
	if err := json.Unmarshal(data, &message); err != nil {
		log.GetLogger().Errorf("failed to unmarshal body: %s", err.Error())
		m.setMetricsResponse(http.StatusInternalServerError, err.Error(), w)
		return
	}

	var viewType string
	var metricType json.RawMessage
	var exist bool
	if metricType, exist = message[workermanager.ViewType]; !exist {
		log.GetLogger().Errorf("failed to unmarshal body's view type")
		m.setMetricsResponse(http.StatusInternalServerError, "invalid view type", w)
		return
	}
	if err := json.Unmarshal(metricType, &viewType); err != nil {
		log.GetLogger().Errorf("failed to unmarshal body's view type: %s", err.Error())
		m.setMetricsResponse(http.StatusInternalServerError, err.Error(), w)
		return
	}

	var object json.RawMessage
	if object, exist = message[workermanager.ViewObject]; !exist {
		log.GetLogger().Errorf("failed to unmarshal body's object type")
		m.setMetricsResponse(http.StatusInternalServerError, "invalid obj type", w)
		return
	}
	switch viewType {
	case workermanager.WorkerMetric:
		m.handleWorkerViewMetrics(w, r, object)
	case workermanager.PendingRequestMetric:
		m.handlePendingRequestMetrics(w, r, object)
	case workermanager.DiskUsageRequestMetric:
		m.handleDiskUsageRequestMetrics(w, r, object)
	default:
		log.GetLogger().Errorf("this %s type of data cannot be processed", viewType)
	}

	m.setMetricsResponse(http.StatusOK, "report metrics successfully", w)
	describe := fmt.Sprintf("report metrics, cost time is %s", time.Now().Sub(startTime).String())
	m.WriteLog(&config.RequestInfo{}, 0, 0, describe)
}

func (m *FunctionAPI) handleWorkerViewMetrics(w http.ResponseWriter, r *http.Request, data json.RawMessage) {
	var views []workermanager.WorkerView
	if err := json.Unmarshal(data, &views); err != nil {
		log.GetLogger().Errorf("failed to unmarshal body's worker view: %s", err.Error())
		m.setMetricsResponse(http.StatusInternalServerError, err.Error(), w)
		return
	}
	log.GetLogger().Debugf("receive metrics with %d views, view type %s", len(views), workermanager.WorkerMetric)
	for _, view := range views {
		podfetcher.AddAutoScalerMetrics(r.Context(), view)
	}
}

func (m *FunctionAPI) handlePendingRequestMetrics(w http.ResponseWriter, r *http.Request, data json.RawMessage) {
	var views []workermanager.PendingRequestView
	if err := json.Unmarshal(data, &views); err != nil {
		log.GetLogger().Errorf("failed to unmarshal body's pending request view: %s", err.Error())
		m.setMetricsResponse(http.StatusInternalServerError, err.Error(), w)
		return
	}
	log.GetLogger().Debugf("receive metrics with %d views, view type %s", len(views),
		workermanager.PendingRequestMetric)
	for _, view := range views {
		podfetcher.AddPendingRequestMetrics(view)
	}
}

func (m *FunctionAPI) handleDiskUsageRequestMetrics(w http.ResponseWriter, r *http.Request, data json.RawMessage) {
	var views workermanager.MirroredDiskSpaceView
	if err := json.Unmarshal(data, &views); err != nil {
		log.GetLogger().Errorf("failed to unmarshal body's diskUsage request view: %s", err.Error())
		m.setMetricsResponse(http.StatusInternalServerError, err.Error(), w)
		return
	}
	log.GetLogger().Debugf("receive metrics with %+v views, view type %s", views, workermanager.DiskUsageRequestMetric)
	_, ok := cache.NodeMirroredDiskCache.Load(views.NodeIP)
	if !ok {
		log.GetLogger().Debugf("failed to load node: %s from node cache", views.NodeIP)
		return
	}
	cache.NodeMirroredDiskCache.Store(views.NodeIP, views.FreeDiskSpace)
	log.GetLogger().Debugf("success store node: %s mirror disk space: %s to cache", views.NodeIP, views.FreeDiskSpace)
}

func (m *FunctionAPI) setMetricsResponse(statusCode int, msg string, w http.ResponseWriter) {
	res := ReportMetricsResponse{
		Code:    statusCode,
		Message: msg,
	}
	resBody, err := json.Marshal(res)
	if err != nil {
		log.GetLogger().Errorf("failed to set metrics response %s", err.Error())
		resBody = snerror.Marshal(snerror.New(utils.JSONParseError,
			"report metrics error"))
	}
	w.WriteHeader(statusCode)
	if _, err := w.Write(resBody); err != nil {
		log.GetLogger().Errorf("failed to write resBody %s", err.Error())
	}
}

func (m *FunctionAPI) setDeleteResponse(statusCode int, reserved bool, message string, w http.ResponseWriter) {
	res := DeleteWorkerResponse{
		Code:     statusCode,
		Reserved: reserved,
		Message:  message,
	}
	resBody, err := json.Marshal(res)
	if err != nil {
		log.GetLogger().Errorf("failed to set delete response %s", err.Error())
		response := snerror.Marshal(snerror.New(utils.JSONParseError,
			"function instance deletion error"))
		_, writeErr := w.Write(response)
		if writeErr != nil {
			log.GetLogger().Errorf("failed to write resBody %s", writeErr.Error())
			return
		}
		return
	}
	w.WriteHeader(statusCode)
	_, writeErr := w.Write(resBody)
	if writeErr != nil {
		log.GetLogger().Errorf("failed to write resBody %s", writeErr.Error())
		return
	}
}

// WriteLog writes log
func (m *FunctionAPI) WriteLog(reqInfo *config.RequestInfo, innerCode int, code int, describe string) {
	objectLog := &interfacelog.InterfaceLog{}
	if reqInfo != nil {
		objectLog.BusinessID = reqInfo.BusinessID
		// objectLog.SrcAppID not use
		objectLog.FunctionName = reqInfo.FunctionName
		// objectLog.Alias  not use
		objectLog.Version = reqInfo.FunctionVersion
		objectLog.TraceID = reqInfo.TraceID
	} else {
		objectLog.BusinessID = m.interfaceLog.BusinessID
		// objectLog.SrcAppID not use
		objectLog.FunctionName = m.interfaceLog.FunctionName
		// objectLog.Alias  not use
		objectLog.Version = m.interfaceLog.Version
	}
	if innerCode != 0 {
		// for 2 <= base <= 36. The result uses the lower-case letters 'a' to 'z'
		objectLog.InnerCode = strconv.FormatInt(int64(innerCode), strConvMinDigitValue)
	}
	if code != 0 {
		// for 2 <= base <= 36. The result uses the lower-case letters 'a' to 'z'
		objectLog.Code = strconv.FormatInt(int64(code), strConvMinDigitValue)
	}

	objectLog.Desc = describe

	interfacelog.Write(*objectLog)
}

func (m *FunctionAPI) errorResponse(r *config.RequestInfo, w http.ResponseWriter, err error, describe string) {
	errorCode := utils.StatusInternalServerError
	errorMessage := err.Error()
	if snErr, ok := err.(snerror.SNError); ok {
		errorCode = snErr.Code()
		errorMessage = snErr.Error()
	}
	// error response code must not be 0
	if errorCode == 0 {
		log.GetLogger().Warnf("invalid error code for %s", r.TraceID)
		errorCode = utils.StatusInternalServerError
	}

	w.Header().Add(constants.HeaderStartDuration, podfetcher.ErrorDuration+"/-2") // '-2' means error time
	w.WriteHeader(http.StatusInternalServerError)
	response, mErr := json.Marshal(snerror.BadResponse{Code: errorCode, Message: errorMessage})
	if mErr != nil {
		respDescribe := fmt.Sprintf("response marshal error %s", mErr.Error())
		m.WriteLog(r, utils.StatusInternalServerError,
			utils.ErrTrans[utils.StatusInternalServerError], respDescribe)
		return
	}
	_, wErr := w.Write(response)
	if wErr != nil {
		respDescribe := fmt.Sprintf("writing response error %s", wErr.Error())
		m.WriteLog(r, utils.StatusInternalServerError,
			utils.ErrTrans[utils.StatusInternalServerError], respDescribe)
		return
	}
	return
}

func (m *FunctionAPI) scaleLimitResponse(r *config.RequestInfo, w http.ResponseWriter,
	functionService *faasfunction.FunctionService, message string, innerCode int) {
	costTime := time.Now().Sub(r.StartTime)
	r.Duration = fmt.Sprintf("%s/%s", r.Duration, costTime)
	w.Header().Add(constants.HeaderStartDuration, r.Duration)
	w.WriteHeader(http.StatusOK)
	resp := GetWorkerSuccessResponse{
		Code:     innerCode,
		Message:  message,
		Worker:   m.convertFunctionService2Worker(functionService, r.TraceID),
		Instance: nil,
	}
	respData, err := json.Marshal(resp)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal response. funcName: %s, traceID: %s, error: %s",
			r.FunctionName, r.TraceID, err.Error())
		return
	}
	_, err = w.Write(respData)
	if err != nil {
		log.GetLogger().Errorf("failed to write response. funcName: %s, traceID: %s, error: %s",
			r.FunctionName, r.TraceID, err.Error())
		return
	}
	log.GetLogger().Debugf("[scaleLimitResponse] current ins info is %v", resp.Instance)
	m.WriteLog(r, innerCode, utils.ErrTrans[utils.StatusInternalServerError], message)
}

func (m *FunctionAPI) successResponse(w http.ResponseWriter,
	reqInfo *config.RequestInfo, functionService *faasfunction.FunctionService, instance *commontypes.Instance) {
	costTime := time.Now().Sub(reqInfo.StartTime)
	reqInfo.Duration = fmt.Sprintf("%s/%s", reqInfo.Duration, costTime)
	w.Header().Add(constants.HeaderStartDuration, reqInfo.Duration)
	w.WriteHeader(http.StatusOK)
	if commonUtils.IsDirectFunc(reqInfo.PodTemplate,
		functionService.Spec.Configuration.Runtime.Language) {
		instance.IsDirectFunc = true
	}
	instance.HasInitializer = len(functionService.Spec.Configuration.Initializer.Handler) != 0
	resp := GetWorkerSuccessResponse{
		Code: utils.StatusOk,
		Message: fmt.Sprintf("get worker %s successfully, traceID %s",
			functionService.Spec.Configuration.FunctionName, reqInfo.TraceID),
		Worker:   m.convertFunctionService2Worker(functionService, reqInfo.TraceID),
		Instance: instance,
	}
	if reqInfo.LoadErr != nil {
		resp.Code = reqInfo.LoadErr.Code
		resp.Message = reqInfo.LoadErr.Message
	}
	log.GetLogger().Debugf("current instance info is %v", resp.Instance)
	log.GetLogger().Infof("succeed to describe function instance: %s for: %s, cost time: %s, traceID %s",
		instance.InstanceID, reqInfo.FunctionName, costTime, reqInfo.TraceID)
	respData, err := json.Marshal(resp)
	if err != nil {
		log.GetLogger().Errorf("function name: %s json marshal response error, traceID %s: %s",
			reqInfo.FunctionName, reqInfo.TraceID, err.Error())
		return
	}
	_, err = w.Write(respData)
	if err != nil {
		log.GetLogger().Errorf("function name: %s write response error, traceID %s: %s",
			reqInfo.FunctionName, reqInfo.TraceID, err.Error())
		return
	}
	describe := fmt.Sprintf("describe success, getPod/specifyPod/deployFunction/deployLayers/startFunction"+
		"/describePod cost time is %s", reqInfo.Duration)
	m.WriteLog(reqInfo, 0, 0, describe)
}

func (m *FunctionAPI) convertFunctionService2Worker(
	functionService *faasfunction.FunctionService, tenantID string) *Worker {
	var instances []*commontypes.Instance
	for _, i := range functionService.Status.Instances {
		instances = append(instances, &commontypes.Instance{
			IP:           i.NodeIP,
			Port:         i.NodePort,
			InstanceID:   i.InstanceID,
			DeployedNode: i.NodeName,
			TenantID:     tenantID,
		})
	}
	return &Worker{
		FunctionName:    functionService.Spec.Configuration.FunctionName,
		FunctionVersion: utils.VersionRestore(functionService.Spec.Configuration.FunctionVersion),
		Tenant:          functionService.Spec.Configuration.TenantID,
		Business:        functionService.Spec.Configuration.BusinessID,
		Instances:       instances,
	}
}

func (m *FunctionAPI) queryTerminatingLongRunByHostIP(c *gin.Context) {
	pm := poolmanager.GetPoolManager()
	if pm == nil {
		log.GetLogger().Errorf("failed to handle request")
		c.Status(http.StatusInternalServerError)
		return
	}
	pods := pm.QueryTerminatingLongRunByHostIP(c.Param("hostIP"))
	c.JSON(http.StatusOK, pods)
}

func (m *FunctionAPI) updateSnapshotCriuOpts(service *faasfunction.FunctionService) {
	cfg, err := config.GetConfig()
	if err != nil {
		return
	}
	service.Spec.SnapshotDeployment.SnapshotCriuOpts = cfg.PoolMgrConfig.CheckpointPodConfig.CRIUCheckpointOpts
}

func handleComponentHealthCheck(w http.ResponseWriter, request *http.Request) {
	if !etcd3.GetRouterEtcdClient().GetEtcdStatusLostContact() {
		w.WriteHeader(http.StatusInternalServerError)
		w.Header().Add(constants.HeaderRouterEtcdState, "false")
		return
	}
	w.WriteHeader(http.StatusOK)
	return
}

func (m *FunctionAPI) setInstancesResponse(statusCode int, instances commontypes.QueryInstance, message string,
	w http.ResponseWriter) {
	res := commontypes.QueryInstanceResponse{
		Code:      statusCode,
		Instances: instances,
		Message:   message,
	}

	resBody, err := json.Marshal(res)
	if err != nil {
		log.GetLogger().Errorf("failed to set query instances response %s", err.Error())
		response := snerror.Marshal(snerror.New(utils.JSONParseError,
			"function instance query error"))
		_, writeErr := w.Write(response)
		if writeErr != nil {
			log.GetLogger().Errorf("failed to write JSONParseError resBody %s", writeErr.Error())
			return
		}
		return
	}
	w.WriteHeader(statusCode)
	_, writeErr := w.Write(resBody)
	if writeErr != nil {
		log.GetLogger().Errorf("failed to write resBody %s", writeErr.Error())
		return
	}
}

func (m *FunctionAPI) queryInstances(w http.ResponseWriter, r *http.Request) {
	funcSvcID := r.URL.Query().Get(constants.FuncSvcID)
	log.GetLogger().Infof("receive queryInstance req, funcSvcID: %s", funcSvcID)
	instanceMap, isBridgeFunc, scalingNum := cache.WorkerCache.GetInstanceMap(funcSvcID)
	if instanceMap == nil {
		errMsg := fmt.Sprintf("failed to get function cache, funcSvcID: %s", funcSvcID)
		log.GetLogger().Warnf("failed to get instance map: %s", errMsg)
		m.setInstancesResponse(http.StatusInternalServerError, commontypes.QueryInstance{}, errMsg, w)
		return
	}
	expectNum, err := cache.WorkerCache.GetFuncExpectedNum(funcSvcID)
	if err != nil {
		log.GetLogger().Warnf("failed to get function expected num, err: %s,", err.Error())
		expectNum = -1
	}
	var podsName []string
	for _, instance := range instanceMap {
		podsName = append(podsName, instance.PodName)
	}
	qi := commontypes.QueryInstance{
		ClusterID:      clusterID,
		PodsName:       podsName,
		ScalingNum:     scalingNum,
		ExpectNum:      expectNum,
		ExpectedMinIns: cache.WorkerCache.GetFuncExpectedMinIns(funcSvcID),
		ExpectedSnap:   checkpoint.GetCheckpointManager().GetSnapshotCount(funcSvcID),
	}
	if isBridgeFunc {
		bridgePodsInfo := make(map[string]string, len(instanceMap))
		for _, instance := range instanceMap {
			bridgePodsInfo[instance.StateID] = instance.PodName
		}
		qi.BridgePodsInfo = bridgePodsInfo
	}
	m.setInstancesResponse(http.StatusOK, qi, "query instances successfully", w)
}

func (m *FunctionAPI) parseVPARequest(r *http.Request) (*config.VPARequest, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	request := &config.VPARequest{}
	err = json.Unmarshal(data, request)
	if err != nil {
		return nil, err
	}

	return request, nil
}

func (m *FunctionAPI) revisePodResource(c *gin.Context) {
	var err error
	defer func() {
		if err != nil {
			c.Writer.Header().Set(constants.HeaderInnerCode, strconv.Itoa(snerror.VPAResourceMsgTransferError))
			m.setVPAResponse(http.StatusInternalServerError, err.Error(), c.Writer)
		} else {
			m.setVPAResponse(http.StatusOK, "adjust pause pod resource success", c.Writer)
		}
	}()

	startTime := time.Now()
	traceID := commonUtils.InitTraceID(c.Request)
	requestInfo, err := m.parseVPARequest(c.Request)
	if err != nil {
		log.GetLogger().Errorf("failed to parse instance resource view request traceID: %s, error: %s", traceID,
			err.Error())
		return
	}
	log.GetLogger().Infof("receive the instanceview from busproxy, nodeName: %s, nodeIp: %s, CPUUsage: %f,"+
		" MemoryUsage: %f, traceID: %s", requestInfo.Node.NodeName, requestInfo.Node.NodeIP, requestInfo.Node.CPUUsage,
		requestInfo.Node.MemoryUsage, traceID)
	if logconfig.LogLevel == zap.DebugLevel {
		for _, funcResource := range requestInfo.InstanceList.VpaInsView {
			log.GetLogger().Debugf("receive the instance vpa resource, nodeName: %s, funcName: %s, podName: %s, "+
				"vpaResource: %+v, containerIDs: %+v, traceID: %s", requestInfo.Node.NodeName, funcResource.FuncName,
				funcResource.PodName, funcResource.VpaResource, *funcResource.ContainerIDs, traceID)
		}
	}
	cVpaAlignResourceValue, err := calculateVPAAlignResourceValue(requestInfo, traceID)
	if err != nil {
		log.GetLogger().Errorf("failed to calculate vpa align resource value, traceId: %s error: %s",
			traceID, err.Error())
		return
	}
	if cVpaAlignResourceValue.requestCPU < 0 ||
		cVpaAlignResourceValue.limitCPU < 0 || cVpaAlignResourceValue.limitMemory < 0 {
		errMsg := fmt.Sprintf("the value of calculate vpa align resource is negative, traceID: %s", traceID)
		log.GetLogger().Error(errMsg)
		err = errors.New(errMsg)
		return
	}
	if cVpaAlignResourceValue.requestCPU > cVpaAlignResourceValue.limitCPU {
		// In the CAE scenario, only the request CPU is adjusted.
		// As a result, the alignment value of limit CPU is less than that of request CPU.
		cVpaAlignResourceValue.limitCPU = cVpaAlignResourceValue.requestCPU
	}
	requestInfo.Node.NodeName = strings.TrimPrefix(requestInfo.Node.NodeName, constants.DefaultAZ+"-#-")
	pausePodName := utils.VpaPausePodPrefix + requestInfo.Node.NodeName
	pausePod, err := m.podFetcher.GetPodByName(pausePodName, utils.FunctionNameSpace())
	if err != nil {
		log.GetLogger().Errorf("failed to get pause pod by name %s, error: %s", pausePodName, err.Error())
		return
	}
	if checkIsNeedUpdatePausePodResource(cVpaAlignResourceValue, pausePod, traceID) {
		err = m.updateVpaPod(cVpaAlignResourceValue, pausePod, traceID)
		if err != nil {
			log.GetLogger().Errorf("failed to update pause pod by name %s, error: %s", pausePodName, err.Error())
			return
		}
	}
	log.GetLogger().Infof("success to adjust pause pod resources by node %s with traceID: %s, cost time: %s",
		requestInfo.Node.NodeIP, traceID, time.Since(startTime).String())
}

type vpaAlignResourceValue struct {
	requestCPU  int64
	limitCPU    int64
	limitMemory int64
}

func checkIsNeedUpdatePausePodResource(v *vpaAlignResourceValue, pausePod *corev1.Pod, traceID string) bool {
	lVpaRequestCPU, lVpaLimitCPU, lVpaLimitMemory := getLastVpaAlignResourceValue(pausePod, traceID)
	diffRequestCPU := v.requestCPU - lVpaRequestCPU
	diffLimitCPU := v.limitCPU - lVpaLimitCPU
	diffLimitMemory := v.limitMemory - lVpaLimitMemory
	threshold := vpa.GetVpaThresholdCPU()
	if diffRequestCPU > threshold || diffLimitCPU > threshold || diffLimitMemory > threshold {
		// scale up > threshold
		return true
	}
	if -diffRequestCPU > threshold || -diffLimitCPU > threshold || -diffLimitMemory > threshold {
		// scale down > threshold
		return true
	}
	return false
}

func getLastVpaAlignResourceValue(pausePod *corev1.Pod, traceID string) (int64, int64, int64) {
	lastRequest := pausePod.Spec.Containers[0].Resources.Requests
	lastLimit := pausePod.Spec.Containers[0].Resources.Limits
	lastVpaAlignRequestCPU := lastRequest.Cpu().MilliValue() - config.GetInitVpaPodSpec().CPU
	lastVpaAlignLimitCPU := lastLimit.Cpu().MilliValue() - config.GetInitVpaPodSpec().CPU
	lastVpaAlignLimitMemory := (lastLimit.Memory().Value() >>
		utils.MiToByteExponent) - config.GetInitVpaPodSpec().Memory
	log.GetLogger().Infof("vpa-align pause pod %s last time request cpu %d, limit cpu %d, memory %d, traceID: %s",
		pausePod.Name, lastVpaAlignRequestCPU, lastVpaAlignLimitCPU, lastVpaAlignLimitMemory, traceID)
	return lastVpaAlignRequestCPU, lastVpaAlignLimitCPU, lastVpaAlignLimitMemory
}

func calculateVPAAlignResourceValue(requestInfo *config.VPARequest, traceID string) (*vpaAlignResourceValue, error) {
	var totalVpaRequestCPU, totalVpaLimitCPU, totalVpaMem int64
	var totalInitRequestCPU, totalInitLimitCPU, totalInitMem int64
	for _, ins := range requestInfo.InstanceList.VpaInsView {
		if ins.VpaResource.CPU.Share >= math.MaxInt32 {
			errMsg := fmt.Sprintf("funcName: %s cpu share: %d is exceeded the limit", ins.FuncName,
				ins.VpaResource.CPU.Share)
			return nil, errors.New(errMsg)
		}
		// cpu.shares = (cpu in milli-cores * 1000) / 1024，
		insRequestCPU := ins.VpaResource.CPU.Share * utils.CPUToMillCores / utils.CPUShare
		if ins.VpaResource.CPU.Share*utils.CPUToMillCores%utils.CPUShare > 0 {
			insRequestCPU++
		}
		insLimitCPU := ins.VpaResource.CPU.Quota / utils.CPUPeriodToMillCores
		insMemory := ins.VpaResource.Memory.Limit >> utils.MiToByteExponent
		log.GetLogger().Debugf("vpa-align funcName: %s vpa request cpu: %d,limit cpu %d, memory %d, traceID %s",
			ins.FuncName, insRequestCPU, insLimitCPU, insMemory, traceID)
		podName := strings.TrimPrefix(ins.PodName, constants.DefaultAZ+"-#-")
		pod, err := utils.GetPodInformer().Lister().Pods(utils.FunctionNameSpace()).Get(podName)
		if err != nil {
			log.GetLogger().Warnf("the pod %s not existed, traceID: %s", podName, traceID)
			continue
		}
		initRequest, initLimit := resourcehelper.PodRequestsAndLimits(pod)
		log.GetLogger().Debugf("vpa-align funcName: %s init request cpu: %d, limit cpu %d, memory %d, traceID %s",
			ins.FuncName, initRequest.Cpu().MilliValue(), initLimit.Cpu().MilliValue(),
			initLimit.Memory().Value()>>utils.MiToByteExponent, traceID)
		totalInitRequestCPU += initRequest.Cpu().MilliValue()
		totalInitLimitCPU += initLimit.Cpu().MilliValue()
		totalInitMem += initLimit.Memory().Value() >> utils.MiToByteExponent

		totalVpaRequestCPU += insRequestCPU
		totalVpaLimitCPU += insLimitCPU
		totalVpaMem += insMemory
	}
	v := &vpaAlignResourceValue{
		requestCPU:  totalVpaRequestCPU - totalInitRequestCPU,
		limitCPU:    totalVpaLimitCPU - totalInitLimitCPU,
		limitMemory: totalVpaMem - totalInitMem,
	}
	log.GetLogger().Infof("vpa-align node %s request cpu %d, limit cpu %d, memory %d traceID: %s",
		requestInfo.Node.NodeName, v.requestCPU, v.limitCPU, v.limitMemory, traceID)
	return v, nil
}

func (m *FunctionAPI) updateVpaPod(v *vpaAlignResourceValue, pausePod *corev1.Pod, traceID string) error {
	log.GetLogger().Infof("vpa-align update vpa pause pod %s with request cpu %d, limit cpu %d, memory %d traceID: %s",
		pausePod.Name, v.requestCPU, v.limitCPU, v.limitMemory, traceID)
	pausePodResources := getResourceByCPUAndMem(v.requestCPU, v.limitCPU, v.limitMemory)
	vpa.SetPausePodResourceRequirements(pausePod.Name, &pausePodResources)
	err := vpa.NewVpaPausePod().DeleteVpaPod(context.Background(), pausePod)
	if err != nil {
		log.GetLogger().Errorf("failed to delete vpa pause pod %s with traceID %s, error: %s",
			pausePod.Name, traceID, err.Error())
		return err
	}
	return nil
}

func getResourceByCPUAndMem(requestCPU, limitCPU, mem int64) corev1.ResourceRequirements {
	var pausePodResources = corev1.ResourceRequirements{
		Requests: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", requestCPU)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", mem)),
		},
		Limits: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", limitCPU)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", mem)),
		},
	}
	return pausePodResources
}

func (m *FunctionAPI) setVPAResponse(statusCode int, message string, w http.ResponseWriter) {
	res := VPAResponse{
		Code:    statusCode,
		Message: message,
	}
	resBody, err := json.Marshal(res)
	if err != nil {
		log.GetLogger().Errorf("failed to set vpa response %s", err.Error())
		response := snerror.Marshal(snerror.New(utils.JSONParseError,
			"vpa response error"))
		_, writeErr := w.Write(response)
		if writeErr != nil {
			log.GetLogger().Errorf("failed to write resBody %s", writeErr.Error())
			return
		}
		return
	}
	w.WriteHeader(statusCode)
	_, writeErr := w.Write(resBody)
	if writeErr != nil {
		log.GetLogger().Errorf("failed to write resBody %s", writeErr.Error())
		return
	}
}

// RegisterAPI register api
func (m *FunctionAPI) RegisterAPI(router *gin.Engine) {
	router.GET("/componenthealth", func(c *gin.Context) {
		handleComponentHealthCheck(c.Writer, c.Request)
	})
	router.POST(m.Path+"/:name"+"/worker", tracer.WrapGinHandler(m.describeFunction))
	router.POST(m.Path+"/funcmeta", m.getFuncMetaInfo)
	router.DELETE(m.Path+"/worker/delete", func(c *gin.Context) {
		m.deleteFunctionInstance(c.Writer, c.Request)
	})
	router.POST(m.Path+"/worker/metrics", func(c *gin.Context) {
		m.reportMetrics(c.Writer, c.Request)
	})
	router.GET(m.Path+"/longrun/:hostIP", m.queryTerminatingLongRunByHostIP)
	router.GET(m.Path+"/instances", func(c *gin.Context) {
		m.queryInstances(c.Writer, c.Request)
	})
	router.POST("/worker-manager/v1/microservices"+"/:name"+"/worker", m.scaleUpFunction)
	router.DELETE("/worker-manager/v1/microservices"+"/:name"+"/worker", m.scaleDownFunction)

	router.POST("/worker-manager/v1/instanceview/resourceview", m.revisePodResource)
}
