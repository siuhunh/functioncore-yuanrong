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

// Package podfetcher -
package podfetcher

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	fhttp "github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/podevent"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/strutil"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/tokenrotation"
	"functioncore/pkg/common/tracer"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/common/uuid"
	commonvpa "functioncore/pkg/common/vpa"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/iam"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/preload"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
	"functioncore/pkg/worker-manager/vpcmanager"
)

const (
	// LayerInfoSep define the LayerInfo separation character
	LayerInfoSep = ","
	// for network
	commonTimeout = 3
	// defaultPodGeneralizeTimeout reserves 60s for generalizePod and 3s for network
	defaultPodGeneralizeTimeout = 30 + commonTimeout
	// ErrorDuration define exception cold time
	ErrorDuration = "-2/-2/-2/-2/-2"
	// maxEnqueueRetries define the max enqueue times
	maxRetries                            = 3
	setBillingSwitchRetryInterval         = 5 * time.Second
	setBillingSwitchTimeout               = 10 * time.Second
	workerPort                            = "8081"
	defaultWorkerMetricsTick              = 60
	defaultResourceUsageTick              = 1
	defaultCaeMetricsTick                 = 15
	defaultBridgeMetricsTick              = 1
	defaultBridgeMetricsCollectionTimeout = 1
	// defaultUpdateWorkerAgencyTimeout reserves 30s for update worker agency
	defaultUpdateWorkerAgencyTimeout = 30

	memoryAccess = 1024
	base         = 10
	bitSize      = 64

	maxLogPaths = 20

	functionTaskApplyPrefix = "functiontask"

	customContainerDefaultWaitReadyTimeout = 5 * time.Minute

	// split cost from worker-manager cost
	costSeparator           = "/"
	getPodCostPrefix        = "choosePod:"
	specializePodCostPrefix = "workerMgrTotal:"
	costLength              = 2
	getPodCostIndex         = 0
	specializePodCostIndex  = 1
	podNameMinLength        = 2
	podNameIndex            = 0
	bridgeMaxInsRatio       = 1.2
)

// enum of request priority
const (
	PriorityPrewarmInstance = iota
	PriorityDefault
	PriorityFirstInstance
	PriorityReservedInstance
)

// generalizePodBackoff make backoff in [8s,12s]
var generalizePodBackoff = wait.Backoff{
	Steps:    3, // retry times (include first time)
	Duration: 2 * time.Second,
	Factor:   1,
	Jitter:   5,
}
var (
	clusterID             = os.Getenv(constants.ClusterID)
	cacheManagerClusterIP string
	safeVpcDNSAddr        = os.Getenv(constants.DNSResolveAddrKey)
)

const (
	retryTime                     = 5
	redisRetryTime                = 3
	retryInterval                 = 1 * time.Second
	defaultSubmitResultRetryTimes = 3
	redisCxtTimeout               = 10 * time.Second
	expire                        = "expire"
	circuitBreaking               = "circuit_breaking"
	redisRetryInterval            = 100 * time.Millisecond
	k8sGetServiceTimeout          = 5 * time.Second

	restoreFailedAlarmExpireTime     = 1 * time.Hour
	restoreFailedCountExpireTime     = 12 * time.Hour
	restoreFailedNumStep             = 1
	singleFuncRestoreFailedThreshold = 2
	multiFuncRestoreFailedThreshold  = 5
	restoreFailedFuncKey             = "restore-failed-func-collect"
)

// enum of creator info index
const (
	CreatorFuncNameIndex = iota
	CreatorFuncVersionIndex
	CreatorStateIDIndex
)

// CREvent cr event in queue
type CREvent struct {
	pod             *corev1.Pod
	requestInfo     *config.RequestInfo
	functionService *faasfunction.FunctionService
}

type restoreFailedInfo struct {
	funcSvcID string
	tenantID  string
	count     int64
}

// GetFuncMeta -
func (c *CREvent) GetFuncMeta() *faasfunction.FunctionService {
	return c.functionService
}

// Ctx is context for functionService
type Ctx struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// PodFetcher define a structure to generalizePod from nodemanager
type PodFetcher struct {
	workerClient          *fhttp.Client
	VpcHandler            *vpcmanager.VpcHandler
	PoolMgr               *poolmanager.PoolMgr
	KubeClient            kubernetes.Interface
	podLister             listercorev1.PodLister
	Queue                 queue
	stopCh                <-chan struct{}
	contexts              map[string]*Ctx
	contextsMutex         sync.Mutex
	podGeneralizeTimeout  time.Duration
	httpsEnable           bool
	IdleRetryInfo         *IdleRetryList
	caeBusInstanceMutexes map[string]*sync.Mutex
	instanceMutex         sync.Mutex
}

var (
	podFetcher    *PodFetcher
	updateEtcdMap = sync.Map{}

	readWorkerContainerLogLimitBytes int64 = 4096
)

func getRequestPriority(req *config.RequestInfo) int {
	if req.Applier == constants.WorkerManagerApplier {
		return PriorityReservedInstance
	}
	if req.Applier == constants.PredictionApplier {
		return PriorityPrewarmInstance
	}
	if req.IsFirstInstance {
		return PriorityFirstInstance
	}
	return PriorityDefault
}

func getGeneralizeTimeoutFromCfg() (int, error) {
	podGeneralizeTimeout := defaultPodGeneralizeTimeout
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config error: %s", err.Error())
		return podGeneralizeTimeout, err
	}
	if cfg == nil {
		log.GetLogger().Errorf("config is nil")
		return podGeneralizeTimeout, err
	}

	if cfg.PodGeneralizeTimeout > 0 {
		podGeneralizeTimeout = cfg.PodGeneralizeTimeout
	}

	return podGeneralizeTimeout, nil
}

// CreatePodFetcher creates pod fetcher
func CreatePodFetcher(workerClient *fhttp.Client, stopCh <-chan struct{}) error {
	kubeClient := utils.GetKubernetesClient()
	if kubeClient == nil {
		return errors.New("failed to get client")
	}
	poolMgr := poolmanager.GetPoolManager()
	if poolMgr == nil {
		return errors.New("failed to get pool manager")
	}
	q := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second), // for queue capacity
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(500), 2000)},        // for queue rate
	))
	vpcController, err := vpcmanager.GetVPCController()
	if err != nil {
		return err
	}
	podGeneralizeTimeout, err := getGeneralizeTimeoutFromCfg()
	if err != nil {
		log.GetLogger().Errorf("failed to get generalize timeout config, err: %s", err.Error())
		return err
	}
	alarm.GetClient().RegisterAlarmType(alarm.OBSConnection)
	idleRetryInfo, err := initIdleRetryList()
	if err != nil {
		log.GetLogger().Errorf("failed to init idle retry list, err: %s", err.Error())
		return err
	}
	podFetcher = &PodFetcher{
		workerClient: workerClient,
		VpcHandler:   vpcController,
		KubeClient:   kubeClient,
		PoolMgr:      poolMgr,
		podLister:    utils.GetPodInformer().Lister(),
		Queue: queue{
			RateLimitingInterface: q,
			processingQueue:       make(map[string]interface{}, utils.DefaultMapSize),
		},
		stopCh:               stopCh,
		podGeneralizeTimeout: time.Duration(podGeneralizeTimeout) * time.Second,
		httpsEnable:          config.EnableHTTPS(),
		IdleRetryInfo:        idleRetryInfo,
	}
	podFetcher.InitContexts()
	podFetcher.InitCAEBusInstanceMutexes()
	vpcmanager.RegisterSendPatPodIPFunc(podFetcher.SendPatPodIPToWorkerByRetry)
	poolmanager.RegisterCleanPodsCallBack(podFetcher.CleanPodsInNode)
	poolmanager.RegisterDoActionsCallBack(podFetcher.DoActions)
	poolmanager.RegisterSetBillingSwitchToWorkerCallBack(podFetcher.SetBillingSwitchToWorker)
	go podFetcher.Run()
	return nil
}

// queue is wrapper of RateLimitingInterface, which add storage processing event
type queue struct {
	workqueue.RateLimitingInterface
	mutex           sync.Mutex
	processingQueue map[string]interface{}
}

// Done remove event from queue
func (q *queue) Done(event interface{}) {
	q.RateLimitingInterface.Done(event)
	q.RateLimitingInterface.Forget(event)
	ev, ok := event.(CREvent)
	if !ok {
		log.GetLogger().Errorf("failed to converted interface to CREvent")
		return
	}
	q.mutex.Lock()
	delete(q.processingQueue, ev.pod.Name)
	q.mutex.Unlock()
}

// AddRateLimited add event to queue
func (q *queue) AddRateLimited(event interface{}) {
	q.RateLimitingInterface.AddRateLimited(event)
	ev, ok := event.(CREvent)
	if !ok {
		log.GetLogger().Warnf("failed to converted interface to CREvent")
		return
	}
	q.mutex.Lock()
	q.processingQueue[ev.pod.Name] = event
	q.mutex.Unlock()
	return
}

// DeleteEventInQueue delete processing event in queue
func (q *queue) DeleteEventInQueue(podName string) CREvent {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if event, exist := q.processingQueue[podName]; exist {
		ev, ok := event.(CREvent)
		if !ok {
			log.GetLogger().Warnf("failed to converted interface to CREvent")
			return CREvent{}
		}
		q.RateLimitingInterface.Done(event)
		q.RateLimitingInterface.Forget(event)
		delete(q.processingQueue, ev.pod.Name)
		return ev
	}
	return CREvent{}
}

// GetPodFetcher get pod fetcher
func GetPodFetcher() *PodFetcher {
	return podFetcher
}

// InitContexts init contexts map
func (p *PodFetcher) InitContexts() {
	p.contexts = make(map[string]*Ctx, utils.DefaultMapSize)
}

// InitCAEBusInstanceMutexes init caeBusInstanceMutexes map
func (p *PodFetcher) InitCAEBusInstanceMutexes() {
	p.caeBusInstanceMutexes = make(map[string]*sync.Mutex, utils.DefaultMapSize)
}

// GetContext get context for funcSvcID
func (p *PodFetcher) GetContext(funcSvcID string) (context.Context, bool) {
	p.contextsMutex.Lock()
	ctx, ok := p.contexts[funcSvcID]
	p.contextsMutex.Unlock()
	if !ok {
		return nil, false
	}
	return ctx.ctx, true
}

// GetCAEBusInstanceMutexes get instanceMutex for funcSvcID
func (p *PodFetcher) GetCAEBusInstanceMutexes(funcSvcID string) *sync.Mutex {
	p.instanceMutex.Lock()
	defer p.instanceMutex.Unlock()
	mux, ok := p.caeBusInstanceMutexes[funcSvcID]
	if ok && mux != nil {
		return mux
	}
	mux = new(sync.Mutex)
	p.caeBusInstanceMutexes[funcSvcID] = mux
	return mux
}

// CreateNewContext create new context for functionService
func (p *PodFetcher) CreateNewContext(functionServiceID string) context.Context {
	p.contextsMutex.Lock()
	if ctx, exist := p.contexts[functionServiceID]; exist {
		p.contextsMutex.Unlock()
		return ctx.ctx
	}
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, utils.FunctionServiceIDKey{},
		functionServiceID+commonutils.LayerListSep+time.Now().String())
	p.contexts[functionServiceID] = &Ctx{
		ctx:        ctx,
		cancelFunc: cancel,
	}
	p.contextsMutex.Unlock()
	log.GetLogger().Infof("create newContext for functionService %v", ctx.Value(utils.FunctionServiceIDKey{}))

	return ctx
}

// CancelAndCreateNewContext cancel and create new context for functionService
func (p *PodFetcher) CancelAndCreateNewContext(functionServiceID string) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	ctx = context.WithValue(ctx, utils.FunctionServiceIDKey{},
		functionServiceID+commonutils.LayerListSep+time.Now().String())
	p.contextsMutex.Lock()
	if ctxOld, exist := p.contexts[functionServiceID]; exist {
		ctxOld.cancelFunc()
		log.GetLogger().Warnf("cancel and create newContext for functionService %v",
			ctxOld.ctx.Value(utils.FunctionServiceIDKey{}))
	}
	p.contexts[functionServiceID] = &Ctx{
		ctx:        ctx,
		cancelFunc: cancel,
	}
	p.contextsMutex.Unlock()
	log.GetLogger().Infof("create newContext for functionService %v", ctx.Value(utils.FunctionServiceIDKey{}))

	return ctx
}

// CancelAndDeleteContext cancel and delete context for functionService
func (p *PodFetcher) CancelAndDeleteContext(functionServiceID string) {
	p.contextsMutex.Lock()
	if ctxOld, exist := p.contexts[functionServiceID]; exist {
		delete(p.contexts, functionServiceID)
		p.contextsMutex.Unlock()
		ctxOld.cancelFunc()
		log.GetLogger().Warnf("cancel and delete context for functionService %v",
			ctxOld.ctx.Value(utils.FunctionServiceIDKey{}))
		return
	}
	p.contextsMutex.Unlock()
}

// DeleteCAEBusInstanceMutexes delete cae bus instance mutexes
func (p *PodFetcher) DeleteCAEBusInstanceMutexes(functionServiceID string) {
	p.instanceMutex.Lock()
	if _, exist := p.caeBusInstanceMutexes[functionServiceID]; exist {
		delete(p.caeBusInstanceMutexes, functionServiceID)
		log.GetLogger().Infof("delete cae bus instance mutexes for functionService %s", functionServiceID)
	}
	p.instanceMutex.Unlock()
}

// Run for async process cr event
func (p *PodFetcher) Run() {
	defer p.Queue.ShutDown()
	wait.Until(p.processItem, time.Second, p.stopCh)
}

func (p *PodFetcher) processItem() {
	log.GetLogger().Infof("podFetcher starts to process item")
	for p.processNextItem() {
		// continue looping
	}
}

func (p *PodFetcher) processNextItem() bool {
	event, quit := p.Queue.Get()
	if quit {
		log.GetLogger().Warnf("the queue of PodFetcher has been closed")
		return false
	}
	defer p.Queue.Done(event)
	ev, ok := event.(CREvent)
	if !ok {
		log.GetLogger().Warnf("failed to converted interface to CREvent")
		return false
	}
	if err := p.processEventWithRetry(p.processCREvent, event); err != nil {
		log.GetLogger().Errorf("failed to create functionService %s and configure generalized pod %s "+
			"with retry, error: %s", ev.functionService.ID, ev.requestInfo.WorkerID, err.Error())
		p.DeletePod(ev.pod, ev.requestInfo.TraceID)
	}
	return true
}

func (p *PodFetcher) processEventWithRetry(f func(event interface{}) error, event interface{}) error {
	if err := f(event); err == nil {
		p.Queue.Forget(event)
		return nil
	} else if p.Queue.NumRequeues(event) < maxRetries {
		p.Queue.AddRateLimited(event)
		return nil
	} else {
		p.Queue.Forget(event)
		return err
	}
}

func (p *PodFetcher) processCREvent(event interface{}) error {
	ev, ok := event.(CREvent)
	if !ok {
		log.GetLogger().Errorf("failed to converted interface to CREvent")
		p.Queue.Forget(event)
		return nil
	}
	if ev.pod != nil {
		if err := p.configureGeneralizedPod(ev); err != nil {
			return err
		}
	}
	return nil
}

func (p *PodFetcher) configureGeneralizedPod(ev CREvent) error {
	err := p.SetLabelsAndOwnerReference(ev.functionService, ev.pod, ev.requestInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to set pod ownerReference worker: %s, pod: %s, traceID: %s ",
			ev.requestInfo.WorkerID, ev.pod.Name, ev.requestInfo.TraceID)
		return err
	}
	log.GetLogger().Debugf("succeed to configure generalizedPod worker: %s, pod: %s, traceID: %s",
		ev.requestInfo.WorkerID, ev.pod.Name, ev.requestInfo.TraceID)
	return nil
}

// GeneralizePodWithRetry gets pod with retry
func (p *PodFetcher) GeneralizePodWithRetry(ctx context.Context, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	var err error
	var pod *corev1.Pod
	if err = wait.ExponentialBackoffWithContext(ctx, generalizePodBackoff, func(context.Context) (bool, error) {
		pod, err = p.GeneralizePod(ctx, requestInfo, functionService)
		if err != nil && (err == utils.ErrReachMaxOnDemandInstanceNumPerTenant ||
			err == utils.ErrReachMaxInstanceNum || err == utils.ErrCancelGeneralizePod) {
			return true, err
		}
		if err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		log.GetLogger().Errorf("failed to generalize pod for %s with retry traceID %s and error %s",
			functionService.ID, requestInfo.TraceID, err.Error())
		return nil, err
	}
	return pod, nil
}

type decorateFunc func(ctx context.Context, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error)

// GetPodFromPrewarmPool -
func (p *PodFetcher) GetPodFromPrewarmPool(funcSvcID string, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) *corev1.Pod {
	chooseStartTime := time.Now()
	pod := cache.WorkerCache.GetPodFromPrewarmPool(funcSvcID)
	if pod != nil {
		snErr := vpa.SendFuncVpaReqToBus(constants.VPACreate, pod, reqInfo, service)
		if snErr != nil {
			log.GetLogger().Errorf("send func: %s vpa req failed, begin to delete pod %s, traceId: %s, "+
				"snCode: %d, error: %s", service.ID, pod.Name, reqInfo.TraceID, snErr.Code(), snErr.Error())
			if deleteErr := p.DeletePodByK8S(pod.Name, reqInfo.TraceID); deleteErr != nil {
				log.GetLogger().Errorf("failed to delete pod %s by k8s with traceID %s and error %s",
					pod.Name, reqInfo.TraceID, deleteErr.Error())
			}
			return nil
		}

		err := p.generalizeRequestToWorker(pod, reqInfo, service, chooseStartTime)
		if err != nil {
			return nil
		}
		log.GetLogger().Infof("get pod from prewarm pool, func: %s, pod name: %s", funcSvcID, pod.Name)
		return pod
	}
	return nil
}

// WithContext decorate f function with context
func (p *PodFetcher) WithContext(f decorateFunc, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	if p == nil {
		return nil, fmt.Errorf("podfetcher is nil")
	}
	if requestInfo.IsNeedDeleteSnapshot {
		ctx, _ := context.WithTimeout(context.Background(), customContainerDefaultWaitReadyTimeout)
		return f(ctx, requestInfo, functionService)
	}
	p.contextsMutex.Lock()
	if ctx, exist := p.contexts[functionService.ID]; exist {
		p.contextsMutex.Unlock()
		return f(ctx.ctx, requestInfo, functionService)
	}
	p.contextsMutex.Unlock()
	log.GetLogger().Errorf("failed to decorate function with context %s with traceID: %s",
		functionService.ID, requestInfo.TraceID)
	return nil, snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
}

func (p *PodFetcher) refreshDynamicResources(requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) {
	if requestInfo.StateID != "" && requestInfo.CreateOptions != nil {
		var cpu, mem int64
		if utils.IsVpaFeatureEnabled() {
			cpu, mem = utils.GetCreateOptionResource(requestInfo.CreateOptions.CPU, requestInfo.CreateOptions.Memory)
		} else {
			cpu, mem = p.PoolMgr.GetStatefulDynamicClosestStandard(requestInfo.CreateOptions.CPU,
				requestInfo.CreateOptions.Memory)
		}
		log.GetLogger().Infof("Get stateful dynamic standard is %d-%d with traceID: %s, stateID: %s, "+
			"createOptions: %+v", cpu, mem, requestInfo.TraceID, requestInfo.StateID, requestInfo.CreateOptions)
		requestInfo.CPU = cpu
		requestInfo.Memory = mem
	}
	if requestInfo.CPU == 0 && requestInfo.Memory == 0 {
		requestInfo.CPU = functionService.Spec.Configuration.Resource.CPU
		requestInfo.Memory = functionService.Spec.Configuration.Resource.Mem
		log.GetLogger().Infof("request cpu and memory both 0, use configuration resource: %d-%d, traceID: %s, stateID: %s",
			requestInfo.CPU, requestInfo.Memory, requestInfo.TraceID, requestInfo.StateID)
	}
}

func statefulFilter(requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	if requestInfo.StateID == "" {
		return nil, nil
	}
	if !cache.IsCurrentClustersStatusRunning() && requestInfo.IsReload {
		return nil, fmt.Errorf("cluster status is not running")
	}
	stateMetaInfo, ok := cache.StateWorkersList.Load(requestInfo.StateID)
	if !ok {
		existLocal, err := cache.UpdateStateWorkerList(requestInfo.StateID)
		if err != nil {
			log.GetLogger().Warnf("UpdateStateWorkerList failed, stateID: %s, error is %s", requestInfo.StateID, err.Error())
			return nil, snerror.New(snerror.StatefulFunctionReloading, snerror.ErrText(snerror.StatefulFunctionReloading))
		}
		if !existLocal {
			return nil, snerror.New(snerror.StatefulFunctionReloading, snerror.ErrText(snerror.StatefulFunctionReloading))
		}
	}
	if requestInfo.CreateOptions == nil {
		cpu, memory := cache.GetCPUMemoryOfStateRouters(stateMetaInfo)
		requestInfo.CreateOptions = &commontypes.StateCreateOptions{
			CPU:    cpu,
			Memory: memory,
		}
	}
	// if requestInfo.Creator is not exist for stateful function, set Creator from cache
	if requestInfo.Creator == "" {
		requestInfo.Creator = getCreatorFromStateMetaInfo(stateMetaInfo)
	}
	if !cache.WorkerCache.CheckAndInsertStateIDMaxInstance(functionService.ID, requestInfo.StateID) {
		if cache.WorkerCache.CheckStatefulReloading(functionService.ID, requestInfo.StateID) {
			log.GetLogger().Warnf("instance: %s with StateID: %s is reloading", functionService.ID, requestInfo.StateID)
			return nil, snerror.New(snerror.StatefulFunctionReloading, snerror.ErrText(snerror.StatefulFunctionReloading))
		}
		pod := cache.WorkerCache.GetExistedStatePod(functionService.ID, requestInfo.StateID)
		if pod != nil {
			log.GetLogger().Infof("instance: %s with StateID: %s has been existed, try to build pod return",
				functionService.ID, requestInfo.StateID)
			return pod, nil
		}
		return nil, fmt.Errorf("instance: %s with StateID: %s is not exist and not reloading",
			functionService.ID, requestInfo.StateID)
	}
	return nil, nil
}

func getCreatorFromStateMetaInfo(value interface{}) string {
	stateMeta, ok := value.(cache.StateMetadata)
	if !ok {
		return ""
	}
	return stateMeta.Creator
}

func updateSnapshotCriuOpts(functionMeta *faasfunction.FunctionService) {
	cfg, err := config.GetConfig()
	if err != nil {
		return
	}
	if functionMeta != nil {
		functionMeta.Spec.SnapshotDeployment.SnapshotCriuOpts = cfg.PoolMgrConfig.CheckpointPodConfig.CRIUCheckpointOpts
	}
}

func updateRequestPoolResource(requestInfo *config.RequestInfo, businessType string) {
	resource, err := utils.GetPoolResource(requestInfo.CPU, requestInfo.Memory, businessType)
	if err != nil {
		log.GetLogger().Warnf("failed to get pool resource, traceID: %s, cpu: %d, mem: %d",
			requestInfo.TraceID, requestInfo.CPU, requestInfo.Memory)
		return
	}
	requestInfo.PoolResource = commonvpa.PodResourceInfo{
		Worker: commonvpa.ResourceConfig{
			CPURequest:    resource.Worker.RequestCPU,
			CPULimit:      resource.Worker.LimitCPU,
			MemoryRequest: resource.Worker.RequestMem,
			MemoryLimit:   resource.Worker.LimitMem,
		},
		Runtime: commonvpa.ResourceConfig{
			CPURequest:    resource.Runtime.RequestCPU,
			CPULimit:      resource.Runtime.LimitCPU,
			MemoryRequest: resource.Runtime.RequestMem,
			MemoryLimit:   resource.Runtime.LimitMem,
		},
	}
}

func updatePodViewResource(requestInfo *config.RequestInfo, pod *corev1.Pod) {
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == utils.RuntimeContainerName {
			requestInfo.ViewResource.Runtime = getResourceConfigAsInt64(pod.Spec.Containers[i].Resources)
		} else if pod.Spec.Containers[i].Name == utils.WorkerContainerName {
			requestInfo.ViewResource.Worker = getResourceConfigAsInt64(pod.Spec.Containers[i].Resources)
		}
	}
}

func getResourceConfigAsInt64(resource corev1.ResourceRequirements) commonvpa.ResourceConfig {
	return commonvpa.ResourceConfig{
		CPULimit:      resource.Limits.Cpu().MilliValue(),
		CPURequest:    resource.Requests.Cpu().MilliValue(),
		MemoryLimit:   resource.Limits.Memory().Value(),
		MemoryRequest: resource.Requests.Memory().Value(),
	}
}

// GeneralizePod generalize pod
func (p *PodFetcher) GeneralizePod(ctx context.Context, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	var pod *corev1.Pod
	var err error
	startTime := time.Now()
	updateSnapshotCriuOpts(functionService)
	// If migrate, we should pull up a new instance of the stateID
	if !requestInfo.IsMigrating {
		pod, err = statefulFilter(requestInfo, functionService)
		if err != nil {
			return nil, err
		}
		if pod != nil {
			return pod, nil
		}
	}
	err = checkFunctionCipherEffectivity(requestInfo, functionService)
	if err != nil {
		return nil, err
	}
	p.refreshDynamicResources(requestInfo, functionService)
	updateRequestPoolResource(requestInfo, functionService.Spec.Configuration.BusinessType)
	if strings.Contains(requestInfo.Applier, functionTaskApplyPrefix) &&
		commonutils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) {
		funcSvcID := utils.CrName(requestInfo.BusinessID, requestInfo.TenantID,
			requestInfo.FunctionName, requestInfo.FunctionVersion)
		mux := p.GetCAEBusInstanceMutexes(funcSvcID)
		if mux != nil {
			mux.Lock()
			defer mux.Unlock()
			if pod = cache.WorkerCache.GetCAEBusInstance(funcSvcID); pod != nil {
				log.GetLogger().Infof("succeed to generalized pod %s for cae func %s, traceID: %s",
					pod.Name, requestInfo.FunctionName, requestInfo.TraceID)
				return pod, nil
			}
		}
	}
	if requestInfo.IsReload && requestInfo.StateID != "" && !checkIsNeedGeneralizeReloadStatePod(requestInfo) {
		log.GetLogger().Errorf("no need to generalize stateful func %s pod, "+
			"the state route of stateID %s is already deleted, traceID: %s",
			requestInfo.FunctionName, requestInfo.StateID, requestInfo.TraceID)
		return nil, errors.New("state router of the stateID was already deleted, no need to generalized it")
	}
	podPlaceholder := "pod-placeholder-" + uuid.New().String()
	if err = reserveClusterBridgeInstance(functionService, podPlaceholder); err != nil {
		resetStateIDMap(requestInfo, functionService)
		errMsg := fmt.Sprintf("reserveClusterBridgeInstance of %s failed:%s",
			functionService.Spec.Configuration.FunctionVersionURN, err.Error())
		log.GetLogger().Errorf(errMsg)
		return nil, errors.New(errMsg)
	}
	if commonutils.GetEnableHCS() {
		if err = recordCPUUsage(requestInfo); err != nil {
			return nil, err
		}
		defer func() {
			releaseCPUResource(err, pod, requestInfo.CPU)
		}()
	}
	pod, err = p.generalizePodWithMaxInstance(ctx, requestInfo, functionService)
	if err != nil && requestInfo.StateID != "" && !commonutils.IsUserError(err) {
		log.GetLogger().Errorf("stateful function generalized error, begin retry. funcName: %s, stateID: %s, "+
			"traceID: %s, error: %s", requestInfo.FunctionName, requestInfo.StateID, requestInfo.TraceID, err.Error())
		pod, err = p.retryForStatefulFunc(ctx, requestInfo, functionService)
	}
	if err != nil {
		processGeneralizePodErr(requestInfo, functionService, podPlaceholder)
		return nil, err
	}
	if err := utils.UpdateClusterBridgeInstance(functionService.Spec.Configuration.IsBridgeFunction,
		functionService.Spec.Configuration.FunctionVersionURN, podPlaceholder, pod.Name); err != nil {
		log.GetLogger().Warnf("failed to updateClusterBridgeInstance for %s:%s",
			functionService.Spec.Configuration.FunctionVersionURN, err.Error())
	}
	updatePodViewResource(requestInfo, pod)
	requestInfo.CreationTime = time.Now().Unix()
	workerIns := utils.MakeWorkerInsWithRequest(pod, requestInfo, utils.GetCRSignature(functionService),
		functionService.Spec.Configuration.BusinessType)
	if requestInfo.StateID != "" {
		if !p.needRefreshStateRouterWhenReloadStatefulInstance(requestInfo, pod.Name) {
			return nil, fmt.Errorf("no need to generate stateful pod of stateID: %s", requestInfo.StateID)
		}
		workerIns.Creator = requestInfo.Creator
		etcdhandler.PutSNStateWorkerFromETCD(requestInfo, workerIns)
		workerIns.OwnerIP = pod.Status.HostIP
	}
	actions, metadata := cache.WorkerCache.HandleInstance(functionService.ID,
		cache.NewInstanceMeta(workerIns, eventsource.Added))
	funcCtx := p.CreateNewContext(functionService.ID)
	workerIns.IsDirectFunc = commonutils.IsDirectFunc(pod, functionService.Spec.Configuration.Runtime.Language)
	workerIns.HasInitializer = len(functionService.Spec.Configuration.Initializer.Handler) != 0
	workerIns.EnableWebsocket = functionService.Spec.Configuration.EnableWebsocket
	cache.WorkerCache.DeleteReloadDoneState(functionService.ID, requestInfo.StateID)
	etcdhandler.PutInstanceToETCD(funcCtx, workerIns)
	// send 2 pat pods when worker has only 1 pat pod and instance pod has store in worker cache
	p.handleVpcHook(pod, requestInfo, functionService)
	p.DoActions(functionService.ID, actions, metadata)

	totalDuration := time.Now().Sub(startTime).Seconds()
	instanceNum, scalingNum := cache.WorkerCache.GetInstanceNum(functionService.ID)

	m := monitoring.ColdStartMetrics{Delay: totalDuration}
	monitoring.PublishColdStartMetrics(m)
	log.GetLogger().Infof("%s | %s | %s | %f | %s | %s |"+
		"currentNum: %d, scalingNum: %d | deployNode: %s | podName: %s | isReload %t | succeed to generalized pod",
		requestInfo.FunctionName, requestInfo.TenantID, requestInfo.TraceID, totalDuration, requestInfo.Duration,
		requestInfo.StateID, instanceNum, scalingNum, pod.Spec.NodeName, pod.Name, requestInfo.IsReload)

	go func() {
		snErr := vpa.SendFuncVpaReqToBus(constants.VPAReady, pod, requestInfo, functionService)
		if snErr != nil {
			log.GetLogger().Errorf("send func: %s vpa req failed, pod name: %s, traceId: %s, snCode: %d,"+
				" error: %s", functionService.ID, pod.Name, requestInfo.TraceID, snErr.Code(), snErr.Error())
		}
	}()

	crEvent := CREvent{
		pod:             pod,
		requestInfo:     requestInfo,
		functionService: functionService,
	}
	p.Queue.AddRateLimited(crEvent)
	return pod, err
}

func releaseCPUResource(err error, pod *corev1.Pod, cpu int64) {
	if err != nil || pod == nil {
		utils.ReleaseCPUResource(cpu)
	}
}

func recordCPUUsage(requestInfo *config.RequestInfo) error {
	if !config.EnableHCSLicense {
		return nil
	}
	config.CPUMu.Lock()
	defer config.CPUMu.Unlock()
	if requestInfo.CPU+config.CurrentCPUUsage > config.CPUQuota {
		log.GetLogger().Warnf("exceeds the CPU license threshold, please expand the hcs license specifications."+
			"current CPU quota: %d , current CPU usage: %d, request cpu: %d", config.CPUQuota, config.CurrentCPUUsage,
			requestInfo.CPU)
		return utils.ErrInsufficientLicense
	}
	config.CurrentCPUUsage = requestInfo.CPU + config.CurrentCPUUsage
	log.GetLogger().Infof("currentCPUUsage:%d , request cpu: %d", config.CurrentCPUUsage, requestInfo.CPU)
	return nil
}

func checkFunctionCipherEffectivity(req *config.RequestInfo, fs *faasfunction.FunctionService) error {
	envKmsKeyID := fs.Spec.Configuration.EnvCipherKeyID
	codeKmsKeyID := fs.Spec.Code.Location.Nsp.CodeCipherKeyID
	if len(envKmsKeyID) == 0 && len(codeKmsKeyID) == 0 {
		return nil
	}
	var envEncryptKmsCipher string
	for _, value := range fs.Spec.Configuration.Env {
		if value.Name == utils.EnvEncryptKmsCipher {
			envEncryptKmsCipher = value.Value
		}
	}
	envErrMsg := checkCipherUseful(req, fs, utils.CheckKmsCipherInfo{
		DecType:       utils.EnvDecType,
		TenantID:      req.TenantID,
		KmsKeyID:      envKmsKeyID,
		EncryptCipher: envEncryptKmsCipher,
	})

	codeEncryptKmsCipher := fs.Spec.Code.Location.Nsp.CodeEncryptKmsCipher
	codeErrMsg := checkCipherUseful(req, fs, utils.CheckKmsCipherInfo{
		DecType:       utils.CodeDecType,
		TenantID:      req.TenantID,
		KmsKeyID:      codeKmsKeyID,
		EncryptCipher: codeEncryptKmsCipher,
	})

	if len(envErrMsg) > 0 || len(codeErrMsg) > 0 {
		return errors.New(envErrMsg + codeErrMsg)
	}
	return nil
}

func checkCipherUseful(requestInfo *config.RequestInfo, functionService *faasfunction.FunctionService,
	info utils.CheckKmsCipherInfo) string {
	if info.KmsKeyID == "" {
		return ""
	}
	info.CipherKey = strings.Join([]string{requestInfo.TenantID, info.DecType, info.KmsKeyID}, utils.CipherCacheGap)
	cipherCache, ok := utils.GetUserCipherCache(info.CipherKey)
	if !ok {
		err := utils.CheckAndStoreCipherCache(functionService, info)
		if err != nil {
			log.GetLogger().Errorf("user's env cipher is invalids, err: %s, tenant: %s, function: %s", err.Error(),
				requestInfo.TenantID, requestInfo.FunctionName)
			return fmt.Sprintf("user's %s cipher is invalids ", info.DecType)
		}
	}
	if !utils.CheckUserCipherUsefulAndUpdateLastUseTime(info.CipherKey, cipherCache) {
		log.GetLogger().Errorf("user's env cipher is unuseful, tenant: %s, function: %s",
			requestInfo.TenantID, requestInfo.FunctionName)
		return fmt.Sprintf("user's %s cipher is unuseful ", info.DecType)
	}
	return ""
}

func processGeneralizePodErr(
	requestInfo *config.RequestInfo, functionService *faasfunction.FunctionService, podPlaceholder string) {
	// only stateful function need reset
	resetStateIDMap(requestInfo, functionService)
	if err := utils.UpdateClusterBridgeInstance(functionService.Spec.Configuration.IsBridgeFunction,
		functionService.Spec.Configuration.FunctionVersionURN, podPlaceholder, ""); err != nil {
		log.GetLogger().Warnf("failed to updateClusterBridgeInstance for %s:%s",
			functionService.Spec.Configuration.FunctionVersionURN, err.Error())
	}
}

func resetStateIDMap(requestInfo *config.RequestInfo, functionService *faasfunction.FunctionService) {
	if requestInfo.StateID != "" {
		cache.WorkerCache.ResetStateIDMap(functionService.ID, requestInfo.StateID)
		log.GetLogger().Infof("reset stateID map, funcName: %s, stateID: %s, traceID: %s",
			requestInfo.FunctionName, requestInfo.StateID, requestInfo.TraceID)
		if needRefreshStateRouterNodeID(requestInfo.StateID) {
			log.GetLogger().Infof("needRefreshStateRouterNodeID for stateID %s", requestInfo.StateID)
			refreshStateRouterNodeID(requestInfo.StateID)
		}
	}
}

func reserveClusterBridgeInstance(functionService *faasfunction.FunctionService, podPlaceholder string) error {
	if !functionService.Spec.Configuration.IsBridgeFunction {
		return nil
	}
	if redisclient.GetRedisCmd() == nil {
		return fmt.Errorf("redis client is nil")
	}
	funcURN := functionService.Spec.Configuration.FunctionVersionURN
	ok := redisclient.GetRedisCmd().GetDistributedLockWithRetryTimeout("lock_"+funcURN, "locked",
		utils.DistributeLockTimeout, utils.GetDistributeLockRetryTimes)
	if !ok {
		return fmt.Errorf("get distribute lock failed")
	}
	defer redisclient.GetRedisCmd().ReleaseDistributedLockWithTimeout("lock_" + funcURN)
	result, err := redisclient.GetRedisCmd().GetWithTimeout("cluster_bridge_instances_" + funcURN).Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("get cluster bridge instances failed:%s", err.Error())
	}
	clusterBridgeInstances := &commontypes.ClusterBridgeInstance{}
	if err = json.Unmarshal([]byte(result), clusterBridgeInstances); err != nil {
		log.GetLogger().Warnf("unmarshal ClusterBridgeInstance for %s failed, create it", funcURN)
		clusterBridgeInstances.FunctionURN = funcURN
		if err := redisclient.GetRedisCmd().SAddWithTimeout(utils.AllFuncURNKey, funcURN).Err(); err != nil {
			log.GetLogger().Warnf("add %s to cluster_bridge_instances_all_func_urn_key failed:%s",
				funcURN, err.Error())
		}
	}
	if clusterBridgeInstances.BridgeInstances == nil {
		clusterBridgeInstances.BridgeInstances = make(map[string]map[string]bool)
	}
	total := calTotalIns(clusterBridgeInstances)
	maxIns := getMaxIns(functionService, funcURN)
	if total >= maxIns {
		return fmt.Errorf("bridge instance has reached max %d", maxIns)
	}
	if clusterBridgeInstances.BridgeInstances[clusterID] == nil {
		clusterBridgeInstances.BridgeInstances[clusterID] = make(map[string]bool)
	}
	clusterBridgeInstances.BridgeInstances[clusterID][podPlaceholder] = true
	jsonBytes, err := json.Marshal(clusterBridgeInstances)
	if err != nil {
		return fmt.Errorf("update cluster bridge instances failed:%s", err.Error())
	}
	err = redisclient.GetRedisCmd().SetWithTimeout("cluster_bridge_instances_"+funcURN, jsonBytes, 0).Err()
	if err != nil {
		return fmt.Errorf("update cluster bridge instances failed:%s", err.Error())
	}
	log.GetLogger().Infof("reserveClusterBridgeInstance for %s success, podPlaceholder:%s",
		funcURN, podPlaceholder)
	return nil
}

func calTotalIns(clusterBridgeInstances *commontypes.ClusterBridgeInstance) int {
	var total int
	for _, clusterInstances := range clusterBridgeInstances.BridgeInstances {
		total += len(clusterInstances)
	}
	return total
}

func getMaxIns(functionService *faasfunction.FunctionService, funcURN string) int {
	maxActive := functionService.Spec.Configuration.MaxActive
	if maxActive <= 0 {
		log.GetLogger().Warnf("%s MaxActive is invalid, use default value %d",
			funcURN, utils.DefaultMaxClientForSingleBridgeFunc)
		maxActive = utils.DefaultMaxClientForSingleBridgeFunc
	}
	maxIns := functionService.Spec.Configuration.MaxBaaSConnection / maxActive
	if clusterstatusutil.SomeClusterIsFlowing() {
		spillMaxIns := int(math.Floor(float64(maxIns) * bridgeMaxInsRatio))
		if spillMaxIns == maxIns {
			spillMaxIns++
		}
		log.GetLogger().Infof("some cluster is flowing, spilled maxIns is %d for %s", spillMaxIns, funcURN)
		return spillMaxIns
	}
	return maxIns
}

func (p *PodFetcher) needRefreshStateRouterWhenReloadStatefulInstance(
	requestInfo *config.RequestInfo, podName string) bool {
	funcSvcID := utils.CrName(requestInfo.BusinessID, requestInfo.TenantID,
		requestInfo.FunctionName, requestInfo.FunctionVersion)
	if result, ok := cache.WorkerCache.CheckIsOnlyPodInMap(funcSvcID, podName, requestInfo.StateID); !ok {
		log.GetLogger().Infof("Pod %s already bind with stateID: %s, delete pod: %s",
			result, requestInfo.StateID, podName)
		// delete redundant pod
		err := p.DeletePodByK8S(podName, requestInfo.TraceID)
		if err != nil {
			log.GetLogger().Warnf("DeletePodByK8S when pod redundant err, pod: %s, traceID: %s, stateID: %s, err: %s",
				podName, requestInfo.TraceID, requestInfo.StateID, err.Error())
		}
		cache.WorkerCache.DeleteReloadDoneState(funcSvcID, requestInfo.StateID)
		return false
	}
	cache.WorkerCache.InsertHashKeyInMap(funcSvcID, podName, requestInfo.StateID)
	if !checkIsNeedGeneralizeReloadStatePod(requestInfo) {
		log.GetLogger().Infof("stateID %s not exit in ETCD, no need to generate, pod %s, traceID %s",
			requestInfo.StateID, podName, requestInfo.TraceID)
		cache.WorkerCache.RemoveStateIDFromFuncCache(funcSvcID, requestInfo.StateID)
		cache.WorkerCache.DeleteReloadDoneState(funcSvcID, requestInfo.StateID)
		cache.WorkerCache.DelHashKeyInMap(funcSvcID, requestInfo.StateID)
		// delete pod
		err := p.DeletePodByK8S(podName, requestInfo.TraceID)
		if err != nil {
			log.GetLogger().Warnf("DeletePodByK8S when state not exist error, pod: %s, traceID: %s, stateID: %s, err: %s",
				podName, requestInfo.TraceID, requestInfo.StateID, err.Error())
		}
		return false
	}
	return true
}

// statefulFilter will return error if the stateID does not belong to this cluster
// we need to make sure that the node belongs to the cluster when stateID migration
func needRefreshStateRouterNodeID(stateID string) bool {
	stateMetaInfo, ok := cache.StateWorkersList.Load(stateID)
	if !ok {
		// no need to refresh if the state route not exist
		log.GetLogger().Warnf("stateID %s not exist, no need to refresh", stateID)
		return false
	}
	nodeID := cache.GetNodeIDOfStateRouters(stateMetaInfo)
	if nodeID != "" {
		nodeInfo := cache.GetNodeInfo(nodeID)
		// nodeInfo is nil when node is in another cluster
		return nodeInfo == nil
	}
	// empty nodeID is invalid
	log.GetLogger().Warnf("nodeID of stateID %s is empty, need to refresh", stateID)
	return true
}

func refreshStateRouterNodeID(stateID string) {
	stateMetaInfoValue, ok := cache.StateWorkersList.Load(stateID)
	if !ok {
		log.GetLogger().Warnf("stateID %s not exist, no need to refresh", stateID)
		return
	}
	stateMetaInfo, err := cache.CheckStateMetaInfo(stateMetaInfoValue)
	if err != nil {
		return
	}
	nodeID := cache.GetRandomNodeFromNodeList()
	if nodeID == "" {
		log.GetLogger().Errorf("can not get nodeID of stateID %s, no need to refresh", stateID)
		return
	}
	log.GetLogger().Infof("refreshStateRouterNodeID stateID %s, nodeID %s", stateID, nodeID)
	requestInfo := &config.RequestInfo{
		StateID: stateID,
		CPU:     stateMetaInfo.CPU,
		Memory:  stateMetaInfo.Memory,
	}
	funcName := fmt.Sprintf("%s@%s", stateMetaInfo.ServiceID, stateMetaInfo.FuncName)
	instance := v1alpha1.WorkerInstance{
		FuncName:   funcName,
		NodeName:   nodeID,
		Creator:    stateMetaInfo.Creator,
		TenantID:   stateMetaInfo.TenantID,
		BusinessID: stateMetaInfo.BusinessID,
		Version:    stateMetaInfo.Version,
		StateID:    stateID,
	}
	etcdhandler.PutSNStateWorkerFromETCD(requestInfo, instance)
}

func checkIsNeedGeneralizeReloadStatePod(requestInfo *config.RequestInfo) bool {
	funcSvcID := utils.CrName(requestInfo.BusinessID, requestInfo.TenantID,
		requestInfo.FunctionName, requestInfo.FunctionVersion)
	return cache.CheckStateIDExistedInCacheAndEtcd(funcSvcID, requestInfo.StateID)
}

func (p *PodFetcher) handleVpcHook(pod *corev1.Pod, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) {
	// no need to acquire second pat because worker has 2 pats now
	if len(reqInfo.PatPodInfo) > 1 || len(reqInfo.EndpointTalentPatPodInfo) > 1 || len(reqInfo.PublicPatPodInfo) > 1 {
		log.GetLogger().Debugf("no need to acquire second pat because worker has 2 pats for %s, %s with traceID %s",
			pod.Name, reqInfo.FunctionName, reqInfo.TraceID)
		return
	}
	// call create vpc resource again when worker has only 1 pat pod
	if err := p.VpcHandler.HandleVpcFunctionInfo(service, reqInfo); err != nil {
		log.GetLogger().Errorf("failed to create vpc resource for %s, %s with traceID %s and error %s",
			pod.Name, reqInfo.FunctionName, reqInfo.TraceID, err.Error())
		return
	}
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PatPodInfo, pod.Name)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.EndpointTalentPatPodInfo, pod.Name)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PublicPatPodInfo, pod.Name)
	if len(reqInfo.PatPodInfo) > 1 || len(reqInfo.EndpointTalentPatPodInfo) > 1 || len(reqInfo.PublicPatPodInfo) > 1 {
		patPodRequestParam := &config.PatPodRequestParam{
			FuncSvcID:          service.ID,
			PodName:            pod.Name,
			Timeout:            utils.SendPatIPTimeout,
			TraceID:            reqInfo.TraceID,
			PatPodInfo:         reqInfo.PatPodInfo,
			EndpointPatPodInfo: reqInfo.EndpointTalentPatPodInfo,
			PublicPatPodInfo:   reqInfo.PublicPatPodInfo,
		}
		// send 2 pat pods to specialized worker
		go p.SendPatPodIPToWorkerByRetry(context.Background(), utils.SendPatIPInterval, patPodRequestParam)
		return
	}
	log.GetLogger().Warnf("no need to send pats to worker because it is also return only 1 pat when "+
		"create vpc resource again for %s and %s with traceID %s",
		pod.Name, reqInfo.FunctionName, reqInfo.TraceID)
}

func checkReachMaxInstance(functionService *faasfunction.FunctionService, requestInfo *config.RequestInfo) error {
	// check tenant and func instance quota
	reachMaxOnDemandInsNum, reachMaxReversedInsNum, reachMaxInsNum :=
		cache.TenantCache.CheckReachedMaxInstance(functionService, requestInfo)
	if reachMaxOnDemandInsNum && requestInfo.Applier != constants.WorkerManagerApplier {
		return utils.ErrReachMaxOnDemandInstanceNumPerTenant
	}
	if reachMaxReversedInsNum && requestInfo.Applier == constants.WorkerManagerApplier {
		log.GetLogger().Warnf("reach max reversed instance num per tenant: %s with traceID: %s",
			urnutils.Anonymize(requestInfo.TenantID), requestInfo.TraceID)
	}
	if reachMaxInsNum {
		return utils.ErrReachMaxInstanceNum
	}
	return nil
}

func (p *PodFetcher) generalizePodWithMaxInstance(ctx context.Context, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	if !requestInfo.IsMigrating {
		signature := utils.GetCRSignature(functionService)
		defer cache.WorkerCache.CheckMaxInstanceCallback(functionService, signature, requestInfo)
		if err := checkReachMaxInstance(functionService, requestInfo); err != nil {
			return nil, err
		}
	}
	funcSvcID := utils.CrName(requestInfo.BusinessID, requestInfo.TenantID,
		requestInfo.FunctionName, requestInfo.FunctionVersion)
	var err error
	var pod *corev1.Pod
	if requestInfo.Applier != constants.WorkerManagerApplier {
		pod = p.GetPodFromPrewarmPool(funcSvcID, requestInfo, functionService)
	}
	if pod == nil {
		pod, err = p.GeneralizePodInternal(ctx, requestInfo, functionService)
		if err != nil {
			log.GetLogger().Warnf("worker %s activate failed because of generalizePod error: %s",
				requestInfo.WorkerID, err.Error())
			cache.WorkerCache.UpdateFuncMinScaleUpdatedTime(err, funcSvcID)
			return nil, err
		}
	}
	select {
	case <-ctx.Done():
		log.GetLogger().Warnf("generalizing pod(%s) for functionService %v canceled, deleting pod now",
			pod.Name, ctx.Value(utils.FunctionServiceIDKey{}))
		p.DeletePod(pod, requestInfo.TraceID)
		if commonutils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) {
			etcdhandler.DeleteInitInstanceFromCAERouterEtcd(context.Background(), pod, &(functionService.Spec))
		}
		return nil, utils.ErrCancelGeneralizePod
	default:
	}
	return pod, nil
}

// ScaleUpPrewarmPods -
func (p *PodFetcher) ScaleUpPrewarmPods(funcSvcID string, scaleUpNum int) {
	log.GetLogger().Infof("begin to scale up prewarm pod, funcSvcID: %s, scaleUp num: %d", funcSvcID, scaleUpNum)
	if !utils.GetSmartWarmerEnable() {
		return
	}
	var functionSvc *faasfunction.FunctionService
	for i := 0; i < retryTime; i++ {
		functionSvc = cache.WorkerCache.GetFuncMeta(funcSvcID)
		if functionSvc != nil {
			break
		}
		time.Sleep(retryInterval)
	}
	if functionSvc == nil {
		log.GetLogger().Errorf("failed to get funcMeta, funcSvcID: %s", funcSvcID)
		return
	}
	requestInfo := utils.PackRequestInfo(functionSvc, constants.PredictionApplier, "prewarm-"+uuid.New().String())
	for i := 0; i < scaleUpNum; i++ {
		go p.scaleUpPrewarmPod(requestInfo, functionSvc, funcSvcID)
	}
	return
}

func (p *PodFetcher) scaleUpPrewarmPod(requestInfo *config.RequestInfo, functionSvc *faasfunction.FunctionService,
	funcSvcID string) {
	cache.WorkerCache.IncreaseScalingPrewarmNum(funcSvcID)
	pod, err := p.GeneralizePodInternalByRetry(requestInfo, functionSvc)
	if err != nil {
		log.GetLogger().Errorf("failed to scale up prewarm instance for funcSvc: %s with error: %s",
			funcSvcID)
		cache.WorkerCache.DecreaseScalingPrewarmNum(funcSvcID)
		return
	}
	cache.WorkerCache.AddPrewarmInstances(funcSvcID, pod)
	crEvent := CREvent{
		pod:             pod,
		requestInfo:     requestInfo,
		functionService: functionSvc,
	}
	p.Queue.AddRateLimited(crEvent)
	cache.WorkerCache.DecreaseScalingPrewarmNum(funcSvcID)
	log.GetLogger().Infof("succeed to scale up prewarm instance with pod name: %s for func: %s",
		pod.Name, functionSvc.Spec.Configuration.FunctionName)
}

// GeneralizePodInternalByRetry -
func (p *PodFetcher) GeneralizePodInternalByRetry(reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (*corev1.Pod, error) {
	var pod *corev1.Pod
	err := wait.ExponentialBackoff(generalizePodBackoff, func() (done bool, err error) {
		pod, err = p.WithContext(p.GeneralizePodInternal, reqInfo, service)
		if err != nil {
			log.GetLogger().Warnf("failed to generalize podInternal, traceID: %s", reqInfo.TraceID)
			// if err != nil, it will return Immediately
			return false, nil
		}
		return true, nil
	})
	return pod, err
}

// ScaleDownPrewarmPod -
func (p *PodFetcher) ScaleDownPrewarmPod(funcSvcID string, pods []*corev1.Pod) {
	log.GetLogger().Infof("begin to scale down prewarm pod, len %d", len(pods))
	for i := 0; i < len(pods); i++ {
		if pods[i] == nil {
			log.GetLogger().Error("nil pod")
			continue
		}
		cache.WorkerCache.RemovePrewarmInstances(funcSvcID, pods[i].Name)
		go p.VpcHandler.IdleDeletePatServiceList(pods[i].Name)
		go p.DeletePodByK8S(pods[i].Name, "terminate-"+uuid.New().String())
	}
}

// GeneralizePodInternal generalize pod
func (p *PodFetcher) GeneralizePodInternal(ctx context.Context, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (*corev1.Pod, error) {
	reqInfo.Duration = ErrorDuration
	// generalize pod
	chooseStartTime := time.Now()
	enableCircuitBreaker := atomic.LoadUint32(&p.PoolMgr.Config.DisableCircuitBreaker) == 0
	if enableCircuitBreaker && !cache.WorkerCache.IsAllowed(service.ID) {
		err := snerror.New(snerror.TooManySpecializedFails, snerror.ErrText(snerror.TooManySpecializedFails))
		log.GetLogger().Errorf("the request is not allowed, %s, %s, %s", service.ID, reqInfo.TraceID, err.Error())
		return nil, err
	}
	pod := &corev1.Pod{}
	var err error
	if utils.IsCustomContainerSpec(&service.Spec) {
		if pod, err = p.generalizeCustomContainerPod(ctx, reqInfo, service); err != nil {
			return nil, err
		}
	} else {
		pod, err = p.getPodFromPoolWithSpan(ctx, reqInfo, service)
		if err != nil {
			return nil, err
		}
		// acquire pat pods from vpc controller
		if err = p.getOrCreatePatPodsWithSpan(ctx, reqInfo, service, pod); err != nil {
			return nil, err
		}
	}

	if reqInfo.SnapshotDeployment != nil && reqInfo.SnapshotDeployment.TaskID != "" {
		taskID := reqInfo.SnapshotDeployment.TaskID
		log.GetLogger().Debugf("save checkpoint podName, taskID: %s, podName: %s.", taskID, pod.Name)
		if cache.GetCheckpointCache().IsTaskCanceled(taskID) {
			log.GetLogger().Infof("checkpoint task canceled before generalizePod, taskID: %s, traceID: %s",
				taskID, reqInfo.TraceID)
			p.deletePodByK8SIgnoreResult(pod.Name, reqInfo.TraceID)
			return nil, errors.New("checkpoint canceled")
		}
		cache.GetCheckpointCache().SetTaskInfoPodName(taskID, pod.Name)
	}

	if snErr := vpa.SendFuncVpaReqToBus(constants.VPACreate, pod, reqInfo, service); snErr != nil {
		log.GetLogger().Errorf("send func: %s vpa req failed, begin to delete pod %s, traceId: %s, "+
			"snCode: %d, error: %s", service.ID, pod.Name, reqInfo.TraceID, snErr.Code(), snErr.Error())
		p.deletePodByK8SIgnoreResult(pod.Name, reqInfo.TraceID)
		return nil, snErr
	}
	defer func() {
		reportGeneralizePodEvent(pod, service, reqInfo.Duration, reqInfo.TraceID, err)
	}()
	err = p.generalizePod(pod, reqInfo, service, chooseStartTime, enableCircuitBreaker)
	return pod, err
}

func (p *PodFetcher) deletePodByK8SIgnoreResult(podName, traceID string) {
	if deleteErr := p.DeletePodByK8S(podName, traceID); deleteErr != nil {
		log.GetLogger().Errorf("failed to delete pod %s by k8s with traceID %s and error %s",
			podName, traceID, deleteErr.Error())
	}
}

func reportGeneralizePodEvent(pod *corev1.Pod, service *faasfunction.FunctionService,
	duration, traceID string, eventErr error) {
	stageInfo := &podevent.EventStageInfo{
		EventStage: podevent.SpecializationStage,
	}
	curPod := pod
	if eventErr != nil {
		stageInfo.EventSummary = podevent.PodSpecializeFailed
		stageInfo.EventType = podevent.EventErrorType
		if service != nil && curPod == nil {
			errStr := strings.Split(eventErr.Error(), utils.PodNameSplit)
			if len(errStr) >= podNameMinLength && strings.HasPrefix(errStr[podNameIndex], "custom") {
				curPod = &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: errStr[podNameIndex],
					},
				}
			}
		}
	} else {
		stageInfo.EventSummary = podevent.PodScaleUp
		stageInfo.EventType = podevent.EventInfoType
		getCostTime, specializeCostTime := getTimesFromCost(duration)
		stageInfo.ErrorMessage = fmt.Sprintf("The pod %s is scaled up, get pod cost %s, "+
			"generalize pod cost %s", pod.Name, getCostTime, specializeCostTime)
	}
	podevent.ReportCaePodEvent(curPod, &service.Spec, stageInfo, traceID, eventErr)
}

func getTimesFromCost(cost string) (string, string) {
	costs := strings.Split(cost, costSeparator)
	if len(costs) >= costLength {
		return strings.TrimPrefix(costs[getPodCostIndex], getPodCostPrefix),
			strings.TrimPrefix(costs[specializePodCostIndex], specializePodCostPrefix)
	}
	return "", ""
}

func getChildSpanContext(ctx context.Context, traceParent string) context.Context {
	carrier := propagation.HeaderCarrier{}
	carrier.Set(constants.HeaderTraceParent, traceParent)
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

func (p *PodFetcher) getPodFromPool(ctx context.Context, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (*corev1.Pod, error) {
	pod, err := p.getPodFromNodeManager(ctx, service, reqInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to get pod from node manager for func: %s with traceID: %s and error: %s",
			service.ID, reqInfo.TraceID, err.Error())
		if _, ok := err.(snerror.SNError); !ok {
			err = snerror.New(snerror.GettingPodErrorCode, err.Error())
			return nil, err
		}
		return nil, err
	}

	return pod, nil
}

func (p *PodFetcher) getPodFromPoolWithSpan(ctx context.Context, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (*corev1.Pod, error) {
	var err error
	var pod *corev1.Pod
	if tracer.EnableCommonTracer() {
		pCtx := getChildSpanContext(ctx, reqInfo.TraceParent)
		tr := otel.Tracer(tracer.GetOtelServiceName())
		_, span := tr.Start(pCtx, "GetPodFromPool", trace.WithSpanKind(trace.SpanKindInternal))
		span.SetAttributes(attribute.Key("traceID").String(reqInfo.TraceID))
		span.SetAttributes(attribute.Key("func.name").String(reqInfo.FunctionName))
		span.SetAttributes(attribute.Key("service.id").String(service.ID))
		span.SetAttributes(attribute.Key("urn").String(reqInfo.URN))
		defer func() {
			if pod != nil {
				span.SetAttributes(attribute.Key("pod.name").String(pod.Name))
			}
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "succeed to get pod from pool")
			}
			span.End()
		}()
	}
	pod, err = p.getPodFromPool(ctx, reqInfo, service)

	return pod, err
}

func (p *PodFetcher) getOrCreatePatPods(ctx context.Context, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService, pod *corev1.Pod) error {
	// acquire pat pods from vpc controller
	if err := p.VpcHandler.HandleVpcFunctionInfo(service, reqInfo); err != nil {
		log.GetLogger().Warnf("failed to handler vpc function info with funcName: %s, traceID: %s, error: %s",
			reqInfo.FunctionName, reqInfo.TraceID, err.Error())
		if deleteErr := p.DeletePodByK8S(pod.Name, reqInfo.TraceID); deleteErr != nil {
			log.GetLogger().Warnf("failed to delete pod %s by k8s with traceID %s and error %s",
				pod.Name, reqInfo.TraceID, deleteErr.Error())
		}
		return dealWithVPCError(err)
	}
	// update cache of pat pod and instance pod
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PatPodInfo, pod.Name)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.EndpointTalentPatPodInfo, pod.Name)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PublicPatPodInfo, pod.Name)
	return nil
}

func (p *PodFetcher) getOrCreatePatPodsWithSpan(ctx context.Context, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService, pod *corev1.Pod) error {
	var err error
	if tracer.EnableCommonTracer() {
		pCtx := getChildSpanContext(ctx, reqInfo.TraceParent)
		_, span := otel.Tracer(tracer.GetOtelServiceName()).Start(pCtx,
			"GetOrCreatePatPods", trace.WithSpanKind(trace.SpanKindInternal))
		span.SetAttributes(attribute.Key("func.name").String(reqInfo.FunctionName))
		span.SetAttributes(attribute.Key("pod.name").String(pod.Name))
		span.SetAttributes(attribute.Key("traceID").String(reqInfo.TraceID))
		defer func() {
			span.SetAttributes(attribute.Key("PatPodInfo").String(getPatInfoStr(reqInfo.PatPodInfo)))
			span.SetAttributes(attribute.Key("EndpointPat").String(getPatInfoStr(reqInfo.EndpointTalentPatPodInfo)))
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "succeed to get or create pat pods")
			}
			span.End()
		}()
	}
	err = p.getOrCreatePatPods(ctx, reqInfo, service, pod)

	return err
}

func getPatInfoStr(patInfo []commontypes.NATConfigure) string {
	if len(patInfo) == 0 {
		return ""
	}
	patInfoStr, err := json.Marshal(patInfo)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal %v with error %s", patInfo, err.Error())
		return ""
	}
	return string(patInfoStr)
}

func dealWithVPCError(originErr error) error {
	var err error
	if originErr.Error() == utils.ErrNoOperationalPermissionsVpc.Error() {
		err = snerror.New(snerror.VpcNoOperationalPermissions, utils.ErrNoOperationalPermissionsVpc.Error())
	} else if originErr.Error() == utils.ErrNoAvailableVpcPatInstance.Error() {
		err = snerror.New(snerror.NotEnoughNIC, snerror.ErrText(snerror.NotEnoughNIC))
	} else if strings.Contains(originErr.Error(), utils.ErrVPCNotFound.Error()) {
		err = snerror.New(snerror.VPCNotFound, snerror.ErrText(snerror.VPCNotFound))
	} else if strings.Contains(originErr.Error(), utils.ErrVPCXRoleNotFound.Error()) {
		err = snerror.New(snerror.VPCXRoleNotFound, snerror.ErrText(snerror.VPCXRoleNotFound))
	} else {
		err = snerror.New(snerror.InternalVPCError, originErr.Error())
	}
	return err
}

func (p *PodFetcher) generalizeCustomContainerPod(ctx context.Context, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (*corev1.Pod, error) {
	if err := p.VpcHandler.HandleVpcFunctionInfo(service, reqInfo); err != nil {
		log.GetLogger().Errorf("failed to handler custom container vpc function info with funcName: %s, "+
			"traceID: %s, error: %s", reqInfo.FunctionName, reqInfo.TraceID, err.Error())
		return nil, dealWithVPCError(err)
	}
	// as custom container pod is not created, add tmp pod into pat service list
	tmpPodName := uuid.New().String()
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PatPodInfo, tmpPodName)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.EndpointTalentPatPodInfo, tmpPodName)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PublicPatPodInfo, tmpPodName)

	pod, err := p.getPodFromNodeManager(ctx, service, reqInfo)
	if err != nil {
		if err == utils.ErrNoNodesCanScheduler {
			log.GetLogger().Errorf("failed to get custom image because: %s", err.Error())
			return nil, err
		}
		log.GetLogger().Errorf("failed to get custom container pod from node manager for func: %s "+
			"with traceID: %s and error: %s", service.ID, reqInfo.TraceID, err.Error())
		// remove tmp pod from pat service list as new custom container pod is created failed
		_ = p.VpcHandler.IdleDeletePatServiceList(tmpPodName)
		if _, ok := err.(snerror.SNError); !ok {
			err = snerror.New(snerror.GettingPodErrorCode, err.Error())
			return nil, err
		}
		return nil, err
	}

	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PatPodInfo, pod.Name)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.EndpointTalentPatPodInfo, pod.Name)
	p.VpcHandler.HandlePatServiceList(reqInfo.TenantID, service.ID, reqInfo.PublicPatPodInfo, pod.Name)
	// remove tmp pod from pat service list as new custom container pod is created
	p.VpcHandler.IdleDeletePatServiceList(tmpPodName)

	return pod, nil
}

func (p *PodFetcher) generalizePod(pod *corev1.Pod, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService, chooseStartTime time.Time, enableCircuitBreaker bool) error {
	// it will send generalized request to worker
	err := p.generalizeRequestToWorker(pod, reqInfo, service, chooseStartTime)
	if err != nil {
		if enableCircuitBreaker && commonutils.IsUserError(err) {
			cache.WorkerCache.Fail(service.ID)
		}
		// delete pat pods when generalized pool pod error
		if deleteErr := p.VpcHandler.IdleDeletePatServiceList(pod.Name); deleteErr != nil {
			log.GetLogger().Warnf("failed to idle delete pat service list for %s with %s", pod.Name, reqInfo.TraceID)
		}
		if commonutils.IsCAEFunc(service.Spec.Configuration.BusinessType) {
			etcdhandler.DeleteInitInstanceFromCAERouterEtcd(context.Background(), pod, &(service.Spec))
		}
		return err
	}
	if enableCircuitBreaker {
		cache.WorkerCache.Succeed(service.ID)
	}
	if utils.IsOnDemandPod(pod) {
		strength.GetPodCache().SetState(pod.Name, service.ID, strength.AfterSpecialized)
	}
	log.GetLogger().Infof("succeed to generalizePod: %s for function: %s, cost: %s, deployNode: %s, traceID: %s",
		pod.Name, reqInfo.FunctionName, reqInfo.Duration, pod.Spec.NodeName, reqInfo.TraceID)

	return nil
}

func (p *PodFetcher) generalizeRequestToWorker(pod *corev1.Pod, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService, chooseStartTime time.Time) error {
	choosePodTime := time.Now().Sub(chooseStartTime)
	workerDuration, err := p.specializeWorkerWithSpan(pod, reqInfo, service)
	if err != nil {
		log.GetLogger().Errorf("failed to generalize pod, delete pod %s for func: %s with traceID: %s and error: %s",
			pod.Name, service.ID, reqInfo.TraceID, err.Error())
		if strings.Contains(err.Error(), "the server closed connection before returning the first response byte") {
			panicWorkerLog := p.getWorkerLog(pod)
			log.GetLogger().Errorf("failed to generalize pod %s with traceID: %s and "+
				"worker log %s", pod.Name, reqInfo.TraceID, panicWorkerLog)
		}
		if deleteErr := p.DeletePodByK8S(pod.Name, reqInfo.TraceID); deleteErr != nil {
			log.GetLogger().Errorf("failed to delete pod %s by k8s with traceID %s and error %s",
				pod.Name, reqInfo.TraceID, deleteErr.Error())
		}
		return err
	}
	reqInfo.Duration = initDuration(chooseStartTime, choosePodTime, workerDuration)
	return nil
}

func (p *PodFetcher) specializeWorkerWithSpan(pod *corev1.Pod, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (string, error) {
	var err error
	var duration string
	if tracer.EnableCommonTracer() {
		childCtx := getChildSpanContext(context.Background(), reqInfo.TraceParent)
		_, span := otel.Tracer(tracer.GetOtelServiceName()).Start(childCtx,
			"SpecializeWorker", trace.WithSpanKind(trace.SpanKindClient))
		span.SetAttributes(attribute.Key("func.name").String(reqInfo.FunctionName))
		span.SetAttributes(attribute.Key("pod.name").String(pod.Name))
		span.SetAttributes(attribute.Key("traceID").String(reqInfo.TraceID))
		defer func() {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			} else {
				span.SetStatus(codes.Ok, "succeed to get pod from pool")
			}
			span.End()
		}()
	}
	duration, err = p.sendGeneralizeRequestToWorker(pod, reqInfo, service)
	return duration, err
}

// RemoveRetryAlarm -
func (p *PodFetcher) RemoveRetryAlarm(instance v1alpha1.WorkerInstance) {
	lens := p.IdleRetryInfo.remove(instance.PodName)
	if lens == 0 {
		log.GetLogger().Infof("clear idle retry alarm, worker: %s", instance.PodName)
		alarm.GetClient().ClearAlarm(alarm.IdleModeNotification)
	}
}

// SendPatPodIPToWorkerByRetry -
func (p *PodFetcher) SendPatPodIPToWorkerByRetry(ctx context.Context, retryPeriod time.Duration,
	param *config.PatPodRequestParam) {
	if p.stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil with traceID: %s", param.TraceID)
		return
	}
	retryTimes := 0
	for {
		select {
		case <-ctx.Done():
			// delete cache between pat pod and instance
			p.VpcHandler.DeletePatPods(param.PodName, param.FuncSvcID, param.TraceID)
			log.GetLogger().Warnf("context is canceled for pod: %s with traceID: %s", param.PodName, param.TraceID)
			return
		case <-p.stopCh:
			log.GetLogger().Warnf("stop channel is closed for pod: %s with traceID: %s", param.PodName, param.TraceID)
			return
		default:
			instance, ok := cache.WorkerCache.GetWorkerInsOrPrewarmPod(param.FuncSvcID, param.PodName)
			if !ok {
				// delete cache between pat pod and instance
				p.VpcHandler.DeletePatPods(param.PodName, param.FuncSvcID, param.TraceID)
				log.GetLogger().Errorf("failed to get worker instance by %s, %s and traceID: %s",
					param.FuncSvcID, param.PodName, param.TraceID)
				return
			}
			err := p.sendPatPodIPToWorker(instance, *param)
			if err != nil {
				log.GetLogger().Warnf(
					"failed to send pat ip to worker: %s,node: %s, applier: %s retry times: %d, traceID: %s, error: %s",
					param.PodName, instance.IP, instance.Applier, retryTimes, param.TraceID, err.Error())
				retryTimes++
				time.Sleep(retryPeriod)
				continue
			}
			log.GetLogger().Infof("succeed to send pat ip to worker: %s with retry times: %d and traceID: %s",
				param.PodName, retryTimes, param.TraceID)
			// succeed to return
			return
		}
	}
}

func (p *PodFetcher) sendPatPodIPToWorker(instance v1alpha1.WorkerInstance, param config.PatPodRequestParam) error {
	req, err := initPatPodIPRequest(instance.IP, p.httpsEnable, param)
	defer fhttp.ReleaseRequest(req)
	if err != nil {
		log.GetLogger().Errorf("failed to init pat pod ip request for pod(%s), applier(%s), traceID(%s): %s",
			instance.PodName, instance.Applier, param.TraceID, err.Error())
		return err
	}
	response := fhttp.AcquireResponse()
	defer fhttp.ReleaseResponse(response)

	err = p.workerClient.DoTimeout(req, response, param.Timeout)
	if err != nil {
		log.GetLogger().Errorf("failed to send request to pod(%s) for applier(%s), traceID(%s): %s",
			instance.PodName, instance.Applier, param.TraceID, err.Error())
		return err
	}
	if response.Header.StatusCode() != fhttp.StatusOK {
		snBadResp := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snBadResp)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker with error: %s", err.Error())
			return snerror.NewWithFmtMsg(utils.JSONParseError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
		return snerror.New(snBadResp.Code, snBadResp.Message)
	}

	return nil
}

// SendIdleModeRequestByRetry -
func (p *PodFetcher) SendIdleModeRequestByRetry(ctx context.Context, instance v1alpha1.WorkerInstance, idleMode bool,
	traceID string, retryNum int) {
	for {
		select {
		case <-ctx.Done():
			log.GetLogger().Infof("stop current idle mode retry, instance: %s, traceID: %s",
				instance.PodName, traceID)
			return
		default:
			// check cache is or not exit, return
			if !cache.CheckReserveInstanceExist(instance.PodName) {
				log.GetLogger().Infof("reserve instance %s is not exist, traceID: %s, stop retry",
					instance.PodName, traceID)
				p.RemoveRetryAlarm(instance)
				return
			}
			err := p.SendIdleModeRequestToWorkerInternal(instance, idleMode, traceID, retryNum)
			if err == nil {
				p.RemoveRetryAlarm(instance)
				return
			}
			retryNum++
			time.Sleep(utils.IdleRetryInterval)
			continue
		}
	}
}

// SendIdleModeRequestToWorkerInternal -
func (p *PodFetcher) SendIdleModeRequestToWorkerInternal(instance v1alpha1.WorkerInstance, idleMode bool,
	traceID string, retryNum int) error {
	err := p.sendIdleModeRequestToWorker(instance, idleMode, traceID, utils.SendIdleModeTimeout)
	if err != nil {
		p.IdleRetryInfo.updateRetryNum(instance.PodName, retryNum)
		// the log frequency is controlled to be output every ten times.
		if retryNum%utils.MaxRetryNumAlarm == 0 {
			log.GetLogger().Errorf("failed to send idle mode: %t request to worker: %s, "+
				"traceID: %s, retryNum: %d", idleMode, instance.PodName, traceID, retryNum)
		}
		if retryNum >= utils.MaxRetryNumAlarm {
			errMsg := fmt.Sprintf("error msg=pod name %s failed to send idle mode,", instance.PodName)
			alarm.GetClient().ReportAlarm(alarm.IdleModeNotification, errMsg)
		}
		return err
	}
	log.GetLogger().Infof("succeed to send idle mode: %t request to worker: %s with traceID: %s",
		idleMode, instance.PodName, traceID)
	return nil
}

func (p *PodFetcher) sendIdleModeRequestToWorker(instance v1alpha1.WorkerInstance, idleMode bool,
	traceID string, timeout time.Duration) error {
	req, response, err := initIdleModeRequest(instance.IP, traceID, p.httpsEnable, idleMode)
	if err != nil {
		log.GetLogger().Errorf("failed to init idle mode request for pod(%s), applier(%s), traceID(%s): %s",
			instance.PodName, instance.Applier, traceID, err.Error())
		return err
	}
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)
	err = p.workerClient.DoTimeout(req, response, timeout)
	if err != nil {
		log.GetLogger().Errorf("failed to send request to pod(%s) for applier(%s), traceID(%s): %s",
			instance.PodName, instance.Applier, traceID, err.Error())
		return err
	}
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Errorf("failed to set idle mode to pod(%s), node: %s, applier: %s, respCode: %d, respBody: %s",
			instance.PodName, instance.IP, instance.Applier, response.Header.StatusCode(), string(response.Body()))
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker with error: %s", err.Error())
			return snerror.NewWithFmtMsg(utils.JSONParseError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
		return snerror.New(snErr.Code, snErr.Message)
	}
	return nil
}

// SetBillingSwitchToWorker -
func (p *PodFetcher) SetBillingSwitchToWorker(instance v1alpha1.WorkerInstance, enableBilling bool,
	traceID string) {
	for i := 0; i < maxRetries; i++ {
		if err := p.setBillingSwitchToWorker(instance, enableBilling, traceID, setBillingSwitchTimeout); err == nil {
			break
		}
		if i < maxRetries-1 {
			time.Sleep(setBillingSwitchRetryInterval)
		}
	}
}

func (p *PodFetcher) setBillingSwitchToWorker(instance v1alpha1.WorkerInstance, enableBilling bool,
	traceID string, timeout time.Duration) error {
	req, response, err := initBillingSwitchRequest(instance.IP, traceID, p.httpsEnable, enableBilling)
	if err != nil {
		log.GetLogger().Errorf("failed to set billing switch to %t for pod(%s), applier(%s), traceID(%s): %s",
			enableBilling, instance.PodName, instance.Applier, traceID, err.Error())
		return err
	}
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)
	err = p.workerClient.DoTimeout(req, response, timeout)
	if err != nil {
		log.GetLogger().Errorf("failed to send request to pod(%s) for applier(%s), traceID(%s): %s",
			instance.PodName, instance.Applier, traceID, err.Error())
		return err
	}
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Errorf("failed to set bill to pod(%s), node: %s, applier: %s, respCode: %d, respBody: %s",
			instance.PodName, instance.IP, instance.Applier, response.Header.StatusCode(), string(response.Body()))
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker with error: %s", err.Error())
			return snerror.NewWithFmtMsg(utils.JSONParseError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
		return snerror.New(snErr.Code, snErr.Message)
	}
	log.GetLogger().Infof("succeed to set billing with %t for %s and applier %s with %s",
		enableBilling, instance.PodName, instance.Applier, traceID)
	return nil
}

func checkLogPathSize(functionRequest commontypes.FunctionSpecRequest, reqInfo *config.RequestInfo, podName string) {
	logPathSize := len(functionRequest.Message.Function.LogPolicy.LogPaths)
	if logPathSize > maxLogPaths {
		log.GetLogger().Warnf("log path size is %d, max size is %d, funcName is %s, podName is %s, traceID is %s",
			logPathSize, maxLogPaths, reqInfo.FunctionName, podName, reqInfo.TraceID)
	}
}

func (p *PodFetcher) safetyVpcPodSetting(service *faasfunction.FunctionService) {
	if !service.Spec.Configuration.FuncVpc.IsSafety || cacheManagerClusterIP != "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.TODO(), k8sGetServiceTimeout)
	// cacheManager和workerManager在一个namespace下，所以获取workerManager自身的namespace即可
	cacheManagerService, err := p.KubeClient.CoreV1().Services(os.Getenv(constants.PodNamespaceEnvKey)).Get(ctx,
		"cache-manager", metav1.GetOptions{})
	cancel()
	if err != nil {
		log.GetLogger().Warnf("failed to get cache-manager service cluster ip, err: %s", err.Error())
		return
	}
	log.GetLogger().Infof("success to get cache-manager service cluster ip: %s, safe vpc dns resolve addr: %s",
		cacheManagerService.Spec.ClusterIP, safeVpcDNSAddr)
	cacheManagerClusterIP = cacheManagerService.Spec.ClusterIP
}

func (p *PodFetcher) sendGeneralizeRequestToWorker(pod *corev1.Pod, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService) (string, error) {
	functionRequest, err := initFuncRequest(service, reqInfo)
	// If cae pool pod run out, param pod won't be a pool pod, we need to set UsePoolPod false
	functionRequest.Message.Function.UsePoolPod =
		functionRequest.Message.Function.UsePoolPod && utils.IsCaePoolPod(pod)
	if err != nil {
		return "", fmt.Errorf("failed to init function request for pod(%s), worker(%s) :%s",
			pod.Name, reqInfo.WorkerID, err.Error())
	}
	err = setKmsCipherToFuncRequest(&functionRequest, service)
	if err != nil {
		return "", fmt.Errorf("failed to set KmsCipher to function request for pod(%s), worker(%s) :%s",
			pod.Name, reqInfo.WorkerID, err.Error())
	}
	p.safetyVpcPodSetting(service)
	checkLogPathSize(functionRequest, reqInfo, pod.Name)
	req, response, err := initSpecializeRequest(pod, functionRequest, reqInfo, p.httpsEnable, service)
	if err != nil {
		return "", fmt.Errorf("failed to init specialize request for pod(%s), worker(%s): %s",
			pod.Name, reqInfo.WorkerID, err.Error())
	}
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)
	timeout := p.podGeneralizeTimeout + time.Duration(functionRequest.Message.Function.Timeout)*time.Second
	if functionRequest.Message.Function.Initializer.Timeout > 0 {
		timeout = timeout + time.Duration(functionRequest.Message.Function.Initializer.Timeout)*time.Second
	}
	err = p.workerClient.DoTimeout(req, response, timeout)
	if err != nil {
		return "", fmt.Errorf("failed to send request to node(%s) of pod(%s) ip(%s) "+
			"for worker(%s), err: %s",
			pod.Status.HostIP, pod.Name, pod.Status.PodIP, reqInfo.WorkerID, err.Error())
	}

	snErr := &snerror.BadResponse{}
	if response.Body() != nil && len(response.Body()) != 0 {
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker")
			return "", snerror.NewWithFmtMsg(utils.JSONParseError,
				"failed to unmarshal the response body (%s) from worker", string(response.Body()))
		}
	}
	handleSnErr(snErr, reqInfo, service, pod)
	if response.Header.StatusCode() != fhttp.StatusOK && snErr.Code != snerror.InnerRuntimeInitTimeoutCode {
		return p.handleStatusNotOk(pod, reqInfo, response, snErr)
	}
	if reqInfo.IsReportedRestoreFailAlarm {
		clearRestoreFailAlarmInfo(reqInfo.TraceID)
		alarm.GetClient().ClearAlarm(alarm.CheckpointRestoreFailed)
	}
	alarm.GetClient().ClearAlarm(alarm.OBSConnection)
	if string(response.Header.Peek(constants.HeaderDirectRuntime)) == "true" {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		pod.Labels[utils.LabelDirect] = "true"
	}
	reqInfo.IsPreload = checkPreloadResult(req, response, pod.Status.HostIP, service)
	return string(response.Header.Peek(constants.HeaderLoadDuration)), nil
}

func checkPreloadResult(req *fhttp.Request, response *fhttp.Response,
	hostIP string, service *faasfunction.FunctionService) bool {
	functionIsPreload := false
	layerIsPreload := false
	tenantID := service.Spec.Configuration.TenantID
	functionPreLoadEnable := req.Header.Peek(constants.HeaderPreloadFunctionEnable)
	if string(functionPreLoadEnable) == "true" {
		functionIsPreload = checkFunctionPreloadResult(tenantID, hostIP, response, service)
	}
	layerPreLoadEnable := req.Header.Peek(constants.HeaderPreloadLayerEnable)
	if string(layerPreLoadEnable) == "true" {
		layerIsPreload = checkLayerPreloadResult(tenantID, hostIP, response)
	}
	return functionIsPreload || layerIsPreload
}

func checkFunctionPreloadResult(tenantID string, hostIP string, response *fhttp.Response,
	service *faasfunction.FunctionService) bool {
	isPreloadSuccess := false
	result := response.Header.Peek(constants.HeaderPreloadFunctionResult)
	if string(result) == "" {
		isPreloadSuccess = true
		return isPreloadSuccess
	}
	serviceName, funcName, err := urnutils.GetFunctionNameAndServiceName(service.Spec.Configuration.FunctionName)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal preload result, err %s", err.Error())
		return isPreloadSuccess
	}
	version := service.Spec.Configuration.FunctionVersion
	functionVersionName := commonutils.GetPreloadFunctionVersionName(serviceName, funcName, version)
	go preload.GetClient().ReAddPreloadToOneNode(tenantID, functionVersionName, hostIP, constants.PreloadFunctionType)
	return isPreloadSuccess
}

func checkLayerPreloadResult(tenantID string, hostIP string, response *fhttp.Response) bool {
	isPreloadSuccess := false
	result := response.Header.Peek(constants.HeaderPreloadLayerResult)
	if string(result) == "" {
		isPreloadSuccess = true
		return isPreloadSuccess
	}
	resultArray := commontypes.LayerPreloadResultArray{}
	err := json.Unmarshal(result, &resultArray)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal preload result, err %s", err.Error())
		return isPreloadSuccess
	}
	for _, layerResult := range resultArray.Results {
		log.GetLogger().Warnf("layer preload is not successfully, result: %s", layerResult.Result)
		go preload.GetClient().ReAddPreloadToOneNode(tenantID, layerResult.LayerVersionName,
			hostIP, constants.PreloadLayerType)
	}
	return isPreloadSuccess
}

func handleSnErr(snErr *snerror.BadResponse, reqInfo *config.RequestInfo,
	service *faasfunction.FunctionService, pod *corev1.Pod) {
	if snErr.Code == snerror.RestoreDeadErrorCode {
		handleRestoreDead(reqInfo, service)
		stageInfo := &podevent.EventStageInfo{
			EventStage: podevent.RunningStage,
			// The unified reporting information for the failed restore is "An error occurred, snapshot restore failed"
			ErrorMessage: "An error occurred, " + snerror.ErrEventMessageText(snerror.FunctionRestoreHookFailCode),
			EventSummary: podevent.PodRunningError,
			EventType:    podevent.EventInfoType,
		}
		podevent.ReportCaePodEvent(pod, &service.Spec, stageInfo, reqInfo.TraceID, nil)
	}
	if snErr.Code == snerror.InnerRuntimeInitTimeoutCode {
		log.GetLogger().Infof("init pod %s with traceID %s has some load err, code %d err %s", pod.Name,
			reqInfo.TraceID, snErr.Code, snErr.Message)
		reqInfo.LoadErr = snErr
	}
}

func (p *PodFetcher) handleStatusNotOk(pod *corev1.Pod, reqInfo *config.RequestInfo,
	response *fhttp.Response, snErr *snerror.BadResponse) (string, error) {
	log.GetLogger().Errorf("failed to generalize pod(%s), node: %s, worker: %s, respCode: %d, respBody: %s, traceId: %s",
		pod.Name, pod.Status.HostIP, reqInfo.WorkerID, response.Header.StatusCode(), string(response.Body()), reqInfo.TraceID)
	if snErr.Code == snerror.DownLoadCodeErrorCode {
		alarm.GetClient().ReportAlarm(alarm.OBSConnection,
			fmt.Sprintf("error msg=%s,", snErr.Message))
	}
	return "", snerror.New(snErr.Code, snErr.Message)
}

func handleRestoreDead(reqInfo *config.RequestInfo, service *faasfunction.FunctionService) {
	// check redis restore failNum
	key := service.Spec.Configuration.FunctionVersionURN
	info := getRestoreFailedInfo(key)
	// fix the snapshot failure count issue, the current snapshot recovery failure needs to be included
	// in the count and threshold comparison.
	info.FailedCount = info.FailedCount + 1
	if info.Time == 0 {
		info.Time = time.Now().Unix()
	}
	log.GetLogger().Errorf("func %s snapshot restore failed, count: %d, traceId: %s", key, info.FailedCount,
		reqInfo.TraceID)
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("Get config error!")
		return
	}

	handleRestoreFailedAlarm(reqInfo, service, conf.DisableRestoreFailedAlarm)

	// failed snapshots need to update redis counters before circuit snapshot.
	updateRestoreFailNum(key, info)
	threshold := conf.PoolMgrConfig.CheckpointPodConfig.RestoreFailureThreshold
	if info.FailedCount >= threshold {
		clearRestoreFailAlarmInfo(reqInfo.TraceID)
		alarm.GetClient().ClearAlarm(alarm.CheckpointRestoreFailed)
		reqInfo.IsReportedRestoreFailAlarm = false
		handleCircuitSnapshot(reqInfo.TraceID, service)
		return
	}
}

func handleRestoreFailedAlarm(reqInfo *config.RequestInfo, service *faasfunction.FunctionService,
	disableRestoreFailedAlarm bool) {
	if disableRestoreFailedAlarm {
		return
	}
	if !isRestoreFailAlarmExpired(reqInfo.TraceID) {
		log.GetLogger().Infof("traceID %s | restore alarm is still on", reqInfo.TraceID)
		return
	}
	updateRestoreFailedCollection(reqInfo.TraceID, service)

	funcRestoreFailedSlice, totalCount := getAllRestoreFailedCollection(reqInfo.TraceID)
	if len(funcRestoreFailedSlice) == 0 {
		log.GetLogger().Warnf("traceID %s | failed to get func restore failed info", reqInfo.TraceID)
		return
	}

	tenantIDMap := make(map[string]struct{})
	for _, item := range funcRestoreFailedSlice {
		tenantIDMap[item.tenantID] = struct{}{}
	}
	log.GetLogger().Debugf("traceID %s | total restore failed func info: %v", reqInfo.TraceID, funcRestoreFailedSlice)

	specAddition := fmt.Sprintf("total restore failed: %d, resotre failed func: %d, total tenantID user: %d",
		totalCount, len(funcRestoreFailedSlice), len(tenantIDMap))
	if funcRestoreFailedSlice[0].count >= singleFuncRestoreFailedThreshold ||
		totalCount >= multiFuncRestoreFailedThreshold {
		log.GetLogger().Infof("traceID %s | the most time restore failed func: %v", reqInfo.TraceID,
			funcRestoreFailedSlice[0])
		reportRestoreFailedAlarm(reqInfo, specAddition)
		return
	}
}

func reportRestoreFailedAlarm(reqInfo *config.RequestInfo, specAddition string) {
	reqInfo.IsReportedRestoreFailAlarm = true
	alarm.GetClient().ReportAlarm(alarm.CheckpointRestoreFailed, specAddition)

	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Warnf("traceID %s | redis is not ready when report restore failed alarm", reqInfo.TraceID)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()
	for i := 0; i < redisRetryTime; i++ {
		curTime := time.Now().Unix()
		_, err := redisClient.Set(ctx, constants.RestoreFailedAlarmReported, curTime, restoreFailedAlarmExpireTime).Result()
		if err == nil {
			break
		}

		log.GetLogger().Errorf("traceID %s | failed to get alarm info with err: %s with retry %d times",
			reqInfo.TraceID, err.Error(), i+1)
		time.Sleep(redisRetryInterval)
	}
}

func handleCircuitSnapshot(traceID string, service *faasfunction.FunctionService) {
	snapshotMeta, err := getSnapshotMeta(service)
	if err != nil {
		log.GetLogger().Errorf("failed to get snapshot data: err %s, traceID: %s", err.Error(), traceID)
		return
	}
	etcdKey := etcd3.HandleCellPrefix(snapshotMeta.ETCDKey)
	log.GetLogger().Infof("snapshot key: %s will be circuit breaking, traceID %s", etcdKey, traceID)
	var etcdType = etcd3.Meta
	if commonutils.IsCAEFunc(service.Spec.Configuration.BusinessType) {
		etcdType = etcd3.CAEMeta
	}
	spec, err := handleEtcdSnapshotResult(etcdType, etcdKey, traceID)
	if err != nil {
		log.GetLogger().Errorf("failed to handle snapshot val, traceID: %s, err: %s", traceID, err.Error())
		return
	}
	snapshotMeta.SnapshotResult.SnapshotState = circuitBreaking
	if spec.SnapshotState != circuitBreaking {
		spec.SnapshotState = circuitBreaking
	} else {
		log.GetLogger().Infof("No need to update snapshot state, traceID: %s", traceID)
		return
	}
	value, err := json.Marshal(spec)
	if err != nil {
		log.GetLogger().Errorf("Marshal snapshot info error: %s, traceID: %s", err.Error(), traceID)
		return
	}
	// write default snapshot deployment
	err = utils.SubmitResultByEtcdType(etcdKey, traceID, defaultSubmitResultRetryTimes, value, etcdType)
	if err != nil {
		log.GetLogger().Errorf("%s | failed to update %s snapshot state to circuit breaking and error: %s",
			traceID, etcdKey, err.Error())
	}
}

func handleEtcdSnapshotResult(etcdType etcd3.EtcdType, etcdKey, traceID string) (*commontypes.SnapshotDeploymentSpec,
	error) {
	etcdClient := etcd3.GetETCDClient(etcdType)
	if etcdClient == nil || etcdClient.Client == nil {
		log.GetLogger().Errorf("%s etcd client is nil, traceID: %s", etcdType.String(), traceID)
		return nil, errors.New("etcd client is nil")
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	result, err := etcdClient.GetValues(defaultEtcdCtx, etcdKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get snapshot value, err: %s, traceID: %s", err.Error(), traceID)
		return nil, err
	}
	log.GetLogger().Debugf("get snapshot info key %s, value %s, traceID: %s", etcdKey, result, traceID)
	spec := &commontypes.SnapshotDeploymentSpec{}
	snapshotStr := strings.Join(result, "")
	err = json.Unmarshal([]byte(snapshotStr), spec)
	if err != nil {
		log.GetLogger().Errorf("Unmarshal snapshot info from etcd error: %s, traceID: %s", err.Error(), traceID)
		return nil, err
	}
	return spec, nil
}

func getRestoreFailedInfo(key string) commontypes.FailedInfo {
	info := commontypes.FailedInfo{
		FailedCount: 0,
		Time:        0,
	}

	redisClient := monitoring.GetRedisClient()
	ctx, _ := context.WithTimeout(context.Background(), redisCxtTimeout)
	if redisClient == nil {
		log.GetLogger().Warnf("redis is not ready when upload function instance")
		return info
	}
	result, err := redisClient.HGet(ctx, constants.RestoreFailInfo, key).Result()
	if result == "" {
		return info
	}

	err = json.Unmarshal([]byte(result), &info)
	if err != nil {
		log.GetLogger().Warnf("Unmarshal info error: %s", err.Error())
	}

	return info
}

func getAllRestoreFailedCollection(traceID string) ([]restoreFailedInfo, int64) {
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Warnf("traceID %s | redis is not ready when collect all restore failed count", traceID)
		return nil, 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()
	fields, err := redisClient.HGetAll(ctx, restoreFailedFuncKey).Result()
	if err != nil {
		log.GetLogger().Errorf("traceID %s | failed to get restore failed func times", traceID)
		return nil, 0
	}

	var totalFailedCount int64
	var funcRestoreFailedSlice []restoreFailedInfo
	for field, value := range fields {
		num, err := strconv.ParseInt(value, base, bitSize)
		if err != nil {
			log.GetLogger().Errorf("traceID %s | failed to parse value %s, err: %s", traceID, value, err.Error())
			continue
		}
		service := cache.WorkerCache.GetFuncMeta(field)
		if service == nil {
			continue
		}
		totalFailedCount += num
		funcRestoreFailedSlice = append(funcRestoreFailedSlice, restoreFailedInfo{
			funcSvcID: field,
			tenantID:  service.Spec.Configuration.TenantID,
			count:     num,
		})
	}

	sort.Slice(funcRestoreFailedSlice, func(i, j int) bool {
		return funcRestoreFailedSlice[i].count > funcRestoreFailedSlice[j].count
	})

	return funcRestoreFailedSlice, totalFailedCount
}

func updateRestoreFailedCollection(traceID string, service *faasfunction.FunctionService) {
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Warnf("traceID %s | redis is not ready when update restore faild count", traceID)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()
	funcSvcID := utils.CrName(service.Spec.Configuration.BusinessID, service.Spec.Configuration.TenantID,
		service.Spec.Configuration.FunctionName, service.Spec.Configuration.FunctionVersion)
	var err error
	for i := 0; i < redisRetryTime; i++ {
		err = redisClient.HIncrBy(ctx, restoreFailedFuncKey, funcSvcID, restoreFailedNumStep).Err()
		if err == nil {
			break
		}
		log.GetLogger().Errorf("traceID %s | failed to push key: %s, funcSvcID: %s, err: %s, and try %d times", traceID,
			restoreFailedFuncKey, funcSvcID, err.Error(), i+1)
		time.Sleep(redisRetryInterval)
	}
	for i := 0; i < redisRetryTime; i++ {
		err = redisClient.Expire(ctx, restoreFailedFuncKey, restoreFailedCountExpireTime).Err()
		if err == nil {
			break
		}
		log.GetLogger().Errorf("traceID %s | failed to renew key: %s, funcSvcID: %s, err: %s, and try %d times", traceID,
			restoreFailedFuncKey, funcSvcID, err.Error(), i+1)
		time.Sleep(redisRetryInterval)
	}
}

func updateRestoreFailNum(key string, info commontypes.FailedInfo) {
	var err error
	data, err := json.Marshal(info)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal failedInfo, error:%s", err.Error())
		return
	}
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Warnf("redis is not ready when upload func restore fail num")
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()
	for i := 0; i < redisRetryTime; i++ {
		err = redisClient.HSet(ctx, constants.RestoreFailInfo, key, data).Err()
		if err == nil {
			break
		}
		log.GetLogger().Errorf("failed to push key: %s , with err: %s , and try %d times", key, err.Error(), i+1)
		time.Sleep(redisRetryInterval)
	}
}

func isReportRestoreFailAlarm(traceID string) bool {
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Warnf("traceID %s | redis is not ready when check restore failed alarm is upload", traceID)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()

	_, err := redisClient.Get(ctx, constants.RestoreFailedAlarmReported).Result()
	if err == nil {
		return true
	}
	log.GetLogger().Infof("traceID %s | restore failed alarm is not report or with err: %s", traceID, err.Error())
	return false
}

func isRestoreFailAlarmExpired(traceID string) bool {
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Warnf("traceID %s | redis is not ready when check restore failed alarm expired", traceID)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()
	for i := 0; i < redisRetryTime; i++ {
		_, err := redisClient.Get(ctx, constants.RestoreFailedAlarmReported).Result()
		if err == nil {
			return false
		}
		if errors.Is(err, redis.Nil) {
			return true
		}
		log.GetLogger().Errorf("traceID %s | failed to get alarm info with err: %s, retry %d times", traceID,
			err.Error(), i+1)
		time.Sleep(redisRetryInterval)
		continue
	}
	return false
}

func clearRestoreFailAlarmInfo(traceID string) {
	if !isReportRestoreFailAlarm(traceID) {
		return
	}
	redisClient := monitoring.GetRedisClient()
	if redisClient == nil {
		log.GetLogger().Warnf("traceID %s | redis is not ready when clear restore failed alarm", traceID)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()
	for i := 0; i < redisRetryTime; i++ {
		_, err := redisClient.Del(ctx, restoreFailedFuncKey).Result()
		if err != nil {
			log.GetLogger().Errorf("failed to del restore failed alarm info with err: %s, retry %d times",
				err.Error(), i+1)
			time.Sleep(redisRetryInterval)
			continue
		}
		log.GetLogger().Infof("success to del restore failed count key %s", restoreFailedFuncKey)
		return
	}
	return
}

func (p *PodFetcher) getPodFromNodeManager(ctx context.Context, funSvc *faasfunction.FunctionService,
	req *config.RequestInfo) (*corev1.Pod, error) {
	forceDeploy, err := strconv.ParseBool(req.ForceDeploy)
	if err != nil {
		return nil, fmt.Errorf("failed to parse force deploy label %s", req.ForceDeploy)
	}
	nodeLabel := make([]map[string]string, 0, utils.DefaultSliceSize)
	if len(req.NodeLabel) != 0 {
		err = json.Unmarshal([]byte(req.NodeLabel), &nodeLabel)
		if err != nil {
			return nil, snerror.New(utils.JSONParseError, fmt.Sprintf("failed to parse node label %s", req.NodeLabel))
		}
	}

	if req.CPU == 0 {
		req.CPU = funSvc.Spec.Configuration.Resource.CPU
	}
	if req.Memory == 0 {
		req.Memory = funSvc.Spec.Configuration.Resource.Mem
	}
	podReq := initPodRequest(funSvc, req, nodeLabel, forceDeploy)
	pod, err := p.PoolMgr.GetPod(ctx, &funSvc.Spec, podReq)
	if err != nil {
		return nil, err
	}

	return pod, nil
}

func initPodRequest(funSvc *faasfunction.FunctionService, req *config.RequestInfo,
	nodeLabel []map[string]string, forceDeploy bool) common.PodRequest {
	return common.PodRequest{
		CPU:                      req.CPU,
		Mem:                      req.Memory,
		GpuMemory:                funSvc.Spec.Configuration.Resource.GpuMemory,
		GpuType:                  funSvc.Spec.Configuration.Resource.GpuType,
		FuncName:                 req.FunctionName,
		NodeLabel:                nodeLabel,
		ForceDeploy:              forceDeploy,
		TraceID:                  req.TraceID,
		CurrentWorkerNum:         req.OwnedWorkerView.CurrentWorkerNum,
		ScalingWorkersNum:        req.OwnedWorkerView.ScalingWorkersNum,
		FunSvcID:                 funSvc.ID,
		Applier:                  req.Applier,
		Priority:                 getRequestPriority(req),
		Timestamp:                time.Now().UnixNano(),
		SnapshotDeployment:       req.SnapshotDeployment,
		EnableTMPExpansion:       funSvc.Spec.Configuration.Resource.EnableTmpExpansion,
		PodTemplate:              req.PodTemplate,
		MigratedFrom:             req.MigratedFrom,
		PatPodInfo:               req.PatPodInfo,
		EndpointTalentPatPodInfo: req.EndpointTalentPatPodInfo,
		PublicNetworkPatPodInfo:  req.PublicPatPodInfo,
		RouteInfo: commontypes.RouteInfoData{
			ApigRouteEnable: funSvc.Spec.Configuration.ApigRouteEnable,
			ApigRoutes:      cache.WorkerCache.GetAPIGIP(),
		},
		FuncMountConfig:      getFuncMountConfig(funSvc),
		SvcCidr:              config.GetK8sCidr().SvcCidr,
		IsStatefulFunction:   funSvc.Spec.Configuration.IsStatefulFunction,
		IsBridgeFunction:     funSvc.Spec.Configuration.IsBridgeFunction,
		ClusterID:            req.ClusterID,
		IsNeedDeleteSnapshot: req.IsNeedDeleteSnapshot,
	}
}

// GetPodByName will get pod by name
func (p *PodFetcher) GetPodByName(podName, namespace string) (*corev1.Pod, error) {
	pod, err := p.KubeClient.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return pod, nil
}

// DeletePod define a function to delete a pod form k8s
func (p *PodFetcher) DeletePod(pod *corev1.Pod, traceID string) {
	if pod == nil {
		return
	}
	if pod.DeletionTimestamp != nil {
		log.GetLogger().Warnf("pod %s is deleting from k8s", pod.Name)
		return
	}

	funcSvcID, _ := utils.OwnedByFunctionService(pod)
	p.DeletePodByWorkerInstance(funcSvcID, pod.ObjectMeta.Namespace, traceID, utils.MakeWorkerInsInfo(pod))
}

func isNeedDeletePod(pod *corev1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == utils.WorkerContainerName {
			if status.Ready {
				// wait for worker to finish
				return false
			}
			break
		}
	}
	return true
}

// DeletePodByForce -
func (p *PodFetcher) DeletePodByForce(pod *corev1.Pod) error {
	if !isNeedDeletePod(pod) {
		return nil
	}

	gracePeriodSeconds := int64(0)
	propagationPolicy := metav1.DeletePropagationForeground
	err := p.KubeClient.CoreV1().Pods(utils.FunctionNameSpace()).Delete(context.TODO(), pod.Name,
		metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds, PropagationPolicy: &propagationPolicy})
	if err != nil {
		log.GetLogger().Errorf("failed to delete pod: %s by force with error: %s", pod.Name, err.Error())
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:     utils.FunctionNameSpace(),
			Ctx:           context.TODO(),
			OperationType: utils.DeleteType,
			PodName:       pod.Name,
			OperationOptions: metav1.DeleteOptions{
				GracePeriodSeconds: &gracePeriodSeconds,
				PropagationPolicy:  &propagationPolicy,
			},
		})
		return err
	}
	log.GetLogger().Infof("succeed to delete pod: %s by force for %s", pod.Name, utils.GetFullFuncNameByPod(pod))

	return nil
}

// PrintPodEvents get and print events for pod
func (p *PodFetcher) PrintPodEvents(podName string) {
	events, err := p.KubeClient.CoreV1().Events(utils.FunctionNameSpace()).List(context.Background(), metav1.ListOptions{
		FieldSelector: "involvedObject.name=" + podName,
		TypeMeta:      metav1.TypeMeta{Kind: "Pod"},
	})
	if err != nil {
		log.GetLogger().Errorf("failed to list events for pod: %s, err: %s", podName, err.Error())
		return
	}
	for _, item := range events.Items {
		item := item
		log.GetLogger().Infof("event for pod %s type: %s action: %s reason: %s message: %s count: %d firstTime: "+
			"%s lastTime: %s", podName, item.Type, item.Action, item.Reason, item.Message, item.Count,
			item.FirstTimestamp.String(), item.LastTimestamp.String())
	}
}

// DeletePodByK8S -
func (p *PodFetcher) DeletePodByK8S(podName, traceID string) error {
	deletePolicy := metav1.DeletePropagationForeground
	var err error
	var option metav1.DeleteOptions
	// if deleted pod is bridge function, delete it by force in case k8s pod resource leakage
	if strings.Contains(traceID, "deleteBridgePod-") {
		log.GetLogger().Infof("delete bridge pod %s by force!", podName)
		gracePeriodSeconds := int64(0)
		option = metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds, PropagationPolicy: &deletePolicy}
		err = p.KubeClient.CoreV1().Pods(utils.FunctionNameSpace()).Delete(context.TODO(), podName, option)
	} else {
		option = metav1.DeleteOptions{PropagationPolicy: &deletePolicy}
		err = p.KubeClient.CoreV1().Pods(utils.FunctionNameSpace()).Delete(context.TODO(), podName, option)
	}
	if err != nil && !utils.IsPodNotFound(err) {
		log.GetLogger().Errorf("failed to delete pod '%s' with error: %s, traceID: %s",
			podName, err.Error(), traceID)
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:        utils.FunctionNameSpace(),
			Ctx:              context.TODO(),
			OperationType:    utils.DeleteType,
			PodName:          podName,
			OperationOptions: option,
		})
		return err
	}
	log.GetLogger().Infof("succeed to delete pod %s with traceID %s", podName, traceID)
	return nil
}

// DeletePodByWorkerInstance -
func (p *PodFetcher) DeletePodByWorkerInstance(funcSvcID, namespace, traceID string, instance v1alpha1.WorkerInstance,
) {
	if !commonutils.IsCAEFunc(instance.BusinessType) {
		// delete router in etcd at first, and then delete worker cache, finally delete pod by k8s
		etcdhandler.DeleteInstanceFromETCD(context.Background(), instance)
	}
	// delete cache if pod is a worker instance
	if funcSvcID != "" {
		actions, metadata := cache.WorkerCache.HandleInstance(funcSvcID,
			cache.NewInstanceMeta(instance, eventsource.Deleted))
		p.DoActions(funcSvcID, actions, metadata)
	}

	if err := p.DeletePodByK8S(instance.PodName, traceID); err != nil {
		return
	}

	log.GetLogger().Infof("succeed to delete pod '%s' for %s on node: %s with applier: %s, traceID: %s",
		instance.PodName, funcSvcID, instance.NodeName, instance.Applier, traceID)
}

func (p *PodFetcher) deleteWorkerInsOfStatefulFunc(funcSvcID, traceID string, instance v1alpha1.WorkerInstance) {
	// delete pod first for stateful instance
	if err := p.DeletePodByK8S(instance.PodName, traceID); err != nil {
		log.GetLogger().Errorf("delete by k8s error, funcName: %s, podName: %s, node: %s, traceID: %s, err: %s.",
			funcSvcID, instance.PodName, instance.NodeName, traceID, err.Error())
	}
	// delete router in etcd and handler cache both lead to reload
	etcdhandler.DeleteInstanceFromETCD(context.Background(), instance)
	// delete cache if pod is a worker instance
	if funcSvcID != "" {
		actions, metadata := cache.WorkerCache.HandleInstance(funcSvcID,
			cache.NewInstanceMeta(instance, eventsource.Deleted))
		p.DoActions(funcSvcID, actions, metadata)
	}
	log.GetLogger().Infof("succeed to delete stateful pod '%s' for %s on node: %s with applier: %s, "+
		"stateID: %s, traceID: %s",
		instance.PodName, funcSvcID, instance.NodeName, instance.Applier, instance.StateID, traceID)
}

// CleanPodsInNode clean pods in node
func (p *PodFetcher) CleanPodsInNode(node *corev1.Node, labelSelector, traceID string) error {
	podList, err := p.KubeClient.CoreV1().Pods(utils.FunctionNameSpace()).List(
		context.TODO(), metav1.ListOptions{LabelSelector: labelSelector, FieldSelector: "spec.nodeName=" + node.Name})
	if err != nil {
		log.GetLogger().Errorf("failed to list all pods in node: %s with error: %s", node.Name, err.Error())
		return err
	}
	log.GetLogger().Infof("pod list length is %d in unlabeled node %s", len(podList.Items), node.Name)
	for i := range podList.Items {
		p.DeletePod(&podList.Items[i], traceID)
	}

	return nil
}

// SetLabelsAndOwnerReference define a function set pod label and owner reference
func (p *PodFetcher) SetLabelsAndOwnerReference(functionMeta *faasfunction.FunctionService, workerIns *corev1.Pod,
	requestInfo *config.RequestInfo) error {
	ownerReference := utils.DescribeOwnerReferences()
	patchPodTemplate := buildPatchPodTemplate(functionMeta, workerIns, ownerReference, requestInfo)
	patchLoad := map[string]interface{}{"metadata": patchPodTemplate.ObjectMeta}
	patchLoadBytes, err := json.Marshal(patchLoad)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal patch pod template data %s with error %s",
			patchLoadBytes, err.Error())
		return err
	}
	log.GetLogger().Debugf("set labels and ownerReference patchLoadBytes: %s for functionService %s instance %s",
		patchLoadBytes, functionMeta.ID, workerIns.Name)
	ctx, cancel := context.WithTimeout(context.TODO(), utils.K8sPatchTimeout)
	_, err = p.KubeClient.CoreV1().Pods(workerIns.Namespace).
		Patch(ctx, workerIns.Name, types.MergePatchType, patchLoadBytes, metav1.PatchOptions{})
	cancel()
	if err != nil {
		if k8serror.IsNotFound(err) {
			log.GetLogger().Warnf("set labels and ownerReference to instance %s, which is not found",
				workerIns.Name)
			return nil
		}
		log.GetLogger().Errorf("failed to set labels and ownerReference to instance %s with error %s",
			workerIns.Name, err.Error())
		return err
	}
	patchPodTemplate.Name = workerIns.Name
	return nil
}

// GetPodListByLabel get pod list by label
func (p *PodFetcher) GetPodListByLabel(label map[string]string) ([]*corev1.Pod, error) {
	return p.podLister.List(labels.SelectorFromSet(label))
}

func buildPatchPodLabels(functionMeta *faasfunction.FunctionService, workerIns *corev1.Pod,
	requestInfo *config.RequestInfo) map[string]string {
	patchedLabels := make(map[string]string)
	for k, v := range functionMeta.Labels {
		patchedLabels[k] = v
	}
	funcNamePrefix, serviceID, shortFuncName := utils.SplitFuncName(requestInfo.FunctionName)
	if len(shortFuncName) > utils.MaxLabelValueLength {
		// labels must begin and end with an alphanumeric character, so set first character always X
		shortFuncName = "X" + shortFuncName[len(shortFuncName)-utils.MaxLabelValueLength+1:]
	}
	patchedLabels["specialize"] = "poststart"
	patchedLabels["deployedNode"] = workerIns.Spec.NodeName
	if commonutils.IsDirectFunc(workerIns, functionMeta.Spec.Configuration.Runtime.Language) {
		patchedLabels[utils.LabelDirect] = "true"
	}
	patchedLabels[utils.LabelApplier] = requestInfo.Applier
	patchedLabels[utils.LabelStateID] = requestInfo.StateID
	patchedLabels[utils.LabelBusinessID] = requestInfo.BusinessID
	patchedLabels[utils.LabelTenantID] = requestInfo.TenantID
	patchedLabels[utils.LabelFuncNamePrefix] = funcNamePrefix
	patchedLabels[utils.LabelServiceID] = serviceID
	patchedLabels[utils.LabelShortFuncName] = shortFuncName
	patchedLabels[utils.LabelFunctionVersion] = requestInfo.FunctionVersion
	patchedLabels[utils.LabelFunctionCPU] = strconv.FormatInt(requestInfo.CPU, base)
	patchedLabels[utils.LabelFunctionMemory] = strconv.FormatInt(requestInfo.Memory, base)
	creatorInfo := strings.Split(requestInfo.Creator, ":")
	if len(creatorInfo) == CreatorFuncVersionIndex+1 {
		patchedLabels[utils.LabelCreatorFuncName] = creatorInfo[CreatorFuncNameIndex]
		patchedLabels[utils.LabelCreatorFunctionVersion] = creatorInfo[CreatorFuncVersionIndex]
	} else if len(creatorInfo) == CreatorStateIDIndex+1 {
		patchedLabels[utils.LabelCreatorFuncName] = creatorInfo[CreatorFuncNameIndex]
		patchedLabels[utils.LabelCreatorFunctionVersion] = creatorInfo[CreatorFuncVersionIndex]
		patchedLabels[utils.LabelCreatorStateID] = creatorInfo[CreatorStateIDIndex]
	}

	if functionMeta.Spec.Configuration.IsBridgeFunction {
		patchedLabels[utils.LabelIsBridgeFunction] = utils.TrueString
	}
	if functionMeta.Spec.Configuration.Timeout > utils.GetSyncRequestTimeout() {
		patchedLabels[utils.LabelLongRun] = "true"
	}
	if len(functionMeta.Spec.Configuration.Initializer.Handler) != 0 {
		patchedLabels[utils.LabelHasInitializer] = "true"
	}
	if functionMeta.Spec.Configuration.BusinessType != "" {
		patchedLabels[utils.LabelBusinessType] = functionMeta.Spec.Configuration.BusinessType
	}
	if requestInfo.IsPreload {
		patchedLabels[utils.LabelIsPreload] = "true"
	}

	patchedLabels[utils.LabelCreationTime] = strconv.FormatInt(requestInfo.CreationTime, base)

	return patchedLabels
}

func generateResourceAnnotation(requestInfo *config.RequestInfo) string {
	data, err := json.Marshal(requestInfo.PoolResource)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal pod resource, traceID: %s, err: %s", requestInfo.TraceID,
			err.Error())
		return ""
	}
	return strutil.BytesToString(data)
}

func buildPatchPodTemplate(functionMeta *faasfunction.FunctionService, workerIns *corev1.Pod,
	ownerReference metav1.OwnerReference, requestInfo *config.RequestInfo) *corev1.Pod {
	crSignature := utils.GetCRSignature(functionMeta)
	if crSignature == "" {
		log.GetLogger().Warnf("failed to get crSignature, instance: %s, funcName: %s",
			workerIns.Name, functionMeta.Spec.Configuration.FunctionName)
	}
	patchPodTemplate := &corev1.Pod{}
	if os.Getenv(constants.PodNamespaceEnvKey) != constants.TrustworthinessNameSpace {
		patchPodTemplate.OwnerReferences = append(patchPodTemplate.OwnerReferences, ownerReference)
	}

	ownerIP := utils.GetOwnerIPByRequest(workerIns, requestInfo)
	log.GetLogger().Infof("owner IP for pod %s is %s", workerIns.Name, ownerIP)

	patchPodTemplate.Annotations = map[string]string{
		"crSignature":                crSignature,
		utils.OwnerIPKey:             ownerIP,
		utils.SafeToEvict:            utils.FalseString,
		utils.AnnotationFullFuncName: requestInfo.FunctionName,
		utils.AnnotationResource:     generateResourceAnnotation(requestInfo),
		constants.MinInsNum:          strconv.Itoa(int(functionMeta.Spec.Configuration.AutoScalerConfig.MinimumScale)),
		constants.Properties:         utils.GeneratePropertiesAnnotation(functionMeta.Spec.Configuration.Properties),
	}

	if functionMeta.Labels != nil {
		patchPodTemplate.Labels = buildPatchPodLabels(functionMeta, workerIns, requestInfo)
	}
	return patchPodTemplate
}

// initFuncRequest init function request
func initFuncRequest(service *faasfunction.FunctionService,
	reqInfo *config.RequestInfo) (commontypes.FunctionSpecRequest, error) {
	var functionRequest commontypes.FunctionSpecRequest
	functionRequest.ValidateOnly = false
	if commonutils.IsCAEFunc(service.Spec.Configuration.BusinessType) { // CAE function
		functionRequest.Message.Function.FunctionType = service.Spec.Configuration.CustomContainerConfig.ControlPath
		functionRequest.Message.Function.Language = service.Spec.Configuration.Runtime.Language
		functionRequest.Message.Function.CAECustomS3ConfigInfo = handleCAECustomS3Config(service)
	} else if utils.IsCustomContainerSpec(&service.Spec) { // not CAE but custom image
		functionRequest.Message.Function.FunctionType = service.Spec.Configuration.CustomContainerConfig.ControlPath
		functionRequest.Message.Function.Language = "custom-container"
		functionRequest.Message.Function.CustomContainerPort = service.Spec.Configuration.CustomContainerConfig.Port
	} else { // general runtime
		functionRequest.Message.Function.Language = service.Spec.Configuration.Runtime.Language
	}
	functionRequest.Message.Function.EnableAuthInHeader = service.Spec.Configuration.EnableAuthInHeader
	functionRequest.Message.Function.URN = service.Spec.Configuration.FunctionVersionURN
	functionRequest.Message.Function.Handler = service.Spec.Configuration.Handler.Entry
	functionRequest.Message.Function.EntryFile = service.Spec.Configuration.Handler.Module
	functionRequest.Message.Function.Timeout = service.Spec.Configuration.Timeout
	functionRequest.Message.Function.CodeSha256 = service.Spec.Code.CodeSha256
	functionRequest.Message.Function.CodeSha512 = service.Spec.Code.CodeSha512

	functionRequest.Message.Function.ResourceMetaData.CPU = reqInfo.CPU
	functionRequest.Message.Function.ResourceMetaData.Mem = reqInfo.Memory
	functionRequest.Message.Function.ResourceMetaData.EnableTmpExpansion =
		service.Spec.Configuration.Resource.EnableTmpExpansion
	functionRequest.Message.Function.ResourceMetaData.GpuMemory =
		service.Spec.Configuration.Resource.GpuMemory
	functionRequest.Message.Function.ResourceMetaData.EphemeralStorage =
		service.Spec.Configuration.Resource.EphemeralStorage
	functionRequest.Message.Function.Initializer.Handler = service.Spec.Configuration.Initializer.Handler
	functionRequest.Message.Function.Initializer.Timeout = service.Spec.Configuration.Initializer.Timeout
	functionRequest.Message.Function.Role.XRole = service.Spec.Configuration.Role.XRole
	functionRequest.Message.Function.Role.AppXRole = service.Spec.Configuration.Role.AppXRole
	functionRequest.Message.Function.DomainID = service.Spec.Configuration.DomainID
	functionRequest.Message.Function.LogTankService.StreamID = service.Spec.Configuration.LogTankService.StreamID
	functionRequest.Message.Function.LogTankService.GroupID = service.Spec.Configuration.LogTankService.GroupID
	functionRequest.Message.Function.TraceService.TraceSK = service.Spec.Configuration.TraceService.TraceSK
	functionRequest.Message.Function.TraceService.TraceAK = service.Spec.Configuration.TraceService.TraceAK
	functionRequest.Message.Function.TraceService.Enabled = service.Spec.Configuration.TraceService.Enabled
	functionRequest.Message.Function.TraceService.ProjectName = service.Spec.Configuration.TraceService.ProjectName
	functionRequest.Message.Function.FuncMountConfig = getFuncMountConfig(service)
	functionRequest.Message.Function.MaxStateSize = getMaxStateSize()
	functionRequest.Message.Function.Heartbeat.Handler = service.Spec.Configuration.Heartbeat.Handler
	functionRequest.Message.Function.Heartbeat.Path = service.Spec.Configuration.Heartbeat.Path
	functionRequest.Message.Function.Heartbeat.HealthEnable = service.Spec.Configuration.Heartbeat.HealthEnable
	functionRequest.Message.Function.Heartbeat.Timeout = service.Spec.Configuration.Heartbeat.Timeout
	functionRequest.Message.Function.Heartbeat.FailureThreshold = service.Spec.Configuration.Heartbeat.FailureThreshold
	functionRequest.Message.Function.Heartbeat.PeriodSeconds = service.Spec.Configuration.Heartbeat.PeriodSeconds
	functionRequest.Message.Function.Heartbeat.FirstCheckDelay = service.Spec.Configuration.Heartbeat.FirstCheckDelay
	functionRequest.Message.Function.Heartbeat.SuccessThreshold = service.Spec.Configuration.Heartbeat.SuccessThreshold
	functionRequest.Message.Function.StartupHeartbeat.Handler = service.Spec.Configuration.StartupHeartbeat.Handler
	functionRequest.Message.Function.StartupHeartbeat.Path = service.Spec.Configuration.StartupHeartbeat.Path
	functionRequest.Message.Function.StartupHeartbeat.HealthEnable = service.Spec.Configuration.StartupHeartbeat.HealthEnable
	functionRequest.Message.Function.StartupHeartbeat.Timeout = service.Spec.Configuration.StartupHeartbeat.Timeout
	functionRequest.Message.Function.StartupHeartbeat.FailureThreshold = service.Spec.Configuration.StartupHeartbeat.FailureThreshold
	functionRequest.Message.Function.StartupHeartbeat.PeriodSeconds = service.Spec.Configuration.StartupHeartbeat.PeriodSeconds
	functionRequest.Message.Function.StartupHeartbeat.FirstCheckDelay = service.Spec.Configuration.StartupHeartbeat.FirstCheckDelay
	functionRequest.Message.Function.StartupHeartbeat.SuccessThreshold = service.Spec.Configuration.StartupHeartbeat.SuccessThreshold
	functionRequest.Message.Function.UsePoolPod = utils.IsVpaFeatureEnabled() && service.Spec.Configuration.UsePoolPod
	functionRequest.Message.Function.FileMountConfig = service.Spec.Configuration.FileMountConfig
	functionRequest.Message.Function.CustomJavaArgs = service.Spec.Configuration.CustomJavaArgs
	functionRequest.Message.Function.DNSDomainCfg = service.Spec.Configuration.DNSDomainCfg
	functionRequest.Message.Function.DNSConfig = service.Spec.Configuration.DNSConfig
	functionRequest.Message.Function.EndpointDomainCfg = service.Spec.Configuration.EndpointDomainCfg
	functionRequest.Message.Function.IsStreamEnable = service.Spec.Configuration.IsStreamEnable
	functionRequest.Message.Function.StreamResponse = service.Spec.Configuration.StreamResponse
	functionRequest.Message.Function.RestoreHook.Handler = service.Spec.Configuration.RestoreHook.Handler
	functionRequest.Message.Function.RestoreHook.Timeout = service.Spec.Configuration.RestoreHook.Timeout
	functionRequest.Message.Function.NetworkController.DisablePublicNetwork = service.Spec.Configuration.
		NetworkController.DisablePublicNetwork
	functionRequest.Message.Function.EnableClassIsolation = service.Spec.Configuration.EnableClassIsolation
	functionRequest.Message.Function.EnableWebsocket = service.Spec.Configuration.EnableWebsocket

	functionRequest.Message.Function.EnableGCLog = service.Spec.Configuration.EnableGCLog
	functionRequest.Message.Function.EnableSnapshot = service.Spec.Configuration.EnableSnapshot
	functionRequest.Message.Function.RounteInfo.ApigRouteEnable = service.Spec.Configuration.ApigRouteEnable
	functionRequest.Message.Function.PreStop.Handler = service.Spec.Configuration.PreStop.Handler
	functionRequest.Message.Function.PreStop.Timeout = service.Spec.Configuration.PreStop.Timeout
	functionRequest.Message.Function.PreStop.Type = service.Spec.Configuration.PreStop.Type
	functionRequest.Message.Function.IsBridgeFunction = service.Spec.Configuration.IsBridgeFunction
	functionRequest.Message.Function.IsStatefulFunction = service.Spec.Configuration.IsStatefulFunction
	functionRequest.Message.Function.CustomMetricsReport.Handler =
		service.Spec.Configuration.CustomMetricsReport.Handler
	functionRequest.Message.Function.CustomMetricsReport.Path =
		service.Spec.Configuration.CustomMetricsReport.Path
	functionRequest.Message.Function.CustomMetricsReport.CollectTime =
		service.Spec.Configuration.CustomMetricsReport.CollectTime
	functionRequest.Message.Function.CustomMetricsReport.Timeout =
		service.Spec.Configuration.CustomMetricsReport.Timeout
	functionRequest.Message.Function.DisableInsecureAkSk = service.Spec.Configuration.DisableInsecureAkSk
	functionRequest.Message.Function.BusinessType = service.Spec.Configuration.BusinessType
	functionRequest.Message.Function.LogPolicy = service.Spec.Configuration.LogPolicy
	functionRequest.Message.Function.Properties = service.Spec.Configuration.Properties
	functionRequest.Message.Function.IsSafety = service.Spec.Configuration.FuncVpc.IsSafety
	functionRequest.Message.Function.IsBridgeFunction = service.Spec.Configuration.IsBridgeFunction
	functionRequest.Message.Function.LtsCustomTag = service.Spec.Configuration.LtsCustomTag
	functionRequest.Message.Function.RuntimeTermTimeout = service.Spec.Configuration.RuntimeTermTimeout
	functionRequest.Message.Function.FuncSignature = utils.GetCRSignature(service)
	functionRequest.Message.Function.CustomUploadConfig = handleCustomUploadConfig(service)
	functionRequest.Message.Function.IsMcpFunction = service.Spec.Configuration.IsMcpFunction
	setProxyURL(&functionRequest)
	setRequestMessage(&functionRequest, service)
	setDeployInformation(&functionRequest.Message.Deployment, service)
	if reqInfo.SnapshotDeployment != nil {
		// normal/checkpoint mode go this way
		functionRequest.Message.SnapshotDeployment = *reqInfo.SnapshotDeployment
	} else {
		// restore mode go this way
		functionRequest.Message.SnapshotDeployment = service.Spec.SnapshotDeployment
	}
	// if snapshot is expired, go checkpoint
	handleExpireSnapshot(service, reqInfo)

	// if snapshot is circuit, go cold start
	updateSnapshotState(&functionRequest, service, reqInfo.TraceID)
	if len(service.Spec.Configuration.DNSDomainCfg) != 0 || len(service.Spec.Configuration.EndpointDomainCfg) != 0 {
		domains, err := utils.GetDNSCache().GetDNSDomainNames(service.ID)
		if err != nil {
			return functionRequest, fmt.Errorf("failed to get domain, err:%s", err.Error())
		}
		functionRequest.Message.Function.DNSDomainNames = domains
	}
	return functionRequest, nil
}

func updateSnapshotState(functionRequest *commontypes.FunctionSpecRequest, service *faasfunction.FunctionService,
	traceID string) {
	if functionRequest.Message.SnapshotDeployment.SnapshotSize == 0 {
		log.GetLogger().Debugf("snapshot is unavailable for func %s, version is %s, traceId is %s.",
			service.Spec.Configuration.FunctionURN, service.Version, traceID)
		return
	}

	snapshotMeta, err := getSnapshotMeta(service)
	if err != nil {
		log.GetLogger().Errorf("failed to get snapshot data: err %s, traceID: %s", err.Error(), traceID)
		return
	}
	functionRequest.Message.SnapshotDeployment.SnapshotState = snapshotMeta.SnapshotResult.SnapshotState
}

func getSnapshotMeta(service *faasfunction.FunctionService) (*commontypes.SnapshotMeta, error) {
	funcSvcID := utils.CrName(service.Spec.Configuration.BusinessID, service.Spec.Configuration.TenantID,
		service.Spec.Configuration.FunctionName, service.Spec.Configuration.FunctionVersion)
	snapshotMeta := cache.WorkerCache.GetSnapshotMeta(funcSvcID)
	if snapshotMeta == nil || snapshotMeta.SnapshotResult == nil {
		log.GetLogger().Errorf("failed to get snapshot data %s from cache", funcSvcID)
		return nil, errors.New("snapshot data from cache is nil")
	}
	return snapshotMeta, nil
}

func handleExpireSnapshot(service *faasfunction.FunctionService, reqInfo *config.RequestInfo) {
	if service.Spec.Configuration.EnableSnapshot == "disable" {
		log.GetLogger().Debugf("Snapshot is disable, function is %s, version is %s, traceId is %s.",
			service.Spec.Configuration.FunctionURN, service.Version, reqInfo.TraceID)
		return
	}
	functionName := service.Spec.Configuration.FunctionName
	id := service.Version
	key := functionName + id
	if service.Spec.SnapshotDeployment.IsSnapshotExpire {
		log.GetLogger().Debugf("need to checkpoint")
		if _, exist := updateEtcdMap.Load(key); !exist {
			log.GetLogger().Infof("start to checkpoint expire snapshot, function is %s, version is %s, "+
				"traceId is %s", service.Spec.Configuration.FunctionURN, service.Version, reqInfo.TraceID)
			// set default snapshot in etcd to trigger snapshot
			go updateSnapshotInEtcd(key, service, reqInfo.TraceID)
			updateEtcdMap.Store(key, "")
		}
	} else if _, exist := updateEtcdMap.Load(key); !exist {
		// snapshot is not expired and do not exist map.
		return
	} else {
		updateEtcdMap.Delete(key)
		log.GetLogger().Infof("Snapshot is refresh, delete key %s", key)
	}
}

func updateSnapshotInEtcd(key string, service *faasfunction.FunctionService, traceID string) {
	// Get default snapshot
	snapshotDeployment := getDefaultSnapshotDeployment(service)
	value, err := json.Marshal(snapshotDeployment)
	if err != nil {
		log.GetLogger().Warnf("%s | failed to Marshal snapshotDeployment and error: %s",
			traceID, err.Error())
		updateEtcdMap.Delete(key)
		return
	}
	etcdType := etcd3.Meta
	if commonutils.IsCAEFunc(service.Spec.Configuration.BusinessType) {
		etcdType = etcd3.CAEMeta
	}
	snapshotMeta, err := getSnapshotMeta(service)
	if err != nil {
		log.GetLogger().Errorf("failed to get snapshot data: err %s, traceID: %s", err.Error(), traceID)
		return
	}
	etcdKey := etcd3.HandleCellPrefix(snapshotMeta.ETCDKey)
	// write default snapshot deployment
	err = utils.SubmitResultByEtcdType(etcdKey, traceID, defaultSubmitResultRetryTimes, value, etcdType)
	if err != nil {
		log.GetLogger().Warnf("%s | failed to submit default %s snapshot and error: %s",
			traceID, key, err.Error())
		updateEtcdMap.Delete(key)
	}
}

func getDefaultSnapshotDeployment(service *faasfunction.FunctionService) *commontypes.SnapshotDeploymentSpec {
	snapshotDeployment := &commontypes.SnapshotDeploymentSpec{}
	snapshotDeployment.AppID = service.Spec.SnapshotDeployment.AppID
	snapshotDeployment.BucketID = service.Spec.SnapshotDeployment.BucketID
	// default objectId
	snapshotDeployment.ObjectID = service.Spec.Code.Location.Nsp.ObjectID
	snapshotDeployment.BucketURL = service.Spec.SnapshotDeployment.BucketURL
	snapshotDeployment.IsSnapshotExpire = true
	snapshotVersion := commontypes.SnapshotVersion{}
	snapshotVersion.ObjectID = service.Spec.Code.Location.Nsp.ObjectID
	snapshotDeployment.SnapshotVersions = append(snapshotDeployment.SnapshotVersions, snapshotVersion)
	return snapshotDeployment
}

func setDeployInformation(deploymentSpec *commontypes.FunctionDeploymentSpec, service *faasfunction.FunctionService) {
	deploymentSpec.BucketID = service.Spec.Code.Location.Nsp.BucketID
	deploymentSpec.ObjectID = service.Spec.Code.Location.Nsp.ObjectID
	deploymentSpec.CodeSize = service.Spec.Code.CodeSize

	deploymentSpec.Access, deploymentSpec.Secret, deploymentSpec.SecurityToken = tokenrotation.GetTempToken()
	// base64 layers info
	rawData, err := json.Marshal(service.Spec.Configuration.Layers)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal layer info: %s", err.Error())
	}
	deploymentSpec.Layers = base64.StdEncoding.EncodeToString(rawData)

	cfg, err := config.GetConfig()
	if err == nil {
		deploymentSpec.DeployDir = cfg.DeployDir
	}
}

func setKmsCipherToFuncRequest(functionRequest *commontypes.FunctionSpecRequest,
	service *faasfunction.FunctionService) error {
	tenantID := service.Spec.Configuration.TenantID

	envCipherKeyID := service.Spec.Configuration.EnvCipherKeyID
	envCipher, err := getKmsCache(tenantID, utils.EnvDecType, envCipherKeyID, service.ID)
	if err != nil {
		return err
	}
	functionRequest.Message.Function.EnvPlaintextKmsCipher = envCipher

	codeCipherKeyID := service.Spec.Code.Location.Nsp.CodeCipherKeyID
	codeCipher, err := getKmsCache(tenantID, utils.CodeDecType, codeCipherKeyID, service.ID)
	if err != nil {
		return err
	}
	functionRequest.Message.Deployment.CodePlaintextKmsCipher = codeCipher
	return nil
}

func getKmsCache(tenantID, decType, keyID, funcName string) (string, error) {
	if keyID == "" {
		return "", nil
	}
	envCipherKey := strings.Join([]string{tenantID, decType, keyID}, utils.CipherCacheGap)
	envCipherCache, ok := utils.GetUserCipherCache(envCipherKey)
	if !ok {
		log.GetLogger().Errorf("user %s is encrypt but not get cipher cache, tenant: %s, func: %s",
			tenantID, funcName, decType)
		return "", errors.New("user kms ID isn't empty but not get cipher cache")
	}
	return envCipherCache.Plaintext, nil
}

func getMaxStateSize() string {
	maxStateSize := os.Getenv("MAX_STATE_SIZE")
	if maxStateSize != "" {
		return maxStateSize
	}
	// default 256KB
	return "256"
}

func getFuncMountConfig(service *faasfunction.FunctionService) commontypes.FuncMountConfig {
	funcMountUser := commontypes.FuncMountUser{
		UserID:  service.Spec.Configuration.FuncMountConfig.FuncMountUser.UserID,
		GroupID: service.Spec.Configuration.FuncMountConfig.FuncMountUser.GroupID,
	}
	mounts := service.Spec.Configuration.FuncMountConfig.FuncMounts
	var funcMounts []commontypes.FuncMount
	for _, m := range mounts {
		funcMounts = append(funcMounts, commontypes.FuncMount{
			MountType:      m.MountType,
			MountResource:  m.MountResource,
			MountSharePath: m.MountSharePath,
			LocalMountPath: m.LocalMountPath,
			Status:         m.Status,
			ExtendedInfo:   m.ExtendedInfo,
		})
	}
	return commontypes.FuncMountConfig{
		FuncMountUser: funcMountUser,
		FuncMounts:    funcMounts,
	}
}

func setRequestMessage(functionRequest *commontypes.FunctionSpecRequest, service *faasfunction.FunctionService) {
	for _, value := range service.Spec.Configuration.Env {
		if value.Name == "ENVIRONMENT_KEY" {
			functionRequest.Message.Function.EnvKey = value.Value
			continue
		}
		if value.Name == "WORKER_ENV" {
			functionRequest.Message.Function.Environment = value.Value
			continue
		}
		if value.Name == "USER_DATA" {
			functionRequest.Message.Function.EncryptedUserData = value.Value
			continue
		}
	}
}

func setProxyURL(functionRequest *commontypes.FunctionSpecRequest) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config err when set proxy url, err: %s", err.Error())
		return
	}

	functionRequest.Message.Function.UVIndexURL = cfg.UVIndexURL
	functionRequest.Message.Function.NpmConfigRegistry = cfg.NpmConfigRegistry
}

func getRuntimeContainerID(pod *corev1.Pod) string {
	var runtimeContainerID string
	containerStatues := pod.Status.ContainerStatuses
	for _, containerStatue := range containerStatues {
		if containerStatue.Name == utils.RuntimeContainerName {
			runtimeContainerID = strings.TrimPrefix(containerStatue.ContainerID, "docker://")
			runtimeContainerID = strings.TrimPrefix(runtimeContainerID, "containerd://")
			break
		}
	}
	return runtimeContainerID
}

func initRequestToWorker(podIP, requestPath string, httpsEnable bool) (*fhttp.Request, error) {
	// create a new request to worker
	req := fhttp.AcquireRequest()

	// update path and scheme
	host := podIP + ":" + workerPort
	url := "//" + host + requestPath
	req.URI().Update(url)
	if httpsEnable {
		req.URI().SetScheme("https")
	} else {
		req.URI().SetScheme("http")
	}
	req.Header.SetHost(host)
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	authorization, timestamp := localauth.SignLocally(cfg.LocalAuth.AKey, cfg.LocalAuth.SKey,
		utils.AppID, cfg.LocalAuth.Duration)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.Authorization, authorization)
	workerMetricsTick := atomic.LoadInt64(&cfg.WorkerMetricsTick)
	if workerMetricsTick == 0 {
		workerMetricsTick = defaultWorkerMetricsTick
	}
	resourceUsageTick := atomic.LoadInt64(&cfg.ResourceUsageTick)
	if resourceUsageTick <= 0 {
		resourceUsageTick = defaultResourceUsageTick
	}
	caeMetricsTick := atomic.LoadInt64(&cfg.CaeMetricsTick)
	if caeMetricsTick <= 0 {
		caeMetricsTick = defaultCaeMetricsTick
	}
	req.Header.Set(constants.HeaderMetricsTick, strconv.Itoa(int(workerMetricsTick)))
	req.Header.Set(constants.HeaderResourceUsageTick, strconv.Itoa(int(resourceUsageTick)))
	req.Header.Set(constants.HeaderCaeMetricsTick, strconv.Itoa(int(caeMetricsTick)))
	return req, nil
}

func initPatPodIPRequest(podIP string, httpsEnable bool,
	param config.PatPodRequestParam) (*fhttp.Request, error) {
	req, err := initRequestToWorker(podIP, constants.ChangePatPodIPPath, httpsEnable)
	if err != nil {
		return nil, err
	}
	req.Header.SetMethod(fhttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, param.TraceID)
	req.Header.SetConnectionClose()

	var functionRequest commontypes.FunctionSpecRequest
	// set body
	if len(param.PatPodInfo) != 0 {
		for _, pat := range param.PatPodInfo {
			functionRequest.Message.NATConfigure = append(functionRequest.Message.NATConfigure, pat)
		}
	}
	// set body
	if len(param.EndpointPatPodInfo) != 0 {
		for _, pat := range param.EndpointPatPodInfo {
			functionRequest.Message.EndpointVpcNATConfigure =
				append(functionRequest.Message.EndpointVpcNATConfigure, pat)
		}
	}
	if len(param.PublicPatPodInfo) != 0 {
		for _, pat := range param.PublicPatPodInfo {
			functionRequest.Message.PublicVpcNATConfigure = append(functionRequest.Message.PublicVpcNATConfigure, pat)
		}
	}
	body, err := json.Marshal(functionRequest)
	if err != nil {
		return nil, err
	}
	req.ResetBody()
	req.SetBody(body)

	return req, nil
}

func initIdleModeRequest(podIP, traceID string, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
	req, err := initRequestToWorker(podIP, constants.NotifyFunctionPath, httpsEnable)
	if err != nil {
		return nil, nil, err
	}
	req.Header.SetMethod(fhttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, traceID)
	req.Header.Set(constants.HeaderIdleMode, strconv.FormatBool(idleMode))
	req.Header.SetConnectionClose()

	response := fhttp.AcquireResponse()

	return req, response, nil
}

func initBillingSwitchRequest(podIP, traceID string, httpsEnable,
	enableBilling bool) (*fhttp.Request, *fhttp.Response, error) {
	req, err := initRequestToWorker(podIP, constants.NotifyFunctionPath, httpsEnable)
	if err != nil {
		return nil, nil, err
	}
	req.Header.SetMethod(fhttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, traceID)
	req.Header.Set(constants.HeaderBillingSwitch, strconv.FormatBool(enableBilling))
	req.Header.SetConnectionClose()

	response := fhttp.AcquireResponse()

	return req, response, nil
}

func initSpecializeRequest(pod *corev1.Pod, functionRequest commontypes.FunctionSpecRequest,
	reqInfo *config.RequestInfo, httpsEnable bool,
	service *faasfunction.FunctionService) (*fhttp.Request, *fhttp.Response, error) {
	req, err := initRequestToWorker(pod.Status.PodIP, constants.LoadFunctionPath, httpsEnable)

	if err != nil {
		return nil, nil, err
	}
	setAgencyRequestHeader(req, functionRequest)
	req.Header.SetMethod(fhttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, reqInfo.TraceID)
	req.Header.Set(constants.HeaderTraceParent, reqInfo.TraceParent)
	req.Header.Set(constants.HeaderPodApplier, reqInfo.Applier)
	req.Header.Set(constants.HeaderBridgeFuncFlag,
		strconv.FormatBool(functionRequest.Message.Function.IsBridgeFunction))
	req.Header.Set(constants.HeaderIdleMode, strconv.FormatBool(cache.WorkerCache.GetIdleMode(service.ID)))
	req.Header.Set(constants.HeaderClusterID, clusterID)
	if reqInfo.StateID != "" {
		req.Header.Set(constants.StateID, reqInfo.StateID)
		// stateful function should be persisted
		req.Header.Set(constants.FunctionLifecycleType, constants.PersistenceFunction)
	}
	if commonutils.IsCAEFunc(functionRequest.Message.Function.BusinessType) {
		req.Header.Set(constants.FunctionLifecycleType, constants.PersistenceFunction)
	}
	// config for worker suicide timeout to avoid waiting too long in 30 min
	suicideTimeout := os.Getenv(constants.SuicideTimeoutConfig)
	if len(suicideTimeout) != 0 {
		req.Header.Set(constants.XSuicideTimeoutConfig, suicideTimeout)
	}
	req.Header.Set(constants.CacheManagerClusterID, cacheManagerClusterIP)
	req.Header.Set(constants.DNSResolveAddrKey, safeVpcDNSAddr)
	if reqInfo.IsNeedDeleteSnapshot {
		req.Header.Set(constants.NeedDeleteSnapshot, strconv.FormatBool(reqInfo.IsNeedDeleteSnapshot))
	}
	setPreloadHeader(req, service)
	// get runtime containerID
	runtimeContainerID := getRuntimeContainerID(pod)
	functionRequest.Message.Function.RuntimeContainerID = runtimeContainerID
	functionRequest.Message.Function.RounteInfo.ApigRoutes = cache.WorkerCache.GetAPIGIP()
	functionRequest.Message.SvcCidr = config.GetK8sCidr().SvcCidr
	functionRequest.Message.Function.StreamContentType = utils.GetStreamContentTypeWhiteList()

	cfg, err := config.GetConfig()
	if err != nil {
		return nil, nil, err
	}
	functionRequest.Message.Function.DisableWaitBeforeChangeNet = cfg.DisableWaitBeforeChangeNet
	whiteList := cfg.BodySizeLimitWhitelist
	if whiteList != nil && len(whiteList) > 0 && reqInfo.Memory >= memoryAccess {
		if value, ok := whiteList[reqInfo.TenantID]; ok {
			functionRequest.Message.Function.MaxRequestBodySize = strconv.FormatInt(value, base)
		}
	}
	// set reserved mode and metrics method for bridge, stateful bridge without reportMetrics method
	if functionRequest.Message.Function.IsBridgeFunction && reqInfo.StateID == "" {
		req.Header.Set(constants.FunctionLifecycleType, constants.PersistenceFunction)
		// Bridge entry is com.xx.FGCacheHandler(Module)::handleRequest, metrics method is Module.reportMetrics
		functionRequest.Message.Function.CustomMetricsReport.Handler =
			service.Spec.Configuration.Handler.Module + ".reportMetrics"
		bridgeMetricsUploadTick := atomic.LoadInt64(&cfg.BridgeMetricsUploadTick)
		if bridgeMetricsUploadTick <= 0 {
			bridgeMetricsUploadTick = defaultBridgeMetricsTick
		}
		bridgeMetricsCollectionTimeout := atomic.LoadInt64(&cfg.BridgeMetricsCollectionTimeout)
		if bridgeMetricsCollectionTimeout <= 0 {
			bridgeMetricsCollectionTimeout = defaultBridgeMetricsCollectionTimeout
		}
		functionRequest.Message.Function.CustomMetricsReport.CollectTime = int(bridgeMetricsUploadTick)
		functionRequest.Message.Function.CustomMetricsReport.Timeout = int(bridgeMetricsCollectionTimeout)
	}

	// set body
	if len(reqInfo.PatPodInfo) != 0 {
		for _, pat := range reqInfo.PatPodInfo {
			functionRequest.Message.NATConfigure = append(functionRequest.Message.NATConfigure, pat)
		}
	}
	// set body
	if len(reqInfo.EndpointTalentPatPodInfo) != 0 {
		for _, pat := range reqInfo.EndpointTalentPatPodInfo {
			functionRequest.Message.EndpointVpcNATConfigure =
				append(functionRequest.Message.EndpointVpcNATConfigure, pat)
		}
	}
	log.GetLogger().Debugf("podName: %s, funcName: %s, PublicPatPodInfo length %d, reqInfo.PublicPatPodInfo %+v",
		pod.Name, reqInfo.FunctionName, len(reqInfo.PublicPatPodInfo), reqInfo.PublicPatPodInfo)
	if len(reqInfo.PublicPatPodInfo) != 0 {
		for _, pat := range reqInfo.PublicPatPodInfo {
			functionRequest.Message.PublicVpcNATConfigure = append(functionRequest.Message.PublicVpcNATConfigure, pat)
		}
	}
	body, err := json.Marshal(functionRequest)
	if err != nil {
		return nil, nil, snerror.New(utils.JSONParseError, err.Error())
	}
	req.ResetBody()
	req.SetBody(body)

	response := fhttp.AcquireResponse()
	return req, response, nil
}

func setPreloadHeader(req *fhttp.Request, service *faasfunction.FunctionService) {
	serviceName, funcName, err := urnutils.GetFunctionNameAndServiceName(service.Spec.Configuration.FunctionName)
	if err != nil {
		log.GetLogger().Warnf("failed to get function and service name, err %s", err.Error())
		return
	}
	tenantID := service.Spec.Configuration.TenantID
	version := service.Spec.Configuration.FunctionVersion
	functionVersionName := commonutils.GetPreloadFunctionVersionName(serviceName, funcName, version)
	if cache.GetPreloadCache().CheckFunctionIsPreload(tenantID, functionVersionName) {
		log.GetLogger().Infof("function %s is enable preload", functionVersionName)
		req.Header.Set(constants.HeaderPreloadFunctionEnable, "true")
	}
	for _, layer := range service.Spec.Configuration.Layers {
		layerVersionName := commonutils.GetPreloadLayerVersionName(layer.Name, layer.Version)
		if cache.GetPreloadCache().CheckLayerIsPreload(tenantID, layerVersionName) {
			log.GetLogger().Infof("layer %s is enable preload", layerVersionName)
			req.Header.Set(constants.HeaderPreloadLayerEnable, "true")
			break
		}
	}
}

func setAgencyRequestHeader(req *fhttp.Request, functionRequest commontypes.FunctionSpecRequest) {
	var agencyID = ""
	tenantID := utils.GetTenantIDFromURN(functionRequest.Message.Function.URN)
	if functionRequest.Message.Function.Role.AppXRole != "" {
		agencyID = functionRequest.Message.Function.Role.AppXRole
	} else if functionRequest.Message.Function.Role.XRole != "" {
		agencyID = functionRequest.Message.Function.Role.XRole
	}
	if agencyID != "" {
		domainID := functionRequest.Message.Function.DomainID
		agency := utils.GetUserAgencyFromCacheMap(tenantID, domainID, agencyID)
		disableInsecureAkSk := functionRequest.Message.Function.DisableInsecureAkSk
		if !disableInsecureAkSk {
			req.Header.Set(constants.HeaderAccessKey, agency.AccessKey)
			req.Header.Set(constants.HeaderSecretKey, agency.SecretKey)
		}
		req.Header.Set(constants.HeaderAuthToken, agency.Token)
		req.Header.Set(constants.HeaderSecurityAccessKey, agency.SecurityAk)
		req.Header.Set(constants.HeaderSecuritySecretKey, agency.SecuritySk)
		req.Header.Set(constants.HeaderSecurityToken, agency.SecurityToken)
	}
	if utils.IsIAMTokenFeatureEnable() && tenantID != "" && needSetIAMToken(functionRequest) {
		iam.SetIAMTokenRequestHeader(req, tenantID, functionRequest.Message.Function.URN)
	}
}

// java8、CAE、cpp+stateful
func needSetIAMToken(functionRequest commontypes.FunctionSpecRequest) bool {
	return commonutils.IsCAEFunc(functionRequest.Message.Function.BusinessType) ||
		strings.Contains(functionRequest.Message.Function.Language, "java8") ||
		(strings.Contains(functionRequest.Message.Function.Language, "custom-runtime-rpc") &&
			functionRequest.Message.Function.IsStatefulFunction)
}

// initDuration init duration
func initDuration(chooseStartTime time.Time, choosePodTime time.Duration, workerDuration string) string {
	var duration string
	generalizeEndTime := time.Now().Sub(chooseStartTime) - choosePodTime
	if workerDuration == "" {
		duration = fmt.Sprintf("%s/%s///", choosePodTime, generalizeEndTime)
	} else {
		duration = fmt.Sprintf("choosePod:%s/workerMgrTotal:%s/%s", choosePodTime, generalizeEndTime, workerDuration)
	}
	return duration
}

// SendUpdateAgencyInfoRequestToWorker send update agency info request to worker
func (p *PodFetcher) SendUpdateAgencyInfoRequestToWorker(workerInstance v1alpha1.WorkerInstance, traceID string,
	workerID string, service *faasfunction.FunctionService, podName string) {
	req, response, err := initUpdateAgencyInfoRequest(workerInstance, service, p.httpsEnable)
	if err != nil {
		log.GetLogger().Warnf("failed to init update worker agency info request for pod(%s), worker(%s), "+
			"traceID(%s): err: %s", podName, workerID, traceID, err.Error())
		return
	}
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)
	req.Header.Set(constants.HeaderPodName, podName)
	timeout := time.Duration(defaultUpdateWorkerAgencyTimeout) * time.Second
	err = p.workerClient.DoTimeout(req, response, timeout)
	if err != nil {
		log.GetLogger().Warnf("failed to send request to node(%s) of pod(%s) ip(%s) "+
			"for worker(%s), traceID(%s) and error %s",
			workerInstance.NodeIP, podName, workerInstance.IP, workerID, traceID, err.Error())
		return
	}
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Warnf("failed to update worker agency info of pod(%s), node: %s, worker: %s, "+
			"respCode: %d, respBody: %s", podName, workerInstance.NodeIP, workerID, response.Header.StatusCode(),
			string(response.Body()))
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(response.Body(), snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal the response body from worker")
			return
		}

		return
	}

	log.GetLogger().Infof("succeed to update worker agency info: %s for function: %s, deployNode: %s, "+
		"traceID: %s", podName, service.Spec.Configuration.FunctionName, workerInstance.NodeIP, traceID)
	return
}

func (p *PodFetcher) retryForStatefulFunc(ctx context.Context, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	var err error
	var pod *corev1.Pod
	for i := 1; i < maxRetries; i++ {
		time.Sleep(retryInterval)
		pod, err = p.generalizePodWithMaxInstance(ctx, requestInfo, functionService)
		if err == nil {
			return pod, nil
		} else if commonutils.IsUserError(err) {
			return nil, err
		} else {
			log.GetLogger().Errorf("retry stateful failed. funcName: %s, stateID: %s, traceID: %s, error: %s, "+
				"retry times=%d", requestInfo.FunctionName, requestInfo.StateID, requestInfo.TraceID, err.Error(), i)
		}
	}
	return nil, err
}

func (p *PodFetcher) getWorkerLog(pod *corev1.Pod) string {
	podLogOpts := corev1.PodLogOptions{}
	podLogOpts.Container = utils.WorkerContainerName
	podLogOpts.LimitBytes = &readWorkerContainerLogLimitBytes
	req := p.KubeClient.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &podLogOpts)
	panicPodLog, err := req.Stream(context.Background())
	if err != nil {
		log.GetLogger().Errorf("failed to open stream from podlogs req: %s", err.Error())
		return ""
	}
	defer panicPodLog.Close()
	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, panicPodLog)
	if err != nil {
		log.GetLogger().Errorf("failed to copy information from worker container log to buf: %s", err.Error())
		return ""
	}
	workerLog := buf.String()
	panicIndex := strings.LastIndex(workerLog, "panic:")
	if panicIndex < 0 {
		return "no panic log"
	}
	return workerLog[panicIndex:]
}

// initUpdateAgencyInfoRequest init update worker agency info request
func initUpdateAgencyInfoRequest(workerInstance v1alpha1.WorkerInstance, service *faasfunction.FunctionService,
	httpsEnable bool) (*fhttp.Request, *fhttp.Response, error) {
	req, err := initRequestToWorker(workerInstance.IP, constants.UpdateAgencyInfo, httpsEnable)
	if err != nil {
		return nil, nil, err
	}

	var functionRequest commontypes.FunctionSpecRequest
	functionRequest.Message.Function.Role.XRole = service.Spec.Configuration.Role.XRole
	functionRequest.Message.Function.Role.AppXRole = service.Spec.Configuration.Role.AppXRole
	functionRequest.Message.Function.URN = service.Spec.Configuration.FunctionVersionURN
	functionRequest.Message.Function.DomainID = service.Spec.Configuration.DomainID
	functionRequest.Message.Function.DisableInsecureAkSk = service.Spec.Configuration.DisableInsecureAkSk

	setAgencyRequestHeader(req, functionRequest)

	if commonutils.CheckIAMTokenFunc(service.Spec.Configuration.Runtime.Language,
		service.Spec.Configuration.BusinessType) {
		setIAMTokenHeader(req, workerInstance.TenantID, service.Spec.Configuration.FunctionVersionURN)
	}

	if len(service.IsUpgrading) != 0 {
		req.Header.Set("X-Cluster-Upgrading", service.IsUpgrading)
	}
	req.Header.SetMethod(fhttp.MethodPost)
	req.Header.SetConnectionClose()

	body, err := json.Marshal(functionRequest)
	if err != nil {
		return nil, nil, err
	}
	req.ResetBody()
	req.SetBody(body)

	response := fhttp.AcquireResponse()
	return req, response, nil
}

func setIAMTokenHeader(req *fhttp.Request, tenantID string, urn string) {
	tokenSalt := cache.GetFromExpiringIAMTokenMap(tenantID)
	if tokenSalt == nil {
		log.GetLogger().Warnf("tokenSalt is nil, tenantID: %s, urn: %s", tenantID, urn)
		return
	}
	req.Header.Set(constants.HeaderIAMToken, tokenSalt.Token)
}

// StatefulInstanceFlowOutDelete -
func StatefulInstanceFlowOutDelete(podAction []cache.PodAction) {
	fetcher := GetPodFetcher()
	if fetcher == nil {
		log.GetLogger().Warnf("pod fetcher is nil")
		return
	}
	if len(podAction) == 0 {
		return
	}
	for _, podAct := range podAction {
		functionService := podAct.StatefulMeta.FuncMeta
		trace := podAct.WorkerIns.PodName + "-" + podAct.WorkerIns.StateID

		if functionService == nil {
			log.GetLogger().Warnf("failed to get functionService %s, %s, %s",
				podAct.FuncSvc, podAct.WorkerIns.PodName, trace)
			continue
		}
		if len(cache.WorkerCache.GetFunctionPreStopHandler(podAct.FuncSvc)) != 0 {
			functionService.IsUpgrading = "true"
			workerID := constants.DefaultAZ + "-#-" + podAct.WorkerIns.PodName
			fetcher.SendUpdateAgencyInfoRequestToWorker(podAct.WorkerIns, trace,
				workerID, functionService, podAct.WorkerIns.PodName)
		}
		if err := fetcher.DeletePodByK8S(podAct.WorkerIns.PodName, trace); err != nil {
			log.GetLogger().Warnf("delete state: %s pod: %s error: %s, trace: %s",
				podAct.WorkerIns.StateID, podAct.WorkerIns.PodName, err.Error(), trace)
		}
	}
}

// UpdateAllFuncAgency -
func UpdateAllFuncAgency(stopCh <-chan struct{}, interval time.Duration) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop chan is nil")
		return
	}
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			updateWorkerAgency()
		case _, ok := <-stopCh:
			if !ok {
				ticker.Stop()
				return
			}
		}
	}
}

func updateWorkerAgency() {
	fetcher := GetPodFetcher()
	if fetcher == nil {
		log.GetLogger().Warnf("pod fetcher is nil")
		return
	}
	now := timeutil.NowUnixMillisecond()
	delta := utils.UpdateAgencyInterval.Milliseconds()
	workerAgencyMap := cache.WorkerCache.GetWorkerAgencyMap()
	for _, workerAgency := range workerAgencyMap {
		if len(workerAgency.WorkerIns) == 0 {
			continue
		}
		for _, instance := range workerAgency.WorkerIns {
			if now-instance.CreationTime < delta {
				continue
			}

			traceID := "update-agency-" + uuid.New().String()
			fetcher.SendUpdateAgencyInfoRequestToWorker(instance, traceID,
				instance.InstanceID, workerAgency.FuncMeta, instance.PodName)
		}
	}
}

func handleCAECustomS3Config(service *faasfunction.FunctionService) commontypes.CAECustomS3ConfigInfo {
	if service == nil || service.Spec.Configuration.CAECustomS3ConfigInfo.Endpoint == "" {
		return commontypes.CAECustomS3ConfigInfo{}
	}
	return service.Spec.Configuration.CAECustomS3ConfigInfo
}

func handleCustomUploadConfig(service *faasfunction.FunctionService) commontypes.CustomUploadConfig {
	if service == nil || service.Spec.Configuration.CustomUploadConfig.Endpoint == "" {
		return commontypes.CustomUploadConfig{}
	}
	return service.Spec.Configuration.CustomUploadConfig
}
