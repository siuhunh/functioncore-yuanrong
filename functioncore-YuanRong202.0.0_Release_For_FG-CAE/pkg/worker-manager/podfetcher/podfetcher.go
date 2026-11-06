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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	fhttp "github.com/valyala/fasthttp"
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
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/redisclient/poderrormessage"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/tokenrotation"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
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
	maxRetries                    = 3
	setBillingSwitchRetryInterval = 5 * time.Second
	setBillingSwitchTimeout       = 10 * time.Second
	workerPort                    = "8081"
	defaultWorkerMetricsTick      = 60
	defaultResourceUsageTick      = 1
	defaultCaeMetricsTick         = 15
	// defaultUpdateWorkerAgencyTimeout reserves 30s for update worker agency
	defaultUpdateWorkerAgencyTimeout = 30

	memoryAccess = 1024
	base         = 10

	maxLogPaths = 20

	functionTaskApplyPrefix = "functiontask"
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
func (q *queue) DeleteEventInQueue(podName string) string {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if event, exist := q.processingQueue[podName]; exist {
		ev, ok := event.(CREvent)
		if !ok {
			log.GetLogger().Warnf("failed to converted interface to CREvent")
			return ""
		}
		q.RateLimitingInterface.Done(event)
		q.RateLimitingInterface.Forget(event)
		delete(q.processingQueue, ev.pod.Name)
		return ev.functionService.ID
	}
	return ""
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
	if err = wait.ExponentialBackoffWithContext(ctx, generalizePodBackoff, func() (bool, error) {
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
		cpu, mem := p.PoolMgr.GetStatefulDynamicClosestStandard(requestInfo.CreateOptions.CPU,
			requestInfo.CreateOptions.Memory)
		log.GetLogger().Infof("Get stateful dynamic standard is %s-%s with traceID: %s, stateID: %s",
			cpu, mem, requestInfo.TraceID, requestInfo.StateID)
		requestInfo.CPU = cpu
		requestInfo.Memory = mem
	}
	if requestInfo.CPU == 0 && requestInfo.Memory == 0 {
		requestInfo.CPU = functionService.Spec.Configuration.Resource.CPU
		requestInfo.Memory = functionService.Spec.Configuration.Resource.Mem
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

// GeneralizePod generalize pod
func (p *PodFetcher) GeneralizePod(ctx context.Context, requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	startTime := time.Now()
	updateSnapshotCriuOpts(functionService)
	// If migrate, we should pull up a new instance of the stateID
	if !requestInfo.IsMigrating {
		pod, err := statefulFilter(requestInfo, functionService)
		if err != nil {
			return nil, err
		}
		if pod != nil {
			return pod, nil
		}
	}
	p.refreshDynamicResources(requestInfo, functionService)
	if strings.Contains(requestInfo.Applier, functionTaskApplyPrefix) &&
		commonutils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) {
		funcSvcID := utils.CrName(requestInfo.BusinessID, requestInfo.TenantID,
			requestInfo.FunctionName, requestInfo.FunctionVersion)
		mux := p.GetCAEBusInstanceMutexes(funcSvcID)
		if mux != nil {
			mux.Lock()
			defer mux.Unlock()
			if pod := cache.WorkerCache.GetCAEBusInstance(funcSvcID); pod != nil {
				log.GetLogger().Infof("succeed to generalized pod %s for cae func %s, traceID: %s",
					pod.Name, requestInfo.FunctionName, requestInfo.TraceID)
				return pod, nil
			}
		}
	}
	pod, err := p.generalizePodWithMaxInstance(ctx, requestInfo, functionService)
	if err != nil {
		cache.WorkerCache.ResetStateIDMap(functionService.ID, requestInfo.StateID)
		return nil, err
	}
	requestInfo.CreationTime = time.Now().Unix()
	cache.WorkerCache.InsertHashKeyInMap(functionService.ID, pod.Name, requestInfo.StateID)
	workerIns := utils.MakeWorkerInsWithRequest(pod, requestInfo, utils.GetCRSignature(functionService),
		functionService.Spec.Configuration.BusinessType)
	actions, metadata := cache.WorkerCache.HandleInstance(functionService.ID,
		cache.NewInstanceMeta(workerIns, eventsource.Added))
	funcCtx := p.CreateNewContext(functionService.ID)
	workerIns.IsDirectFunc = commonutils.IsDirectFunc(functionService.Spec.Configuration.Runtime.Language)
	workerIns.HasInitializer = len(functionService.Spec.Configuration.Initializer.Handler) != 0
	if requestInfo.StateID != "" {
		workerIns.Creator = requestInfo.Creator
		etcdhandler.PutSNStateWorkerFromETCD(requestInfo, workerIns)
		workerIns.OwnerIP = pod.Status.HostIP
	}
	cache.WorkerCache.DeleteReloadDoneState(functionService.ID, requestInfo.StateID)
	etcdhandler.PutInstanceToETCD(funcCtx, workerIns)
	// send 2 pat pods when worker has only 1 pat pod and instance pod has store in worker cache
	p.handleVpcHook(pod, requestInfo, functionService)
	p.DoActions(functionService.ID, actions, metadata)

	totalDuration := time.Now().Sub(startTime).Seconds()
	instanceNum, scalingNum := cache.WorkerCache.GetInstanceNum(functionService.ID)

	m := monitoring.ColdStartMetrics{Delay: totalDuration}
	monitoring.PublishColdStartMetrics(m)
	log.GetLogger().Infof("%s | %s | %s | %f | %s | "+
		"currentNum: %d, scalingNum: %d | deployNode: %s | podName: %s | isReload %t | succeed to generalized pod",
		requestInfo.FunctionName, requestInfo.TenantID, requestInfo.TraceID, totalDuration, requestInfo.Duration,
		instanceNum, scalingNum, pod.Spec.NodeName, pod.Name, requestInfo.IsReload)

	crEvent := CREvent{
		pod:             pod,
		requestInfo:     requestInfo,
		functionService: functionService,
	}
	p.Queue.AddRateLimited(crEvent)
	return pod, err
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
		pod, err = p.getPodFromPool(ctx, reqInfo, service)
		if err != nil {
			return nil, err
		}
		// acquire pat pods from vpc controller
		if err = p.getOrCreatePatPods(ctx, reqInfo, service, pod); err != nil {
			return nil, err
		}
	}
	return pod, p.generalizePod(pod, reqInfo, service, chooseStartTime, enableCircuitBreaker)
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
			podErrInfo := utils.GeneratePodErrorInfo(service.Spec.Configuration.Properties, pod.Name, err)
			poderrormessage.ReportPodErrMsgToRedis(redisclient.GetRedisCmd(),
				service.Spec.Configuration.FunctionVersionURN,
				service.Spec.Configuration.AutoScalerConfig.MinimumScale, podErrInfo)
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
	workerDuration, err := p.sendGeneralizeRequestToWorker(pod, reqInfo, service)
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
				log.GetLogger().Warnf("failed to send pat ip to worker: %s, retry times: %d, traceID: %s, error: %s",
					param.PodName, retryTimes, param.TraceID, err.Error())
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
	cacheManagerService, err := p.KubeClient.CoreV1().Services(commonutils.PodNameSpace()).Get(ctx,
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
	if err != nil {
		return "", fmt.Errorf("failed to init function request for pod(%s), worker(%s) :%s",
			pod.Name, reqInfo.WorkerID, err.Error())
	}
	err = setKmsCipherToFuncRequest(&functionRequest, service, reqInfo.TraceID)
	if err != nil {
		return "", fmt.Errorf("failed to set KmsCipher to generalize pod request for pod(%s), err :%s, traceID: %s",
			pod.Name, err.Error(), reqInfo.TraceID)
	}
	p.safetyVpcPodSetting(service)
	checkLogPathSize(functionRequest, reqInfo, pod.Name)
	req, response, err := initSpecializeRequest(pod, functionRequest, reqInfo, p.httpsEnable,
		cache.WorkerCache.GetIdleMode(service.ID))
	if err != nil {
		return "", err
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
	if snErr.Code == snerror.RestoreDeadErrorCode {
		handleRestoreDead(reqInfo.TraceID, service)
	}
	if snErr.Code == snerror.InnerRuntimeInitTimeoutCode {
		log.GetLogger().Infof("init pod %s with traceID %s has some load err, code %d err %s", pod.Name,
			reqInfo.TraceID, snErr.Code, snErr.Message)
		reqInfo.LoadErr = snErr
	}

	if response.Header.StatusCode() != fhttp.StatusOK && snErr.Code != snerror.InnerRuntimeInitTimeoutCode {
		return p.handleStatusNotOk(pod, reqInfo, response, snErr)
	}
	alarm.GetClient().ClearAlarm(alarm.OBSConnection)
	return string(response.Header.Peek(constants.HeaderLoadDuration)), nil
}

func (p *PodFetcher) handleStatusNotOk(pod *corev1.Pod, reqInfo *config.RequestInfo,
	response *fhttp.Response, snErr *snerror.BadResponse) (string, error) {
	log.GetLogger().Errorf("failed to generalize pod(%s), node: %s, worker: %s, respCode: %d, respBody: %s",
		pod.Name, pod.Status.HostIP, reqInfo.WorkerID, response.Header.StatusCode(), string(response.Body()))
	if snErr.Code == snerror.DownLoadCodeErrorCode {
		alarm.GetClient().ReportAlarm(alarm.OBSConnection,
			fmt.Sprintf("error msg=%s,", snErr.Message))
	}
	return "", snerror.New(snErr.Code, snErr.Message)
}

func handleRestoreDead(traceID string, service *faasfunction.FunctionService) {
	// check redis restore failNum
	key := service.Spec.Configuration.FunctionVersionURN
	info := getRestoreFailedInfo(key)
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("Get config error!")
		return
	}
	threshold := conf.PoolMgrConfig.CheckpointPodConfig.RestoreFailureThreshold
	if info.FailedCount >= threshold {
		handleCircuitSnapshot(traceID, service)
		return
	}
	// update redis value
	info.FailedCount = info.FailedCount + 1
	if info.Time == 0 {
		info.Time = time.Now().Unix()
	}
	updateRestoreFailNum(key, info)
}

func handleCircuitSnapshot(traceID string, service *faasfunction.FunctionService) {
	etcdKey := fmt.Sprintf(constants.SnapshotKey, os.Getenv(constants.ClusterID), service.Spec.Configuration.TenantID,
		service.Spec.Configuration.FunctionName, service.Spec.Configuration.FunctionVersion, service.Version)
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient == nil || metaClient.Client == nil {
		log.GetLogger().Errorf("meta client is nil")
		return
	}
	result, err := metaClient.GetValues(defaultEtcdCtx, etcdKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get snapshot value! %s", err.Error())
		return
	}
	log.GetLogger().Debugf("get snapshot info key %s, value %s", result, etcdKey)
	spec := &commontypes.SnapshotDeploymentSpec{}
	snapshotStr := strings.Join(result, "")
	err = json.Unmarshal([]byte(snapshotStr), spec)
	if err != nil {
		log.GetLogger().Errorf("Unmarshal snapshot info from etcd error: %s .", err.Error())
		return
	}
	if spec.SnapshotState != circuitBreaking {
		spec.SnapshotState = circuitBreaking
	} else {
		log.GetLogger().Infof("No need to update snapshot state!")
		return
	}
	value, err := json.Marshal(spec)
	if err != nil {
		log.GetLogger().Errorf("Marshal snapshot info error: %s .", err.Error())
		return
	}
	// write  default snapshot deployment
	if err = utils.SubmitResult(etcdKey, traceID, defaultSubmitResultRetryTimes, value); err != nil {
		log.GetLogger().Warnf("%s | failed to update %s snapshot state to  circuit breaking  and error: %s",
			traceID, etcdKey, err.Error())
	}
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

func updateRestoreFailNum(key string, info commontypes.FailedInfo) {
	redisClient := monitoring.GetRedisClient()
	var err error
	data, err := json.Marshal(info)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal failedInfo, error:%s", err.Error())
		return
	}
	for i := 0; i < redisRetryTime; i++ {
		ctx, _ := context.WithTimeout(context.Background(), redisCxtTimeout)
		if redisClient == nil {
			log.GetLogger().Warnf("redis is not ready when upload function instance")
			continue
		}
		err = redisClient.HSet(ctx, constants.RestoreFailInfo, key, data).Err()
		if err == nil {
			break
		}
		log.GetLogger().Errorf("failed to push key: %s , with err: %s , and try %d times", key, err.Error(), i+1)
		time.Sleep(redisRetryInterval)
	}
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
		IsMigrating:              req.IsMigrating,
		PatPodInfo:               req.PatPodInfo,
		EndpointTalentPatPodInfo: req.EndpointTalentPatPodInfo,
		PublicNetworkPatPodInfo:  req.PublicPatPodInfo,
		RouteInfo: commontypes.RouteInfoData{
			ApigRouteEnable: funSvc.Spec.Configuration.ApigRouteEnable,
			ApigRoutes:      cache.WorkerCache.GetAPIGIP(),
		},
		FuncMountConfig: getFuncMountConfig(funSvc),
		SvcCidr:         config.GetK8sCidr().SvcCidr,
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
	// delete router in etcd at first, and then delete worker cache, finally delete pod by k8s
	etcdhandler.DeleteInstanceFromETCD(context.Background(), instance)
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
	patchedLabels[utils.LabelApplier] = requestInfo.Applier
	patchedLabels[utils.LabelStateID] = requestInfo.StateID
	patchedLabels[utils.LabelBusinessID] = requestInfo.BusinessID
	patchedLabels[utils.LabelTenantID] = requestInfo.TenantID
	patchedLabels[utils.LabelFuncNamePrefix] = funcNamePrefix
	patchedLabels[utils.LabelServiceID] = serviceID
	patchedLabels[utils.LabelShortFuncName] = shortFuncName
	patchedLabels[utils.LabelFunctionVersion] = requestInfo.FunctionVersion
	creatorInfo := strings.Split(requestInfo.Creator, ":")
	if len(creatorInfo) == CreatorFuncVersionIndex+1 {
		patchedLabels[utils.LabelCreatorFuncName] = creatorInfo[CreatorFuncNameIndex]
		patchedLabels[utils.LabelCreatorFunctionVersion] = creatorInfo[CreatorFuncVersionIndex]
	} else if len(creatorInfo) == CreatorStateIDIndex+1 {
		patchedLabels[utils.LabelCreatorFuncName] = creatorInfo[CreatorFuncNameIndex]
		patchedLabels[utils.LabelCreatorFunctionVersion] = creatorInfo[CreatorFuncVersionIndex]
		patchedLabels[utils.LabelCreatorStateID] = creatorInfo[CreatorStateIDIndex]
	}

	patchedLabels[utils.LabelCreationTime] = strconv.FormatInt(requestInfo.CreationTime, base)

	patchedLabels = appendFunctionLabels(patchedLabels, functionMeta)

	return patchedLabels
}

func appendFunctionLabels(patchedLabels map[string]string,
	functionMeta *faasfunction.FunctionService) map[string]string {
	if functionMeta == nil || patchedLabels == nil {
		return patchedLabels
	}
	if functionMeta.Spec.Configuration.IsBridgeFunction {
		patchedLabels[utils.LabelIsBridgeFunction] = utils.TrueString
	}
	if functionMeta.Spec.Configuration.Timeout > utils.GetSyncRequestTimeout() {
		patchedLabels[utils.LabelLongRun] = "true"
	}
	if commonutils.IsDirectFunc(functionMeta.Spec.Configuration.Runtime.Language) {
		patchedLabels[utils.LabelDirect] = "true"
	}
	if len(functionMeta.Spec.Configuration.Initializer.Handler) != 0 {
		patchedLabels[utils.LabelHasInitializer] = "true"
	}
	if functionMeta.Spec.Configuration.BusinessType != "" {
		patchedLabels[utils.LabelBusinessType] = functionMeta.Spec.Configuration.BusinessType
	}

	if functionMeta.Spec.Configuration.IsMcpFunction {
		patchedLabels[utils.LabelMCPFunc] = "true"
	}
	return patchedLabels
}

func buildPatchPodTemplate(functionMeta *faasfunction.FunctionService, workerIns *corev1.Pod,
	ownerReference metav1.OwnerReference, requestInfo *config.RequestInfo) *corev1.Pod {
	crSignature := utils.GetCRSignature(functionMeta)
	if crSignature == "" {
		log.GetLogger().Warnf("failed to get crSignature, instance: %s, funcName: %s",
			workerIns.Name, functionMeta.Spec.Configuration.FunctionName)
	}
	patchPodTemplate := &corev1.Pod{}
	if commonutils.PodNameSpace() != constants.TrustworthinessNameSpace {
		patchPodTemplate.OwnerReferences = append(patchPodTemplate.OwnerReferences, ownerReference)
	}

	ownerIP := utils.GetOwnerIPByRequest(workerIns, requestInfo)
	log.GetLogger().Infof("owner IP for pod %s is %s", workerIns.Name, ownerIP)

	patchPodTemplate.Annotations = map[string]string{
		"crSignature":                crSignature,
		utils.OwnerIPKey:             ownerIP,
		utils.SafeToEvict:            utils.FalseString,
		utils.AnnotationFullFuncName: requestInfo.FunctionName,
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
	functionRequest.Message.Function.EnableGCLog = service.Spec.Configuration.EnableGCLog
	functionRequest.Message.Function.EnableSnapshot = service.Spec.Configuration.EnableSnapshot
	functionRequest.Message.Function.RounteInfo.ApigRouteEnable = service.Spec.Configuration.ApigRouteEnable
	functionRequest.Message.Function.PreStop.Handler = service.Spec.Configuration.PreStop.Handler
	functionRequest.Message.Function.PreStop.Timeout = service.Spec.Configuration.PreStop.Timeout
	functionRequest.Message.Function.PreStop.Type = service.Spec.Configuration.PreStop.Type
	functionRequest.Message.Function.CustomMetricsReport.Handler =
		service.Spec.Configuration.CustomMetricsReport.Handler
	functionRequest.Message.Function.CustomMetricsReport.CollectTime =
		service.Spec.Configuration.CustomMetricsReport.CollectTime
	functionRequest.Message.Function.CustomMetricsReport.Timeout =
		service.Spec.Configuration.CustomMetricsReport.Timeout
	functionRequest.Message.Function.DisableInsecureAkSk = service.Spec.Configuration.DisableInsecureAkSk
	functionRequest.Message.Function.BusinessType = service.Spec.Configuration.BusinessType
	functionRequest.Message.Function.LogPolicy = service.Spec.Configuration.LogPolicy
	functionRequest.Message.Function.Properties = service.Spec.Configuration.Properties
	functionRequest.Message.Function.IsSafety = service.Spec.Configuration.FuncVpc.IsSafety
	functionRequest.Message.Function.LtsCustomTag = service.Spec.Configuration.LtsCustomTag
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

	if len(service.Spec.Configuration.DNSDomainCfg) != 0 || len(service.Spec.Configuration.EndpointDomainCfg) != 0 {
		domains, err := utils.GetDNSCache().GetDNSDomainNames(service.ID)
		if err != nil {
			return functionRequest, fmt.Errorf("failed to get domain, err:%s", err.Error())
		}
		functionRequest.Message.Function.DNSDomainNames = domains
	}
	return functionRequest, nil
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
	etcdKey := fmt.Sprintf(constants.SnapshotKey, os.Getenv(constants.ClusterID), service.Spec.Configuration.TenantID,
		service.Spec.Configuration.FunctionName, service.Spec.Configuration.FunctionVersion, service.Version)
	// write  default snapshot deployment
	if err = utils.SubmitResult(etcdKey, traceID, defaultSubmitResultRetryTimes, value); err != nil {
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
	service *faasfunction.FunctionService, traceID string) error {
	if len(service.Spec.Configuration.EnvCipherKeyID) == 0 && len(service.Spec.Code.Location.Nsp.CodeCipherKeyID) == 0 {
		return nil
	}

	var envEncryptKmsCipher string
	for _, value := range service.Spec.Configuration.Env {
		if value.Name == utils.EnvEncryptKmsCipher {
			envEncryptKmsCipher = value.Value
		}
	}
	envCipher, err := getKmsCache(utils.CheckKmsCipherInfo{
		DecType:       utils.EnvDecType,
		TenantID:      service.Spec.Configuration.TenantID,
		KmsKeyID:      service.Spec.Configuration.EnvCipherKeyID,
		EncryptCipher: envEncryptKmsCipher,
		TraceID:       traceID,
	}, service)
	if err != nil {
		return err
	}
	functionRequest.Message.Function.EnvPlaintextKmsCipher = envCipher

	codeCipher, err := getKmsCache(utils.CheckKmsCipherInfo{
		DecType:       utils.CodeDecType,
		TenantID:      service.Spec.Configuration.TenantID,
		KmsKeyID:      service.Spec.Code.Location.Nsp.CodeCipherKeyID,
		EncryptCipher: service.Spec.Code.Location.Nsp.CodeEncryptKmsCipher,
		TraceID:       traceID,
	}, service)
	if err != nil {
		return err
	}
	functionRequest.Message.Deployment.CodePlaintextKmsCipher = codeCipher
	return nil
}

func getKmsCache(info utils.CheckKmsCipherInfo, functionService *faasfunction.FunctionService) (string, error) {
	if info.KmsKeyID == "" {
		return "", nil
	}

	cipherKey := strings.Join([]string{info.TenantID, info.DecType, info.KmsKeyID,
		functionService.Spec.Configuration.DomainID, utils.GetAgencyIDFromFunctionService(functionService)},
		utils.CipherCacheGap)
	cipherCache, ok := utils.GetUserCipherCache(cipherKey)
	if !ok {
		log.GetLogger().Warnf("failed to get user kms cipher in cache, will try to get from kms,"+
			" tenant: %s, function: %s, traceID: %s", info.TenantID, functionService.ID, info.TraceID)
		var err error
		cipherCache, err = utils.CheckAndStoreCipherCache(functionService, info)
		if err != nil {
			return "", err
		}
	}
	return cipherCache.Plaintext, nil
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
	functionRequest.Message.Function.InstanceTimeoutExitWhiteList = cfg.InstanceTimeoutExitWhiteList
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
	reqInfo *config.RequestInfo, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error) {
	req, err := initRequestToWorker(pod.Status.PodIP, constants.LoadFunctionPath, httpsEnable)

	if err != nil {
		return nil, nil, err
	}
	setAgencyRequestHeader(req, functionRequest)
	req.Header.SetMethod(fhttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, reqInfo.TraceID)
	req.Header.Set(constants.HeaderTraceParent, reqInfo.TraceParent)
	req.Header.Set(constants.HeaderPodApplier, reqInfo.Applier)
	req.Header.Set(constants.HeaderIdleMode, strconv.FormatBool(idleMode))
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
	// get runtime containerID
	runtimeContainerID := getRuntimeContainerID(pod)
	functionRequest.Message.Function.RuntimeContainerID = runtimeContainerID
	functionRequest.Message.Function.RounteInfo.ApigRoutes = cache.WorkerCache.GetAPIGIP()
	functionRequest.Message.SvcCidr = config.GetK8sCidr().SvcCidr
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
	// set body
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

func setAgencyRequestHeader(req *fhttp.Request, functionRequest commontypes.FunctionSpecRequest) {
	var agencyID = ""
	if functionRequest.Message.Function.Role.AppXRole != "" {
		agencyID = functionRequest.Message.Function.Role.AppXRole
	} else if functionRequest.Message.Function.Role.XRole != "" {
		agencyID = functionRequest.Message.Function.Role.XRole
	}
	if agencyID != "" {
		tenantID := utils.GetTenantIDFromURN(functionRequest.Message.Function.URN)
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
}

// initDuration init duration
func initDuration(chooseStartTime time.Time, choosePodTime time.Duration, workerDuration string) string {
	var duration string
	generalizeEndTime := time.Now().Sub(chooseStartTime) - choosePodTime
	if workerDuration == "" {
		duration = fmt.Sprintf("%s/%s///", choosePodTime, generalizeEndTime)
	} else {
		duration = fmt.Sprintf("%s/%s/%s", choosePodTime, generalizeEndTime, workerDuration)
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
			if instance.Applier != "worker-manager" && now-instance.CreationTime < delta {
				continue
			}

			traceID := "update-agency-" + uuid.New().String()
			fetcher.SendUpdateAgencyInfoRequestToWorker(instance, traceID,
				instance.InstanceID, workerAgency.FuncMeta, instance.PodName)
		}
	}
}
