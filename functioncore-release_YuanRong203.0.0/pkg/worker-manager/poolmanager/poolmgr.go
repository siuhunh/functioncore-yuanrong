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

// Package poolmanager manages pool
package poolmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	fhttp "github.com/valyala/fasthttp"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/schedulequeue"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/common/snerror"
	commontypes "functioncore/pkg/common/types"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/detector"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/podhandler"
	"functioncore/pkg/worker-manager/podtemplate"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
)

const (
	coldStartPoolLabelKey         = "coldStartPool"
	specializeLabelKey            = "specialize"
	specializeLabelValuePostStart = "poststart"
	specializeLabelValuePreStart  = "prestart"
	faasAPIName                   = "FunctionService"
	projectID                     = "PROJECTID"
	projectName                   = "PROJECTNAME"
	clusterID                     = "CLUSTERID"
	clusterName                   = "CLUSTERNAME"

	isPoolPodLabelKey = "isPoolPod"
	usedLabelKey      = "used"
	falseString       = "false"
	trueString        = "true"
	k8sHostNameKey    = "kubernetes.io/hostname"
	standardSplitSep  = "-"

	defaultPodNum       = 10
	defaultPoolNum      = 13
	defaultLabelsLength = 5
	defaultRetryTimes   = 3

	podNameMinLength = 6

	defaultLabelLength      = 3
	defaultAnnotationLength = 1
	defaultPodGCTimeMin     = 5

	maxEffectiveEphemeralStorage   = 2048
	defaultRequestEphemeralStorage = 500
	minPodGCTime                   = 1
	maxPodGCTime                   = 60

	maxCreateDeployRetryTime = 5
	createDeployInterval     = 2 * time.Second
	maxDeleteDeployRetryTime = 3
	deleteDeployInterval     = 2 * time.Second

	allTerminatedTimeout = 10 * time.Minute
	defaultUpdateTimeout = 5 * time.Second

	mbToKb = 1024

	cpuIndex    = 1
	memoryIndex = 2

	rolloutKey = "kubectl.kubernetes.io/restartedAt"
)

var (
	poolMgr        *PoolMgr
	poolMgrGuard   sync.Mutex
	ssdRuntimeName = os.Getenv("SSD_RUNTIME_NAME")
	// ErrNoSuchStandard represents error no such standard
	ErrNoSuchStandard = errors.New("ErrNoSuchStandard")
	// ErrRequestExceedLimit represents error request exceed limited
	ErrRequestExceedLimit = errors.New("RequestExceedLimit")
	// ErrGetPodOnDemand represents error node creat pod exceed limited
	ErrGetPodOnDemand = errors.New("GetPodOnDemand")
	// ErrPodNotFound represents error not found any pod
	ErrPodNotFound = errors.New("NotFound")
	// ErrCancelGotPod cancel got pod
	ErrCancelGotPod = errors.New("CancelGotPod")
	// ErrFailedToCreatePodTempalte represents error that failed to create pod template
	ErrFailedToCreatePodTempalte = errors.New("failed to create pod template")
	// ErrGetSpecialTagPod represents error node creat pod exceed limited by special tag
	ErrGetSpecialTagPod = errors.New("failed to get special tag pod")
)

type (
	// nodeName is deploy nodeName e.g. node01
	nodeName string
	// standard pack AllocatableCPU-Memory-Language-NodeType e.g. 300-128-fusion-highend
	standard string
)

type podName struct {
	FullName     string
	PoolName     string
	Standard     standard
	ReplicaSetID string // k8s random string
	ID           string // k8s random string
}

func newPodName(full string) (podName, error) {
	list := strings.Split(full, standardSplitSep)
	if len(list) < podNameMinLength {
		return podName{}, errors.New("too short")
	}

	res := podName{}
	res.FullName = full
	res.PoolName = list[0]
	list = list[1:] // remove first segment
	res.ID = list[len(list)-1]
	list = list[:len(list)-1] // remove last segment
	res.ReplicaSetID = list[len(list)-1]
	list = list[:len(list)-1] // remove last segment

	// the rest are standard
	res.Standard = standard(strings.Join(list, standardSplitSep))
	return res, nil
}

// CreatePoolMgr creates pool manager
func CreatePoolMgr(workerClient *fhttp.Client, stopCh <-chan struct{}) error {
	kubeClient := utils.GetKubernetesClient()
	if kubeClient == nil {
		log.GetLogger().Errorf("failed to get kubeClient")
		return errors.New("failed to get kubeClient client")
	}
	cfg, err := config.GetConfig()
	if err != nil {
		return errors.New("failed to get config")
	}
	podDetector, err := detector.InitPodDetector()
	if err != nil {
		return err
	}

	podHandlerManager := createPodHandlerManager(kubeClient, cfg.PoolMgrConfig)

	poolMgr = &PoolMgr{
		customContainerMgr: newCustomContainerMGR(workerClient, kubeClient, cfg.PoolMgrConfig),
		checkpointPodMgr:   newCheckpointPodMGR(kubeClient, cfg.PoolMgrConfig),
		podDetector:        podDetector,
		podHandlerManager:  podHandlerManager,
		Config:             cfg.PoolMgrConfig,
		poolList:           nil,
		KubeClient:         kubeClient,
		PodUsedRecord:      newPodUsedRecord(),
		stopCh:             stopCh,
		synced:             make(chan struct{}),
		terminatingLongRun: make(map[string]*PodsOnNode),
	}
	go poolMgr.poolConfigHotLoadTask(stopCh)
	return nil
}

func createPodHandlerManager(kubeClient kubernetes.Interface, cfg config.PoolMgrConfig) *podhandler.Manager {
	m := podhandler.CreatePodHandlerManager()
	m.AddPodHandler(podhandler.CustomRuntimeXpu,
		podhandler.CreatePodHandler(podhandler.CustomRuntimeXpu, kubeClient, cfg))
	return m
}

// UpdatePoolManagerConfig update the pool manager config
func (p *PoolMgr) UpdatePoolManagerConfig(newConfig *config.FaasConfig) error {
	if newConfig == nil {
		return errors.New("new config is nil")
	}
	poolMgrGuard.Lock()
	defer poolMgrGuard.Unlock()

	p.Config = newConfig.PoolMgrConfig
	p.customContainerMgr.config = newConfig.PoolMgrConfig
	p.checkpointPodMgr.config = newConfig.PoolMgrConfig
	return nil
}

// GetPoolManager returns the pool manager
func GetPoolManager() *PoolMgr {
	return poolMgr
}

// GetPoolList return pool list
func (p *PoolMgr) GetPoolList() map[standard]*Pool {
	p.poolListLock.RLock()
	defer p.poolListLock.RUnlock()
	mCopy := make(map[standard]*Pool, len(p.poolList))
	for k, v := range p.poolList {
		mCopy[k] = v
	}
	return mCopy
}

// RestorePool Restore pool list
func (p *PoolMgr) RestorePool(newConfig *config.FaasConfig) {
	oldDecoyEnable := p.DecoyFileEnabled
	newDecoyEnable := newConfig.DecoyFileEnabled
	if oldDecoyEnable != newDecoyEnable {
		log.GetLogger().Infof("detect decoy file enable changed from %t to %t",
			oldDecoyEnable, newDecoyEnable)
		p.DecoyFileEnabled = newDecoyEnable
		err := p.processRestorePool()
		if err != nil {
			log.GetLogger().Errorf("failed to create pool: %v", err.Error())
		}
	}
	return
}

func (p *PoolMgr) processRestorePool() error {
	poolConfigList := p.splitPoolConfigListByName(constants.VIPUserType)
	uid := utils.GetWorkerManagerDeployUID()
	if uid == "" {
		return errors.New("empty worker-manager uid")
	}
	for i := 0; i < len(poolConfigList); i++ {
		poolConfig := poolConfigList[i]
		curPool := getPoolStandard(poolConfig)
		deployment := newPoolDeploymentBuilder(p.Config).build(curPool, uid)
		if deployment == nil {
			log.GetLogger().Errorf("failed to build deployment %s", curPool.Name)
			return fmt.Errorf("failed to build deployment %s", curPool.Name)
		}
		err := p.createPoolDeploy(curPool, deployment)
		// cae pool deployment create failed shouldn't block faas pool deployment create
		if err != nil && curPool.isCaePool {
			log.GetLogger().Errorf("create cae pool deployment failed:%s", err.Error())
			continue
		}
		if err != nil {
			log.GetLogger().Errorf("failed to create pool %s, with error: %s.", curPool.Name, err.Error())
			return err
		}
		curPool.scalingConfig = newScalingConfig(poolConfig.PoolSize, curPool.PoolSizeLimit, curPool.Standard)
	}
	// delete redundant pool
	p.deleteRedundantPool()

	return nil
}

// ProcessPoolSizeUpdate process PoolSize update
func (p *PoolMgr) ProcessPoolSizeUpdate() {
	if p.poolList == nil {
		log.GetLogger().Infof("poolList has not been created, return directly")
		return
	}

	newPoolConfigList := p.splitPoolConfigListByName(constants.VIPUserType)
	for _, newPoolConfig := range newPoolConfigList {
		curPoolStandard := standard(utils.PackPoolStandard(newPoolConfig.LimitCPU, newPoolConfig.LimitMemory,
			newPoolConfig.RunTimeName, newPoolConfig.CustomResources, newPoolConfig.Label))
		curPool, ok := p.getPoolByStandard(curPoolStandard)
		if !ok || curPool == nil || curPool.scalingConfig == nil {
			log.GetLogger().Infof("pool %s has not been created, skip", curPoolStandard)
			continue
		}
		oldPoolSize := curPool.PoolSize
		newPoolSize := newPoolConfig.PoolSize
		if newPoolSize < 0 {
			log.GetLogger().Warnf("skip to process PoolSize update, cur pool: %s, newPoolSize: %d",
				curPoolStandard, newPoolSize)
			continue
		}
		if oldPoolSize == newPoolSize {
			continue
		}
		log.GetLogger().Infof("start to process PoolSize update, cur pool: %s, oldPoolSize: %d, oldScalingReplicas:"+
			" %d, newPoolSize: %d", curPoolStandard, oldPoolSize, curPool.scalingConfig.replicas, newPoolSize)
		// update PoolSize in curPool
		curPool.mux.Lock()
		curPool.PoolSize = newPoolSize
		curPool.scalingConfig.basePoolSize = newPoolSize
		// avoid overwrite the replicas reconciled by autoscaling
		if newPoolSize > curPool.scalingConfig.replicas {
			log.GetLogger().Infof("new PoolSize: %d is larger than actual replicas: %d with standard: %s",
				newPoolSize, curPool.scalingConfig.replicas, curPool.Standard)
			curPool.scalingConfig.replicas = newPoolSize
			if err := p.updateDeployReplicas(curPool, newPoolSize); err != nil {
				log.GetLogger().Warnf("failed to update replicas: %d in pool deployment with standard: %s, err: %s",
					newPoolSize, curPool.Standard, err.Error())
			}
			log.GetLogger().Infof("succeed to update replicas: %d in pool deployment with standard: %s",
				newPoolSize, curPool.Standard)
		}
		curPool.mux.Unlock()
	}
}

// ProcessPoolScaleLimitUpdate process PoolScaleLimitUpdate event cased by config update or node list update
func (p *PoolMgr) ProcessPoolScaleLimitUpdate() {
	if p.poolList == nil {
		log.GetLogger().Infof("poolList has not been created, return directly")
		return
	}
	newPoolConfigList := p.splitPoolConfigListByName(constants.VIPUserType)
	for _, newPoolConfig := range newPoolConfigList {
		curPoolStandard := standard(utils.PackPoolStandard(newPoolConfig.LimitCPU, newPoolConfig.LimitMemory,
			newPoolConfig.RunTimeName, newPoolConfig.CustomResources, newPoolConfig.Label))
		curPool, ok := p.getPoolByStandard(curPoolStandard)
		if !ok || curPool == nil || curPool.scalingConfig == nil {
			log.GetLogger().Infof("pool %s has not been created, skip", curPoolStandard)
			continue
		}
		p.processPoolScaleLimitUpdate(newPoolConfig, curPool, curPoolStandard)
	}
}

// ProcessPoolSizeUpdate process PoolSize update
func (p *PoolMgr) processPoolScaleLimitUpdate(
	newPoolConfig config.PoolConfig, curPool *Pool, curPoolStandard standard) {
	if newPoolConfig.PoolScaleLimit < 0 {
		log.GetLogger().Warnf("skip to process PoolScaleLimit update, cur pool: %s, PoolScaleLimit: %f",
			curPoolStandard, newPoolConfig.PoolScaleLimit)
		return
	}
	oldPoolSizeLimit := curPool.PoolSizeLimit
	newPoolSizeLimit :=
		calculatePoolSizeLimit(newPoolConfig.LimitCPU, newPoolConfig.LimitMemory, newPoolConfig.PoolScaleLimit)
	if oldPoolSizeLimit == newPoolSizeLimit {
		return
	}
	log.GetLogger().Infof("process poolSizeLimit update, cur pool: %s, PoolScaleLimit: %f, "+
		"oldPoolSizeLimit: %d, newPoolSizeLimit: %d", curPoolStandard,
		newPoolConfig.PoolScaleLimit, oldPoolSizeLimit, newPoolSizeLimit)
	// update PoolSize in curPool
	curPool.mux.Lock()
	curPool.PoolSizeLimit = newPoolSizeLimit
	curPool.scalingConfig.poolSizeLimit = newPoolSizeLimit
	if curPool.scalingConfig.replicas > newPoolSizeLimit &&
		curPool.scalingConfig.replicas > curPool.scalingConfig.basePoolSize {
		log.GetLogger().Infof("actual replicas: %d is larger than PoolSizeLimit: %d with standard: %s",
			curPool.scalingConfig.replicas, newPoolSizeLimit, curPool.Standard)
		curPool.scalingConfig.replicas = MaxInt(newPoolSizeLimit, curPool.scalingConfig.basePoolSize)
		if err := p.updateDeployReplicas(curPool, curPool.scalingConfig.replicas); err != nil {
			log.GetLogger().Warnf("failed to update replicas: %d in pool deployment with standard: %s, err: %s",
				curPool.scalingConfig.replicas, curPool.Standard, err.Error())
		}
		log.GetLogger().Infof("succeed to update replicas: %d in pool deployment with standard: %s",
			curPool.scalingConfig.replicas, curPool.Standard)
	}
	curPool.mux.Unlock()

}

// GetPoolNum returns the pool number
func (p *PoolMgr) GetPoolNum() int64 {
	p.poolListLock.RLock()
	defer p.poolListLock.RUnlock()
	var poolNum int64
	if poolMgr != nil {
		for _, pool := range poolMgr.poolList {
			poolNum += pool.PodNum
		}
	}
	return poolNum
}

// Run starts node manager
func Run(stopCh <-chan struct{}) {
	if poolMgr != nil {
		err := poolMgr.Start(stopCh)
		if err != nil {
			log.GetLogger().Errorf("failed to start pool manager")
			return
		}
		log.GetLogger().Infof("pool manager has been started")
		if snapshotmanager.GetSnapshotManagerMode() {
			log.GetLogger().Infof("Snapshot manager skip poolMgr checkpointPodMgr/podDetector")
			return
		}
		go poolMgr.checkpointPodMgr.start()
		if poolMgr.podDetector != nil {
			poolMgr.podDetector.Run(stopCh)
		}
	}
}

// PodsOnNode cached pod router on node
type PodsOnNode struct {
	// key: podName --> val: instance router
	Pods map[string]commontypes.InstanceRouter
	sync.RWMutex
}

// PoolMgr manage pool and select a pod to use
type PoolMgr struct {
	customContainerMgr *customContainerMGR
	checkpointPodMgr   *checkpointPodMGR
	podDetector        *detector.PodDetector
	podHandlerManager  *podhandler.Manager
	PodUsedRecord      *PodUsedRecord
	poolListLock       sync.RWMutex
	poolList           map[standard]*Pool
	Config             config.PoolMgrConfig
	KubeClient         kubernetes.Interface

	// key: hostIP --> val: pod routers on that node
	terminatingLongRun     map[string]*PodsOnNode
	terminatingLongRunLock sync.RWMutex

	synced chan struct{}

	projectID        string
	projectName      string
	clusterID        string
	clusterName      string
	DecoyFileEnabled bool

	stopCh <-chan struct{}
}

// InitTerminatingLongRun init terminating long run cache when worker-manager become leader
func (p *PoolMgr) InitTerminatingLongRun(pods []corev1.Pod) {
	for i := range pods {
		if pods[i].Labels[utils.LabelLongRun] == "" {
			continue
		}
		p.addTerminatingLongRun(&pods[i])
	}
	close(p.synced)
}

// AddTerminatingLongRun check if pod is long-run and terminating, and add it to cache
func (p *PoolMgr) AddTerminatingLongRun(pod *corev1.Pod) {
	if pod.Labels[utils.LabelLongRun] == "" || pod.DeletionTimestamp == nil || pod.Status.HostIP == "" {
		return
	}
	p.addTerminatingLongRun(pod)
}

func (p *PoolMgr) addTerminatingLongRun(pod *corev1.Pod) {
	res := strings.Split(pod.Name, "-")
	if len(res) < memoryIndex+1 {
		log.GetLogger().Errorf("invalid pod name %s", pod.Name)
		return
	}
	// NOTICE: current stateful function do not support async long run, so ignore it
	p.terminatingLongRunLock.Lock()
	node, ok := p.terminatingLongRun[pod.Status.HostIP]
	if !ok {
		node = &PodsOnNode{Pods: make(map[string]commontypes.InstanceRouter, 1)}
		p.terminatingLongRun[pod.Status.HostIP] = node
	}
	p.terminatingLongRunLock.Unlock()

	funcName := utils.GetFullFuncNameByPod(pod)
	funcKeyWithRes := pod.Labels[utils.LabelTenantID] + "/" + funcName + "/" + pod.Labels[utils.LabelFunctionVersion] +
		"/" + res[cpuIndex] + "-" + res[memoryIndex]

	node.Lock()
	node.Pods[pod.Name] = commontypes.InstanceRouter{
		InstanceID:              commonutils.GenerateInstanceID(pod.Name),
		WorkerAddr:              pod.Status.PodIP + ":" + utils.P2pPort,
		NodeIP:                  pod.Status.HostIP,
		FunctionKeyWithResource: funcKeyWithRes,
		IsMinInstance:           pod.Labels[utils.LabelApplier] == constants.WorkerManagerApplier,
		IsDirectFunc:            pod.Labels[utils.LabelDirect] == "true",
		HasInitializer:          pod.Labels[utils.LabelHasInitializer] == "true",
	}
	node.Unlock()
}

// RemoveTerminatingLongRun remove terminating long run pod from cache when pod is actual deleted
func (p *PoolMgr) RemoveTerminatingLongRun(pod *corev1.Pod) {
	if pod.Labels[utils.LabelLongRun] == "" {
		return
	}
	if pod.Status.HostIP == "" {
		log.GetLogger().Warnf("hostIP of pod %s is empty", pod.Name)
		return
	}
	// delete from cache
	p.terminatingLongRunLock.Lock()
	node, ok := p.terminatingLongRun[pod.Status.HostIP]
	if !ok {
		p.terminatingLongRunLock.Unlock()
		return
	}

	node.Lock()
	delete(node.Pods, pod.Name)
	if len(node.Pods) == 0 {
		delete(p.terminatingLongRun, pod.Status.HostIP)
	}
	node.Unlock()
	p.terminatingLongRunLock.Unlock()
}

// QueryTerminatingLongRunByHostIP queries terminating long run pod by host IP
func (p *PoolMgr) QueryTerminatingLongRunByHostIP(hostIP string) map[string]commontypes.InstanceRouter {
	<-p.synced
	p.terminatingLongRunLock.RLock()
	node, ok := p.terminatingLongRun[hostIP]
	if !ok {
		p.terminatingLongRunLock.RUnlock()
		return nil
	}

	node.RLock()
	copied := make(map[string]commontypes.InstanceRouter, len(node.Pods))
	for k, v := range node.Pods {
		copied[k] = v
	}
	node.RUnlock()

	p.terminatingLongRunLock.RUnlock()

	return copied
}

func (p *PoolMgr) checkTerminatedReason(pod *corev1.Pod) {
	for i := range pod.Status.ContainerStatuses {
		if pod.Status.ContainerStatuses[i].LastTerminationState.Terminated != nil {
			if pod.Status.ContainerStatuses[i].LastTerminationState.Terminated.ExitCode != 0 {
				log.GetLogger().Warnf("pod %s container %s last terminated with code %d and reason %s", pod.Name,
					pod.Status.ContainerStatuses[i].Name,
					pod.Status.ContainerStatuses[i].LastTerminationState.Terminated.ExitCode,
					pod.Status.ContainerStatuses[i].LastTerminationState.Terminated.Reason)
			}
		}
		if pod.Status.ContainerStatuses[i].State.Terminated != nil {
			if pod.Status.ContainerStatuses[i].State.Terminated.ExitCode != 0 {
				log.GetLogger().Warnf("pod %s container %s terminated with code %d and reason %s", pod.Name,
					pod.Status.ContainerStatuses[i].Name,
					pod.Status.ContainerStatuses[i].State.Terminated.ExitCode,
					pod.Status.ContainerStatuses[i].State.Terminated.Reason)
			}
		}
	}
}

// AddPod will add pod into pool
func (p *PoolMgr) AddPod(pod *corev1.Pod) {
	if utils.IsDsWorker(pod) {
		handleDsWorkerPodEvent(pod)
		return
	}
	p.podDetector.Store(pod)
	p.AddTerminatingLongRun(pod)
	if utils.IsOnDemandPod(pod) {
		_, ok := strength.GetPodCache().GetState(pod.Name)
		// it will clean none-instance remaining pod when worker-manager is restarting
		// it will clean pod when it failed to create pod but k8s creates a delay pod
		if !ok && !utils.IsInstancePod(pod) && utils.IsPodReady(pod) {
			log.GetLogger().Warnf("it will delete pod %s because it is not processing specialized", pod.Name)
			utils.DeletePod(pod)
			// clean pod and return now
			return
		}
	}
	// avoid worker-manager delete checkpoint pods created by snapshot-manager
	if !utils.IsSnapShotModel() && utils.IsSnapshotManagerPod(pod) {
		log.GetLogger().Infof("the pod %s is create by snapshot-manager", pod.Name)
		return
	}

	if utils.IsCustomContainerPod(pod.Name) {
		p.customContainerMgr.updatePod(pod)
		return
	}
	if utils.IsCheckpointPod(pod) {
		p.checkpointPodMgr.updatePod(pod)
		return
	}

	if utils.IsNodeBalanceMigratedPod(pod) {
		notifyNodeBalanceMigratedPod(pod)
	}

	if utils.IsTmpExpansionPod(pod.Name) {
		utils.NotifyPodReadyStatus(pod)
		return
	}

	if !utils.IsPodReady(pod) {
		log.GetLogger().Warnf("onAdd pod %s isn't ready", pod.Name)
		return
	}

	p.podAddOperation(pod)
}

func (p *PoolMgr) podAddOperation(pod *corev1.Pod) {
	used, ok := pod.Annotations[usedLabelKey]
	if !ok {
		log.GetLogger().Debugf("pod %s without used label, which is ignored", pod.Name)
		return
	}

	standardOfPod, err := getStandardFromPodName(pod.Name)
	if err != nil {
		log.GetLogger().Warnf("podName of added pod %s doesn't contain standard", pod.Name)
		return
	}
	pool, ok := p.getPoolByStandard(standardOfPod)
	if !ok || pool == nil {
		log.GetLogger().Warnf("pool %s doesn't exist", standardOfPod)
		return
	}

	if used == utils.FalseString {
		pool.addPodToPool(pod)
	}
	monitoring.AddRatioNum(utils.TypePodNum)
}

// DeletePod will delete pod from pool
func (p *PoolMgr) DeletePod(pod *corev1.Pod) {
	p.podDetector.Delete(pod.Name)
	if utils.IsDsWorker(pod) {
		handleDsWorkerDeleteEvent(pod)
		return
	}
	p.checkTerminatedReason(pod)
	p.RemoveTerminatingLongRun(pod)
	if utils.IsOnDemandPod(pod) {
		strength.GetPodCache().DelState(pod.Name)
		log.GetLogger().Infof("ignore delete container pod: %s", pod.Name)
		return
	}

	podStandard, err := getStandardFromPodName(pod.Name)
	if err != nil {
		log.GetLogger().Warnf("podName of deleted pod %s doesn't contain standard", pod.Name)
		return
	}

	pool, ok := p.getPoolByStandard(podStandard)
	if !ok || pool == nil {
		log.GetLogger().Warnf("pool %s doesn't exist", podStandard)
		return
	}

	pool.deletePodFromPool(pod)
}

// checkAndForceDeleted force deletes pod that all contains has terminated for at lease allTerminatedTimeout,
// in order to cover some exceptions that pod cannot gc and keep terminating
func (p *PoolMgr) checkAndForceDeleted(pod *corev1.Pod) bool {
	if !utils.IsPodStuckTerminating(pod) {
		return false
	}
	go func() {
		log.GetLogger().Infof("start to force delete pod %s", pod.Name)
		err := utils.ForciblyDeletePod(pod)
		if err != nil {
			log.GetLogger().Warnf("failed to force delete pod %s", pod.Name)
		}
	}()
	return true
}

// UpdatePod will update pod in pool
func (p *PoolMgr) UpdatePod(pod *corev1.Pod) {
	if utils.IsDsWorker(pod) {
		handleDsWorkerPodEvent(pod)
		return
	}
	p.AddTerminatingLongRun(pod)
	if p.checkAndForceDeleted(pod) {
		return
	}
	p.podDetector.Store(pod)

	if p.checkPodReadyStatus(pod) {
		return
	}

	handler := p.podHandlerManager.FilterByPodName(pod.Name)
	if handler != nil {
		handler.UpdatePod(pod)
		return
	}

	// Check whether the pod is a worker pod based on the pod name prefix. If not, ignore it.
	podStandard, err := getStandardFromPodName(pod.Name)
	if err != nil {
		log.GetLogger().Debugf("podName of deleted pod %s doesn't contain standard", pod.Name)
		return
	}
	log.GetLogger().Debugf("updated pod(%s) from pool(%s)", pod.Name, podStandard)
	pool, ok := p.getPoolByStandard(podStandard)
	if !ok || pool == nil {
		log.GetLogger().Warnf("pool %s doesn't exist", podStandard)
		return
	}
	if !utils.IsPodReady(pod) {
		log.GetLogger().Debugf("onUpdate pod %s isn't ready", pod.Name)
		pool.deletePodFromPool(pod)
		return
	}

	used, ok := pod.Annotations[usedLabelKey]
	if !ok {
		log.GetLogger().Debugf("pod %s without used label, which is ignored", pod.Name)
		return
	}
	if used == utils.TrueString {
		pool.deletePodFromPool(pod)
	} else {
		if getOwnerKindForPod(pod) == faasAPIName {
			// prevent pod was configured labels by worker-manager fast than nodemanager
			// reAdd pod to pool
			return
		}
		pool.addPodToPool(pod)
	}
}

func (p *PoolMgr) checkPodReadyStatus(pod *corev1.Pod) bool {
	// avoid worker-manager delete checkpoint pods created by snapshot-manager
	if !utils.IsSnapShotModel() && utils.IsSnapshotManagerPod(pod) {
		log.GetLogger().Infof("the pod %s is create by snapshot-manager", pod.Name)
		return true
	}

	if utils.IsOnDemandPod(pod) {
		_, ok := strength.GetPodCache().GetState(pod.Name)
		// it will clean none-instance remaining pod when worker-manager is restarting
		// it will clean pod when it failed to create pod but k8s creates a delay pod
		if !ok && !utils.IsInstancePod(pod) && utils.IsPodReady(pod) {
			log.GetLogger().Warnf("it will delete pod %s because it is not processing specialized", pod.Name)
			utils.DeletePod(pod)
			// clean pod and return now
			return true
		}
	}
	if utils.IsCustomContainerPod(pod.Name) {
		p.customContainerMgr.updatePod(pod)
		return true
	}

	if utils.IsCheckpointPod(pod) {
		p.checkpointPodMgr.updatePod(pod)
		return true
	}

	if utils.IsNodeBalanceMigratedPod(pod) {
		notifyNodeBalanceMigratedPod(pod)
		return true
	}

	if utils.IsTmpExpansionPod(pod.Name) {
		utils.NotifyPodReadyStatus(pod)
		return true
	}
	return false
}

func getOwnerKindForPod(pod *corev1.Pod) string {
	if pod == nil || len(pod.ObjectMeta.OwnerReferences) <= 0 {
		return ""
	}

	return pod.ObjectMeta.OwnerReferences[0].Kind
}

func (p *PoolMgr) initProjectAndCluster() {
	projectID := os.Getenv(projectID)
	if projectID == "" {
		projectID = "default"
	}
	projectName := os.Getenv(projectName)
	if projectName == "" {
		projectName = "Default"
	}
	clusterID := os.Getenv(clusterID)
	if clusterID == "" {
		clusterID = "default"
	}
	clusterName := os.Getenv(clusterName)
	if clusterName == "" {
		clusterName = "Default"
	}
	p.projectID = projectID
	p.projectName = projectName
	p.clusterID = clusterID
	p.clusterName = clusterName
}

func useDefaultValueIfOptional(jsonValue, defaultValue int64) int64 {
	if jsonValue == 0 {
		return defaultValue
	}
	return jsonValue
}

func podRequestPriority(x, y interface{}) bool {
	pa, ok := x.(*common.PodRequest)
	if !ok {
		return false
	}
	pb, ok := y.(*common.PodRequest)
	if !ok {
		return false
	}
	if pa.Priority == pb.Priority {
		return pa.Timestamp < pb.Timestamp
	}
	return pa.Priority > pb.Priority
}

func getPoolStandard(poolConfig config.PoolConfig) *Pool {
	poolSize := poolConfig.PoolSize
	poolStandard := standard(utils.PackPoolStandard(poolConfig.LimitCPU, poolConfig.LimitMemory,
		poolConfig.RunTimeName, poolConfig.CustomResources, poolConfig.Label))
	pool := &Pool{
		Name: utils.PackPoolFullName(poolConfig.Name, poolConfig.LimitCPU, poolConfig.LimitMemory,
			poolConfig.RunTimeName, poolConfig.Label),
		NameSpace:        utils.FunctionNameSpace(),
		Standard:         poolStandard,
		Language:         poolConfig.RunTimeName,
		LimitCPU:         getCPULimit(poolConfig),
		RequestCPU:       poolConfig.RequestCPU,
		LimitMemory:      poolConfig.LimitMemory,
		RequestMemory:    poolConfig.RequestMemory,
		EphemeralStorage: useDefaultValueIfOptional(poolConfig.EphemeralStorage, defaultRequestEphemeralStorage),
		WorkerLimitCPU:   poolConfig.LimitCPU,
		LivenessProbePeriodSeconds: int32(useDefaultValueIfOptional(int64(poolConfig.LivenessProbePeriodSeconds),
			defaultLivenessProbePeriodSeconds)),
		LivenessProbeFailureThreshold: int32(useDefaultValueIfOptional(
			int64(poolConfig.LivenessProbeFailureThreshold), defaultLivenessProbeFailureThreshold)),
		PoolSize:                 poolSize,
		PodList:                  make(map[nodeName]*NodePod, defaultPodNum),
		Annotations:              generatePoolAnnotation(poolConfig.Annotations),
		mux:                      new(sync.Mutex),
		OndemandChan:             make(map[string]chan *corev1.Pod, utils.DefaultMapSize),
		NonIsolated:              poolConfig.NonIsolated,
		specialRuntimeManagerTag: poolConfig.SpecialRuntimeManagerTag,
		isCaePool:                poolConfig.IsCaePool,
	}

	if poolConfig.PoolScaleLimit > 0 {
		pool.PoolSizeLimit =
			calculatePoolSizeLimit(poolConfig.LimitCPU, poolConfig.LimitMemory, poolConfig.PoolScaleLimit)
	} else {
		pool.PoolSizeLimit = math.MaxInt32
	}
	pool.queue = schedulequeue.NewPriorityScheduleQueue(pool.Name, podRequestPriority, signals.WaitForSignal())
	pool.Label = generatePoolLabel(poolConfig.Label, pool.Standard)
	pool.queue.Start()
	return pool
}

func getCPULimit(poolConfig config.PoolConfig) int64 {
	cpuLimit := poolConfig.LimitCPU
	if poolConfig.ActualCPU > 0 {
		cpuLimit = poolConfig.ActualCPU
	}
	return cpuLimit
}

func calculatePoolSizeLimit(cpu int64, memory int64, proportion float64) int32 {
	if proportion == 0 {
		return math.MaxInt32
	}

	info := cache.GetTotalNodeInfoWithFilters(cache.WithLabelValueIn(constants.NodeRoleLabelKey,
		constants.GetValidNodeRoleValues()...))

	cpuResult := int32(math.Floor(float64(info.CPUTotal) * proportion / float64(cpu)))
	memResult :=
		int32(math.Floor(float64(info.MemoryTotal) * proportion / (float64(memory) * mbToKb)))

	log.GetLogger().Infof("poolSizeLimit | cpuTotal %d | memTotal %d | cpu %d | mem %d | cpuResult %d | memResult %d",
		info.CPUTotal, info.MemoryTotal, cpu, memory, cpuResult, memResult)
	return MinInt(cpuResult, memResult)
}

func generatePoolAnnotation(annotation map[string]string) map[string]string {
	if annotation == nil {
		annotation = make(map[string]string, defaultAnnotationLength)
	}
	annotation[usedLabelKey] = utils.FalseString
	return annotation
}

func generatePoolLabel(label map[string]string, standard standard) map[string]string {
	if label == nil {
		label = make(map[string]string, defaultLabelLength)
	}
	label[utils.IsPoolPodLabelKey] = utils.TrueString
	label[coldStartPoolLabelKey] = utils.TrueString
	label[utils.StandardLabelKey] = string(standard)
	return label
}

func getLabelDiffFlag(curPool *Pool) bool {
	for k, v := range curPool.Label {
		if curPool.Deployment.ObjectMeta.Labels[k] != v {
			log.GetLogger().Infof("objectMeta.Labels %s=%s", curPool.Deployment.ObjectMeta.Labels[k], v)
			return true
		}
	}
	return false
}

func (p *PoolMgr) splitPoolConfigListByName(runtimeName string) []config.PoolConfig {
	result := make([]config.PoolConfig, 0, defaultPoolNum)
	poolConfigList := p.Config.PoolList.Pool

	for index := range poolConfigList {
		poolConfigList[index].SpecialRuntimeManagerTag = p.Config.PoolContainerTags.SpecialRuntimeImageTag
	}

	for _, poolConfig := range poolConfigList {
		if poolConfig.RunTimeName == runtimeName {
			continue
		}
		result = append(result, poolConfig)
	}
	return result
}

func (p *PoolMgr) createPool(stopCh <-chan struct{}) error {
	alarm.GetClient().RegisterAlarmType(alarm.PoolDeployment)
	poolConfigList := p.splitPoolConfigListByName(constants.VIPUserType)
	p.poolList = make(map[standard]*Pool, defaultPoolNum)
	faasConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("fail to get faas config")
		return err
	}
	enableProactiveModeConfig := faasConfig.ProactiveEnableList
	uid := utils.GetWorkerManagerDeployUID()
	if uid == "" {
		return errors.New("empty worker-manager uid")
	}
	for i := 0; i < len(poolConfigList); i++ {
		poolConfig := poolConfigList[i]
		curPool := getPoolStandard(poolConfig)
		deployment := newPoolDeploymentBuilder(p.Config).build(curPool, uid)
		if deployment == nil {
			log.GetLogger().Errorf("failed to build deployment %s", curPool.Name)
			return fmt.Errorf("failed to build deployment %s", curPool.Name)
		}
		err = p.createPoolDeployByRetry(curPool, deployment, stopCh)
		// cae pool deployment create failed shouldn't block faas pool deployment create
		if err != nil && curPool.isCaePool {
			log.GetLogger().Errorf("create cae pool deployment failed:%s", err.Error())
			continue
		}
		curPool.scalingConfig = newScalingConfig(poolConfig.PoolSize, curPool.PoolSizeLimit, curPool.Standard)
		p.PodUsedRecord.usedPodRecordVal[curPool.Standard] = 0
		// if pool can not be predicted accurately, turn off proactive operation
		proactiveModePool, ok := enableProactiveModeConfig[string(curPool.Standard)]
		if ok && proactiveModePool {
			curPool.scalingConfig.proactiveMode = 1
		}
		// apply scaling plan to every pool
		go curPool.scalingConfig.applyScalingPlanPeriodically(p.stopCh, curPool.Standard)
		// reset scaling up counter if enable fusion policy
		go curPool.scalingConfig.resetScalingUpCounter(p.stopCh, curPool.Standard)
		// pull up monitor goroutine
		go curPool.monitorPoolReplicas(p.stopCh)
		go curPool.scalingConfig.monitorScalingDown(p.stopCh)
	}
	// receive pod generation record and update counter
	go p.PodUsedRecord.updateUsedPodRecord(p.stopCh)
	// reset used pod record every minute
	go p.PodUsedRecord.uploadAndResetUsedPodRecordEveryMinute(p.stopCh)
	// get prediction result from redis e.g. every 10min
	go p.generateScalingPlanPeriodically(p.stopCh)
	// report normal alarm when all pool deployments existing
	alarm.GetClient().ClearAlarm(alarm.PoolDeployment)
	// delete redundant pool
	p.deleteRedundantPool()

	return nil
}

func (p *PoolMgr) deleteRedundantPool() {
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("snapshot manager mode, skip deleteRedundantPool")
		return
	}
	poolDeployList, err := utils.ListDeployByK8S(utils.GetKubernetesClient(), metav1.ListOptions{
		LabelSelector: "isPoolPod=true",
	})
	if err != nil {
		log.GetLogger().Errorf("failed to list all pool deployment with error: %s", err.Error())
		return
	}
	for _, poolDeploy := range poolDeployList.Items {
		poolStandard := standard(poolDeploy.Spec.Selector.MatchLabels[utils.StandardLabelKey])
		if _, ok := p.getPoolByStandard(poolStandard); !ok {
			log.GetLogger().Infof("begin to delete redundant pool: %s", poolDeploy.Name)
			err := p.deleteRedundantPoolInternal(poolDeploy.Name)
			if err != nil {
				log.GetLogger().Errorf("failed to delete redundant pool %s with error: %s",
					poolDeploy.Name, err.Error())
			}
		}
	}
}

func (p *PoolMgr) deleteRedundantPoolInternal(poolName string) error {
	var err error
	for retryTime := 0; retryTime < maxDeleteDeployRetryTime; retryTime++ {
		err = utils.DeleteDeployByK8S(utils.GetKubernetesClient(), poolName)
		if err == nil {
			log.GetLogger().Debugf("succeed to delete redundant pool %s", poolName)
			break
		}
		time.Sleep(deleteDeployInterval)
	}
	return err
}

func (p *PoolMgr) getDeployment(pool *Pool) *appsv1.Deployment {
	workerManagerDeploy, err := p.KubeClient.AppsV1().
		Deployments("default").
		Get(context.TODO(), "worker-manager", metav1.GetOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to get worker manager deploy: %s", err.Error())
		return nil
	}
	return newPoolDeploymentBuilder(p.Config).build(pool, workerManagerDeploy.UID)
}

func (p *PoolMgr) createPoolDeployByRetry(curPool *Pool, deployment *appsv1.Deployment, stopCh <-chan struct{}) error {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return errors.New("stop channel is nil")
	}
	var err error
	retryTime := 0
	for {
		select {
		case <-stopCh:
			return errors.New("stop channel closed")
		default:
			if retryTime >= maxCreateDeployRetryTime {
				// report abnormal alarm when update pool deployment failed
				alarmInfo := fmt.Sprintf("error msg=failed to update pool deployment with standard: %s,",
					string(curPool.Standard))
				alarm.GetClient().ReportAlarm(alarm.PoolDeployment, alarmInfo)
				if curPool.isCaePool {
					return fmt.Errorf("create cae pool deployment reach failed threshold:%s", err.Error())
				}
			}
			err = p.createPoolDeploy(curPool, deployment)
			if err != nil {
				retryTime++
				time.Sleep(createDeployInterval)
				continue
			}
			// succeed to create pool deployment
			return nil
		}
	}
}

func (p *PoolMgr) createPoolDeploy(curPool *Pool, deployment *appsv1.Deployment) error {
	var err error
	curPool.Deployment, err = p.KubeClient.AppsV1().Deployments(curPool.NameSpace).Get(context.TODO(),
		curPool.Name, metav1.GetOptions{})
	if err == nil {
		if snapshotmanager.GetSnapshotManagerMode() {
			log.GetLogger().Infof("snapshot manager skip modify pool deployment %s", curPool.Name)
			p.setPoolByStandard(curPool.Standard, curPool)
			return nil
		}
		if getLabelDiffFlag(curPool) {
			if err = p.deletePoolDeploy(curPool); err != nil && !k8serror.IsNotFound(err) {
				log.GetLogger().Errorf("failed to delete deployment %s error: %s", curPool.Name, err.Error())
				return err
			}
			log.GetLogger().Infof("delete deployment %s in kubernetes successfully", curPool.Name)
		}
		if err = p.updatePoolDeploy(curPool, deployment); err != nil {
			log.GetLogger().Errorf("failed to update deployment: %s error %s", deployment.Name, err.Error())
			return err
		}
		if pool, ok := p.getPoolByStandard(curPool.Standard); ok && pool != nil {
			pool.Deployment = deployment
			return nil
		}
		// pool is created successfully
		p.setPoolByStandard(curPool.Standard, curPool)
		return nil
	}
	curPool.Deployment, err = p.KubeClient.AppsV1().Deployments(curPool.NameSpace).Create(context.TODO(),
		deployment, metav1.CreateOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to create deployment %s error: %s", deployment.Name, err.Error())
		return err
	}
	// pool is created successfully
	p.setPoolByStandard(curPool.Standard, curPool)
	log.GetLogger().Infof("succeed to create deployment for standard: %s", curPool.Standard)

	return nil
}

func (p *PoolMgr) updatePoolDeploy(curPool *Pool, deployment *appsv1.Deployment) error {
	var err error
	log.GetLogger().Debugf("deployment: %s already exists, then begin to update", deployment.Name)
	curPool.Deployment.ObjectMeta.Labels = curPool.Label
	if val, ok := curPool.Deployment.Spec.Template.Annotations[rolloutKey]; ok {
		deployment.Spec.Template.Annotations[rolloutKey] = val
	}
	if !equality.Semantic.DeepDerivative(curPool.Deployment.Spec, deployment.Spec) {
		curPool.Deployment.Spec = deployment.Spec
		curPool.Deployment.Spec.Template.Spec.HostAliases = p.Config.PoolList.HostAliases
		if curPool.Deployment, err = p.KubeClient.AppsV1().Deployments(curPool.NameSpace).Update(context.TODO(),
			curPool.Deployment, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}
	log.GetLogger().Debugf("deployment: %s succeeds to update", deployment.Name)
	return nil
}

func (p *PoolMgr) deletePoolDeploy(curPool *Pool) error {
	if err := p.KubeClient.AppsV1().Deployments(curPool.NameSpace).
		Delete(context.TODO(), curPool.Name, metav1.DeleteOptions{}); err != nil {
		return err
	}
	return nil
}

// Start function initialize pool
func (p *PoolMgr) Start(stopCh <-chan struct{}) error {
	p.initProjectAndCluster()
	if err := p.createPool(stopCh); err != nil {
		log.GetLogger().Errorf("failed to create pool with error: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("%d pool deployments are created or updated successfully", len(p.Config.PoolList.Pool))

	return nil
}

// GetPod returns pod according to PodRequest
func (p *PoolMgr) GetPod(ctx context.Context, spec *faasfunction.FunctionServiceSpec, req common.PodRequest,
) (*corev1.Pod, error) {
	startTime := time.Now()
	log.GetLogger().Infof("get pod with request %+v", req)
	var demandNodes []string
	if len(req.NodeLabel) != 0 {
		demandNodes = cache.GetNodesByLabel(req.NodeLabel)
	}
	var err error
	req.CustomResource, err = decodeCustomResources(spec.Configuration.Resource.CustomResources)
	if err != nil {
		return nil, err
	}

	podCreateReq := &podhandler.PodCreateReq{
		Spec:        spec,
		Ctx:         ctx,
		PodReq:      &req,
		DemandNodes: demandNodes,
	}

	pod, _, err := p.selectPod(podCreateReq)
	if err != nil {
		return nil, err
	}

	log.GetLogger().Debugf("succeed to get pod %s from node manager, cost time: %v with traceID: %s",
		pod.Name, time.Now().Sub(startTime), req.TraceID)
	go p.updatePodLabels(pod, podCreateReq.PodReq.EnableTMPExpansion)
	return pod, nil
}

func (p *PoolMgr) reclaimUnusedSelectedPod(pod *corev1.Pod, enableTMPExpansion bool) {
	podStandard, err := getStandardFromPodName(pod.Name)
	if err != nil {
		log.GetLogger().Warnf("podName of added pod %s doesn't contain standard", pod.Name)
		return
	}
	pool, ok := p.getPoolByStandard(podStandard)
	if !ok || pool == nil {
		log.GetLogger().Warnf("pool %s doesn't exist", podStandard)
		return
	}
	pool.mux.Lock()
	pool.reclaimUnusedSelectedPod(pod, enableTMPExpansion)
	pool.mux.Unlock()
}

func (p *PoolMgr) createMigratePod(req *podhandler.PodCreateReq) (*corev1.Pod, bool, error) {
	// custom container needs swr-addon, we can't create it directly by pod template
	if utils.IsCustomContainerSpec(req.Spec) {
		pod, err := p.customContainerMgr.createMigratePod(
			createCustomContainerPodParam{
				spec: req.Spec,
				ctx:  req.Ctx,
				req:  *req.PodReq,
			})
		return pod, false, err
	}

	var availableNodes map[string]*cache.NodeInfo

	// check target node for temp-expansion
	if req.PodReq.EnableTMPExpansion {
		demandNodes := utils.ParseDemandNodes(req.PodReq.PodTemplate)
		if len(demandNodes) == 0 {
			return nil, false, errors.New("no demand node found")
		}
		availableNodes, _ = cache.GetAvailableNodesForTMPExpansion()
		if len(availableNodes) == 0 {
			return nil, false, snerror.New(snerror.InsufficientEphemeralStorage,
				snerror.ErrText(snerror.InsufficientEphemeralStorage))
		}
		if _, ok := availableNodes[demandNodes[0]]; !ok {
			go cache.ReduceNodeStorage(availableNodes, "")
			return nil, false, errors.New("insufficient ephemeralStorage for migrate")
		}
	}

	pod, err := utils.CreatePod(req.Ctx, &utils.CreatePodTemplate{
		TraceID:    req.PodReq.TraceID,
		Spec:       &req.PodReq.PodTemplate.Spec,
		ObjectMeta: &req.PodReq.PodTemplate.ObjectMeta,
	})

	if req.PodReq.EnableTMPExpansion {
		if err != nil {
			go cache.ReduceNodeStorage(availableNodes, "")
		} else {
			go cache.ReduceNodeStorage(availableNodes, pod.Spec.NodeName)
		}
	}

	return pod, false, err
}

func (p *PoolMgr) selectPod(req *podhandler.PodCreateReq) (*corev1.Pod, bool, error) {
	if req.PodReq.PodTemplate != nil {
		return p.createMigratePod(req)
	}

	handler := p.podHandlerManager.FilterByPodCreateReq(req)
	if handler != nil {
		pod, err := p.createPodByPodHandler(handler, req)
		return pod, false, err
	}
	log.GetLogger().Infof("IsCustomContainerSpec:%t, UseCaePoolPod:%t, IsVpaFeatureEnabled:%t, "+
		"FilterCheckpointCustom:%t", utils.IsCustomContainerSpec(req.Spec), utils.UseCaePoolPod(req.Spec),
		utils.IsVpaFeatureEnabled(), FilterCheckpointCustom(*req.PodReq))
	if utils.IsCustomContainerSpec(req.Spec) &&
		(!utils.UseCaePoolPod(req.Spec) || !utils.IsVpaFeatureEnabled() || FilterCheckpointCustom(*req.PodReq)) {
		return p.creatCustomPod(req)
	}

	// checkpoint non custom container
	if p.checkpointPodMgr.filter(*req.PodReq) {
		return p.getCheckPointPod(req)
	}

	// snapshot-manager handle abnormal snapshot data
	if snapshotmanager.GetSnapshotManagerMode() {
		if req.PodReq.SnapshotDeployment == nil || req.PodReq.SnapshotDeployment.BucketID == "" {
			return nil, false, snerror.New(snerror.CancelCheckpoint, "No SnapshotDeployment Or BucketId is empty Error")
		}
	}

	pod, ok, err := p.getPodFromPool(req)
	if err == nil && pod != nil && utils.IsCaePoolPod(pod) {
		etcdhandler.PutInitInstanceToCAERouterEtcd(req.Ctx, pod, req.Spec)
	}
	return pod, ok, err
}

func (p *PoolMgr) getPodFromPool(req *podhandler.PodCreateReq) (*corev1.Pod, bool, error) {
	if utils.UseCaePoolPod(req.Spec) && isNeedMountFile(req.Spec.Configuration.FileMountConfig) {
		err := checkMountFile(req.Spec.Configuration.FileMountConfig)
		if err != nil {
			return nil, false, err
		}
	}
	// get pool pod
	pod, err := p.getPoolPod(req)
	if err != nil && utils.UseCaePoolPod(req.Spec) && utils.IsCustomContainerSpec(req.Spec) {
		log.GetLogger().Warnf("get cae pool pod failed:%s, try create custom pod", err.Error())
		return p.creatCustomPod(req)
	}
	// when EnableTMPExpansion is enabled, no matching pod is found from the pool, try to create a pod
	if req.PodReq.EnableTMPExpansion && pod == nil {
		return p.createTMPExpansionPod(req)
	}
	return pod, true, err
}

func (p *PoolMgr) getCheckPointPod(req *podhandler.PodCreateReq) (*corev1.Pod, bool, error) {
	// schedule to high end pool by default policy
	pool := p.getPoolFromPoolList(*req.PodReq, constants.FusionLanguage, req.PodReq.CustomResource, nil)
	if pool == nil {
		log.GetLogger().Warnf("no such standard for standard %s with traceID: %s",
			fmtStandardInfo(req.PodReq.CPU, req.PodReq.Mem, constants.FusionLanguage), req.PodReq.TraceID)
		return nil, false, ErrNoSuchStandard
	}
	pod, err := p.checkpointPodMgr.createPod(req.Ctx, pool, *req.PodReq, req.Spec.Configuration.BusinessType)
	return pod, false, err
}

func (p *PoolMgr) creatCustomPod(req *podhandler.PodCreateReq) (*corev1.Pod, bool, error) {
	// schedule to high end pool by default policy
	pool := p.getPoolFromPoolList(*req.PodReq, constants.FusionLanguage, req.PodReq.CustomResource, nil)
	if pool == nil && commonutils.IsCAEFunc(req.Spec.Configuration.BusinessType) {
		cfg, _ := config.GetConfig()
		specKey := fmt.Sprintf("pool-%d-%d", req.PodReq.CPU, req.PodReq.Mem)
		if poolSpec, ok := cfg.CaeCustomizePoolSpec[specKey]; ok {
			pool = &Pool{
				LimitCPU:               poolSpec.LimitCPU,
				LimitMemory:            poolSpec.LimitMEM,
				RequestCPU:             poolSpec.RequestCPU,
				RequestMemory:          poolSpec.RequestMEM,
				WorkerLimitCPU:         poolSpec.LimitCPU,
				EphemeralStorage:       poolSpec.EphemeralStorage,
				CaeCustomizePoolEnable: true,
			}
		}
	}
	if pool == nil {
		log.GetLogger().Errorf("no such standard for standard %s with traceID: %s",
			fmtStandardInfo(req.PodReq.CPU, req.PodReq.Mem, constants.FusionLanguage), req.PodReq.TraceID)
		return nil, false, ErrNoSuchStandard
	}
	pod, err := p.customContainerMgr.createPod(
		createCustomContainerPodParam{
			pool:           pool,
			spec:           req.Spec,
			ctx:            req.Ctx,
			req:            *req.PodReq,
			customResource: req.PodReq.CustomResource,
			demandNodes:    req.DemandNodes,
		})
	return pod, false, err
}

func (p *PoolMgr) getPoolPod(req *podhandler.PodCreateReq) (*corev1.Pod, error) {
	profile := cache.WorkerCache.GetProfile(req.PodReq.FunSvcID)
	labels := utils.GetFilterByFuncProfile(profile)
	forceDeploy := req.PodReq.ForceDeploy
	if utils.UseCaePoolPod(req.Spec) {
		labels["poolType"] = "cae"
		req.PodReq.ForceDeploy = false
	}
	log.GetLogger().Debugf("begin to choose pod for function %s with pool labels %v based on "+
		"func profiling with traceID: %s", req.PodReq.FuncName, labels, req.PodReq.TraceID)
	pod, err := p.getPodFromPoolList(req.Ctx, *req.PodReq, labels, req)
	if utils.IsNeedRetryScheduling(err, labels) {
		log.GetLogger().Warnf("failed to choose pod for function %s with pool labels %v "+
			"with traceID: %s, try in default pool", req.PodReq.FuncName, labels, req.PodReq.TraceID)
		labels[utils.LabelNodeType] = utils.DefaultNodeType
		pod, err = p.getPodFromPoolList(req.Ctx, *req.PodReq, labels, req)
	}
	req.PodReq.ForceDeploy = forceDeploy
	return pod, err
}

func (p *PoolMgr) createTMPExpansionPod(req *podhandler.PodCreateReq) (*corev1.Pod, bool, error) {
	log.GetLogger().Infof("there is no pool pod that meets the requirements, try to create a tmp expansion "+
		"pod with traceID: %s", req.PodReq.TraceID)
	// schedule to high end pool by default policy
	pool := p.getPoolFromPoolList(*req.PodReq, constants.FusionLanguage, req.PodReq.CustomResource, nil)
	if pool == nil {
		log.GetLogger().Warnf("no such standard for standard %s with traceID: %s",
			fmtStandardInfo(req.PodReq.CPU, req.PodReq.Mem, constants.FusionLanguage), req.PodReq.TraceID)
		return nil, false, ErrNoSuchStandard
	}
	availableNodes, _ := cache.GetAvailableNodesForTMPExpansion()
	if len(availableNodes) == 0 {
		log.GetLogger().Warnf("currently all nodes have insufficient ephemeral storage resources, "+
			"traceID: %s", req.PodReq.TraceID)
		// report insufficient resource alarm
		errMsg := fmt.Sprintf("error msg=specialize failed due to insufficient ephemeral storage, "+
			"available nodes nums: %d, pool pod nums: %d, traceID: %s, standard: %s",
			len(availableNodes), pool.PodNum, req.PodReq.TraceID, pool.Standard)
		alarm.GetClient().ReportAlarm(alarm.InsufficientEphemeralStorage, errMsg)
		return nil, false, snerror.New(snerror.InsufficientEphemeralStorage,
			snerror.ErrText(snerror.InsufficientEphemeralStorage))
	}
	podTemplate := podtemplate.CreateTmpExpansionPodTemplate(pool.Deployment.Spec.Template.Spec.DeepCopy(),
		pool.Deployment.Spec.Template.ObjectMeta.DeepCopy(), req, cache.GetNodeNameList(availableNodes))
	if podTemplate == nil {
		log.GetLogger().Warnf("failed to create tmp expansion pod template with traceID: %s",
			req.PodReq.TraceID)
		go cache.ReduceNodeStorage(availableNodes, "")
		return nil, false, ErrFailedToCreatePodTempalte
	}
	pod, err := utils.CreatePod(req.Ctx, podTemplate)
	if err != nil {
		go cache.ReduceNodeStorage(availableNodes, "")
		return nil, false, err
	}
	go cache.ReduceNodeStorage(availableNodes, pod.Spec.NodeName)
	return pod, false, err
}

func (p *PoolMgr) createPodByPodHandler(handler podhandler.PodHandler,
	req *podhandler.PodCreateReq) (*corev1.Pod, error) {
	pool := p.getPoolFromPoolList(*req.PodReq, constants.FusionLanguage, req.PodReq.CustomResource, nil)
	if pool == nil {
		log.GetLogger().Warnf("no such standard for standard %s with traceID: %s",
			fmtStandardInfo(req.PodReq.CPU, req.PodReq.Mem, constants.FusionLanguage), req.PodReq.TraceID)
		return nil, ErrNoSuchStandard
	}
	return handler.CreatePod(req, &podhandler.PodTemplate{
		Spec: pool.Deployment.Spec.Template.Spec.DeepCopy(),
		Meta: pool.Deployment.Spec.Template.ObjectMeta.DeepCopy(),
	})
}

func fmtStandardInfo(cpu, mem int64, language string) string {
	return fmt.Sprintf("cpu: %d, mem: %d, language: %s", cpu, mem, language)
}

func (p *PoolMgr) getPodInternal(ctx context.Context, pool *Pool, req common.PodRequest,
	createReq *podhandler.PodCreateReq) (*corev1.Pod, error) {
	if pool == nil {
		log.GetLogger().Warnf("no such standard for standard %s with traceID: %s",
			fmtStandardInfo(req.CPU, req.Mem, constants.FusionLanguage), req.TraceID)
		return nil, errors.New("pool is nil")
	}
	if createReq.Spec.Configuration.IsUsingSpecificTag {
		return pool.getPodInternalBySpecialTag(ctx, createReq.DemandNodes, req,
			atomic.LoadUint32(&p.Config.DisablePriorityQueue) == 0)
	}
	return pool.getPodInternal(ctx, createReq.DemandNodes, req, p.Config.ReservedAntiAffinity,
		atomic.LoadUint32(&p.Config.DisablePriorityQueue) == 0)
}

func (p *PoolMgr) getPodFromPoolList(ctx context.Context, req common.PodRequest,
	labels map[string]string, createReq *podhandler.PodCreateReq) (*corev1.Pod, error) {
	// NOTE: demand nodes is assigned by busproxy, it is weak demand
	// pool labels is learned by func profiling, it is strong demand
	// prefer to use demand nodes if it exist in our target pool, otherwise will ignore demand nodes
	customResource := createReq.PodReq.CustomResource
	pool := p.getPoolFromPoolList(req, constants.FusionLanguage, customResource, labels)
	if pool == nil {
		log.GetLogger().Warnf("no such standard for standard %s with traceID: %s",
			fmtStandardInfo(req.CPU, req.Mem, constants.FusionLanguage), req.TraceID)
		return nil, ErrNoSuchStandard
	}
	if pool.scalingConfig != nil {
		atomic.AddInt32(&pool.scalingConfig.waitingGeneralizedReqNums, 1)
		monitoring.AddRatioNum(utils.TypeRequestNum)
		pool.scalingConfig.considerScaleUp(req, labels)
		defer atomic.AddInt32(&pool.scalingConfig.waitingGeneralizedReqNums, -1)
	}
	var (
		pod *corev1.Pod
		err error
	)
	if len(customResource) != 0 {
		pod, err = pool.getPodInternalWithCustomResource(customResource)
	} else {
		pod, err = p.getPodInternal(ctx, pool, req, createReq)
	}
	if err != nil {
		log.GetLogger().Warnf("get pod internal error: %s with traceID: %s", err.Error(), req.TraceID)
		return nil, err
	}
	log.GetLogger().Infof("succeed to chosen pod %s, deployed node %s in pool(%s), and the number of remaining "+
		"pods is %d for func: %s with traceID: %s", pod.Name, pod.Spec.NodeName, pool.string(), pool.PodNum,
		req.FuncName, req.TraceID)
	select {
	// when received stop signal, skip sending used pod record
	// avoid filling cache channel fully
	case <-signals.WaitForSignal():
	default:
		currentReplica := atomic.LoadInt32(&pool.scalingConfig.replicas)
		log.GetLogger().Infof("pool name: %s, current pool size: %d, remaining number: %d", pool.Standard,
			currentReplica, pool.PodNum)
		usedPodRecordChanEle := &podUsedRecordChanElement{
			poolStandard: pool.Standard,
			usedNumber:   int64(currentReplica) - pool.PodNum,
		}
		p.PodUsedRecord.UsedPodRecordChan <- usedPodRecordChanEle
	}

	return pod, nil
}

func (p *PoolMgr) getPoolFromPoolList(req common.PodRequest, language string, customResource map[string]int64,
	labels map[string]string) *Pool {
	cpu := req.CPU
	mem := req.Mem
	podScheduleInfo := cache.WorkerCache.GetScheduleLabelNodes(req.FunSvcID)
	if podScheduleInfo.MatchedSSDPoolLabel {
		language = ssdRuntimeName
	}
	poolStandard := utils.PackPoolStandard(cpu, mem, language, customResource, labels)
	log.GetLogger().Infof("get pool from poolList with poolStandard %s, traceID: %s", poolStandard, req.TraceID)
	pool, exist := p.getPoolByStandard(standard(poolStandard))
	if !exist && len(customResource) != 0 {
		// if there is no pool with custom resource, try standard pool without custom resource
		pool, exist = p.getPoolByStandard(standard(utils.PackPoolStandard(cpu, mem, language, nil, labels)))
	}
	if !exist {
		return nil
	}
	return pool
}

func (p *PoolMgr) getPoolByStandard(poolStandard standard) (*Pool, bool) {
	p.poolListLock.RLock()
	defer p.poolListLock.RUnlock()
	if p.poolList == nil {
		return nil, false
	}
	pool, exist := p.poolList[poolStandard]
	return pool, exist
}

func (p *PoolMgr) setPoolByStandard(poolStandard standard, pool *Pool) {
	p.poolListLock.Lock()
	defer p.poolListLock.Unlock()
	if p.poolList == nil {
		return
	}
	p.poolList[poolStandard] = pool
}

func (p *PoolMgr) getStateSpecs() []commontypes.StateCreateOptions {
	p.poolListLock.RLock()
	defer p.poolListLock.RUnlock()
	var specs []commontypes.StateCreateOptions
	for _, pool := range p.poolList {
		specs = append(specs, commontypes.StateCreateOptions{
			CPU:    pool.LimitCPU,
			Memory: pool.LimitMemory,
		})
	}
	return specs
}

// GetStatefulDynamicClosestStandard get the closest standard by input cpu and memory
// choose maximum standard between two resources
func (p *PoolMgr) GetStatefulDynamicClosestStandard(cpu, mem int64) (int64, int64) {
	if cpu == 0 && mem == 0 {
		return cpu, mem
	}
	specs := p.getStateSpecs()
	if len(specs) == 0 {
		return 0, 0
	}
	sort.Slice(specs, func(i, j int) bool {
		if specs[i].Memory == specs[j].Memory {
			return specs[i].CPU < specs[j].CPU
		}
		return specs[i].Memory < specs[j].Memory
	})
	findSpecIndex := func(target int64, isCPU bool) int {
		index := -1
		for i, spec := range specs {
			if isCPU && target <= spec.CPU || !isCPU && target <= spec.Memory {
				index = i
				break
			}
		}
		if index < 0 || index > len(specs)-1 {
			index = len(specs) - 1
		}
		return index
	}
	specCPUIndex := findSpecIndex(cpu, true)
	specMemIndex := findSpecIndex(mem, false)
	if specCPUIndex < 0 || specCPUIndex >= len(specs) || specMemIndex < 0 || specMemIndex >= len(specs) {
		return 0, 0
	}
	if specs[specMemIndex].CPU >= specs[specCPUIndex].CPU && specs[specMemIndex].Memory >= specs[specCPUIndex].Memory {
		return specs[specMemIndex].CPU, specs[specMemIndex].Memory
	}
	return specs[specCPUIndex].CPU, specs[specCPUIndex].Memory
}

func (p *PoolMgr) updatePodLabels(pod *corev1.Pod, enableTMPExpansion bool) error {
	t := time.Now()
	var patchBytes []byte
	if !enableTMPExpansion {
		patchBytes = []byte(`{"metadata":{"annotations":{"used":"true"},
						"labels":{"isPoolPod":"false","specialize":"prestart","enableTMPExpansion":"false"}}}`)
	} else {
		patchBytes = []byte(`{"metadata":{"annotations":{"used":"true"},
						"labels":{"isPoolPod":"false","specialize":"prestart","enableTMPExpansion":"true"}}}`)
	}
	ctx, cancel := context.WithTimeout(context.TODO(), utils.K8sPatchTimeout)
	_, err := p.KubeClient.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.StrategicMergePatchType,
		patchBytes, metav1.PatchOptions{})
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to update the pod %s label, error %s", pod.Name, err.Error())
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:               pod.Namespace,
			Ctx:                     context.TODO(),
			PodName:                 pod.Name,
			OperationType:           utils.PatchType,
			StrategicMergePatchType: types.StrategicMergePatchType,
			PatchBytes:              patchBytes,
			OperationOptions:        metav1.PatchOptions{},
		})
		return err
	}
	log.GetLogger().Debugf("the pod %s label updated, cost time %s", pod.Name, time.Now().Sub(t))
	return nil
}

func (p *PoolMgr) updateDeployReplicas(pool *Pool, replicas int32) error {
	t := time.Now()
	patchBytes := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	ctx, cancel := context.WithTimeout(context.TODO(), utils.K8sPatchTimeout)
	_, err := p.KubeClient.AppsV1().Deployments(pool.NameSpace).
		Patch(ctx, pool.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to update the deployment %s replicas as %d, error %s",
			pool.Name, replicas, err.Error())
		return err
	}
	log.GetLogger().Debugf("the deployment %s replicas updated as %d, cost time %s",
		pool.Name, replicas, time.Now().Sub(t))
	return nil
}

const defaultPoolHangingTimeout = 300

var poolHangingTimeout = parsePoolHangingTimeout()

func parsePoolHangingTimeout() time.Duration {
	env := os.Getenv("POOL_HANGING_TIMEOUT")
	if env == "" {
		return defaultPoolHangingTimeout * time.Second
	}
	if val, err := strconv.Atoi(env); err == nil && val > 0 {
		return time.Duration(val) * time.Second
	}
	return defaultPoolHangingTimeout * time.Second
}

func (p *PoolMgr) checkAndRolloutDeploy(pool *Pool) {
	if atomic.LoadUint32(&p.Config.DisablePoolRollout) != 0 {
		return
	}

	if pool.getHangingTime() >= poolHangingTimeout {
		pool.resetHangingTime()
		if err := p.rolloutDeploy(pool); err != nil {
			return
		}
	}
}

func (p *PoolMgr) rolloutDeploy(pool *Pool) error {
	t := time.Now()
	patch := []byte(fmt.Sprintf(`{"spec": {"template": {"metadata": {"annotations": {"%s": "%s"}}}}}`,
		rolloutKey, time.Now().Format("20060102150405")))
	ctx, cancel := context.WithTimeout(context.TODO(), utils.K8sPatchTimeout)
	_, err := p.KubeClient.AppsV1().Deployments(pool.NameSpace).
		Patch(ctx, pool.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to rollout the deployment %s, error %s", pool.Name, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to rollout deployment %s cost %s", pool.Name, time.Since(t))
	return nil
}

func getStandardFromPodName(name string) (standard, error) {
	pn, err := newPodName(name)
	if err != nil {
		log.GetLogger().Errorf("invalid pod name %s, error: %s", name, err.Error())
		return "", err
	}
	return pn.Standard, nil
}

func decodeCustomResources(rawJSON string) (map[string]int64, error) {
	if len(rawJSON) == 0 {
		return nil, nil
	}
	customResource := make(map[string]int64, utils.DefaultMapSize)
	err := json.Unmarshal([]byte(rawJSON), &customResource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse custom resource from request %s", rawJSON)
	}
	return customResource, nil
}

func notifyNodeBalanceMigratedPod(pod *corev1.Pod) {
	if pod.Annotations[usedLabelKey] == trueString {
		return
	}
	utils.NotifyPodReadyStatus(pod)
}

func handleDsWorkerPodEvent(pod *corev1.Pod) {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionFalse {
			log.GetLogger().Infof("handle the ds-worker pod: %s event, conditions Ready become: false",
				pod.Name)
			if _, ok := cache.NodeTaintCache.Load(pod.Spec.NodeName); ok {
				log.GetLogger().Warnf("ds-worker: %s node: %s has been tainted",
					pod.Name, pod.Spec.NodeName)
				return
			}
			go setDsWorkerHostTaintLabel(pod)
		}
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			log.GetLogger().Infof("handle the ds-worker pod: %s event, conditions Ready: true",
				pod.Name)
			go recoverDsWorkerHostTaintLabel(pod)
		}
	}
	return
}

func handleDsWorkerDeleteEvent(pod *corev1.Pod) {
	if _, ok := cache.NodeTaintCache.Load(pod.Spec.NodeName); ok {
		log.GetLogger().Warnf("ds-worker: %s node: %s has been tainted",
			pod.Name, pod.Spec.NodeName)
		return
	}
	go setDsWorkerHostTaintLabel(pod)
}

func setDsWorkerHostTaintLabel(pod *corev1.Pod) {
	log.GetLogger().Infof("start to set ds-worker: %s crash taint label to node: %s, node IP: %s",
		pod.Name, pod.Spec.NodeName, pod.Status.HostIP)
	taint := corev1.Taint{
		Key:    utils.DsWorkerTaintKey,
		Value:  utils.DsWorkerTaintValue,
		Effect: corev1.TaintEffectPreferNoSchedule,
	}
	nodeCache := cache.GetK8sNodeCacheByDeepCopy(pod.Spec.NodeName)
	if nodeCache == nil {
		log.GetLogger().Warnf("get node: %s from nodeCache failed, nodeCache is nil",
			pod.Spec.NodeName)
		return
	}
	var hasTaint bool
	for _, t := range nodeCache.Spec.Taints {
		if utils.IsDsWorkerTaint(t) {
			hasTaint = true
			break
		}
	}

	if !hasTaint {
		log.GetLogger().Infof("node: %s, node IP: %s has no ds-worker crash taint",
			pod.Spec.NodeName, pod.Status.HostIP)
		nodeCache.Spec.Taints = append(nodeCache.Spec.Taints, taint)
		ctx, cancel := context.WithTimeout(context.Background(), defaultUpdateTimeout)
		if _, err := utils.GetKubernetesClient().CoreV1().Nodes().Update(ctx,
			nodeCache, metav1.UpdateOptions{}); err != nil {
			log.GetLogger().Warnf("set ds-worker taint label to node: %s, node IP: %s error: %s",
				pod.Spec.NodeName, pod.Status.HostIP, err.Error())
			cancel()
			return
		}
		cancel()
	}

	cache.NodeTaintCache.Store(nodeCache.Name, time.Now())

	log.GetLogger().Infof("set ds-worker: %s crash taint label to node: %s, node IP: %s success",
		pod.Name, pod.Spec.NodeName, pod.Status.HostIP)
	processLongTimeFailure(pod)
	return
}

func recoverDsWorkerHostTaintLabel(pod *corev1.Pod) {
	log.GetLogger().Infof("start to recover ds-worker: %s taint to node: %s, node IP: %s",
		pod.Name, pod.Spec.NodeName, pod.Status.HostIP)
	nodeCache := cache.GetK8sNodeCacheByDeepCopy(pod.Spec.NodeName)
	if nodeCache == nil {
		log.GetLogger().Warnf("get node: %s from nodeCache failed, nodeCache is nil",
			pod.Spec.NodeName)
		return
	}
	var updatedTaints []corev1.Taint
	for _, taint := range nodeCache.Spec.Taints {
		if !utils.IsDsWorkerTaint(taint) {
			updatedTaints = append(updatedTaints, taint)
		}
	}
	nodeCache.Spec.Taints = updatedTaints
	ctx, cancel := context.WithTimeout(context.Background(), defaultUpdateTimeout)
	if _, err := utils.GetKubernetesClient().CoreV1().Nodes().Update(ctx,
		nodeCache, metav1.UpdateOptions{}); err != nil {
		log.GetLogger().Warnf("set ds-worker taint label to node: %s, node IP: %s error: %s",
			pod.Spec.NodeName, pod.Status.HostIP, err.Error())
		cancel()
		return
	}
	cancel()
	cache.NodeTaintCache.Delete(pod.Spec.NodeName)
	log.GetLogger().Infof("recover ds-worker: %s taint label to node: %s, node IP: %s success",
		pod.Name, pod.Spec.NodeName, pod.Status.HostIP)
	return
}

func processLongTimeFailure(pod *corev1.Pod) {
	log.GetLogger().Infof("ds-worker: %s, node: %s, node IP: %s waiting for long time failure.",
		pod.Name, pod.Spec.NodeName, pod.Status.HostIP)
	time.Sleep(time.Duration(utils.GetEnvDsWorkerCrashTimeout()) * time.Second)
	timeStamp, ok := cache.NodeTaintCache.Load(pod.Spec.NodeName)
	if !ok {
		log.GetLogger().Warnf("ds-worker: %s node: %s has released taint now",
			pod.Name, pod.Spec.NodeName)
		return
	}
	ts, ok := timeStamp.(time.Time)
	if !ok {
		return
	}
	duration := time.Now().Sub(ts).Seconds()
	if duration < float64(utils.GetEnvDsWorkerCrashTimeout()) {
		return
	}
	log.GetLogger().Infof("start to process long time failure, ds-worker: %s, node: %s, node IP: %s",
		pod.Name, pod.Spec.NodeName, pod.Status.HostIP)
	podList, err := utils.GetKubernetesClient().CoreV1().Pods(utils.DefaultNamespace).List(context.TODO(),
		metav1.ListOptions{LabelSelector: "isPoolPod=false", FieldSelector: "spec.nodeName=" + pod.Spec.NodeName})
	if err != nil {
		log.GetLogger().Errorf("failed to get instance pods by node %s with %s and %s",
			pod.Spec.NodeName, err.Error())
		return
	}
	if podList == nil || podList.Items == nil {
		return
	}
	for _, podInstance := range podList.Items {
		if podInstance.Labels == nil {
			continue
		}
		// bridge instance will not be influenced by ds-worker crash event
		if podInstance.Labels["stateID"] != "" && !utils.IsBridgeInstanceByPodLabel(podInstance.Labels) {
			deletePodByName(podInstance)
		}
		continue
	}
	log.GetLogger().Infof("process ds-worker: %s long time crash success, node: %s, node IP: %s",
		pod.Name, pod.Spec.NodeName, pod.Status.HostIP)
}

func deletePodByName(pod corev1.Pod) {
	deletePolicy := metav1.DeletePropagationForeground
	var err error
	var option metav1.DeleteOptions

	// if deleted pod is bridge function, delete it by force in case k8s pod resource leakage
	if pod.Labels != nil && utils.IsBridgeInstanceByPodLabel(pod.Labels) {
		log.GetLogger().Infof("delete bridge pod %s by force!", pod.Name)
		gracePeriodSeconds := int64(0)
		option = metav1.DeleteOptions{GracePeriodSeconds: &gracePeriodSeconds, PropagationPolicy: &deletePolicy}
		err = utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).Delete(context.TODO(), pod.Name, option)
	} else {
		option = metav1.DeleteOptions{PropagationPolicy: &deletePolicy}
		err = utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).Delete(context.TODO(), pod.Name, option)
	}
	if err != nil && !k8serror.IsNotFound(err) {
		log.GetLogger().Errorf("failed to delete pod '%s' with error: %s",
			pod.Name, err.Error())
		utils.RetryPodOperationList.AddPodOperation(&utils.PodOperationEle{
			Namespace:        utils.FunctionNameSpace(),
			Ctx:              context.TODO(),
			OperationType:    utils.DeleteType,
			PodName:          pod.Name,
			OperationOptions: option,
		})
		return
	}
	log.GetLogger().Infof("succeed to delete pod %s", pod.Name)
	return
}

func (p *PoolMgr) poolConfigHotLoadTask(stopCh <-chan struct{}) {
	for {
		select {
		case <-stopCh:
			log.GetLogger().Infof("stop chan closed, pool config hot load task exit")
			return
		case handleFileNum, ok := <-config.FileHandleNumChan:
			if !ok {
				log.GetLogger().Infof("fileHandleNumChan closed, pool config hot load task exit")
				return
			}
			poolList := p.GetPoolList()
			for _, pool := range poolList {
				if pool == nil {
					continue
				}
				if err := utils.UpdateFileHandleNumByK8s(pool.Deployment.Name, int(handleFileNum)); err != nil {
					log.GetLogger().Warnf("update deployment %s fileHandleNum to %d failed:%s",
						pool.Name, handleFileNum, err.Error())
				}
			}
		}
	}
}
