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

package cache

import (
	"context"
	"errors"
	"os"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/metrics"
	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/types"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/state"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

const (
	retryTime                = 5
	retryInterval            = 1 * time.Second
	defaultRollingPercentage = 50
	maxRollingPercentage     = 100
)

// ActionType is type of action
type ActionType int

// enums of action type
const (
	ActionPull ActionType = iota
	ActionReduce
	ActionIdleModeChanged
	ActionPullPrewarm
	ActionReducePrewarm
	ActionMigrateInstance
	ActionStatefulInstanceRePull
	ActionMicroServiceInstanceRePull
	ActionStatefulInstanceFlowOutDelete
	ActionRollingUpdate
)

// StatusType define status type
type StatusType int

const (
	// NotChanged define status not changed
	NotChanged StatusType = iota
	// IsChanged define status is changes
	IsChanged
	// IsDeleted define status is deleted
	IsDeleted
)

var (
	caeRollingPercentage = parseCAERollingPercent()
	imgDownloadNodeIP    = os.Getenv("IMAGE_DOWNLOAD_NODE_IP")
)

func parseCAERollingPercent() int {
	if env := os.Getenv("CAERollingPercent"); env != "" {
		if val, err := strconv.Atoi(env); err == nil && val > 0 && val <= maxRollingPercentage {
			return val
		}
	}
	return defaultRollingPercentage
}

// Handler -
type Handler interface {
	HandleInsMetadata(funcSvcID string, minScale, maxScale int64, idleMode bool,
		profileMeta types.ProfileMetaData) ([]Action, Metadata)
	HandleFunctionRepo(funcSvcID string, event *FunctionMeta) ([]Action, Metadata)
	HandleInstance(funcSvcID string, event *InstanceMeta) ([]Action, Metadata)
	HandleMigratingInstance(funcSvcID string, event *InstanceMeta, specificNode string) ([]Action, Metadata)
	HandleMigratingInstanceFailed(funcSvcID string, event *InstanceMeta)
	HandleInstanceAutoScaleDown(funcSvcID string, event *AutoScaleDownMeta)
	HandleFailure(funcSvcID string, event *PullFailureMeta) ([]Action, Metadata)
	HandlePrediction(funcSvcID string, predictionNum int, funcName string) []Action
}

// EventType is type of event
type EventType int

// FunctionMeta function meta
type FunctionMeta struct {
	FuncMeta    *faasfunction.FunctionService
	FuncInfo    metrics.FunctionInfo
	MinScale    int64
	MaxScale    int64
	CRSignature string
	EventKind   string
	EventType   string
}

// InstanceMeta instance meta
type InstanceMeta struct {
	Instance  v1alpha1.WorkerInstance
	EventType string
}

// PullFailureMeta pull failure meta
type PullFailureMeta struct {
	FailedNums        int
	CRSignature       string
	Delay             time.Duration
	IsRollingReserved bool
}

// FuncActions wrap actions with funcSvcID and Metadata
type FuncActions struct {
	FuncSvcID string
	Actions   []Action
	Metadata  Metadata
}

// Action describes action of pulling up reversed instance or reducing some instances
type Action struct {
	SnapshotMeta         *types.SnapshotMeta
	PodTemplate          *corev1.Pod
	PrewarmPod           []*corev1.Pod
	ScaleUpPrewarmPodNum int
	Type                 ActionType
	// reversed instance or pull
	NeedPull        int
	MigratedIns     v1alpha1.WorkerInstance
	NeedReduce      []v1alpha1.WorkerInstance
	NeedIdle        []v1alpha1.WorkerInstance
	IdleMode        bool
	RollingReserved int
	RollingOnDemand int
	// default 50 for 50%
	RollingPercentage int
}

// Metadata function metadata
type Metadata struct {
	FuncMeta      *faasfunction.FunctionService
	FuncInfo      utils.FunctionInfo
	CRSignature   string
	Status        StatusType
	Delay         time.Duration
	Creator       string
	CreateOptions *types.StateCreateOptions
	// StateID In every pod informer event, store the stateID for each stateful DoAction
	StateID string
}

// AutoScaleDownMeta describe instances what scaled down by autoscaler
type AutoScaleDownMeta struct {
	Instances []v1alpha1.WorkerInstance
}

// NewFuncMeta new func meta
func NewFuncMeta(funcMeta *faasfunction.FunctionService, event evs.Event, crSignature string) *FunctionMeta {
	if funcMeta == nil {
		return &FunctionMeta{
			FuncMeta:    &faasfunction.FunctionService{},
			EventKind:   event.Kind,
			EventType:   event.EventType,
			CRSignature: crSignature,
		}
	}
	return &FunctionMeta{
		FuncMeta: funcMeta,
		FuncInfo: metrics.FunctionInfo{
			FunctionName: funcMeta.Spec.Configuration.FunctionName,
			TenantID:     funcMeta.Spec.Configuration.TenantID,
			BusinessID:   funcMeta.Spec.Configuration.BusinessID,
			Version:      funcMeta.Spec.Configuration.FunctionVersion,
		},
		MinScale:    funcMeta.Spec.Configuration.AutoScalerConfig.MinimumScale,
		MaxScale:    funcMeta.Spec.Configuration.AutoScalerConfig.MaximumScale,
		EventKind:   event.Kind,
		EventType:   event.EventType,
		CRSignature: crSignature,
	}
}

// NewInstanceMeta new instance meta
func NewInstanceMeta(instance v1alpha1.WorkerInstance, eventType string) *InstanceMeta {
	return &InstanceMeta{Instance: instance, EventType: eventType}
}

// NewPullFailureMeta new pull failure meta
func NewPullFailureMeta(failedNums int, crSignature string, delay time.Duration) *PullFailureMeta {
	return &PullFailureMeta{FailedNums: failedNums, CRSignature: crSignature, Delay: delay}
}

// GetAndClearRollingInstance after rolling update finished, we should clear all rolling instance if present.
func (w *workerCache) GetAndClearRollingInstance(funcSvcID, crSignature string) []v1alpha1.WorkerInstance {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.Lock()
	defer fc.Unlock()
	if fc.crSignature != crSignature || len(fc.rollingInstances) == 0 {
		return nil
	}

	instance := make([]v1alpha1.WorkerInstance, 0, len(fc.rollingInstances))
	for i := range fc.rollingInstances {
		instance = append(instance, fc.rollingInstances[i])
	}
	fc.rollingInstances = make(map[string]v1alpha1.WorkerInstance)
	return instance
}

// GenerateInitRollingCAEFuncActions generate rolling actions for cae functions when workerCache is initialized
func (w *workerCache) GenerateInitRollingCAEFuncActions() []FuncActions {
	funcActions := make([]FuncActions, 0)
	w.functions.Range(func(funcSvcIDIf, funcCache interface{}) bool {
		fc, ok := funcCache.(*functionCache)
		if !ok {
			return true
		}
		funcSvcID, ok := funcSvcIDIf.(string)
		if !ok {
			return true
		}
		fc.RLock()
		defer fc.RUnlock()
		// metadata is nil or is not CAE function, return
		if fc.funcMeta == nil || !commonutils.IsCAEFunc(fc.funcMeta.Spec.Configuration.BusinessType) {
			return true
		}
		if len(fc.rollingInstances) == 0 {
			return true
		}
		reserved := 0
		onDemand := 0
		for _, instance := range fc.rollingInstances {
			if instance.Applier == constants.WorkerManagerApplier {
				reserved++
			} else {
				onDemand++
			}
		}
		meta := fc.funcMeta
		signature := fc.crSignature
		funcActions = append(funcActions, FuncActions{
			FuncSvcID: funcSvcID,
			Actions: []Action{
				{Type: ActionRollingUpdate, RollingReserved: reserved, RollingOnDemand: onDemand,
					RollingPercentage: caeRollingPercentage},
			},
			Metadata: Metadata{FuncMeta: meta, CRSignature: signature, Status: IsChanged},
		})
		return true
	})
	return funcActions
}

func (w *workerCache) UpdateFuncMinScaleUpdatedTime(err error, funcSvcID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get function cache with %s when updating function minScaleUpdatedTime",
			funcSvcID)
		return
	}
	if fc.isUserErrorAlarmSign == commonutils.IsUserError(err) {
		return
	}
	fc.Lock()
	fc.isUserErrorAlarmSign = !fc.isUserErrorAlarmSign
	fc.minScaleUpdatedTime = time.Now()
	fc.Unlock()
}

// HandleInsMetadata generate actions for instance metadata event
func (w *workerCache) HandleInsMetadata(funcSvcID string, insMetaData config.InstanceMetaData,
	profileMeta types.ProfileMetaData, info utils.FunctionInfo) ([]Action, Metadata) {
	metadata := Metadata{Status: NotChanged, FuncInfo: info}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		fc = w.createSimpleFunctionCache(funcSvcID, insMetaData, profileMeta, info)
		fc.Lock()
		log.GetLogger().Infof("first time to update scale of func: %s with (%d, %d, %t)",
			funcSvcID, fc.minScale, fc.maxScale, fc.idleMode)
		_, _, actions := w.calActions(fc)
		fc.minScaleUpdatedTime = time.Now()
		fc.Unlock()
		return actions, metadata
	}
	fc.Lock()
	oldMin := fc.minScale
	oldMax := fc.maxScale
	oldIdleMode := fc.idleMode
	fc.minScale = insMetaData.MinInstance
	if utils.GetUpdateFunctionCacheEnable() {
		fc.maxScale = fc.originalMaxScale
	} else {
		fc.maxScale = insMetaData.MaxInstance
	}
	fc.idleMode = insMetaData.IdleMode
	fc.profile = profileMeta
	needPull, needReduce, actions := w.calActions(fc)
	if oldMin != insMetaData.MinInstance {
		fc.minScaleUpdatedTime = time.Now()
	}
	// send idle mode to all reserved instances
	reservedInstances := make([]v1alpha1.WorkerInstance, 0, len(fc.instances))
	for _, instance := range fc.instances {
		if instance.Applier == constants.WorkerManagerApplier {
			reservedInstances = append(reservedInstances, instance)
		}
	}
	if len(reservedInstances) > 0 {
		actions = append(actions, Action{Type: ActionIdleModeChanged,
			NeedIdle: reservedInstances, IdleMode: insMetaData.IdleMode})
	}
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	log.GetLogger().Infof("update scale of func: %s from (%d, %d, %t) to (%d, %d, %t), need pull: %d and reduce: %d",
		funcSvcID, oldMin, oldMax, oldIdleMode, fc.minScale, fc.maxScale, fc.idleMode, needPull, needReduce)
	fc.Unlock()
	return actions, metadata
}

func (w *workerCache) handleFunctionDelete(funcSvcID, crSignature string) []Action {
	// race: need to implement LoadAndDelete
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.Lock()
	if crSignature != fc.crSignature {
		fcSig := fc.crSignature
		fc.Unlock()
		log.GetLogger().Warnf("signature mismatched for func delete event, func: %s fcSig: %s eventSig: %s",
			funcSvcID, fcSig, crSignature)
		return nil
	}
	// release stateID with this funcSvcID
	fc.releaseState()

	for _, hook := range fc.deleteHook {
		hook(fc)
	}

	w.functions.Delete(funcSvcID)
	close(fc.reservedInsChan)
	reduce := make([]v1alpha1.WorkerInstance, 0, len(fc.instances)+len(fc.rollingInstances))
	for _, instance := range fc.instances {
		reduce = append(reduce, instance)
	}
	for _, instance := range fc.rollingInstances {
		reduce = append(reduce, instance)
	}
	var prewarmPool []*corev1.Pod
	for _, v := range fc.prewarmPool {
		prewarmPool = append(prewarmPool, v)
	}
	fc.crSignature = ""
	fc.resetInstance()
	fc.resetScalingNum()
	fc.rollingInstances = make(map[string]v1alpha1.WorkerInstance)
	fc.initInstances = map[string]struct{}{}
	fc.Unlock()

	return []Action{{Type: ActionReduce, NeedReduce: reduce}, {Type: ActionReducePrewarm, PrewarmPod: prewarmPool}}
}

func (w *workerCache) calActions(fc *functionCache) (int, int, []Action) {
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("snapshot manager skip reversed calActions")
		return 0, 0, []Action{}
	}
	if utils.IsPullTriggerFunction(fc.funcMeta) {
		return int(fc.minScale), 0, []Action{{Type: ActionPull, NeedPull: int(fc.minScale)}}
	}

	reversed := make([]v1alpha1.WorkerInstance, 0, len(fc.instances))
	normal := make([]v1alpha1.WorkerInstance, 0, len(fc.instances))
	needReduce := make([]v1alpha1.WorkerInstance, 0, len(fc.instances))
	needPull := 0

	for _, instance := range fc.instances {
		if instance.Applier == constants.WorkerManagerApplier {
			reversed = append(reversed, instance)
			continue
		}
		normal = append(normal, instance)
	}

	// first check minScale
	reduceNum := int(int64(len(reversed)) + fc.scalingReversed - fc.minScale)
	log.GetLogger().Infof("check minScale for func %s, reversed: %d, scaling: %d, minScale: %d, reduceNum: %d",
		fc.funcSvcID, len(reversed), fc.scalingReversed, fc.minScale, reduceNum)
	if reduceNum <= 0 {
		needPull = -reduceNum
		fc.scalingReversed += int64(-reduceNum)
	} else if int64(len(reversed)) > fc.minScale {
		// scalingReversed instance will be deleted after they pulled up, so ignore when cal needReduce
		reduceNum = int(int64(len(reversed)) - fc.minScale)
		sortedReserved := fc.sortWorkerInstance(reversed)
		for i := 0; i < reduceNum; i++ {
			fc.deleteInstance(sortedReserved[i].PodName, true)
		}
		needReduce = append(needReduce, sortedReserved[:reduceNum]...)
	}

	// then check maxScale
	reduceNum = int(int64(len(normal)) - fc.maxScale)
	log.GetLogger().Debugf("check maxScale for func %s, normal: %d, maxScale: %d reduceNum: %d",
		fc.funcSvcID, len(normal), fc.maxScale, reduceNum)
	if reduceNum > 0 {
		reduceNum = commonutils.Min(reduceNum, len(normal))
		sortedNormal := fc.sortWorkerInstance(normal)
		for i := 0; i < reduceNum; i++ {
			fc.deleteInstance(sortedNormal[i].PodName, false)
		}
		needReduce = append(needReduce, sortedNormal[:reduceNum]...)
	}
	needReduce = append(needReduce, fc.reduceRollingWhenDisabled()...)
	return needPull, len(needReduce), []Action{{Type: ActionReduce, NeedReduce: needReduce},
		{Type: ActionPull, NeedPull: needPull}}
}

// HandleFunctionRepo for etcd event, only add and delete
func (w *workerCache) HandleFunctionRepo(funcSvcID string, event *FunctionMeta) ([]Action, Metadata) {
	metadata := Metadata{FuncMeta: event.FuncMeta, CRSignature: event.CRSignature, Status: NotChanged}
	// current EventKind has only two type, so below EventKind is always FunctionRepoEventKind
	if event.EventType == evs.Deleted {
		metadata.Status = IsDeleted
		if event.FuncMeta != nil && event.FuncMeta.Spec.Configuration.FuncVpc.VpcID != "" {
			processDeleteDomainConfig(funcSvcID,
				event.FuncMeta.Spec.Configuration.FuncVpc.VpcID, event.FuncMeta.Spec.Configuration.DNSDomainCfg)
		}
		if event.FuncMeta != nil && event.FuncMeta.Spec.Configuration.EndpointTenantVpc.VpcID != "" {
			processDeleteDomainConfig(funcSvcID, event.FuncMeta.Spec.Configuration.EndpointTenantVpc.VpcID,
				event.FuncMeta.Spec.Configuration.EndpointDomainCfg)
		}
		if event.FuncMeta != nil && event.FuncMeta.Spec.Configuration.CustomContainerConfig.Image != "" {
			ImageNodeCache.Delete(event.FuncMeta.Spec.Configuration.FunctionVersionURN)
			go DeleteImageCache(funcSvcID)
			defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			err := etcd3.GetMetaEtcdClient().Delete(defaultEtcdCtx, getETCDCustomImageSizeKey(event.FuncMeta))
			if err != nil {
				log.GetLogger().Warnf("failed to delete custom image size from etcd, func: %s", funcSvcID)
			}
		}
		return w.handleFunctionDelete(funcSvcID, event.CRSignature), metadata
	}

	// function meta add or update
	fc := w.createFunctionCacheIfNotExist(funcSvcID, event.CRSignature, event.FuncInfo, event.FuncMeta)
	funcScheduleNodes := getScheduleNodesByLabels(event.FuncMeta.Spec.Configuration.FgsTags)
	fc.Lock()
	fc.funcMeta = event.FuncMeta
	fc.originalMaxScale = event.FuncMeta.Spec.Configuration.AutoScalerConfig.MaximumScale
	fc.minScaleUpdatedTime = time.Now()
	fc.info = event.FuncInfo
	fc.funcScheduleNodes = funcScheduleNodes
	if event.FuncMeta != nil {
		fc.dnsDomainCfg = event.FuncMeta.Spec.Configuration.DNSDomainCfg
		fc.vpcID = event.FuncMeta.Spec.Configuration.FuncVpc.VpcID
		fc.endpointDomainCfg = event.FuncMeta.Spec.Configuration.EndpointDomainCfg
		fc.endpointTenantVpcID = event.FuncMeta.Spec.Configuration.EndpointTenantVpc.VpcID
		fc.updateFunctionClusters(clusterstatusutil.ClusterStatusCache.ClusterStatusMap)
		fc.updateClusterMaxScale()
		// when dns domain config update, it will update dns ip list at once
		go fc.ProcessUpdateDomainConfig(funcSvcID)

		if event.FuncMeta.Spec.Configuration.CustomContainerConfig.Image != "" {
			handleCacheForImageAcceleration(funcSvcID, event.CRSignature, event.FuncMeta)
			if imgDownloadNodeIP != "" {
				fc.recoverCustomImageSize()
				go fc.getAndSetImageSize(event.FuncMeta.Spec.Configuration.CustomContainerConfig.Image, event.FuncMeta)
			}
		}
	}
	if event.CRSignature != fc.crSignature {
		log.GetLogger().Infof("function update %s, fcSig: %s eventSig: %s", funcSvcID, fc.crSignature,
			event.CRSignature)
		var actions []Action
		if commonutils.IsCAEFunc(fc.funcMeta.Spec.Configuration.BusinessType) &&
			!fc.funcMeta.Spec.Configuration.IsStatefulFunction {
			actions = w.handleCAESignatureChanged(fc, event)
		} else {
			actions = w.handleSignatureChanged(fc, event)
		}
		fc.Unlock()
		metadata.Status = IsChanged
		return actions, metadata
	}
	_, _, actions := w.calActions(fc)
	fc.Unlock()

	return actions, metadata
}

func processDeleteDomainConfig(funcSvcID, vpcID string, infos []types.DNSDomainInfo) {
	if len(infos) == 0 {
		return
	}
	for _, domainInfo := range infos {
		utils.GetDNSCache().ProcessDeleteDomainConfig(funcSvcID, vpcID, domainInfo.DomainName)
	}
}

func (w *workerCache) handleCAESignatureChanged(fc *functionCache, event *FunctionMeta) []Action {
	fc.crSignature = event.CRSignature
	fc.scalingReversed = fc.minScale
	// when function updated, breaker will be reset, and it will allow all specialized requests
	fc.resetBreaker()
	fc.resetScalingNum()

	// move to rolling instances
	for k, v := range fc.instances {
		fc.rollingInstances[k] = v
	}
	onDemand := 0
	for _, v := range fc.rollingInstances {
		if v.Applier != constants.WorkerManagerApplier {
			onDemand++
		}
	}

	fc.resetInstance()
	fc.scalingDone = make(map[string]int64, initCacheSize)
	reducePrewarm := make([]*corev1.Pod, 0, len(fc.prewarmPool))
	for _, instance := range fc.prewarmPool {
		reducePrewarm = append(reducePrewarm, instance)
	}
	scaleUpPrewarmPodNum := len(fc.prewarmPool)
	return []Action{
		{Type: ActionRollingUpdate, RollingReserved: int(fc.minScale), RollingOnDemand: onDemand,
			RollingPercentage: caeRollingPercentage},
		{Type: ActionReducePrewarm, PrewarmPod: reducePrewarm},
		{Type: ActionPullPrewarm, ScaleUpPrewarmPodNum: scaleUpPrewarmPodNum},
	}
}

func (w *workerCache) handleSignatureChanged(fc *functionCache, event *FunctionMeta) []Action {
	fc.crSignature = event.CRSignature
	fc.scalingReversed = fc.minScale
	// when function updated, breaker will be reset, and it will allow all specialized requests
	fc.resetBreaker()
	fc.resetScalingNum()
	needReduce := make([]v1alpha1.WorkerInstance, 0, len(fc.instances))
	for _, instance := range fc.instances {
		needReduce = append(needReduce, instance)
	}
	fc.resetInstance()
	fc.scalingDone = make(map[string]int64, initCacheSize)
	reducePrewarm := make([]*corev1.Pod, 0, len(fc.prewarmPool))
	for _, instance := range fc.prewarmPool {
		reducePrewarm = append(reducePrewarm, instance)
	}
	scaleUpPrewarmPodNum := len(fc.prewarmPool)
	if fc.funcMeta != nil && fc.funcMeta.Spec.Configuration.IsStatefulFunction {
		return []Action{
			{Type: ActionReduce, NeedReduce: needReduce},
		}
	}
	return []Action{
		{Type: ActionPull, NeedPull: int(fc.minScale)},
		{Type: ActionReduce, NeedReduce: needReduce},
		{Type: ActionReducePrewarm, PrewarmPod: reducePrewarm},
		{Type: ActionPullPrewarm, ScaleUpPrewarmPodNum: scaleUpPrewarmPodNum},
	}
}

func (fc *functionCache) countReversedInstance() int64 {
	count := int64(0)
	for _, instance := range fc.instances {
		if instance.Applier == constants.WorkerManagerApplier {
			count++
		}
	}
	return count
}

func (w *workerCache) handleInstanceDelete(funcSvcID string, event *InstanceMeta) ([]Action, Metadata) {
	metadata := Metadata{Status: NotChanged}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get functionCache when handle instance delete, funcSvcID: %s", funcSvcID)
		return nil, metadata
	}
	fc.Lock()
	defer fc.Unlock()
	delete(fc.scalingDone, event.Instance.PodName)
	delete(fc.initInstances, event.Instance.PodName)
	refreshMetadataWhenHandleInstanceDelete(&metadata, fc, event)
	if event.Instance.Signature != fc.crSignature {
		if fc.funcMeta == nil || !fc.funcMeta.Spec.Configuration.IsStatefulFunction {
			log.GetLogger().Warnf("signature mismatched for delete event, podSig: %s fcSig: %s, func: %s, pod: %s",
				event.Instance.Signature, fc.crSignature, funcSvcID, event.Instance.PodName)
			return nil, metadata
		}
	}
	for _, hook := range fc.deleteInstanceHook {
		go hook(fc, event.Instance)
	}
	existed := false
	if _, existed = fc.instances[event.Instance.PodName]; existed {
		fc.deleteInstance(event.Instance.PodName, event.Instance.Applier == constants.WorkerManagerApplier)
	}
	defer MetricsCache.UpdateFuncInstanceCurrentCount(&fc.info, len(fc.instances)+len(fc.rollingInstances))

	if _, ok := fc.rollingInstances[event.Instance.PodName]; ok {
		delete(fc.rollingInstances, event.Instance.PodName)
		log.GetLogger().Infof("delete rolling instance, podName: %s", event.Instance.PodName)
		return nil, metadata
	}
	if event.Instance.Applier == constants.PredictionApplier {
		w.removePrewarmInstances(fc, event.Instance.PodName)
	}
	// reload condition:(stateID in stateMap and currentNum is 0) and {podName:stateID} not repeat in fc.CheckHashKeyInMap
	log.GetLogger().Debugf("worker instance deleted, event: %+v", event)
	if isNeedReloadStatefulFunction(fc, event) {
		fc.delHashKeyInMap(event.Instance.StateID)
		fc.resetStateIDMap(event.Instance.StateID)
		fc.storeReloadingState(event.Instance.StateID)
		return w.statefulFunctionReload(metadata, event)
	}
	if event.Instance.Applier == constants.ASBResApplier && needPullUpMicroServiceInstance(fc) {
		return w.microServiceFunctionReload(metadata)
	}
	var actions []Action
	if existed && event.Instance.Applier == constants.WorkerManagerApplier {
		defer DeleteReserveInstanceTimestamp(event.Instance.PodName)
		log.GetLogger().Debugf("succeed to delete reserved instance with pod %s and scalingReserved %d for %s",
			event.Instance.PodName, fc.scalingReversed, fc.funcSvcID)
		reversedCount := fc.countReversedInstance() + fc.scalingReversed
		if reversedCount >= fc.minScale {
			return nil, metadata
		}
		log.GetLogger().Infof("reversed deleted and need scaling: %s %s %d", fc.funcSvcID, event.Instance.PodName,
			fc.scalingReversed+1)
		fc.scalingReversed++
		actions = append(actions, Action{Type: ActionPull, NeedPull: 1})
	}
	actions = w.pullPrewarmPodIfInstanceNotEnough(funcSvcID, fc, actions, event)
	return actions, metadata
}

func isNeedReloadStatefulFunction(fc *functionCache, event *InstanceMeta) bool {
	return event.Instance.StateID != "" && fc.checkStateIDKeyExist(event.Instance.StateID) &&
		fc.CheckHashKeyInMap(event.Instance.StateID, event.Instance.PodName) && !event.Instance.IsMigrating
}

func refreshMetadataWhenHandleInstanceDelete(metadata *Metadata, fc *functionCache, event *InstanceMeta) {
	metadata.Creator = event.Instance.Creator
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	if event.Instance.StateID != "" {
		metadata.CreateOptions = &types.StateCreateOptions{
			CPU:    int64(event.Instance.CPU),
			Memory: int64(event.Instance.Memory),
		}
	}
}

// stateful instance's stateID if still in stateGroup, reload
func (w *workerCache) statefulFunctionReload(metadata Metadata, event *InstanceMeta) ([]Action, Metadata) {
	log.GetLogger().Infof("matched stateful instance reload, stateID: %s, pod name :%s ",
		event.Instance.StateID, event.Instance.PodName)
	metadata.StateID = event.Instance.StateID
	var actions []Action
	actions = append(actions, Action{Type: ActionStatefulInstanceRePull, NeedPull: 1})
	return actions, metadata
}

func needPullUpMicroServiceInstance(fc *functionCache) bool {
	autoScalerPullUpInstanceNum := int64(fc.expectedNum) - fc.minScale
	if autoScalerPullUpInstanceNum > fc.maxScale {
		autoScalerPullUpInstanceNum = fc.maxScale
	}
	curOnDemandNum := fc.getCurOnDemandNum()
	log.GetLogger().Debugf("Calculate whether need to pull up micro service instance,"+
		"fc.maxScale: %d, fc.minScale: %d, fc.expectedNum: %d, fc.getCurOnDemandNum: %d",
		fc.maxScale, fc.minScale, fc.expectedNum, curOnDemandNum)
	return autoScalerPullUpInstanceNum > curOnDemandNum
}

func (w *workerCache) microServiceFunctionReload(metadata Metadata) ([]Action, Metadata) {
	var actions []Action
	actions = append(actions, Action{Type: ActionMicroServiceInstanceRePull, NeedPull: 1})
	return actions, metadata
}

func (w *workerCache) pullPrewarmPodIfInstanceNotEnough(funcSvcID string, fc *functionCache, actions []Action,
	event *InstanceMeta) []Action {
	var isReservedScaling int
	// reserved Instances are automatically replenished
	for _, v := range actions {
		if v.Type == ActionPull {
			isReservedScaling = 1
			break
		}
	}

	if compareCurrentPodWithPredictionNum(fc) > isReservedScaling {
		log.GetLogger().Infof("funcSvcID is %s, current deleted pod is: %s, need to add prewarm pool",
			funcSvcID, event.Instance.PodName)
		actions = append(actions, Action{Type: ActionPullPrewarm, ScaleUpPrewarmPodNum: 1})
	}
	return actions
}

func broadcastReservedIns(reservedInsChan chan v1alpha1.WorkerInstance, instance v1alpha1.WorkerInstance,
	funcSvcID string) {
	if reservedInsChan == nil {
		return
	}
	// broadcast to all waiting requests
	for {
		// push reserved instance to channel non-blocking
		select {
		case reservedInsChan <- instance:
			log.GetLogger().Infof("broadcast pod: %s to reservedInsChan for: %s", instance.PodName, funcSvcID)
		default:
			// no waiting request
			return
		}
	}
}

func reduceInstance(event *InstanceMeta) []Action {
	if event.Instance.Applier == constants.PredictionApplier {
		pod := &corev1.Pod{}
		pod.Name = event.Instance.PodName
		return []Action{{Type: ActionReducePrewarm, PrewarmPod: []*corev1.Pod{pod}}}
	}
	return []Action{{Type: ActionReduce, NeedReduce: []v1alpha1.WorkerInstance{event.Instance}}}
}

func (fc *functionCache) handleReservedInstanceUpdate(event *InstanceMeta) []Action {
	if fc.reservedInsNodePreDis[event.Instance.NodeName] > 0 {
		fc.reservedInsNodePreDis[event.Instance.NodeName]--
	}
	fc.scalingDone[event.Instance.PodName] = time.Now().Unix()
	fc.scalingReversed--
	if fc.scalingReversed < 0 && commonutils.IsCAEFunc(event.Instance.BusinessType) {
		log.GetLogger().Warnf("get scaling num for func %s error, scaling: %d, podName: %s", fc.funcSvcID,
			fc.scalingReversed, event.Instance.PodName)
		fc.scalingReversed = 0
	}

	reversedCount := fc.countReversedInstance() + fc.scalingReversed
	log.GetLogger().Infof("check minScale for func %s, scaling: %d, reservedCount: %d, minScale: %d",
		fc.funcSvcID, fc.scalingReversed, reversedCount, fc.minScale)
	if reversedCount <= fc.minScale {
		// broadcast the reserved instance to all waiting requests
		fc.broadcastReservedIns(event.Instance)
		// To avoid channel panic, unlock the channel after the push channel.
		AddReserveInstanceTimestamp(&fc.info, event.Instance.PodName, true)
		if fc.predictionNum >= 0 && compareCurrentPodWithPredictionNum(fc) < 0 {
			return []Action{{Type: ActionReducePrewarm, PrewarmPod: fc.takeOutPrewarmPod(1)}}
		}
		return nil
	}
	if event.Instance.MigratedFrom != "" {
		var reduce v1alpha1.WorkerInstance
		if ins, ok := fc.instances[event.Instance.MigratedFrom]; ok {
			reduce = ins
		} else {
			reduce = event.Instance
		}
		fc.deleteInstance(reduce.PodName, true)
		return reduceInstance(&InstanceMeta{Instance: reduce})
	}
	migratingReservedIns, ok := fc.getMigratingReservedIns()
	if !ok {
		log.GetLogger().Infof("it has no migrating reserved instance and it will scale down %s for %s",
			event.Instance.PodName, fc.funcSvcID)
		fc.deleteInstance(event.Instance.PodName, true)
		return reduceInstance(event)
	}
	// prefer to delete migrating reserved instance
	log.GetLogger().Infof("prefer to delete migrating instance %s for %s when reserved count > min scale",
		migratingReservedIns.PodName, fc.funcSvcID)
	fc.deleteInstance(migratingReservedIns.PodName, true)
	return reduceInstance(&InstanceMeta{Instance: migratingReservedIns})
}

func (w *workerCache) handleInstanceUpdate(funcSvcID string, event *InstanceMeta) ([]Action, Metadata) {
	metadata := Metadata{Status: NotChanged}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get functionCache when handle instance update, funcSvcID: %s", funcSvcID)
		return reduceInstance(event), metadata
	}
	fc.Lock()
	log.GetLogger().Debugf("handle instance update with name %s, applier %s and event type %s for %s",
		event.Instance.PodName, event.Instance.Applier, event.EventType, funcSvcID)
	defer fc.Unlock()
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	metadata.StateID = event.Instance.StateID
	if event.Instance.Signature != fc.crSignature {
		if _, ok := fc.rollingInstances[event.Instance.PodName]; ok {
			log.GetLogger().Infof("rolling instance updated, func %s pod %s", funcSvcID, event.Instance.PodName)
			return nil, metadata
		}
		log.GetLogger().Errorf("signature mismatched, old: %s new: %s, func: %s, pod: %s",
			event.Instance.Signature, fc.crSignature, funcSvcID, event.Instance.PodName)
		return reduceInstance(event), metadata
	}

	if event.Instance.Applier == constants.PredictionApplier {
		return w.deletePrewarmPodIfRedundant(fc, event), metadata
	}

	_, existed := fc.instances[event.Instance.PodName]
	fc.addInstance(event.Instance.PodName, event.Instance)

	if existed {
		return nil, metadata
	}

	defer MetricsCache.UpdateFuncInstanceCurrentCount(&fc.info, len(fc.instances)+len(fc.rollingInstances))

	var actions []Action
	if event.Instance.Applier == constants.WorkerManagerApplier && fc.scalingDone[event.Instance.PodName] == 0 {
		log.GetLogger().Infof("decreasing scalingReversed, func: %s, pod: %s", funcSvcID, event.Instance.PodName)
		actions = fc.handleReservedInstanceUpdate(event)
	} else if event.Instance.Applier != constants.WorkerManagerApplier &&
		fc.getCurOnDemandNum() > fc.maxScale && !event.Instance.IsMigrating {
		log.GetLogger().Infof("current instance num exceeds maxScale, reduce instance, "+
			"func: %s, pod: %s", funcSvcID, event.Instance.PodName)
		fc.deleteInstance(event.Instance.PodName, false)
		actions = reduceInstance(event)
	}
	actions = append(actions, reduceRollingInstance(funcSvcID, fc, event)...)

	return actions, metadata
}

func reduceRollingInstance(funcSvcID string, fc *functionCache, event *InstanceMeta) []Action {
	// only new pulled instance can reduce rollingInstance
	if _, ok := fc.initInstances[event.Instance.PodName]; ok {
		delete(fc.initInstances, event.Instance.PodName)
		return nil
	}
	for name, instance := range fc.rollingInstances {
		if instance.Applier == event.Instance.Applier ||
			(instance.Applier != constants.WorkerManagerApplier &&
				event.Instance.Applier != constants.WorkerManagerApplier) {
			delete(fc.rollingInstances, name)
			log.GetLogger().Infof("reduce oldInstace %s of applier %s for func %s when new instance %s is ready",
				name, instance.Applier, funcSvcID, event.Instance.PodName)
			return []Action{
				{Type: ActionReduce, NeedReduce: []v1alpha1.WorkerInstance{instance}},
			}
		}
	}
	return nil
}

// HandleMigratingInstance handle instance migrating
func (w *workerCache) HandleMigratingInstance(funcSvcID string, event *InstanceMeta,
	podTemplate *corev1.Pod) ([]Action, Metadata) {
	metadata := Metadata{Status: NotChanged}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get fc cache when handle instance migrating for %s", funcSvcID)
		return reduceInstance(event), metadata
	}
	fc.Lock()
	defer fc.Unlock()

	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	metadata.StateID = event.Instance.StateID
	if event.Instance.Signature != fc.crSignature {
		log.GetLogger().Errorf("signature mismatched, old: %s new: %s, func: %s, pod: %s",
			event.Instance.Signature, fc.crSignature, funcSvcID, event.Instance.PodName)
		return reduceInstance(event), metadata
	}

	_, existed := fc.instances[event.Instance.PodName]
	if existed && event.Instance.IsMigrating {
		if event.Instance.Applier == constants.WorkerManagerApplier {
			fc.scalingReversed++
			log.GetLogger().Infof("check minScale for func %s, scaling: %d, minScale: %d",
				fc.funcSvcID, fc.scalingReversed, fc.minScale)
		}
		// the DoActions() method will pull up an instance and then delete event instance
		return []Action{{Type: ActionMigrateInstance, MigratedIns: event.Instance,
			PodTemplate: podTemplate}}, metadata
	}

	return nil, metadata
}

// HandleMigratingInstanceFailed handle migrating instance failed
func (w *workerCache) HandleMigratingInstanceFailed(funcSvcID string, event *InstanceMeta) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get fc cache when handle migrating instance failed for %s", funcSvcID)
		return
	}
	fc.Lock()
	if event.Instance.Signature != fc.crSignature {
		log.GetLogger().Errorf("no need update scaling with signature mismatched, old: %s new: %s, func: %s, pod: %s",
			event.Instance.Signature, fc.crSignature, funcSvcID, event.Instance.PodName)
		fc.Unlock()
		return
	}
	if workerInstance, ok := fc.instances[event.Instance.PodName]; ok {
		workerInstance.IsStartMigrate = false
		fc.instances[event.Instance.PodName] = workerInstance
	}

	if event.Instance.Applier == constants.WorkerManagerApplier {
		fc.scalingReversed--
		log.GetLogger().Infof("check minScale for func %s, scaling: %d, minScale: %d",
			fc.funcSvcID, fc.scalingReversed, fc.minScale)
	}
	fc.Unlock()
}

// CheckMigratingInstanceShouldBeContinued check the instance migration should be continued.
func (w *workerCache) CheckMigratingInstanceShouldBeContinued(funcSvcID, podName, traceID string) bool {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get fc cache when migrating instance for %s, traceID: %s", funcSvcID, traceID)
		return false
	}
	fc.Lock()
	defer fc.Unlock()
	workerInstance, ok := fc.instances[podName]
	if !ok {
		return false
	}
	if workerInstance.IsStartMigrate {
		log.GetLogger().Warnf("instance %s for %s is already migrating phase, traceID: %s", podName, funcSvcID, traceID)
		return false
	}
	workerInstance.IsStartMigrate = true
	fc.instances[podName] = workerInstance
	return true
}

// SetMigratingInstanceToDone when the instance migration is complete or an error occurs, set the status to false.
func (w *workerCache) SetMigratingInstanceToDone(funcSvcID, podName, traceID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get fc cache when set migrate instance for %s, traceID: %s", funcSvcID, traceID)
		return
	}
	fc.Lock()
	defer fc.Unlock()
	workerInstance, ok := fc.instances[podName]
	if !ok || !workerInstance.IsStartMigrate {
		return
	}
	workerInstance.IsStartMigrate = false
	fc.instances[podName] = workerInstance
}

func compareCurrentPodWithPredictionNum(fc *functionCache) int {
	return fc.predictionNum - (len(fc.instances) + len(fc.prewarmPool) + int(fc.scalingNum) + fc.scalingPrewarm)
}

func (w *workerCache) deletePrewarmPodIfRedundant(fc *functionCache, event *InstanceMeta) []Action {
	if !utils.GetSmartWarmerEnable() {
		return reduceInstance(event)
	}
	if fc.scalingDone[constants.PredictionApplier+event.Instance.PodName] == 0 {
		// valid only on first update
		fc.scalingDone[constants.PredictionApplier+event.Instance.PodName] = time.Now().Unix()
		if fc.predictionNum >= 0 && compareCurrentPodWithPredictionNum(fc) < 0 {
			log.GetLogger().Infof("begin to delete redundant pod, func: %s, podName: %s", event.Instance.FuncName,
				event.Instance.PodName)
			return reduceInstance(event)
		}
	}
	return nil
}

// HandleInstance handle instance event
func (w *workerCache) HandleInstance(funcSvcID string, event *InstanceMeta) ([]Action, Metadata) {
	switch event.EventType {
	case evs.Added, evs.Updated:
		return w.handleInstanceUpdate(funcSvcID, event)
	case evs.Deleted:
		return w.handleInstanceDelete(funcSvcID, event)
	default:
		log.GetLogger().Errorf("unknown instance eventType: %s", event.EventType)
		return nil, Metadata{Status: NotChanged}
	}
}

// HandleInstanceAutoScaleDown -
func (w *workerCache) HandleInstanceAutoScaleDown(funcSvcID string, event *AutoScaleDownMeta) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get functionCache when handle scale down, funcSvcID: %s", funcSvcID)
		return
	}
	now := timeutil.NowUnixMillisecond()
	fc.Lock()
	for i := range event.Instances {
		if _, ok := fc.instances[event.Instances[i].PodName]; ok {
			fc.autoScaleTimestamp[event.Instances[i].NodeName] = now
		}
	}
	fc.Unlock()
	return
}

// HandleFailure -
func (w *workerCache) HandleFailure(funcSvcID string, event *PullFailureMeta) ([]Action, Metadata) {
	metadata := Metadata{Status: NotChanged}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get functionCache when handle instance update, funcSvcID: %s", funcSvcID)
		return nil, metadata
	}
	fc.Lock()
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	if event.CRSignature != fc.crSignature {
		fcSig := fc.crSignature
		fc.Unlock()
		log.GetLogger().Warnf("signature mismatched for fail event, func: %s fcSig: %s eventSig: %s",
			funcSvcID, fcSig, event.CRSignature)
		return nil, metadata
	}
	fc.scalingReversed -= int64(event.FailedNums)
	_, _, actions := w.calActions(fc)
	fc.Unlock()
	baseDelay := event.Delay
	baseDelay = baseDelay << 1
	if baseDelay >= utils.MaxDelayForReservedIns {
		baseDelay = utils.MaxDelayForReservedIns
	}
	metadata.Delay = baseDelay
	return actions, metadata
}

// NeedReRolling checks whether we need to re-rolling when rolling failed
// current we stop re-rolling when function is deleted, updated or disabled
func (w *workerCache) NeedReRolling(funcSvcID string, event *PullFailureMeta) bool {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Infof("stop re-rolling when function is deleted, func: %s", funcSvcID)
		return false
	}
	fc.Lock()
	defer fc.Unlock()
	if fc.crSignature != event.CRSignature {
		log.GetLogger().Infof("stop re-rolling when function is updated, func: %s fcSig: %s eventSig: %s",
			funcSvcID, fc.crSignature, event.CRSignature)
		return false
	}
	if event.IsRollingReserved && fc.minScale == 0 {
		fc.scalingReversed -= int64(event.FailedNums)
		log.GetLogger().Infof("stop re-rolling reserved when function minScale is 0, func: %s", funcSvcID)
		return false
	}
	if !event.IsRollingReserved && fc.maxScale == 0 {
		log.GetLogger().Infof("stop re-rolling on-demand when function is disabled, func: %s", funcSvcID)
		return false
	}
	return true
}

// HandlePrediction -
func (w *workerCache) HandlePrediction(funcSvcID string, predictionNum int, funcName string) []Action {
	var fc *functionCache
	var ok bool
	for i := 0; i < retryTime; i++ {
		fc, ok = w.getFunctionCache(funcSvcID)
		if ok {
			break
		}
		time.Sleep(retryInterval)
	}
	if fc == nil {
		log.GetLogger().Warnf("failed to get functionCache for prediction, funcSvcID %s", funcSvcID)
		return nil
	}

	fc.Lock()
	defer fc.Unlock()
	fc.predictionNum = predictionNum

	scaleLen := compareCurrentPodWithPredictionNum(fc)
	if scaleLen > 0 {
		return []Action{{Type: ActionPullPrewarm, ScaleUpPrewarmPodNum: scaleLen}}
	} else if scaleLen < 0 {
		return []Action{{Type: ActionReducePrewarm, PrewarmPod: fc.takeOutPrewarmPod(-scaleLen)}}
	}
	log.GetLogger().Infof("succeed to handle prewarm, funcName: %s, scale up len is: %d",
		funcName, scaleLen)
	return nil
}

// GetAPIGIP -
func (w *workerCache) GetAPIGIP() []string {
	w.apigIP.Lock()
	defer w.apigIP.Unlock()
	return w.apigIP.ips
}

// UpdateAPIGIP -
func (w *workerCache) UpdateAPIGIP(apigIP []string) {
	w.apigIP.Lock()
	defer w.apigIP.Unlock()
	w.apigIP.ips = apigIP
}

// DeleteAPIGIP -
func (w *workerCache) DeleteAPIGIP() {
	w.apigIP.Lock()
	defer w.apigIP.Unlock()
	w.apigIP.ips = nil
}

// GetPublicNetworkConf -
func (w *workerCache) GetPublicNetworkConf(projectID string) string {
	pubNetworkConf, ok := w.publicNetworkConfig.Load(projectID)
	if !ok {
		return ""
	}
	publicNetworkConf, ok := pubNetworkConf.(*PublicNetworkConfig)
	if !ok {
		log.GetLogger().Warnf("failed to assert type *PublicNetworkConfig. value: %T, projectID: %s",
			publicNetworkConf, projectID)
		return ""
	}
	return publicNetworkConf.Bandwidth
}

// UpdatePublicNetworkConf -
func (w *workerCache) UpdatePublicNetworkConf(projectID string, publicNetworkConf PublicNetworkConfig) {
	w.publicNetworkConfig.Store(projectID, &PublicNetworkConfig{
		Bandwidth: publicNetworkConf.Bandwidth,
	})
	return
}

// DeletePublicNetworkConf -
func (w *workerCache) DeletePublicNetworkConf(projectID string) {
	w.publicNetworkConfig.Delete(projectID)
}

func getCurrentAvailableClusters() []string {
	var clusters []string
	clusterstatusutil.ClusterStatusCache.Lock()
	for clusterName, clusterStatus := range clusterstatusutil.ClusterStatusCache.ClusterStatusMap {
		if clusterStatus.State == "running" {
			clusters = append(clusters, clusterName)
		}
	}
	clusterstatusutil.ClusterStatusCache.Unlock()
	return clusters
}

func (w *workerCache) UpdateFuncMaxScale() {
	w.functions.Range(func(key, value interface{}) bool {
		if fc, ok := value.(*functionCache); ok {
			fc.updateFunctionClusters(clusterstatusutil.ClusterStatusCache.ClusterStatusMap)
			fc.updateClusterMaxScale()
		}
		return true
	})
}

func (fc *functionCache) releaseState() {
	fc.stateMap.Range(func(key, value any) bool {
		stateID, ok := key.(string)
		if !ok {
			return true
		}
		go state.SendDeleteStateReq(stateID, fc.info)
		return true
	})
}

// IsCurrentClustersStatusRunning -
func IsCurrentClustersStatusRunning() bool {
	clusterstatusutil.ClusterStatusCache.Lock()
	defer clusterstatusutil.ClusterStatusCache.Unlock()
	currentCluster := clusterstatusutil.ClusterStatusCache.ClusterStatusMap[clusterID]
	if currentCluster.State == clusterstatusutil.ClusterRunningStatus || currentCluster.State == "" {
		return true
	}
	log.GetLogger().Warnf("cluster: %s current status is: %s, not running", clusterID,
		currentCluster.State)
	return false
}

// GetPodInMap -
func (w *workerCache) GetPodInMap(funcSvcID, stateID string) (string, bool) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get state pod for func: %s", funcSvcID)
		return "", false
	}
	fc.Lock()
	defer fc.Unlock()
	value, ok := fc.stateFulEventCheckRepeat.Load(stateID)
	if !ok {
		log.GetLogger().Warnf("failed to load pod from stateful map for: %s, stateID: %s", funcSvcID, stateID)
		return "", false
	}
	podName, ok := value.(string)
	if !ok {
		log.GetLogger().Warnf("failed to convert pod from stateful map for: %s, stateID: %s",
			funcSvcID, stateID)
		return "", false
	}
	log.GetLogger().Infof("success get state pod: %s, stateID: %s", podName, stateID)
	return podName, true
}

// GetStatePodActionWithoutReload -
func (w *workerCache) GetStatePodActionWithoutReload(funcSvcID, creator, stateID string) ([]PodAction, error) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get functionCache when handle state router update "+
			"without reload, funcSvcID: %s", funcSvcID)
		return nil, errors.New("failed to get functionCache when handle state router update")
	}
	fc.Lock()
	defer fc.Unlock()
	metadata := Metadata{Status: NotChanged}
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	metadata.Creator = creator
	metadata.StateID = stateID
	var stateAction []PodAction
	for _, instance := range fc.instances {
		if instance.StateID == stateID {
			var actions []Action
			actions = append(actions, Action{Type: ActionStatefulInstanceFlowOutDelete, NeedPull: 0,
				NeedReduce: []v1alpha1.WorkerInstance{instance}})
			stateAction = append(stateAction, PodAction{FuncSvc: funcSvcID, StatefulAction: actions,
				StatefulMeta: metadata, WorkerIns: instance})
			fc.stateFulEventCheckRepeat.Delete(stateID)
			fc.stateMap.Delete(stateID)
			log.GetLogger().Infof("success get state: %s pod: %s action without reload",
				stateID, instance.PodName)
		}
	}

	return stateAction, nil
}

// GetStatePodActionPullUp -
func (w *workerCache) GetStatePodActionPullUp(funcSvcID, creator, stateID string,
	cpu, memory int64) ([]PodAction, error) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get functionCache when handle state "+
			"router update with pull up, funcSvcID: %s", funcSvcID)
		return nil, errors.New("failed to get functionCache when handle state router update")
	}
	fc.Lock()
	defer fc.Unlock()
	metadata := Metadata{Status: NotChanged}
	metadata.Creator = creator
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	metadata.CreateOptions = &types.StateCreateOptions{
		CPU:    cpu,
		Memory: memory,
	}
	metadata.StateID = stateID
	var stateAction []PodAction
	var actions []Action
	fc.storeReloadingState(stateID)
	actions = append(actions, Action{Type: ActionStatefulInstanceRePull, NeedPull: 1})
	stateAction = append(stateAction, PodAction{FuncSvc: funcSvcID, StatefulAction: actions, StatefulMeta: metadata})
	log.GetLogger().Infof("success get state pod action pull up, funcSvcID: %s, stateID: %s", funcSvcID, stateID)
	return stateAction, nil
}

// GetStatePodActionReduce -
func (w *workerCache) GetStatePodActionReduce(funcSvcID, creator, stateID string) ([]PodAction, error) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get functionCache when handle state router update "+
			"with reduce, funcSvcID: %s", funcSvcID)
		return nil, errors.New("failed to get functionCache when handle state router update")
	}
	fc.Lock()
	defer fc.Unlock()
	metadata := Metadata{Status: NotChanged}
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	metadata.Creator = creator
	metadata.StateID = stateID
	var stateAction []PodAction
	for _, instance := range fc.instances {
		var actions []Action
		// only reduce the instance with event's stateID
		if instance.StateID == stateID {
			actions = append(actions, Action{Type: ActionReduce, NeedPull: 0,
				NeedReduce: []v1alpha1.WorkerInstance{instance}})
			stateAction = append(stateAction, PodAction{FuncSvc: funcSvcID, StatefulAction: actions,
				StatefulMeta: metadata, WorkerIns: instance})
			break
		}
	}
	log.GetLogger().Infof("success get state pod action reduce")
	return stateAction, nil
}

// ClearStateCacheWhenLabelsNotReady when labels not ready, we should clear fc.stateMap
func (w *workerCache) ClearStateCacheWhenLabelsNotReady(funcSvcID string,
	instance v1alpha1.WorkerInstance, event evs.Event) {
	// label is ready
	if event.EventType != evs.Deleted || instance.StateID != "" {
		return
	}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get functionCache when clear state cache"+
			"funcSvcID: %s, podName: %s", funcSvcID, instance.PodName)
		return
	}
	fc.Lock()
	defer fc.Unlock()
	if fc.funcMeta == nil || !fc.funcMeta.Spec.Configuration.IsStatefulFunction {
		return
	}
	log.GetLogger().Infof("stateful pod %s exited, we need clear state cache for %s", instance.PodName, funcSvcID)
	deleted := false
	fc.stateFulEventCheckRepeat.Range(func(stateID, value interface{}) bool {
		if podName, ok := value.(string); ok {
			if instance.PodName == podName {
				_, deletedEventMap := fc.stateFulEventCheckRepeat.LoadAndDelete(stateID)
				_, deletedStateMap := fc.stateMap.LoadAndDelete(stateID)
				_, deletedReloadingMap := fc.stateReloading.LoadAndDelete(stateID)
				log.GetLogger().Infof("find pod %s, clearStateCacheWhenLabelsNotReady of stateID: %s succeed, "+
					"deletedEventMap: %t, deletedStateMap: %t, deletedReloadingMap: %t",
					podName, stateID, deletedEventMap, deletedStateMap, deletedReloadingMap)
				deleted = true
				return false
			}
		}
		return true
	})
	if !deleted {
		log.GetLogger().Warnf("can not find stateID of podName: %s stateFulEventCheckRepeat, ignore it",
			instance.PodName)
	}
}
