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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snerror"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
)

// PullType instance type to pull
type PullType int

type willDeleteCheckTask struct {
	timeout        int
	workerInstance v1alpha1.WorkerInstance
	closeCh        chan interface{}
	podFetcher     *PodFetcher
}

func (w *willDeleteCheckTask) run() {
	timer := time.NewTimer(time.Second * time.Duration(w.timeout))
	defer timer.Stop()
	defer willDeleteTasks.Delete(w.workerInstance.InstanceID)
	select {
	case <-w.closeCh:
		log.GetLogger().Infof("will delete check for %s has been canceled", w.workerInstance.InstanceID)
	case <-timer.C:
		traceID := "terminate-" + uuid.New().String()
		log.GetLogger().Warnf("will delete check timeout for %s, clear instance and etcd, traceID:%s",
			w.workerInstance.PodName, traceID)
		if err := w.podFetcher.DeletePodByK8S(w.workerInstance.PodName, traceID); err != nil {
			log.GetLogger().Warnf("delete pod %s by k8s failed:%s, traceID:%s",
				w.workerInstance.PodName, err.Error(), traceID)
		}
	}
}

func (w *willDeleteCheckTask) cancel() {
	willDeleteTasks.Delete(w.workerInstance.InstanceID)
	close(w.closeCh)
}

// enum of pull type
const (
	Reserved = PullType(iota)
	CAERollingOnDemand
	percent = 100
	// cae request timeout 900s, gap 20s
	willDeleteCheckTimeout = 920
)

var (
	pullTypeName   = [...]string{"reserved", "rolling-demand"}
	pullApplier    = [...]string{constants.WorkerManagerApplier, constants.ASBResApplier}
	pullErrChecker = [...]func(error) bool{
		func(err error) bool {
			return err != nil
		},
		func(err error) bool {
			if err == nil || errors.Is(err, tools.ErrReachMaxInstanceNum) ||
				errors.Is(err, tools.ErrReachMaxOnDemandInstanceNumPerTenant) {
				return false
			}
			return true
		},
	}
	willDeletedInstance         = sync.Map{}
	willDeleteTasks             = sync.Map{}
	willDeleteCheckTaskCancelCh = make(chan string, 10)
	clearWillDeletePodRecord    = 3600
)

func init() {
	go willDeleteCheckTaskLoop()
	go clearWillDeleteInsRecord(signals.WaitForSignal())
}

func willDeleteCheckTaskLoop() {
	for {
		select {
		case instanceID, ok := <-willDeleteCheckTaskCancelCh:
			if !ok {
				log.GetLogger().Errorf("will delete task cancel channel has been closed")
				return
			}
			cancelWillDeleteTask(instanceID)
		}
	}
}

func cancelWillDeleteTask(instanceID string) {
	deleteCheckTaskIf, ok := willDeleteTasks.Load(instanceID)
	if ok {
		deleteCheckTask, ok := deleteCheckTaskIf.(*willDeleteCheckTask)
		if ok {
			deleteCheckTask.cancel()
		}
	}
}

// CancelWillDeleteTask used for cancel will delete check task
func CancelWillDeleteTask(instanceID string) {
	willDeleteCheckTaskCancelCh <- instanceID
}

// String get literal name for PullType
func (p PullType) String() string {
	if p < 0 || int(p) >= len(pullTypeName) {
		return fmt.Sprintf("invalid pullType(%d)", int(p))
	}
	return pullTypeName[p]
}

// Applier get applier for PullType
func (p PullType) Applier() string {
	if p < 0 || int(p) >= len(pullApplier) {
		return fmt.Sprintf("invalid pullType(%d)", int(p))
	}
	return pullApplier[p]
}

// CheckPullErr checks whether pull error should consider as failure
func (p PullType) CheckPullErr(err error) bool {
	if p < 0 || int(p) >= len(pullErrChecker) {
		return true
	}
	return pullErrChecker[p](err)
}

// PullUpInstance pull up reserved instance or rolling instance
func (p *PodFetcher) PullUpInstance(functionMeta *faasfunction.FunctionService, pullType PullType) error {
	request := tools.PackRequestInfo(functionMeta, pullType.Applier(), pullType.String()+"-"+uuid.New().String())

	log.GetLogger().Infof("begin to pull up %s instance for func: %s, name: %s, force: %s, traceID: %s", pullType,
		functionMeta.Spec.Configuration.FunctionName, request.FunctionName, request.ForceDeploy, request.TraceID)
	pod, err := p.WithContext(p.GeneralizePodWithRetry, request, functionMeta)
	if err != nil {
		log.GetLogger().Warnf("failed to pull up %s instance for func: %s with error: %s, traceID: %s",
			pullType, functionMeta.Spec.Configuration.FunctionName, err.Error(), request.TraceID)
		return err
	}
	log.GetLogger().Infof("succeed to pull up %s instance with pod name: %s for func: %s, traceID: %s", pullType,
		pod.Name, functionMeta.Spec.Configuration.FunctionName, request.TraceID)

	return nil
}

func (p *PodFetcher) provision(functionMeta *faasfunction.FunctionService,
	needPullNums int, crSignature string, delay time.Duration) {
	ctx, ok := p.GetContext(functionMeta.ID)
	if !ok {
		return
	}
	failedNums, exceedLimitNum := p.ProvisionWorkerPod(functionMeta, needPullNums, crSignature, Reserved)
	if exceedLimitNum > 0 {
		actions, metadata := cache.WorkerCache.HandleFailure(functionMeta.ID,
			cache.NewPullFailureMeta(exceedLimitNum, crSignature, delay))
		p.DoActions(functionMeta.ID, actions, metadata)
	}
	failedNums -= exceedLimitNum
	if failedNums > 0 {
		select {
		case <-ctx.Done():
			log.GetLogger().Warnf("cancel to pull up reserved instance for: %s with nums: %d",
				functionMeta.ID, failedNums)
			return
		case <-time.After(delay):
			// sleep 200ms, 400ms, 800ms, 1600ms, ..., 10s, 10s, 10s, ...
			log.GetLogger().Infof("retry to pull up reserved instance for: %s with nums: %d after %v",
				functionMeta.ID, failedNums, delay)
			// generate actions for pull failure event
			actions, metadata := cache.WorkerCache.HandleFailure(functionMeta.ID,
				cache.NewPullFailureMeta(failedNums, crSignature, delay))
			p.DoActions(functionMeta.ID, actions, metadata)
		}
	}
}

// DoActions do actions
func (p *PodFetcher) DoActions(funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
	if metadata.Status == cache.IsDeleted {
		p.CancelAndDeleteContext(funcSvcID)
		go p.DeleteCAEBusInstanceMutexes(funcSvcID)
	} else if metadata.Status == cache.IsChanged {
		p.CancelAndCreateNewContext(funcSvcID)
	} else {
		// create new context if it is not exists
		p.CreateNewContext(funcSvcID)
	}

	// do action
	for i, action := range actions {
		switch action.Type {
		case cache.ActionMigrateInstance:
			go p.migrateInstance(metadata, action.MigratedIns, action.PodTemplate)
		case cache.ActionIdleModeChanged:
			for _, instance := range action.NeedIdle {
				ctx, retryNum := p.IdleRetryInfo.updateCtxAndRetryNum(instance.PodName)
				go p.SendIdleModeRequestByRetry(ctx, instance, action.IdleMode,
					"idle-"+uuid.New().String(), retryNum)
			}
		case cache.ActionPull:
			p.ScaleUpIns(funcSvcID, action, metadata)
		case cache.ActionStatefulInstanceRePull:
			go p.statefulInstanceReload(funcSvcID, action, metadata)
		case cache.ActionMicroServiceInstanceRePull:
			go p.microServiceInstanceReload(funcSvcID, action, metadata)
		case cache.ActionReducePrewarm:
			go p.ScaleDownPrewarmPod(funcSvcID, action.PrewarmPod)
		case cache.ActionPullPrewarm:
			go p.ScaleUpPrewarmPods(funcSvcID, action.ScaleUpPrewarmPodNum)
		case cache.ActionRollingUpdate:
			go p.RollingAction(funcSvcID, actions[i], metadata)
		default:
		}

		functionService := cache.WorkerCache.GetFuncMeta(funcSvcID)

		// reduce pods
		for _, instance := range action.NeedReduce {
			p.reduceInstance(functionService, funcSvcID, instance)
		}

	}
}

// ScaleUpIns -
func (p *PodFetcher) ScaleUpIns(funcSvcID string, action cache.Action, metadata cache.Metadata) {
	if action.NeedPull <= 0 {
		return
	}
	if metadata.FuncMeta == nil {
		funcInfo := metadata.FuncInfo
		log.GetLogger().Warnf("funcMeta is nil for %s it will query silent function from etcd", funcSvcID)
		cache.UpdateSilentFuncCache(funcInfo.FuncName, funcInfo.Business, funcInfo.Tenant, funcInfo.Version)
		metadata.FuncMeta = cache.WorkerCache.GetFuncMeta(funcSvcID)
		if metadata.FuncMeta == nil {
			log.GetLogger().Errorf("funcMeta is nil for %s", funcSvcID)
			return
		}
		metadata.CRSignature = tools.GetCRSignature(metadata.FuncMeta)
	}
	// pull up reserved instances or pull trigger pod
	if metadata.Delay == 0 {
		metadata.Delay = tools.BaseDelayForReservedIns
	}
	go p.provision(metadata.FuncMeta, action.NeedPull, metadata.CRSignature, metadata.Delay)
}

// RollingAction -
func (p *PodFetcher) RollingAction(funcSvcID string, action cache.Action, metadata cache.Metadata) {
	ctx, ok := p.GetContext(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("context not exits for func %s when rolling update", funcSvcID)
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(1 + 1)

	go func() {
		p.Rolling(ctx, action.RollingReserved, action.RollingPercentage, Reserved, metadata)
		wg.Done()
	}()
	go func() {
		p.Rolling(ctx, action.RollingOnDemand, action.RollingPercentage, CAERollingOnDemand, metadata)
		wg.Done()
	}()
	wg.Wait()
	metadata.Status = cache.NotChanged

	remained := cache.WorkerCache.GetAndClearRollingInstance(funcSvcID, metadata.CRSignature)
	if len(remained) != 0 {
		log.GetLogger().Infof("rolling update finished for %s, start to reduce remained instance", funcSvcID)
		p.DoActions(funcSvcID, []cache.Action{{Type: cache.ActionReduce, NeedReduce: remained}}, metadata)
	}
}

// Rolling pull rolling instance needed
func (p *PodFetcher) Rolling(ctx context.Context, totalNeed, percentage int, pullType PullType,
	metadata cache.Metadata) {
	delay := tools.BaseDelayForReservedIns

	rollingStep := commonutils.Max(totalNeed*percentage/percent, 1)
	rolling := rollingStep
	if totalNeed <= 0 {
		return
	}

	log.GetLogger().Infof("start to rolling %s for func %s witch total num %d and step %d", pullType,
		metadata.FuncMeta.ID, totalNeed, rolling)

	for totalNeed > 0 {
		select {
		case <-ctx.Done():
			log.GetLogger().Infof("rolling %s context done for %s", pullType, metadata.FuncMeta.ID)
			return
		default:
		}

		if rolling == 0 {
			rolling = rollingStep
		}
		rolling = commonutils.Min(rolling, totalNeed)
		failed, _ := p.ProvisionWorkerPod(metadata.FuncMeta, rolling, metadata.CRSignature, pullType)
		totalNeed -= rolling - failed
		rolling = failed
		if failed == 0 {
			delay = tools.BaseDelayForReservedIns
			continue
		}

		if !cache.WorkerCache.NeedReRolling(metadata.FuncMeta.ID, &cache.PullFailureMeta{FailedNums: totalNeed,
			CRSignature: metadata.CRSignature, IsRollingReserved: pullType == Reserved}) {
			log.GetLogger().Infof("no need to rolling %s for func %s with num %d",
				pullType, metadata.FuncMeta.ID, totalNeed)
			return
		}

		timer := time.NewTimer(delay)

		select {
		case <-ctx.Done():
			log.GetLogger().Infof("rolling %s context done for %s", pullType, metadata.FuncMeta.ID)
			timer.Stop()
			return
		case <-timer.C:
		}
		delay = commonutils.Min(delay<<1, tools.MaxDelayForReservedIns)
	}
}

// ProvisionWorkerPod provision worker pod and return failed nums
func (p *PodFetcher) ProvisionWorkerPod(functionMeta *faasfunction.FunctionService, requiredMinInsNum int,
	crSignature string, pullType PullType) (int, int) {
	var wg sync.WaitGroup
	var failedNums int32
	var exceedLimitedNums int32
	startTime := time.Now()
	if tools.IsPullTriggerFunction(functionMeta) {
		log.GetLogger().Infof("get vpc_pullTrigger request for func: %s", functionMeta.ID)
		if err := p.VpcHandler.ProvisionPullTriggerPod(functionMeta, int32(requiredMinInsNum)); err != nil {
			log.GetLogger().Errorf("failed to pull up trigger deploy for %s with %s", functionMeta.ID, err.Error())
			// failedNums is 1 and exceedLimitedNums is 0, it will retry to pull up pull trigger by retry after interval
			return 1, 0
		}
		return 0, 0
	}

	log.GetLogger().Infof("start to pull up %s instances with desired nums: %d for func: %s", pullType,
		requiredMinInsNum, functionMeta.ID)

	pullUpFunc := func() {
		err := p.PullUpInstance(functionMeta, pullType)
		if err != nil {
			if e, ok := err.(snerror.SNError); ok && e.Code() == snerror.GettingPodErrorCode &&
				e.Error() == poolmanager.ErrRequestExceedLimit.Error() {
				atomic.AddInt32(&exceedLimitedNums, 1)
			}
			if pullType.CheckPullErr(err) {
				atomic.AddInt32(&failedNums, 1)
			}
		}
		cache.WorkerCache.SetMinScaleError(functionMeta.ID, crSignature, err)
		cache.WorkerCache.BroadcastScaleResult(functionMeta.ID, crSignature, err)
		wg.Done()
	}

	for i := 0; i < requiredMinInsNum; i++ {
		wg.Add(1)
		go pullUpFunc()
	}
	wg.Wait()
	log.GetLogger().Infof("finished to pull up %s instances with failed nums: %d exceed nums: %d "+
		"requiredMinInsNum is %d for func:  %s with sig: %s, cost time: %s", pullType,
		atomic.LoadInt32(&failedNums), atomic.LoadInt32(&exceedLimitedNums), requiredMinInsNum,
		functionMeta.ID, crSignature, time.Since(startTime).String())

	return int(atomic.LoadInt32(&failedNums)), int(atomic.LoadInt32(&exceedLimitedNums))
}

func (p *PodFetcher) migrateInstance(metadata cache.Metadata, migratedIns v1alpha1.WorkerInstance,
	podTemplate *corev1.Pod) {
	if metadata.FuncMeta == nil {
		return
	}
	traceID := "migrating-" + uuid.New().String()
	log.GetLogger().Infof("Start to migrateInstance, funcSvcID: %s, stateID: %s, traceID: %s",
		metadata.FuncMeta.ID, migratedIns.StateID, traceID)
	// migrating pod to other node
	request := tools.PackRequestInfo(metadata.FuncMeta, migratedIns.Applier, traceID)
	request.CPU, request.Memory = int64(migratedIns.CPU), int64(migratedIns.Memory)
	// fc.funcMeta.StateID is empty if the function instance has not been reloaded
	request.StateID = migratedIns.StateID
	// this request will not be limited by max instance
	request.IsMigrating = true
	if podTemplate != nil {
		request.PodTemplate = podTemplate
		request.MigratedFrom = migratedIns.PodName
	}
	var err error
	var generalizedIns *corev1.Pod
	var isSucceedToMigrate bool
	defer func() {
		monitoring.AddMigratingMetrics(isSucceedToMigrate)
	}()
	// if stateful function and not bridge, delete first
	if migratedIns.StateID != "" && !migratedIns.IsBridgeFunc {
		p.deleteWorkerInsOfStatefulFunc(metadata.FuncMeta.ID, traceID, migratedIns)
		// stateful function will reload, no need to generalize a new instance
		isSucceedToMigrate = true
		return
	}
	for i := 0; i < maxRetries; i++ {
		if i != 0 {
			time.Sleep(retryInterval)
		}
		generalizedIns, err = p.WithContext(p.GeneralizePod, request, metadata.FuncMeta)
		if err == nil {
			break
		}
	}
	if err != nil {
		if migratedIns.Applier == constants.WorkerManagerApplier {
			p.SetBillingSwitchToWorker(migratedIns, true, traceID)
			// scaling reserved rollback
			cache.WorkerCache.HandleMigratingInstanceFailed(metadata.FuncMeta.ID,
				cache.NewInstanceMeta(migratedIns, eventsource.Updated))
		}
		log.GetLogger().Errorf("failed to migrate pod %s with traceID %s and error %s",
			migratedIns.PodName, traceID, err.Error())
		return
	}
	if generalizedIns == nil {
		log.GetLogger().Errorf("failed to migrate pod %s with nil ins, traceID %s", migratedIns.PodName, traceID)
		return
	}
	isSucceedToMigrate = true
	cache.WorkerCache.SetMigratingInstanceToDone(metadata.FuncMeta.ID, migratedIns.PodName, traceID)
	log.GetLogger().Infof("succeed to migrate pod %s to %s on node %s with traceID %s",
		migratedIns.PodName, generalizedIns.Name, generalizedIns.Spec.NodeName, traceID)
	if migratedIns.Applier != constants.WorkerManagerApplier {
		if cfg, err := config.GetConfig(); err == nil && cfg.MarkDeleteConfig.EnableFeature &&
			migratedIns.BusinessType == constants.BusinessTypeCAE {
			serviceID := metadata.FuncMeta.ID
			actions, metadata := cache.WorkerCache.HandleInstance(serviceID,
				cache.NewInstanceMeta(migratedIns, eventsource.Deleted))
			p.DoActions(serviceID, actions, metadata)
			if err := p.markWorkerInstanceWillDelete(migratedIns,
				cfg.MarkDeleteConfig.WillDeleteReservedTime); err == nil {
				return
			}
		}
		// delete router etcd、worker cache and delete pod when succeed to migrate normal instance
		// prefer to delete migrated instance in worker cache when succeed to migrate reserved instance
		// delete pod by k8s error will be added to queue to retry
		p.DeletePodByWorkerInstance(metadata.FuncMeta.ID, tools.FunctionNameSpace(), traceID, migratedIns)
	}
}

func (p *PodFetcher) reduceInstance(functionService *faasfunction.FunctionService,
	funcSvcID string, instance v1alpha1.WorkerInstance) {
	traceID := "terminate-" + uuid.New().String()
	workerID := constants.DefaultAZ + "-#-" + instance.PodName
	if functionService != nil && len(cache.WorkerCache.GetFunctionPreStopHandler(funcSvcID)) != 0 {
		p.SendUpdateAgencyInfoRequestToWorker(instance, traceID, workerID, functionService, instance.PodName)
	}
	// @See DTS2024082314358
	if cfg, err := config.GetConfig(); err == nil && cfg.MarkDeleteConfig.EnableFeature &&
		commonutils.IsCAEFunc(instance.BusinessType) {
		if err := p.markWorkerInstanceWillDelete(instance, cfg.MarkDeleteConfig.WillDeleteReservedTime); err == nil {
			return
		}
	}
	go p.DeletePodByK8S(instance.PodName, traceID)
	if functionService != nil && !commonutils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) {
		go etcdhandler.DeleteInstanceFromETCD(context.Background(), instance)
	}
}

func clearWillDeleteInsRecord(stopCh <-chan struct{}) {
	ticker := time.NewTicker(time.Duration(clearWillDeletePodRecord) * time.Second)
	defer ticker.Stop()
	// In order to avoid put worker instance to route etcd, we need to clear will delete record after a long time.
	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()
			willDeletedInstance.Range(func(instanceID, timestampIf any) bool {
				timestamp, ok := timestampIf.(int64)
				if !ok {
					return true
				}
				if now-int64(clearWillDeletePodRecord) > timestamp {
					willDeletedInstance.Delete(instanceID)
				}
				return true
			})
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warn("WillDeleteInstance clean task exit")
				return
			}
		}
	}
}

func (p *PodFetcher) runWillDeleteTask(workerInstance v1alpha1.WorkerInstance, reservedTime int) {
	deleteCheckTaskIf, ok := willDeleteTasks.LoadOrStore(workerInstance.InstanceID, &willDeleteCheckTask{
		timeout:        willDeleteCheckTimeout,
		workerInstance: workerInstance,
		closeCh:        make(chan interface{}),
		podFetcher:     p,
	})
	if !ok {
		deleteCheckTask, ok := deleteCheckTaskIf.(*willDeleteCheckTask)
		if ok {
			go deleteCheckTask.run()
		}
	}
	if reservedTime > 0 {
		clearWillDeletePodRecord = reservedTime
	}
}

func (p *PodFetcher) markWorkerInstanceWillDelete(workerInstance v1alpha1.WorkerInstance, reservedTime int) error {
	if _, loaded := willDeletedInstance.LoadOrStore(workerInstance.InstanceID, time.Now().Unix()); loaded {
		log.GetLogger().Warnf("WorkerInstance %s has been marked will delete, do nothing", workerInstance.InstanceID)
		return nil
	}
	key := tools.GetWorkerKeyByInstance(workerInstance)
	log.GetLogger().Infof("mark WorkerInstance of %s will deleted", key)
	workerInfo, err := etcdhandler.QueryETCDWithRetry(context.Background(), key)
	if err != nil {
		log.GetLogger().Warnf("failed to query etcd with key:%s, :%s", key, err.Error())
		return err
	}
	workerIns := &v1alpha1.WorkerInstance{}
	err = json.Unmarshal([]byte(workerInfo), workerIns)
	if err != nil {
		log.GetLogger().Warnf(fmt.Sprintf("failed to unmarshal WorkerInstance, key:%s", key))
		return err
	}
	p.runWillDeleteTask(workerInstance, reservedTime)
	// @see utils.GetWorkerKeyByInstance
	workerIns.BusinessID = workerInstance.BusinessID
	workerIns.TenantID = workerInstance.TenantID
	workerIns.FuncName = workerInstance.FuncName
	workerIns.Version = workerInstance.Version
	workerIns.InstanceID = workerInstance.InstanceID
	workerIns.WillDelete = true
	err = etcdhandler.PutInstanceToETCDWithErr(context.Background(), *workerIns, etcdhandler.UpdateOperation)
	if err != nil {
		log.GetLogger().Warnf(fmt.Sprintf("failed to update WorkerInstance, key:%s, reason:%s", key, err.Error()))
	} else {
		log.GetLogger().Infof(fmt.Sprintf("mark worker %s instance will delete success", key))
	}
	return err
}

func (p *PodFetcher) statefulInstanceReload(funcSvcID string, action cache.Action, metadata cache.Metadata) {
	if action.NeedPull <= 0 {
		return
	}
	if metadata.FuncMeta == nil {
		log.GetLogger().Warnf("funcMeta is nil for %s", funcSvcID)
		return
	}
	// pull up reserved instances or pull trigger pod
	if metadata.Delay == 0 {
		metadata.Delay = tools.BaseDelayForReservedIns
	}

	if _, err := p.WithContext(p.GeneralizePod, makeStatefulInsReloadRequestInfo(metadata),
		metadata.FuncMeta); err != nil {
		log.GetLogger().Errorf("reload stateful: %s instance, stateID: %s error: %s",
			funcSvcID, metadata.StateID, err.Error())
	}
}

func (p *PodFetcher) microServiceInstanceReload(funcSvcID string, action cache.Action, metadata cache.Metadata) {
	if action.NeedPull <= 0 {
		return
	}
	if metadata.FuncMeta == nil {
		log.GetLogger().Warnf("funcMeta is nil for %s", funcSvcID)
		return
	}

	log.GetLogger().Debugf("Try to reload micro service instance: %s, businessType: %s, needPull: %d",
		funcSvcID, metadata.FuncMeta.Spec.Configuration.BusinessType, action.NeedPull)

	if metadata.Delay == 0 {
		metadata.Delay = tools.BaseDelayForReservedIns
	}

	if _, err := p.WithContext(p.GeneralizePod, makeMicroServiceReloadRequestInfo(metadata),
		metadata.FuncMeta); err != nil {
		log.GetLogger().Warnf("reload microservice function: %s instance, error: %s",
			funcSvcID, err.Error())
	}
}

func makeStatefulInsReloadRequestInfo(metadata cache.Metadata) *config.RequestInfo {
	requestInfo := &config.RequestInfo{
		CPU:             metadata.FuncMeta.Spec.Configuration.Resource.CPU,
		Memory:          metadata.FuncMeta.Spec.Configuration.Resource.Mem,
		Namespace:       tools.FunctionNameSpace(),
		BusinessID:      metadata.FuncMeta.Spec.Configuration.BusinessID,
		TenantID:        metadata.FuncMeta.Spec.Configuration.TenantID,
		FunctionName:    metadata.FuncMeta.Spec.Configuration.FunctionName,
		FunctionVersion: metadata.FuncMeta.Spec.Configuration.FunctionVersion,
		URN:             metadata.FuncMeta.Spec.Configuration.FunctionURN,
		Creator:         metadata.Creator,
		Applier:         constants.FunctionTask,
		ForceDeploy:     "false",
		TraceID:         uuid.New().String(),
		StartTime:       time.Now(),
		StateID:         metadata.StateID,
		IsReload:        true,
	}
	if metadata.CreateOptions != nil {
		requestInfo.CPU = metadata.CreateOptions.CPU
		requestInfo.Memory = metadata.CreateOptions.Memory
	}
	log.GetLogger().Infof("makeStatefulInsReloadRequestInfo, traceID %s, stateID: %s",
		requestInfo.TraceID, requestInfo.StateID)
	return requestInfo
}

func makeMicroServiceReloadRequestInfo(metadata cache.Metadata) *config.RequestInfo {
	return &config.RequestInfo{
		CPU:             metadata.FuncMeta.Spec.Configuration.Resource.CPU,
		Memory:          metadata.FuncMeta.Spec.Configuration.Resource.Mem,
		Namespace:       tools.FunctionNameSpace(),
		BusinessID:      metadata.FuncMeta.Spec.Configuration.BusinessID,
		TenantID:        metadata.FuncMeta.Spec.Configuration.TenantID,
		FunctionName:    metadata.FuncMeta.Spec.Configuration.FunctionName,
		FunctionVersion: metadata.FuncMeta.Spec.Configuration.FunctionVersion,
		URN:             metadata.FuncMeta.Spec.Configuration.FunctionURN,
		Applier:         constants.ASBResApplier,
		ForceDeploy:     "false",
		TraceID:         uuid.New().String(),
		StartTime:       time.Now(),
		StateID:         metadata.StateID,
		IsReload:        true,
	}
}
