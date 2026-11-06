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

// Package functioninstancehandler -
package functioninstancehandler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient/podevent"
	commutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	workercache "functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/cache/willdeleteworker"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/preload"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
	"functioncore/pkg/worker-manager/vpcmanager"
)

const (
	defaultCAERouterDeleteDelay = 180 * time.Second
)

var caeRouterDeleteDelay = parseCAERouterDeleteDelay()

func parseCAERouterDeleteDelay() time.Duration {
	env := os.Getenv("CAE_ROUTER_DELETE_DELAY")
	if env == "" {
		return defaultCAERouterDeleteDelay
	}
	val, err := strconv.Atoi(env)
	if err != nil || val <= 0 {
		return defaultCAERouterDeleteDelay
	}
	return time.Duration(val) * time.Second
}

// FunctionInstanceHandler define function service handler
type FunctionInstanceHandler struct {
	EtcdClient  *etcd3.EtcdClient
	podFetcher  *podfetcher.PodFetcher
	poolManager *poolmanager.PoolMgr
	VpcHandler  *vpcmanager.VpcHandler
	podLister   listercorev1.PodLister

	tmpDeletingPod sync.Map
}

// NewEventHandler new event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient, podFetcher *podfetcher.PodFetcher,
	podLister listercorev1.PodLister) *FunctionInstanceHandler {
	controller, err := vpcmanager.GetVPCController()
	if err != nil {
		fmt.Printf("failed to init vpc controller with error: %s and exit now\n", err.Error())
		log.GetLogger().Errorf("failed to init vpc controller with error: %s and exit now", err.Error())
		log.GetLogger().Sync()
		os.Exit(0)
	}
	poolManager := poolmanager.GetPoolManager()
	if poolManager == nil {
		log.GetLogger().Errorf("failed to get pool manager")
		return nil
	}

	handler := &FunctionInstanceHandler{
		podLister:   podLister,
		podFetcher:  podFetcher,
		poolManager: poolManager,
		EtcdClient:  etcdctl,
		VpcHandler:  controller,
	}
	return handler
}

// HandleEvent handle event
func (handler *FunctionInstanceHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.PodEventKind {
		return errors.New("failed to match")
	}

	log.GetLogger().Debugf("processing instance event, resource key: %s, type %s", event.KeyRef, event.EventType)
	namespace, name, err := cache.SplitMetaNamespaceKey(event.KeyRef)
	if err != nil {
		log.GetLogger().Errorf("invalid resource key: %s", event.KeyRef)
		return nil
	}

	// HandleEvent can have latencies. Get the latest "pod" struct from memory.
	pod, err := handler.podLister.Pods(namespace).Get(name)

	if err != nil {
		var ok bool
		if k8serrors.IsNotFound(err) {
			pod, ok = event.Object.(*corev1.Pod)
			if !ok {
				// Since Added and Updated events do not assign event.Object, we cleverly ignores the two types of
				// events when pod does not exist.
				return nil
			}
		} else {
			log.GetLogger().Errorf("failed to get pod %s/%s, err %s", namespace, name, err.Error())
			return err
		}
	}
	if utils.IsVPAPod(pod) {
		return handleVPAPodDelete(event, pod)
	}

	handler.handlePoolPod(pod, event.EventType)
	if !utils.IsPoolPod(pod) && !utils.IsDsWorker(pod) {
		return handler.handleNonPoolPod(pod, event)
	}

	return nil
}

func handleVPAPodDelete(event evs.Event, pod *corev1.Pod) error {
	if event.EventType == evs.Deleted && strings.Contains(pod.Name, utils.VpaPausePodPrefix) {
		req := vpa.GetPausePodResourceRequirements(pod.Name)
		nodeName := utils.GetVpaPausePodNodeName(pod)
		err := vpa.NewVpaPausePod().AddVpaPod(context.Background(), nodeName, req)
		if err != nil {
			log.GetLogger().Errorf("the delete vpa pod %s is failed to restart, err: %s", pod.Name, err.Error())
			return err
		}
		log.GetLogger().Infof("the delete vpa pod %s is succeed to restart in node: %s", pod.Name, nodeName)
	}
	return nil
}

func (handler *FunctionInstanceHandler) handlePoolPod(pod *corev1.Pod, eventType string) {
	log.GetLogger().Debugf("handle pool pod: %s, type %s", pod.Name, eventType)
	switch eventType {
	case evs.Added:
		handler.poolManager.AddPod(pod)
	case evs.Deleted:
		handler.poolManager.DeletePod(pod)
	case evs.Updated:
		handler.poolManager.UpdatePod(pod)
	default:
		log.GetLogger().Errorf("unsupported event type %s", eventType)
	}
}

func (handler *FunctionInstanceHandler) checkPodTurnedUnready(pod *corev1.Pod) bool {
	// specialized and not ready pods can't be restarted, worker-manager will delete it,
	// The following condition may occur: runtime or worker was killed by oom-killer.
	// It will send termination request to worker when pod is custom container pod and worker is ready.
	// If pod is custom container pod and worker is not ready, it will be deleted by force.
	// if pod is a bridge function pod, delete it by force
	if pod.DeletionTimestamp == nil && !utils.IsPodReady(pod) &&
		((!utils.IsCustomContainerPod(pod.Name) && !utils.IsTmpExpansionPod(pod.Name)) ||
			utils.IsWorkerReady(pod)) {
		log.GetLogger().Infof("worker turned to unready status and will be deleted with name: %s", pod.Name)
		handler.podFetcher.PrintPodEvents(pod.Name)
		if pod.Labels[utils.LabelIsBridgeFunction] == utils.TrueString {
			handler.podFetcher.DeletePod(pod, "deleteBridgePod-"+pod.Name)
		} else {
			handler.podFetcher.DeletePod(pod, "handleNonPoolPod-"+pod.Name)
		}
		preload.CheckAndReportWarn(pod)
		return true
	}
	return false
}

func (handler *FunctionInstanceHandler) deleteCAERouter(pod *corev1.Pod, event evs.Event, funcSvcID string,
	isCAE bool, ev podfetcher.CREvent) {
	// ev.GetFuncMeta() != nil ==> pod is unlabel and deleted, can't get ev from queue again when pod is acutal deleted,
	// so we should delete from etcd with delay
	if isCAE && (event.EventType == evs.Deleted || ev.GetFuncMeta() != nil) {
		workerIns := utils.MakeWorkerInsInfo(pod)
		fillWorkerInsInfo(&workerIns, funcSvcID, ev)
		if event.EventType != evs.Deleted {
			log.GetLogger().Infof("delete router for pod %s after %s", pod.Name, caeRouterDeleteDelay)
			time.AfterFunc(caeRouterDeleteDelay, func() {
				etcdhandler.DeleteInstanceFromETCD(context.Background(), workerIns)
			})
		} else {
			go etcdhandler.DeleteInstanceFromETCD(context.Background(), workerIns)
		}
	}
}

func (handler *FunctionInstanceHandler) handleNonPoolPod(pod *corev1.Pod, event evs.Event) error {
	if event.EventType == evs.Deleted && utils.IsReservedInstance(pod) {
		workercache.DeleteReserveInstanceTimestamp(pod.Name)
	}

	if commutils.GetEnableHCS() {
		// Confirm whether pod owner exists, belong to worker-manager
		calCurrentCPUUsage(pod, event)
	}

	if handler.checkPodTurnedUnready(pod) {
		return nil
	}

	var ev podfetcher.CREvent

	// ignore pod with no owner to avoid adding worker into etcd with no owner ip
	funcSvcID, ok := utils.OwnedByFunctionService(pod)
	if !ok {
		if pod.DeletionTimestamp == nil {
			return nil
		}
		ev = handler.podFetcher.Queue.DeleteEventInQueue(pod.Name)
		if ev.GetFuncMeta() != nil {
			funcSvcID = ev.GetFuncMeta().ID
		}
		log.GetLogger().Infof("pod (%s) is deleted, so should delete pod in queue, funcSvcId %s",
			pod.Name, funcSvcID)
		if funcSvcID == "" {
			return nil
		}
	}

	businessType, ok := pod.Labels[utils.LabelBusinessType]
	if !ok && ev.GetFuncMeta() != nil {
		businessType = ev.GetFuncMeta().Spec.Configuration.BusinessType
	}

	isCAE := commutils.IsCAEFunc(businessType)
	// it will delete pat service pod when instance is already deleted
	if event.EventType == evs.Deleted {
		go handler.VpcHandler.IdleDeletePatServiceList(pod.Name)
	}

	// check if need to delete router for cae instance
	handler.deleteCAERouter(pod, event, funcSvcID, isCAE, ev)

	// The pod has either started deleting or been deleted.
	if event.EventType == evs.Deleted || pod.DeletionTimestamp != nil || !utils.IsPodReady(pod) {
		if handler.filterOutRedundantDeletingEvent(pod.Name, event.EventType) {
			return nil
		}
		handler.dealWithNotReadyPod(pod, funcSvcID)
		log.GetLogger().Warnf("crId: %s, pod: %s is deleting and need to delete pod info from etcd",
			funcSvcID, pod.Name)
		event.EventType = evs.Deleted
	}

	return handler.dealWithReadyInstance(pod, event, funcSvcID, !isCAE, ev)
}

func calCurrentCPUUsage(pod *corev1.Pod, event evs.Event) {
	if event.EventType == evs.Deleted && config.EnableHCSLicense {
		_, ok := utils.OwnedByFunctionService(pod)
		if !ok {
			return
		}
		for i := range pod.Spec.Containers {
			if pod.Spec.Containers[i].Name == utils.RuntimeContainerName {
				utils.ReleaseCPUResource(pod.Spec.Containers[i].Resources.Limits.Cpu().MilliValue())
				log.GetLogger().Infof("pod is destroyed to release cpu resources: %d, current user function cpu "+
					"resource usage is %d", pod.Spec.Containers[i].Resources.Limits.Cpu().MilliValue(),
					config.CurrentCPUUsage)
				break
			}
		}
	}
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

func (handler *FunctionInstanceHandler) dealWithNotReadyPod(pod *corev1.Pod, funcSvcID string) {
	if pod.Labels[utils.LabelEnableTMPExpansion] == utils.TrueString {
		workercache.RecoverNodeRemainCapacityStorage(pod.Spec.NodeName, pod.Name)
		handler.podFetcher.DeletePodByForce(pod)
	}
	strength.GetPodCache().DelState(pod.Name)
	if utils.IsCustomContainerPod(pod.Name) && isNeedDeletePod(pod) {
		handler.podFetcher.DeletePodByK8S(pod.Name, "delete-not-ready-pod")
	}
	nodeInfo := workercache.GetNodeInfo(pod.Spec.NodeName)
	if nodeInfo != nil && !nodeInfo.IsReady {
		log.GetLogger().Warnf("node %s is not ready, it will delete pod %s by force for %s",
			pod.Spec.NodeName, pod.Name, funcSvcID)
		utils.ForciblyDeletePod(pod)
	}
}

func (handler *FunctionInstanceHandler) filterOutRedundantDeletingEvent(podName string, eventType string) bool {
	// If loaded, the deleting event is redundant.
	var loaded bool
	if eventType == evs.Deleted {
		_, loaded = handler.tmpDeletingPod.LoadAndDelete(podName)
	} else {
		_, loaded = handler.tmpDeletingPod.LoadOrStore(podName, nil)
	}
	return loaded
}

func (handler *FunctionInstanceHandler) dealWithReadyInstance(pod *corev1.Pod,
	event evs.Event, funcSvcID string, deleteRouter bool, ev podfetcher.CREvent) error {
	fetcher := podfetcher.GetPodFetcher()
	if fetcher == nil {
		return errors.New("pod fetcher is nil")
	}

	if pod.Labels[utils.LabelApplier] == constants.PredictionApplier && event.EventType != evs.Deleted {
		// Because the predicted instance metadata is corev1.Pod, it needs to be updated here
		workercache.WorkerCache.UpdatePrewarmInstancesIfNotExist(funcSvcID, pod)
	}

	// cal actions by worker cache and do action to pull up reserved instances or reduce instances
	workerIns := utils.MakeWorkerInsInfo(pod)
	fillWorkerInsInfo(&workerIns, funcSvcID, ev)
	if workerIns.StateID != "" && workerIns.OwnerIP == "" && pod.Status.HostIP != "" {
		workerIns.OwnerIP = pod.Status.HostIP
	}
	workercache.WorkerCache.ClearStateCacheWhenLabelsNotReady(funcSvcID, workerIns, event)
	actions, metadata := workercache.WorkerCache.HandleInstance(funcSvcID,
		workercache.NewInstanceMeta(workerIns, event.EventType))
	handler.podFetcher.DoActions(funcSvcID, actions, metadata)
	if metadata.FuncMeta != nil && commutils.IsCAEFunc(metadata.FuncMeta.Spec.Configuration.BusinessType) {
		workerIns.BusinessType = constants.BusinessTypeCAE
		workerIns.EnableWebsocket = metadata.FuncMeta.Spec.Configuration.EnableWebsocket
	}
	if metadata.FuncMeta != nil && commutils.IsDirectFunc(pod, metadata.FuncMeta.Spec.Configuration.Runtime.Language) {
		workerIns.IsDirectFunc = true
	}
	if metadata.FuncMeta != nil && len(metadata.FuncMeta.Spec.Configuration.Initializer.Handler) != 0 {
		workerIns.HasInitializer = true
	}
	if metadata.FuncMeta != nil && !workerIns.IsBridgeFunc {
		workerIns.IsBridgeFunc = metadata.FuncMeta.Spec.Configuration.IsBridgeFunction
	}
	if pod.Labels[utils.LabelApplier] == constants.PredictionApplier {
		return nil
	}

	ctx := handler.podFetcher.CreateNewContext(funcSvcID)
	switch event.EventType {
	case evs.Added, evs.Updated:
		go etcdhandler.PutInstanceToETCD(ctx, workerIns)
	case evs.Deleted:
		// delete data in the  will delete worker cache
		willdeleteworker.GetCache().DeleteWillDeleteWorkerInstance(funcSvcID, workerIns.InstanceID)
		functionService := workercache.WorkerCache.GetFuncMeta(funcSvcID)
		podevent.ReportScaleDownCaeEvent(pod, functionService)
		if deleteRouter {
			go etcdhandler.DeleteInstanceFromETCD(context.Background(), workerIns)
		}
	default:
		log.GetLogger().Errorf("unsupported event type %s", event.EventType)
	}

	return nil
}

func fillWorkerInsInfo(workerIns *v1alpha1.WorkerInstance, funcSvcID string, ev podfetcher.CREvent) {
	if workerIns != nil && workerIns.CreationTime == 0 {
		log.GetLogger().Warnf("pod labels is not ready, filling worker info by function cache, "+
			"workerPodName: %s", workerIns.PodName)
		funcMeta := ev.GetFuncMeta()
		if funcMeta != nil {
			workerIns.BusinessType = funcMeta.Spec.Configuration.BusinessType
			workerIns.BusinessID = funcMeta.Spec.Configuration.BusinessID
			workerIns.TenantID = funcMeta.Spec.Configuration.TenantID
			workerIns.FuncName = funcMeta.Spec.Configuration.FunctionName
			workerIns.Version = funcMeta.Spec.Configuration.FunctionVersion
			log.GetLogger().Infof("workerIns updated, BusinessType:%s, BusinessID:%s, TenantID:%s, FuncName:%s, "+
				"Version:%s, InstanceID:%s", workerIns.BusinessType, workerIns.BusinessID, workerIns.TenantID,
				workerIns.FuncName, workerIns.Version, workerIns.InstanceID)
		} else {
			log.GetLogger().Warnf("funcMeta not found for %s, filling worker info by function cache failed", funcSvcID)
		}
	}
}
