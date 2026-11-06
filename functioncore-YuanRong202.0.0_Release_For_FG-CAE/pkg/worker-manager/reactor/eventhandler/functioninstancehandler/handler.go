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
	"sync"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	commutils "functioncore/pkg/common/utils"
	workercache "functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/cache/willdeleteworker"
	"functioncore/pkg/worker-manager/etcdhandler"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/strength"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
)

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
	handler.handlePoolPod(pod, event.EventType)
	if !utils.IsPoolPod(pod) && !utils.IsDsWorker(pod) {
		return handler.handleNonPoolPod(pod, event)
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

func (handler *FunctionInstanceHandler) handleNonPoolPod(pod *corev1.Pod, event evs.Event) error {
	if event.EventType == evs.Deleted && utils.IsReservedInstance(pod) {
		workercache.DeleteReserveInstanceTimestamp(pod.Name)
	}

	// specialized and not ready pods can't be restarted, worker-manager will delete it,
	// The following condition may occur: runtime or worker was killed by oom-killer.
	// It will send termination request to worker when pod is custom container pod and worker is ready.
	// If pod is custom container pod and worker is not ready, it will be deleted by force.
	// if pod is a bridge function pod, delete it by force
	if pod.DeletionTimestamp == nil && !utils.IsPodReady(pod) &&
		((!utils.IsCustomContainerPod(pod.Name) && !utils.IsTmpExpansionPod(pod.Name)) ||
			utils.IsWorkerReady(pod)) {
		log.GetLogger().Infof("worker turned to unready status and will be deleted with name: %s", pod.Name)
		if pod.Labels[utils.LabelIsBridgeFunction] == utils.TrueString {
			handler.podFetcher.DeletePod(pod, "deleteBridgePod-"+pod.Name)
		} else {
			handler.podFetcher.DeletePod(pod, "handleNonPoolPod-"+pod.Name)
		}
		return nil
	}
	// ignore pod with no owner to avoid adding worker into etcd with no owner ip
	funcSvcID, ok := utils.OwnedByFunctionService(pod)
	if !ok {
		if pod.DeletionTimestamp == nil {
			return nil
		}
		funcSvcID = handler.podFetcher.Queue.DeleteEventInQueue(pod.Name)
		log.GetLogger().Debugf("pod (%s) is deleted, so should delete pod in queue, funcSvcId %s",
			pod.Name, funcSvcID)
		if funcSvcID == "" {
			return nil
		}
	}
	// it will delete pat service pod when instance is already deleted
	if event.EventType == evs.Deleted {
		go handler.VpcHandler.IdleDeletePatServiceList(pod.Name)
	}
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

	return handler.dealWithReadyInstance(pod, event, funcSvcID)
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
	event evs.Event, funcSvcID string) error {
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
	if workerIns.StateID != "" && workerIns.OwnerIP == "" && pod.Status.HostIP != "" {
		workerIns.OwnerIP = pod.Status.HostIP
	}
	actions, metadata := workercache.WorkerCache.HandleInstance(funcSvcID,
		workercache.NewInstanceMeta(workerIns, event.EventType))
	handler.podFetcher.DoActions(funcSvcID, actions, metadata)
	if metadata.FuncMeta != nil && commutils.IsCAEFunc(metadata.FuncMeta.Spec.Configuration.BusinessType) {
		workerIns.BusinessType = constants.BusinessTypeCAE
	}
	if metadata.FuncMeta != nil && commutils.IsDirectFunc(metadata.FuncMeta.Spec.Configuration.Runtime.Language) {
		workerIns.IsDirectFunc = true
	}
	if metadata.FuncMeta != nil && len(metadata.FuncMeta.Spec.Configuration.Initializer.Handler) != 0 {
		workerIns.HasInitializer = true
	}
	if metadata.FuncMeta != nil && !workerIns.IsMCPFunc {
		workerIns.IsMCPFunc = metadata.FuncMeta.Spec.Configuration.IsMcpFunction
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
		go etcdhandler.DeleteInstanceFromETCD(context.Background(), workerIns)
	default:
		log.GetLogger().Errorf("unsupported event type %s", event.EventType)
	}

	return nil
}
