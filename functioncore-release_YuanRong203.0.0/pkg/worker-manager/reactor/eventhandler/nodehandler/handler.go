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

// Package nodehandler -
package nodehandler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/preload"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
)

// NodeHandler define function service handler
type NodeHandler struct {
	// Routing Etcd Client
	EtcdClient *etcd3.EtcdClient
	StopCh     <-chan struct{}
}

// NodeInfo stores node info
type NodeInfo struct {
	NodeIP             string            `json:"nodeIP"`
	AllocatableCPU     int64             `json:"allocatableCPU"`
	AllocatableMemory  int64             `json:"allocatableMemory"`
	AllocatableStorage int64             `json:"allocatableStorage"`
	Label              map[string]string `json:"label"`
}

// NewEventHandler new event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient, stopCh <-chan struct{}) *NodeHandler {
	handler := &NodeHandler{
		EtcdClient: etcdctl,
		StopCh:     stopCh,
	}
	return handler
}

func containsScheduleLabel(labels map[string]string) bool {
	for _, value := range labels {
		if strings.HasSuffix(value, "Schedule") {
			return true
		}
	}
	return false
}

func needUpdateFuncScheduleNode(node *corev1.Node) bool {
	oldNode := cache.GetNodeInfo(node.Name)
	if oldNode == nil {
		return containsScheduleLabel(node.Labels)
	}

	// check schedule labels of new node and old node are same
	oldLabelCount := 0
	for key, value := range oldNode.Label {
		if !strings.HasSuffix(value, "Schedule") {
			continue
		}
		oldLabelCount++

		// label updated or removed
		if node.Labels[key] != value {
			return true
		}
	}

	// check if new labels added
	newLabelCount := 0
	for _, value := range node.Labels {
		if strings.HasSuffix(value, "Schedule") {
			newLabelCount++
		}
	}

	return oldLabelCount != newLabelCount
}

// HandleEvent handle event
func (handler *NodeHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.NodeEventKind {
		return errors.New("failed to match")
	}

	log.GetLogger().Debugf("processing node event, resource key: %s, type %s", event.KeyRef, event.EventType)
	node, ok := event.Object.(*corev1.Node)
	if !ok {
		log.GetLogger().Errorf("invalid resource type: %v", reflect.TypeOf(event.Object))
		return fmt.Errorf("invalid resource type: %v", reflect.TypeOf(event.Object))
	}

	needUpdate := needUpdateFuncScheduleNode(node)

	switch event.EventType {
	case evs.Added:
		poolmanager.AddNode(node)
		cache.AddNode(node)
		vpa.NewVpaPausePod().AddVpaPod(context.Background(), node.Name, nil)
		if node.Labels[constants.NodeRoleLabelKey] != "" {
			go preload.GetClient().AddAllPreloadToOneNode(node)
		}
	case evs.Updated:
		poolmanager.UpdateNode(node)
		cache.UpdateNode(node)
	case evs.Deleted:
		poolmanager.DeleteNode(node)
		cache.DeleteNode(node)
		monitoring.DeleteNodeCPUInfoFromRedis(utils.GetNodeIPAddress(node))
		pausePodName := utils.VpaPausePodPrefix + node.Name
		vpa.NewVpaPausePod().DeleteVpaPod(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: pausePodName}})
		vpa.RemovePausePodResourceRequirements(pausePodName)
	default:
		log.GetLogger().Errorf("unsupported event type %s", event.EventType)
		return errors.New("unsupported event type")
	}
	if needUpdate {
		cache.WorkerCache.UpdateFuncScheduleNodes(node)
	}
	manager := poolmanager.GetPoolManager()
	if manager != nil {
		manager.ProcessPoolScaleLimitUpdate()
	}
	return nil
}
