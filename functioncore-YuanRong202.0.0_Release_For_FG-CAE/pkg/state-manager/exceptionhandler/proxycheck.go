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

// Package exceptionhandler handling exceptions
package exceptionhandler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/resourcemanager"
	"functioncore/pkg/state-manager/scheduler"
	"functioncore/pkg/state-manager/state/instancemanager"
)

const (
	workerInstancePrefix     = "/sn/workers"
	functiontaskSign         = "function-task"
	workerInstanceEtcdKeyLen = 13
	keepConnAliveTTL         = 3 * time.Second
	tenantStringNum          = 5
	tenantNum                = 6
	functionStringNum        = 7
	functionNameNum          = 8
	versionStringNum         = 9
	nodeIDNum                = 12
)

// InitProxyChecker init proxy checker
func InitProxyChecker(stopChan <-chan struct{}) error {
	if err := initAvailableNodeMap(); err != nil {
		log.GetLogger().Errorf("init AvailableMap error: %s", err.Error())
		return err
	}
	// start worker instance watcher
	stateClient := etcd3.GetStateEtcdClient()
	go watchBusProxy(stateClient, stopChan)

	return nil
}

func initAvailableNodeMap() error {
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("failed to init state ETCD client")
		return nil
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := stateClient.GetResponse(defaultEtcdCtx, workerInstancePrefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("list path(%s) error: %s", workerInstancePrefix, err.Error())
		return err
	}
	for _, kv := range resp.Kvs {
		// parse nodeID from key
		nodeID := parseNodeIDFromKey(string(kv.Key))
		// add node info into availableNodes
		scheduler.AddAvailableNodes(nodeID, kv.Value)
		if !isEventKeyValid(strings.Split(string(kv.Key), "/")) {
			continue
		}
		// add node info into instanceMap
		functionName := parseFunctionNameFromKey(string(kv.Key))
		scheduler.AddInstanceMap(functionName, nodeID, kv.Value)
	}
	return nil
}

func watchBusProxy(stateClient *etcd3.EtcdClient, stopChan <-chan struct{}) {
	watchChan := createWatcher(stateClient)
	log.GetLogger().Infof("begin to watch bus proxy instances")
	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("the channel received the result may be closed and try to reconnect")
				watchChan = stateClient.Watch(context.Background(), workerInstancePrefix, 0, true)
				continue
			}

			splitKeys := strings.Split(event.Key, "/")
			if isEventKeyValid(splitKeys) {
				updateFunctionInstanceMap(event)
			}

			if !isBusProxyInstance(event.Key) {
				continue
			}

			err := processBusProxyEvent(event)
			if err != nil {
				log.GetLogger().Warnf("failed to process busproxy event %v: %s", event, err.Error())
				continue
			}
		case _, ok := <-stopChan:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed")
			}
			return
		}
	}
}

func createWatcher(stateClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if stateClient.Client == nil {
				log.GetLogger().Warnf("failed to init state etcd client")
				continue
			}
			watchChan := stateClient.Watch(context.Background(), workerInstancePrefix, 0, true)
			ticker.Stop()
			return watchChan
		}
	}
}

func isBusProxyInstance(key string) bool {
	splitKeys := strings.Split(key, "/")
	if !isEventKeyValid(splitKeys) {
		return false
	}
	if len(splitKeys) > tenantNum && splitKeys[tenantNum] != "0" {
		return false
	}
	return true
}

func isEventKeyValid(splitKeys []string) bool {
	if len(splitKeys) != workerInstanceEtcdKeyLen {
		return false
	}
	if splitKeys[tenantStringNum] != "tenant" || splitKeys[functionStringNum] != "function" ||
		splitKeys[versionStringNum] != "version" {
		return false
	}
	return true
}

func parseNodeIDFromKey(key string) string {
	if !isBusProxyInstance(key) {
		return ""
	}
	splitKeys := strings.Split(key, "/")
	return splitKeys[len(splitKeys)-1]
}

func parseFunctionNameFromKey(key string) string {
	splitKeys := strings.Split(key, "/")
	if len(splitKeys) != workerInstanceEtcdKeyLen {
		return ""
	}
	return splitKeys[functionNameNum]
}

func processBusProxyEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		err := processAddEvent(event)
		if err != nil {
			return err
		}
	case etcd3.DELETE:
		err := processDeleteEvent(event)
		if err != nil {
			return err
		}
	case etcd3.ERROR:
		log.GetLogger().Warnf("etcd error event: %s", event.Value)
	default:
		log.GetLogger().Warnf("unsupported event: %s", event.Value)
	}
	return nil
}

func processAddEvent(event *etcd3.Event) error {
	nodeID, err := extractNodeID(event)
	if err != nil {
		return err
	}
	scheduler.AddAvailableNodes(nodeID, event.Value)
	cpuCapacity := resourcemanager.GetNodeCPUMilliValue(nodeID)
	memoryCapacity := resourcemanager.GetNodeMemoryMilliValue(nodeID)

	if cpuCapacity == 0 || memoryCapacity == 0 {
		resourcemanager.ClearNodeMap(nodeID)
		return nil
	}

	resourcemanager.AddNodeToNodeMap(nodeID, cpuCapacity, memoryCapacity)
	log.GetLogger().Infof("add node %s to nodemap when proxy is online", nodeID)
	return nil
}

func processDeleteEvent(event *etcd3.Event) error {
	nodeID, err := extractNodeID(event)
	if err != nil {
		log.GetLogger().Errorf("failed to extract the nodeID from the etcd event: %s", err.Error())
		return err
	}

	isNodeInCluster := true
	_, err = resourcemanager.GetCPU(nodeID)
	if err != nil {
		log.GetLogger().Infof("The node %s is not in this cluster or not ready, reason: %s", nodeID, err.Error())
		isNodeInCluster = false
	}

	scheduler.DelAvailableNodes(nodeID)
	resourcemanager.ClearNodeMap(nodeID)

	funcName, err := extractFunctionName(event)
	if err != nil {
		log.GetLogger().Errorf("failed to extract the function name from the etcd event: %s", err.Error())
		return err
	}

	if !clusterstatusutil.IsCurrentClustersStatusRunning() {
		log.GetLogger().Infof("This cluster is not running, no need to processFNTaskOffline")
		return nil
	}

	if funcName == functiontaskSign {
		// Only cluster with this node need to compete for state
		if !isNodeInCluster {
			log.GetLogger().Infof("No need to processFNTaskOffline now, nodeId %s", nodeID)
			nodeStates, getStateIDsError := instancemanager.GetNodeStateID(nodeID)
			if getStateIDsError != nil {
				// we can only wait for other clusters to operate, just log error
				log.GetLogger().Errorf("failed to get node assigned data: %s", getStateIDsError.Error())
				return nil
			}
			for stateID := range nodeStates {
				go instancemanager.ConstructTimerForCompetitionOfStateID(stateID, nodeID)
			}
			return nil
		}
		log.GetLogger().Infof("start to process the functiontask offline event: %s", event.Key)
		err = processFNTaskOffline(nodeID)
		if err != nil {
			log.GetLogger().Errorf("failed to process the functiontask offline event: %s", err.Error())
			return err
		}
	}
	return nil
}

func processFNTaskOffline(nodeID string) error {
	nodeStates, err := instancemanager.GetNodeStateID(nodeID)
	if err != nil {
		log.GetLogger().Errorf("failed to get node assigned data: %s", err.Error())
		return err
	}

	if nodeStates != nil {
		for stateID, _ := range nodeStates {
			err = instancemanager.CompeteStateAndTryToUpdate(stateID, nodeID)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// updateFunctionInstanceMap update minFunctionInstance Map
func updateFunctionInstanceMap(event *etcd3.Event) {
	nodeID, err := parseNodeID(event.Value)
	if err != nil {
		return
	}
	functionName, err := extractFunctionName(event)
	if err != nil {
		log.GetLogger().Errorf("failed to add function instance to Map,extractFunctionName Error: %s",
			err.Error())
		return
	}
	switch event.Type {
	case etcd3.PUT:
		scheduler.AddInstanceMap(functionName, nodeID, event.Value)
	case etcd3.DELETE:
		scheduler.DeleteInstanceMap(functionName, nodeID)
	default:
		return
	}
}

func parseNodeID(eventValue []byte) (string, error) {
	var valueMap map[string]string
	err := json.Unmarshal(eventValue, &valueMap)
	if err != nil {
		return "", err
	}
	if nodeID, ok := valueMap["nodeName"]; ok && nodeID != "" {
		return valueMap["nodeName"], nil
	}
	return "", nil
}

func extractNodeID(event *etcd3.Event) (string, error) {
	splitKeys := strings.Split(event.Key, "/")
	if len(splitKeys) != workerInstanceEtcdKeyLen {
		return "", errors.New("error length worker instance")
	}
	return splitKeys[nodeIDNum], nil
}

func extractFunctionName(event *etcd3.Event) (string, error) {
	splitKeys := strings.Split(event.Key, "/")
	if len(splitKeys) != workerInstanceEtcdKeyLen {
		return "", errors.New("invalid length of worker instance")
	}
	return splitKeys[functionNameNum], nil
}

func judgeFNTaskOffline(event *etcd3.Event) (string, error) {
	splitKeys := strings.Split(event.Key, "/")
	if len(splitKeys) != workerInstanceEtcdKeyLen {
		return "", errors.New("invalid length of the worker instance key")
	}
	return splitKeys[functionNameNum], nil
}
