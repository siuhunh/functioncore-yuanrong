/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package stream process streaming data
package stream

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/frontend-invoke/constant"
)

const (
	// InstanceKey stream handler etcd registry key
	InstanceKey = "/sn/workflow/streamhandler/instances/ips/"

	// healthCheckDefaultPort grpc stream handler health check default port
	healthCheckDefaultPort = "28917"
	healthCheckVersion     = "v1.0"

	// healthCheckTimeout health check timeout
	healthCheckTimeout  = 10 * time.Second
	healthCheckDuration = 30 * time.Second

	failureThreshold = 3
)

var (
	manager = &ServiceManage{}
)

// ServiceManage Stream Handlers service manager
type ServiceManage struct {
	Mu                sync.RWMutex
	NodesIP           []string
	HealthNodes       []string
	CheckFailureCount int
	Alarm             bool
}

// InitDiscovery init stream-handler discovery
func InitDiscovery(exit <-chan struct{}) error {
	routeEtcd := etcd3.GetRouterEtcdClient()
	if routeEtcd == nil || routeEtcd.Client == nil {
		log.GetLogger().Errorf("failed to init stream discovery, route etcd client is nil")
		return errors.New("route etcd client is nil")
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	streamServiceSlice, err := routeEtcd.GetValues(defaultEtcdCtx, InstanceKey)
	if err != nil {
		log.GetLogger().Warnf("failed to init stream discovery with "+
			"get key %s error: %s", InstanceKey, err.Error())
		return err
	}
	if len(streamServiceSlice) > 0 {
		manager.Mu.Lock()
		if err = json.Unmarshal([]byte(streamServiceSlice[0]), &manager.NodesIP); err != nil {
			log.GetLogger().Warnf("failed to Unmarshal streamServiceSlice")
		}
		manager.Mu.Unlock()
	}
	alarm.GetClient().RegisterAlarmType(alarm.StreamHandlerConnection)
	siftHealthStreamHandlers()
	go doHealthCheck(exit)

	return nil
}

// GetRandomStreamsAddr get random streams nodes
func GetRandomStreamsAddr() (string, error) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	manager.Mu.RLock()
	defer manager.Mu.RUnlock()
	if len(manager.HealthNodes) == 0 {
		return "", errors.New("there are no healthy streamHandler instance")
	}
	return manager.HealthNodes[r.Intn(len(manager.HealthNodes))], nil
}

// doHealthCheck  check stream node health
func doHealthCheck(exit <-chan struct{}) {
	if exit == nil {
		log.GetLogger().Warn("empty exit channel")
		return
	}
	log.GetLogger().Info("start to check stream node health")
	ticker := time.NewTicker(healthCheckDuration)
	defer ticker.Stop()
	for {
		select {
		case <-exit:
			return
		case <-ticker.C:
			siftHealthStreamHandlers()
		}
	}
}

func siftHealthStreamHandlers() {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{
		Timeout:   healthCheckTimeout,
		Transport: tr,
	}
	healthCheckPort := func() string {
		if constant.FConfig.StreamConfig.HealthCheckPort == "" {
			return healthCheckDefaultPort
		}
		return constant.FConfig.StreamConfig.HealthCheckPort
	}

	healthCheck := func(nodeIP string) bool {
		resp, err := client.Get(fmt.Sprintf("https://%s:%s/%s/healthcheck",
			nodeIP, healthCheckPort(), healthCheckVersion))
		if err != nil {
			log.GetLogger().Warnf("failed to do health check of node %s, err: %s", nodeIP, err)
			return false
		}
		defer resp.Body.Close()
		return true
	}

	manager.Mu.RLock()
	nodesIPs := manager.NodesIP
	manager.Mu.RUnlock()
	var healthNodes []string
	var unHealthNodes []string
	checkStatus := true
	for _, nodeIP := range nodesIPs {
		if len(nodeIP) == 0 {
			continue
		}
		if net.ParseIP(nodeIP) == nil {
			log.GetLogger().Warnf("invalid IP address %s", nodeIP)
			continue
		}

		if !healthCheck(nodeIP) {
			unHealthNodes = append(unHealthNodes, nodeIP)
			checkStatus = false
			continue
		}
		healthNodes = append(healthNodes, nodeIP)
	}
	handleHealthCheckAlarm(unHealthNodes, checkStatus)
	manager.Mu.Lock()
	manager.HealthNodes = healthNodes
	manager.Mu.Unlock()
}

func handleHealthCheckAlarm(nodes []string, status bool) {
	if status && manager.Alarm {
		alarm.GetClient().ClearAlarm(alarm.StreamHandlerConnection)
		manager.Alarm = false
		return
	}
	if !status {
		manager.CheckFailureCount++
		if manager.CheckFailureCount >= failureThreshold && !manager.Alarm {
			alarm.GetClient().ReportAlarm(alarm.StreamHandlerConnection,
				fmt.Sprintf("failed to do health check of streamHandler nodes %v", nodes))
			manager.Alarm = true
		}
	}
}

// ProcessStreamEvent -
func ProcessStreamEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		if event.Value == nil || len(event.Value) == 0 {
			manager.Mu.Lock()
			manager.NodesIP = []string{}
			manager.HealthNodes = []string{}
			manager.Mu.Unlock()
			return nil
		}
		var nodeIPs []string
		if err := json.Unmarshal(event.Value, &nodeIPs); err != nil {
			log.GetLogger().Errorf("failed to Unmarshal streamHandler event, err: %s", err.Error())
			return err
		}
		manager.Mu.Lock()
		manager.NodesIP = nodeIPs
		log.GetLogger().Infof("process streamHandler add event, stream nodes: %v", manager.NodesIP)
		manager.Mu.Unlock()
		siftHealthStreamHandlers()
		return nil
	case etcd3.DELETE:
		log.GetLogger().Infof("process streamHandler delete event")
		manager.Mu.Lock()
		manager.NodesIP = []string{}
		manager.HealthNodes = []string{}
		manager.Mu.Unlock()
		return nil
	case etcd3.ERROR:
		return errors.New("get unexpected result when watching etcd")
	default:
		return errors.New("undefined etcd event")
	}
}

// IsStreamEvent -
func IsStreamEvent(key string) bool {
	return strings.Contains(key, InstanceKey)
}
