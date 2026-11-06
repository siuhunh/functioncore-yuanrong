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

// Package utils worker manager client
package utils

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/healthcheck"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
)

const (
	targetServiceName                             = "worker-manager"
	targetServicePort                             = "58866"
	defaultRequestWorkerManagerImageTimeout       = 25 * time.Minute
	defaultRequestWorkerManagerInstanceNumTimeout = 10 * time.Second
	dialTimeout                                   = 3 * time.Second
	idleConnTimeout                               = 90 * time.Second
	connKeepAlive                                 = 30 * time.Second
	maxIdleConns                                  = 100
	keyArrayLen                                   = 7
	clusterIDIndex                                = 4
	nodeIPIndex                                   = 5
	podNameIndex                                  = 6
)

var (
	once                   sync.Once
	httpClient             *http.Client
	workerManagerHealthCtx *healthcheck.Context
)

// GetContext return context with worker-manager's health
func GetContext() context.Context {
	if workerManagerHealthCtx == nil {
		return context.Background()
	}
	return workerManagerHealthCtx.GetContext()
}

// GetWorkerManagerClient -
func GetWorkerManagerClient() *http.Client {
	once.Do(func() {
		tr := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: connKeepAlive,
			}).DialContext,
			MaxIdleConns:        maxIdleConns,
			ForceAttemptHTTP2:   true,
			IdleConnTimeout:     idleConnTimeout,
			TLSHandshakeTimeout: dialTimeout,
		}
		httpClient = &http.Client{
			Timeout:   defaultRequestWorkerManagerImageTimeout,
			Transport: tr,
		}
		cfg, err := config.GetConfig()
		if err != nil {
			log.GetLogger().Errorf("failed to get configuration, %s", err.Error())
			return
		}
		if cfg.HTTPSConfig.HTTPSEnable {
			tr.TLSClientConfig = tls.GetClientTLSConfig()
		}
	})
	return httpClient
}

// WorkerMgrInstance -
type WorkerMgrInstance struct {
	ClusterID string
	PodName   string
	NodeIP    string
}

// GetWorkerMgrInsByKey /sn/worker-manager/instances/cluster001/10.247.13.123/worker-manager-6bc89d77fb-58w2d
func GetWorkerMgrInsByKey(key string) (*WorkerMgrInstance, error) {
	instanceInfoArr := strings.Split(key, "/")
	if len(instanceInfoArr) != keyArrayLen {
		log.GetLogger().Errorf("instance key invalid:%s", key)
		return nil, errors.New("instance key invalid" + key)
	}
	clusterID := instanceInfoArr[clusterIDIndex]
	if clusterID == "" {
		log.GetLogger().Errorf("clusters id can not be empty:%s", key)
		return nil, errors.New("clusters id can not be empty: " + key)
	}
	nodeIP := instanceInfoArr[nodeIPIndex]
	if nodeIP == "" {
		log.GetLogger().Errorf("NodeIP can not be empty:%s", key)
		return nil, errors.New("NodeIP can not be empty: " + key)
	}
	podName := instanceInfoArr[podNameIndex]
	if podName == "" {
		log.GetLogger().Errorf("PodName can not be empty:%s", key)
		return nil, errors.New("PodName can not be empty: " + key)
	}
	workerMgrInstance := &WorkerMgrInstance{
		ClusterID: clusterID,
		PodName:   podName,
		NodeIP:    nodeIP,
	}
	return workerMgrInstance, nil
}
