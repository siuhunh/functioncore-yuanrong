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

// Package utils worker manager client
package utils

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/functionbus/common"
)

const (
	targetServiceName = "worker-manager.%s.svc.cluster.local"
	targetServicePort = "58866"
	commonTimeout     = 3 * time.Second
	// defaultRequestWorkerTimeout timeout for requesting worker from worker-manager.
	// reserve 33s for worker-manager to generalize worker and 3s for communication
	defaultRequestWorkerTimeout = 33*time.Second + commonTimeout
	// request to workerManager only for custom image
	defaultRequestWorkerManagerImageTimeout = 25 * time.Minute
	appID                                   = "ondemand"
	dialTimeout                             = 3 * time.Second
	idleConnTimeout                         = 90 * time.Second
	connKeepAlive                           = 30 * time.Second
	maxIdleConns                            = 100
)

var (
	once       sync.Once
	httpClient *http.Client
)

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
		if common.FConfig.HTTPSConfig.HTTPSEnable {
			tr.TLSClientConfig = tls.GetClientTLSConfig()
		}

		httpClient = &http.Client{
			Timeout:   defaultRequestWorkerManagerImageTimeout,
			Transport: tr,
		}
	})
	return httpClient
}

// GetWorkerManagerBaseURL -
func GetWorkerManagerBaseURL() string {
	scheme := tls.GetURLScheme(common.FConfig.HTTPSConfig.HTTPSEnable)
	targetServiceWithNameSpace := fmt.Sprintf(targetServiceName, os.Getenv(constants.PodNamespaceEnvKey))
	return scheme + "://" + targetServiceWithNameSpace + ":" + targetServicePort
}

// FillInWorkerManagerRequestHeaders contains authorization, source and so on
func FillInWorkerManagerRequestHeaders(request *http.Request) {
	authorization, timestamp := GenerateAuthorization()
	request.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	request.Header.Set(constants.Authorization, authorization)
	request.Header.Set(constants.HeaderEventSourceID, appID)
	request.Header.Set(constants.HeaderCallType, "active")
}

// GenerateAuthorization for request to worker manager
func GenerateAuthorization() (string, string) {
	var authorization, timestamp string
	authorization, timestamp = localauth.SignLocally(common.FConfig.LocalAuth.AKey,
		common.FConfig.LocalAuth.SKey, appID, common.FConfig.LocalAuth.Duration)
	return authorization, timestamp
}
