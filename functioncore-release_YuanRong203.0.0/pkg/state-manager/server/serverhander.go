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

package server

import (
	"errors"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/cluster"
	"functioncore/pkg/common/logger/log"
	commonTLS "functioncore/pkg/common/tls"
	"functioncore/pkg/state-manager/config"
)

// Handler define api server handler
type Handler struct {
	Registry       *gin.Engine
	Conf           *config.FConfig
	Discovery      cluster.Discovery
	Name           string
	AuthCheck      bool
	ForwardRequest bool
}

const modLockName = "state-manager"

// NewServerHandler New Server Handler
func NewServerHandler() (*Handler, error) {
	handler := Handler{
		Name:           "state-managerserver",
		Registry:       nil,
		AuthCheck:      false,
		ForwardRequest: false,
		Discovery:      nil,
	}

	return &handler, nil
}

// GetRole Write Discovery Role
func (a *Handler) GetRole(w http.ResponseWriter, r *http.Request) {
	role := a.Discovery.Role()
	_, err := w.Write([]byte(role + "\n"))
	if err != nil {
		log.GetLogger().Errorf("writing a role error: %s", err.Error())
	}
}

// GetLeader Write Discovery Leader
func (a *Handler) GetLeader(w http.ResponseWriter, r *http.Request) {
	leader := a.Discovery.Leader()
	if leader == nil {
		_, err := w.Write([]byte("Not found leader" + "\n"))
		if err != nil {
			log.GetLogger().Errorf("writing error: %s", err.Error())
			return
		}
	} else {
		_, err := w.Write([]byte(leader.Host + "\n"))
		if err != nil {
			log.GetLogger().Errorf("writing the leader host error: %s", err.Error())
			return
		}
	}
}

// ServeHTTP return Registry Router Serve HTTP
func (a *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/role" || r.URL.Path == "/leader" || r.URL.Path == "/health" {
		// use node self api
		a.Registry.ServeHTTP(w, r)
		return
	}

	if a.Discovery.Role() != cluster.ClusterRoleLeader && a.ForwardRequest {
		a.Discovery.ForwardRequest(w, r)
		return
	}

	a.Registry.ServeHTTP(w, r)
}

func createClusterDiscovery(conf *config.FConfig, stopCh <-chan struct{},
	wg *sync.WaitGroup) (cluster.Discovery, error) {
	if conf == nil {
		return nil, errors.New("conf is nil when create clusterDiscovery")
	}
	if !conf.Cluster.IsClusterMode {
		return cluster.NewSingleNodeCluster(conf.Address, stopCh, wg, log.GetLogger())
	}

	// default use lease resource lock for leader elect
	if conf.HTTPSConfig.HTTPSEnable {
		tlsConfig := commonTLS.GetClientTLSConfig()
		return cluster.NewResourceLockCluster(conf.Address, stopCh, modLockName, tlsConfig)
	}
	return cluster.NewResourceLockCluster(conf.Address, stopCh, modLockName, nil)
}
