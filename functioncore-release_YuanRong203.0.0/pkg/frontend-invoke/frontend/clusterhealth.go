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

package frontend

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	healthy    = "healthy"
	unhealthy  = "unhealthy"
	subhealthy = "subhealthy"
	unknown    = "unknown"

	task             = "functiontask"
	instanceManager  = "instancemanager"
	functionAccessor = "functionaccessor"

	proxyPort = "22423"

	defaultHealthTimeOut = 5 * time.Second
)

var components = [...]string{task, instanceManager, functionAccessor}

type healthyStatus string

// ClusterHealth -
func ClusterHealth(w http.ResponseWriter, r *http.Request) {
	resultMap := make(map[string]healthyStatus, len(components))

	initComponentsStatus(resultMap)
	if !etcd3.GetRouterEtcdClient().GetEtcdStatusLostContact() {
		resultMap[functionAccessor] = subhealthy
		resultMap[task] = unknown
		resultMap[instanceManager] = unknown
	}
	if authCheck(r) != nil {
		w.WriteHeader(http.StatusUnauthorized)
		log.GetLogger().Errorf("failed to return auth error")
		return
	}

	// check task and instance manager
	err := checkCoreComponentsHealth(resultMap, r)
	if err != nil {
		log.GetLogger().Errorf("failed to check component health %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		bytes, err := json.Marshal(resultMap)
		if err != nil {
			_, err := w.Write([]byte(err.Error()))
			if err != nil {
				log.GetLogger().Errorf("failed to write rsp err %s", err.Error())
			}
		}
		_, err = w.Write(bytes)
		if err != nil {
			log.GetLogger().Errorf("failed to write rsp err %s", err.Error())
		}
		return
	}
	w.WriteHeader(http.StatusOK)
	return
}

func initComponentsStatus(resultMap map[string]healthyStatus) {
	for _, component := range components {
		resultMap[component] = healthy
	}
}

func checkCoreComponentsHealth(resultMap map[string]healthyStatus, r *http.Request) error {
	if resultMap == nil {
		return fmt.Errorf("failed to check, result map is nil")
	}

	if resultMap[functionAccessor] == subhealthy {
		return errors.New("frontend health status is subhealthy")
	}

	if functionlb.NodeRoute.GetNodeNum() == 0 {
		resultMap[task] = unhealthy
		resultMap[instanceManager] = unknown
		return errors.New("no available proxy to request")
	}
	return sendClusterHealthCheckToTask(resultMap, r)
}

func sendClusterHealthCheckToTask(resultMap map[string]healthyStatus, r *http.Request) error {
	// Traverse all nodes. If one node is available, the cluster is available.
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	var err error
	isHealthy, isStatusInternalServerError := false, false
	functionlb.NodeRoute.NodeList.Range(func(key, value any) bool {
		node, ok := value.(*functionlb.Node)
		if !ok {
			return true
		}
		setRequest(req, r, node)
		client := util.GetGlobalClient()
		err = client.DoTimeout(req, resp, defaultHealthTimeOut)
		if err != nil {
			log.GetLogger().Errorf("failed to request proxy %s, err %s", req.URI().String(), err.Error())
			return true
		}
		if resp == nil {
			return true
		}
		if resp.StatusCode() == http.StatusInternalServerError {
			log.GetLogger().Errorf("failed to request proxy, err %s ", string(resp.Body()))
			if string(resp.Header.Peek(constants.HeaderRouterEtcdState)) == "false" {
				resultMap[instanceManager] = subhealthy
			} else {
				resultMap[instanceManager] = unhealthy
			}
			err = fmt.Errorf("failed to check instance manager healthy")
			isStatusInternalServerError = true
			return false
		}
		if resp.StatusCode() == http.StatusOK {
			err = nil
			isHealthy = true
			return false
		}
		err = fmt.Errorf("failed to request proxy")
		log.GetLogger().Errorf("failed to request proxy err code %d uri %s", resp.StatusCode(), req.URI().String())
		return true
	})
	if !isHealthy && !isStatusInternalServerError {
		// all tasks are unhealthy
		resultMap[task] = unhealthy
		resultMap[instanceManager] = unknown
	}
	return err
}

func setRequest(req *fasthttp.Request, r *http.Request, node *functionlb.Node) {
	req.Header.SetMethod(http.MethodGet)
	req.Header.Set(constants.HeaderAuthorization, r.Header.Get(constants.HeaderAuthorization))
	req.Header.Set(constants.HeaderAuthTimestamp, r.Header.Get(constants.HeaderAuthTimestamp))
	req.Header.ResetConnectionClose()
	req.SetRequestURI("/componenthealth")
	req.Header.SetHost(fmt.Sprintf("%s:%s", node.NodeIP, proxyPort))
	req.URI().SetScheme(tls.GetURLScheme(constant.FConfig.HTTPSConfig.HTTPSEnable))
}
