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

// Package utils implement tools of state-manager module
package utils

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strings"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/state-manager/config"
)

const (
	minLenOfSNIDFunc  = 3
	siteOfSNID        = 1
	defaultTypeKey    = "stateworkers"
	defaultBusinessID = "yrk"
	defaultVersion    = "$latest"
	defaultRetryDelay = 3
	// HTTPContentType define http content type
	HTTPContentType = "Content-Type"
	// ContentTypeJSON define http json content type
	ContentTypeJSON = "application/json; charset=UTF-8"
	// StateWorkerKey -
	StateWorkerKey = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/"
)

var (
	// ClusterID define the cluster ID info of current region
	clusterID = os.Getenv("CLUSTER_ID")
)

// GetServiceID get sn service id by function name
func GetServiceID(funcName string) string {
	if !strings.HasPrefix(funcName, urnutils.ServicePrefix) {
		return ""
	}
	fields := strings.Split(funcName, urnutils.TenantProductSplitStr)
	if len(fields) < minLenOfSNIDFunc {
		return ""
	}

	return fields[siteOfSNID]
}

// IsStateful check stateful by prefix of function name
func IsStateful(funcName string) bool {
	return strings.HasPrefix(funcName, urnutils.ServicePrefix)
}

// GetETCDPath get etcd path
func GetETCDPath(tenantID, proxyFunction, stateID string) string {
	var key etcdkey.EtcdKey
	key = &etcdkey.StateWorkersKey{
		TypeKey:    defaultTypeKey,
		BusinessID: defaultBusinessID,
		// tenant ID is same as bus proxy for 0
		TenantID: "0",
		Function: proxyFunction,
		Version:  defaultVersion,
		Zone:     utils.AzEnv(),
		StateID:  stateID,
	}
	return key.String()
}

// AuthCheck -
func AuthCheck(r *http.Request) error {
	requestSign := r.Header.Get(constants.HeaderAuthorization)
	timestamp := r.Header.Get(constants.HeaderAuthTimestamp)
	err := localauth.AuthCheckLocally(config.FConfigs.LocalAuth.AKey, config.FConfigs.LocalAuth.SKey,
		requestSign, timestamp, config.FConfigs.LocalAuth.Duration)
	if err != nil {
		log.GetLogger().Errorf("failed to check authorization locally, error: %s", err.Error())
		return err
	}
	return nil
}

// ErrorResponse -
func ErrorResponse(w http.ResponseWriter, code int, message string) error {
	w.Header().Set(HTTPContentType, ContentTypeJSON)
	w.WriteHeader(http.StatusInternalServerError)
	responseJSONBytes, err := json.Marshal(snerror.BadResponse{
		Code:    code,
		Message: message,
	})
	if _, err = w.Write(responseJSONBytes); err != nil {
		return err
	}
	return nil
}

// GetClusterID -
func GetClusterID() string {
	return clusterID
}

// GetSNStateWorkerPath -
func GetSNStateWorkerPath(stateID string) string {
	if stateID == "" {
		return ""
	}
	return StateWorkerKey + defaultVersion + "/" + utils.AzEnv() + "/" + stateID
}

// PutStateWorkersToETCDWithRetry -
func PutStateWorkersToETCDWithRetry(key string, value []byte) error {
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient.Client == nil {
		log.GetLogger().Warnf("failed to init state ETCD client")
		return errors.New("failed to init state ETCD client")
	}
	var err error
	for i := 1; i <= defaultRetryDelay; i++ {
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		err = stateClient.Put(defaultEtcdCtx, key, string(value))
		if err == nil {
			log.GetLogger().Infof("Succeed to put value to etcd, key: %s", key)
			return nil
		}
		if err != context.DeadlineExceeded {
			return err
		}
		log.GetLogger().Warnf("Put value to etcd with key %s timeout, try time %d", key, i)
		time.Sleep(defaultRetryDelay * time.Second)
	}
	return err
}
