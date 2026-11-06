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

// Package utils defines utils used for autoscaler
package utils

import (
	"context"
	"errors"

	"go.etcd.io/etcd/client/v3"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

// InitETCDClient -
func InitETCDClient(stopCh <-chan struct{}) error {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get cfg with error: %s", err.Error())
		return err
	}
	return etcd3.InitParam().
		WithStopCh(stopCh).
		WithAlarmSwitch().
		WithCaeRouterEtcdConfig(cfg.RouteEtcd).
		WithCAEMetaEtcdConfig(cfg.CAEMetaEtcd).
		WithCAEMetaEtcdProxyConfig(cfg.CAEMetaEtcdProxy).
		InitClient()
}

// GetFunctionListFromEtcd -
func GetFunctionListFromEtcd() (*clientv3.GetResponse, error) {
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || etcd3.GetCAEMetaEtcdClient().Client == nil {
		log.GetLogger().Error("failed to get cae meta etcd client, it is nil!")
		return nil, errors.New("failed to get cae meta etcd client, it is nil")
	}
	resp, err := client.GetResponse(defaultCtx, etcd3.HandleCellPrefix(FunctionMetadataPrefix), clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("failed to connect to etcd, err: %s", err.Error())
		return nil, err
	}
	return resp, nil
}
