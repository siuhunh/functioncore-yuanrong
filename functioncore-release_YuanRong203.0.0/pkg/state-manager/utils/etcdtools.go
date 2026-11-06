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

// Package utils has some etcd tools, get the etcd client
package utils

import (
	"os"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/state-manager/config"
)

// StateEtcd define etcd handler
type StateEtcd struct {
	EtcdClt *etcd3.EtcdClient
}

// NewStateEtcd new etcd handler
func NewStateEtcd() (*StateEtcd, error) {
	stateEtcd := &StateEtcd{
		EtcdClt: etcd3.GetStateEtcdClient(),
	}
	return stateEtcd, nil
}

// InitETCDClient -
func InitETCDClient(stopCh <-chan struct{}, cfg *config.FConfig) error {
	if os.Getenv("MICROSERVICE_CLUSTER_ENABLE") == "true" {
		return etcd3.InitParam().
			WithStopCh(stopCh).
			WithAlarmSwitch().
			WithMetaEtcdConfig(cfg.EtcdCfg).
			WithRouterEtcdConfig(&cfg.RouterETCD).
			WithMetaEtcdProxyConfig(cfg.MetaEtcdProxy).
			WithCaeRouterEtcdConfig(cfg.StateEtcdCfg).
			WithCaeRouterEtcdConfig(cfg.CAERouterEtcd).
			WithCAEMetaEtcdConfig(cfg.CAEMetaEtcd).
			InitClient()
	}
	return etcd3.InitParam().
		WithStopCh(stopCh).
		WithAlarmSwitch().
		WithMetaEtcdConfig(cfg.EtcdCfg).
		WithRouterEtcdConfig(&cfg.RouterETCD).
		WithMetaEtcdProxyConfig(cfg.MetaEtcdProxy).
		WithCaeRouterEtcdConfig(cfg.CAERouterEtcd).
		WithCAEMetaEtcdConfig(cfg.CAEMetaEtcd).
		InitClient()
}
