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

// Package constant constant var and error code
package constant

import "functioncore/pkg/common/snerror"

const (
	// RequestParamInvalid error code of invalid request param
	RequestParamInvalid = 170001
	// InternalServerError error code of internal server error
	InternalServerError = 170002
)

// CodeText key is error code and value is error message
var CodeText = map[int]string{
	RequestParamInvalid:          "request param invalid",
	InternalServerError:          "internal system error",
	snerror.RequestAuthCheckFail: snerror.ErrText(snerror.RequestAuthCheckFail),
	snerror.FunctionNotStateful:  snerror.ErrText(snerror.FunctionNotStateful),
}

const (
	// AlarmServerAddress -
	AlarmServerAddress = "ALARM_SERVER_ADDRESS"
	// StateCounterCfg -
	StateCounterCfg = "STATE_COUNTER_CFG"
	// Scheduler -
	Scheduler = "SCHEDULER"
	// Cluster -
	Cluster = "CLUSTER"
	// KubeCfgTimeout -
	KubeCfgTimeout = "KUBE_CFG_TIMEOUT"
	// Address -
	Address = "ADDRESS"
	// Log -
	Log = "LOG"
	// LocalAuth -
	LocalAuth = "LOCAL_AUTH"
	// HTTPSConfig -
	HTTPSConfig = "HTTPS_CONFIG"
	// Etcd -
	Etcd = "ETCD"
	// RouteEtcd -
	RouteEtcd = "ROUTER_ETCD"
	// CAERouterEtcd cae router etcd
	CAERouterEtcd = "CAE_ROUTER_ETCD"
	// CAEMetaEtcd cae meta etcd
	CAEMetaEtcd = "CAE_META_ETCD"
	// StateEtcd -
	StateEtcd = "STATE_ETCD"
	// MetaEtcdProxy -
	MetaEtcdProxy = "META_ETCD_PROXY"
	// ScaleData -
	ScaleData = "SCALE_DATA"
)
