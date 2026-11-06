/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package common contains loadbalancer common define
package common

import (
	"time"

	"functioncore/pkg/common/api/schedulerapi"
)

var (
	// Creator -
	Creator RPCClientCreator
)

// RPCClientConfig -
type RPCClientConfig struct {
	IP             string
	Port           string
	ServerKey      string
	RequestTimeout time.Duration
}

// RPCClientCreator -
type RPCClientCreator func(RPCClientConfig) (RPCClient, error)

// RPCClient -
type RPCClient interface {
	Start()
	Stop()
	IsEnable() bool
	ServerKey() string
	schedulerapi.Scheduler
}

// CreateRPCClient -
func CreateRPCClient(config RPCClientConfig) (RPCClient, error) {
	return Creator(config)
}

// SetRPCClientCreator -
func SetRPCClientCreator(c RPCClientCreator) {
	Creator = c
}
