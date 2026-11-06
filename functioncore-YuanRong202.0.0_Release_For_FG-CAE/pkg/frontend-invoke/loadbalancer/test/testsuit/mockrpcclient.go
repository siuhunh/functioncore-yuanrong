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

// Package testsuit contains test tool for loadbalancer api test
package testsuit

import (
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
)

type mockRPCClient struct {
	scheduler   schedulerapi.Scheduler
	retainError error
}

func createMockRPCClient(scheduler schedulerapi.Scheduler,
	_ common.RPCClientConfig, retainError error) (common.RPCClient, error) {
	return &mockRPCClient{
		scheduler:   scheduler,
		retainError: retainError,
	}, nil
}

// Start -
func (c *mockRPCClient) Start() {}

func (c *mockRPCClient) Stop() {}

func (c *mockRPCClient) IsEnable() bool {
	return true
}

// ServerKey get client used scheduler server key
func (c *mockRPCClient) ServerKey() string {
	return ""
}

// AcquireInstance -
func (c *mockRPCClient) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	rsp, err := c.scheduler.AcquireInstance(req)
	return rsp, err
}

// ReleaseInstance -
func (c *mockRPCClient) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	err := c.scheduler.ReleaseInstance(req)
	return err
}

// RetainInstance -
func (c *mockRPCClient) RetainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	if c.retainError != nil {
		return nil, c.retainError
	}
	rsp, err := c.scheduler.RetainInstance(req)
	return rsp, err
}
