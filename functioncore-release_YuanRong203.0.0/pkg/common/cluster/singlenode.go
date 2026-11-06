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

// Package cluster New Single Node Cluster
package cluster

import (
	"net/http"
	"sync"

	"functioncore/pkg/common/logger/log"
)

// SingleNodeCluster define a single node cluster
type SingleNodeCluster struct {
	wg            *sync.WaitGroup
	leader        LeaderInfo
	log           log.FormatLogger
	listenAddress string
	role          string
	stopCh        <-chan struct{}
	callback      DiscoverCallback
}

// NewSingleNodeCluster New Single Node Cluster
func NewSingleNodeCluster(listenAddress string, stopCh <-chan struct{},
	wg *sync.WaitGroup, log log.FormatLogger) (*SingleNodeCluster, error) {
	logger = log
	return &SingleNodeCluster{
		listenAddress: listenAddress,
		stopCh:        stopCh,
		wg:            wg,
		log:           log,
	}, nil
}

// Discovery set leader
func (c *SingleNodeCluster) Discovery() error {
	// set self to leader
	c.setRole(ClusterRoleLeader)

	// call callback
	if c.callback != nil {
		err := c.callback(c.stopCh)
		if err != nil {
			c.log.Errorf("single node cluster discovery error: leader callback error %s", err.Error())
			return err
		}
	}

	return nil
}

// ForwardRequest Forward Request
func (c *SingleNodeCluster) ForwardRequest(w http.ResponseWriter, r *http.Request) {
	// this should not be called
	c.log.Errorf("single node cluster should always be leader, no implement for forward")
}

// Leader current we direct read leader from etcd
func (c *SingleNodeCluster) Leader() *LeaderInfo {
	// if the node is leader, return advertiseURL
	if c.role == ClusterRoleLeader {
		return &LeaderInfo{
			Host: getAdvertiseURL(c.listenAddress),
		}
	}

	c.log.Warnf("single node cluster should always be leader, leader must be itself")

	return nil
}

func (c *SingleNodeCluster) getRole() string {
	return c.role
}

func (c *SingleNodeCluster) setRole(role string) {
	c.role = role
}

// Role return get Role
func (c *SingleNodeCluster) Role() string {
	return c.getRole()
}

// SetCallback Set Call back
func (c *SingleNodeCluster) SetCallback(f DiscoverCallback) {
	c.callback = f
}
