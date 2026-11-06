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

// Package cluster types
package cluster

import "net/http"

// DiscoverCallback define discover callback function
type DiscoverCallback func(stopCh <-chan struct{}) error

// LeaderInfo define leader info
type LeaderInfo struct {
	// leader host info
	Host string
}

// Discovery define cluster discovery
type Discovery interface {
	// set callback when become leader
	SetCallback(f DiscoverCallback)

	// start the cluster discovery go routine
	Discovery() error

	// return the cluster Leader info
	Leader() *LeaderInfo

	// return the role in cluster
	Role() string

	// forward request to leader
	ForwardRequest(w http.ResponseWriter, r *http.Request)
}
