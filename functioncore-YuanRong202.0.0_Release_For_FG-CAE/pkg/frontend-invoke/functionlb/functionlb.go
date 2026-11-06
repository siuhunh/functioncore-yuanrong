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

// Package functionlb manage Function LB and deal with worker instance
package functionlb

import (
	"sync"

	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/frontend-invoke/util"
)

var (
	// FunctionLB global function LB
	FunctionLB = &Workers{
		WorkerMap:      &sync.Map{},
		instancesMutex: &sync.Mutex{},
		stateInstance: &Worker{
			WorkerInstancesConn: &sync.Map{},
			workerType:          util.StateIns,
			stateSingleFlight:   singleflight.NewSingleFlight(),
		},
	}
	// NodeRoute global Node Info
	NodeRoute = &Nodes{}
)
