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

package priority

import (
	"functioncore/pkg/state-manager/scheduler/metadata"
)

// AbsPriorityAlgorithm is abstract priority algorithm
type AbsPriorityAlgorithm interface {
	// Init with config
	Init(config interface{}) error

	// Param:  Target state instance info
	//         NodeInfo list in cluster
	// Return: NodeScore list for all input node
	//         error
	Score(*metadata.TargetInfo, []*metadata.NodeInfo) ([]*metadata.NodeScore, error)
}
