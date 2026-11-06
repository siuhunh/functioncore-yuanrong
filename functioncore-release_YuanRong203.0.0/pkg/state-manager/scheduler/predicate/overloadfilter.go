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

package predicate

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/scheduler/metadata"
)

const (
	overloadFilterName = "overloadFilter"
	maxCPUUsage        = 99.9 // don't filter by cpu usage
	maxMemoryUsage     = 99.9 // don't filter by mem usage
)

type overloadFilter struct {
}

// ResourceUsage is usage of resource
type ResourceUsage struct {
	CPURatio float32
	MEMRatio float32
}

// Filter filter unexpect node
func (f *overloadFilter) Filter(
	target *metadata.TargetInfo, infos []*metadata.NodeInfo) ([]*metadata.NodeInfo, error) {
	var filteredNode []*metadata.NodeInfo
	for idx := range infos {
		nodeID := infos[idx].ID
		res, err := metadata.GetNodeResource(nodeID)
		if err != nil {
			log.GetLogger().Warnf("get node resource of %s failed, err: %s", nodeID, err.Error())
			continue
		}
		resUse := ResourceUsage{}
		resUse.CPURatio = float32(res.CPU) / float32(res.CPUCapacity)
		resUse.MEMRatio = float32(res.Memory) / float32(res.MemoryCapacity)
		log.GetLogger().Debugf("predicate]resUse.CPURatio %v, resUse.MEMRatio %v, id %v",
			resUse.CPURatio, resUse.MEMRatio, infos[idx].ID)

		if resUse.CPURatio < maxCPUUsage && resUse.MEMRatio < maxMemoryUsage {
			infos[idx].Cache = resUse
			filteredNode = append(filteredNode, infos[idx])
		}
	}
	return filteredNode, nil
}
