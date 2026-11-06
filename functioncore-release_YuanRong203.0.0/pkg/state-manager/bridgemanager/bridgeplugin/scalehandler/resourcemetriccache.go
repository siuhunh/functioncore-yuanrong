/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package scalehandler -
package scalehandler

import (
	"sync"

	"functioncore/pkg/common/resourceusage"
)

const noUseTime = 30

// ResourceMetric -
type ResourceMetric struct {
	resource      map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList
	resourceMutex sync.RWMutex
}

func initResourceMetricCache() ResourceMetric {
	return ResourceMetric{
		resource:      make(map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList),
		resourceMutex: sync.RWMutex{},
	}
}

func (resourceMetric *ResourceMetric) updateResource(resource map[resourceusage.
	FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList) {
	resourceMetric.resourceMutex.Lock()
	defer resourceMetric.resourceMutex.Unlock()
	resourceMetric.resource = resource
}

func (resourceMetric *ResourceMetric) getResourceByFuncSvcID(funcSvcID string) map[resourceusage.
	PodName]resourceusage.BridgeResourceList {
	resourceMetric.resourceMutex.RLock()
	defer resourceMetric.resourceMutex.RUnlock()
	return resourceMetric.resource[resourceusage.FuncSvcID(funcSvcID)]
}
