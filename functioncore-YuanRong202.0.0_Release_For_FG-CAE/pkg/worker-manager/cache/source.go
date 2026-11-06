/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package cache

import (
	"sync"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

// CheckEventSource check event source using default FuncSource
func CheckEventSource(event eventsource.Event, funcSvcID string) bool {
	return funcSource.CheckEventSource(event, funcSvcID)
}

var (
	funcSource = NewFuncSource()
)

// NewFuncSource return new FuncSource
func NewFuncSource() *FuncSource {
	return &FuncSource{
		instance: make(map[string]string),
		repo:     make(map[string]string),
	}
}

// FuncSource cached etcd event source for function
type FuncSource struct {
	instance map[string]string
	repo     map[string]string
	sync.Mutex
}

// CheckEventSource check the event source of funcSvcID
// if event is from caeMeta etcd, and SourceCache has cached data from meta etcd before,
// the check fails and won't cache this event
func (f *FuncSource) CheckEventSource(event eventsource.Event, funcSvcID string) bool {
	meta := etcd3.Meta.String()
	caeMeta := etcd3.CAEMeta.String()
	f.Lock()
	if event.SourceType == caeMeta {
		oldIns := f.instance[funcSvcID]
		oldRepo := f.repo[funcSvcID]
		if oldIns == meta || oldRepo == meta {
			f.Unlock()
			log.GetLogger().Warnf("function from meta etcd exists, skip update from cae, key %s, "+
				"insSource %s, repoSource %s", event.KeyRef, oldIns, oldRepo)
			return false
		}
	}
	if event.Kind == utils.InstanceMetadataEventKind {
		if event.EventType == eventsource.Deleted {
			delete(f.instance, funcSvcID)
		} else {
			f.instance[funcSvcID] = event.SourceType
		}
	} else {
		if event.EventType == eventsource.Deleted {
			delete(f.repo, funcSvcID)
		} else {
			f.repo[funcSvcID] = event.SourceType
		}
	}
	f.Unlock()
	return true
}
