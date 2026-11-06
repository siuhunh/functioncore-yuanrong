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

// Package workerevaluator contains worker metrics and supports observers
package workerevaluator

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
)

// Observer - other module describe workerEvaluator
type Observer interface {
	OnObserved(content interface{}, recovering bool)
}

// Observe observer specific functionKey
func (we *WorkerEvaluator) Observe(funcKeyWithResource string, observer Observer) {
	log.GetLogger().Debugf("add function: %s observer", urnutils.AnonymizeTenantKey(funcKeyWithResource))
	we.observers.Store(funcKeyWithResource, observer)
	// publish existed metrics
	we.metrics.Range(func(k, v interface{}) bool {
		if v, ok := v.(*WorkerMetrics); ok {
			if v.FuncKeyWithResource == funcKeyWithResource {
				if k, ok := k.(string); ok {
					log.GetLogger().Debugf("report existed worker: %s function: %s", k,
						urnutils.AnonymizeTenantKey(funcKeyWithResource))
					observer.OnObserved(v, true)
				}
			}
		}
		return true
	})
}

func (we *WorkerEvaluator) publish(funcKeyWithResource string, content interface{}) {
	obj, ok := we.observers.Load(funcKeyWithResource)
	if !ok {
		log.GetLogger().Debugf("no observer for function %s", urnutils.AnonymizeTenantKey(funcKeyWithResource))
	} else {
		observer, ok := obj.(Observer)
		if !ok || observer == nil {
			log.GetLogger().Errorf("funcKeyWithResource: %s observer in map is wrong",
				urnutils.AnonymizeTenantKey(funcKeyWithResource))
			we.UnObserve(funcKeyWithResource)
		} else {
			observer.OnObserved(content, false)
		}
	}
}

// UnObserve delete observer
func (we *WorkerEvaluator) UnObserve(funcKeyWithResource string) {
	log.GetLogger().Infof("delFuncObserver | %s", urnutils.FuncKeyWithResToShort(funcKeyWithResource))
	we.observers.Delete(funcKeyWithResource)
}
