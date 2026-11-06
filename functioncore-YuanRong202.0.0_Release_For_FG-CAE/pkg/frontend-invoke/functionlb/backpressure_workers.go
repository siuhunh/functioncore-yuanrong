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
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/frontend-invoke/constant"
)

const defaultMapSize = 5

type deleteFunc func(az, instance, urn string)

type deleteCallback struct {
	function   deleteFunc
	urnWithRes string
}

// BackPressureWorkers workers from back pressure instead of router etcd to take effect in lb sooner. Record
// redundant workers to avoid back pressure message later than router etcd deleting event. Check periodically
// to clear out-of-date workers from back pressure.
type BackPressureWorkers struct {
	externalInstanceMutex *sync.Mutex
	// key: workerInstanceKey
	workers             map[string]deleteCallback
	lastWorkersSnapshot map[string]deleteCallback
}

func initBackPressureWorkers(mutex *sync.Mutex) *BackPressureWorkers {
	return &BackPressureWorkers{
		workers:               make(map[string]deleteCallback, defaultMapSize),
		lastWorkersSnapshot:   make(map[string]deleteCallback, defaultMapSize),
		externalInstanceMutex: mutex,
	}
}

func (w *BackPressureWorkers) checkPeriodically(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("nil stopCh")
		return
	}

	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case _, ok := <-t.C:
			if !ok {
				log.GetLogger().Errorf("ticker closed unexpectedly")
				return
			}
			w.checkAndDeleteOutOfDateWorkers()
		case <-stopCh:
			return
		}
	}
}

func (w *BackPressureWorkers) checkAndDeleteOutOfDateWorkers() {
	w.externalInstanceMutex.Lock()
	for worker, callback := range w.lastWorkersSnapshot {
		if _, ok := w.workers[worker]; ok {
			delete(w.workers, worker)
			log.GetLogger().Infof("back pressure worker is out of date, worker %s urnWithRes %s",
				worker, callback.urnWithRes)
			callback.function(constant.NativeAz, worker, callback.urnWithRes)
		}
	}
	w.lastWorkersSnapshot = w.workers
	w.externalInstanceMutex.Unlock()
}

func (w *BackPressureWorkers) add(workerInstanceKey string, callback deleteCallback) {
	w.workers[workerInstanceKey] = callback
}

func (w *BackPressureWorkers) delete(workerInstanceKey string) {
	delete(w.workers, workerInstanceKey)
}
