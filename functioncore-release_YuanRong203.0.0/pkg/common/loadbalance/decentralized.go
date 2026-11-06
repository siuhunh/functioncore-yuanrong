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

package loadbalance

import (
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"

	"functioncore/pkg/common/logger/log"
)

type workerRequests struct {
	instance interface{}
	// total requests
	requestNums int
}

const (
	defaultSmoothNum        = 5
	defaultRequestThreshold = 10
)

// DecentralizedLB Solve load imbalance in decentralized scenarios by sensing state requests
type DecentralizedLB struct {
	workers         []*workerRequests
	rnd             *rand.Rand
	workerNum       int
	smoothNum       int
	totalRequestNum int
}

// Next get dispatcher worker
func (w *DecentralizedLB) Next(_ string, _ bool) interface{} {
	if w.workerNum == 0 {
		return nil
	}
	if w.workerNum == 1 {
		w.workers[0].requestNums++
		w.totalRequestNum++
		return w.workers[0].instance
	}
	return w.calByRandom()
}

func (w *DecentralizedLB) calByRandom() interface{} {
	// Number of workers used for smoothing
	if w.workerNum < w.smoothNum {
		w.smoothNum = w.workerNum
	}

	if w.totalRequestNum <= defaultRequestThreshold {
		w.smoothNum = w.workerNum
	}
	// find the minimum requestNum
	index := -1
	request := math.MaxInt32
	indexArray := w.rnd.Perm(w.workerNum)
	for i := 0; i < w.smoothNum && i < len(indexArray); i++ {
		if w.workers[indexArray[i]].requestNums < request && indexArray[i] < len(w.workers) {
			request = w.workers[indexArray[i]].requestNums
			index = indexArray[i]
			if request == 0 {
				break
			}
		}
	}
	if index == -1 {
		log.GetLogger().Errorf("failed to get instance")
		return nil
	}
	w.workers[index].requestNums++
	w.totalRequestNum++
	return w.workers[index].instance
}

// Add -
func (w *DecentralizedLB) Add(node interface{}, weight int) {
	workerRequests := &workerRequests{instance: node, requestNums: 0}
	w.workers = append(w.workers, workerRequests)
	w.workerNum++
}

// Remove -
func (w *DecentralizedLB) Remove(node interface{}) {
	for i, requests := range w.workers {
		if requests.instance == node {
			w.workers = append(w.workers[:i], w.workers[i+1:]...)
			w.workerNum--
			if w.totalRequestNum < requests.requestNums {
				log.GetLogger().Warnf("total request num %d is smaller than worker request num %d",
					w.totalRequestNum, requests.requestNums)
				w.totalRequestNum = 0
			} else {
				w.totalRequestNum -= requests.requestNums
			}
			break
		}
	}
}

// Done -
func (w *DecentralizedLB) Done(node interface{}, cost time.Duration) {
	for _, requests := range w.workers {
		if requests.instance == node {
			requests.requestNums--
			w.totalRequestNum--
			break
		}
	}
}

// RemoveAll remove all nodes
func (w *DecentralizedLB) RemoveAll() {
}

// Reset reset all nodes
func (w *DecentralizedLB) Reset() {
}

// NextWithRequest -
func (w *DecentralizedLB) NextWithRequest(req *Request, move bool) interface{} {
	return w.Next(req.Name, move)
}

// SetConcurrency -
func (w *DecentralizedLB) SetConcurrency(concurrency int) {}

// SetIsLongRun -
func (w *DecentralizedLB) SetIsLongRun(isLongRun bool) {}

// SetIsCaeFunc -
func (w *DecentralizedLB) SetIsCaeFunc(isCaeFunc bool) {}

// Start -
func (w *DecentralizedLB) Start() {}

// Stop -
func (w *DecentralizedLB) Stop() {}

// NoLock -
func (w *DecentralizedLB) NoLock() bool {
	return false
}

// NewDecentralizedLB init NonCenterLB
func NewDecentralizedLB() *DecentralizedLB {
	decentralizedLB := &DecentralizedLB{
		rnd:       rand.New(rand.NewSource(time.Now().UnixNano())),
		workerNum: 0,
	}
	smoothNum, err := strconv.Atoi(os.Getenv("SMOOTH_NUM"))
	if err == nil && smoothNum > 0 {
		decentralizedLB.smoothNum = smoothNum
	} else {
		decentralizedLB.smoothNum = defaultSmoothNum
	}
	log.GetLogger().Infof("smooth num of decentralized LB is %d", decentralizedLB.smoothNum)
	return decentralizedLB
}
