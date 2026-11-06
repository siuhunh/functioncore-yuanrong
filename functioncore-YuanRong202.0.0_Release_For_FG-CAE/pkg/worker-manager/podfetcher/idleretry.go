/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package podfetcher -
package podfetcher

import (
	"context"
	"sync"

	"functioncore/pkg/common/alarm"
)

// IdleRetryList indicates the all information structure of the retried reserved instance.
type IdleRetryList struct {
	retryMap map[string]*RetryInfo
	lock     sync.Mutex
}

// RetryInfo indicates the information structure of the single retried reserved instance.
type RetryInfo struct {
	retryNum int
	ctx      context.Context
	cancel   func()
}

func initIdleRetryList() (*IdleRetryList, error) {
	alarm.GetClient().RegisterAlarmType(alarm.IdleModeNotification)
	return &IdleRetryList{
		retryMap: make(map[string]*RetryInfo),
	}, nil
}

func (w *IdleRetryList) remove(podName string) int {
	w.lock.Lock()
	_, ok := w.retryMap[podName]
	if ok {
		delete(w.retryMap, podName)
	}
	lens := len(w.retryMap)
	w.lock.Unlock()
	return lens
}

func (w *IdleRetryList) updateCtxAndRetryNum(podName string) (context.Context, int) {
	retryNum := 0
	w.lock.Lock()
	data, ok := w.retryMap[podName]
	if ok {
		retryNum = data.retryNum
		data.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	info := RetryInfo{
		ctx:      ctx,
		cancel:   cancel,
		retryNum: retryNum,
	}
	w.retryMap[podName] = &info
	w.lock.Unlock()
	return ctx, retryNum
}

func (w *IdleRetryList) updateRetryNum(podName string, retryNum int) {
	w.lock.Lock()
	data, ok := w.retryMap[podName]
	if ok {
		data.retryNum = retryNum
	}
	w.lock.Unlock()
}
