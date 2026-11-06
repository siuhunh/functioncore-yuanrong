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

package utils

import (
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/logger/log"
)

// PodStatus -
type PodStatus struct {
	Pod *corev1.Pod
	Err error
}

// PodWaiter -
type PodWaiter struct {
	ch            chan PodStatus
	unschedulable int32
}

// NewPodWaiter -
func NewPodWaiter() *PodWaiter {
	return &PodWaiter{
		ch: make(chan PodStatus, 1),
	}
}

// SetUnschedulable -
func (w *PodWaiter) SetUnschedulable(unschedulable bool) bool {
	var old int32
	if unschedulable {
		old = atomic.SwapInt32(&w.unschedulable, 1)
	} else {
		old = atomic.SwapInt32(&w.unschedulable, 0)
	}
	return old != 0
}

// IsUnschedulable -
func (w *PodWaiter) IsUnschedulable() bool {
	return atomic.LoadInt32(&w.unschedulable) != 0
}

// WaitStatus -
func (w *PodWaiter) WaitStatus() <-chan PodStatus {
	return w.ch
}

// NotifyStatus -
func (w *PodWaiter) NotifyStatus(status PodStatus) {
	select {
	case w.ch <- status:
	default:
		log.GetLogger().Warnf("failed to write channel for pod %s, already notified?", status.Pod.Name)
	}
}
