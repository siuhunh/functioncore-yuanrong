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

// Package status include status of autoscaler
package status

import (
	"fmt"
	"sync"
)

// Status is autoscaler status
type Status int32

// Status codes
const (
	Ready Status = iota
	FlowOut
	FlowIn
)

var (
	autoScalerStatus = struct {
		sync.RWMutex
		status Status
	}{}
)

// Set sets autoscaler status, this method is concurrent security.
func Set(s Status) {
	autoScalerStatus.Lock()
	autoScalerStatus.status = s
	autoScalerStatus.Unlock()
}

// Get returns autoscaler status, this method is concurrent security.
func Get() Status {
	autoScalerStatus.RLock()
	status := autoScalerStatus.status
	autoScalerStatus.RUnlock()
	return status
}

// String return string according case
func (s Status) String() string {
	switch s {
	case FlowIn:
		return "flowIn"
	case FlowOut:
		return "flowOut"
	case Ready:
		return "ready"
	default:
		return fmt.Sprintf("Unknown status - %d", s)
	}
}
