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

// Package status include status of persistence
package status

import (
	"fmt"
	"sync"
)

// Status is persistence status
type Status int32

// Status codes
const (
	NoPersistence Status = iota
	Persistence
)

var (
	persistenceStatus = struct {
		sync.RWMutex
		status Status
	}{}
)

// Set sets persistence status, this method is concurrent security.
func Set(s Status) {
	persistenceStatus.Lock()
	persistenceStatus.status = s
	persistenceStatus.Unlock()
}

// Get returns persistence status, this method is concurrent security.
func Get() Status {
	persistenceStatus.RLock()
	status := persistenceStatus.status
	persistenceStatus.RUnlock()
	return status
}

// String return string according case
func (s Status) String() string {
	switch s {
	case NoPersistence:
		return "noPersistence"
	case Persistence:
		return "persistence"
	default:
		return fmt.Sprintf("Unknown status - %d", s)
	}
}
