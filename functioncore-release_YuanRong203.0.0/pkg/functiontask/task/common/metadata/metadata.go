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

// Package metadata represents metadata.
package metadata

import (
	"strings"

	"functioncore/pkg/common/types"
)

// Metadata describes metadata for a state.
type Metadata struct {
	TenantID      string
	ServiceID     string
	FunctionName  string
	StateID       string
	StateKey      string
	Creator       string
	CreateOptions *types.StateCreateOptions
}

const (
	separator       = ":"
	stateHashPrefix = "state-hash"
	statePartsLen   = 4

	// StateIDPrefix is prefix word for stateID metaKey
	StateIDPrefix = "stateID"
)

// Key returns actual state id used to storage state into redis.
func (m *Metadata) Key() string {
	return Gen(m.TenantID, m.ServiceID, StateIDPrefix, m.StateID)
}

// Slot returns the slot name.
func (m *Metadata) Slot() string {
	return Slot(m.TenantID, m.ServiceID)
}

// StateHashKey return state hash key.
func (m *Metadata) StateHashKey() string {
	return GeneStateHashKey(m.TenantID, m.ServiceID, m.FunctionName)
}

// Slot returns the slot name.
func Slot(tenantID, serviceID string) string {
	return "{" + tenantID + separator + serviceID + "}"
}

// Gen returns key consist of tenant id, service id and key.
// warning: ExtractMetaKey depend on func Gen. any changes should be synchronous
func Gen(tenantID, serviceID, keyPrefix, key string) string {
	return strings.Join([]string{Slot(tenantID, serviceID), keyPrefix, key}, separator)
}

// GeneStateHashKey generate state hash key.
func GeneStateHashKey(tenantID, serviceID, funcName string) string {
	return strings.Join([]string{stateHashPrefix, Slot(tenantID, serviceID), funcName}, separator)
}

// ExtractMetaKey extract value from metaKey
// warning: ExtractMetaKey depend on func Gen. any changes should be synchronous
func ExtractMetaKey(metaKey string) (tenantID, serviceID, id string) {
	metas := strings.Split(metaKey, separator)
	if len(metas) != statePartsLen {
		return
	}
	tenantID = metas[0][1:]
	serviceID = metas[1][:len(metas[1])-1]
	id = metas[3]
	return
}
