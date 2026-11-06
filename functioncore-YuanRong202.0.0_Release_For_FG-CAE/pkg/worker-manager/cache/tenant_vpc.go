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

	"functioncore/pkg/worker-manager/config"
)

var (
	tenantVPC = &TenantVPC{}
)

// GetTenantVPCCache return global TenantVPC cache
func GetTenantVPCCache() *TenantVPC {
	return tenantVPC
}

// TenantVPC is cache for vpc config of tenant
type TenantVPC struct {
	// key: tenantID value: *config.TenantVpcRouter
	router sync.Map
}

// Get return tenant vpc router by tenantID
func (tv *TenantVPC) Get(tenantID string) (*config.TenantVpcRouter, bool) {
	vpcRouter, ok := tv.router.Load(tenantID)
	if !ok {
		return nil, false
	}
	v, ok := vpcRouter.(*config.TenantVpcRouter)
	if !ok {
		return nil, false
	}
	return v, true
}

// Set set tenant vpc router by tenantID
func (tv *TenantVPC) Set(tenantID string, tenantVpcRouter *config.TenantVpcRouter) {
	tv.router.Store(tenantID, tenantVpcRouter)
}

// Delete delete tenant vpc router by tenantID
func (tv *TenantVPC) Delete(tenantID string) {
	tv.router.Delete(tenantID)
}
