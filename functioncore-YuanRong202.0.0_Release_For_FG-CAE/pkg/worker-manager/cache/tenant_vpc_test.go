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
	"testing"

	"github.com/stretchr/testify/assert"

	"functioncore/pkg/worker-manager/config"
)

func TestTenantVPC(t *testing.T) {
	tenantID := "123456789"
	tenantVPC := config.TenantVpcRouter{
		TenantVpcCidr: "192.168.0.1/24",
		Gateways:      []string{"192.168.0.1"},
	}
	val, ok := GetTenantVPCCache().Get(tenantID)
	assert.Nil(t, val)
	assert.False(t, ok)

	GetTenantVPCCache().Set(tenantID, &tenantVPC)

	val, ok = GetTenantVPCCache().Get(tenantID)
	assert.Equal(t, &tenantVPC, val)
	assert.True(t, ok)

	GetTenantVPCCache().Delete(tenantID)
	val, ok = GetTenantVPCCache().Get(tenantID)
	assert.Nil(t, val)
	assert.False(t, ok)
}
