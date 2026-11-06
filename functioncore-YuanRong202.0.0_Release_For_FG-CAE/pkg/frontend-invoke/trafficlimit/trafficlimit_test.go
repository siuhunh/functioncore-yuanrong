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

package trafficlimit

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"

	"functioncore/pkg/common/etcd3"
)

// TestTenantTraffic test for tenant traffic limiter
func TestTenantTraffic(t *testing.T) {
	tenantID01 := "tenantID1"
	tenantID02 := "tenantID2"
	tenantID03 := "tenantID3"
	tenanting1 := TenantInfo{Quota: 17, Limiter: &rate.Limiter{}}
	tenanting2 := TenantInfo{Quota: 13, Limiter: &rate.Limiter{}}
	tenanting3 := TenantInfo{Quota: 20, Limiter: &rate.Limiter{}}
	TenantBuf.tenantInfo.Store(tenantID01, tenanting1)
	TenantBuf.tenantInfo.Store(tenantID02, tenanting2)
	TenantBuf.tenantInfo.Store(tenantID03, tenanting3)
	TenantBuf.syncLimiter(tenantID01, tenanting1)
	TenantBuf.syncLimiter(tenantID02, tenanting2)
	TenantBuf.syncLimiter(tenantID03, tenanting3)
	for i := 0; i < 6; i++ {
		assert.Equal(t, nil, TenantTrafficLimit(tenantID01))
	}
	for i := 7; i < 20; i++ {
		assert.Error(t, TenantTrafficLimit(tenantID01))
	}
	for i := 0; i < 4; i++ {
		assert.Equal(t, nil, TenantTrafficLimit(tenantID02))
	}
	for i := 5; i < 20; i++ {
		assert.Error(t, TenantTrafficLimit(tenantID02))
	}
	for i := 0; i < 7; i++ {
		assert.Equal(t, nil, TenantTrafficLimit(tenantID03))
	}
	for i := 8; i < 20; i++ {
		assert.Error(t, TenantTrafficLimit(tenantID03))
	}
	fmt.Println("test update")
	time.Sleep(5 * time.Second)
	//update tenant quota
	event1 := &etcd3.Event{
		Type:  etcd3.PUT,
		Key:   "/sn/qos/business/yrk/tenant/tenantID1",
		Value: []byte(`{"quota":10}`),
	}
	TenantBuf.ProcessUpdate(event1)
	for i := 0; i < 3; i++ {
		assert.Equal(t, nil, TenantTrafficLimit(tenantID01))
	}
	for i := 4; i < 20; i++ {
		assert.Error(t, TenantTrafficLimit(tenantID01))
	}
	time.Sleep(5 * time.Second)
	//delete tenant quota
	fmt.Println("test delete")
	event2 := &etcd3.Event{
		Type: etcd3.DELETE,
		Key:  "/sn/qos/business/yrk/tenant/tenantID2",
	}
	TenantBuf.ProcessDelete(event2)
	for i := 0; i < 40; i++ {
		assert.Equal(t, nil, TenantTrafficLimit(tenantID02))
	}
	fmt.Println("test large scale")
	event3 := &etcd3.Event{
		Type:  etcd3.PUT,
		Key:   "/sn/qos/business/yrk/tenant/tenantID3",
		Value: []byte(`{"quota":200000}`),
	}
	TenantBuf.ProcessUpdate(event3)
	for i := 0; i < 40000; i++ {
		assert.Equal(t, nil, TenantTrafficLimit(tenantID03))
	}
}

func TestTenantTakeOneToken(t *testing.T) {
	TenantBuf.tenantInfo = &sync.Map{}
	allow, quota := TenantBuf.tenantTakeOneToken("test")
	assert.Equal(t, allow, true)
	assert.Equal(t, quota, 1800)
	TenantBuf.tenantInfo.Store("test", 18)
	allow, quota = TenantBuf.tenantTakeOneToken("test")
	assert.Equal(t, allow, true)
	assert.Equal(t, quota, 1800)
}
