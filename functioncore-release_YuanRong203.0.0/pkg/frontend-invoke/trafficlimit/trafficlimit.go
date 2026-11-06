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

// Package trafficlimit is for trigger traffic limitation
package trafficlimit

import (
	"errors"
	"fmt"
	"sync"

	"golang.org/x/time/rate"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
)

// TenantContainer tenant information and tenant's Limiter
type TenantContainer struct {
	tenantInfo *sync.Map
}

// TenantInfo -
type TenantInfo struct {
	Quota   int
	Limiter *rate.Limiter
}

var (
	// TenantBuf -
	TenantBuf = &TenantContainer{
		tenantInfo: &sync.Map{},
	}
)

// RateLimiter rate limiter struct
type RateLimiter struct {
	*rate.Limiter
	reset chan struct{}
}

// MapContainer the map object which stores function info
type MapContainer struct {
	limitMap *sync.Map
}

// Take return if a function request is allowed
func (r *RateLimiter) take() bool {
	r.reset <- struct{}{}
	return r.Limiter.Allow()
}

// TenantTrafficLimit is the main function of tenant traffic limitation
func TenantTrafficLimit(tenantID string) error {
	allow, quota := TenantBuf.tenantTakeOneToken(tenantID)
	if !allow {
		errMsg := fmt.Sprintf("Requests reached the max quota %d of the tenant %s",
			quota, urnutils.Anonymize(tenantID))
		return errors.New(errMsg)
	}
	return nil
}

func (t *TenantContainer) tenantTakeOneToken(tenantID string) (bool, int) {
	tenantInfo := t.getTenantInfo(tenantID)
	if tenantInfo.Limiter == nil {
		log.GetLogger().Debugf("traffic limiter is invalid")
		return true, tenantInfo.Quota
	}
	return tenantInfo.Limiter.Allow(), tenantInfo.Quota
}

// getTenantInfo  to generator the tenant limiter
func (t *TenantContainer) getTenantInfo(tenantID string) TenantInfo {
	getInfo, ok := t.tenantInfo.Load(tenantID)
	if !ok {
		log.GetLogger().Debugf("failed to get tenant info for tenant %s.", urnutils.Anonymize(tenantID))
		return TenantInfo{Limiter: nil, Quota: constants.DefaultTenantLimitRate}
	}
	tenantInfo, ok := getInfo.(TenantInfo)
	if !ok {
		log.GetLogger().Warnf("invalid tenant info type for tenant %s.", urnutils.Anonymize(tenantID))
		return TenantInfo{Limiter: nil, Quota: constants.DefaultTenantLimitRate}
	}
	return tenantInfo
}

func (t *TenantContainer) syncLimiter(tenantID string, tenantInfo TenantInfo) {
	tenantInfo.Limiter = t.getTenantLimiter(tenantInfo)
	t.tenantInfo.Store(tenantID, tenantInfo)
}
