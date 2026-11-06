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

package ratelimit

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"

	"golang.org/x/time/rate"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
)

const (
	// validTenantPathCount  is the separator count of tenant Path
	validTenantPathCount = 7
	// tenantMark is mark of etcd of tenant
	tenantMark    = "tenant"
	tenantIndex   = 5
	tenantIDIndex = 6
	keySeparator  = "/"
)

// TenantLimiter - tenantLimiter
type TenantLimiter struct {
	Quota   int
	Limiter *rate.Limiter
}

// TenantLimitContainer -
type TenantLimitContainer struct {
	tenantInfo *sync.Map
}

var (
	tenantLimitContainer     *TenantLimitContainer
	tenantLimitContainerOnce sync.Once
)

// GetTenantLimiterContainer -
func GetTenantLimiterContainer() *TenantLimitContainer {
	tenantLimitContainerOnce.Do(func() {
		tenantLimitContainer = &TenantLimitContainer{tenantInfo: &sync.Map{}}
	})
	return tenantLimitContainer
}

// TakeOneToken -
func (tlc *TenantLimitContainer) TakeOneToken(tenantID string) error {
	tenantLimiter := tlc.getTenantLimiter(tenantID)
	if tenantLimiter.Limiter == nil {
		return nil
	}
	if allow := tenantLimiter.Limiter.Allow(); allow {
		return nil
	}
	return fmt.Errorf("requests reached the max quota %d of the tenant %s",
		tenantLimiter.Quota, urnutils.Anonymize(tenantID))
}

func (tlc *TenantLimitContainer) getTenantLimiter(tenantID string) TenantLimiter {
	getInfo, exist := tlc.tenantInfo.Load(tenantID)
	if !exist {
		log.GetLogger().Debugf("failed to get tenant limiter for tenant %s ", urnutils.Anonymize(tenantID))
		return TenantLimiter{Quota: constants.DefaultTenantLimitRate}
	}

	tenantLimiter, assertOK := getInfo.(TenantLimiter)
	if !assertOK {
		log.GetLogger().Warnf("invalid tenant info type for tenant %s.", urnutils.Anonymize(tenantID))
		return TenantLimiter{Quota: constants.DefaultTenantLimitRate}
	}
	return tenantLimiter
}

func (tlc *TenantLimitContainer) syncLimiter(tenantID string, tenantLimiter TenantLimiter) {
	limitRate := float64(tenantLimiter.Quota)
	limitBucketSize := int(math.Ceil(float64(tenantLimiter.Quota)) / constants.TrafficRedundantRate)
	if tenantLimiter.Limiter == nil {
		tenantLimiter.Limiter = rate.NewLimiter(rate.Limit(limitRate), limitBucketSize)
	} else {
		tenantLimiter.Limiter.SetLimit(rate.Limit(limitRate))
		tenantLimiter.Limiter.SetBurst(limitBucketSize)
	}
	tlc.tenantInfo.Store(tenantID, tenantLimiter)
}

// ProcessUpdate -
func (tlc *TenantLimitContainer) ProcessUpdate(tenantID string, data map[string]int) {
	quota, exist := data["quota"]
	if !exist {
		log.GetLogger().Errorf("failed to get the quota value")
		return
	}
	tenantLimiter := tlc.getTenantLimiter(tenantID)
	tenantLimiter.Quota = quota
	tlc.syncLimiter(tenantID, tenantLimiter)
	log.GetLogger().Infof("update tenant %s quota update to %d.", urnutils.Anonymize(tenantID), quota)
}

// ProcessDelete -
func (tlc *TenantLimitContainer) ProcessDelete(tenantID string) {
	tenantLimiter := tlc.getTenantLimiter(tenantID)
	tenantLimiter.Limiter = nil
	tlc.tenantInfo.Delete(tenantID)
	log.GetLogger().Infof("delete tenant %s quota", urnutils.Anonymize(tenantID))
}

// Update -
func Update(event *etcd3.Event) {
	s := strings.Split(event.Key, keySeparator)
	if len(s) <= tenantIDIndex {
		log.GetLogger().Errorf("failed to get the tenantID")
		return
	}
	var (
		data     map[string]int
		tenantID string
	)
	if tenantID = s[tenantIDIndex]; tenantID == "" {
		log.GetLogger().Errorf("failed to get tenantID from event.Key :%s", event.Key)
		return
	}
	if err := json.Unmarshal(event.Value, &data); err != nil || data == nil {
		log.GetLogger().Errorf("failed to unmarshal the etcd event value")
		return
	}

	GetTenantLimiterContainer().ProcessUpdate(tenantID, data)
}

// Delete -
func Delete(event *etcd3.Event) {
	s := strings.Split(event.Key, keySeparator)
	if len(s) <= tenantIDIndex {
		log.GetLogger().Errorf("failed to get the tenantID")
		return
	}
	var tenantID string
	if tenantID = s[tenantIDIndex]; tenantID == "" {
		log.GetLogger().Errorf("failed to get tenantID from event.Key :%s", event.Key)
		return
	}
	GetTenantLimiterContainer().ProcessDelete(tenantID)
}

// TenantQOSFilter -
func TenantQOSFilter(etcdKey string) bool {
	s := strings.Split(etcdKey, keySeparator)
	if len(s) != validTenantPathCount {
		return false
	}
	if s[tenantIndex] != tenantMark {
		return false
	}
	return true
}
