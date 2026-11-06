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
	"encoding/json"
	"math"
	"strings"

	"golang.org/x/time/rate"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
)

const (
	tenantIDIndex = 6
)

// TrafficVariables is a struct of traffic limitation parameters
type TrafficVariables struct {
	InstanceLimitRate  float64
	InstanceBucketSize int
	FuncLimitRate      float64
	FuncBucketSize     int
}

// TenantTrafficParams -
type TenantTrafficParams struct {
	TenantLimitRate  float64
	TenantBucketSize int
}

// judgeTrafficParams is to choose default parameters or the parameters got from json
func judgeTrafficParams(config *constant.Config) *TrafficVariables {
	trafficParams := &TrafficVariables{
		InstanceLimitRate:  constant.DefaultInstanceLimitRate,
		InstanceBucketSize: constant.DefaultInstanceBucketSize,
		FuncLimitRate:      constant.DefaultFuncLimitRate,
		FuncBucketSize:     constant.DefaultFuncBucketSize,
	}
	// instance and func limit configs are from Config
	if config.TrafficLimitParams == nil {
		return trafficParams
	}
	if config.TrafficLimitParams.InstanceLimitRate >= 1 {
		trafficParams.InstanceLimitRate = config.TrafficLimitParams.InstanceLimitRate
	}
	if config.TrafficLimitParams.InstanceBucketSize >= 1 {
		trafficParams.InstanceBucketSize = config.TrafficLimitParams.InstanceBucketSize
	}
	if config.TrafficLimitParams.FuncLimitRate >= 1 {
		trafficParams.FuncLimitRate = config.TrafficLimitParams.FuncLimitRate
	}
	if config.TrafficLimitParams.FuncBucketSize >= 1 {
		trafficParams.FuncBucketSize = config.TrafficLimitParams.FuncBucketSize
	}
	return trafficParams
}

func (t *TenantContainer) getTenantLimiter(info TenantInfo) *rate.Limiter {
	limitRate := float64(info.Quota) / constant.FunctionAccessorInitCopies
	limitBucketSize := int(math.Ceil(float64(info.Quota)) /
		constant.FunctionAccessorInitCopies * constant.TrafficRedundantRate)
	tenantLimiter := rate.NewLimiter(rate.Limit(limitRate), limitBucketSize)
	return tenantLimiter
}

// ProcessUpdate -
func (t *TenantContainer) ProcessUpdate(event *etcd3.Event) error {
	var data map[string]int
	strs := strings.Split(event.Key, constant.KeySeparator)
	if len(strs) <= tenantIDIndex {
		log.GetLogger().Errorf("failed to get the tenantID")
		return nil
	}
	tenantID := strs[tenantIDIndex]
	if err := json.Unmarshal(event.Value, &data); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the etcd event value")
		return err
	}
	if data == nil {
		log.GetLogger().Errorf("failed to update the quota value")
		return nil
	}
	quota, ok := data["quota"]
	if !ok {
		log.GetLogger().Errorf("failed to get the quota value")
		return nil
	}
	tenantMsg := t.getTenantInfo(tenantID)
	tenantMsg.Quota = quota
	t.syncLimiter(tenantID, tenantMsg)
	log.GetLogger().Infof("update tenant %s quota update to %d.", urnutils.Anonymize(tenantID), quota)
	return nil
}

// ProcessDelete -
func (t *TenantContainer) ProcessDelete(event *etcd3.Event) {
	strs := strings.Split(event.Key, constant.KeySeparator)
	if len(strs) <= tenantIDIndex {
		log.GetLogger().Errorf("failed to get the tenantID")
		return
	}
	tenantID := strs[tenantIDIndex]
	quota := constant.DefaultTenantLimitRate
	tenantMsg := t.getTenantInfo(tenantID)
	tenantMsg.Quota = quota
	t.syncLimiter(tenantID, tenantMsg)
	log.GetLogger().Infof("delete tenant %s quota, and set it to default %d.", urnutils.Anonymize(tenantID), quota)
}
