/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package breaker for specialization failure
package breaker

import (
	"encoding/json"
	"os"
	"sync"

	"functioncore/pkg/common/logger/log"
)

var (
	defaultRuleConfig = &Rule{
		TimeWindow:          10,
		MaxConsecutiveFails: 3,
		MaxAllowedFails:     10,
		CoolingTimeout:      10,
		HalfOpenPassRate:    0.5,
	}
	ruleConfig = defaultRuleConfig
	ruleOnce   sync.Once
)

// Rule ruler config for breaker
type Rule struct {
	TimeWindow          int     `json:"timeWindow"`
	MaxConsecutiveFails int     `json:"maxConsecutiveFails"`
	MaxAllowedFails     int     `json:"maxAllowedFails"`
	CoolingTimeout      int     `json:"coolingTimeout"`
	HalfOpenPassRate    float64 `json:"halfOpenPassRate"`
}

// GetRule new rule with system environment
func GetRule() *Rule {
	ruleOnce.Do(func() {
		ruleConfigStr := os.Getenv("CIRCUIT_BREAKER_RULE_CONFIG")
		if ruleConfigStr == "" {
			ruleConfig = defaultRuleConfig
			log.GetLogger().Warnf("use default rule config for circuit breaker")
			return
		}
		if err := json.Unmarshal([]byte(ruleConfigStr), ruleConfig); err != nil {
			ruleConfig = defaultRuleConfig
			log.GetLogger().Warnf("failed to unmarshal rule config with %s, %s", ruleConfigStr, err.Error())
		}
	})
	return ruleConfig
}
