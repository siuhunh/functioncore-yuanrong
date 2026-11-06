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
	"reflect"
	"sync"
	"testing"
)

func TestGetRule(t *testing.T) {
	ownedRuleConfig := &Rule{
		TimeWindow:          9,
		MaxConsecutiveFails: 3,
		MaxAllowedFails:     8,
		CoolingTimeout:      7,
		HalfOpenPassRate:    0.6,
	}
	ownedRuleConfigBytes, _ := json.Marshal(ownedRuleConfig)
	tests := []struct {
		name   string
		setEnv func()
		want   *Rule
	}{
		{
			name: "test with no set env",
			setEnv: func() {
				ruleOnce = sync.Once{} // every test case use new sync once
			},
			want: defaultRuleConfig,
		},
		{
			name: "test with empty env",
			setEnv: func() {
				ruleOnce = sync.Once{} // every test case use new sync once
				os.Setenv("CIRCUIT_BREAKER_RULE_CONFIG", "")
			},
			want: defaultRuleConfig,
		},
		{
			name: "test with owned rule config",
			setEnv: func() {
				ruleOnce = sync.Once{} // every test case use new sync once
				os.Setenv("CIRCUIT_BREAKER_RULE_CONFIG", string(ownedRuleConfigBytes))
			},
			want: ownedRuleConfig,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setEnv()
			if got := GetRule(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRule() = %v, want %v", got, tt.want)
			}
		})
	}
}
