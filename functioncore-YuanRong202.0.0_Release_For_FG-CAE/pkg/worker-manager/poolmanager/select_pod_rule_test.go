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

package poolmanager

import (
	"sync"
	"testing"
)

func Test_selectPodRuleFactory_GetRule(t *testing.T) {
	type fields struct {
		normalPriorityPackingPool sync.Pool
	}
	type args struct {
		selectPodRuleType string
	}
	tests := []struct {
		name                      string
		fields                    fields
		args                      args
		wantNormalPriorityPacking bool
	}{
		{
			"test NormalPriorityPackingRule",
			fields{
				normalPriorityPackingPool: sync.Pool{},
			},
			args{
				selectPodRuleType: NormalPriorityPackingRule,
			},
			true,
		},
		{
			"test not NormalPriorityPackingRule",
			fields{
				normalPriorityPackingPool: sync.Pool{},
			},
			args{
				selectPodRuleType: "test",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &selectPodRuleFactory{
				normalPriorityPackingPool: sync.Pool{New: func() interface{} {
					return &NormalPriorityPacking{}
				}},
			}
			rule := s.GetRule(tt.args.selectPodRuleType)
			if _, ok := rule.(*NormalPriorityPacking); ok != tt.wantNormalPriorityPacking {
				t.Errorf("failed")
			}
		})
	}
}

func Test_selectPodRuleFactory_PutRuleToPool(t *testing.T) {
	type fields struct {
		normalPriorityPackingPool sync.Pool
	}
	type args struct {
		selectPodRuleType string
		rule              SelectPodRule
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test put pool success",
			fields{
				normalPriorityPackingPool: sync.Pool{},
			},
			args{
				selectPodRuleType: NormalPriorityPackingRule,
				rule:              &NormalPriorityPacking{},
			},
		},
		{
			"test put default pool success",
			fields{
				normalPriorityPackingPool: sync.Pool{},
			},
			args{
				selectPodRuleType: "test",
				rule:              &NormalPriorityPacking{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &selectPodRuleFactory{
				normalPriorityPackingPool: tt.fields.normalPriorityPackingPool,
			}
			s.PutRuleToPool(tt.args.selectPodRuleType, tt.args.rule)
		})
	}
}
