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

	corev1 "k8s.io/api/core/v1"
)

// SelectPodRuleFactory is selectPodRuleFactory
var SelectPodRuleFactory selectPodRuleFactory

const (
	// NormalPriorityPackingRule is normalPriorityPackingRule
	NormalPriorityPackingRule = "NormalPriorityPackingRule"
)

// SelectPodRule select pod rule interface
type SelectPodRule interface {
	Recover() SelectPodRule
	WithEnableTMPExpansion(enableTMPExpansion bool) SelectPodRule
	WithNoScheduleNodes(noScheduleNodes []string) SelectPodRule
	WithFuncName(funcName string) SelectPodRule
	SelectPod(p *Pool) *corev1.Pod
	GetEnableTMPExpansion() bool
	GetFuncName() string
}

type selectPodRuleFactory struct {
	// only normalPriorityPackingPool now
	// if there are other selectPodRuleType, we need more pool
	normalPriorityPackingPool sync.Pool
}

func init() {
	SelectPodRuleFactory = selectPodRuleFactory{
		normalPriorityPackingPool: sync.Pool{New: func() interface{} {
			return &NormalPriorityPacking{}
		}},
	}
}

func (s *selectPodRuleFactory) GetRule(selectPodRuleType string) SelectPodRule {
	switch selectPodRuleType {
	case NormalPriorityPackingRule:
		return s.normalPriorityPackingPool.Get().(*NormalPriorityPacking)
	default:
		return s.normalPriorityPackingPool.Get().(*NormalPriorityPacking)
	}

	return s.normalPriorityPackingPool.Get().(*NormalPriorityPacking)
}

func (s *selectPodRuleFactory) PutRuleToPool(selectPodRuleType string, rule SelectPodRule) {
	switch selectPodRuleType {
	case NormalPriorityPackingRule:
		rule.Recover()
		s.normalPriorityPackingPool.Put(rule)
	default:
		rule.Recover()
		s.normalPriorityPackingPool.Put(rule)
	}
}
