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

// Package scaler -
package scaler

import (
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/logger/log"
)

const (
	usedLimitRate = 1000
	percent       = 100
	noUseTime     = 15
	threshold     = 1e-9
)

// FunctionScaler defines FunctionScaler's struct
type FunctionScaler struct {
	functionSvcID      string
	metricConfig       config.MetricConfig
	resiliencyStrategy Strategy
}

func (s *FunctionScaler) generateActions(totalExpected int64, curInsCount int64, actions map[string]Action) {
	var needPull int64
	var needReduce int64
	if actions == nil {
		actions = make(map[string]Action)
	}
	if totalExpected > curInsCount {
		needPull = totalExpected - curInsCount
		pullAction := Action{
			Type:          ActionPull,
			NeedPull:      needPull,
			TotalExpected: totalExpected,
		}
		actions[s.functionSvcID] = pullAction
	} else if totalExpected < curInsCount {
		needReduce = curInsCount - totalExpected
		reduceAction := Action{
			Type:          ActionReduce,
			NeedReduce:    needReduce,
			TotalExpected: totalExpected,
		}
		actions[s.functionSvcID] = reduceAction
	} else {
		log.GetLogger().Infof("curInsCount %d equals totalExpected %d for funcSvc: %s", curInsCount, totalExpected,
			s.functionSvcID)
	}
	return
}
