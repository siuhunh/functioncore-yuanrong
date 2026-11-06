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
	"fmt"
	"math"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/logger/log"
)

const (
	usedLimitRate     = 1000
	percent           = 100
	noUseTime         = 15
	caeMetricsUseTime = 30
	threshold         = 1e-9
)

// FunctionScaler defines FunctionScaler's struct
type FunctionScaler struct {
	functionSvcID      string
	metricConfig       config.MetricConfig
	resiliencyStrategy Strategy
}

func (s *FunctionScaler) generateActions(totalExpected, expectedForPredict int64,
	curInsCount, scalingNum int64, actions map[string]Action) {

	if actions == nil {
		actions = make(map[string]Action)
	}

	if expectedForPredict > 0 {
		s.generateActionsForPredict(totalExpected, expectedForPredict, curInsCount, scalingNum, actions)
	} else {
		s.generateActionsForNormal(totalExpected, curInsCount, scalingNum, actions)
	}

	return
}

// if current == totalExpected, do nothing
func (s *FunctionScaler) generateActionsForPredict(totalExpected, expectedForPredict int64,
	curInsCount, scalingNum int64, actions map[string]Action) {

	if actions == nil {
		actions = make(map[string]Action)
	}

	nowInstanceNum := curInsCount + scalingNum

	if nowInstanceNum == 0 && expectedForPredict > 0 {
		// rule 1: if not have instance now but predict have instance, we use predict instance num
		scalePercent := config.GetScaleUpPercent(s.functionSvcID)
		// when scaling up from 0, the step size is calculated based on expectedForPredict
		needPull := calculateAdjustedScale(expectedForPredict, expectedForPredict, scalePercent)
		pullAction := Action{Type: ActionPull, NeedPull: needPull,
			TotalExpected: expectedForPredict, ScalingNum: scalingNum}
		actions[s.functionSvcID] = pullAction
		log.GetLogger().Infof("generate actions for predict, func: %s need pull %d instance, curInsCount: %d, "+
			"scalingNum: %d, expectedForPredict: %d, totalExpected: %d", s.functionSvcID, needPull,
			curInsCount, scalingNum, expectedForPredict, totalExpected)
	} else if totalExpected < nowInstanceNum {
		// rule 2: if totalExpected think need scale down, not use expectedForPredict
		scalePercent := config.GetScaleDownPercent(s.functionSvcID)
		needReduce := calculateAdjustedScale(nowInstanceNum-totalExpected, curInsCount, scalePercent)
		reduceAction := Action{Type: ActionReduce, NeedReduce: needReduce,
			TotalExpected: totalExpected, ScalingNum: scalingNum}
		actions[s.functionSvcID] = reduceAction
		log.GetLogger().Infof("generate actions for predict, func: %s need reduce %d instance, curInsCount: %d, "+
			"scalingNum: %d, expectedForPredict: %d, totalExpected: %d", s.functionSvcID, needReduce,
			curInsCount, scalingNum, expectedForPredict, totalExpected)
	} else if totalExpected > nowInstanceNum {
		// rule 3: if expectedForPredict over than totalExpected, use expectedForPredict
		s.generateActionWhenNeedPullUpInstance(totalExpected, expectedForPredict, curInsCount, scalingNum, actions)
	} else if totalExpected == nowInstanceNum && expectedForPredict > nowInstanceNum {
		// rule 4: if totalExpected equals curInsCount and expectedForPredict over than curInsCount, use expectedForPredict
		scalePercent := config.GetScaleUpPercent(s.functionSvcID)
		needPull := calculateAdjustedScale(expectedForPredict-nowInstanceNum, curInsCount, scalePercent)
		pullAction := Action{Type: ActionPull, NeedPull: needPull,
			TotalExpected: expectedForPredict, ScalingNum: scalingNum}
		actions[s.functionSvcID] = pullAction
		log.GetLogger().Infof("generate actions for predict, func: %s need pull %d instance, curInsCount: %d, "+
			"scalingNum: %d, expectedForPredict: %d, totalExpected: %d", s.functionSvcID, needPull,
			curInsCount, scalingNum, expectedForPredict, totalExpected)
	} else {
		// make the next reduction later
		SetLastScaleUpTime(s.functionSvcID)
		log.GetLogger().Infof("curInsCount %d equals totalExpected %d and "+
			"expectedForPredict %d for funcSvc: %s", curInsCount, totalExpected,
			expectedForPredict, s.functionSvcID)
	}
}

func (s *FunctionScaler) generateActionWhenNeedPullUpInstance(
	totalExpected int64, expectedForPredict int64, curInsCount int64, scalingNum int64, actions map[string]Action) {
	nowInstanceNum := curInsCount + scalingNum
	if expectedForPredict > totalExpected {
		scalePercent := config.GetScaleUpPercent(s.functionSvcID)
		needPull := calculateAdjustedScale(expectedForPredict-nowInstanceNum, curInsCount, scalePercent)
		pullAction := Action{Type: ActionPull, NeedPull: needPull,
			TotalExpected: expectedForPredict, ScalingNum: scalingNum}
		actions[s.functionSvcID] = pullAction
		log.GetLogger().Infof("generate actions for predict, func: %s need pull %d instance, curInsCount: %d, "+
			"scalingNum : %d, expectedForPredict: %d, totalExpected: %d", s.functionSvcID,
			needPull, curInsCount, scalingNum, expectedForPredict, totalExpected)
	} else {
		scalePercent := config.GetScaleUpPercent(s.functionSvcID)
		needPull := calculateAdjustedScale(totalExpected-nowInstanceNum, curInsCount, scalePercent)
		pullAction := Action{Type: ActionPull, NeedPull: needPull,
			TotalExpected: totalExpected, ScalingNum: scalingNum}
		actions[s.functionSvcID] = pullAction
		log.GetLogger().Infof("generate actions for predict, func: %s need pull %d instance, curInsCount: %d, "+
			"scalingNum: %d, expectedForPredict: %d, totalExpected: %d", s.functionSvcID, needPull,
			curInsCount, scalingNum, expectedForPredict, totalExpected)
	}
}

func calculateAdjustedScale(scaleNum int64, currentNum int64, scalePercent int64) int64 {
	// If not configured or not legally configured, the original calculated value is used
	if scalePercent <= 0 {
		return scaleNum
	}
	adjustedScaleNum, err := saferPercentCalc(currentNum, scalePercent)
	if err != nil {
		log.GetLogger().Warnf("saferPercentCalc error: %s, use scaleNum", err.Error())
		return scaleNum
	}
	if adjustedScaleNum < scaleNum {
		log.GetLogger().Debugf("adjustedScaleNum smaller than scaleNum, adjustScaleNum is %d, scaleNum %d",
			adjustedScaleNum, scaleNum)
		return adjustedScaleNum
	}
	return scaleNum
}

func saferPercentCalc(value int64, scalePercent int64) (int64, error) {
	res := float64(value) * float64(scalePercent) / percent
	if res > math.MaxInt64 || res < math.MinInt64 {
		return 0, fmt.Errorf("overflow: %v%% of %d exceeds int64 range", scalePercent, value)
	}
	return int64(math.Ceil(res)), nil
}

func (s *FunctionScaler) generateActionsForNormal(totalExpected int64, curInsCount, scalingNum int64,
	actions map[string]Action) {

	if actions == nil {
		actions = make(map[string]Action)
	}

	nowInstanceNum := curInsCount + scalingNum

	if totalExpected > nowInstanceNum {
		needPull := totalExpected - nowInstanceNum
		scalePercent := config.GetScaleUpPercent(s.functionSvcID)
		needPull = calculateAdjustedScale(needPull, curInsCount, scalePercent)
		pullAction := Action{
			Type:          ActionPull,
			NeedPull:      needPull,
			TotalExpected: totalExpected,
			ScalingNum:    scalingNum,
		}
		actions[s.functionSvcID] = pullAction
		log.GetLogger().Infof("generate actions for normal, func: %s need pull %d instance, curInsCount: %d, "+
			"scalingNum: %d, totalExpected: %d", s.functionSvcID, needPull,
			curInsCount, scalingNum, totalExpected)
	} else if totalExpected < nowInstanceNum {
		needReduce := nowInstanceNum - totalExpected
		scalePercent := config.GetScaleDownPercent(s.functionSvcID)
		needReduce = calculateAdjustedScale(needReduce, curInsCount, scalePercent)
		reduceAction := Action{
			Type:          ActionReduce,
			NeedReduce:    needReduce,
			TotalExpected: totalExpected,
			ScalingNum:    scalingNum,
		}
		actions[s.functionSvcID] = reduceAction
		log.GetLogger().Infof("generate actions for normal, func: %s need reduce %d instance, curInsCount: %d, "+
			"scalingNum: %d, totalExpected: %d", s.functionSvcID, needReduce,
			curInsCount, scalingNum, totalExpected)
	} else {
		// make the next reduction later
		SetLastScaleUpTime(s.functionSvcID)
		log.GetLogger().Infof("curInsCount %d equals totalExpected %d for funcSvc: %s", curInsCount, totalExpected,
			s.functionSvcID)
	}
}
