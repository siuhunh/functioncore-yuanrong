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
	"functioncore/pkg/common/types/functiontypes"
)

// Strategy -
type Strategy interface {
	calExpectedScaleNums() (int64, int64, error)
}

func createStrategy(asbResConfig functiontypes.AsbResConfig, functionSvcID string,
	config config.MetricConfig) Strategy {

	scaleStrategy := asbResConfig.ScaleStrategy
	extendStrategies := asbResConfig.ExtendScaleStrategies

	strategies := createResourceAvgStrategy(functionSvcID, config)

	// if all not set, default use cpu
	if isExtendStrategiesEmpty(extendStrategies) && isStrategyEmpty(scaleStrategy) {
		log.GetLogger().Debugf("createStrategy, scaler resource config for func %s is %+v", functionSvcID, strategies)
		return strategies.withCPUResource()
	}

	// it means only extendStrategies, so just use extendStrategies
	if isStrategyEmpty(scaleStrategy) {
		log.GetLogger().Debugf("createStrategy, scaler resource config for func %s is %+v", functionSvcID, strategies)
		return strategies.withExtendStrategies(extendStrategies)
	}

	// mixed, cpu memory and extend strategy
	if len(scaleStrategy) == 1 && scaleStrategy[0] == "mem" {
		strategies = strategies.withMemoryResource()
	} else if len(scaleStrategy) > 1 {
		strategies = strategies.withCPUResource().withMemoryResource()
	} else {
		strategies = strategies.withCPUResource()
	}

	strategies.withExtendStrategies(extendStrategies)
	log.GetLogger().Debugf("createStrategy, scaler resource config for func %s is %+v", functionSvcID, strategies)
	return strategies

}

func isExtendStrategiesEmpty(extendScaleStrategies []functiontypes.ExtendScaleStrategy) bool {
	return extendScaleStrategies == nil || len(extendScaleStrategies) == 0
}

func isStrategyEmpty(strategy []string) bool {
	return strategy == nil || len(strategy) == 0
}

func createMixStrategy(functionSvcID string, config config.MetricConfig) Strategy {
	return &ResourceAvgStrategy{
		resources: []ResourceConfig{
			createCPUResourceConfig(config),
			createMEMResourceConfig(config),
		},
		functionSvcID: functionSvcID,
	}
}

func createMemStrategy(functionSvcID string, config config.MetricConfig) Strategy {
	return &ResourceAvgStrategy{
		resources: []ResourceConfig{
			createMEMResourceConfig(config),
		},
		functionSvcID: functionSvcID,
	}
}

func createCPUStrategy(functionSvcID string, config config.MetricConfig) Strategy {
	return &ResourceAvgStrategy{
		resources: []ResourceConfig{
			createCPUResourceConfig(config),
		},
		functionSvcID: functionSvcID,
	}
}
