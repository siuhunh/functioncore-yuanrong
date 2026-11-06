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
)

// Strategy -
type Strategy interface {
	calExpectedScaleNums() (int64, error)
}

func createStrategy(strategy []string, functionSvcID string, config config.MetricConfig) Strategy {
	if strategy == nil || len(strategy) == 0 {
		return createCPUStrategy(functionSvcID, config)
	}
	if len(strategy) > 1 {
		return createMixStrategy(functionSvcID, config)
	}
	if len(strategy) == 1 && strategy[0] == "mem" {
		return createMemStrategy(functionSvcID, config)
	}
	return createCPUStrategy(functionSvcID, config)
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
