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

package scaler

import (
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/resourceusage"
)

const (
	// CPU resource name for cpu
	CPU = "cpu"
	// MEM resource name for mem
	MEM = "mem"
)

// ResourceConfig Type definition for resources which are used for scaling.
type ResourceConfig struct {
	resourceName                 string
	targetResourceUtilPercentage int64
	scaleUpTolerance             float64
	scaleDownTolerance           float64
}

func (resCfg *ResourceConfig) getResourceName() string {
	return resCfg.resourceName
}

func (resCfg *ResourceConfig) getTargetResourceUtilPercentage() int64 {
	return resCfg.targetResourceUtilPercentage
}

func (resCfg *ResourceConfig) getScaleUpTolerance() float64 {
	return resCfg.scaleUpTolerance
}

func (resCfg *ResourceConfig) getScaleDownTolerance() float64 {
	return resCfg.scaleDownTolerance
}

func (resCfg *ResourceConfig) getResourceStatus(res *resourceusage.Resource) (float64, int64) {
	switch resCfg.resourceName {
	case CPU:
		return res.CPUUsage, res.CPULimit
	case MEM:
		return res.MemoryUsage, res.MemoryLimit
	default:
		//default branch
		return 0, 0
	}
}

func (resCfg *ResourceConfig) getUsedLimitRate() float64 {
	switch resCfg.resourceName {
	case CPU:
		return usedLimitRate
	case MEM:
		return 1
	default:
		return 1
	}

}

func createMEMResourceConfig(config config.MetricConfig) ResourceConfig {
	return ResourceConfig{
		resourceName:                 MEM,
		targetResourceUtilPercentage: config.TargetMemUtilPercentage,
		scaleUpTolerance:             config.ScaleUpTolerance,
		scaleDownTolerance:           config.ScaleDownTolerance,
	}
}

func createCPUResourceConfig(config config.MetricConfig) ResourceConfig {
	return ResourceConfig{
		resourceName:                 CPU,
		targetResourceUtilPercentage: config.TargetCPUUtilPercentage,
		scaleUpTolerance:             config.ScaleUpTolerance,
		scaleDownTolerance:           config.ScaleDownTolerance,
	}
}
