/*
*
*  * Copyright (c) 2024 Huawei Technologies Co., Ltd
*  *
*  * This software is licensed under Mulan PSL v2.
*  * You can use this software according to the terms and conditions of the Mulan PSL v2.
*  * You may obtain a copy of Mulan PSL v2 at:
*  *
*  * http://license.coscl.org.cn/MulanPSL2
*  *
*  * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
*  * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
*  * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
*  * See the Mulan PSL v2 for more details.
*
 */

package scaler

import (
	"testing"

	"functioncore/pkg/autoscaler/config"
)

func TestResourceConfig_CreateCPUResources(t *testing.T) {
	var wantTargetUtilPercentage int64 = 80
	wantResName := CPU
	var wantUseLimitRate float64 = 1000
	cpuResCfg := createCPUResourceConfig(config.MetricConfig{
		TargetCPUUtilPercentage: wantTargetUtilPercentage,
	})

	if wantResName != cpuResCfg.getResourceName() {
		t.Errorf("getResourceName() got = %v, want %v", wantResName, cpuResCfg.getResourceName())
	}

	if wantUseLimitRate != cpuResCfg.getUsedLimitRate() {
		t.Errorf("getUsedLimitRate() got = %v, want %v", wantResName, cpuResCfg.getUsedLimitRate())
	}

}

func TestResourceConfig_CreateMEMResources(t *testing.T) {
	var wantTargetUtilPercentage int64 = 80
	wantResName := MEM
	var wantUseLimitRate float64 = 1
	cpuResCfg := createMEMResourceConfig(config.MetricConfig{
		TargetCPUUtilPercentage: wantTargetUtilPercentage,
	})

	if wantResName != cpuResCfg.getResourceName() {
		t.Errorf("getResourceName() got = %v, want %v", wantResName, cpuResCfg.getResourceName())
	}

	if wantUseLimitRate != cpuResCfg.getUsedLimitRate() {
		t.Errorf("getUsedLimitRate() got = %v, want %v", wantResName, cpuResCfg.getUsedLimitRate())
	}
}

func TestResourceConfig_CreateInvalidResources(t *testing.T) {
	wantResName := "Invalid"
	var wantUseLimitRate float64 = 1
	resCfg := ResourceConfig{
		resourceName:                 "Invalid",
		targetResourceUtilPercentage: 10,
		scaleUpTolerance:             1,
	}

	if wantResName != resCfg.getResourceName() {
		t.Errorf("getResourceName() got = %v, want %v", wantResName, resCfg.getResourceName())
	}

	if wantUseLimitRate != resCfg.getUsedLimitRate() {
		t.Errorf("getUsedLimitRate() got = %v, want %v", wantResName, resCfg.getUsedLimitRate())
	}

	usage, limit := resCfg.getResourceStatus(nil)

	if usage != 0 || limit != 0 {
		t.Errorf("usage and limit should be 0 for invalid resource")
	}
}
