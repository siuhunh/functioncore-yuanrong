/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package configutil

import (
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
)

// UpdateMemoryControlConfig update memory control config
func UpdateMemoryControlConfig(newConfig *types.MemoryControlConfig, oldConfig *types.MemoryControlConfig) {
	if newConfig == nil || oldConfig == nil {
		return
	}
	if newConfig.LowerMemoryPercent > 0 {
		log.GetLogger().Infof("LowerMemoryPercent update old: %f, new: %f",
			oldConfig.LowerMemoryPercent, newConfig.LowerMemoryPercent)
		oldConfig.LowerMemoryPercent = newConfig.LowerMemoryPercent
	}

	if newConfig.HighMemoryPercent > 0 {
		log.GetLogger().Infof("HighMemoryPercent update old: %f, new: %f",
			oldConfig.HighMemoryPercent, newConfig.HighMemoryPercent)
		oldConfig.HighMemoryPercent = newConfig.HighMemoryPercent
	}

	if newConfig.StatefulHighMemPercent > 0 {
		log.GetLogger().Infof("StatefulHighMemPercent update old: %f, new: %f",
			oldConfig.StatefulHighMemPercent, newConfig.StatefulHighMemPercent)
		oldConfig.StatefulHighMemPercent = newConfig.StatefulHighMemPercent
	}

	if newConfig.BodyThreshold > 0 {
		log.GetLogger().Infof("BodyThreshold update old: %d, new: %d",
			oldConfig.BodyThreshold, newConfig.BodyThreshold)
		oldConfig.BodyThreshold = newConfig.BodyThreshold
	}

	if newConfig.MemDetectIntervalMs > 0 {
		log.GetLogger().Infof("MemDetectIntervalMs update old: %d, new: %d",
			oldConfig.MemDetectIntervalMs, newConfig.MemDetectIntervalMs)
		oldConfig.MemDetectIntervalMs = newConfig.MemDetectIntervalMs
	}
}
