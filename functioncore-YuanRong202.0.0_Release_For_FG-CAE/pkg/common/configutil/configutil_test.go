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
	"testing"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/types"
)

func TestUpdateMemoryControlConfig(t *testing.T) {
	convey.Convey("TestUpdateMemoryControlConfig", t, func() {
		UpdateMemoryControlConfig(nil, nil)
		oldConfig := &types.MemoryControlConfig{}
		newConfig := &types.MemoryControlConfig{
			LowerMemoryPercent:     0.6,
			StatefulHighMemPercent: 0.85,
		}
		UpdateMemoryControlConfig(newConfig, oldConfig)
		convey.So(oldConfig.LowerMemoryPercent, convey.ShouldEqual, newConfig.LowerMemoryPercent)
		convey.So(oldConfig.StatefulHighMemPercent, convey.ShouldEqual, newConfig.StatefulHighMemPercent)
	})
	convey.Convey("TestUpdateMemoryControlConfig 2", t, func() {
		oldConfig := &types.MemoryControlConfig{}
		newConfig := &types.MemoryControlConfig{
			LowerMemoryPercent:     0.6,
			HighMemoryPercent:      0.8,
			StatefulHighMemPercent: 0.85,
			MemDetectIntervalMs:    1,
			BodyThreshold:          2,
		}
		UpdateMemoryControlConfig(newConfig, oldConfig)
		convey.So(oldConfig.LowerMemoryPercent, convey.ShouldEqual, newConfig.LowerMemoryPercent)
		convey.So(oldConfig.StatefulHighMemPercent, convey.ShouldEqual, newConfig.StatefulHighMemPercent)
	})
}
