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

package trafficlimit

import (
	"functioncore/pkg/frontend-invoke/constant"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func Test_judgeTrafficParams(t *testing.T) {
	convey.Convey("TrafficLimitParams is nil", t, func() {
		config := &constant.Config{}
		trafficVariables := judgeTrafficParams(config)
		convey.ShouldNotBeNil(trafficVariables)
	})
	convey.Convey("TrafficLimitParams is not nil", t, func() {
		config := &constant.Config{
			TrafficLimitParams: &constant.TrafficLimitParams{
				InstanceLimitRate:  2.0,
				InstanceBucketSize: 2,
				FuncLimitRate:      2.0,
				FuncBucketSize:     2,
			},
		}
		trafficVariables := judgeTrafficParams(config)
		convey.ShouldNotBeNil(trafficVariables)
	})
}
