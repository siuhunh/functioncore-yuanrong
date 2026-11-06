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

package instance

import (
	"reflect"
	"testing"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/frontend-invoke/loadbalancer/common"
)

var (
	testFuncKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	testInstanceKey    = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	testProcessor      = &common.Processor{
		ProcessorID:  1,
		ThreadNum:    1,
		LeaseVersion: 1,
	}
)

func TestAddProcessor(t *testing.T) {
	convey.Convey("TestAddProcessor", t, func() {
		instance := CreateInstance(testFuncKeyWithRes, testInstanceKey)
		instance.AddProcessor(testProcessor)
		enable := instance.TrySchedule(false)
		convey.So(enable, convey.ShouldBeTrue)

		enable = instance.TrySchedule(false)
		convey.So(enable, convey.ShouldBeFalse)

		enable = instance.TrySchedule(true)
		convey.So(enable, convey.ShouldBeTrue)

		processor := instance.Done()
		convey.So(processor, convey.ShouldBeNil)
		processor = instance.Done()
		convey.So(processor, convey.ShouldNotBeNil)
		convey.So(reflect.DeepEqual(testProcessor, processor), convey.ShouldBeTrue)

		processor = instance.Done()
		convey.So(processor, convey.ShouldBeNil)

		enable = instance.TrySchedule(false)
		convey.So(enable, convey.ShouldBeFalse)
	})
}
