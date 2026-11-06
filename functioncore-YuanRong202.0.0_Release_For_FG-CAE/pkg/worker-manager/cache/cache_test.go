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

package cache

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/config"
	tools "functioncore/pkg/worker-manager/utils"
)

const (
	funcSvcID = "function01"
)

var (
	count = 0
)

func NewAutoScaler() *autoscaler.AutoScaler {
	return &autoscaler.AutoScaler{}
}

func TestGetOrCreateAutoScaler(t *testing.T) {
	convey.Convey("TestGetOrCreateAutoScaler", t, func() {
		WorkerCache.HandleInsMetadata(funcSvcID, config.InstanceMetaData{
			MinInstance: 0, MaxInstance: 1, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})
		scaler, ok := GetOrCreateAutoScaler(funcSvcID, NewAutoScaler)
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(scaler, convey.ShouldNotBeNil)

		newScaler, newOk := GetOrCreateAutoScaler(funcSvcID, NewAutoScaler)
		convey.So(newOk, convey.ShouldBeTrue)
		convey.So(newScaler, convey.ShouldEqual, scaler)
	})
}
