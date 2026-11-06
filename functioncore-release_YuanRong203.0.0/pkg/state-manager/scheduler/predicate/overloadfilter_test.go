/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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
package predicate

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	rm "functioncore/pkg/state-manager/resourcemanager"
	"functioncore/pkg/state-manager/scheduler/metadata"
)

func Test_overloadFilter_Filter(t *testing.T) {
	convey.Convey("Filter", t, func() {
		filterFoo := overloadFilter{}
		infos := make([]*metadata.NodeInfo, 1, 1)
		infos[0] = &metadata.NodeInfo{ID: "node1"}

		convey.Convey("test 1", func() {
			patches := [2]*gomonkey.Patches{
				gomonkey.ApplyFunc(rm.GetAllResource,
					func(nodeName string) (*rm.NodeResourceInfo, error) {
						return &rm.NodeResourceInfo{
							CPU:            1,
							CPUCapacity:    100,
							Memory:         1,
							MemoryCapacity: 100,
						}, nil
					}),
			}
			defer patches[0].Reset()
			_, err := filterFoo.Filter(nil, infos)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}
