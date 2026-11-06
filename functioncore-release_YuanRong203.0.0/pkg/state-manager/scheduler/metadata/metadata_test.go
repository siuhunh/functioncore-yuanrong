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

package metadata

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	rm "functioncore/pkg/state-manager/resourcemanager"
)

func TestGetAffinityNode(t *testing.T) {
	convey.Convey("GetAffinityNode", t, func() {
		convey.Convey("test 1", func() {
			res, _ := GetAffinityNode("typeA")
			convey.So(res, convey.ShouldEqual, nil)
		})
		convey.Convey("test 2", func() {
			stateType := "typeA"
			affinityTable.table[stateType] = make(map[string]struct{})
			_, err := GetAffinityNode(stateType)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestGetNodeAssignedData(t *testing.T) {
	convey.Convey("GetNodeAssignedData", t, func() {
		convey.Convey("test 1", func() {
			_, err := GetNodeAssignedData("node1")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("test 2", func() {
			getStateIDFunc = func(s string) (i map[string]struct{}, e error) {
				return make(map[string]struct{}), nil
			}
			_, err := GetNodeAssignedData("node1")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test 3", func() {
			getStateIDFunc = func(s string) (i map[string]struct{}, e error) {
				return nil, errors.New("error")
			}
			_, err := GetNodeAssignedData("node1")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestGetNodes(t *testing.T) {
	convey.Convey("GetNodes", t, func() {
		errFoo := errors.New("err")
		nodesFoo := make([]string, 1, 1)
		nodesFoo[0] = "node1"
		convey.Convey("test 1", func() {
			patches := [1]*gomonkey.Patches{
				gomonkey.ApplyFunc(rm.GetAllNodes,
					func() ([]string, error) {
						return nil, errFoo
					}),
			}
			defer patches[0].Reset()
			_, err := GetNodes()
			convey.So(err, convey.ShouldEqual, errFoo)
		})

		convey.Convey("test 2", func() {
			patches := [1]*gomonkey.Patches{
				gomonkey.ApplyFunc(rm.GetAllNodes,
					func() ([]string, error) {
						return nodesFoo, nil
					}),
			}
			defer patches[0].Reset()
			_, err := GetNodes()
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestGetTargetInfo(t *testing.T) {
	convey.Convey("GetTargetInfo", t, func() {
		convey.Convey("test 1", func() {
			_, err := GetTargetInfo("function")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func wrapRegister() error {
	Register(func(s string) (i map[string]struct{}, e error) {
		return nil, nil
	})
	return nil
}

func Test_wrapRegister(t *testing.T) {
	convey.Convey("wrapRegister", t, func() {
		convey.Convey("test 1", func() {
			err := wrapRegister()
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func wrapAddNode() error {
	AddNode("typeA", "stateID")
	return nil
}

func TestAddNode(t *testing.T) {
	convey.Convey("AddNode", t, func() {
		convey.Convey("test 1", func() {
			err := wrapAddNode()
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestGetDirectStateLoad(t *testing.T) {
	nodeID := "node1"
	GetDirectStateLoad(nodeID)
}

func TestGetIndirectStateLoad(t *testing.T) {
	nodeID := "node1"
	GetIndirectStateLoad(nodeID)
}
