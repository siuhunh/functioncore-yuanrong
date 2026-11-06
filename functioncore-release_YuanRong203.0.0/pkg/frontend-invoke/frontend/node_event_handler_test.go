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

package frontend

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/frontend-invoke/functionlb"
)

func TestProcessNodeEvent(t *testing.T) {
	convey.Convey("test process node event", t, func() {
		convey.Convey("process err", func() {
			defer gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return errors.New("")
			}).Reset()
			convey.Convey("add event", func() {
				nodeRoute := &functionlb.Nodes{}
				defer gomonkey.ApplyMethod(reflect.TypeOf(nodeRoute), "AddNode", func(_ *functionlb.Nodes,
					node *functionlb.Node, nodeKey string) {
				}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processNodeEvent(event), convey.ShouldBeError)
			})
			convey.Convey("remove event", func() {
				nodeRoute := &functionlb.Nodes{}
				defer gomonkey.ApplyMethod(reflect.TypeOf(nodeRoute), "RemoveNode", func(_ *functionlb.Nodes,
					nodeKey string) {
				}).Reset()
				event := &etcd3.Event{Type: etcd3.DELETE}
				convey.So(processNodeEvent(event), convey.ShouldBeNil)
			})
		})
		convey.Convey("process ok", func() {
			defer gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return nil
			}).Reset()
			convey.Convey("add event", func() {
				nodeRoute := &functionlb.Nodes{}
				defer gomonkey.ApplyMethod(reflect.TypeOf(nodeRoute), "AddNode", func(_ *functionlb.Nodes,
					node *functionlb.Node, nodeKey string) {
				}).Reset()
				event := &etcd3.Event{Type: etcd3.PUT}
				convey.So(processNodeEvent(event), convey.ShouldBeNil)
			})
			convey.Convey("remove event", func() {
				nodeRoute := &functionlb.Nodes{}
				defer gomonkey.ApplyMethod(reflect.TypeOf(nodeRoute), "RemoveNode", func(_ *functionlb.Nodes,
					nodeKey string) {
				}).Reset()
				event := &etcd3.Event{Type: etcd3.DELETE}
				convey.So(processNodeEvent(event), convey.ShouldBeNil)
			})
		})
		convey.Convey("test other event", func() {
			convey.Convey("test etcd err event", func() {
				event := &etcd3.Event{Type: etcd3.ERROR}
				convey.So(processNodeEvent(event), convey.ShouldBeNil)
			})
			convey.Convey("test etcd undefined event", func() {
				event := &etcd3.Event{Type: -1}
				convey.So(processNodeEvent(event), convey.ShouldBeNil)
			})
		})
	})
}

func TestIsTaskNode(t *testing.T) {
	convey.Convey("Test IsTaskNode", t, func() {
		convey.Convey("is not TaskNode", func() {
			key1 := ""
			convey.So(IsTaskNode(key1), convey.ShouldBeFalse)
			key2 := " /sn/workers/business/yrk/tenant/0/function/function/version/$latest/defaultaz/node01"
			convey.So(IsTaskNode(key2), convey.ShouldBeFalse)
		})
		convey.Convey("is node", func() {
			key := " /sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/node01"
			convey.So(IsTaskNode(key), convey.ShouldBeTrue)
		})
	})
}
