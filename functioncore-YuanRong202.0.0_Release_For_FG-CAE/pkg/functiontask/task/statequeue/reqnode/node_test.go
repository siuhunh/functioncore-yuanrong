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

// Package reqnode implements the node which represents a request, and the node are stored both in tree and queue.
package reqnode

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/common"
)

// TestAddSame checks adding requests with he same request id list.
func TestAddSame(t *testing.T) {
	root := new(Node)
	head := NewHead()
	node1 := New(&common.Request{
		ReqIDList: []string{"1"},
	})
	node2 := New(&common.Request{
		ReqIDList: []string{"1"},
	})
	Convey("add first node", t, func() {
		err := node1.Add(root, head)
		So(err, ShouldBeNil)
		Convey("add second node", func() {
			err := node2.Add(root, head)
			So(err, ShouldResemble, fmt.Errorf("the request already exists"))
		})
	})
}

// TestAddParent checks adding request after its children.
func TestAddParent(t *testing.T) {
	root := new(Node)
	head := NewHead()
	node1 := New(&common.Request{
		ReqIDList: []string{"1", "2"},
	})
	node2 := New(&common.Request{
		ReqIDList: []string{"1"},
	})
	Convey("add first node", t, func() {
		err := node1.Add(root, head)
		So(err, ShouldBeNil)
		Convey("add second node", func() {
			err := node2.Add(root, head)
			So(err, ShouldResemble, fmt.Errorf("the descendant of the request already exists"))
		})
	})
}

// TestAddChild checks adding request after its parent.
func TestAddChild(t *testing.T) {
	root := new(Node)
	head := NewHead()
	node1 := New(&common.Request{
		ReqIDList: []string{"1"},
	})
	node2 := New(&common.Request{
		ReqIDList: []string{"1", "2"},
	})
	Convey("add first node", t, func() {
		err := node1.Add(root, head)
		So(err, ShouldBeNil)
		Convey("add second node", func() {
			err := node2.Add(root, head)
			So(err, ShouldBeNil)
		})
	})
}

// TestAddNoRel checks adding requests with no relation.
func TestAddNoRel(t *testing.T) {
	root := new(Node)
	head := NewHead()
	node1 := New(&common.Request{
		ReqIDList: []string{"3", "4"},
	})
	node2 := New(&common.Request{
		ReqIDList: []string{"3", "5"},
	})
	Convey("add first node", t, func() {
		err := node1.Add(root, head)
		So(err, ShouldBeNil)
		Convey("add second node", func() {
			err := node2.Add(root, head)
			So(err, ShouldBeNil)
			Convey("remove first node", func() {
				node1.Remove()
				So(head.Next, ShouldEqual, node2)
				Convey("remove second node", func() {
					node2.Remove()
					So(head.Next, ShouldEqual, head)
				})
			})
		})
	})
}
