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

package loadbalance

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

const (
	nodeKey        = "faas-scheduler-6b758c8b74-5zdwv"
	funcKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
)

var (
	node1 = &Node{
		Key: nodeKey,
	}

	node2 = &Node{
		Key: nodeKey + "1",
	}

	node3 = &Node{
		Key: nodeKey + "2",
	}
)

type mockRealNode struct {
	state bool
}

func (node *mockRealNode) IsEnable() bool {
	return node.state
}

func TestConsistentHashWithVirtualNode(t *testing.T) {
	replica := 12
	lb := NewConsistentHashWithVirtualNode(replica)
	nodes := []*Node{node1, node2, node3}
	for i := range nodes {
		err := lb.Add(nodes[i])
		assert.Nil(t, err, "add node should success", i)
		assert.Equal(t, replica*(i+1), len(lb.keys), i)
		assert.Equal(t, replica*(i+1), len(lb.hash), i)
	}

	hashedNode := lb.Next(funcKeyWithRes)
	assert.NotNil(t, hashedNode)

	deleted := lb.Delete("key_not_exit")
	assert.Nil(t, deleted)
	assert.Equal(t, replica*(len(nodes)), len(lb.keys))

	assert.Equal(t, hashedNode, lb.Next(funcKeyWithRes))

	deleted = lb.Delete(hashedNode.Key)
	assert.Equal(t, deleted, hashedNode)
	assert.Equal(t, replica*(len(nodes)-1), len(lb.keys))

	hashedNode2 := lb.Next(funcKeyWithRes)
	assert.NotEqual(t, hashedNode2, hashedNode)
}

func TestStatefulConsistent(t *testing.T) {
	convey.Convey("TestStatefulConsistentHashWithOneNode", t, func() {
		lb := CreateNoLockLB()
		outNode := lb.Next(funcKeyWithRes)
		convey.So(outNode, convey.ShouldBeNil)

		lb.Add(node1)
		lb.Add(node1)

		outNode = lb.Next(funcKeyWithRes)
		convey.So(outNode, convey.ShouldNotBeNil)
		convey.So(outNode.Key, convey.ShouldEqual, node1.Key)

		outNode = lb.Delete(nodeKey)
		convey.So(outNode, convey.ShouldNotBeNil)
		convey.So(outNode.Key, convey.ShouldEqual, node1.Key)

		outNode = lb.Delete(nodeKey)
		convey.So(outNode, convey.ShouldBeNil)

		outNode = lb.Next(funcKeyWithRes)
		convey.So(outNode, convey.ShouldBeNil)

		lb.Add(node2)
		lb.Add(node3)
		lb.Add(node1)

		outNode = lb.Next(funcKeyWithRes)
		convey.So(outNode, convey.ShouldNotBeNil)
		convey.So(outNode.Key, convey.ShouldEqual, node2.Key)

	})
}

func BenchmarkStatefulConsistentHashWithThreeNode(b *testing.B) {
	lb := CreateNoLockLB()

	lb.Add(node1)
	lb.Add(node2)
	lb.Add(node3)

	for i := 0; i < b.N; i++ {
		lb.Next(funcKeyWithRes + "3")
	}
}
