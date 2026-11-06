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

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
)

// Node represents a node which contains a invoke request.
type Node struct {
	Req      *common.Request
	Prev     *Node
	Next     *Node
	Parent   *Node
	Children []*Node
}

// New creates a node with the input request.
func New(r *common.Request) *Node {
	return &Node{Req: r}
}

// NewHead returns an empty head for a queue.
func NewHead() *Node {
	head := new(Node)
	head.Next = head
	head.Prev = head
	return head
}

// Add the node to the parent whose reqIDList is the ancestor of the node, and enqueue.
func (node *Node) Add(parent *Node, head *Node) error {
	if config.Conf.ResolveDeadlock {
		// add node to tree.
		for _, n := range parent.Children {
			rel := getRelation(node.Req.ReqIDList, n.Req.ReqIDList)
			if rel == parentRel {
				return fmt.Errorf("the descendant of the request already exists")
			}
			if rel == equalRel {
				return fmt.Errorf("the request already exists")
			}
			if rel == childRel {
				return node.Add(n, head)
			}
		}
		node.Parent = parent
		parent.Children = append(parent.Children, node)
		node.Children = []*Node{}
	}

	// enqueue.
	oldPrev := head.Prev
	oldPrev.Next = node
	node.Prev = oldPrev
	node.Next = head
	head.Prev = node
	return nil
}

// Remove the node from the tree, append its children to its parent, and dequeue.
func (node *Node) Remove() {
	if config.Conf.ResolveDeadlock {
		origin := node.Parent.Children
		target := origin[:0]
		target = node.handleTarget(origin, target)
		node.Parent.Children = target
	}

	oldPrev := node.Prev
	oldNext := node.Next
	oldPrev.Next = oldNext
	oldNext.Prev = oldPrev
}

func (node *Node) handleTarget(origin []*Node, target []*Node) []*Node {
	for _, n := range origin {
		if n == node {
			for _, nn := range n.Children {
				nn.Parent = node.Parent
			}
			target = append(target, n.Children...)
		} else {
			target = append(target, n)
		}
	}
	return target
}

// GetNodeOrItsDescendants returns the node whose reqIDList is list or the descendant of list.
func (node *Node) GetNodeOrItsDescendants(list common.ReqIDList) *Node {
	for _, n := range node.Children {
		rel := getRelation(list, n.Req.ReqIDList)
		if rel == parentRel || rel == equalRel {
			return n
		}
		if rel == childRel {
			return n.GetNodeOrItsDescendants(list)
		}
	}
	return nil
}
