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

package reqnode

import (
	"testing"

	"functioncore/pkg/functiontask/task/common"
)

// BenchmarkAddParallelNode checks the benchmark for adding parallel nodes.
func BenchmarkAddParallelNode(b *testing.B) {
	root := new(Node)
	head := NewHead()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		node := New(common.NewRequest(common.RequestOptions{
			FuncName:   "test",
			ReqIDList:  []string{common.NewReqID(nil, "", "", 0)},
			Config:     "",
		}))
		err := node.Add(root, head)
		if err != nil {
			print(err)
		}
	}
}

// BenchmarkAddChildNode checks the benchmark for adding nodes which has parent-and-child relation.
func BenchmarkAddChildNode(b *testing.B) {
	root := new(Node)
	head := NewHead()
	reqIDList := []string{common.NewReqID(nil, "", "", 0)}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reqIDList = append(reqIDList, common.NewReqID(nil, "", "", 0))
		node := New(common.NewRequest(common.RequestOptions{
			FuncName:   "test",
			ReqIDList:  reqIDList,
			Config:     "",
		}))
		err := node.Add(root, head)
		if err != nil {
			print(err)
		}
	}
}
