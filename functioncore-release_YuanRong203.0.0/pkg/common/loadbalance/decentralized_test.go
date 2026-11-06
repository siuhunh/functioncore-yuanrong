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

package loadbalance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Decentralized(t *testing.T) {
	lb := NewDecentralizedLB()
	lb.Add("instance1", 0)
	lb.Add("instance2", 0)
	lb.Add("instance3", 0)
	lb.Add("instance4", 0)
	lb.Add("instance5", 0)
	lb.Add("instance6", 0)
	resultMap := make(map[string]int, 5)
	for i := 0; i < 100000; i++ {
		instance := lb.Next("", false)
		resultMap[instance.(string)]++
	}
	for key, value := range resultMap {
		t.Logf("instance is %s, num is %d", key, value)
	}

	lb.Remove("instance3")
	lb.Remove("instance4")
	lb.Remove("instance5")
	lb.Remove("instance6")
	resultMap1 := make(map[string]int, 5)
	for i := 0; i < 100000; i++ {
		instance := lb.Next("", false)
		resultMap1[instance.(string)]++
	}
	for key, value := range resultMap1 {
		t.Logf("instance is %s, num is %d", key, value)
	}
}

func Test_DecentralizedDone(t *testing.T) {
	lb1 := NewDecentralizedLB()
	lb1.Add("instance1", 0)
	lb1.Add("instance2", 0)
	lb1.Add("instance3", 0)
	lb1.Next("", false)
	lb1.Next("", false)
	lb1.Next("", false)
	lb1.Done("instance1", 0)
	instance := lb1.Next("", false)
	assert.Equal(t, "instance1", instance.(string))
	assert.Equal(t, 3, lb1.totalRequestNum)
}

func Test_DecentralizedRemove(t *testing.T) {
	lb1 := NewDecentralizedLB()
	lb1.Add("instance1", 0)
	lb1.Add("instance2", 0)
	lb1.Add("instance3", 0)
	lb1.Remove("instance1")
	lb1.Remove("instance2")
	instance := lb1.Next("", false)
	assert.Equal(t, "instance3", instance.(string))
}
