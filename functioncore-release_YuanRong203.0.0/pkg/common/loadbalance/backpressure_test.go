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

func TestNodeRREmpty(t *testing.T) {
	rr := NewNodeRR()
	assert.Equal(t, "myself", rr.Next("myself"))
}

func TestNodeRR(t *testing.T) {
	rr := NewNodeRR()
	rr.Add("node-1")

	assert.Equal(t, "node-1", rr.Next("node-1"))

	rr.Add("node-2")

	assert.Equal(t, "node-2", rr.Next("node-1"))
	assert.Equal(t, "node-1", rr.Next("node-2"))

	rr.Add("node-3")

	assert.Equal(t, "node-2", rr.Next("node-1"))
	assert.Equal(t, "node-3", rr.Next("node-2"))
	assert.Equal(t, "node-1", rr.Next("node-3"))

	rr.Del("node-2")

	assert.Equal(t, "node-3", rr.Next("node-1"))
	assert.Equal(t, "node-1", rr.Next("node-3"))
}

func TestNodeRR2(t *testing.T) {
	rr := NewNodeRR()
	rr.Add("node-1")
	rr.Add("node-1")

	assert.Equal(t, "node-1", rr.Next("node-1"))

	rr.Add("node-2")
	rr.Add("node-2")

	assert.Equal(t, "node-2", rr.Next("node-1"))
	assert.Equal(t, "node-1", rr.Next("node-2"))

	rr.Del("node-2")

	assert.Equal(t, "node-2", rr.Next("node-1"))
	assert.Equal(t, "node-1", rr.Next("node-2"))

	rr.Del("node-2")

	assert.Equal(t, "node-1", rr.Next("node-1"))
}
