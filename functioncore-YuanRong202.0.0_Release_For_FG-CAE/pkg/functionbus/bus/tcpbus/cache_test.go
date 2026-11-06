/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package tcpbus

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConnCache(t *testing.T) {
	cc := ConnCache{}

	ch := cc.GetOrCreateConnWaiter("test1")
	assert.NotNil(t, ch, "get or create should not be nil")

	cc.SetOrUpdateConnReady("test1", RuntimeReport{Stage: "load"})

	select {
	case r := <-ch:
		assert.Equal(t, "load", r.Stage)
	default:
		assert.Fail(t, "should get ready conn")
	}

	cc.SetOrUpdateConnReady("test2", RuntimeReport{Stage: "initialize"})
	ch2 := cc.GetOrCreateConnWaiter("test2")
	assert.NotNil(t, ch2, "get or create should not be nil")
	select {
	case r := <-ch2:
		assert.Equal(t, "initialize", r.Stage)
	default:
		assert.Fail(t, "should get ready conn")
	}

	cc.Delete("test1")
	cc.Delete("test2")
}
