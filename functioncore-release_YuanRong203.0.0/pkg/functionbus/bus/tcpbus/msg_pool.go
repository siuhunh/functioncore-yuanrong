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

package tcpbus

import (
	"sync"

	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
)

var mp = msgPool{
	Pool: sync.Pool{New: func() interface{} {
		return protocol.NewMessage()
	}},
}

type msgPool struct {
	sync.Pool
}

// Get -
func (m *msgPool) Get() *protocol.Message {
	ret, ok := m.Pool.Get().(*protocol.Message)
	if !ok {
		return nil
	}
	ret.Reset()
	return ret
}

// Put -
func (m *msgPool) Put(msg *protocol.Message) {
	m.Pool.Put(msg)
}
