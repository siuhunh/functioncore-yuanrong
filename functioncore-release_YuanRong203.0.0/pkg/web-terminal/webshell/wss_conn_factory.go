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

// Package webshell define function to exec webshell
package webshell

import (
	"github.com/gorilla/websocket"
)

// K8s -
const K8s = iota

// WssConn -
type WssConn interface {
	GetWssConn(requestInfo *RequestInfo) (*websocket.Conn, error)
}

// WssConnFactory -
func WssConnFactory(t int) WssConn {
	switch t {
	case K8s:
		return &K8sWssConn{}
	default:
		return &K8sWssConn{}
	}
}
