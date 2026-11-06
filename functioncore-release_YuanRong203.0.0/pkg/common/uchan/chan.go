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

// Package uchan implements unbounded channel which has unlimited buffer size
package uchan

import (
	"container/list"
)

// NewUnboundedChan returns an unbounded chan with initCap
func NewUnboundedChan(initCap int) *UnboundedChan {
	if initCap <= 0 {
		initCap = 1
	}
	in := make(chan interface{}, initCap)
	out := make(chan interface{}, initCap)
	uc := &UnboundedChan{
		In:  in,
		Out: out,
		buf: list.New(),
	}
	go uc.run(in, out)
	return uc
}

// UnboundedChan channel with unlimited buffer size
// use In as the input and Out as the output of UnboundedChan
type UnboundedChan struct {
	buf *list.List
	In  chan<- interface{}
	Out <-chan interface{}
}

func (uc *UnboundedChan) run(in, out chan interface{}) {
	if in == nil || out == nil {
		return
	}
	defer close(out)

	consume := func() bool {
		select {
		case val, ok := <-in:
			if !ok {
				return false
			}
			uc.buf.PushBack(val)
		case out <- uc.buf.Front().Value:
			uc.buf.Remove(uc.buf.Front())
		}
		return true
	}

loop:
	for {
		val, ok := <-in
		if !ok {
			break loop
		}
		select {
		case out <- val:
			continue
		default:
		}
		uc.buf.PushBack(val)

		for uc.buf.Len() > 0 {
			if !consume() {
				break loop
			}
		}
	}

	for uc.buf.Len() > 0 {
		out <- uc.buf.Front().Value
		uc.buf.Remove(uc.buf.Front())
	}
	return
}
