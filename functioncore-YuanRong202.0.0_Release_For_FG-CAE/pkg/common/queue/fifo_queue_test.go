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

// Package queue imply multiple queues
package queue

import (
	"testing"
)

func mkFifoObj(name string, val interface{}) testFifoObject {
	return testFifoObject{name: name, val: val}
}

type testFifoObject struct {
	name string
	val  interface{}
}

func TestFifoQueue(t *testing.T) {
	f := NewFifoQueue()
	const amount = 500
	for i := 0; i < amount; i++ {
		f.Push(mkFifoObj(string([]rune{'a', rune(i)}), i+1))
	}

	for u := uint64(0); u < amount; u++ {
		f.Push(mkFifoObj(string([]rune{'b', rune(u)}), u+1))
	}

	if f.Len() != amount*2 {
		t.Error("fifo queue length error")
	}

	lastInt := 0
	lastUint := uint64(0)
	for i := 0; i < amount*2; i++ {
		obj, err := f.Pop()
		if err != nil {
			t.Error(err.Error())
			return
		}
		switch value := obj.(testFifoObject).val.(type) {
		case int:
			if value <= lastInt {
				t.Errorf("got %v (int) out of order, last was %v", obj, lastInt)
			}
			lastInt = value
		case uint64:
			if value <= lastUint {
				t.Errorf("got %v (uint) out of order, last was %v", obj, lastUint)
			} else {
				lastUint = value
			}
		default:
			t.Fatalf("unexpected type %#v", obj)
		}
	}

	if f.Len() != 0 {
		t.Error("fifo queue length 0 error")
	}
}

func TestFifoQueue_Front(t *testing.T) {
	f := NewFifoQueue()
	if _, err := f.Front(); err == nil {
		t.Error("failed to test front of empty queue")
	}
	if _, err := f.Pop(); err == nil {
		t.Error("failed to test pop of empty queue")
	}
	f.Push("")
	if _, err := f.Front(); err != nil {
		t.Errorf("failed to test front of queue: %s", err.Error())
	}
}
