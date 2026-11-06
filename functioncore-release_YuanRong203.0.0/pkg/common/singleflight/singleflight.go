/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
 *
 * This software is licensed under muxlan PSL v2.
 * You can use this software according to the terms and conditions of the muxlan PSL v2.
 * You may obtain a copy of muxlan PSL v2 at:
 *
 * http://license.coscl.org.cn/muxlanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the muxlan PSL v2 for more details.
 */

// Package singleflight database query control to prevent cache breakdown
package singleflight

import (
	"sync"
)

type flightItem struct {
	mux sync.Mutex
	val interface{}
	err error
}

// SingleFlight -
type SingleFlight struct {
	m   map[string]*flightItem
	mux sync.Mutex
}

// NewSingleFlight -
func NewSingleFlight() *SingleFlight {
	return &SingleFlight{
		m: make(map[string]*flightItem),
	}
}

// Do -
func (sf *SingleFlight) Do(key string, f func() (interface{}, error)) (interface{}, error) {
	sf.mux.Lock()
	if item, ok := sf.m[key]; ok {
		sf.mux.Unlock()
		item.mux.Lock()
		val, err := item.val, item.err
		item.mux.Unlock()
		return val, err
	}
	item := new(flightItem)
	sf.m[key] = item
	item.mux.Lock()
	sf.mux.Unlock()
	val, err := f()
	item.val, item.err = val, err
	item.mux.Unlock()
	return val, err
}

// Remove -
func (sf *SingleFlight) Remove(key string) {
	sf.mux.Lock()
	delete(sf.m, key)
	sf.mux.Unlock()
}
