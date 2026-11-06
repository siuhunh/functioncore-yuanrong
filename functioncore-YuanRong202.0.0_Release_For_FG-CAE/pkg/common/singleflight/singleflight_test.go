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
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

// TestSingleFlight_Do -
func TestSingleFlight_Do(t *testing.T) {
	// simulate 10 concurrent requests and query the key 'test' from the database,
	// it is expected that the database will be accessed only once
	convey.Convey("test: single flight do", t, func() {
		count := 0
		sf := NewSingleFlight()
		concurrentNum := 10
		var wg sync.WaitGroup
		wg.Add(concurrentNum)
		for i := 0; i < concurrentNum; i++ {
			go func() {
				defer wg.Done()
				sf.Do("test", func() (interface{}, error) {
					count++
					return nil, nil
				})
			}()
		}
		wg.Wait()
		convey.So(count, convey.ShouldEqual, 1)
	})
}

// TestSingleFlight -
func TestNewSingleFlight_Remove(t *testing.T) {
	convey.Convey("test: single flight remove", t, func() {
		count := 0
		concurrentNum := 10
		sf := NewSingleFlight()
		key := "test"
		concurrentTestFunc := func() {
			var wg sync.WaitGroup
			wg.Add(concurrentNum)
			for i := 0; i < concurrentNum; i++ {
				go func() {
					defer wg.Done()
					sf.Do(key, func() (interface{}, error) {
						count++
						return nil, nil
					})
				}()
			}
			wg.Wait()
		}
		concurrentTestFunc()
		convey.So(count, convey.ShouldEqual, 1)
		sf.Remove(key)
		concurrentTestFunc()
		convey.So(count, convey.ShouldEqual, 2)
	})
}
