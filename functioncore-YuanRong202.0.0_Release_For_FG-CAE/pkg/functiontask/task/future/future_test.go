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

package future

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/common"
)

// TestGet checks filling the future.
func TestGet(t *testing.T) {
	Convey("create invoke future", t, func() {
		f := New()
		resp := &common.Response{
			Code: 999,
		}
		f.Set(resp)
		So(f.Get(), ShouldResemble, resp)
		So(f.GetUntil(time.Millisecond), ShouldResemble, resp)
	})
}

// TestGetUntil checks filling the future with timeout.
func TestGetUntil(t *testing.T) {
	Convey("create invoke future", t, func(c C) {
		f := New()
		resp := &common.Response{
			Code: 999,
		}
		f.Set(resp)
		So(f.Get(), ShouldResemble, resp)
		So(f.GetUntil(time.Millisecond), ShouldResemble, resp)
	})
}

// TestGetUntil1 checks filling the future with timeout.
func TestGetUntil1(t *testing.T) {
	Convey("create invoke future", t, func(c C) {
		f := New()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func(c C) {
			time.Sleep(time.Second)
			resp := &common.Response{
				Code:    0,
				Message: "123",
				Result:  "",
				StateID: "3225",
				State:   "",
			}
			f.Set(resp)
			c.So(f.Get(), ShouldResemble, resp)
			wg.Done()
		}(c)
		So(f.GetUntil(time.Millisecond), ShouldBeNil)
		wg.Wait()
	})
}

// TestGetUntil2 checks filling the future with timeout.
func TestGetUntil2(t *testing.T) {
	Convey("create invoke future", t, func(c C) {
		f := New()
		resp := &common.Response{
			Code:    0,
			Message: "143652",
			Result:  "",
			StateID: "365425",
			State:   "",
		}
		go func(c C) {
			time.Sleep(time.Millisecond)
			f.Set(resp)
			c.So(f.Get(), ShouldResemble, resp)
		}(c)
		So(f.GetUntil(time.Second), ShouldResemble, resp)
	})
}

// TestGetConcurrently checks filling the future with timeout.
func TestGetConcurrently(t *testing.T) {
	Convey("create invoke future", t, func(c C) {
		f := New()
		resp := &common.Response{
			Code: 999,
		}
		go func(c C) {
			time.Sleep(time.Second)
			f.Set(resp)
		}(c)
		ch := make(chan bool, 100)
		for i := 0; i < 50; i++ {
			go func(c C) {
				c.So(f.Get(), ShouldResemble, resp)
				ch <- true
			}(c)
		}
		for i := 0; i < 25; i++ {
			go func(c C) {
				c.So(f.GetUntil(time.Microsecond), ShouldBeNil)
				ch <- true
			}(c)
		}
		for i := 0; i < 25; i++ {
			go func(c C) {
				c.So(f.GetUntil(time.Second*2), ShouldResemble, resp)
				ch <- true
			}(c)
		}
		for i := 0; i < 100; i++ {
			<-ch
		}
		So(f.val, ShouldNotBeNil)
		So(f.Get(), ShouldResemble, resp)
	})
}

// TestClearConcurrently checks filling the future with timeout.
func TestClearConcurrently(t *testing.T) {
	Convey("create invoke future", t, func(c C) {
		cnt := 10000
		futureList := make([]*Future, cnt)
		resp := &common.Response{
			Code: 999,
		}
		for i := 0; i < cnt; i++ {
			futureList[i] = New()
			futureList[i].Set(resp)
		}
		wg := sync.WaitGroup{}
		for i := 0; i < cnt; i++ {
			wg.Add(1)
			go func(c C, i int) {
				time.Sleep(time.Duration(rand.Int()%5) * time.Second)
				futureList[i].Clear()
				wg.Done()
			}(c, i)
		}
		for i := 0; i < cnt; i++ {
			wg.Add(1)
			go func(c C, i int) {
				time.Sleep(time.Duration(rand.Int()%5) * time.Second)
				c.So(futureList[i].Get(), ShouldResemble, resp)
				wg.Done()
			}(c, i)
		}
		for i := 0; i < cnt; i++ {
			go func(c C, i int) {
				time.Sleep(5 * time.Second)
				futureList[i].Set(resp)
			}(c, i)
		}
		wg.Wait()
	})
}
