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

// Package futurestore implements the future store.
package futurestore

import (
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

// TestNew checks creating a future store.
func TestNew(t *testing.T) {
	Convey("create future store", t, func() {
		store := New()
		So(store, ShouldNotBeNil)
		Convey("get future", func() {
			So(store.Get("xxx"), ShouldBeNil)
			Convey("get future", func() {
				f := snfuture.NewExternalEvent("t")
				store.Add(f)
				Convey("get future", func() {
					So(store.Get(f.ID), ShouldEqual, f)
				})
			})
		})
	})
}

// TestRecycle checks recycle futures.
func TestRecycle(t *testing.T) {
	Convey("create future store", t, func() {
		store := New()
		So(store, ShouldNotBeNil)
		Convey("get future", func() {
			So(store.Get("xxx"), ShouldBeNil)
			cnt := 100000
			for i := 0; i < cnt; i++ {
				f := snfuture.NewExternalEvent(strconv.Itoa(i))
				store.Add(f)
				go func() {
					time.Sleep(time.Second)
					store.Delete(f.ID)
				}()
			}
			time.Sleep(3 * time.Second)
			So(len(store.futureStore), ShouldEqual, 0)
		})
	})
}

// TestRecycleTwice checks recycle futures.
func TestRecycleTwice(t *testing.T) {
	Convey("create future store", t, func() {
		store := New()
		So(store, ShouldNotBeNil)
		Convey("get future", func() {
			So(store.Get("xxx"), ShouldBeNil)
			cnt := 10
			for i := 0; i < cnt; i++ {
				f := snfuture.NewExternalEvent(strconv.Itoa(i))
				store.Add(f)
				go func() {
					time.Sleep(time.Second)
					store.Delete(f.ID)
				}()
			}
			time.Sleep(3 * time.Second)
			for i := 0; i < cnt; i++ {
				f := snfuture.NewExternalEvent(strconv.Itoa(i))
				store.Add(f)
				go func() {
					time.Sleep(time.Second)
					store.Delete(f.ID)
				}()
			}
			time.Sleep(3 * time.Second)
			So(len(store.futureStore), ShouldEqual, 0)
		})
	})
}

// TestConcurrentMapWrites checks concurrent map writes.
func TestConcurrentMapWrites(t *testing.T) {
	Convey("create future store", t, func() {
		store := New()
		So(store, ShouldNotBeNil)
		Convey("get future", func() {
			So(store.Get("xxx"), ShouldBeNil)
			cnt := 100000
			ch := make(chan struct{}, cnt)
			for i := 0; i < cnt; i++ {
				go func(id int) {
					store.Add(snfuture.NewExternalEvent(strconv.Itoa(id)))
					ch <- struct{}{}
				}(i)
			}
			for i := 0; i < cnt; i++ {
				<-ch
			}
			So(1, ShouldEqual, 1)
		})
	})
}

// TestAddRefCountConcurrently checks add reference count concurrently.
func TestAddRefCountConcurrently(t *testing.T) {
	store := New()
	times := 1000000
	Convey("reference counter must equal with 1000000", t, func() {
		So(store, ShouldNotBeNil)
		So(store.Get("xxx"), ShouldBeNil)
		fut := snfuture.NewExternalEvent("Test")
		id := fut.ID
		store.Add(fut)
		wg := sync.WaitGroup{}
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func() {
				store.AddRefCount(id)
				wg.Done()
			}()
		}
		wg.Wait()
		So(store.AddRefCount(id)-2, ShouldEqual, times)
	})
}

// TestDeleteConcurrently checks delete future concurrently.
func TestDeleteConcurrently(t *testing.T) {
	store := New()
	times := 1000000
	Convey("reference counter must equal with 0", t, func() {
		So(store, ShouldNotBeNil)
		So(store.Get("xxx"), ShouldBeNil)
		fut := snfuture.NewExternalEvent("Test")
		id := fut.ID
		store.Add(fut)
		wg := sync.WaitGroup{}
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func() {
				store.AddRefCount(id)
				wg.Done()
			}()
		}
		wg.Wait()
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func() {
				store.Delete(id)
				wg.Done()
			}()
		}
		wg.Wait()
		So(store.Get(id), ShouldNotBeNil)
		store.Delete(id)
		So(len(store.futureStore), ShouldEqual, 0)
	})
}

// TestDetail checks get detail of future store.
func TestDetail(t *testing.T) {
	num := 10
	Convey("get detail of future store", t, func() {
		pre := len(Detail())
		for i := 0; i < num; i++ {
			store := New()
			So(store, ShouldNotBeNil)
			//futures:=
			for j := 0; j < num; j++ {
				fut := snfuture.NewExternalEvent("test" + strconv.Itoa(i) + strconv.Itoa(j))
				store.Add(fut)
			}
		}
		detail := Detail()
		So(len(detail)-pre, ShouldEqual, num*num)
	})
}

// TestSetIfNotExist checks SetIfNotExist.
func TestSetIfNotExist(t *testing.T) {
	Convey("test SetIfNotExist", t, func() {
		store := New()
		num := 10
		So(store, ShouldNotBeNil)
		for i := 0; i < num; i++ {
			fut := snfuture.NewExternalEvent("test" + strconv.Itoa(i))
			store.SetIfNotExist(fut)
		}
		for i := 0; i < num; i++ {
			fut := snfuture.NewExternalEvent("test" + strconv.Itoa(i))
			store.SetIfNotExist(fut)
		}
		So(len(store.futureStore), ShouldEqual, num)
	})
}

func TestFutureStore_deleteFromStore(t *testing.T) {
	type fields struct {
		mutex       *sync.RWMutex
		futureStore map[snfuture.ID]*snfuture.Future
	}
	type args struct {
		future *snfuture.Future
	}
	var f fields
	var a args
	mutex := new(sync.RWMutex)
	f.mutex = mutex
	futureStore := map[snfuture.ID]*snfuture.Future{}
	future := &snfuture.Future{}
	futureStore["aaa"] = future
	future.ID = "aaa"
	a.future = future
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &FutureStore{
				mutex:       tt.fields.mutex,
				futureStore: tt.fields.futureStore,
			}
			s.deleteFromStore(a.future)
		})
	}
}

// TestFutureStore_GetTTLFuture -
func TestFutureStore_GetTTLFuture(t *testing.T) {
	Convey("test get ttl future", t, func() {
		s := &FutureStore{
			futureStore: map[snfuture.ID]*snfuture.Future{},
			mutex:       new(sync.RWMutex),
		}
		config.Conf.FutureTTL = 0
		s.Add(&snfuture.Future{
			SetTime:  time.Now(),
			Metadata: snfuture.Metadata{ID: "test"},
		})
		futures := s.GetTTLFuture()
		So(len(futures), ShouldEqual, 1)
		So(futures[0], ShouldEqual, "test")
		s.ClearFuturesByID(futures)
		futures = s.GetTTLFuture()
		So(len(futures), ShouldEqual, 0)
	})
}
