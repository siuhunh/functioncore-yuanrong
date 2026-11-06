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
package utils

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestLru(t *testing.T) {
	var cacheMap sync.Map
	var lru = NewLru(&cacheMap)
	lru.cacheMemMax = 20
	convey.Convey("Lru", t, func() {
		convey.Convey("case 1", func() {
			lru.AddCache("123", "aaa", 3)
			lru.AddCache("124", "aaaa", 4)
			lru.AddCache("125", "aaaaa", 5)
			lru.AddCache("126", "aaaaaa", 6)
			_, _ = lru.GetCache("124", false)
			convey.So(lru.list.Len(), convey.ShouldEqual, 4)
			convey.So(lru.list.Front().Value.(ListNode).key, convey.ShouldEqual, "124")
		})
		convey.Convey("case 2", func() {
			lru.AddCache("127", "aaaaaaaaaaaaaaa", 14)
			convey.So(lru.list.Len(), convey.ShouldEqual, 2)
			convey.So(lru.list.Front().Value.(ListNode).key, convey.ShouldEqual, "127")
		})
		convey.Convey("case 3", func() {
			lru.DelCache("127")
			convey.So(lru.list.Len(), convey.ShouldEqual, 1)
			convey.So(lru.list.Front().Value.(ListNode).key, convey.ShouldEqual, "124")
			convey.So(lru.codeSize, convey.ShouldEqual, 4)
		})
	})
}

func TestLruNil(t *testing.T) {
	var cacheMap sync.Map
	lru := NewLru(&cacheMap)
	convey.Convey("LruNil", t, func() {
		convey.Convey("case 1", func() {
			lru.list = nil
			lru.cache = nil
			lru.AddCache("123", "aaa", 3)
			_, _ = lru.GetCache("123", false)
			lru.DelCache("123")
			convey.So(lru.cache, convey.ShouldBeNil)
			convey.So(lru.list, convey.ShouldBeNil)
		})
	})
}

func TestNewLru(t *testing.T) {
	var cacheMap sync.Map
	convey.Convey("NewLru", t, func() {
		convey.Convey("case 1", func() {
			defer gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
				return nil, errors.New("123")
			}).Reset()
			lru := NewLru(&cacheMap)
			convey.So(lru.cacheMemMax, convey.ShouldEqual, int64(defaultCacheMemMax))
		})
	})
}

func TestLRU_AddRepeat(t *testing.T) {
	m := sync.Map{}
	lru := NewLru(&m)

	key := "123"

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 100; i++ {
		randValue := strconv.Itoa(rnd.Intn(100000))
		randSize := rnd.Int63n(100)

		lru.AddCache(key, randValue, randSize)

		assert.Equal(t, randSize, lru.codeSize)
		v, ok := lru.GetCache(key, false)
		assert.Equal(t, true, ok)
		assert.Equal(t, randValue, v.(string))

		count := 0
		m.Range(func(key, value interface{}) bool {
			count++
			return true
		})
		assert.Equal(t, 1, count, "map should only have one value")
		assert.Equal(t, 1, lru.list.Len(), "list should only have one value")
	}

}

func TestParseMaxUsePercent(t *testing.T) {
	convey.Convey("ParseMaxUsePercent", t, func() {
		convey.Convey("test error", func() {
			p := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return "123.a3"
			})
			defer p.Reset()
			res := parseMaxUsePercent()
			convey.ShouldEqual(res, defaultMaxUsePercent)
		})
		convey.Convey("test success", func() {
			exceptPercent := 0.3
			p := gomonkey.ApplyFunc(os.Getenv, func(key string) string {
				return fmt.Sprintf("%f", exceptPercent)
			})
			defer p.Reset()
			res := parseMaxUsePercent()
			convey.ShouldEqual(res, exceptPercent)
		})
	})
}
