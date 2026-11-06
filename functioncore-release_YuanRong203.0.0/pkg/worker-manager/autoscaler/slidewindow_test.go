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

package autoscaler

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/workermanager"
)

func TestRingBuffer(t *testing.T) {
	rb := newRingBuffer(2)
	elem := rb.get(1)
	elem.val = 1
	elem = rb.get(2)
	elem.val = 2

	elem = rb.get(3)
	assert.Equal(t, 1, elem.val)
	rb.rangeAll(func(offset int, elem *ringElem) {
		if offset == 0 {
			assert.Equal(t, 2, elem.val)
		} else if offset == 1 {
			assert.Equal(t, 1, elem.val)
		} else {
			t.Fail()
		}
	})

	rb.advance()

	elem = rb.get(1)
	assert.Equal(t, 2, elem.val)
	elem = rb.get(2)
	assert.Equal(t, 1, elem.val)
	rb.rangeAll(func(offset int, elem *ringElem) {
		if offset == 0 {
			assert.Equal(t, 1, elem.val)
		} else if offset == 1 {
			assert.Equal(t, 2, elem.val)
		} else {
			t.Fail()
		}
	})

	rb.advance()

	rb.rangeAll(func(offset int, elem *ringElem) {
		if offset == 0 {
			assert.Equal(t, 2, elem.val)
		} else if offset == 1 {
			assert.Equal(t, 1, elem.val)
		} else {
			t.Fail()
		}
	})
}

func TestSlideWindowRangeVisit(t *testing.T) {
	convey.Convey("TestSlideWindowRangeVisit async johnson", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		count := 0
		ch := make(chan interface{})
		swr := &slideWindowRange{
			rangeFn: func(offset int, instanceID string, list []workermanager.ReportMetrics) {
				count++
				close(ch)
			},
		}
		ringElems := make([]*ringElem, 0)
		ringElems = append(ringElems, &ringElem{val: slideWindowValue{"key": make([]workermanager.ReportMetrics, 0)}})
		sw := &slideWindow{
			executor: DefaultExecutor,
			ring: &ringBuffer{
				slots: ringElems,
			},
		}
		//swr.visit(sw)
		_ = sw.async(context.Background(), swr)
		select {
		case <-ch:
		}
		convey.So(count, convey.ShouldEqual, 1)
	})
	convey.Convey("TestSlideWindowRangeVisit do johnson", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		count := 0
		ch := make(chan interface{})
		swr := &slideWindowRange{
			rangeFn: func(offset int, instanceID string, list []workermanager.ReportMetrics) {
				count++
				close(ch)
			},
		}
		swa := &slideWindowAdd{instanceID: "key1"}
		ringElems := make([]*ringElem, 0)
		ringElems = append(ringElems, &ringElem{val: slideWindowValue{"key": make([]workermanager.ReportMetrics, 0)}})
		sw := newSlideWindow(DefaultExecutor, time.Microsecond, 1)
		//swr.visit(sw)
		_ = sw.do(context.Background(), swa, swr)
		select {
		case <-ch:
		}
		convey.So(count, convey.ShouldEqual, 1)
	})
}

func TestSlideWindowDeleteVisit(t *testing.T) {
	convey.Convey("TestSlideWindowDeleteVisit johnson", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		swr := &slideWindowDelete{key: "key"}
		ringElems := make([]*ringElem, 0)
		ringElems = append(ringElems, &ringElem{val: slideWindowValue{"key": make([]workermanager.ReportMetrics, 0)}})
		sw := &slideWindow{
			ring: &ringBuffer{
				slots: ringElems,
			},
		}
		swr.visit(sw)
		elem := ringElems[0]
		swv, _ := elem.val.(slideWindowValue)
		convey.So(len(swv), convey.ShouldEqual, 0)
	})
}

func TestSlideWindowAddVisit(t *testing.T) {
	convey.Convey("TestSlideWindowAddVisit johnson", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		swr := &slideWindowAdd{instanceID: "key1"}
		ringElems := make([]*ringElem, 0)
		ringElems = append(ringElems, &ringElem{val: slideWindowValue{"key": make([]workermanager.ReportMetrics, 0)}})
		ringElems = append(ringElems, &ringElem{val: slideWindowValue{"key1": make([]workermanager.ReportMetrics, 0)}})
		sw := &slideWindow{
			ring: &ringBuffer{
				slots: ringElems,
			},
			interval: defaultExecutorInterval,
		}
		swr.visit(sw)
		convey.So(len(ringElems), convey.ShouldEqual, 2)
	})
}
