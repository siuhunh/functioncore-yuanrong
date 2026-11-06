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

package invokectx

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testCtx struct {
	a string
	b int
}

func (ctx *testCtx) Reset() { ctx.a = "" }

type testCtx2 struct {
	c string
}

func (ctx *testCtx2) Reset() { ctx.c = "" }

func TestInvokeCtx(t *testing.T) {
	metrics := NewMetrics()
	getter := metrics.Register(func() Poolable { return &testCtx{} })

	ic := metrics.AcquireInvokeCtx()
	ctx := getter(ic).(*testCtx)
	ctx.a = "my-awesome-a"
	ctx.b = 123

	// multi-get should get the same result
	secondGetCtx := getter(ic).(*testCtx)
	assert.Equal(t, secondGetCtx, ctx)
	assert.Equal(t, "my-awesome-a", secondGetCtx.a)
	assert.Equal(t, 123, secondGetCtx.b)

	retrivedIC, ok := metrics.GetInvokeCtx(ic.ID)
	assert.True(t, ok)
	retrivedCtx := getter(retrivedIC).(*testCtx)
	assert.Equal(t, ctx, retrivedCtx)
	assert.Equal(t, "my-awesome-a", retrivedCtx.a)
	assert.Equal(t, 123, retrivedCtx.b)

	metrics.ReleaseInvokeCtx(retrivedIC)

	_, ok = metrics.GetInvokeCtx(ic.ID)
	assert.False(t, ok)

	// *innerCtx is pooled. The address of ctx == ctx2. But ctx.a is reset.
	ic2 := metrics.AcquireInvokeCtx()
	ctx2 := getter(ic2).(*testCtx)
	assert.Equal(t, ctx, ctx2)
	assert.NotEqual(t, ic.ID, ic2.ID)
	assert.Equal(t, "", ctx2.a)
	assert.Equal(t, 123, ctx2.b)

	ic3 := metrics.AcquireInvokeCtx()
	ctx3 := getter(ic3).(*testCtx)
	assert.NotEqual(t, ctx, ctx3)
	assert.NotEqual(t, ic.ID, ic3.ID)
}

func TestMultipleCtx(t *testing.T) {
	metrics := NewMetrics()
	getter := metrics.Register(func() Poolable { return &testCtx{} })
	getter2 := metrics.Register(func() Poolable { return &testCtx2{} })

	ic := metrics.AcquireInvokeCtx()
	ctx := getter(ic).(*testCtx)
	ctx.a = "my-awesome-a"
	ctx.b = 123

	ctx2 := getter2(ic).(*testCtx2)
	ctx2.c = "my-awesome-c"

	retrivedIC, ok := metrics.GetInvokeCtx(ic.ID)
	assert.True(t, ok)
	retrivedCtx := getter(retrivedIC).(*testCtx)
	assert.Equal(t, ctx, retrivedCtx)

	retrivedCtx2 := getter2(retrivedIC).(*testCtx2)
	assert.Equal(t, ctx2, retrivedCtx2)
}

func TestNotify(t *testing.T) {
	metrics := NewMetrics()
	notifyCh := metrics.RegisterNotifier()
	assert.NotNil(t, notifyCh)

	ic := metrics.AcquireInvokeCtx()
	x := <-notifyCh
	assert.Equal(t, true, x)

	metrics.ReleaseInvokeCtx(ic)
	y := <-notifyCh
	assert.Equal(t, false, y)
}

func TestLen(t *testing.T) {
	metrics := NewMetrics()
	assert.Equal(t, 0, metrics.Len())

	ic := metrics.AcquireInvokeCtx()
	assert.Equal(t, 1, metrics.Len())

	ic2 := metrics.AcquireInvokeCtx()
	assert.Equal(t, 2, metrics.Len())

	metrics.ReleaseInvokeCtx(ic)
	assert.Equal(t, 1, metrics.Len())

	metrics.ReleaseInvokeCtx(ic2)
	assert.Equal(t, 0, metrics.Len())
}

func TestRange(t *testing.T) {
	metrics := NewMetrics()
	r := metrics.RegisterRange(func() Poolable { return &testCtx{} })
	r2 := metrics.RegisterRange(func() Poolable { return &testCtx2{} })

	ic := metrics.AcquireInvokeCtx()

	n := 0
	r.Range(func(ic InvokeCtx, p Poolable) bool {
		n++
		return true
	})
	assert.Equal(t, 0, n)
	assert.Equal(t, 0, r.Len())

	ctx := r.Mark(ic).(*testCtx)
	ctx.a = "my-awesome-a"
	ctx.b = 123

	n = 0
	r.Range(func(ic InvokeCtx, p Poolable) bool {
		rangedCtx := p.(*testCtx)
		assert.Equal(t, ctx, rangedCtx)
		assert.Equal(t, "my-awesome-a", rangedCtx.a)
		assert.Equal(t, 123, rangedCtx.b)

		retrivedRangedCtx := r.Mark(ic).(*testCtx)
		assert.Equal(t, ctx, retrivedRangedCtx)
		n++
		return true
	})
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, r.Len())

	// acquire 3 testCtx2
	ctx2 := r2.Mark(ic).(*testCtx2)
	ctx2.c = "my-awesome-c"

	n = 0
	r2.Range(func(ic InvokeCtx, p Poolable) bool {
		rangedCtx := p.(*testCtx2)
		assert.Equal(t, ctx2, rangedCtx)
		assert.Equal(t, "my-awesome-c", rangedCtx.c)

		retrivedRangedCtx := r2.Mark(ic).(*testCtx2)
		assert.Equal(t, ctx2, retrivedRangedCtx)
		n++
		return true
	})
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, r2.Len())

	ic2 := metrics.AcquireInvokeCtx()
	ctx3 := r2.Mark(ic2).(*testCtx2)
	ctx3.c = "second c"

	ids := []uint64{}
	r2.Range(func(ic InvokeCtx, p Poolable) bool {
		ids = append(ids, ic.ID)
		return true
	})
	assert.Equal(t, 2, len(ids))
	assert.NoError(t, contains(ids, ic.ID))
	assert.NoError(t, contains(ids, ic2.ID))

	ic3 := metrics.AcquireInvokeCtx()
	ctx4 := r2.Mark(ic3).(*testCtx2)
	ctx4.c = "another c"

	ids = []uint64{}
	r2.Range(func(ic InvokeCtx, p Poolable) bool {
		ids = append(ids, ic.ID)
		return true
	})
	assert.Equal(t, 3, len(ids))
	assert.NoError(t, contains(ids, ic.ID))
	assert.NoError(t, contains(ids, ic2.ID))
	assert.NoError(t, contains(ids, ic3.ID))

	// release
	r2.Unmark(ic2)

	ids = []uint64{}
	r2.Range(func(ic InvokeCtx, p Poolable) bool {
		ids = append(ids, ic.ID)
		return true
	})
	assert.Equal(t, 2, len(ids))
	assert.NoError(t, contains(ids, ic.ID))
	assert.NoError(t, contains(ids, ic3.ID))

	// r should not be affected
	n = 0
	r.Range(func(ic InvokeCtx, p Poolable) bool {
		rangedCtx := p.(*testCtx)
		assert.Equal(t, ctx, rangedCtx)
		assert.Equal(t, "my-awesome-a", rangedCtx.a)
		assert.Equal(t, 123, rangedCtx.b)

		retrivedRangedCtx := r.Mark(ic).(*testCtx)
		assert.Equal(t, ctx, retrivedRangedCtx)
		n++
		return true
	})
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, r.Len())

	// release ic should automatically release ctx and ctx2
	metrics.ReleaseInvokeCtx(ic)

	ids = []uint64{}
	r.Range(func(ic InvokeCtx, p Poolable) bool {
		ids = append(ids, ic.ID)
		return true
	})
	assert.Equal(t, 0, len(ids))

	ids = []uint64{}
	r2.Range(func(ic InvokeCtx, p Poolable) bool {
		ids = append(ids, ic.ID)
		return true
	})
	assert.Equal(t, 1, len(ids))
	assert.NoError(t, contains(ids, ic3.ID))
}

func contains(ids []uint64, id uint64) error {
	for _, x := range ids {
		if id == x {
			return nil
		}
	}
	return fmt.Errorf("%v does not contains %d", ids, id)
}
