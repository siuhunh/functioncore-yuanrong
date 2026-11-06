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
	"container/list"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExecutor(t *testing.T) {
	e := NewExecutor(1*time.Millisecond, 16)
	var wg sync.WaitGroup

	wg.Add(1)
	type contextKey struct{}
	var key contextKey
	ctx := context.WithValue(context.Background(), key, struct{}{})
	e.Do(ctx, func(ctx context.Context) {
		assert.NotNil(t, ctx.Value(key))
		wg.Done()
	})
	wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := e.Async(ctx, func(ctx context.Context) {})
	assert.Equal(t, context.Canceled, err)

	var count int32
	ctx, cancel = context.WithCancel(context.Background())
	e.Every(ctx, 100*time.Millisecond, func(ctx context.Context) {
		atomic.AddInt32(&count, 1)
	})
	time.Sleep(380 * time.Millisecond)
	cancel()
	e.Close()
}

func TestExecutor_processTick(t *testing.T) {
	type fields struct {
		ring     *ringBuffer
		eventCh  chan *executorEvent
		stopCh   chan struct{}
		interval time.Duration
		key      executorContextKey
	}
	fn := func(context.Context) {}
	ll := list.New()
	ll.PushFront(&executorTask{
		ctx:    context.Background(),
		fn:     fn,
		circle: 1,
	})
	ll.PushFront(&executorTask{
		ctx:    context.Background(),
		fn:     fn,
		circle: 0,
	})
	var slots []*ringElem
	ringElem := &ringElem{
		val: ll,
	}
	slots = append(slots, ringElem)
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "case 01 normal scenario",
			fields: fields{
				ring:     &ringBuffer{
					slots:      slots,
					currentPos: 0,
				},
				eventCh:  nil,
				stopCh:   nil,
				interval: 5,
				key:      executorContextKey{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Executor{
				ring:     tt.fields.ring,
				eventCh:  tt.fields.eventCh,
				stopCh:   tt.fields.stopCh,
				interval: tt.fields.interval,
				key:      tt.fields.key,
			}
			e.processTick()
		})
	}
}