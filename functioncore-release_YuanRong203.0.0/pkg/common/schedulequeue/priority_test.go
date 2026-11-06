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

package schedulequeue

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_priorityScheduleQueue_sorted(t *testing.T) {
	queue := NewPriorityScheduleQueue("testqueue", func(x, y interface{}) bool {
		return x.(int) < y.(int)
	}, make(chan struct{}))

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	n := 100

	excepted := make([]int, 0, n)
	actual := make([]int, 0, n)

	for i := 0; i < n; i++ {
		r := rnd.Intn(10000)
		excepted = append(excepted, r)
		queue.Add(r)
	}

	inner, ok := queue.(*priorityScheduleQueue)
	assert.Equal(t, true, ok, "queue type mismatched!")

	for inner.queue.Len() > 0 {
		actual = append(actual, inner.pop().(*Item).Val.(int))
	}

	sort.Ints(excepted)

	assert.Equal(t, n, len(actual), "queue length mismatched!")
	assert.Equal(t, excepted, actual, "item in priorityScheduleQueue should be sorted")
}

func Test_priorityScheduleQueue_ScheduleTimeout(t *testing.T) {
	queue := NewPriorityScheduleQueue("testqueue", func(x, y interface{}) bool {
		return x.(int) < y.(int)
	}, make(chan struct{}))

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	n := 100

	wg := sync.WaitGroup{}
	wg.Add(n)

	for i := 0; i < n; i++ {
		go func() {
			item := queue.Add(rnd.Intn(10000))
			ctx, _ := context.WithTimeout(context.TODO(), 1*time.Millisecond)
			err := item.Wait(ctx)
			assert.Error(t, err, "should trigger timeout error")
			wg.Done()
		}()
	}

	// to trigger timeout
	time.Sleep(100 * time.Millisecond)
	go queue.Start()

	wg.Wait()
}

func Test_priorityScheduleQueue_ScheduleWithPriority(t *testing.T) {
	queue := NewPriorityScheduleQueue("testqueue", func(x, y interface{}) bool {
		return x.(int) < y.(int)
	}, make(chan struct{}))

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	n := 100

	wg := sync.WaitGroup{}
	wg.Add(n)

	excepted := make([]int, 0, n)
	actual := make([]int, 0, n)

	for i := 0; i < n; i++ {
		r := rnd.Intn(10000)
		excepted = append(excepted, r)
	}

	for i := 0; i < n; i++ {
		go func(i int) {
			item := queue.Add(excepted[i])
			// no timeout
			err := item.Wait(context.TODO())
			assert.Nil(t, err, "schedule should success")
			// note: here append is thread safe because schedule queue is serial waiting for queue.Done
			actual = append(actual, excepted[i])
			queue.Done(item)
			wg.Done()
		}(i)
	}

	time.Sleep(100 * time.Millisecond)
	go queue.Start()

	wg.Wait()

	sort.Ints(excepted)
	assert.Equal(t, excepted, actual, "schedule should in priority")
}

func Test_priorityScheduleQueue_ScheduleWithPriority_Atomic(t *testing.T) {
	queue := NewPriorityScheduleQueue("testqueue", func(x, y interface{}) bool {
		return x.(int) < y.(int)
	}, make(chan struct{}))

	n := 100

	wg := sync.WaitGroup{}
	wg.Add(n)

	go queue.Start()

	count := uint32(0)

	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			item := queue.Add(i)
			timeout := 10000 * time.Millisecond
			if i%3 == 0 {
				timeout = 3 * time.Millisecond
			}
			ctx, _ := context.WithTimeout(context.TODO(), timeout)
			err := item.Wait(ctx)
			if err != nil {
				return
			}
			// if move defer queue.Done(item) before error check, the test will fail
			defer queue.Done(item)

			// assert only one item can access operation below
			assert.Equal(t, uint32(1), atomic.AddUint32(&count, 1), "count should be 1")
			time.Sleep(5 * time.Millisecond)
			atomic.AddUint32(&count, ^uint32(0))
		}(i)
	}

	wg.Wait()
}

func Test_priorityScheduleQueue_StopAndClear(t *testing.T) {
	stopCh := make(chan struct{})
	queue := NewPriorityScheduleQueue("testqueue", func(x, y interface{}) bool {
		return x.(int) < y.(int)
	}, stopCh)

	n := 100

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))

	wg := sync.WaitGroup{}
	wg.Add(n)

	mux := sync.Mutex{}

	excepted := make([]int, 0, n)
	actual := make([]int, 0, n)

	for i := 0; i < n; i++ {
		r := rnd.Intn(10000)
		excepted = append(excepted, r)
	}

	for i := 0; i < n; i++ {
		go func(i int) {
			item := queue.Add(excepted[i])
			// no timeout
			err := item.Wait(context.TODO())
			assert.Nil(t, err, "schedule should success")
			// here append is not thread safe, because queue is closed and will clear all item in queue
			mux.Lock()
			actual = append(actual, excepted[i])
			mux.Unlock()
			queue.Done(item)
			wg.Done()
		}(i)
	}
	close(stopCh)
	queue.Start()

	wg.Wait()
	sort.Ints(excepted)
	sort.Ints(actual)
	assert.Equal(t, excepted, actual, "item in queue can still be scheduled after stop")

	wg.Add(n)

	excepted = make([]int, 0, n)
	actual = make([]int, 0, n)

	for i := 0; i < n; i++ {
		r := rnd.Intn(10000)
		excepted = append(excepted, r)
	}

	for i := 0; i < n; i++ {
		go func(i int) {
			item := queue.Add(excepted[i])
			// no timeout
			err := item.Wait(context.TODO())
			assert.Nil(t, err, "schedule should success")
			mux.Lock()
			actual = append(actual, excepted[i])
			mux.Unlock()
			queue.Done(item)
			wg.Done()
		}(i)
	}

	wg.Wait()
	sort.Ints(excepted)
	assert.NotEqual(t, excepted, actual, "schedule will not in priority")
	sort.Ints(actual)
	assert.Equal(t, excepted, actual, "item can still be add to queue")

}

func Test_priorityScheduleQueue_NoDeadLock(t *testing.T) {
	stopCh := make(chan struct{})
	queue := NewPriorityScheduleQueue("testqueue", func(x, y interface{}) bool {
		return x.(int) < y.(int)
	}, stopCh)
	queue.Start()

	n := 100
	wg := sync.WaitGroup{}
	wg.Add(n)

	// try to construct a scene that context is time out and at the same time item is scheduled by queue
	for i := 0; i < n; i++ {
		go func(i int) {
			item := queue.Add(i)
			// no timeout
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := item.Wait(ctx)
			if err != nil {
				return
			}
			defer queue.Done(item)
		}(i * 2)
		func(i int) {
			item := queue.Add(i)
			// no timeout
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := item.Wait(ctx)
			if err != nil {
				return
			}
			defer queue.Done(item)
		}(i*2 + 1)
		wg.Done()
	}
	wg.Wait()

	item := queue.Add(n)
	// assert there is no deadlock error!
	item.Wait(context.TODO())
}
