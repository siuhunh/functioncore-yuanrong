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
	"sync/atomic"
)

// Item wrappers actual value for schedule
type Item struct {
	Val       interface{}
	isTimeout uint32
	waitDone  chan struct{}
	scheduled chan struct{}
}

// Wait blocks until item is scheduled successfully or timed out waiting for schedule
// usage:
//  `item := queue.Add(something)
//  `err := item.Wait(ctx)
//  `if err != nil {}
//  `defer queue.Done(item)
// see test Test_priorityScheduleQueue_ScheduleWithPriority_Atomic
func (i *Item) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		atomic.StoreUint32(&i.isTimeout, 1)
		// NOTE: here need to notify schedule queue that isTimeout has been changed
		// otherwise, schedule queue may load isTimeout before store, and result in queue deadlock
		// see Test_priorityScheduleQueue_NoDeadLock
		close(i.waitDone)
		return ErrScheduleTimeout
	case <-i.scheduled:
		close(i.waitDone)
		return nil
	}
}

// signal unblocks Item.Wait
// should only be called once when
// - item is popped from queue
// - item is added to a closed queue
func (i *Item) signal() {
	close(i.scheduled)
}
