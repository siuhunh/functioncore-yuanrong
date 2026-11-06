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

package fusion

import "time"

// limiter's add function returns true if it is called more than "slot" times within "duration".
// limiter adds rate limiting capability to runtime restart logic. It prevents runtime from crashing repeatedly
// at a super fast rate.
type limiter struct {
	ts       []time.Time
	duration time.Duration
}

func newLimiter(slot int, duration time.Duration) *limiter {
	if slot <= 0 {
		return nil
	}

	return &limiter{
		ts:       make([]time.Time, slot),
		duration: duration,
	}
}

// add returns true if limit is hit
func (l *limiter) add() bool {
	last := l.ts[len(l.ts)-1]
	if !last.IsZero() {
		shouldLimit := false
		if last.Sub(l.ts[0]) <= l.duration {
			shouldLimit = true
		}
		l.ts = append(l.ts[1:], make([]time.Time, 1)...)
		l.ts[len(l.ts)-1] = time.Now()
		return shouldLimit
	}
	for i := 0; i < len(l.ts); i++ {
		if l.ts[i].IsZero() {
			l.ts[i] = time.Now()
			break
		}
	}
	return false
}
