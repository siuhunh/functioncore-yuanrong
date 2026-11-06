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

// Package timeutil provides some utils for time
package timeutil

import "time"

const (
	// NanosecondToMillisecond is the factor to convert nanosecond to millisecond,
	// it should be equal to time.Millisecond/time.Nanosecond
	NanosecondToMillisecond = 1000000
)

// UnixMillisecond convert time.Time to unix timestamp in millisecond
func UnixMillisecond(t time.Time) int64 {
	return t.UnixNano() / NanosecondToMillisecond
}

// NowUnixMillisecond get current unix timestamp in millisecond
func NowUnixMillisecond() int64 {
	return UnixMillisecond(time.Now())
}
