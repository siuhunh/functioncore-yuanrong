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

// Package queue -
package queue

import "errors"

// ErrQueueIsEmpty -
var ErrQueueIsEmpty = errors.New("empty queue")

var errAssertionFailed = errors.New("assertion failed")

// Queue is interface and imply fifo queue and priority queue.
type Queue interface {
	Push(obj interface{}) error
	Pop() (interface{}, error)
	Front() (interface{}, error)
	Len() int
}
