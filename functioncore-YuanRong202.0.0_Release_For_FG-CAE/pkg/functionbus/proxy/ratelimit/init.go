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

package ratelimit

import (
	"os"
	"strconv"
)

var (
	traceIDBucketSize int
	traceIDFrequency  float64
)

const (
	// HLT has case that invoke function 1000 times.
	// Modify the invokeLimit entry in configmap if you want to chang the limit
	defaultInvokeBucketSize = 1000
	defaultInvokeRate       = 1 // token refresh rate, 1 token per second
	minInvokeRate           = 0.01
)

// InitLimitConfig init config for rate limit
func InitLimitConfig() {
	traceIDBucketSize = defaultInvokeBucketSize
	traceIDFrequency = defaultInvokeRate
	if bs, err := strconv.Atoi(os.Getenv("INVOKE_BUCKET_SIZE")); err == nil && bs > 0 {
		traceIDBucketSize = bs
	}
	if fr, err := strconv.ParseFloat(os.Getenv("INVOKE_RATE"), 64); err == nil && fr > 0.00001 {
		traceIDFrequency = fr
	}
}
