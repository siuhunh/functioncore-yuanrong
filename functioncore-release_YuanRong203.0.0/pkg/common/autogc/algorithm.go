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

package autogc

// Algorithm is algorithm for adjusting GOGC
type Algorithm interface {
	Init(totalMemory, threshold uint64)
	NextGOGC(currentMemory uint64, preGOGC int) int
}

const (
	defaultMaxGOGC   = 500
	defaultMinGCStep = 50 * MB
	percent          = 100
)

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// DefaultAlg defines default algorithm to adjust GOGC
// when current memory <= threshold, it will adjust GOGC to match threshold, but not above defaultMaxGOGC (500)
// when current memory > threshold, it will adjust GOGC so that GC will trigger every defaultMinGCStep (50MB) heap alloc
type DefaultAlg struct {
	total     uint64
	threshold uint64
	maxGOGC   int
}

// Init initializes alg with total memory and memory threshold
func (da *DefaultAlg) Init(total, threshold uint64) {
	da.total = total
	da.threshold = threshold
	da.maxGOGC = defaultMaxGOGC
}

// NextGOGC calculates appropriated GOGC with current memory and previous GOGC
func (da *DefaultAlg) NextGOGC(currentMemory uint64, preGOGC int) int {
	if da.threshold >= currentMemory+defaultMinGCStep {
		return min(da.maxGOGC, int(percent*(float64(da.threshold)/float64(currentMemory)-1.0)))
	}
	return int(percent * defaultMinGCStep / currentMemory)
}
