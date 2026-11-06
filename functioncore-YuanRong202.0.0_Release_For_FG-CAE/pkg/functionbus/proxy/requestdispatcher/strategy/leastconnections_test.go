/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package strategy -
package strategy

import (
	"math"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

type fakeScheduleEntry struct {
	workerID        string
	currentRequests int
	concurrency     int
	isLocalWorker   bool
	isMinInstance   bool
	priority        int
	nodeIP          string
	willDelete      bool
}

func TestLeastConnections(t *testing.T) {
	convey.Convey("TestLeastConnections", t, func() {
		var arrary []fakeScheduleEntry
		enrtyWorker1 := fakeScheduleEntry{
			workerID:        "worker1",
			currentRequests: 0,
			concurrency:     100,
		}
		arrary = append(arrary, enrtyWorker1)
		index := LeastConnections(arrary, nil, connNums, defaultCheckFunction)
		convey.So(arrary[index].workerID, convey.ShouldEqual, enrtyWorker1.workerID)

		enrtyWorker2 := fakeScheduleEntry{
			workerID:        "worker2",
			currentRequests: 1,
			concurrency:     100,
		}
		arrary = append(arrary, enrtyWorker2)
		index = LeastConnections(arrary, nil, connNums, defaultCheckFunction)
		convey.So(arrary[index].workerID, convey.ShouldEqual, enrtyWorker1.workerID)

		index = LeastConnections(arrary, map[string]struct{}{enrtyWorker1.workerID: {}}, connNums, defaultCheckFunction)
		convey.So(arrary[index].workerID, convey.ShouldEqual, enrtyWorker2.workerID)

		enrtyWorker3 := fakeScheduleEntry{
			workerID:        "worker1",
			currentRequests: 0,
			concurrency:     -1,
		}
		var wrongArrary []fakeScheduleEntry
		wrongArrary = append(wrongArrary, enrtyWorker3)
		index = LeastConnections(wrongArrary, nil, connNums, defaultCheckFunction)
		convey.So(index, convey.ShouldEqual, -1)

		var emptyArrary []fakeScheduleEntry
		index = LeastConnections(emptyArrary, nil, connNums, defaultCheckFunction)
		convey.So(index, convey.ShouldEqual, -1)
	})
}

func defaultCheckFunction(entry fakeScheduleEntry, used map[string]struct{}) bool {
	if _, ok := used[entry.workerID]; ok {
		return false
	}
	if entry.concurrency-entry.currentRequests > 0 {
		return true
	}
	return false
}

func connNums(entry fakeScheduleEntry, used map[string]struct{}) int {
	if _, ok := used[entry.workerID]; ok {
		return math.MaxInt
	}
	return entry.currentRequests
}
