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
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestRoundRobin(t *testing.T) {
	convey.Convey("TestLeastConnections", t, func() {
		roundRobinIndex := 0
		var arrary []fakeScheduleEntry
		enrtyWorker1 := fakeScheduleEntry{
			workerID:        "worker1",
			currentRequests: 0,
			concurrency:     100,
		}
		arrary = append(arrary, enrtyWorker1)
		roundRobinIndex = RoundRobin(roundRobinIndex, arrary, nil, defaultCheckFunction)
		convey.So(arrary[roundRobinIndex].workerID, convey.ShouldEqual, enrtyWorker1.workerID)

		enrtyWorker2 := fakeScheduleEntry{
			workerID:        "worker2",
			currentRequests: 1,
			concurrency:     100,
		}
		arrary = append(arrary, enrtyWorker2)
		roundRobinIndex = RoundRobin(roundRobinIndex, arrary, nil, defaultCheckFunction)
		convey.So(arrary[roundRobinIndex].workerID, convey.ShouldEqual, enrtyWorker2.workerID)

		roundRobinIndex = RoundRobin(roundRobinIndex, arrary, nil, defaultCheckFunction)
		convey.So(arrary[roundRobinIndex].workerID, convey.ShouldEqual, enrtyWorker1.workerID)
		roundRobinIndex = RoundRobin(roundRobinIndex, arrary, map[string]struct{}{enrtyWorker2.workerID: {}}, defaultCheckFunction)
		convey.So(arrary[roundRobinIndex].workerID, convey.ShouldEqual, enrtyWorker1.workerID)
		roundRobinIndex = RoundRobin(roundRobinIndex, arrary, map[string]struct{}{enrtyWorker2.workerID: {}}, defaultCheckFunction)
		convey.So(arrary[roundRobinIndex].workerID, convey.ShouldEqual, enrtyWorker1.workerID)
		roundRobinIndex = RoundRobin(roundRobinIndex, arrary, map[string]struct{}{enrtyWorker1.workerID: {}}, defaultCheckFunction)
		convey.So(arrary[roundRobinIndex].workerID, convey.ShouldEqual, enrtyWorker2.workerID)

		enrtyWorker3 := fakeScheduleEntry{
			workerID:        "worker1",
			currentRequests: 0,
			concurrency:     -1,
		}
		arrary = append(arrary, enrtyWorker3)
		roundRobinIndex = RoundRobin(roundRobinIndex, arrary, nil, defaultCheckFunction)
		convey.So(roundRobinIndex, convey.ShouldEqual, 0)

		var emptyArrary []fakeScheduleEntry
		roundRobinIndex = RoundRobin(roundRobinIndex, emptyArrary, nil, defaultCheckFunction)
		convey.So(roundRobinIndex, convey.ShouldEqual, -1)
	})
}
