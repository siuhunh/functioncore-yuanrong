/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

package cache

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestCheckpointCache_All(t *testing.T) {
	convey.Convey("TestCheckpointCache base add, query and del", t, func() {
		GetCheckpointCache().AddCheckpointTaskInfo("taskID1")
		convey.So(1, convey.ShouldEqual, len(GetCheckpointCache().checkpointTaskList))
		convey.So(GetCheckpointCache().GetTaskInfoCancel("taskID1"), convey.ShouldBeFalse)
		convey.So(GetCheckpointCache().GetTaskInfoPodName("taskID1") == "", convey.ShouldBeTrue)
		GetCheckpointCache().SetTaskInfoCancel("taskID1")
		GetCheckpointCache().SetTaskInfoPodName("taskID1", "podName1")
		convey.So(GetCheckpointCache().GetTaskInfoCancel("taskID1"), convey.ShouldBeTrue)
		convey.So("podName1", convey.ShouldEqual, GetCheckpointCache().GetTaskInfoPodName("taskID1"))

		convey.So(GetCheckpointCache().IsTaskExist("taskID1"), convey.ShouldBeTrue)
		convey.So(GetCheckpointCache().IsTaskExist("taskID2"), convey.ShouldBeFalse)
		convey.So(GetCheckpointCache().IsTaskCanceled("taskID1"), convey.ShouldBeTrue)
		convey.So(GetCheckpointCache().IsTaskCanceled("taskID2"), convey.ShouldBeFalse)

		GetCheckpointCache().DelCheckpointTaskInfo("taskID1")
		convey.So(0, convey.ShouldEqual, len(GetCheckpointCache().checkpointTaskList))
	})
	convey.Convey("TestCheckpointCache set or query taskID not exist", t, func() {
		GetCheckpointCache().checkpointTaskList = make(map[string]checkpointTaskInfo)
		GetCheckpointCache().SetTaskInfoCancel("taskID1")
		GetCheckpointCache().SetTaskInfoPodName("taskID1", "podName1")
		convey.So(0, convey.ShouldEqual, len(GetCheckpointCache().checkpointTaskList))
		convey.So(GetCheckpointCache().GetTaskInfoCancel("taskID1"), convey.ShouldBeFalse)
		convey.So(GetCheckpointCache().GetTaskInfoPodName("taskID1") == "", convey.ShouldBeTrue)
	})
}
