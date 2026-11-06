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
package terminal

import (
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestJudgeInput(t *testing.T) {
	convey.Convey("TestJudgeInput", t, func() {
		var testInputEvent inputEvent

		testInputEvent = judgeInput([]byte{3})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyCtrlC)
		testInputEvent = judgeInput([]byte{8})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyBackspace)
		testInputEvent = judgeInput([]byte{9})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyTab)
		testInputEvent = judgeInput([]byte{13})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyEnter)
		testInputEvent = judgeInput([]byte{27})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyESC)
		testInputEvent = judgeInput([]byte{27, 90})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyUnknown)
		testInputEvent = judgeInput([]byte{27, 91, 50})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyIns)
		testInputEvent = judgeInput([]byte{27, 91, 51})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyDel)
		testInputEvent = judgeInput([]byte{27, 91, 53})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyPgup)
		testInputEvent = judgeInput([]byte{27, 91, 54})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyPgdn)
		testInputEvent = judgeInput([]byte{27, 91, 65})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyUp)
		testInputEvent = judgeInput([]byte{27, 91, 66})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyDown)
		testInputEvent = judgeInput([]byte{27, 91, 67})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyRight)
		testInputEvent = judgeInput([]byte{27, 91, 68})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyLeft)
		testInputEvent = judgeInput([]byte{27, 91, 70})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyEnd)
		testInputEvent = judgeInput([]byte{27, 91, 72})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyHome)
		testInputEvent = judgeInput([]byte{27, 91, 172})
		convey.So(testInputEvent.inputKey, convey.ShouldEqual, keyUnknown)

	})
}
