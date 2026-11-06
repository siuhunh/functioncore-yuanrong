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

package runtime

import (
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/tcpmsg"
)

func TestNewRuntimeState(t *testing.T) {
	convey.Convey("TestGenerateLogHeader", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessageWithNoResp, func(r *tcpmsg.SocketConn,
				msg *tcpmsg.Message) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&ProcessStatus{}), "Get", func(_ *ProcessStatus) tcpmsg.RuntimeProcessStatus {
				return tcpmsg.RuntimeProcessStatusRunning
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		NewRuntimeState(true)
		runtimeState := GetDirectRuntimeState()
		go func() {
			time.Sleep(100 * time.Millisecond)
			runtimeState.Unblock()
		}()
		go func() {
			time.Sleep(110 * time.Millisecond)
			runtimeState.Unblock()
		}()
		err := runtimeState.WaitUnblock(true)
		time.Sleep(150 * time.Millisecond)
		convey.So(err, convey.ShouldBeNil)
	})
}
