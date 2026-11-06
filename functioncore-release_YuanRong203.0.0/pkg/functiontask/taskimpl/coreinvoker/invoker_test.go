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

package coreinvoker

import (
	"functioncore/pkg/core-sdk/types"
	"sync"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/snerror"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common"
)

func TestInvoker_Invoke(t *testing.T) {
	invoke := &Invoker{}
	Convey("TestInvoker_Invoke", t, func() {
		Convey("test: TypeAssertionMsgConfig failed", func() {
			args := &common.InvokeArgs{
				ReqIDList: []common.ReqID{"aa", "bb"},
			}
			resp := invoke.Invoke(args)
			So(resp.Code, ShouldEqual, common.EFieldDefinition)
		})
		Convey("test: CallOnDemand failed", func() {
			patch := ApplyFunc(functioncore.CallOnDemand,
				func(args types.CallArgs, nodeID, stateID string, raw []byte, config functioncore.MsgConfig) functioncore.Result {
					return functioncore.Result{
						ErrCode: 211400,
					}
				})
			defer patch.Reset()
			args := &common.InvokeArgs{
				ReqIDList: []common.ReqID{"aa", "bb"},
				InvokeConfig: &functioncore.MsgConfig{
					FuncCallID: 1,
				},
				StateID: "xxx",
			}
			resp := invoke.Invoke(args)
			So(resp.Code, ShouldEqual, 211400)
		})
		Convey("test: invalidReply", func() {
			patch := ApplyFunc(functioncore.CallOnDemand,
				func(args types.CallArgs, nodeID, stateID string, raw []byte, config functioncore.MsgConfig) functioncore.Result {
					return functioncore.Result{}
				})
			defer patch.Reset()
			args := &common.InvokeArgs{
				ReqIDList: []common.ReqID{"aa", "bb"},
				InvokeConfig: &functioncore.MsgConfig{
					FuncCallID: 1,
				},
				StateID: "xxx",
			}
			resp := invoke.Invoke(args)
			So(resp.Code, ShouldEqual, common.EFieldDefinition)
		})
		Convey("test: unmarshalCallMessage failed", func() {

			patch := ApplyFunc(functioncore.CallOnDemand,
				func(args types.CallArgs, nodeID, stateID string, raw []byte, config functioncore.MsgConfig) functioncore.Result {
					return functioncore.Result{
						Reply: []byte("aaa"),
					}
				})
			defer patch.Reset()
			args := &common.InvokeArgs{
				ReqIDList: []common.ReqID{"aa", "bb"},
				InvokeConfig: &functioncore.MsgConfig{
					FuncCallID: 1,
				},
				StateID: "xxx",
			}
			resp := invoke.Invoke(args)
			So(resp.Code, ShouldEqual, common.EOK)
		})
		Convey("test: success", func() {
			content := "this is content"
			stateID := "xxx"
			patch := ApplyFunc(functioncore.CallOnDemand,
				func(args types.CallArgs, nodeID, stateID string, raw []byte, config functioncore.MsgConfig) functioncore.Result {
					reply, _ := proto.Marshal(&common.CallResult{
						State: &common.State{
							Id:      stateID,
							Content: content,
						},
					})
					return functioncore.Result{
						Reply: reply,
					}
				})
			defer patch.Reset()
			args := &common.InvokeArgs{
				ReqIDList: []common.ReqID{"aa", "bb"},
				InvokeConfig: &functioncore.MsgConfig{
					FuncCallID: 1,
				},
				StateID: stateID,
			}
			resp := invoke.Invoke(args)
			So(resp.Code, ShouldEqual, common.EOK)
			So(resp.StateID, ShouldEqual, stateID)
			So(resp.State, ShouldEqual, content)
		})
	})
}

// TestInvoker_Invoke2_call -
func TestInvoker_Invoke2_call(t *testing.T) {
	Convey("test: call for silent func", t, func() {
		cache := sync.Map{}
		patch := ApplyFunc(functioncore.CallOnDemand, func(args types.CallArgs, nodeID, stateID string,
			raw []byte, config functioncore.MsgConfig) functioncore.Result {
			if _, ok := cache.Load(args.FuncName); ok {
				return functioncore.Result{
					ErrCode: common.EOK,
					Reply:   []byte("test ok"),
				}
			}
			return functioncore.Result{
				ErrCode: snerror.RefreshSilentFunc,
				Reply:   []byte(snerror.ErrText(snerror.RefreshSilentFunc)),
			}
		})
		defer patch.Reset()
		resp := call(&common.InvokeArgs{
			FuncName: "test",
		}, []byte{}, &functioncore.MsgConfig{})
		So(resp.Code, ShouldEqual, snerror.RefreshSilentFunc)
	})
}
