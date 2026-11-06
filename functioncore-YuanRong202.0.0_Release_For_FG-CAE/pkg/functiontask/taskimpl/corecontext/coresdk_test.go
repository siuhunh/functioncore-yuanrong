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

package corecontext

import (
	"errors"
	"reflect"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types/bridgetypes"
	commonUtils "functioncore/pkg/core-sdk/common"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/core-sdk/sdk/bridgefunc"
	"functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/api/samenode"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/statequeue"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

var ctx = New(1, &functioncore.HookArgs{
	SrcTenant:   "xx",
	LogType:     "x",
	TraceID:     "xxxx",
	SNServiceID: "xxx",
})

func TestContext_InvokeConfig(t *testing.T) {
	Convey("TestContext_InvokeConfig", t, func() {
		So(ctx.InvokeConfig("xx", false, "", nil), ShouldNotBeNil)
	})
}

func TestMsgConfigFields(t *testing.T) {
	Convey("TestMsgConfigFields", t, func() {
		res := MsgConfigFields(&functioncore.MsgConfig{})
		So(res, ShouldNotBeNil)
	})
}

func TestContext_CallTask(t *testing.T) {
	ctx := New(1, &functioncore.HookArgs{})
	patches := []*Patches{
		ApplyFunc(functioncore.Call,
			func(destFuncName string, stateID string, args []byte, config functioncore.MsgConfig) functioncore.Result {
				resp := invoke.TaskResponse{
					Code:    0,
					Message: "sss",
				}
				reply, _ := resp.Marshal()
				return functioncore.Result{
					Reply:   reply,
					ErrCode: 0,
					ErrMsg:  "xxx",
					Log:     "",
				}
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestContext_CallTask", t, func() {
		Convey("test: IsSameNode failed", func() {
			patches2 := []*Patches{
				ApplyFunc(samenode.CallTask, func(c taskcontext.Context, method int32,
					req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
					reflect.ValueOf(resp).Elem().FieldByName("Code").SetInt(int64(common.EFunctionTask))
					reflect.ValueOf(resp).Elem().FieldByName("Message").SetString("unsupported task request")
					return
				}),
			}
			coreconfig.NodeID = "testStateNodeID"
			functioncore.FunctionCheck.GetNodeIDByStateID = func(stateID string) string {
				return coreconfig.NodeID
			}
			defer func() {
				for _, p := range patches2 {
					p.Reset()
				}
				functioncore.FunctionCheck.GetNodeIDByStateID = nil
				coreconfig.NodeID = ""
			}()
			var req = &invoke.TaskRequest{}
			var resp = &invoke.TaskResponse{}
			ctx.CallTask(common.MethodTaskInvoke, req, resp)
			So(resp.GetCode(), ShouldEqual, int32(common.EFunctionTask))
		})
		Convey("test: functioncore.Call failed", func() {
			patch := ApplyFunc(functioncore.Call,
				func(destFuncName string, stateID string, args []byte, config functioncore.MsgConfig) functioncore.Result {
					return functioncore.Result{
						Reply:   nil,
						ErrCode: 1,
						ErrMsg:  "xxx",
						Log:     "",
					}
				})
			patch.ApplyFunc((*statequeue.Map).CheckAndClearOldStateQueue, func(
				_ *statequeue.Map, meta *metadata.Metadata) {
			})
			functioncore.FunctionCheck.GetNodeIDByStateID = func(stateID string) string {
				return "testNodeXXX"
			}
			defer func() {
				patch.Reset()
				functioncore.FunctionCheck.GetNodeIDByStateID = nil
			}()
			var req = &invoke.TaskRequest{}
			var resp = &invoke.TaskResponse{}
			ctx.CallTask(common.MethodTaskInvoke, req, resp)
			So(resp.Code, ShouldEqual, 1)
		})
		Convey("test: success", func() {
			functioncore.FunctionCheck.GetNodeIDByStateID = func(stateID string) string {
				return "testNodeXXX"
			}
			patch := ApplyFunc((*statequeue.Map).CheckAndClearOldStateQueue, func(
				_ *statequeue.Map, meta *metadata.Metadata) {
			})
			defer func() {
				patch.Reset()
				functioncore.FunctionCheck.GetNodeIDByStateID = nil
			}()
			var req = &invoke.TaskRequest{}
			var resp = &invoke.TaskResponse{}
			ctx.CallTask(common.MethodTaskInvoke, req, resp)
			So(resp.Code, ShouldEqual, 0)
		})
	})
}

func TestContext_CreateState(t *testing.T) {
	ctx := New(1, &functioncore.HookArgs{})
	patches := []*Patches{
		ApplyFunc(functioncore.CreateState,
			func(funcName, stateID string, config types.CreateStateConfig) (string, string, snerror.SNError) {
				return stateID, "nodename", nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestContext_CreateState", t, func() {
		Convey("test: functioncore.CreateState success", func() {
			coreconfig.NodeID = "nodename"
			functioncore.FunctionCheck.GetNodeIDByStateID = func(stateID string) string {
				return "nodename"
			}
			stateID, _, err := ctx.CreateState("xx", taskcontext.NewState)
			So(err, ShouldBeNil)
			So(stateID, ShouldEqual, "0")
			So(ctx.IsSameNode(stateID), ShouldBeTrue)
		})
		Convey("test: functioncore.CreateState failed", func() {
			coreconfig.NodeID = "nodename"
			patch := ApplyFunc(functioncore.CreateState,
				func(funcName, stateID string, config types.CreateStateConfig) (string, string, snerror.SNError) {
					return "", "", snerror.New(common.EFunctionTask, "xx")
				})
			defer patch.Reset()
			_, _, err := ctx.CreateState("xx", taskcontext.NewState)
			So(err.Code(), ShouldEqual, common.EFunctionTask)
		})
	})
}
func TestContext_SetResult(t *testing.T) {
	ctx := New(1, &functioncore.HookArgs{})
	patches := []*Patches{
		ApplyFunc(functioncore.SetResult,
			func(invokeID uint64, errCode int32, result []byte, logs string) ([]byte, error) {
				return nil, nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestContext_SetResult", t, func() {
		Convey("test: code is not ok", func() {
			So(func() {
				ctx.SetResult(&invoke.Response{
					Code: 111,
				})
			}, ShouldNotPanic)
		})
		Convey("test: marshal failed", func() {
			patch := ApplyMethod(reflect.TypeOf(&invoke.Response{}), "Marshal",
				func(_ *invoke.Response) ([]byte, error) {
					return nil, errors.New("marshal failed")
				})
			defer patch.Reset()
			So(func() {
				ctx.SetResult(&invoke.Response{})
			}, ShouldNotPanic)
		})
		Convey("test: functioncore.SetResult failed", func() {
			patch := ApplyFunc(functioncore.SetResult,
				func(invokeID uint64, errCode int32, result []byte, logs string) ([]byte, error) {
					return nil, errors.New("xx")
				})
			defer patch.Reset()
			So(func() {
				ctx.SetResult(&invoke.Response{})
			}, ShouldNotPanic)
		})
		Convey("test: success", func() {
			So(func() {
				ctx.SetResult(&invoke.Response{})
			}, ShouldNotPanic)
		})
	})
}
func TestContext_SetResultWithLog(t *testing.T) {
	ctx := New(1, &functioncore.HookArgs{})
	patches := []*Patches{
		ApplyFunc(functioncore.SetResult,
			func(invokeID uint64, errCode int32, result []byte, logs string) ([]byte, error) {
				return nil, nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestContext_SetResultWithLog", t, func() {
		Convey("test: code is not ok", func() {
			So(func() {
				ctx.SetResultWithLog(&invoke.Response{
					Code: 111,
				}, "")
			}, ShouldNotPanic)
		})
		Convey("test: marshal failed", func() {
			patch := ApplyMethod(reflect.TypeOf(&invoke.Response{}), "Marshal",
				func(_ *invoke.Response) ([]byte, error) {
					return nil, errors.New("marshal failed")
				})
			defer patch.Reset()
			So(func() {
				ctx.SetResultWithLog(&invoke.Response{}, "")
			}, ShouldNotPanic)
		})
		Convey("test: functioncore.SetResult failed", func() {
			patch := ApplyFunc(functioncore.SetResult,
				func(invokeID uint64, errCode int32, result []byte, logs string) ([]byte, error) {
					return nil, errors.New("xx")
				})
			defer patch.Reset()
			So(func() {
				ctx.SetResultWithLog(&invoke.Response{}, "")
			}, ShouldNotPanic)
		})
		Convey("test: success", func() {
			So(func() {
				ctx.SetResultWithLog(&invoke.Response{}, "")
			}, ShouldNotPanic)
		})
	})
}
func TestContext_SetResultWithCode(t *testing.T) {
	ctx := New(1, &functioncore.HookArgs{})
	patches := []*Patches{
		ApplyFunc(functioncore.SetResult,
			func(invokeID uint64, errCode int32, result []byte, logs string) ([]byte, error) {
				return nil, nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestContext_SetResultWithCode", t, func() {
		Convey("test: code is not ok", func() {
			So(func() {
				ctx.SetResultWithCode(1, &invoke.Response{
					Code: 111,
				}, "")
			}, ShouldNotPanic)
		})
		Convey("test: marshal failed", func() {
			patch := ApplyMethod(reflect.TypeOf(&invoke.Response{}), "Marshal",
				func(_ *invoke.Response) ([]byte, error) {
					return nil, errors.New("marshal failed")
				})
			defer patch.Reset()
			So(func() {
				ctx.SetResultWithCode(1, &invoke.Response{}, "")
			}, ShouldNotPanic)
		})
		Convey("test: functioncore.SetResult failed", func() {
			patch := ApplyFunc(functioncore.SetResult,
				func(invokeID uint64, errCode int32, result []byte, logs string) ([]byte, error) {
					return nil, errors.New("xx")
				})
			defer patch.Reset()
			So(func() {
				ctx.SetResultWithCode(1, &invoke.Response{}, "")
			}, ShouldNotPanic)
		})
		Convey("test: success", func() {
			So(func() {
				ctx.SetResultWithCode(1, &invoke.Response{}, "")
			}, ShouldNotPanic)
		})
	})
}

func TestContext_GetBridgeFunc(t *testing.T) {
	Convey("TestContext_CreateState", t, func() {
		Convey("test: functioncore.GetBridgeFunc failed", func() {
			patch := ApplyFunc(bridgefunc.ApplyStateKey,
				func(clientInstance bridgetypes.ClientInstance, traceID string) (string, bool, snerror.SNError) {
					return "12345678", false, snerror.New(commonUtils.BridgeFuncParamErrCode, commonUtils.BridgeFuncParamErrMsg)
				})
			defer patch.Reset()
			stateKey, err := ctx.GetFuncStateKey("xx", bridgetypes.ClientInstance{})
			So(err, ShouldNotBeNil)
			So(stateKey, ShouldEqual, "")
		})
		Convey("test: functioncore.GetBridgeFunc success", func() {
			patch := ApplyFunc(bridgefunc.ApplyStateKey,
				func(clientInstance bridgetypes.ClientInstance, traceID string) (string, bool, snerror.SNError) {
					return "12345678", false, nil
				})
			defer patch.Reset()
			stateKey, err := ctx.GetFuncStateKey("xx", bridgetypes.ClientInstance{})
			So(err, ShouldBeNil)
			So(stateKey, ShouldEqual, "12345678")
		})
	})
}

func TestContext_ReleaseBridge(t *testing.T) {
	Convey("TestContext_ReleaseBridge", t, func() {
		Convey("test: functioncore.GetBridgeFunc failed", func() {
			patch := ApplyFunc(bridgefunc.Release,
				func(clientInstance bridgetypes.ClientInstance, traceID string) (bool, snerror.SNError) {
					return false, snerror.New(commonUtils.BridgeFuncParamErrCode, commonUtils.BridgeFuncParamErrMsg)
				})
			defer patch.Reset()
			isTerminate, err := ctx.Release("xx", bridgetypes.ClientInstance{})
			So(err, ShouldNotBeNil)
			So(isTerminate, ShouldEqual, false)
		})
		Convey("test: functioncore.ReleaseBridge success", func() {
			patch := ApplyFunc(bridgefunc.Release,
				func(clientInstance bridgetypes.ClientInstance, traceID string) (bool, snerror.SNError) {
					return true, nil
				})
			defer patch.Reset()
			isTerminate, err := ctx.Release("xx", bridgetypes.ClientInstance{})
			So(err, ShouldBeNil)
			So(isTerminate, ShouldEqual, true)
		})
	})
}
