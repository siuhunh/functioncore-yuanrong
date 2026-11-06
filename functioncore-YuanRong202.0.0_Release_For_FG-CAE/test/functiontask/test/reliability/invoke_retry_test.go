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

package reliability

import (
	"fmt"
	"reflect"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	uuid "github.com/satori/go.uuid"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/invoke"
	"functioncore/pkg/functiontask/task/api/util"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/test/functiontask/test/testcontext"
)

func setError(resp taskcontext.Serializable, code int, message string) {
	reflect.ValueOf(resp).Elem().FieldByName("Code").
		Set(reflect.ValueOf(int32(code)))
	reflect.ValueOf(resp).Elem().FieldByName("Message").Set(
		reflect.ValueOf(message))
}

func newStateReq() *invoke.Request {
	req := &invoke.Request{
		RequestPb: invoke.RequestPb{
			FuncName:        "xx",
			Version:         "xx",
			Payload:         "xx",
			ByKey:           false,
			Key:             "",
			StateID:         "new-state",
			CountOfFuncName: 0,
		},
	}
	return req
}

var stateSeq = 0

func certainStateIDReq() *invoke.Request {
	req := &invoke.Request{
		RequestPb: invoke.RequestPb{
			FuncName:        "xx",
			Version:         "xx",
			Payload:         "xx",
			ByKey:           false,
			Key:             "",
			StateID:         "xx" + fmt.Sprint(stateSeq),
			CountOfFuncName: 0,
		},
	}
	stateSeq++
	return req
}

var stateKeyReq = 0

func byKeyReq() *invoke.Request {
	req := &invoke.Request{
		RequestPb: invoke.RequestPb{
			FuncName:        "xx",
			Version:         "xx",
			Payload:         "xx",
			ByKey:           true,
			Key:             "kk" + fmt.Sprint(stateSeq),
			StateID:         "",
			CountOfFuncName: 0,
		},
	}
	stateKeyReq++
	return req
}

var scheduleErrMsg = "failed to send call request"

func TestInvokeRetryNewState(t *testing.T) {
	Convey("TestInvokeRetryNewState,retry after callTask failed", t, func() {
		config.Conf.InitStateClient = true
		config.Conf.PersistentFuture = false
		times := 0
		failedCount := 1
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(&testcontext.Context{}), "CallTask",
				func(c *testcontext.Context, method int32,
					req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
					if method != common.MethodTaskInvoke {
						setError(resp, common.EOK, "xx")
						return
					}
					if times < failedCount {
						setError(resp, common.ScheduleInsErr, scheduleErrMsg)
					} else {
						setError(resp, common.EOK, "xx")
					}
					times++
					return
				}),
			ApplyMethod(reflect.TypeOf(&stateclient.StateStore{}), "StateExists",
				func(_ *stateclient.StateStore, key string) (bool, common.TaskError) {
					return true, nil
				}),
			ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName, stateID string) (string, common.TaskError) {
				return stateID, nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		doInvoke := func(req *invoke.Request) taskcontext.Serializable {
			taskCtx := testcontext.New(common.MethodInvoke)
			taskCtx.SetServiceID(uuid.NewV4().String())
			resp := req.Caller(taskCtx)
			return resp
		}

		Convey("test: invoke new-state, unexpected error", func() {
			patch := ApplyMethod(reflect.TypeOf(&testcontext.Context{}), "CallTask",
				func(c *testcontext.Context, method int32,
					req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
					setError(resp, common.EFunctionCore, "xxx")
					return
				})
			defer patch.Reset()
			failedCount = 0
			req := newStateReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: invoke new-state, success", func() {
			failedCount = 0
			req := newStateReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EOK)
		})
		Convey("test: invoke new-state, failed once", func() {
			failedCount = 1
			req := newStateReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EOK)
		})
		Convey("test: invoke new-state, failed once, but createState failed", func() {
			failedCount = 1
			times = 0
			req := newStateReq()
			cnt := 0
			patch := ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName, stateID string) (string, common.TaskError) {
				cnt++
				if cnt > 1 {
					return "", common.NewTaskError(common.ECreateState, "xx")
				}
				return uuid.NewV4().String(), nil
			})
			defer patch.Reset()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.ECreateState)
		})
		Convey("test: invoke new-state, failed always", func() {
			failedCount = 1000
			req := newStateReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.ScheduleInsErr)
		})
	})
}
func TestInvokeRetryByKey(t *testing.T) {
	Convey("TestInvokeRetryByKey,retry after callTask failed", t, func() {
		config.Conf.InitStateClient = true
		config.Conf.PersistentFuture = false
		times := 0
		failedCount := 1
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(&testcontext.Context{}), "CallTask",
				func(c *testcontext.Context, method int32,
					req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
					if method != common.MethodTaskInvoke {
						setError(resp, common.EOK, "xx")
						return
					}
					if times < failedCount {
						setError(resp, common.ScheduleInsErr, scheduleErrMsg)
					} else {
						setError(resp, common.EOK, "xx")
					}
					times++
					return
				}),
			ApplyMethod(reflect.TypeOf(&stateclient.StateStore{}), "StateExists",
				func(_ *stateclient.StateStore, key string) (bool, common.TaskError) {
					return true, nil
				}),
			ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName, stateID string) (string, common.TaskError) {
				return stateID, nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		doInvoke := func(req *invoke.Request) taskcontext.Serializable {
			taskCtx := testcontext.New(common.MethodInvoke)
			taskCtx.SetServiceID(uuid.NewV4().String())
			resp := req.Caller(taskCtx)
			return resp
		}

		Convey("test: invoke by key, unexpected error", func() {
			patch := ApplyMethod(reflect.TypeOf(&testcontext.Context{}), "CallTask",
				func(c *testcontext.Context, method int32,
					req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
					setError(resp, common.EFunctionCore, "xxx")
					return
				})
			defer patch.Reset()
			failedCount = 0
			req := byKeyReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: invoke by key, success", func() {
			failedCount = 0
			req := byKeyReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EOK)
		})
		Convey("test: invoke by key, failed once", func() {
			failedCount = 1
			req := byKeyReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EOK)
		})
		Convey("test: invoke by key, failed once, but createState failed", func() {
			failedCount = 1
			req := byKeyReq()
			cnt := 0
			patch := ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName, stateID string) (string, common.TaskError) {
				cnt++
				if cnt > 1 {
					return "", common.NewTaskError(common.ECreateState, "xx")
				}
				return stateID, nil
			})
			defer patch.Reset()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.ECreateState)
		})
		Convey("test: invoke by key, failed always", func() {
			failedCount = 1000
			req := byKeyReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.ScheduleInsErr)
		})
	})
}
func TestInvokeRetryCertainStateID(t *testing.T) {
	Convey("TestInvokeRetryCertainStateID,retry after callTask failed", t, func() {
		config.Conf.InitStateClient = true
		config.Conf.PersistentFuture = false
		times := 0
		failedCount := 1
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(&testcontext.Context{}), "CallTask",
				func(c *testcontext.Context, method int32,
					req taskcontext.RequestWithStateID, resp taskcontext.Serializable) {
					if method != common.MethodTaskInvoke {
						setError(resp, common.EOK, "xx")
						return
					}
					if times < failedCount {
						setError(resp, common.ScheduleInsErr, scheduleErrMsg)
					} else {
						setError(resp, common.EOK, "xx")
					}
					times++
					return
				}),
			ApplyMethod(reflect.TypeOf(&stateclient.StateStore{}), "StateExists",
				func(_ *stateclient.StateStore, key string) (bool, common.TaskError) {
					return true, nil
				}),
			ApplyFunc(util.CreateState, func(taskCtx taskcontext.Context, funcName, stateID string) (string, common.TaskError) {
				return stateID, nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		doInvoke := func(req *invoke.Request) taskcontext.Serializable {
			taskCtx := testcontext.New(common.MethodInvoke)
			taskCtx.SetServiceID(uuid.NewV4().String())
			resp := req.Caller(taskCtx)
			return resp
		}
		Convey("test: invoke by stateID, call stateExists failed", func() {
			patch := ApplyMethod(reflect.TypeOf(&stateclient.StateStore{}), "StateExists",
				func(_ *stateclient.StateStore, key string) (bool, common.TaskError) {
					return false, common.NewTaskError(common.EFunctionCore, "xx")
				})
			defer patch.Reset()
			req := certainStateIDReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: invoke by stateID, does not exist", func() {
			patch := ApplyMethod(reflect.TypeOf(&stateclient.StateStore{}), "StateExists",
				func(_ *stateclient.StateStore, key string) (bool, common.TaskError) {
					return false, nil
				})
			defer patch.Reset()
			req := certainStateIDReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EStateIDNotFound)
			So(resp.GetMessage(), ShouldContainSubstring, "recycled or not exists")
		})
		Convey("test: invoke by stateID, exists", func() {
			patch := ApplyMethod(reflect.TypeOf(&stateclient.StateStore{}), "StateExists",
				func(_ *stateclient.StateStore, key string) (bool, common.TaskError) {
					return true, nil
				})
			defer patch.Reset()
			req := certainStateIDReq()
			resp := doInvoke(req)
			So(resp.GetCode(), ShouldEqual, common.EOK)
		})
	})

}
