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

package coreadaptor

import (
	"errors"
	"reflect"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/adaptor"
	"functioncore/pkg/functiontask/task/api/service"
	"functioncore/pkg/functiontask/task/api/triggerbykey"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/statequeue"
	"functioncore/pkg/functiontask/task/taskcontext"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

var cfg = &coreconfig.Config{
	Config: config.Config{
		PersistentFuture: true,
	},
}
var ada *CoreAdaptor

func init() {
	config.Conf.InitStateClient = true
	ada = Init(cfg)
	SetDefaultFailureMode(FailureContinues)
}

func TestCoreAdaptor_MsgHandler(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(triggerbykey.ByKey,
			func(taskCtx taskcontext.Context, req *triggerbykey.Request) (int32, string, string) {
				return 1, "", ""
			}),
		ApplyFunc(service.GetAllStateID,
			func(taskCtx taskcontext.Context) (map[string]string, common.TaskError) {
				return nil, nil
			}),
		ApplyFunc(service.ClearAllState,
			func(taskCtx taskcontext.Context) common.TaskError {
				return nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestCoreAdaptor_MsgHandler", t, func() {
		Convey("ExternalRequest, unmarshal FuncArgs failed", func() {
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: true})
			So(err, ShouldBeNil)
		})
		Convey("ExternalRequest, unmarshal FuncArgs success", func() {
			args := `{"body":"aaa"}`
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: true, FuncArgs: []byte(args)})
			So(err, ShouldBeNil)
		})
		Convey("ExternalRequest, no need to InitStateClient", func() {
			config.Conf.InitStateClient = false
			args := `{"body":"aaa"}`
			stateKey := `{"source":"", "cmd":"getServiceState", "statekey":"xxx"}`
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: true, FuncArgs: []byte(args), StateKey: stateKey})
			So(err, ShouldBeNil)
		})
		Convey("ExternalRequest, getServiceState cmd", func() {
			config.Conf.InitStateClient = true
			args := `{"body":"aaa"}`
			stateKey := `{"source":"", "cmd":"getServiceState", "statekey":"xxx"}`
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: true, FuncArgs: []byte(args), StateKey: stateKey})
			time.Sleep(100 * time.Millisecond)
			So(err, ShouldBeNil)
		})
		Convey("ExternalRequest, deleteServiceState cmd", func() {
			config.Conf.InitStateClient = true
			args := `{"body":"aaa"}`
			stateKey := `{"source":"", "cmd":"deleteServiceState", "statekey":"xxx"}`
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: true, FuncArgs: []byte(args), StateKey: stateKey})
			time.Sleep(100 * time.Millisecond)
			So(err, ShouldBeNil)
		})
		Convey("ExternalRequest, other cmd, cli", func() {
			config.Conf.InitStateClient = true
			args := `{"body":"aaa"}`
			stateKey := `{"source":"cli", "cmd":"xxx", "statekey":"xxx"}`
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: true, FuncArgs: []byte(args),
					StateKey: stateKey})
			time.Sleep(100 * time.Millisecond)
			So(err, ShouldBeNil)
		})
		Convey("ExternalRequest, other cmd, invoke", func() {
			config.Conf.InitStateClient = true
			args := `{"body":"aaa"}`
			stateKey := `{"source":"invoke", "cmd":"xxx", "statekey":"xxx"}`
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: true, FuncArgs: []byte(args), StateKey: stateKey})
			time.Sleep(100 * time.Millisecond)
			So(err, ShouldBeNil)
		})
		Convey("failed to call CommonAdaptor", func() {
			patch := ApplyMethod(reflect.TypeOf(&adaptor.Adaptor{}), "CommonAdaptor",
				func(_ *adaptor.Adaptor, taskCtx taskcontext.Context, funcName string, event []byte, source int32) error {
					return nil
				})
			defer patch.Reset()
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: false})
			So(err, ShouldBeNil)
		})
		Convey("success to call CommonAdaptor", func() {
			patch := ApplyMethod(reflect.TypeOf(&adaptor.Adaptor{}), "CommonAdaptor",
				func(_ *adaptor.Adaptor, taskCtx taskcontext.Context, funcName string, event []byte, source int32) error {
					return errors.New("xx")
				})
			defer patch.Reset()
			err := ada.MsgHandler("xx", 0,
				&functioncore.HookArgs{ExternalRequest: false})
			So(err, ShouldNotBeNil)
		})
	})
}
func TestCoreAdaptor_ExpiredCb(t *testing.T) {
	patches := []*Patches{ApplyMethod(reflect.TypeOf(ada.StateQueueMap), "DelStateIfIdle",
		func(_ *statequeue.Map, stateID string) bool {
			return true
		}),
		ApplyFunc(functioncore.DeleteStateMapping,
			func(stateID string, tenantID string, proxyFunction string) snerror.SNError {
				return nil
			}),
		ApplyFunc(functioncore.SRem,
			func(key string, value string) (int, snerror.SNError) {
				return 0, nil
			}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestCoreAdaptor_ExpiredCb", t, func() {
		Convey("not stateID", func() {
			So(func() {
				ada.ExpiredCb("xxx:xx")
			}, ShouldNotPanic)
		})
		Convey("DelStateIfIdle failed", func() {
			patch := ApplyMethod(reflect.TypeOf(ada.StateQueueMap), "DelStateIfIdle",
				func(_ *statequeue.Map, stateID string) bool {
					return false
				})
			defer patch.Reset()
			So(func() {
				ada.ExpiredCb("xxx:stateID:xx")
			}, ShouldNotPanic)
		})
		Convey("functioncore.DeleteStateMapping failed", func() {
			patch := ApplyFunc(functioncore.DeleteStateMapping,
				func(stateID string, tenantID string, proxyFunction string) snerror.SNError {
					return snerror.New(1, "xx")
				})
			defer patch.Reset()
			So(func() {
				ada.ExpiredCb("xxx:stateID:xx")
			}, ShouldNotPanic)
		})
		Convey("functioncore.SRem failed", func() {
			patch := ApplyFunc(functioncore.SRem,
				func(key string, value string) (int, snerror.SNError) {
					return 0, snerror.New(1, "xxx")
				})
			defer patch.Reset()
			So(func() {
				ada.ExpiredCb("xxx:stateID:xx")
			}, ShouldNotPanic)
		})
		Convey("CallBack success", func() {
			So(func() {
				ada.ExpiredCb("xxx:stateID:xx")
			}, ShouldNotPanic)
		})
	})
}
