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

package main

import (
	"errors"
	"net/http"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/util"
	"functioncore/pkg/functiontask/taskimpl/coreadaptor"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
)

func init() {
	SetDefaultFailureMode(FailureContinues)
}

func TestMainFunc(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(util.DelayExit, func(module string, err error) {
			panic(err)
		}),
		ApplyFunc(http.HandleFunc,
			func(pattern string, handler func(http.ResponseWriter, *http.Request)) {
				return
			}),
		ApplyFunc(coreconfig.Init, func() (*coreconfig.Config, error) {
			config := new(coreconfig.Config)
			config.InitStateClient = true
			return config, nil
		}),
		ApplyFunc(coreadaptor.Init, func(taskConfig *coreconfig.Config) *coreadaptor.CoreAdaptor {
			return &coreadaptor.CoreAdaptor{}
		}),
		ApplyFunc(functioncore.RegFuncInfo, func(info functioncore.RegisterInfo) error {
			return nil
		}),
		ApplyFunc(functioncore.Init, func(stopCh <-chan struct{}) error {
			return nil
		}),
		ApplyFunc(functioncore.InitStateClient, func(config functioncore.StateClientConfig) error {
			return nil
		}),
		ApplyFunc(functioncore.RegMsgHandler, func(adaptor functioncore.AbsAdaptor) error {
			return nil
		}),
		ApplyFunc(http.ListenAndServe, func(addr string, handler http.Handler) error {
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	Convey("TestMainFunc", t, func() {
		Convey("failed to call functioncore.RegFuncInfo in initFunctionCore", func() {
			err := errors.New("functioncore.RegFuncInfo")
			patch := ApplyFunc(functioncore.RegFuncInfo, func(info functioncore.RegisterInfo) error {
				return err
			})
			defer patch.Reset()
			So(main, ShouldPanic)
		})

		Convey("failed to call functioncore.Init in initCoreSDK", func() {
			err := errors.New("functioncore.Init")
			patch := ApplyFunc(functioncore.Init, func(stopCh <-chan struct{}) error {
				return err
			})
			defer patch.Reset()
			So(main, ShouldPanic)
		})

		Convey("failed to call functioncore.InitStateClient in initCoreSDK", func() {
			err := errors.New("functioncore.InitStateClient")
			patch := ApplyFunc(functioncore.InitStateClient, func(config functioncore.StateClientConfig) error {
				return err
			})
			defer patch.Reset()
			So(main, ShouldPanic)
		})

		Convey("failed to call functioncore.RegMsgHandler in initCoreSDK", func() {
			err := errors.New("functioncore.RegMsgHandler")
			patch := ApplyFunc(functioncore.RegMsgHandler, func(adaptor functioncore.AbsAdaptor) error {
				return err
			})
			defer patch.Reset()
			So(main, ShouldPanic)
		})

		Convey("failed to call http.ListenAndServe", func() {
			err := errors.New("http.ListenAndServe")
			patch := ApplyFuncSeq(http.ListenAndServe, []OutputCell{
				{
					Params{err},
					1,
				},
				{
					Params{nil},
					1,
				},
			})
			defer patch.Reset()
			So(main, ShouldPanic)
		})
		Convey("success to call http.ListenAndServe", func() {
			So(main, ShouldNotPanic)
		})
	})
}