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
package waitfuture

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/deadlock"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/test/functiontask/test/testcontext"
)

type Checker struct {
}

func (c *Checker) Start(*deadlock.StartRequest) *deadlock.StartResponse {
	return nil
}

func (c *Checker) End(*deadlock.EndRequest) *deadlock.EndResponse {
	return nil
}

func TestEnd(t *testing.T) {
	Convey("status nok", t, func() {
		taskCtx := testcontext.New(common.MethodInvoke)
		checker := &Checker{}
		startResp := &deadlock.StartResponse{
			DeadlockID: "DeadlockID",
		}
		var patches []*gomonkey.Patches
		//patches = append(patches, gomonkey.ApplyFunc(createStateQueue,
		//	func(taskCtx taskcontext.Context, metaData *metadata.Metadata, req *filter.RequestPb) {
		//	}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(checker), "End",
			func(checker *Checker, endReq *deadlock.EndRequest) *deadlock.EndResponse {
				return &deadlock.EndResponse{
					Code: 1,
				}
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		end(taskCtx, "state-id", checker, startResp, []string{""})
	})
	Convey("ResolveDeadlock false", t, func() {
		config.Conf.ResolveDeadlock = false
		taskCtx := testcontext.New(common.MethodInvoke)
		checker := &Checker{}
		startResp := &deadlock.StartResponse{
			DeadlockID: "DeadlockID",
		}
		var patches []*gomonkey.Patches
		//patches = append(patches, gomonkey.ApplyFunc(createStateQueue,
		//	func(taskCtx taskcontext.Context, metaData *metadata.Metadata, req *filter.RequestPb) {
		//	}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(checker), "End",
			func(checker *Checker, endReq *deadlock.EndRequest) *deadlock.EndResponse {
				return &deadlock.EndResponse{
					Code: 1,
				}
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		end(taskCtx, "state-id", checker, startResp, []string{""})
	})
	Convey("success", t, func() {
		config.Conf.ResolveDeadlock = true
		taskCtx := testcontext.New(common.MethodInvoke)
		checker := &Checker{}
		startResp := &deadlock.StartResponse{
			DeadlockID: "DeadlockID",
		}
		var patches []*gomonkey.Patches
		//patches = append(patches, gomonkey.ApplyFunc(createStateQueue,
		//	func(taskCtx taskcontext.Context, metaData *metadata.Metadata, req *filter.RequestPb) {
		//	}))
		patches = append(patches, gomonkey.ApplyMethod(reflect.TypeOf(checker), "End",
			func(checker *Checker, endReq *deadlock.EndRequest) *deadlock.EndResponse {
				return &deadlock.EndResponse{
					Code: 0,
				}
			}))
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		end(taskCtx, "state-id", checker, startResp, []string{""})
	})
}
