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
package efs

import (
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

func TestMockExtFutureStore_SetResult(t *testing.T) {
	store := &futurestore.FutureStore{}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(store), "Get",
			func(_ *futurestore.FutureStore, _ snfuture.ID) *snfuture.Future {
				return nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type fields struct {
		s     *futurestore.FutureStore
		delay time.Duration
	}
	type args struct {
		tenantID  string
		serviceID string
		sncFuture *snfuture.Future
		result    *common.Response
	}
	var f fields
	var a args
	sncFuture := &snfuture.Future{}
	result := &common.Response{}

	f.s = store
	f.delay = 1 * time.Second
	a.result = result
	a.sncFuture = sncFuture
	want := common.NewTaskError(common.ESetValue, "no such future: ")
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &MockExtFutureStore{
				s:     tt.fields.s,
				delay: tt.fields.delay,
			}
			if got := store.SetResult(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture, tt.args.result); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMockExtFutureStore_SetResult_2(t *testing.T) {
	store := &futurestore.FutureStore{}
	snFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: "ServiceID",
		FuncName:  common.SetTimeoutFuncName,
		StateID:   "StateID",
		ID:        "ID",
	})
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(store), "Get",
			func(_ *futurestore.FutureStore, _ snfuture.ID) *snfuture.Future {
				return snFuture
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type fields struct {
		s     *futurestore.FutureStore
		delay time.Duration
	}
	type args struct {
		tenantID  string
		serviceID string
		sncFuture *snfuture.Future
		result    *common.Response
	}
	var f fields
	var a args
	sncFuture := &snfuture.Future{}
	result := &common.Response{}

	f.s = store
	f.delay = 1 * time.Second
	a.result = result
	a.sncFuture = sncFuture
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &MockExtFutureStore{
				s:     tt.fields.s,
				delay: tt.fields.delay,
			}
			if got := store.SetResult(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture, tt.args.result); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMockExtFutureStore_ClearResult(t *testing.T) {
	store := &futurestore.FutureStore{}
	snFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: "ServiceID",
		FuncName:  common.SetTimeoutFuncName,
		StateID:   "StateID",
		ID:        "ID",
	})
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(store), "Get",
			func(_ *futurestore.FutureStore, _ snfuture.ID) *snfuture.Future {
				return snFuture
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type fields struct {
		s     *futurestore.FutureStore
		delay time.Duration
	}
	type args struct {
		tenantID  string
		serviceID string
		sncFuture *snfuture.Future
	}
	var f fields
	var a args
	f.s = store
	f.delay = 1 * time.Second
	a.sncFuture = snFuture
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &MockExtFutureStore{
				s:     tt.fields.s,
				delay: tt.fields.delay,
			}
			if got := store.ClearResult(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClearResult() = %v, want %v", got, tt.want)
			}
		})
	}
}
