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

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

func TestEmptyExtFutureStore_SetResult(t *testing.T) {
	type args struct {
		tenantID  string
		serviceID string
		sncFuture *snfuture.Future
		result    *common.Response
	}
	var a args
	tests := []struct {
		name string
		args args
		want common.TaskError
	}{
		{"case1", a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &EmptyExtFutureStore{}
			if got := store.SetResult(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture, tt.args.result); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEmptyExtFutureStore_ClearResult(t *testing.T) {
	type args struct {
		tenantID  string
		serviceID string
		sncFuture *snfuture.Future
	}
	var a args
	tests := []struct {
		name string
		args args
		want common.TaskError
	}{
		{"case1", a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &EmptyExtFutureStore{}
			if got := store.ClearResult(tt.args.tenantID, tt.args.serviceID, tt.args.sncFuture); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ClearResult() = %v, want %v", got, tt.want)
			}
		})
	}
}
