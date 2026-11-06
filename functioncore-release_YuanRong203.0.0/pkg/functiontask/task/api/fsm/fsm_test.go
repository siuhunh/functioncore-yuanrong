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
package fsm

import (
	"reflect"
	"testing"

	"functioncore/pkg/functiontask/task/taskcontext"
)

func Test_resultState_Result(t *testing.T) {
	type fields struct {
		baseState baseState
		result    taskcontext.Serializable
	}
	var f fields
	var result taskcontext.Serializable
	var want taskcontext.Serializable
	f.result = result
	tests := []struct {
		name   string
		fields fields
		want   taskcontext.Serializable
	}{
		{"case1", f, want},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &resultState{
				baseState: tt.fields.baseState,
				result:    tt.fields.result,
			}
			if got := s.Result(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Result() = %v, want %v", got, tt.want)
			}
		})
	}
}
