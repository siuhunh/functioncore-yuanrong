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
package taskcontext

import (
	"testing"

	"functioncore/pkg/functiontask/task/common"
)

func TestReqIDListImpl_ReqID(t *testing.T) {
	type fields struct {
		reqIDList common.ReqIDList
	}
	var f fields
	reqIDList := common.ReqIDList{}
	reqIDList = append(reqIDList, "id1")
	f.reqIDList = reqIDList
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"case1", f, "id1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ReqIDListImpl{
				reqIDList: tt.fields.reqIDList,
			}
			if got := c.ReqID(); got != tt.want {
				t.Errorf("ReqID() = %v, want %v", got, tt.want)
			}
		})
	}
}
