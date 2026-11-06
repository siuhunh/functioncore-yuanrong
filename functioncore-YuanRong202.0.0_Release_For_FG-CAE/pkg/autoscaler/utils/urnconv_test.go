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

// Package utils -
package utils

import (
	"reflect"
	"testing"
)

func TestGetFunctionInfoByURN(t *testing.T) {
	type args struct {
		urn string
	}
	tests := []struct {
		name string
		args args
		want FunctionInfo
	}{
		{
			name: "test001",
			args: args{
				urn: "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms:latest",
			},
			want: FunctionInfo{
				Business: "yrk",
				Tenant:   "fb6a00ff7ae54a5fbb8ff855d0841d00",
				FuncName: "0@default@puresleep-java-10ms",
				Version:  "latest",
			},
		},
		{
			name: "test001",
			args: args{
				urn: "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms",
			},
			want: FunctionInfo{
				Business: "yrk",
				Tenant:   "fb6a00ff7ae54a5fbb8ff855d0841d00",
				FuncName: "0@default@puresleep-java-10ms",
			},
		},
		{
			name: "test001",
			args: args{
				urn: "",
			},
			want: FunctionInfo{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFunctionInfoByURN(tt.args.urn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFunctionInfoByURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_acquireBrief(t *testing.T) {
	type args struct {
		business string
		tenant   string
	}
	var a args
	a.business = "yrkkkk"
	a.tenant = "6d5b16f6ef0e4b7d93@8d5035356aa378"
	tests := []struct {
		name string
		args args
		want string
	}{
		{"case1", a, "yrkk6d5b8d50"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := acquireBrief(tt.args.business, tt.args.tenant); got != tt.want {
				t.Errorf("acquireBrief() = %v, want %v", got, tt.want)
			}
		})
	}
}
