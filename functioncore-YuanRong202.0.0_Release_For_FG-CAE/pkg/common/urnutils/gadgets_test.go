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

package urnutils

import (
	"reflect"
	"testing"
)

func TestComplexFuncName_GetSvcIDWithPrefix(t *testing.T) {
	tests := []struct {
		name   string
		fields ComplexFuncName
		want   string
	}{
		{
			name: "normal",
			fields: ComplexFuncName{
				prefix:    ServiceIDPrefix,
				ServiceID: "absserviceid",
				FuncName:  "absFuncName",
			},
			want: "0-absserviceid",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ComplexFuncName{
				prefix:    tt.fields.prefix,
				ServiceID: tt.fields.ServiceID,
				FuncName:  tt.fields.FuncName,
			}
			if got := c.GetSvcIDWithPrefix(); got != tt.want {
				t.Errorf("GetSvcIDWithPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComplexFuncName_ParseFrom(t *testing.T) {
	type args struct {
		name string
	}
	tests := []struct {
		name string
		args args
		want *ComplexFuncName
	}{
		{
			name: "normal",
			args: args{
				name: "0-absserviceid-absFunc-Name",
			},
			want: &ComplexFuncName{
				prefix:    ServiceIDPrefix,
				ServiceID: "absserviceid",
				FuncName:  "absFunc-Name",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ComplexFuncName{}
			if got := c.ParseFrom(tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseFrom() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestComplexFuncName_String(t *testing.T) {
	tests := []struct {
		name   string
		fields ComplexFuncName
		want   string
	}{
		{
			name: "normal",
			fields: ComplexFuncName{
				prefix:    ServiceIDPrefix,
				ServiceID: "absserviceid",
				FuncName:  "absFunc-Name",
			},
			want: "0-absserviceid-absFunc-Name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ComplexFuncName{
				prefix:    tt.fields.prefix,
				ServiceID: tt.fields.ServiceID,
				FuncName:  tt.fields.FuncName,
			}
			if got := c.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewComplexFuncName(t *testing.T) {
	type args struct {
		svcID    string
		funcName string
	}
	tests := []struct {
		name string
		args args
		want *ComplexFuncName
	}{
		{
			name: "normal",
			args: args{
				svcID:    "absserviceid",
				funcName: "absFunc-Name",
			},
			want: &ComplexFuncName{
				prefix:    ServiceIDPrefix,
				ServiceID: "absserviceid",
				FuncName:  "absFunc-Name",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewComplexFuncName(tt.args.svcID, tt.args.funcName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewComplexFuncName() = %v, want %v", got, tt.want)
			}
		})
	}
}
