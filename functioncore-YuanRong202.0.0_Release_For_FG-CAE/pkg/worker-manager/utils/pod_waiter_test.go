/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package utils

import "testing"

func Test_newPodWaiter(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "case_01 normal scenario",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewPodWaiter(); got.ch == nil {
				t.Errorf("NewPodWaiter failed")
			}
		})
	}
}

func Test_podWaiter_setUnschedulable(t *testing.T) {
	type fields struct {
		ch            chan PodStatus
		unschedulable int32
	}
	type args struct {
		unschedulable bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "case_01 unschedulable is true",
			fields: fields{ch: make(chan PodStatus, 1), unschedulable: int32(2)},
			args:   args{true},
			want:   true,
		}, {
			name:   "case_02 unschedulable is false",
			fields: fields{ch: make(chan PodStatus, 1), unschedulable: int32(2)},
			args:   args{false},
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &PodWaiter{
				ch:            tt.fields.ch,
				unschedulable: tt.fields.unschedulable,
			}
			if got := c.SetUnschedulable(tt.args.unschedulable); got != tt.want {
				t.Errorf("SetUnschedulable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_podWaiter_isUnschedulable(t *testing.T) {
	type fields struct {
		ch            chan PodStatus
		unschedulable int32
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{ch: make(chan PodStatus, 1), unschedulable: int32(2)},
			want:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &PodWaiter{
				ch:            tt.fields.ch,
				unschedulable: tt.fields.unschedulable,
			}
			if got := c.IsUnschedulable(); got != tt.want {
				t.Errorf("IsUnSchedulable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_podWaiter_waitStatus(t *testing.T) {
	type fields struct {
		ch            chan PodStatus
		unschedulable int32
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name:   "case_01 normal scenario",
			fields: fields{ch: make(chan PodStatus, 1), unschedulable: int32(2)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &PodWaiter{
				ch:            tt.fields.ch,
				unschedulable: tt.fields.unschedulable,
			}
			if got := c.WaitStatus(); got == nil {
				t.Errorf("WaitStatus() failed")
			}
		})
	}
}
