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

// Package utils gets function services from URNs
package utils

import "testing"

func TestAccuracy_Equal(t *testing.T) {
	type args struct {
		a float64
		b float64
	}
	tests := []struct {
		name string
		ac   Accuracy
		args args
		want bool
	}{
		{name: "test001", ac: FloatAccuracy, args: args{a: 3.14001, b: 3.14002}, want: true},
		{name: "test002", ac: FloatAccuracy, args: args{a: 3.14001, b: 3.15001}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ac.Equal(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAccuracy_Greater(t *testing.T) {
	type args struct {
		a float64
		b float64
	}
	tests := []struct {
		name string
		ac   Accuracy
		args args
		want bool
	}{
		{name: "test001", ac: FloatAccuracy, args: args{a: 3.14002, b: 3.14001}, want: false},
		{name: "test002", ac: FloatAccuracy, args: args{a: 3.15001, b: 3.14001}, want: true},
		{name: "test003", ac: FloatAccuracy, args: args{a: 3.14001, b: 3.15001}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ac.Greater(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("Greater() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAccuracy_GreaterOrEqual(t *testing.T) {
	type args struct {
		a float64
		b float64
	}
	tests := []struct {
		name string
		ac   Accuracy
		args args
		want bool
	}{
		{name: "test001", ac: FloatAccuracy, args: args{a: 3.14002, b: 3.14001}, want: true},
		{name: "test002", ac: FloatAccuracy, args: args{a: 3.15001, b: 3.14001}, want: true},
		{name: "test003", ac: FloatAccuracy, args: args{a: 3.14001, b: 3.15001}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ac.GreaterOrEqual(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("GreaterOrEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAccuracy_Smaller(t *testing.T) {
	type args struct {
		a float64
		b float64
	}
	tests := []struct {
		name string
		ac   Accuracy
		args args
		want bool
	}{
		{name: "test001", ac: FloatAccuracy, args: args{a: 3.14002, b: 3.14001}, want: false},
		{name: "test002", ac: FloatAccuracy, args: args{a: 3.15001, b: 3.14001}, want: false},
		{name: "test003", ac: FloatAccuracy, args: args{a: 3.14001, b: 3.15001}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ac.Smaller(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("Smaller() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAccuracy_SmallerOrEqual(t *testing.T) {
	type args struct {
		a float64
		b float64
	}
	tests := []struct {
		name string
		ac   Accuracy
		args args
		want bool
	}{
		{name: "test001", ac: FloatAccuracy, args: args{a: 3.14002, b: 3.14001}, want: true},
		{name: "test002", ac: FloatAccuracy, args: args{a: 3.15001, b: 3.14001}, want: false},
		{name: "test003", ac: FloatAccuracy, args: args{a: 3.14001, b: 3.15001}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ac.SmallerOrEqual(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("SmallerOrEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}
