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

package poolmanager

import (
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// test for new ResourceRequirementsBuilder
func Test_newResourceRequirementsBuilder(t *testing.T) {
	tests := []struct {
		name string
		want *resourceRequirementsBuilder
	}{
		{
			name: "case_01_normal_scenario",
			want: &resourceRequirementsBuilder{
				data: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{},
					Limits:   corev1.ResourceList{},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newResourceRequirementsBuilder(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newResourceRequirementsBuilder() = %v, want %v", got, tt.want)
			}
		})
	}
}

// test for set cpu
func Test_resourceRequirementsBuilder_setCPU(t *testing.T) {
	type fields struct {
		data corev1.ResourceRequirements
	}
	type args struct {
		request int64
		limit   int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *resourceRequirementsBuilder
	}{
		{
			name: "case_01_normal_scenario",
			fields: fields{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{},
					Requests: map[corev1.ResourceName]resource.Quantity{},
				},
			},
			args: args{
				request: int64(2),
				limit:   int64(4),
			},
			want: &resourceRequirementsBuilder{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%dm", int64(4)))},
					Requests: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%dm", int64(2)))},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &resourceRequirementsBuilder{
				data: tt.fields.data,
			}
			b.setCPU(tt.args.request, tt.args.limit)
			if !reflect.DeepEqual(b, tt.want) {
				t.Errorf("setCPU() = %v, want %v", b, tt.want)
			}
		})
	}
}

// test for set mem
func Test_resourceRequirementsBuilder_setMemory(t *testing.T) {
	type fields struct {
		data corev1.ResourceRequirements
	}
	type args struct {
		request int64
		limit   int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *resourceRequirementsBuilder
	}{
		{
			name: "case_01_normal_scenario",
			fields: fields{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{},
					Requests: map[corev1.ResourceName]resource.Quantity{},
				},
			},
			args: args{
				request: int64(4),
				limit:   int64(8),
			},
			want: &resourceRequirementsBuilder{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{"memory": resource.MustParse(fmt.Sprintf("%dMi", int64(8)))},
					Requests: map[corev1.ResourceName]resource.Quantity{"memory": resource.MustParse(fmt.Sprintf("%dMi", int64(4)))},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &resourceRequirementsBuilder{
				data: tt.fields.data,
			}
			b.setMemory(tt.args.request, tt.args.limit)
			if !reflect.DeepEqual(b, tt.want) {
				t.Errorf("setMemory() = %v, want %v", b, tt.want)
			}
		})
	}
}

// test for set ephemeral storage
func Test_resourceRequirementsBuilder_setEphemeralStorage(t *testing.T) {
	type fields struct {
		data corev1.ResourceRequirements
	}
	type args struct {
		request int64
		limit   int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *resourceRequirementsBuilder
	}{
		{
			name: "case_01_normal_scenario",
			fields: fields{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{},
					Requests: map[corev1.ResourceName]resource.Quantity{},
				},
			},
			args: args{
				request: int64(4),
				limit:   int64(8),
			},
			want: &resourceRequirementsBuilder{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{"ephemeral-storage": resource.MustParse(fmt.Sprintf("%dMi", int64(8)))},
					Requests: map[corev1.ResourceName]resource.Quantity{"ephemeral-storage": resource.MustParse(fmt.Sprintf("%dMi", int64(4)))},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &resourceRequirementsBuilder{
				data: tt.fields.data,
			}
			b.setEphemeralStorage(tt.args.request, tt.args.limit)
			if !reflect.DeepEqual(b, tt.want) {
				t.Errorf("setEphemeralStorage() = %v, want %v", b, tt.want)
			}
		})
	}
}

// test for set custom resource
func Test_resourceRequirementsBuilder_setCustomResource(t *testing.T) {
	type fields struct {
		data corev1.ResourceRequirements
	}
	type args struct {
		customResource map[string]int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *resourceRequirementsBuilder
	}{
		{
			name: "case_01_normal_scenario",
			fields: fields{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{},
					Requests: map[corev1.ResourceName]resource.Quantity{},
				},
			},
			args: args{
				customResource: map[string]int64{"log-size": int64(8)},
			},
			want: &resourceRequirementsBuilder{
				data: corev1.ResourceRequirements{
					Limits:   map[corev1.ResourceName]resource.Quantity{"log-size": resource.MustParse(fmt.Sprintf("%d", int64(8)))},
					Requests: map[corev1.ResourceName]resource.Quantity{"log-size": resource.MustParse(fmt.Sprintf("%d", int64(8)))},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &resourceRequirementsBuilder{
				data: tt.fields.data,
			}
			b.setCustomResource(tt.args.customResource)
			if !reflect.DeepEqual(b, tt.want) {
				t.Errorf("setCustomResource() = %v, want %v", b, tt.want)
			}
		})
	}
}

// test for build resource requirements
func Test_resourceRequirementsBuilder_build(t *testing.T) {
	type fields struct {
		data corev1.ResourceRequirements
	}
	tests := []struct {
		name   string
		fields fields
		want   corev1.ResourceRequirements
	}{
		{
			name: "case_01_normal_scenario",
			fields: fields{
				data: corev1.ResourceRequirements{},
			},
			want: corev1.ResourceRequirements{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &resourceRequirementsBuilder{
				data: tt.fields.data,
			}
			if got := b.build(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("build() = %v, want %v", got, tt.want)
			}
		})
	}
}
