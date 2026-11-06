/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package scalehandler -
package scalehandler

import (
	"sync"
	"testing"

	"functioncore/pkg/common/resourceusage"
)

func TestResourceMetric_getResourceByFuncSvcID(t *testing.T) {
	type fields struct {
		resource      map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList
		resourceMutex sync.RWMutex
	}
	type args struct {
		funcSvcID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantLen int
	}{
		{
			"test get resource failed ",
			fields{
				resource: map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList{
					"func1": {"podA": resourceusage.BridgeResourceList{}, "podB": resourceusage.BridgeResourceList{}},
				},
				resourceMutex: sync.RWMutex{},
			},
			args{
				funcSvcID: "func2",
			},
			0,
		},
		{
			"test get resource",
			fields{
				resource: map[resourceusage.FuncSvcID]map[resourceusage.PodName]resourceusage.BridgeResourceList{
					"func1": {"podA": resourceusage.BridgeResourceList{}, "podB": resourceusage.BridgeResourceList{}},
				},
				resourceMutex: sync.RWMutex{},
			},
			args{
				funcSvcID: "func1",
			},
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resourceMetric := &ResourceMetric{
				resourceMutex: tt.fields.resourceMutex,
			}
			resourceMetric.updateResource(tt.fields.resource)
			if got := resourceMetric.getResourceByFuncSvcID(tt.args.funcSvcID); len(got) != tt.wantLen {
				t.Errorf("getResourceByFuncSvcID() = %v, want %v", got, tt.wantLen)
			}
		})
	}
}

func Test_initResourceMetricCache(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			"test initResource",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := initResourceMetricCache(); got.resource == nil {
				t.Errorf("initResourceMetricCache() = %v, want %v", got, got.resource)
			}
		})
	}
}
