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

// Package resourceusage deal the resource usage in common
package resourceusage

import (
	"math"
	"reflect"
	"testing"
	"time"
)

func TestResourceList_GetLatestData(t *testing.T) {
	tests := []struct {
		name string
		rl   ResourceList
		want *Resource
	}{
		{
			"test 1",
			ResourceList{
				{
					CPUUsage: 100,
				},
			},
			&Resource{
				CPUUsage: 100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rl.GetLatestData(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetLatestData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResourceList_GetLatestDataTime(t *testing.T) {
	tests := []struct {
		name string
		rl   ResourceList
		want int64
	}{
		{
			"test 1",
			ResourceList{
				{
					CPUUsage:   100,
					ReportTime: 100,
				},
			},
			100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.rl.GetLatestDataTime(); got != tt.want {
				t.Errorf("GetLatestDataTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResourceList_GetAvgDataInTime(t *testing.T) {
	now := time.Now().Unix()
	rl := ResourceList([]Resource{
		{
			CPUUsage:    2.2,
			MemoryUsage: 2.2,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 16,
		},
		{
			CPUUsage:    1.1,
			MemoryUsage: 1.1,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 2,
		},
		{
			CPUUsage:    2.2,
			MemoryUsage: 2.2,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 1,
		},
	})
	res := rl.GetAvgDataInTime(now, 15)
	if (math.Abs(res.CPUUsage)-1.65 >= 0.001) || (math.Abs(res.MemoryUsage)-1.65 >= 0.001) ||
		res.MemoryLimit != 5 || res.CPULimit != 5 {
		t.Errorf("failed to TestResourceList_GetAvgDataInTime")
	}

	rl = ResourceList([]Resource{
		{
			CPUUsage:    2.2,
			MemoryUsage: 2.2,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 16,
		},
		{
			CPUUsage:    1.1,
			MemoryUsage: 1.1,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 16,
		},
		{
			CPUUsage:    2.2,
			MemoryUsage: 2.2,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 16,
		},
	})
	res = rl.GetAvgDataInTime(now, 15)
	if res != nil {
		t.Errorf("failed to TestResourceList_GetAvgDataInTime")
	}

	rl = ResourceList([]Resource{
		{
			CPUUsage:    2.2,
			MemoryUsage: 2.2,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 16,
		},
		{
			CPUUsage:    1.1,
			MemoryUsage: 1.1,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now - 16,
		},
		{
			CPUUsage:    0,
			MemoryUsage: 2.2,
			CPULimit:    5,
			MemoryLimit: 5,
			ReportTime:  now,
		},
	})
	res = rl.GetAvgDataInTime(now, 15)
	if res != nil {
		t.Errorf("failed to TestResourceList_GetAvgDataInTime")
	}

}
