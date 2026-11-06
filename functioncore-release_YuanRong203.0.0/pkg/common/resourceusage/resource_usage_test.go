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
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	mockUtils "functioncore/test/utils"
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

func TestPredictResourceList_AggregateSumPredictResource(t *testing.T) {
	tests := []struct {
		name string
		l    PredictResourceList
		want PredictResource
	}{
		{
			name: "test1",
			l: PredictResourceList{{
				CPUUtil:    1,
				MemoryUtil: 1,
				Timestamp:  123,
			}, {
				CPUUtil:    2,
				MemoryUtil: 2,
				Timestamp:  123,
			},
			},
			want: PredictResource{
				CPUUtil:    3,
				MemoryUtil: 3,
				Timestamp:  123,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.l.AggregateSumPredictResource(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AggregateSumPredictResource() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPredictResourceList_GetAvgData(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		l    PredictResourceList
		want PredictResource
	}{
		{
			name: "test1",
			l: PredictResourceList{{
				CPUUtil:    1,
				MemoryUtil: 1,
				Timestamp:  now.Unix(),
			}, {
				CPUUtil:    2,
				MemoryUtil: 2,
				Timestamp:  now.Unix(),
			},
			},
			want: PredictResource{
				CPUUtil:    1.5,
				MemoryUtil: 1.5,
				Timestamp:  now.Truncate(time.Minute).Unix(),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.l.GetAvgData(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetAvgData() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPredictResourceList_AggregateMinutePredictResource(t *testing.T) {
	now := time.Now()
	tests := []struct {
		name string
		l    PredictResourceList
		want PredictResourceList
	}{
		{
			name: "test1",
			l: PredictResourceList{{
				CPUUtil:    1,
				MemoryUtil: 1,
				Timestamp:  now.Unix(),
			}, {
				CPUUtil:    2,
				MemoryUtil: 2,
				Timestamp:  now.Unix(),
			}, {
				CPUUtil:    3,
				MemoryUtil: 3,
				Timestamp:  now.Add(70 * time.Second).Unix(),
			},
			},
			want: PredictResourceList{{
				CPUUtil:    1.5,
				MemoryUtil: 1.5,
				Timestamp:  now.Truncate(time.Minute).Unix(),
			}, {
				CPUUtil:    3,
				MemoryUtil: 3,
				Timestamp:  now.Add(70 * time.Second).Truncate(time.Minute).Unix(),
			},
			},
		},
	}
	convey.Convey("TestPredictResourceList_AggregateMinutePredictResource", t, func() {
		resource := tests[0].l.AggregateMinutePredictResource()
		r := make(map[int64]PredictResource)
		w := make(map[int64]PredictResource)
		for _, predictResource := range resource {
			r[predictResource.Timestamp] = predictResource
		}
		for _, predictResource := range tests[0].want {
			w[predictResource.Timestamp] = predictResource
		}
		convey.So(r, convey.ShouldResemble, w)
	})
}

func TestResourceList_GetAvgExtendSystemDataInTime(t *testing.T) {

	rl := ResourceList{}
	resource := rl.GetAvgExtendSystemDataInTime(time.Now().Unix(), 15)
	if resource != nil {
		t.Errorf("failed to test GetAvgExtendSystemDataInTime")
	}

	rl = ResourceList{
		{
			ReportTime:                    time.Now().Unix() - 10,
			ReceiveBytesPerSecond:         100,
			TransmitBytesPerSecond:        110,
			FileSystemWriteBytesPerSecond: 120,
			FileSystemReadBytesPerSecond:  130,
		},
		{
			ReportTime:                    time.Now().Unix() - 10,
			ReceiveBytesPerSecond:         100,
			TransmitBytesPerSecond:        110,
			FileSystemWriteBytesPerSecond: 120,
			FileSystemReadBytesPerSecond:  130,
		},
	}
	resource = rl.GetAvgExtendSystemDataInTime(time.Now().Unix(), 15)
	if resource == nil || resource.ReceiveBytesPerSecond != 100 || resource.TransmitBytesPerSecond != 110 ||
		resource.FileSystemReadBytesPerSecond != 130 || resource.FileSystemWriteBytesPerSecond != 120 {
		t.Errorf("failed to test GetAvgExtendSystemDataInTime")
	}

}

func TestPredictResourceListMap_Convert2List4Redis(t *testing.T) {
	convey.Convey("test Convert2List4Redis", t, func() {
		convey.Convey("case1: success", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return []byte("aaa"), nil
				}),
			})
			defer patches.ResetAll()

			lm := PredictResourceListMap{
				"key1": {{}},
				"key2": {{}},
			}
			list := lm.Convert2List4Redis()
			convey.So(len(list), convey.ShouldEqual, 4)
		})
		convey.Convey("case2: failed to marshal", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return nil, errors.New("error")
				}),
			})
			defer patches.ResetAll()

			lm := PredictResourceListMap{
				"key1": {{}},
				"key2": {{}},
			}
			list := lm.Convert2List4Redis()
			convey.So(len(list), convey.ShouldEqual, 0)
		})
	})
}

func TestResourceList_Convert2PredictResourceList(t *testing.T) {
	convey.Convey("test Convert2PredictResourceList", t, func() {
		convey.Convey("case1: success", func() {
			rl := ResourceList{
				{
					CPUUsage:    1.0,
					MemoryUsage: 2.0,
					ReportTime:  3,
				},
				{
					CPUUsage:    4.0,
					MemoryUsage: 5.0,
					ReportTime:  6,
				},
			}
			list := rl.Convert2PredictResourceList()
			convey.So(len(list), convey.ShouldEqual, 2)
		})
	})
}
