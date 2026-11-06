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

// Package workermanagereventhandler -
package caeavailableclustereventhandler

import (
	"functioncore/pkg/autoscaler/cache"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	mockUtils "functioncore/test/utils"
)

func TestWorkerCaeAvailableCluster_HandleEvent(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
	}
	type args struct {
		event eventsource.Event
	}

	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case 1 test succeed",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Deleted,
					Kind:       utils.CaeAvailableClusterEventKind,
					KeyRef:     mockUtils.ETCDCaeAvailableClusterKeyForTest,
					StartTime:  time.Now(),
				},
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
		{
			"case 2 test failed",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.FunctionRepoEventKind,
					KeyRef:     mockUtils.ETCDCaeAvailableClusterKeyForTest,
					StartTime:  time.Now(),
				},
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
		{
			"case 3 test invalid",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.CaeAvailableClusterEventKind,
					KeyRef:     mockUtils.ETCDInvalidCaeAvailableClusterKeyForTest,
					StartTime:  time.Now(),
				},
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
		{
			"case 4 test value invalid",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.CaeAvailableClusterEventKind,
					KeyRef:     mockUtils.ETCDCaeAvailableClusterKeyForTest,
					StartTime:  time.Now(),
				},
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handler := &CaeAvailableClusterHandler{
				EtcdClient: tt.fields.EtcdClient,
			}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestCaeAvailableClusterHandler_HandleEventResult(t *testing.T) {
	Convey("TestCaeAvailableClusterHandler_HandleEventResult", t, func() {
		handler := &CaeAvailableClusterHandler{
			EtcdClient: &etcd3.EtcdClient{},
		}
		// Add
		etcdAddValue := &etcd3.Event{
			Type:      0,
			Key:       "",
			Value:     []byte(mockUtils.ETCDCaeAvailableClusterValueForTest),
			PrevValue: []byte(mockUtils.ETCDCaeAvailableClusterValueForTest),
			Rev:       0,
		}
		eventAdd := eventsource.Event{
			SourceType: "",
			EventType:  eventsource.Added,
			Kind:       utils.CaeAvailableClusterEventKind,
			KeyRef:     mockUtils.ETCDCaeAvailableClusterKeyForTest,
			Object:     etcdAddValue,
			StartTime:  time.Now(),
		}
		err := handler.HandleEvent(eventAdd)
		So(err, ShouldBeNil)
		funcSvcID := utils.GetFuncSvcIDByCaeAvailableClusterKey(mockUtils.ETCDCaeAvailableClusterKeyForTest)
		clusters, ok := cache.GetInstance().GetCaeAvailableCluster(funcSvcID)
		So(ok, ShouldBeTrue)
		So(clusters, ShouldResemble, []string{"cluster001", "cluster002"})

		// Delete
		event := eventsource.Event{
			SourceType: "",
			EventType:  eventsource.Deleted,
			Kind:       utils.CaeAvailableClusterEventKind,
			KeyRef:     mockUtils.ETCDCaeAvailableClusterKeyForTest,
			StartTime:  time.Now(),
		}
		deleteErr := handler.HandleEvent(event)
		So(deleteErr, ShouldBeNil)
		_, exist := cache.GetInstance().GetCaeAvailableCluster(funcSvcID)
		So(exist, ShouldBeFalse)
	})
}
