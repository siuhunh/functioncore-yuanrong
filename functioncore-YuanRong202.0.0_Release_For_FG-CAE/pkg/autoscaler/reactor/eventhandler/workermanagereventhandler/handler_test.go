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
package workermanagereventhandler

import (
	"testing"
	"time"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	mockUtils "functioncore/test/utils"
)

func TestWorkerManagerHandler_HandleEvent(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
	}
	type args struct {
		event eventsource.Event
	}
	clustermgr.CreateClusterMgr(make(<-chan struct{}), &config.Config{})
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
					EventType:  "",
					Kind:       utils.WorkManagerEventKind,
					KeyRef:     mockUtils.ETCDWorkerManagerKeyForTest,
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
					KeyRef:     mockUtils.ETCDWorkerManagerKeyForTest,
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
					Kind:       utils.WorkManagerEventKind,
					KeyRef:     mockUtils.ETCDInvalidWorkerManagerKeyForTest,
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
			handler := &WorkerManagerHandler{
				EtcdClient: tt.fields.EtcdClient,
			}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
