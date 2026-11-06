/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package apighandler
package apighandler

import (
	"reflect"
	"testing"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

func TestNewEventHandler(t *testing.T) {
	type args struct {
		etcdctl *etcd3.EtcdClient
	}
	tests := []struct {
		name string
		args args
		want *RouteHandler
	}{
		{
			name: "case_01 normal scenario",
			args: args{etcdctl: &etcd3.EtcdClient{}},
			want: &RouteHandler{
				EtcdClient: &etcd3.EtcdClient{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEventHandler(tt.args.etcdctl); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEventHandler() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAPIGHandler_HandleEvent(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
	}
	type args struct {
		event eventsource.Event
	}
	addEtcdValue := &etcd3.Event{
		Type: 0,
		Key:  "/sn/apig/address",
		Value: []byte(` {
            "apigIps" : [
                "192.168.1.1",
                "192.168.1.2",
                "192.168.1.3",
                "192.168.1.4"
            ]
        }`),
		PrevValue: []byte(mockutils.ETCDFuncValueForTest),
		Rev:       0,
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case_01 invalid event kind",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind: "unkown",
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		}, {
			name:   "case_02 invalid apig metadata value",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:   utils.APIGEventKind,
				KeyRef: "/sn/apig/address",
				Object: nil,
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		}, {
			name:   "case_03 add apig metadata key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.APIGEventKind,
				EventType: eventsource.Added,
				KeyRef:    "/sn/apig/address",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		}, {
			name:   "case_03 delete apig metadata key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.APIGEventKind,
				EventType: eventsource.Deleted,
				KeyRef:    "/sn/apig/address",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handler := &RouteHandler{
				EtcdClient: tt.fields.EtcdClient,
			}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
