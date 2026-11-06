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

// Package tenantmetadatahandler
package tenantmetadatahandler

import (
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func TestNewEventHandler(t *testing.T) {
	type args struct {
		etcdctl *etcd3.EtcdClient
	}
	tests := []struct {
		name string
		args args
		want *TenantMetadataHandler
	}{
		{
			name: "case_01 normal scenario",
			args: args{etcdctl: &etcd3.EtcdClient{}},
			want: &TenantMetadataHandler{
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

func TestTenantMetadataHandler_HandleEvent(t *testing.T) {
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
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name:   "case_01 invalid event kind",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind: "unkown",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		}, {
			name:   "case_02 invalid tenant metadata key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:   utils.TenantMetadataEventKind,
				KeyRef: "/sn/quota/mistake",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		}, {
			name:   "case_03 default tenant metadata key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:   utils.TenantMetadataEventKind,
				KeyRef: "/sn/quota/cluster/cluster001/default/instancemetadata",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(handleDefaultEvent,
						func(event eventsource.Event) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: false,
		}, {
			name:   "case_04 tenant metadata key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:   utils.TenantMetadataEventKind,
				KeyRef: "/sn/quota/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/instancemetadata",
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(handleTenantMetadataEvent,
						func(event eventsource.Event, tenantID string) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handler := &TenantMetadataHandler{
				EtcdClient: tt.fields.EtcdClient,
			}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_handleTenantMetadataEvent(t *testing.T) {
	type args struct {
		event    eventsource.Event
		tenantID string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "case_01 invalid event",
			args: args{
				event: eventsource.Event{
					Object: nil,
				},
				tenantID: "test",
			},
			wantErr: true,
		}, {
			name: "case_02 unmarshal failed",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Added,
					KeyRef:    mockUtils.ETCDTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(""),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
				tenantID: "test",
			},
			wantErr: true,
		}, {
			name: "case_03 add event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Added,
					KeyRef:    mockUtils.ETCDTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(mockUtils.ETCDTenantMetaValueForTest),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
				tenantID: "test",
			},
			wantErr: false,
		}, {
			name: "case_04 update event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Updated,
					KeyRef:    mockUtils.ETCDTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(mockUtils.ETCDTenantMetaValueForTest),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
				tenantID: "test",
			},
			wantErr: false,
		}, {
			name: "case_05 delete event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Deleted,
					KeyRef:    mockUtils.ETCDTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     nil,
						PrevValue: []byte(mockUtils.ETCDTenantMetaValueForTest),
						Rev:       0,
					},
					StartTime: time.Now(),
				},
				tenantID: "test",
			},
			wantErr: false,
		}, {
			name: "case_06 unkown event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: "unkown",
					KeyRef:    mockUtils.ETCDTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(mockUtils.ETCDTenantMetaValueForTest),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
				tenantID: "test",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handleTenantMetadataEvent(tt.args.event, tt.args.tenantID); (err != nil) != tt.wantErr {
				t.Errorf("handleTenantMetadataEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handleDefaultEvent(t *testing.T) {
	type args struct {
		event eventsource.Event
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "case_01 invalid event",
			args: args{
				event: eventsource.Event{
					Object: nil,
				},
			},
			wantErr: true,
		}, {
			name: "case_02 unmarshal failed",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Added,
					KeyRef:    mockUtils.ETCDDefaultTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(""),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
			},
			wantErr: true,
		}, {
			name: "case_03 add event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Added,
					KeyRef:    mockUtils.ETCDDefaultTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(mockUtils.ETCDTenantMetaValueForTest),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
			},
			wantErr: false,
		}, {
			name: "case_04 update event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Updated,
					KeyRef:    mockUtils.ETCDDefaultTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(mockUtils.ETCDTenantMetaValueForTest),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
			},
			wantErr: false,
		}, {
			name: "case_05 delete event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: eventsource.Deleted,
					KeyRef:    mockUtils.ETCDDefaultTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     nil,
						PrevValue: []byte(mockUtils.ETCDTenantMetaValueForTest),
						Rev:       0,
					},
					StartTime: time.Now(),
				},
			},
			wantErr: false,
		}, {
			name: "case_06 unkown event",
			args: args{
				event: eventsource.Event{
					Kind:      utils.TenantMetadataEventKind,
					EventType: "unkown",
					KeyRef:    mockUtils.ETCDDefaultTenantMetaKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     []byte(mockUtils.ETCDTenantMetaValueForTest),
						PrevValue: nil,
						Rev:       0,
					},
					StartTime: time.Now(),
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handleDefaultEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("handleDefaultEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
