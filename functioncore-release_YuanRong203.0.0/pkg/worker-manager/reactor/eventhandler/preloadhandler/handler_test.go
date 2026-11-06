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

// Package preloadhandler
package preloadhandler

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/preload"
	"functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

func TestNewEventHandler(t *testing.T) {
	want := &Handler{
		EtcdClient: &etcd3.EtcdClient{},
	}
	if got := NewEventHandler(&etcd3.EtcdClient{}); !reflect.DeepEqual(got, want) {
		t.Errorf("NewEventHandler() = %v, want %v", got, want)
	}
}

func TestHandleEvent(t *testing.T) {
	urnutils.SetSeparator("@")
	type fields struct {
		EtcdClient *etcd3.EtcdClient
	}
	type args struct {
		event eventsource.Event
	}
	addEtcdValue := &etcd3.Event{
		Type:      0,
		Key:       "/cold-start-preload/business/yrk/tenant/123/function/0@default@func1/version/latest",
		Value:     []byte(`{ "bucketID": "1234"}`),
		PrevValue: []byte(mockutils.ETCDFuncValueForTest),
		Rev:       0,
	}
	patches := gomonkey.ApplyFunc(preload.IsEnable,
		func() bool {
			return true
		})
	defer patches.Reset()
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
		},
		{
			name:   "case_02 invalid preload key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:   utils.PreloadEventKind,
				KeyRef: "/cold-start-preload-invalid",
				Object: nil,
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case_03 add function preload key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Added,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/function/0@default@func1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&preload.Client{}), "StartAddPreloadToAllNodes",
						func(_ *preload.Client, tenantID, typeStr string, preloadInfo config.PreloadInfo) {}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case_04 delete function preload key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Deleted,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/function/0@default@func1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&preload.Client{}), "StartDelPreloadOfALLNodes",
						func(_ *preload.Client, tenantID, typeStr, name string) {}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case_05 failed to unmarshal",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Added,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/function/0@default@func1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal error")
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case_06 invalid event type",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: "test",
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/function/0@default@func1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case_07 add layer preload key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Added,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/layer/layer1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&preload.Client{}), "StartAddPreloadToAllNodes",
						func(_ *preload.Client, tenantID, typeStr string, preloadInfo config.PreloadInfo) {}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case_08 delete preload layer key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Deleted,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/layer/layer1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&preload.Client{}), "StartDelPreloadOfALLNodes",
						func(_ *preload.Client, tenantID, typeStr, name string) {}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "case_09 delete no support key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Deleted,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/nosupport/layer1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case_10 add no support key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Added,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/nosupport/layer1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name:   "case_11 add function preload key",
			fields: fields{&etcd3.EtcdClient{}},
			args: args{event: eventsource.Event{
				Kind:      utils.PreloadEventKind,
				EventType: eventsource.Added,
				KeyRef:    "/cold-start-preload/business/yrk/tenant/123/layer/layer1/version/latest",
				Object:    addEtcdValue,
				StartTime: time.Now(),
			}},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&preload.Client{}), "StartAddPreloadToAllNodes",
						func(_ *preload.Client, tenantID, typeStr string, preloadInfo config.PreloadInfo) {}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handler := &Handler{
				EtcdClient: tt.fields.EtcdClient,
			}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestParserPreLoadKey(t *testing.T) {
	t.Run("invalid clod start preload", func(t *testing.T) {
		_, err := parserPreLoadKey("/xxx")
		assert.EqualError(t, err, "invalid clod start preload key /xxx in etcd")
	})
	t.Run("invalid clod start preload key length", func(t *testing.T) {
		errKey := "/cold-start-preload/business/yrk/tenant/123/layer/layer1/version"
		_, err := parserPreLoadKey(errKey)
		assert.EqualError(t, err, fmt.Sprintf("invalid clod start preload key %s length in etcd", errKey))
	})
	t.Run("layerPreloadKey", func(t *testing.T) {
		errKey := "/cold-start-preload/business/yrk/tenant/123/layer/layer1/version/latest"
		parse, err := parserPreLoadKey(errKey)
		assert.NoError(t, err)
		assert.Equal(t, "123", parse.projectID)
		assert.Equal(t, "layer1", parse.name)
		assert.Equal(t, "latest", parse.version)
		assert.Equal(t, "layer", parse.typeStr)
	})
	t.Run("funcPreloadKey", func(t *testing.T) {
		errKey := "/cold-start-preload/business/yrk/tenant/123/function/0@default@func1/version/latest"
		parse, err := parserPreLoadKey(errKey)
		assert.NoError(t, err)
		assert.Equal(t, "123", parse.projectID)
		assert.Equal(t, "0@default@func1", parse.name)
		assert.Equal(t, "latest", parse.version)
		assert.Equal(t, "function", parse.typeStr)
	})
}
