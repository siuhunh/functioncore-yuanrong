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

// Package functionrepohandler -
package functionrepohandler

import (
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/eventsource"
	"functioncore/pkg/autoscaler/instance"
	"functioncore/pkg/autoscaler/scaler"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types/functiontypes"
	mockUtils "functioncore/test/utils"
)

var (
	scaleMgr *scaler.ScaleMgr
)

func TestFunctionRepoHandler_HandleEvent(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
	}
	type args struct {
		event eventsource.Event
	}
	etcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte(mockUtils.ETCDCAEFuncValueForTest),
		PrevValue: []byte(mockUtils.ETCDCAEFuncValueForTest),
		Rev:       0,
	}

	nonEtcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte(mockUtils.ETCDCAEFuncValueForTest),
		PrevValue: []byte(mockUtils.ETCDFuncValueForTest),
		Rev:       0,
	}

	invalidEtcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte("invalid"),
		PrevValue: []byte("invalid"),
		Rev:       0,
	}

	instanceValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte(mockUtils.ETCDInsMetaValueForTest),
		PrevValue: nil,
		Rev:       0,
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case 1 test FunctionRepoEventKind add event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Added,
					Kind:       utils.FunctionRepoEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     etcdValue,
					StartTime:  time.Now(),
				},
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(scaler.GetScaleManager, func() *scaler.ScaleMgr {
						return scaleMgr
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(scaleMgr), "UpdateScalerGroup",
						func(_ *scaler.ScaleMgr, functionSvcID string, asbresConfig functiontypes.AsbResConfig) {
							return
						}),
					gomonkey.ApplyFunc(instance.UpdateInfo, func(funcSvcID string, funcMetadata *functiontypes.FunctionEtcdInfo) {
						return
					}),
				})
				return patches
			},
		},
		{
			"case 1 test FunctionRepoEventKind delete event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Deleted,
					Kind:       utils.FunctionRepoEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     etcdValue,
					StartTime:  time.Now(),
				},
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(scaler.GetScaleManager, func() *scaler.ScaleMgr {
						return scaleMgr
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(scaleMgr), "UpdateScalerGroup",
						func(_ *scaler.ScaleMgr, functionSvcID string, asbresConfig functiontypes.AsbResConfig) {
							return
						}),
					gomonkey.ApplyFunc(instance.UpdateInfo, func(funcSvcID string, funcMetadata *functiontypes.FunctionEtcdInfo) {
						return
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(scaleMgr), "RemoveScaler",
						func(_ *scaler.ScaleMgr, functionSvcID string) *scaler.ScaleMgr {
							return &scaler.ScaleMgr{}
						}),
					gomonkey.ApplyFunc(instance.DeleteInfo, func(funcMetadata *functiontypes.FunctionEtcdInfo) {
						return
					}),
				})
				return patches
			},
		},
		{
			"case 1 test InstanceMetadataEventKind add event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Added,
					Kind:       utils.InstanceMetadataEventKind,
					KeyRef:     mockUtils.ETCDInsMetaKeyForTest,
					Object:     instanceValue,
					StartTime:  time.Now(),
				},
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{}, nil
					}),
				})
				return patches
			},
		},
		{
			"case 1 test InstanceMetadataEventKind delete event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Deleted,
					Kind:       utils.InstanceMetadataEventKind,
					KeyRef:     mockUtils.ETCDInsMetaKeyForTest,
					Object:     instanceValue,
					StartTime:  time.Now(),
				},
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{}, nil
					}),
				})
				return patches
			},
		},
		{
			"case 2 test invalid event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Deleted,
					Kind:       "invalid",
					KeyRef:     mockUtils.ETCDInsMetaKeyForTest,
					Object:     instanceValue,
					StartTime:  time.Now(),
				},
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{}, nil
					}),
				})
				return patches
			},
		},
		{
			"case 2 test invalid event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Deleted,
					Kind:       utils.InstanceMetadataEventKind,
					KeyRef:     "invalid",
					Object:     instanceValue,
					StartTime:  time.Now(),
				},
			},
			true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{}, nil
					}),
				})
				return patches
			},
		},
		{
			"case 1 test FunctionRepoEventKind delete event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Deleted,
					Kind:       utils.FunctionRepoEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     invalidEtcdValue,
					StartTime:  time.Now(),
				},
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(scaler.GetScaleManager, func() *scaler.ScaleMgr {
						return scaleMgr
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(scaleMgr), "UpdateScalerGroup",
						func(_ *scaler.ScaleMgr, functionSvcID string, asbresConfig functiontypes.AsbResConfig) {
							return
						}),
					gomonkey.ApplyFunc(instance.UpdateInfo, func(funcSvcID string, funcMetadata *functiontypes.FunctionEtcdInfo) {
						return
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(scaleMgr), "RemoveScaler",
						func(_ *scaler.ScaleMgr, functionSvcID string) *scaler.ScaleMgr {
							return &scaler.ScaleMgr{}
						}),
					gomonkey.ApplyFunc(instance.DeleteInfo, func(funcMetadata *functiontypes.FunctionEtcdInfo) {
						return
					}),
				})
				return patches
			},
		},
		{
			"case 1 test non CAE FunctionRepoEventKind delete event",
			fields{
				EtcdClient: &etcd3.EtcdClient{},
			},
			args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Deleted,
					Kind:       utils.FunctionRepoEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     nonEtcdValue,
					StartTime:  time.Now(),
				},
			},
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(scaler.GetScaleManager, func() *scaler.ScaleMgr {
						return scaleMgr
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(scaleMgr), "UpdateScalerGroup",
						func(_ *scaler.ScaleMgr, functionSvcID string, asbresConfig functiontypes.AsbResConfig) {
							return
						}),
					gomonkey.ApplyFunc(instance.UpdateInfo, func(funcSvcID string, funcMetadata *functiontypes.FunctionEtcdInfo) {
						return
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(scaleMgr), "RemoveScaler",
						func(_ *scaler.ScaleMgr, functionSvcID string) *scaler.ScaleMgr {
							return &scaler.ScaleMgr{}
						}),
					gomonkey.ApplyFunc(instance.DeleteInfo, func(funcMetadata *functiontypes.FunctionEtcdInfo) {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			handler := &FunctionRepoHandler{
				EtcdClient: tt.fields.EtcdClient,
			}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
