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

// Package functionrepohandler -
package functionrepohandler

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockutils "functioncore/test/utils"
)

func TestHandleTest(t *testing.T) {
	t.Run("TestFunctionRepoHandler_HandleEvent", TestFunctionRepoHandler_HandleEvent)
	t.Run("TestFunctionRepoHandler_HandleEvent_InstanceMeta", TestFunctionRepoHandler_HandleEvent_InstanceMeta)
	t.Run("TestNewEventHandler", TestNewEventHandler)
	t.Run("TestProcessFunctionDeleted", TestProcessFunctionDeleted)
}

func TestFunctionRepoHandler_HandleEvent(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		VpcHandler *vpcmanager.VpcHandler
	}
	type args struct {
		event eventsource.Event
	}
	etcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte(mockutils.ETCDFuncValueForTest),
		PrevValue: nil,
		Rev:       0,
	}
	deletedEtcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     nil,
		PrevValue: []byte(mockutils.ETCDFuncValueForTest),
		Rev:       0,
	}
	mockPodFetcher := &podfetcher.PodFetcher{}
	mockVpcHandler := &vpcmanager.VpcHandler{}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockutils.PatchesFunc
		wantErr   bool
	}{
		{
			name: "test001",
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name: "test002",
			args: args{
				event: eventsource.Event{
					Kind:      utils.FunctionRepoEventKind,
					StartTime: time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		// function repo add event
		{
			name: "test003",
			args: args{
				event: eventsource.Event{
					Kind:      utils.FunctionRepoEventKind,
					EventType: eventsource.Added,
					StartTime: time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{
							IdleRetryInfo: &podfetcher.IdleRetryList{},
						}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "CreateNewContext",
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test004",
			args: args{
				event: eventsource.Event{
					Kind:      utils.FunctionRepoEventKind,
					EventType: eventsource.Added,
					KeyRef:    mockutils.ETCDFuncKeyForTest,
					Object:    etcdValue,
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{
							IdleRetryInfo: &podfetcher.IdleRetryList{},
						}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "CreateNewContext",
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// function repo delete event
		{
			name:   "test005",
			fields: fields{VpcHandler: mockVpcHandler},
			args: args{
				event: eventsource.Event{
					Kind:      utils.FunctionRepoEventKind,
					EventType: eventsource.Deleted,
					KeyRef:    mockutils.ETCDFuncKeyForTest,
					Object:    deletedEtcdValue,
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{
							IdleRetryInfo: &podfetcher.IdleRetryList{},
						}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "CreateNewContext",
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockVpcHandler), "CleanPullTrigger",
						func(_ *vpcmanager.VpcHandler, funcName string) {
							return
						}),
					gomonkey.ApplyFunc(etcdhandler.DeleteInstanceByPrefixFromETCD,
						func(ctx context.Context, service *faasfunction.FunctionService) {
							return
						}),
					gomonkey.ApplyFunc(etcdhandler.OperateETCDWithRetry,
						func(ctx context.Context, path, value, operate string) error {
							return nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// function repo delete event with parse error
		{
			name:   "test005",
			fields: fields{VpcHandler: mockVpcHandler},
			args: args{
				event: eventsource.Event{
					Kind:      utils.FunctionRepoEventKind,
					EventType: eventsource.Deleted,
					KeyRef:    mockutils.ETCDFuncKeyForTest,
					Object: &etcd3.Event{
						Type:      0,
						Key:       "",
						Value:     nil,
						PrevValue: []byte(""),
						Rev:       0,
					},
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{
							IdleRetryInfo: &podfetcher.IdleRetryList{},
						}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "CreateNewContext",
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockVpcHandler), "CleanPullTrigger",
						func(_ *vpcmanager.VpcHandler, funcName string) {
							return
						}),
					gomonkey.ApplyFunc(etcdhandler.DeleteInstanceByPrefixFromETCD,
						func(ctx context.Context, service *faasfunction.FunctionService) {
							return
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &FunctionRepoHandler{
				EtcdClient: tt.fields.EtcdClient,
				VpcHandler: tt.fields.VpcHandler,
			}
			patches := tt.patchFunc()
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(50 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestFunctionRepoHandler_HandleEvent_InstanceMeta(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		VpcHandler *vpcmanager.VpcHandler
	}
	type args struct {
		event eventsource.Event
	}
	etcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte(mockutils.ETCDInsMetaValueForTest),
		PrevValue: nil,
		Rev:       0,
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockutils.PatchesFunc
		wantErr   bool
	}{
		// instance meta add event
		{
			name: "test001",
			args: args{
				event: eventsource.Event{
					Kind:      utils.InstanceMetadataEventKind,
					EventType: eventsource.Added,
					KeyRef:    mockutils.ETCDInsMetaKeyForTest,
					Object:    etcdValue,
					StartTime: time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{
							IdleRetryInfo: &podfetcher.IdleRetryList{},
						}
					}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext,
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
					gomonkey.ApplyFunc(utils.IsPullTriggerFunction,
						func(funcMeta *faasfunction.FunctionService) bool {
							return false
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// instance meta delete event
		{
			name: "test002",
			args: args{
				event: eventsource.Event{
					Kind:      utils.InstanceMetadataEventKind,
					EventType: eventsource.Deleted,
					KeyRef:    mockutils.ETCDInsMetaKeyForTest,
					Object:    etcdValue,
					StartTime: time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{
							IdleRetryInfo: &podfetcher.IdleRetryList{},
						}
					}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext,
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
					gomonkey.ApplyFunc(utils.IsPullTriggerFunction,
						func(funcMeta *faasfunction.FunctionService) bool {
							return false
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// parse etcd value error
		{
			name: "test003",
			args: args{
				event: eventsource.Event{
					Kind:      utils.InstanceMetadataEventKind,
					EventType: eventsource.Added,
					KeyRef:    mockutils.ETCDInsMetaKeyForTest,
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
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{
							IdleRetryInfo: &podfetcher.IdleRetryList{},
						}
					}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).CreateNewContext,
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
					gomonkey.ApplyFunc(utils.IsPullTriggerFunction,
						func(funcMeta *faasfunction.FunctionService) bool {
							return false
						}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &FunctionRepoHandler{
				EtcdClient: tt.fields.EtcdClient,
				VpcHandler: tt.fields.VpcHandler,
			}
			patches := tt.patchFunc()
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestNewEventHandler(t *testing.T) {
	type args struct {
		etcdctl *etcd3.EtcdClient
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test001",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
						return nil, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := NewEventHandler(tt.args.etcdctl); got == nil {
				t.Errorf("NewEventHandler() = %v, want not nil", got)
			}
			patches.ResetAll()
		})
	}
}

func TestProcessFunctionDeleted(t *testing.T) {
	convey.Convey("TestProcessFunctionDeleted", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.TransferRepoMetaToFunctionService, func(source *config.FunctionMetaInfo,
				target *faasfunction.FunctionService) error {
				target.Spec.Configuration.FuncVpc.VpcID = source.ExtendedMetaData.FuncVpc.VpcID
				target.Spec.Configuration.FuncVpc.DomainID = source.ExtendedMetaData.FuncVpc.DomainID
				info := types.DNSDomainInfo{
					DomainName: "www.domainDelete.com",
				}
				target.Spec.Configuration.DNSDomainCfg = []types.DNSDomainInfo{info}
				return nil
			}),
			gomonkey.ApplyFunc(etcdhandler.DeleteInstanceByPrefixFromETCD,
				func(ctx context.Context, service *faasfunction.FunctionService) {
					return
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		info := config.FunctionMetaInfo{
			ExtendedMetaData: config.ExtendedMetaData{
				FuncVpc: config.Vpc{
					DomainID: "domain001",
					VpcID:    "vpc_delete_001",
				},
			},
			S3MetaData: config.S3MetaData{
				BucketID: "bucket001",
				ObjectID: "object001",
			},
		}
		metaInfo, _ := json.Marshal(info)
		handler := &FunctionRepoHandler{}
		event := evs.Event{
			Object: &etcd3.Event{
				PrevValue: metaInfo,
			},
			EventType: evs.Deleted,
			StartTime: time.Now(),
		}
		funcSvcID := "funcSvcID001"
		dnsDomainMap := map[string][]string{
			"multi.autotest-001.com": {"192.168.100.102", "192.168.100.101", "192.168.100.100"},
			"test.autotest-001.com":  {"192.168.100.1"},
		}
		utils.GetDNSCache().StoreDNSCache(funcSvcID,
			"vpc_delete_001", "www.domainDelete.com", dnsDomainMap)
		domainMap, err := utils.GetDNSCache().GetDNSDomainNames(funcSvcID)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(domainMap))
		err = handler.processFunctionDeleted(context.Background(), &podfetcher.PodFetcher{}, event, funcSvcID)
		assert.Nil(t, err)
		_, err = utils.GetDNSCache().GetDNSDomainNames(funcSvcID)
		assert.NotNil(t, err)
		assert.Equal(t, utils.ErrDomainInfoInvalid, err)

		funcSvcID02 := "funcSvcID002"
		utils.GetDNSCache().StoreDNSCache(funcSvcID,
			"vpc_delete_001", "www.domainDelete.com", dnsDomainMap)
		utils.GetDNSCache().StoreDNSCache(funcSvcID02,
			"vpc_delete_001", "www.domainDelete.com", dnsDomainMap)
		domainMap, err = utils.GetDNSCache().GetDNSDomainNames(funcSvcID02)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(domainMap))
		err = handler.processFunctionDeleted(context.Background(), &podfetcher.PodFetcher{}, event, funcSvcID)
		assert.Nil(t, err)
		domainMap, err = utils.GetDNSCache().GetDNSDomainNames(funcSvcID02)
		assert.Nil(t, err)
		assert.Equal(t, 2, len(domainMap))
		time.Sleep(time.Second)
	})
}
