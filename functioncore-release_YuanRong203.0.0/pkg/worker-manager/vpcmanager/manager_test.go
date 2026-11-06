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

package vpcmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"plugin"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typedappsv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	appsv1fake "k8s.io/client-go/kubernetes/typed/apps/v1/fake"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

type mockKV struct {
	sync.RWMutex
	kv map[string]string
}

var FakeEtcd mockKV

// TestGetVPCController test GetVPCController
func TestGetVPCController(t *testing.T) {
	convey.Convey("GetVPCController", t, func() {
		convey.Convey("success", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
					return &kubernetes.Clientset{}
				}),
				gomonkey.ApplyFunc(plugin.Open, func(path string) (*plugin.Plugin, error) {
					return &plugin.Plugin{}, nil
				}),
				gomonkey.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client,
					almType alarm.Type, specAddition string) {
				}),
				gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
					if s == "InitController" {
						return func() error {
							return nil
						}, nil
					}
					if s == "CheckResource" {
						return func(ch chan string) error {
							return nil
						}, nil
					}
					if s == "CreateResource" {
						return func(request []byte) ([]byte, error) {
							return []byte{}, nil
						}, nil
					}
					if s == "DeleteResource" {
						return func(string2 string) error {
							return nil
						}, nil
					}
					return func() {}, nil
				}),
				gomonkey.ApplyFunc(cleanPatKey,
					func() error {
						return nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
					func(a *alarm.Client, almType alarm.Type) {
						return
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			_, err := GetVPCController()
			convey.So(err, convey.ShouldEqual, nil)
		})

		convey.Convey("fail", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(plugin.Open, func(path string) (*plugin.Plugin, error) {
					return &plugin.Plugin{}, errors.New("test error")
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
			}
			_, err := GetVPCController()
			convey.So(err, convey.ShouldNotEqual, nil)
			for _, p := range patches {
				p.Reset()
			}
		})

	})

	type args struct {
		source string
	}
	tests := []struct {
		name        string
		want        *VpcHandler
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 kubeClient nil", nil, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(plugin.Open, func(path string) (*plugin.Plugin, error) {
					return nil, errors.New("realpath failed")
				}),
				gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
			})
			return patches
		}},
		{"case2 initControllerAndCheckResource error", nil, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(plugin.Open, func(path string) (*plugin.Plugin, error) {
					return &plugin.Plugin{}, nil
				}),
				gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
					return &kubernetes.Clientset{}
				}),
				gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
					if s == "InitController" {
						return func() error {
							return nil
						}, nil
					}
					if s == "CheckResource" {
						return func(ch chan string) error {
							return nil
						}, errors.New("mock error")
					}
					return func() {}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
			})
			return patches
		}},
		{"case3 cleanPatKey error", nil, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(plugin.Open, func(path string) (*plugin.Plugin, error) {
					return &plugin.Plugin{}, nil
				}),
				gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
					return &kubernetes.Clientset{}
				}),
				gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
					if s == "InitController" {
						return func() error {
							return nil
						}, nil
					}
					if s == "CheckResource" {
						return func(ch chan string) error {
							return nil
						}, nil
					}
					return func() {}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
					f()
				}),
				gomonkey.ApplyFunc(cleanPatKey,
					func() error {
						return errors.New("cleanPatKey error")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := GetVPCController()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetVPCController() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

// TestVpcHandler_HandleVpcFunctionInfo test HandleVpcFunctionInfo
func TestVpcHandler_HandleVpcFunctionInfo(t *testing.T) {
	type fields struct {
		KubeClient kubernetes.Interface
		PluginVpc  PluginVPC
		VpcMutex   sync.Mutex
		VpcChannel chan string
	}
	type args struct {
		service     *faasfunction.FunctionService
		requestInfo *config.RequestInfo
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        *config.RequestInfo
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test with vpc id is empty",
			args: args{
				service: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							NetworkController: types.NetworkController{
								DisablePublicNetwork: true,
							},
						},
					},
				},
				requestInfo: &config.RequestInfo{
					TraceID: "test",
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			wantErr: false,
		},
		{
			name:   "test with timeout",
			fields: fields{},
			args: args{
				service: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "FuncVpc",
							},
							EndpointTenantVpc: faasfunction.Vpc{
								VpcID: "EndpointTenantVpc",
							},
						},
					},
				},
				requestInfo: &config.RequestInfo{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							time.Sleep(2 * time.Second)
							return []types.NATConfigure{}, nil
						}),
					gomonkey.ApplyFunc(time.After, func(d time.Duration) <-chan time.Time {
						return time.NewTimer(3 * time.Millisecond).C
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "test with finish count sub to 0",
			fields: fields{},
			args: args{
				service: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "FuncVpc",
							},
							EndpointTenantVpc: faasfunction.Vpc{
								VpcID: "EndpointTenantVpc",
							},
						},
					},
				},
				requestInfo: &config.RequestInfo{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							time.Sleep(1 * time.Second)
							return []types.NATConfigure{
								{
									PatPodName: fmt.Sprintf("pat-pod-name-%d", vpcType),
								},
							}, nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name:   "test with create func vpc resource return error",
			fields: fields{},
			args: args{
				service: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "FuncVpc",
							},
							EndpointTenantVpc: faasfunction.Vpc{
								VpcID: "EndpointTenantVpc",
							},
						},
					},
				},
				requestInfo: &config.RequestInfo{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							if vpcType == funcVpc {
								return nil, errors.New("mock create func vpc resource error")
							}
							return []types.NATConfigure{
								{
									PatPodName: fmt.Sprintf("pat-pod-name-%d", vpcType),
								},
							}, nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name:   "test with create endpoint vpc resource return error",
			fields: fields{},
			args: args{
				service: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FuncVpc: faasfunction.Vpc{
								VpcID: "FuncVpc",
							},
							EndpointTenantVpc: faasfunction.Vpc{
								VpcID: "EndpointTenantVpc",
							},
						},
					},
				},
				requestInfo: &config.RequestInfo{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							if vpcType == endpointTenantVpc {
								return nil, errors.New("mock create endpoint vpc resource error")
							}
							return []types.NATConfigure{
								{
									PatPodName: fmt.Sprintf("pat-pod-name-%d", vpcType),
								},
							}, nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			v := &VpcHandler{
				KubeClient: tt.fields.KubeClient,
				VpcPlugin:  tt.fields.PluginVpc,
				VpcChannel: tt.fields.VpcChannel,
			}
			err := v.HandleVpcFunctionInfo(tt.args.service, tt.args.requestInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("HandleVpcFunctionInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			time.Sleep(1 * time.Second)
			patches.ResetAll()
		})
	}
}

// TestVpcHandler_SendDeleteRequest test recordDeleteRequest
func TestVpcHandler_SendDeleteRequest(t *testing.T) {
	convey.Convey("recordDeleteRequest", t, func() {
		var k = "test"
		var handler = &VpcHandler{}
		mockFuncSvcID := "mock-funcSvcID"
		convey.Convey("list empty", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
					return func(string2 string) error {
						return errors.New("create error")
					}, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			err := handler.recordDeleteRequest(0, k, mockFuncSvcID)
			convey.So(err, convey.ShouldNotEqual, nil)
			k = "0/1/2/3/4/5"
			err = handler.recordDeleteRequest(0, k, mockFuncSvcID)
			convey.So(err, convey.ShouldNotEqual, nil)
		})

		convey.Convey("list not empty", func() {
			err := handler.recordDeleteRequest(1, k, mockFuncSvcID)
			convey.So(err, convey.ShouldEqual, nil)
		})

	})
}

// TestVpcHandler_CheckPullTriggerInfo test CheckPullTriggerInfo
func TestVpcHandler_CheckPullTriggerInfo(t *testing.T) {
	convey.Convey("CheckPullTriggerInfo", t, func() {
		var handler = &VpcHandler{
			patKeyList:   make(map[string]map[string]struct{}, defaultKeyLen),
			instanceList: make(map[string]map[string]*PatInstance, defaultKeyLen),
		}
		var funcSvcID = "mock-funcSvcID"
		var patServiceID = "patServiceID"
		var podName = "podName"
		convey.Convey("test not have Instances", func() {
			ok := handler.CheckPullTriggerInfo(funcSvcID, patServiceID, podName)
			convey.So(ok, convey.ShouldEqual, false)
		})
	})
}

// TestParseFunctionMeta test ParseFunctionMeta
func TestParseFunctionMeta(t *testing.T) {
	convey.Convey("ParseFunctionMeta", t, func() {
		meta := &faasfunction.FunctionService{}
		meta.Spec.Configuration = faasfunction.ConfigurationSpec{
			Resource:        faasfunction.ResourceSpec{CPU: 10, Mem: 10},
			VPCTriggerImage: "TriggerImage",
			DomainID:        "DomainID",
			TenantID:        "TenantID",
			FuncVpc:         faasfunction.Vpc{},
			Role:            faasfunction.Role{},
		}
		convey.Convey("fail", func() {
			meta.Spec.Configuration.FunctionName = "functionname"
			i := ParseFunctionMeta(meta)
			convey.So(i, convey.ShouldEqual, nil)
		})
		convey.Convey("success", func() {
			meta.Spec.Configuration.FunctionName = "1@2@functionname"
			i := ParseFunctionMeta(meta)
			convey.So(i.PodName, convey.ShouldEqual, "functionname")
		})
	})
}

func TestVpcHandler_handleVpcChannelReceiveAndSendToWorker(t *testing.T) {
	type fields struct {
		KubeClient kubernetes.Interface
		PluginVpc  PluginVPC
		VpcMutex   sync.Mutex
		VpcChannel chan string
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test normal",
			fields: fields{
				VpcChannel: make(chan string, 1),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			mockTenantID := "mock-tenant-id"
			mockFuncSvcID := "mock-function-service-id"
			deletedPodName := "mock-pat-pod-name-deleted"
			v := &VpcHandler{
				KubeClient:   tt.fields.KubeClient,
				VpcPlugin:    tt.fields.PluginVpc,
				VpcChannel:   tt.fields.VpcChannel,
				patKeyList:   make(map[string]map[string]struct{}, defaultKeyLen),
				instanceList: make(map[string]map[string]*PatInstance, defaultKeyLen),
			}
			RegisterSendPatPodIPFunc(
				func(ctx context.Context, retryPeriod time.Duration, param *config.PatPodRequestParam) {
					return
				})
			v.storeTwoWay(deletedPodName, buildPatKey(mockTenantID, deletedPodName), &PatInstance{
				instanceType: workerInstanceType,
				funcSvcID:    mockFuncSvcID,
				podName:      deletedPodName,
			})
			value, _ := json.Marshal(&ChangedPatPodInfo{
				TenantID: mockTenantID,
				PatPod: types.NATConfigure{
					PatPodName: deletedPodName,
				},
			})
			v.VpcChannel <- string(value)
			go func() {
				time.Sleep(1 * time.Second)
				close(v.VpcChannel)
			}()
			v.handleVpcChannelReceive()
			// clean map
			v.instanceList = nil
			v.patKeyList = nil
			patches.ResetAll()
		})
	}
}

func TestVpcHandler_handleVpcResource(t *testing.T) {
	finishCount := int32(5)
	type fields struct {
		KubeClient kubernetes.Interface
		PluginVpc  PluginVPC
		VpcMutex   sync.Mutex
		VpcChannel chan string
	}
	type args struct {
		info    *config.RequestInfo
		param   *handleVpcParam
		service *faasfunction.FunctionService
	}
	isMocked := false
	tests := []struct {
		name         string
		fields       fields
		args         args
		want         []types.NATConfigure
		patchesFunc  mockUtils.PatchesFunc
		createPatErr bool
		isMocked     bool
	}{
		{
			name:   "test with succeed to return pat pod",
			fields: fields{},
			args: args{
				info: &config.RequestInfo{
					TraceID: "test-trace-id",
				},
				param: &handleVpcParam{&finishCount,
					make(chan error), make(chan struct{}), endpointTenantVpc, false, true},
				service: &faasfunction.FunctionService{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return []types.NATConfigure{}, nil
						}),
				})
				return patches
			},
			createPatErr: false,
			isMocked:     true,
		}, // test with succeed to return pat pod
		{
			name:   "test with create resource error",
			fields: fields{},
			args: args{
				info: &config.RequestInfo{
					TraceID: "test-trace-id",
				},
				param: &handleVpcParam{&finishCount,
					make(chan error), make(chan struct{}), endpointTenantVpc, false, true},
				service: &faasfunction.FunctionService{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return nil, errors.New("mock create vpc resource error")
						}),
				})
				return patches
			},
			createPatErr: true,
			isMocked:     true,
		}, // test with create resource error
		{
			name:   "test with unknown vpc type",
			fields: fields{},
			args: args{
				info: &config.RequestInfo{
					TraceID: "test-trace-id",
				},
				param: &handleVpcParam{&finishCount,
					make(chan error), make(chan struct{}), 3, false, true},
				service: &faasfunction.FunctionService{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return []types.NATConfigure{}, nil
						}),
				})
				return patches
			},
			createPatErr: true,
			isMocked:     true,
		}, // test with unknown vpc type
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			v := &VpcHandler{
				KubeClient: tt.fields.KubeClient,
				VpcPlugin:  tt.fields.PluginVpc,
				VpcChannel: tt.fields.VpcChannel,
			}
			go func() {
				if tt.createPatErr {
					err, _ := <-tt.args.param.errCh
					fmt.Println("receive from errCh is ", err)
				}
			}()
			v.handleVpcResource(tt.args.info, tt.args.param, tt.args.service)
			if isMocked != tt.isMocked {
				t.Errorf("want %v but found %v", tt.isMocked, isMocked)
			}
			patches.ResetAll()
		})
	}
}

func Test_describeRequest(t *testing.T) {
	type args struct {
		traceID        string
		service        *faasfunction.FunctionService
		vpcType        int
		needInit       bool
		needNewVersion bool
	}
	tests := []struct {
		name string
		args args
		want config.VpcControllerRequester
	}{
		{"case1 vpc type funcVpc", args{
			traceID: "test",
			service: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FuncVpc: faasfunction.Vpc{
							VpcID: "FuncVpc",
						},
						EndpointTenantVpc: faasfunction.Vpc{
							VpcID: "EndpointTenantVpc",
						},
					},
				},
			},
			vpcType: 1,
		},
			config.VpcControllerRequester{
				TraceID: "test",
				Vpc: config.Vpc{
					VpcID: "FuncVpc",
				},
				NeedInit:       false,
				NeedNewVersion: false,
			}},
		{"case2 vpc type endpointTenantVpc", args{
			traceID: "test",
			service: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FuncVpc: faasfunction.Vpc{
							VpcID: "FuncVpc",
						},
						EndpointTenantVpc: faasfunction.Vpc{
							VpcID: "EndpointTenantVpc",
						},
					},
				},
			},
			vpcType: 2,
		},
			config.VpcControllerRequester{
				TraceID: "test",
				Vpc: config.Vpc{
					VpcID: "EndpointTenantVpc",
				},
				NeedInit:       false,
				NeedNewVersion: false,
			}},
		{"case3 vpc type funcVpc with extendedInfo", args{
			traceID: "test",
			service: &faasfunction.FunctionService{
				Spec: faasfunction.FunctionServiceSpec{
					Configuration: faasfunction.ConfigurationSpec{
						FuncVpc: faasfunction.Vpc{
							VpcID: "FuncVpc",
						},
						EndpointTenantVpc: faasfunction.Vpc{
							VpcID: "EndpointTenantVpc",
						},
						FuncMountConfig: faasfunction.FuncMountConfig{
							FuncMounts: []faasfunction.FuncMount{{
								MountType:    "sfs3.0",
								ExtendedInfo: "127.0.0.1/8",
							}},
						},
					},
				},
			},
			vpcType: 1,
		},
			config.VpcControllerRequester{
				TraceID: "test",
				Vpc: config.Vpc{
					VpcID:        "FuncVpc",
					ExtendedInfo: "127.0.0.1/8",
				},
				NeedInit:       false,
				NeedNewVersion: false,
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := describeRequest(tt.args.traceID, tt.args.service, tt.args.vpcType, tt.args.needInit, tt.args.needNewVersion)
			if !reflect.DeepEqual(tt.want, request) {
				t.Errorf("describeRequest error, request=%+v, want=%+v", request, tt.want)
			}
		})
	}
}

func TestVpcHandler_IdleDeletePatServiceList1(t *testing.T) {
	type fields struct {
		KubeClient   kubernetes.Interface
		PluginVpc    PluginVPC
		VpcChannel   chan string
		PatKeyList   map[string]map[string]struct{}
		InstanceList map[string]map[string]*PatInstance
	}
	type args struct {
		instanceName string
	}
	instanceName := "hello-#-world"
	fakePugin := PluginVPC{
		Plugin: &plugin.Plugin{},
		deleteResource: func(string2 string) error {
			return nil
		},
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			fields: fields{
				PluginVpc: fakePugin,
				InstanceList: map[string]map[string]*PatInstance{
					"1/2/3/4/5/6": {
						"world": &PatInstance{},
					},
				},
				PatKeyList: map[string]map[string]struct{}{
					"world": {
						"1/2/3/4/5/6": struct{}{},
					},
				},
			},
			args: args{
				instanceName: instanceName,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup,
						func(_ *plugin.Plugin, symName string) (plugin.Symbol, error) {
							return nil, nil
						}),
				})
				return patches
			}},
		{
			name: "test success with instance list is 2",
			fields: fields{
				PluginVpc: fakePugin,
				InstanceList: map[string]map[string]*PatInstance{
					"1/2/3/4/5/6": {
						"world": &PatInstance{},
						"hello": &PatInstance{},
					},
				},
				PatKeyList: map[string]map[string]struct{}{
					"world": {
						"1/2/3/4/5/6": struct{}{},
					},
				},
			},
			args: args{
				instanceName: instanceName,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{
			name: "test failed with pat key length is not 6",
			fields: fields{
				PluginVpc: fakePugin,
				InstanceList: map[string]map[string]*PatInstance{
					"1/2/3/4/5/6/7": {
						"world": &PatInstance{},
					},
				},
				PatKeyList: map[string]map[string]struct{}{
					"world": {
						"1/2/3/4/5/6/7": struct{}{},
					},
				},
			},
			args: args{
				instanceName: instanceName,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup,
						func(_ *plugin.Plugin, symName string) (plugin.Symbol, error) {
							return nil, nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test with len is 0",
			fields: fields{
				PluginVpc: fakePugin,
				InstanceList: map[string]map[string]*PatInstance{
					"1/2/3/4/5/6": {
						"world1": &PatInstance{},
					},
				},
				PatKeyList: map[string]map[string]struct{}{
					"world1": {
						"1/2/3/4/5/6": struct{}{},
					},
				},
			},
			args: args{
				instanceName: instanceName,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{
			name: "test with nil plugin",
			fields: fields{
				PluginVpc: PluginVPC{Plugin: nil},
			},
			args: args{
				instanceName: instanceName,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &VpcHandler{
				KubeClient:   tt.fields.KubeClient,
				VpcPlugin:    tt.fields.PluginVpc,
				VpcChannel:   tt.fields.VpcChannel,
				patKeyList:   tt.fields.PatKeyList,
				instanceList: tt.fields.InstanceList,
			}

			patches := tt.patchesFunc()
			if err := v.IdleDeletePatServiceList(tt.args.instanceName); (err != nil) != tt.wantErr {
				t.Errorf("IdleDeletePatServiceList() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestVpcHandler_CleanPullTrigger(t *testing.T) {
	type fields struct {
		KubeClient   kubernetes.Interface
		PluginVpc    PluginVPC
		VpcChannel   chan string
		patKeyList   map[string]map[string]struct{}
		instanceList map[string]map[string]*PatInstance
	}
	type args struct {
		FuncName string
	}
	isMocked := false
	fakePugin := PluginVPC{}
	fakePugin.Plugin = &plugin.Plugin{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		isMocked    bool
	}{
		{
			name: "test with plugin vpc nil",
			fields: fields{
				PluginVpc: PluginVPC{Plugin: nil},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			isMocked: false,
		},
		{
			name: "test with invalid func name",
			fields: fields{
				PluginVpc: fakePugin,
			},
			args: args{
				FuncName: "0@hello@world@123",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			isMocked: false,
		},
		{
			name: "test success",
			fields: fields{
				KubeClient: &kubernetes.Clientset{},
				PluginVpc:  fakePugin,
				patKeyList: map[string]map[string]struct{}{
					"world": {
						"hello": struct{}{},
					},
				},
				instanceList: map[string]map[string]*PatInstance{
					"hello": {
						"world":  &PatInstance{},
						"world1": &PatInstance{},
						"world2": &PatInstance{},
					},
				},
			},
			args: args{
				FuncName: "0@hello@world",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "AppsV1",
						func(_ *kubernetes.Clientset) typedappsv1.AppsV1Interface {
							isMocked = true
							return &appsv1fake.FakeAppsV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&appsv1fake.FakeAppsV1{}), "Deployments",
						func(_ *appsv1fake.FakeAppsV1, namespace string) typedappsv1.DeploymentInterface {
							isMocked = true
							return &appsv1fake.FakeDeployments{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&appsv1fake.FakeDeployments{}), "Delete",
						func(_ *appsv1fake.FakeDeployments, ctx context.Context, name string, opts metav1.DeleteOptions) error {
							isMocked = true
							return nil
						}),
				})
				return patches
			},
			isMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &VpcHandler{
				KubeClient:   tt.fields.KubeClient,
				VpcPlugin:    tt.fields.PluginVpc,
				VpcChannel:   tt.fields.VpcChannel,
				patKeyList:   tt.fields.patKeyList,
				instanceList: tt.fields.instanceList,
			}
			patches := tt.patchesFunc()
			v.CleanPullTrigger(tt.args.FuncName)
			patches.ResetAll()
			if isMocked != tt.isMocked {
				t.Errorf("expect: %v autual: %v", tt.isMocked, isMocked)
			}
		})
	}
}

func TestVpcHandler_HandlePatServiceList(t *testing.T) {
	type fields struct {
		KubeClient kubernetes.Interface
		PluginVpc  PluginVPC
		VpcChannel chan string
	}
	type args struct {
		tenantID   string
		funcSvcID  string
		patPodInfo []types.NATConfigure
		pod        *corev1.Pod
	}
	fakePugin := PluginVPC{}
	fakePugin.Plugin = &plugin.Plugin{}
	mockPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-pod-name"},
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockUtils.PatchesFunc
	}{
		{
			name: "test with success",
			fields: fields{
				PluginVpc: fakePugin,
			},
			args: args{
				tenantID:  "mock-tenant-id",
				funcSvcID: "mock-function-service-id",
				patPodInfo: []types.NATConfigure{
					{
						PatPodName: "mock-pat-pod-name",
					},
				},
				pod: mockPod,
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &VpcHandler{
				KubeClient:   tt.fields.KubeClient,
				VpcPlugin:    tt.fields.PluginVpc,
				VpcChannel:   tt.fields.VpcChannel,
				patKeyList:   make(map[string]map[string]struct{}, defaultKeyLen),
				instanceList: make(map[string]map[string]*PatInstance, defaultKeyLen),
			}
			patches := tt.patchFunc()
			v.HandlePatServiceList(tt.args.tenantID, tt.args.funcSvcID, tt.args.patPodInfo, tt.args.pod.Name)
			patches.ResetAll()
		})
	}
}

func TestVpcHandler_sendPatPodIPToAllWorkers(t *testing.T) {
	type fields struct {
		KubeClient kubernetes.Interface
		VpcPlugin  PluginVPC
		VpcChannel chan string
	}
	type args struct {
		changedPatPodInfo *ChangedPatPodInfo
		instances         map[string]*PatInstance
	}
	mockPodName := "mock-pod-name"
	mockFuncSvcID := "mock-function-service-id"
	mockTenantID := "mock-tenant-id"
	mockVpcID := "mock-vpc-id"
	isMocked := false
	fakePugin := PluginVPC{}
	fakePugin.Plugin = &plugin.Plugin{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		initCache   bool
		patType     int
		isMocked    bool
	}{
		{
			name: "test with funcMeta is nil",
			fields: fields{
				VpcPlugin: fakePugin,
			},
			args: args{
				instances: map[string]*PatInstance{
					"patKey1": {
						instanceType: pullTriggerPodType,
					},
					"patKey2": {
						instanceType: workerInstanceType,
						funcSvcID:    mockFuncSvcID,
						podName:      mockPodName,
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
		{
			name: "test with funcMeta is not nil",
			fields: fields{
				VpcPlugin: fakePugin,
			},
			args: args{
				changedPatPodInfo: &ChangedPatPodInfo{
					TenantID: mockTenantID,
					VpcID:    mockVpcID,
					PatPod:   types.NATConfigure{},
				},
				instances: map[string]*PatInstance{
					"patKey": {
						instanceType: workerInstanceType,
						funcSvcID:    mockFuncSvcID,
						podName:      mockPodName,
					},
				},
			},
			initCache: true,
			patType:   funcVpc,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return nil, errors.New("mock create vpc resource error")
						}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name: "test with funcMeta is not nil and create vpc resource return 1 func vpc",
			fields: fields{
				VpcPlugin: fakePugin,
			},
			args: args{
				changedPatPodInfo: &ChangedPatPodInfo{
					TenantID: mockTenantID,
					VpcID:    mockVpcID,
					PatPod:   types.NATConfigure{},
				},
				instances: map[string]*PatInstance{
					"patKey": {
						instanceType: workerInstanceType,
						funcSvcID:    mockFuncSvcID,
						podName:      mockPodName,
					},
				},
			},
			initCache: true,
			patType:   funcVpc,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return []types.NATConfigure{
								{
									PatPodName: "pat-pod-name-001",
								},
							}, nil
						}),
				})
				return patches
			},
			isMocked: true,
		}, // test with funcMeta is not nil and create vpc resource return 1 func vpc
		{
			name: "test with funcMeta is not nil and create vpc resource return 1 func vpc",
			args: args{
				changedPatPodInfo: &ChangedPatPodInfo{
					TenantID: mockTenantID,
					VpcID:    mockVpcID,
					PatPod:   types.NATConfigure{},
				},
				instances: map[string]*PatInstance{
					"patKey": {
						instanceType: workerInstanceType,
						funcSvcID:    mockFuncSvcID,
						podName:      mockPodName,
					},
				},
			},
			initCache: true,
			patType:   funcVpc,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return []types.NATConfigure{
								{
									PatPodName: "pat-pod-name-001",
								},
								{
									PatPodName: "pat-pod-name-002",
								},
							}, nil
						}),
				})
				return patches
			},
			isMocked: true,
		}, // test with funcMeta is not nil and create vpc resource return 2 func vpc
		{
			name: "test with funcMeta is not nil and create vpc resource return 1 endpoint vpc",
			fields: fields{
				VpcPlugin: fakePugin,
			},
			args: args{
				changedPatPodInfo: &ChangedPatPodInfo{
					TenantID: mockTenantID,
					VpcID:    mockVpcID,
					PatPod:   types.NATConfigure{},
				},
				instances: map[string]*PatInstance{
					"patKey": {
						instanceType: workerInstanceType,
						funcSvcID:    mockFuncSvcID,
						podName:      mockPodName,
					},
				},
			},
			initCache: true,
			patType:   endpointTenantVpc,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return []types.NATConfigure{
								{
									PatPodName: "pat-pod-name-001",
								},
							}, nil
						}),
				})
				return patches
			},
			isMocked: true,
		}, // test with funcMeta is not nil and create vpc resource return 1 endpoint vpc
		{
			name: "test with funcMeta is not nil and create vpc resource return unknown vpc",
			fields: fields{
				VpcPlugin: fakePugin,
			},
			args: args{
				changedPatPodInfo: &ChangedPatPodInfo{
					TenantID: mockTenantID,
					VpcID:    mockVpcID,
					PatPod:   types.NATConfigure{},
				},
				instances: map[string]*PatInstance{
					"patKey": {
						instanceType: workerInstanceType,
						funcSvcID:    mockFuncSvcID,
						podName:      mockPodName,
					},
				},
			},
			initCache: true,
			patType:   0,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							isMocked = true
							return []types.NATConfigure{
								{
									PatPodName: "pat-pod-name-001",
								},
							}, nil
						}),
				})
				return patches
			},
			isMocked: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &VpcHandler{
				KubeClient:   tt.fields.KubeClient,
				VpcPlugin:    tt.fields.VpcPlugin,
				VpcChannel:   tt.fields.VpcChannel,
				patKeyList:   make(map[string]map[string]struct{}, defaultKeyLen),
				instanceList: make(map[string]map[string]*PatInstance, defaultKeyLen),
			}
			if tt.initCache {
				mockFuncMeta := &faasfunction.FunctionService{}
				if tt.patType == funcVpc {
					mockFuncMeta.Spec.Configuration.FuncVpc.VpcID = mockVpcID
				} else if tt.patType == endpointTenantVpc {
					mockFuncMeta.Spec.Configuration.EndpointTenantVpc.VpcID = mockVpcID
				}
				cache.WorkerCache.CreateFunctionCacheByFuncMeta(mockFuncSvcID, mockFuncMeta)
			}

			sendPatPodIPToWorkerByRetry = func(ctx context.Context, retryPeriod time.Duration, param *config.PatPodRequestParam) {
				return
			}
			patches := tt.patchesFunc()
			v.sendPatPodIPToAllWorkers(tt.args.changedPatPodInfo, tt.args.instances)
			if tt.isMocked != isMocked {
				t.Errorf("isMocked expect %v but found %v", tt.isMocked, isMocked)
			}
			patches.ResetAll()
			sendPatPodIPToWorkerByRetry = nil
			if tt.initCache {
				cache.WorkerCache.DeleteFunctionCache(mockFuncSvcID)
			}
		})
	}
}

func TestVpcHandler_recoverPatPodsCache(t *testing.T) {
	type fields struct {
		KubeClient kubernetes.Interface
		VpcPlugin  PluginVPC
		VpcChannel chan string
	}
	type args struct {
		funcMeta *faasfunction.FunctionService
		pod      *corev1.Pod
		vpcType  int
		traceID  string
	}
	mockFuncMeta := &faasfunction.FunctionService{}
	mockFuncMeta.ID = "mockFuncSvcID"
	mockPod := &corev1.Pod{}
	mockPod.Name = "mockPodName"
	tests := []struct {
		name         string
		fields       fields
		args         args
		patchesFunc  mockUtils.PatchesFunc
		isCheckCache bool
		initCacheNum int
		wantErr      bool
	}{
		{
			name: "test success for func vpc with no new created",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  funcVpc,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							return []types.NATConfigure{
								{
									PatPodName: "mockPatPodName1",
								},
								{
									PatPodName: "mockPatPodName2",
								},
							}, nil
						}),
				})
				return patches
			},
			isCheckCache: true,
			wantErr:      false,
		},
		{
			name: "test success for endpoint vpc with no new created",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  endpointTenantVpc,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							return []types.NATConfigure{
								{
									PatPodName: "mockPatPodName1",
								},
								{
									PatPodName: "mockPatPodName2",
								},
							}, nil
						}),
				})
				return patches
			},
			isCheckCache: true,
			wantErr:      false,
		},
		{
			name: "test success for func vpc with cache has 2 pat pod",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  funcVpc,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			isCheckCache: true,
			initCacheNum: 2,
			wantErr:      false,
		},
		{
			name: "test success for func vpc with cache has 1 pat pod",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  funcVpc,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							return []types.NATConfigure{
								{
									PatPodName: "mockPatPodName1",
								},
								{
									PatPodName: "mockPatPodName2",
								},
							}, nil
						}),
				})
				return patches
			},
			isCheckCache: true,
			initCacheNum: 1,
			wantErr:      false,
		},
		{
			name: "test error for func vpc with 1 pat pod is created",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  funcVpc,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							return []types.NATConfigure{
								{
									PatPodName: "mockPatPodName1",
								},
							}, nil
						}),
				})
				return patches
			},
			isCheckCache: false,
			wantErr:      true,
		},
		{
			name: "test error for func vpc with 1 public pat pod is created",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  publicVPC,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							return []types.NATConfigure{
								{
									PatPodName: "mockPatPodName1",
								},
							}, nil
						}),
				})
				return patches
			},
			isCheckCache: false,
			wantErr:      true,
		},
		{
			name: "test success for func vpc with cache has 1 public pat pod",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  publicVPC,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*PluginVPC).createVpcResource,
						func(_ *PluginVPC, service *faasfunction.FunctionService, traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
							return []types.NATConfigure{
								{
									PatPodName: "mockPatPodName1",
								},
								{
									PatPodName: "mockPatPodName2",
								},
							}, nil
						}),
				})
				return patches
			},
			isCheckCache: true,
			initCacheNum: 1,
			wantErr:      false,
		},
		{
			name: "test success for func vpc with cache has 2 public pat pod",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				funcMeta: mockFuncMeta,
				pod:      mockPod,
				vpcType:  publicVPC,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			isCheckCache: true,
			initCacheNum: 2,
			wantErr:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &VpcHandler{
				KubeClient:   tt.fields.KubeClient,
				VpcPlugin:    tt.fields.VpcPlugin,
				VpcChannel:   tt.fields.VpcChannel,
				patKeyList:   make(map[string]map[string]struct{}, defaultKeyLen),
				instanceList: make(map[string]map[string]*PatInstance, defaultKeyLen),
			}
			patches := tt.patchesFunc()
			if tt.isCheckCache {
				cache.WorkerCache.CreateFunctionCacheByFuncMeta(tt.args.funcMeta.ID, tt.args.funcMeta)
			}
			if tt.initCacheNum > 0 {
				var vpcPats []types.NATConfigure
				switch tt.initCacheNum {
				case 1:
					vpcPats = []types.NATConfigure{
						{
							PatPodName: "mockPatPodName3",
						},
					}
				case 2:
					vpcPats = []types.NATConfigure{
						{
							PatPodName: "mockPatPodName3",
						},
						{
							PatPodName: "mockPatPodName4",
						},
					}
				}
				switch tt.args.vpcType {
				case funcVpc:
					cache.WorkerCache.SetFuncVpcPats(tt.args.funcMeta.ID, vpcPats)
				case endpointTenantVpc:
					cache.WorkerCache.SetEndpointVpcPats(tt.args.funcMeta.ID, vpcPats)
				case publicVPC:
					cache.WorkerCache.SetPublicVpcPats(tt.args.funcMeta.ID, vpcPats)
				}
			}
			if _, err := v.recoverPatPodsCache(tt.args.funcMeta, tt.args.pod, tt.args.vpcType, tt.args.traceID); (err != nil) != tt.wantErr {
				t.Errorf("recoverPatPodsCache() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.isCheckCache {
				var vpcPats []types.NATConfigure
				switch tt.args.vpcType {
				case funcVpc:
					vpcPats = cache.WorkerCache.GetFuncVpcPats(tt.args.funcMeta.ID)
				case endpointTenantVpc:
					vpcPats = cache.WorkerCache.GetEndpointVpcPats(tt.args.funcMeta.ID)
				case publicVPC:
					vpcPats = cache.WorkerCache.GetPublicVpcPats(tt.args.funcMeta.ID)
				}
				// clean cache
				cache.WorkerCache.DeleteFunctionCache(tt.args.funcMeta.ID)
				if len(vpcPats) != 2 {
					t.Errorf("vpc pats length expect 2 but found %d", len(vpcPats))
					return
				}
			}
			patches.ResetAll()
		})
	}
}

func TestVpcHandler_RecoverPatPodsCache(t *testing.T) {
	type fields struct {
		KubeClient kubernetes.Interface
		VpcPlugin  PluginVPC
		VpcChannel chan string
	}
	type args struct {
		pods []corev1.Pod
	}
	isMocked := false
	mockFuncSvcID := "mockFuncSvcID"
	mockFuncMeta := &faasfunction.FunctionService{}
	mockFuncMeta.ID = mockFuncSvcID
	mockFuncMeta.Spec.Configuration.FuncVpc.VpcID = "mockFuncVpcID"
	mockFuncMeta.Spec.Configuration.EndpointTenantVpc.VpcID = "mockEndpointVpcID"
	mockPods := make([]corev1.Pod, 0)
	mockPod := corev1.Pod{}
	mockPod.Name = "mockPodName"
	mockPods = append(mockPods, mockPod)
	tests := []struct {
		name          string
		fields        fields
		args          args
		patchesFunc   mockUtils.PatchesFunc
		needInitCache bool
		isMocked      bool
	}{
		{
			name: "test success",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{pods: mockPods},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.OwnedByFunctionService,
						func(pod *corev1.Pod) (string, bool) {
							isMocked = true
							return mockFuncSvcID, true
						}),
					gomonkey.ApplyFunc((*VpcHandler).recoverPatPodsCache,
						func(_ *VpcHandler, funcMeta *faasfunction.FunctionService, pod *corev1.Pod, vpcType int, traceID string) ([]types.NATConfigure, error) {
							isMocked = true
							return []types.NATConfigure{
								{
									PatPodName:   "mockPatPodName1",
									IsNewCreated: true,
								},
								{
									PatPodName:   "mockPatPodName2",
									IsNewCreated: false,
								},
							}, nil
						}),
				})
				return patches
			},
			needInitCache: true,
			isMocked:      true,
		},
		{
			name: "test with cache has no funcMeta",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{pods: mockPods},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.OwnedByFunctionService,
						func(pod *corev1.Pod) (string, bool) {
							isMocked = true
							return mockFuncSvcID, true
						}),
				})
				return patches
			},
			needInitCache: false,
			isMocked:      true,
		},
		{
			name: "test with pod is not a instance",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{pods: mockPods},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.OwnedByFunctionService,
						func(pod *corev1.Pod) (string, bool) {
							isMocked = true
							return "", false
						}),
				})
				return patches
			},
			needInitCache: false,
			isMocked:      true,
		},
		{
			name: "test with recoverPatPodsCache return error",
			fields: fields{
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{pods: mockPods},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.OwnedByFunctionService,
						func(pod *corev1.Pod) (string, bool) {
							isMocked = true
							return mockFuncSvcID, true
						}),
					gomonkey.ApplyFunc((*VpcHandler).recoverPatPodsCache,
						func(_ *VpcHandler, funcMeta *faasfunction.FunctionService, pod *corev1.Pod, vpcType int, traceID string) ([]types.NATConfigure, error) {
							isMocked = true
							return nil, errors.New("mock recoverPatPodsCache return error")
						}),
				})
				return patches
			},
			needInitCache: true,
			isMocked:      true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMocked = false
			if tt.needInitCache {
				cache.WorkerCache.CreateFunctionCacheByFuncMeta(mockFuncSvcID, mockFuncMeta)
			}
			patches := tt.patchesFunc()
			v := &VpcHandler{
				KubeClient: tt.fields.KubeClient,
				VpcPlugin:  tt.fields.VpcPlugin,
				VpcChannel: tt.fields.VpcChannel,
			}
			sendPatPodIPToWorkerByRetry = func(ctx context.Context, retryPeriod time.Duration, param *config.PatPodRequestParam) {
				fmt.Println("mock sendPatPodIPToWorkerByRetry do nothing")
				return
			}
			v.RecoverPatPodsCache(tt.args.pods)
			if tt.isMocked != isMocked {
				t.Errorf("isMocked expect %v but found %v", tt.isMocked, isMocked)
				return
			}
			patches.ResetAll()
			if tt.needInitCache {
				cache.WorkerCache.DeleteFunctionCache(mockFuncSvcID)
			}
		})
	}
}

func Test_cleanPatKey(t *testing.T) {
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:    "case01 get pat key err",
			wantErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return nil, errors.New("test err")
						}),
				})
				return patches
			},
		}, {
			name:    "case02 delete pat key err, print warn log",
			wantErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							FakeEtcd.kv = make(map[string]string)
							FakeEtcd.kv["key/key/key/key/key/key"] = `["a","c"]`
							var kvs []*mvccpb.KeyValue
							for k, v := range FakeEtcd.kv {
								kvs = append(kvs, &mvccpb.KeyValue{
									Key:   []byte(k),
									Value: []byte(v),
								})
							}
							resp := &clientv3.GetResponse{
								Kvs: kvs,
							}
							return resp, nil
						}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Delete, func(_ *etcd3.EtcdClient,
						ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) error {
						return errors.New("test")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := cleanPatKey(); (err != nil) != tt.wantErr {
				t.Errorf("cleanPatKey() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestVpcHandler_ProvisionPullTriggerPod(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		KubeClient kubernetes.Interface
		VpcPlugin  PluginVPC
		VpcChannel chan string
	}
	type args struct {
		functionMeta *faasfunction.FunctionService
		replicas     int32
	}
	isMocked := false
	sameMinScale := int32(2)
	diffMinScale := int32(1)
	mockFuncSvcID1 := "mockFuncSvcID001"
	mockFuncMeta1 := &faasfunction.FunctionService{
		ObjectMeta: faasfunction.ObjectMeta{
			ID: mockFuncSvcID1,
		},
		Spec: faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{
				FunctionName: mockUtils.MockPullTriggerEtcdKey,
			},
		},
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
		isMocked    bool
	}{
		{
			name: "test success with same pat pods",
			fields: fields{
				EtcdClient: nil,
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				functionMeta: mockFuncMeta1,
				replicas:     int32(sameMinScale),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*VpcHandler).HandleVpcFunctionInfo,
						func(_ *VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							isMocked = true
							var patPods []types.NATConfigure
							json.Unmarshal([]byte(mockUtils.MockPullTriggerPatPodsEnvValue), &patPods)
							fmt.Println("pat pods len is ", len(patPods))
							requestInfo.PatPodInfo = patPods
							return nil
						}),
					gomonkey.ApplyFunc((*VpcHandler).CheckPullTriggerInfo,
						func(_ *VpcHandler, funcSvcID, patServiceID, podName string) bool {
							isMocked = true
							return true
						}),
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*v1.Deployment, error) {
							isMocked = true
							return &v1.Deployment{
								Spec: v1.DeploymentSpec{
									Replicas: &sameMinScale,
									Template: corev1.PodTemplateSpec{
										Spec: corev1.PodSpec{
											Containers: []corev1.Container{
												{
													Name: pullTriggerContainerName,
													Env: []corev1.EnvVar{
														{
															Name:  pullTriggerEnvKey,
															Value: mockUtils.MockPullTriggerPatPodsEnvValue,
														},
													},
												},
											},
										},
									},
								},
							}, nil
						}),
				})
				return patches
			},
			wantErr:  false,
			isMocked: true,
		}, // test success with same pat pods
		{
			name: "test success with same pat pods but replicas changed",
			fields: fields{
				EtcdClient: nil,
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				functionMeta: mockFuncMeta1,
				replicas:     diffMinScale,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*VpcHandler).HandleVpcFunctionInfo,
						func(_ *VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							isMocked = true
							var patPods []types.NATConfigure
							json.Unmarshal([]byte(mockUtils.MockPullTriggerPatPodsEnvValue), &patPods)
							requestInfo.PatPodInfo = patPods
							return nil
						}),
					gomonkey.ApplyFunc((*VpcHandler).CheckPullTriggerInfo,
						func(_ *VpcHandler, funcSvcID, patServiceID, podName string) bool {
							isMocked = true
							return true
						}),
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*v1.Deployment, error) {
							isMocked = true
							return &v1.Deployment{
								Spec: v1.DeploymentSpec{
									Replicas: &sameMinScale,
									Template: corev1.PodTemplateSpec{
										Spec: corev1.PodSpec{
											Containers: []corev1.Container{
												{
													Name: pullTriggerContainerName,
													Env: []corev1.EnvVar{
														{
															Name:  pullTriggerEnvKey,
															Value: mockUtils.MockPullTriggerPatPodsEnvValue,
														},
													},
												},
											},
										},
									},
								},
							}, nil
						}),
					gomonkey.ApplyFunc(tools.UpdateReplicasByK8s,
						func(deployName string, replicas int32) error {
							isMocked = true
							return nil
						}),
				})
				return patches
			},
			wantErr:  false,
			isMocked: true,
		}, // test success with same pat pods but replicas changed
		{
			name: "test failed with create pat pods error but no retry",
			fields: fields{
				EtcdClient: nil,
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				functionMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName: mockUtils.MockPullTriggerEtcdKey,
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*VpcHandler).HandleVpcFunctionInfo,
						func(_ *VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							isMocked = true
							return errors.New("mock create pat pods error")
						}),
					gomonkey.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client,
						almType alarm.Type, specAddition string) {
					}),
				})
				return patches
			},
			wantErr:  false,
			isMocked: true,
		}, // test failed with create pat pods error
		{
			name: "test success with not same pat pods",
			fields: fields{
				EtcdClient: nil,
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				functionMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName: mockUtils.MockPullTriggerEtcdKey,
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*VpcHandler).HandleVpcFunctionInfo,
						func(_ *VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							isMocked = true
							var patPods []types.NATConfigure
							json.Unmarshal([]byte(mockUtils.MockPullTriggerPatPodsEnvValue), &patPods)
							requestInfo.PatPodInfo = []types.NATConfigure{
								patPods[0],
							}
							return nil
						}),
					gomonkey.ApplyFunc((*VpcHandler).CheckPullTriggerInfo,
						func(_ *VpcHandler, funcSvcID, patServiceID, podName string) bool {
							isMocked = true
							return true
						}),
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*v1.Deployment, error) {
							isMocked = true
							return &v1.Deployment{
								Spec: v1.DeploymentSpec{
									Template: corev1.PodTemplateSpec{
										Spec: corev1.PodSpec{
											Containers: []corev1.Container{
												{
													Name: pullTriggerContainerName,
													Env: []corev1.EnvVar{
														{
															Name:  pullTriggerEnvKey,
															Value: mockUtils.MockPullTriggerPatPodsEnvValue,
														},
													},
												},
											},
										},
									},
								},
							}, nil
						}),
					gomonkey.ApplyFunc(tools.UpdateDeployByK8S,
						func(k8sClient kubernetes.Interface, deploy *v1.Deployment) error {
							isMocked = true
							return nil
						}),
				})
				return patches
			},
			wantErr:  false,
			isMocked: true,
		}, // test success with not same pat pods
		{
			name: "test failed with get deploy return k8s not found error",
			fields: fields{
				EtcdClient: nil,
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				functionMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName: mockUtils.MockPullTriggerEtcdKey,
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*VpcHandler).HandleVpcFunctionInfo,
						func(_ *VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							isMocked = true
							var patPods []types.NATConfigure
							json.Unmarshal([]byte(mockUtils.MockPullTriggerPatPodsEnvValue), &patPods)
							requestInfo.PatPodInfo = patPods
							return nil
						}),
					gomonkey.ApplyFunc((*VpcHandler).CheckPullTriggerInfo,
						func(_ *VpcHandler, funcSvcID, patServiceID, podName string) bool {
							isMocked = true
							return true
						}),
					gomonkey.ApplyFunc(k8serror.IsNotFound,
						func(err error) bool {
							return true
						}),
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*v1.Deployment, error) {
							isMocked = true
							return nil, errors.New("NotFound")
						}),
				})
				return patches
			},
			wantErr:  true,
			isMocked: true,
		}, // test failed with get deploy return k8s not found error
		{
			name: "test failed with get deploy return normal error",
			fields: fields{
				EtcdClient: nil,
				KubeClient: nil,
				VpcPlugin:  PluginVPC{Plugin: nil},
				VpcChannel: nil,
			},
			args: args{
				functionMeta: &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FunctionName: mockUtils.MockPullTriggerEtcdKey,
						},
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*VpcHandler).HandleVpcFunctionInfo,
						func(_ *VpcHandler, service *faasfunction.FunctionService,
							requestInfo *config.RequestInfo) error {
							isMocked = true
							var patPods []types.NATConfigure
							json.Unmarshal([]byte(mockUtils.MockPullTriggerPatPodsEnvValue), &patPods)
							requestInfo.PatPodInfo = patPods
							return nil
						}),
					gomonkey.ApplyFunc((*VpcHandler).CheckPullTriggerInfo,
						func(_ *VpcHandler, funcSvcID, patServiceID, podName string) bool {
							isMocked = true
							return true
						}),
					gomonkey.ApplyFunc(tools.GetDeployByK8S,
						func(k8sClient kubernetes.Interface, deployName string) (*v1.Deployment, error) {
							isMocked = true
							return nil, errors.New("mock get deploy error")
						}),
				})
				return patches
			},
			wantErr:  true,
			isMocked: true,
		}, // test failed with get deploy return normal error
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &VpcHandler{
				KubeClient: tt.fields.KubeClient,
				VpcPlugin:  tt.fields.VpcPlugin,
				VpcChannel: tt.fields.VpcChannel,
			}
			isMocked = false
			patches := tt.patchesFunc()
			err := v.ProvisionPullTriggerPod(tt.args.functionMeta, tt.args.replicas)
			if (err != nil) != tt.wantErr {
				t.Errorf("expect %v but found %v", tt.wantErr, err)
			}
			if isMocked != tt.isMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
			}
			patches.ResetAll()
		})
	}
}

func TestDeleteRemainedVpcPullTrigger(t *testing.T) {
	convey.Convey("DeleteRemainedVpcPullTrigger", t, func() {
		convey.Convey("success", func() {
			deployNameMap := make(map[string]bool)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(tools.ListDeployByK8S,
					func(k8sClient kubernetes.Interface, options metav1.ListOptions) (*v1.DeploymentList, error) {
						deployNameMap["test1"] = false
						deployNameMap["test2"] = false
						deployNameMap["test3"] = false
						return &v1.DeploymentList{
							Items: []v1.Deployment{
								{
									ObjectMeta: metav1.ObjectMeta{
										Name: "test1",
									},
								},
								{
									ObjectMeta: metav1.ObjectMeta{
										Name: "test2",
									},
								},
								{
									ObjectMeta: metav1.ObjectMeta{
										Name: "test3",
									},
								},
							},
						}, nil
					}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse", func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Key: []byte("test1"),
							},
							{
								Key: []byte("test2"),
							},
						},
					}, nil
				}),
				gomonkey.ApplyFunc(tools.DeleteDeployByK8S,
					func(k8sClient kubernetes.Interface, name string) error {
						deployNameMap[name] = true
						return nil
					}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			DeleteRemainedVpcPullTrigger()
			convey.So(deployNameMap["test1"], convey.ShouldBeFalse)
			convey.So(deployNameMap["test2"], convey.ShouldBeFalse)
			convey.So(deployNameMap["test3"], convey.ShouldBeTrue)
		})
	})
}

func TestCheckExists(t *testing.T) {
	convey.Convey("test check exists", t, func() {
		handler := &VpcHandler{}
		convey.So(handler.checkExists("123"), convey.ShouldBeFalse)
		instances := make(map[string]map[string]*PatInstance)
		item := make(map[string]*PatInstance)
		instances["123"] = item
		handler = &VpcHandler{
			instanceList: instances,
			patKeyList:   nil,
			vpcMutex:     sync.Mutex{},
			KubeClient:   nil,
			VpcPlugin:    PluginVPC{},
			VpcChannel:   nil,
		}
		convey.So(handler.checkExists("123"), convey.ShouldBeTrue)
	})
}

func TestVpcHandler_handleVpcChannelReceive(t *testing.T) {
	type fields struct {
		instanceList map[string]map[string]*PatInstance
		patKeyList   map[string]map[string]struct{}
		vpcMutex     sync.Mutex
		KubeClient   kubernetes.Interface
		VpcPlugin    PluginVPC
		VpcChannel   chan string
	}
	tests := []struct {
		name        string
		fields      fields
		funcSvcID   string
		patPods     []types.NATConfigure
		triggerInfo *CreateVpcTriggerInstruct
		IsExists    bool
	}{
		{
			name: "test receive second pat from vpc channel",
			fields: fields{
				instanceList: make(map[string]map[string]*PatInstance),
				patKeyList:   make(map[string]map[string]struct{}),
				VpcPlugin:    PluginVPC{},
				VpcChannel:   make(chan string),
			},
			funcSvcID: "mock-funcSvcID-001",
			patPods: []types.NATConfigure{
				{
					PatPodName: "mock-pat-pod-name-001",
					IsDeleted:  false,
				},
			},
			triggerInfo: &CreateVpcTriggerInstruct{
				Namespace: "mock-namespace-001",
				PodName:   "mock-trigger-pod-name-001",
			},
			IsExists: true,
		},
		{
			name: "test receive deleted pat from vpc channel",
			fields: fields{
				instanceList: make(map[string]map[string]*PatInstance),
				patKeyList:   make(map[string]map[string]struct{}),
				VpcPlugin:    PluginVPC{},
				VpcChannel:   make(chan string),
			},
			funcSvcID: "mock-funcSvcID-002",
			patPods: []types.NATConfigure{
				{
					PatPodName: "mock-pat-pod-name-002",
					IsDeleted:  true,
				},
			},
			triggerInfo: &CreateVpcTriggerInstruct{
				Namespace: "mock-namespace-002",
				PodName:   "mock-trigger-pod-name-002",
			},
			IsExists: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := &VpcHandler{
				instanceList: tt.fields.instanceList,
				patKeyList:   tt.fields.patKeyList,
				vpcMutex:     tt.fields.vpcMutex,
				KubeClient:   tt.fields.KubeClient,
				VpcPlugin:    tt.fields.VpcPlugin,
				VpcChannel:   tt.fields.VpcChannel,
			}
			// mock provision pull trigger pod and update vpc cache
			v.updatePullTriggerCache(tt.funcSvcID, tt.patPods, tt.triggerInfo)
			// check if pull trigger pod and pat are in cache
			if len(tt.patPods) == 0 {
				return
			}
			pat := tt.patPods[0]
			patKey := buildPatKey(tt.triggerInfo.Namespace, pat.PatPodName)
			instancePods, ok := v.loadInstancesByKey(patKey)
			if !ok {
				t.Errorf("it was not found pull trigger pod by %s in cache", patKey)
			}
			fmt.Println("pull trigger pod is: ", instancePods)
			go v.handleVpcChannelReceive()
			// send current pat to vpc channel
			patPodInfo := &ChangedPatPodInfo{}
			patPodInfo.PatPod = pat
			patPodInfo.TenantID = tt.triggerInfo.Namespace
			patPodMsg, _ := json.Marshal(patPodInfo)
			fmt.Println(string(patPodMsg))
			v.VpcChannel <- string(patPodMsg)
			time.Sleep(1 * time.Second) // wait handleVpcChannelReceive method handle vpc channel
			// check if pull trigger pod and pat are in cache again
			instancePods, ok = v.loadInstancesByKey(patKey)
			if ok != tt.IsExists {
				t.Errorf("expect %v but found %v", tt.IsExists, ok)
			}
			fmt.Println("pull trigger pod is: ", instancePods)
		})
	}
}
