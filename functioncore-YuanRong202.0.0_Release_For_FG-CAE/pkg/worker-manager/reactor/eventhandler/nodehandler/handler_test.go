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

// Package nodehandler
package nodehandler

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockutils "functioncore/test/utils"
)

func TestNodeHandler_HandleEvent(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		VpcHandler *vpcmanager.VpcHandler
	}
	type args struct {
		event eventsource.Event
	}

	nodeValue := &corev1.Node{}
	value := `{
    "funcMetaData": {
        "layers": [],
        "name": "0@default@puresleep-java-10ms",
        "description": "",
        "functionUrn": "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms",
        "tenantId": "fb6a00ff7ae54a5fbb8ff855d0841d00",
        "reversedConcurrency": 0,
        "tags": null,
        "created": "2022-06-07 04:20:23.895 UTC",
        "functionUpdateTime": "2022-06-07T12:20:23+08:00",
        "functionVersionUrn": "sn:cn:yrk:fb6a00ff7ae54a5fbb8ff855d0841d00:function:0@default@puresleep-java-10ms:latest",
        "revisionId": "20220607042023895",
        "codeSize": 32696,
        "codeSha512": "a0a03391b763b06a4638b51dd03adc9292006f0119773237273a4224f67f937e1ac055e28ea094ecee46ee82a26064bf0e707d3553a1ce34e47f93e6454a7c2c",
        "handler": "com.huawei.demo.TriggerTests::apigTest",
        "runtime": "java8",
        "timeout": 15,
        "version": "latest",
        "versionDescription": "",
        "deadLetterConfig": "",
        "latestVersionUpdateTime": "2022-06-07T12:20:23+08:00",
        "publishTime": "",
        "businessId": "yrk",
        "functionType": "",
        "id": "943d69aa-ab6a-450c-80c2-10f4b07a4244",
        "func_id": "d8b5294d-0eab-4bf7-b623-33ec1bf379f7",
        "func_name": "puresleep-java-10ms",
        "domain_id": "833fc78659514766be1dc2e2819d72bf",
        "project_name": "cn-north-7",
        "service": "default",
        "dependencies": "",
        "isStatefulFunction": false,
        "type": "v2",
        "enable_auth_in_header": false
    },
    "s3MetaData": {
        "appId": "",
        "bucketId": "functionstorage",
        "objectId": "functions/fb6a00ff7ae54a5fbb8ff855d0841d00/default/puresleep-java-10ms/latest/pure-sleep10ms.jar",
        "bucketUrl": "https://obs.cn-north-7.ulanqab.huawei.com",
        "code_type": "jar",
        "code_url": "",
        "code_filename": "pure-sleep10ms.jar",
        "func_code": {
            "file": "",
            "link": ""
        }
    },
    "envMetaData": {
        "environment": "",
        "encrypted_user_data": ""
    },
    "resourceMetaData": {
        "cpu": 4000,
        "memory": 8192,
        "enable_dynamic_memory": false,
        "customResources": ""
    },
    "instanceMetaData": {
        "maxInstance": -1,
        "minInstance": 0,
        "concurrentNum": 100,
        "instanceType": "",
        "idleMode": false
    },
    "extendedMetaData": {
        "image_name": "latest-220607122023@spbyt",
        "role": {
            "xrole": "",
            "app_xrole": ""
        },
        "func_vpc": null,
        "endpoint_tenant_vpc": null,
        "mount_config": null,
        "strategy_config": {
            "concurrency": -1,
            "concurrentNum": 1
        },
        "extend_config": "",
        "initializer": {
            "initializer_handler": "",
            "initializer_timeout": 0
        },
        "heartbeat": null,
        "enterprise_project_id": "0",
        "log_tank_service": {
            "logGroupId": "3eb52dd5-11be-4530-ab57-63e1a480bc9d",
            "logStreamId": "7f69706e-1815-45d5-a50c-25105bc0eb90"
        },
        "tracing_config": {
            "enabled": false,
            "tracing_ak": "",
            "tracing_sk": ""
        },
        "custom_container_config": {
            "control_path": "",
            "image": "",
            "command": null,
            "args": null,
            "working_dir": "",
            "uid": 0,
            "gid": 0
        }
    }
}`
	etcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte(value),
		PrevValue: nil,
		Rev:       0,
	}
	mockPodFetcher := &podfetcher.PodFetcher{}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockutils.PatchesFunc
		wantErr   bool
	}{
		{
			name: "test001",
			fields: fields{
				EtcdClient: nil,
				VpcHandler: nil,
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Added,
					Kind:       utils.NodeEventKind,
					KeyRef:     "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest",
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher,
						func() *podfetcher.PodFetcher {
							return &podfetcher.PodFetcher{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "CreateNewContext",
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
				})
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				patches.Append(mockutils.GetListPodsPatch(&corev1.PodList{}, nil))

				return patches
			},
			wantErr: false,
		},
		{
			name: "test001 with list pods error",
			fields: fields{
				EtcdClient: nil,
				VpcHandler: nil,
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Added,
					Kind:       utils.NodeEventKind,
					KeyRef:     "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest",
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{},
							}, nil
						}),
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher,
						func() *podfetcher.PodFetcher {
							return &podfetcher.PodFetcher{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "CreateNewContext",
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
				})
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				patches.Append(mockutils.GetListPodsPatch(&corev1.PodList{}, errors.New("mock list pods error")))

				return patches
			},
			wantErr: false,
		},
		{
			name: "test002",
			fields: fields{
				EtcdClient: nil,
				VpcHandler: nil,
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Updated,
					Kind:       utils.NodeEventKind,
					KeyRef:     "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest",
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher,
						func() *podfetcher.PodFetcher {
							return &podfetcher.PodFetcher{}
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
			name: "test003",
			fields: fields{
				EtcdClient: nil,
				VpcHandler: nil,
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.NodeEventKind,
					KeyRef:     "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest",
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name: "test004",
			fields: fields{
				EtcdClient: nil,
				VpcHandler: nil,
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.PodEventKind,
					KeyRef:     "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest",
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name: "test005",
			fields: fields{
				EtcdClient: nil,
				VpcHandler: nil,
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.NodeEventKind,
					KeyRef:     "/sn/functions/business/yrk/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@puresleep-java-10ms/version/latest",
					Object:     etcdValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &NodeHandler{
				EtcdClient: tt.fields.EtcdClient,
				StopCh:     make(chan struct{}, 1),
			}
			patches := tt.patchFunc()
			NewEventHandler(handler.EtcdClient, make(chan struct{}))
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_needUpdateFuncScheduleNode(t *testing.T) {

	testNode := &corev1.Node{}
	testNode.Name = "test_exit_node"
	testNode.Labels = map[string]string{
		"xxx":     "yyy",
		"test111": cache.NoScheduleLabelSuffix,
	}

	cache.AddNode(testNode)

	defer cache.DeleteNode(testNode)

	tests := []struct {
		name     string
		nodeName string
		label    map[string]string

		expected bool
	}{
		{
			name:     "node has schedule label",
			nodeName: "test_node1",
			label: map[string]string{
				"aaa":  "bbb",
				"test": cache.DefaultLabelValue,
			},

			expected: true,
		},
		{
			name:     "node has no schedule label, no exit node",
			nodeName: "test_node2",
			label: map[string]string{
				"aaa":  "bbb",
				"test": "ccc",
			},

			expected: false,
		},
		{
			name:     "schedule label removed from node",
			nodeName: "test_exit_node",
			label: map[string]string{
				"aaa":  "bbb",
				"test": "ccc",
			},

			expected: true,
		},
		{
			name:     "schedule label are same",
			nodeName: "test_exit_node",
			label: map[string]string{
				"xxx":     "bbbb",
				"test111": cache.NoScheduleLabelSuffix,
				"ddddd":   "eeee",
			},

			expected: false,
		},
		{
			name:     "schedule label added",
			nodeName: "test_exit_node",
			label: map[string]string{
				"xxx":     "bbbb",
				"test111": cache.NoScheduleLabelSuffix,
				"test222": cache.PreferredScheduleLabelSuffix,
				"ddddd":   "eeee",
			},

			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &corev1.Node{}
			node.Name = tt.nodeName
			node.Labels = tt.label

			assert.Equal(t, tt.expected, needUpdateFuncScheduleNode(node), tt.name)
		})
	}
}
