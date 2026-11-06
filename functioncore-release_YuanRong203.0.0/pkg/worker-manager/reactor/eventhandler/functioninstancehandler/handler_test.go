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

// Package functioninstancehandler
package functioninstancehandler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/listers/core/v1"
	k8cache "k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/detector"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/podhandler"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockUtils "functioncore/test/utils"
)

func TestFunctionInstanceHandler_HandleEvent(t *testing.T) {
	type fields struct {
		EtcdClient  *etcd3.EtcdClient
		podFetcher  *podfetcher.PodFetcher
		poolManager *poolmanager.PoolMgr
		VpcHandler  *vpcmanager.VpcHandler
		podLister   v1.PodLister
		podDetector *detector.PodDetector
	}
	type args struct {
		event eventsource.Event
	}
	etcdValue := &etcd3.Event{
		Type:      0,
		Key:       "",
		Value:     []byte(mockUtils.ETCDFuncValueForTest),
		PrevValue: nil,
		Rev:       0,
	}
	nodeValue := &corev1.Pod{}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockUtils.PatchesFunc
		wantErr   bool
	}{
		{
			name: "test01",
			fields: fields{
				EtcdClient:  nil,
				podFetcher:  nil,
				poolManager: nil,
				VpcHandler:  nil,
				podLister:   mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Added,
					Kind:       utils.FunctionRepoEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     etcdValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager,
						func() *poolmanager.PoolMgr {
							return &poolmanager.PoolMgr{}
						}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).DoActions, func(_ *podfetcher.PodFetcher,
						funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test02",
			fields: fields{
				EtcdClient:  &etcd3.EtcdClient{},
				podFetcher:  &podfetcher.PodFetcher{},
				poolManager: &poolmanager.PoolMgr{},
				VpcHandler:  &vpcmanager.VpcHandler{},
				podLister:   mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Added,
					Kind:       utils.PodEventKind,
					KeyRef:     "mypod",
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "", "", nil
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager,
						func() *poolmanager.PoolMgr {
							return &poolmanager.PoolMgr{}
						}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test03",
			fields: fields{
				EtcdClient: &etcd3.EtcdClient{},
				podFetcher: &podfetcher.PodFetcher{
					KubeClient: &kubernetes.Clientset{},
				},
				poolManager: &poolmanager.PoolMgr{},
				VpcHandler:  &vpcmanager.VpcHandler{},
				podLister:   mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
				podDetector: &detector.PodDetector{},
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Added,
					Kind:       utils.PodEventKind,
					KeyRef:     "mypod",
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "default", "pod1", nil
						}),
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher,
						func() *podfetcher.PodFetcher {
							return &podfetcher.PodFetcher{}
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager,
						func() *poolmanager.PoolMgr {
							return &poolmanager.PoolMgr{}
						}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&detector.PodDetector{}), "Store",
						func(_ *detector.PodDetector, pod *corev1.Pod) {}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test04",
			fields: fields{
				EtcdClient:  &etcd3.EtcdClient{},
				podFetcher:  &podfetcher.PodFetcher{},
				poolManager: &poolmanager.PoolMgr{},
				VpcHandler:  &vpcmanager.VpcHandler{},
				podLister:   mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  eventsource.Updated,
					Kind:       utils.PodEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "default", "pod1", nil
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager,
						func() *poolmanager.PoolMgr {
							return &poolmanager.PoolMgr{}
						}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&detector.PodDetector{}), "Store",
						func(_ *detector.PodDetector, pod *corev1.Pod) {}),
					gomonkey.ApplyFunc((*podhandler.Manager).FilterByPodName,
						func(_ *podhandler.Manager, podName string) podhandler.PodHandler {
							return nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test05",
			fields: fields{
				EtcdClient:  &etcd3.EtcdClient{},
				podFetcher:  &podfetcher.PodFetcher{},
				poolManager: &poolmanager.PoolMgr{},
				VpcHandler:  &vpcmanager.VpcHandler{},
				podLister:   mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.PodEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "default", "pod1", nil
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager,
						func() *poolmanager.PoolMgr {
							return &poolmanager.PoolMgr{}
						}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// funSvcId is not empty and test handler.dealWithReadyInstance method
		{
			name: "test06",
			fields: fields{
				EtcdClient:  &etcd3.EtcdClient{},
				podFetcher:  &podfetcher.PodFetcher{},
				poolManager: &poolmanager.PoolMgr{},
				VpcHandler:  &vpcmanager.VpcHandler{},
				podLister:   mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.PodEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "default", "pod1", nil
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager,
						func() *poolmanager.PoolMgr {
							return &poolmanager.PoolMgr{}
						}),
					gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "mock-funcSvcID", true
					}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
			wantErr: true,
		},
		// mock pod fetch is nil
		{
			name: "test08",
			fields: fields{
				EtcdClient:  &etcd3.EtcdClient{},
				podFetcher:  &podfetcher.PodFetcher{},
				poolManager: &poolmanager.PoolMgr{},
				VpcHandler:  &vpcmanager.VpcHandler{},
				podLister:   mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
			},
			args: args{
				event: eventsource.Event{
					SourceType: "",
					EventType:  "",
					Kind:       utils.PodEventKind,
					KeyRef:     mockUtils.ETCDFuncKeyForTest,
					Object:     nodeValue,
					StartTime:  time.Now(),
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "default", "pod1", nil
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager,
						func() *poolmanager.PoolMgr {
							return &poolmanager.PoolMgr{}
						}),
					gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "mock-funcSvcID", true
					}),
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return nil
					}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
			wantErr: true,
		},
		// mock poolManager is nil
		{
			name: "mock pool manager is nil",
			args: args{
				event: eventsource.Event{},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
			wantErr: true,
		},
		// mock pool manager is nil and event.Object is nil
		{
			name: "mock pool manager is nil and event.Object is nil",
			args: args{
				event: eventsource.Event{
					Kind: utils.PodEventKind,
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "default", "pod1", errors.New("mock error")
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return &vpcmanager.VpcHandler{}, nil
						}),
					gomonkey.ApplyFunc(utils.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "mock GetVPCController error",
			args: args{
				event: eventsource.Event{
					Kind: utils.PodEventKind,
				},
			},
			patchFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(k8cache.SplitMetaNamespaceKey,
						func(string) (string, string, error) {
							return "default", "pod1", errors.New("mock error")
						}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController,
						func() (*vpcmanager.VpcHandler, error) {
							return nil, errors.New("test")
						}),
					gomonkey.ApplyFunc(os.Exit, func(code int) {}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &FunctionInstanceHandler{
				EtcdClient:  tt.fields.EtcdClient,
				podFetcher:  tt.fields.podFetcher,
				poolManager: tt.fields.poolManager,
				VpcHandler:  tt.fields.VpcHandler,
				podLister:   tt.fields.podLister,
			}
			patches := tt.patchFunc()
			newHandler := NewEventHandler(handler.EtcdClient, handler.podFetcher, handler.podLister)
			if newHandler != nil {
				assert.Equal(t, handler.podLister, newHandler.podLister)
				assert.Equal(t, handler.EtcdClient, newHandler.EtcdClient)
				assert.Equal(t, handler.podFetcher, newHandler.podFetcher)
			}
			if err := handler.HandleEvent(tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("HandleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestFunctionInstanceHandler_handleNonPoolPod(t *testing.T) {
	defer gomonkey.ApplyMethod(reflect.TypeOf(&podfetcher.PodFetcher{}), "PrintPodEvents", func(_ *podfetcher.PodFetcher, _ string) {}).Reset()
	type fields struct {
		EtcdClient  *etcd3.EtcdClient
		podFetcher  *podfetcher.PodFetcher
		poolManager *poolmanager.PoolMgr
		VpcHandler  *vpcmanager.VpcHandler
	}
	type args struct {
		pod   *corev1.Pod
		event eventsource.Event
	}
	mockPodFetcher := &podfetcher.PodFetcher{}
	mockPod := &corev1.Pod{}
	mockPod.Labels = map[string]string{"applier": constants.WorkerManagerApplier}
	mockReadyPod := &corev1.Pod{}
	mockReadyPod.Status.Conditions = []corev1.PodCondition{{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}}
	mockDeletingPod := &corev1.Pod{}
	mockDeletingPod.DeletionTimestamp = &metav1.Time{}
	mockBridgePod := &corev1.Pod{}
	mockBridgePod.Labels = map[string]string{"isBridgeFunction": "true"}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		// test pod not ready
		{
			name: "test001",
			args: args{
				pod: &corev1.Pod{},
				event: eventsource.Event{
					EventType: eventsource.Deleted,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "DeletePod",
						func(_ *podfetcher.PodFetcher, pod *corev1.Pod, traceID string) {
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// test delete bridge function
		{
			name: "test_delete_bridge_function",
			args: args{
				pod: mockBridgePod,
				event: eventsource.Event{
					EventType: eventsource.Deleted,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "DeletePod",
						func(_ *podfetcher.PodFetcher, pod *corev1.Pod, traceID string) {
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// test delete reserved instance
		{
			name: "test002",
			args: args{
				pod: mockPod,
				event: eventsource.Event{
					EventType: eventsource.Deleted,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "DeletePod",
						func(_ *podfetcher.PodFetcher, pod *corev1.Pod, traceID string) {
						}),
				})
				return patches
			},
			wantErr: false,
		},
		// test ready pod but deletion timestamp is nil
		{
			name: "test003",
			args: args{
				pod: mockReadyPod,
				event: eventsource.Event{
					EventType: eventsource.Deleted,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		},
		// test delete ready pod
		{
			name: "test004",
			args: args{
				pod: mockReadyPod,
				event: eventsource.Event{
					EventType: eventsource.Deleted,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "funcSvcID", true
					}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							fmt.Println("succeed to mock IdleDeletePatServiceList")
							return nil
						}),
				})
				return patches
			},
			wantErr: true,
		},
		// test deleting pod with update event
		{
			name: "test005",
			args: args{
				pod: mockDeletingPod,
				event: eventsource.Event{
					EventType: eventsource.Updated,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "funcSvcID", true
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test custom container pod and worker is ready",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "custom-123456-001"},
					Status: corev1.PodStatus{
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:  utils.WorkerContainerName,
								Ready: true,
							},
						},
					},
				},
				event: eventsource.Event{
					EventType: eventsource.Updated,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).DeletePod,
						func(_ *podfetcher.PodFetcher, pod *corev1.Pod, traceID string) {
							fmt.Println("succeed to mock delete pod")
							return
						}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).IdleDeletePatServiceList,
						func(_ *vpcmanager.VpcHandler, instanceName string) error {
							fmt.Println("succeed to mock IdleDeletePatServiceList")
							return nil
						}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test custom container pod and worker is not ready and return error of pod fetcher is nil",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "custom-123456-001"},
					Status: corev1.PodStatus{
						ContainerStatuses: []corev1.ContainerStatus{
							{
								Name:  utils.WorkerContainerName,
								Ready: false,
							},
						},
					},
				},
				event: eventsource.Event{
					EventType: eventsource.Updated,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).DeletePodByK8S,
						func(_ *podfetcher.PodFetcher, _, _ string) error {
							fmt.Println("succeed to mock delete pod by force")
							return nil
						}),
					gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "funcSvcID", true
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &FunctionInstanceHandler{
				EtcdClient:  tt.fields.EtcdClient,
				podFetcher:  tt.fields.podFetcher,
				poolManager: tt.fields.poolManager,
				VpcHandler:  tt.fields.VpcHandler,
			}
			patches := tt.patchesFunc()
			if err := handler.handleNonPoolPod(tt.args.pod, tt.args.event); (err != nil) != tt.wantErr {
				t.Errorf("handleNonPoolPod() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(1 * time.Second)
			patches.ResetAll()
		})
	}
}

func TestFunctionInstanceHandler_dealWithReadyInstance(t *testing.T) {
	predictionPod := &corev1.Pod{}
	predictionPod.Labels = make(map[string]string)
	predictionPod.Labels[utils.LabelApplier] = constants.PredictionApplier
	type fields struct {
		EtcdClient  *etcd3.EtcdClient
		podFetcher  *podfetcher.PodFetcher
		poolManager *poolmanager.PoolMgr
		VpcHandler  *vpcmanager.VpcHandler
	}
	type args struct {
		pod       *corev1.Pod
		event     eventsource.Event
		funcSvcId string
	}
	mockPodFetcher := &podfetcher.PodFetcher{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test dealWithReadyInstance with delete event type",
			args: args{
				pod: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
					Name:      "pod1_test22",
					Namespace: "default",
					Labels:    map[string]string{"funcName": "test2", "businessType": "CAE"},
				}},
				funcSvcId: "mock-funcSvcID",
				event: eventsource.Event{
					EventType: evs.Deleted,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
						instance v1alpha1.WorkerInstance) {
						return
					}),
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{}
					}),
					gomonkey.ApplyFunc(etcdhandler.DeleteInstanceFromETCD, func(ctx context.Context,
						instance v1alpha1.WorkerInstance) {
						return
					}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).DoActions, func(_ *podfetcher.PodFetcher,
						funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
						return
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(mockPodFetcher), "CreateNewContext",
						func(_ *podfetcher.PodFetcher, _ string) context.Context {
							return context.Background()
						}),
					gomonkey.ApplyFunc(cache.WorkerCache.HandleInstance, func(funcSvcID string, event *cache.InstanceMeta) ([]cache.Action, cache.Metadata) {
						return []cache.Action{}, cache.Metadata{}
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "test dealWithReadyInstance with unknown event type",
			args: args{
				pod:       &corev1.Pod{},
				funcSvcId: "mock-funcSvcID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
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
			name: "test dealWithReadyInstance with prediction event",
			args: args{
				pod:       predictionPod,
				funcSvcId: "mock-funcSvcID",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := &FunctionInstanceHandler{
				EtcdClient:  tt.fields.EtcdClient,
				podFetcher:  tt.fields.podFetcher,
				poolManager: tt.fields.poolManager,
				VpcHandler:  tt.fields.VpcHandler,
			}
			patches := tt.patchesFunc()
			if err := handler.dealWithReadyInstance(tt.args.pod, tt.args.event, tt.args.funcSvcId, true, podfetcher.CREvent{}); (err != nil) != tt.wantErr {
				t.Errorf("dealWithReadyInstance() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
			time.Sleep(100 * time.Millisecond)
		})
	}
}

func Test_handleVPAPodDelete(t *testing.T) {
	var (
		event = eventsource.Event{
			EventType: evs.Deleted,
		}
		pod = &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pause-pod-test123",
			},
		}
		patch = gomonkey.NewPatches()
	)

	convey.Convey("test handleVPAPodDelete", t, func() {
		convey.Convey("test add vpa pod error", func() {
			mockNewVpaPausePod(patch, &vpa.VerticalPausePod{})
			mockAddVpaPod(patch, errors.New("add vpa pod error"))
			err := handleVPAPodDelete(event, pod)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "add vpa pod error")
		})

		convey.Convey("test add vpa pod success", func() {
			mockNewVpaPausePod(patch, &vpa.VerticalPausePod{})
			mockAddVpaPod(patch, nil)
			err := handleVPAPodDelete(event, pod)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test other event should return", func() {
			event.EventType = evs.Added
			err := handleVPAPodDelete(event, pod)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("pod spec node name is empty", func() {
			mockNewVpaPausePod(patch, &vpa.VerticalPausePod{})
			patch.ApplyMethod(reflect.TypeOf(&vpa.VerticalPausePod{}), "AddVpaPod",
				func(_ *vpa.VerticalPausePod, ctx context.Context, nodeName string, req *corev1.ResourceRequirements) error {
					convey.So(nodeName, convey.ShouldEqual, "test123")
					return nil
				})
			err := handleVPAPodDelete(event, pod)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func mockNewVpaPausePod(patch *gomonkey.Patches, vpaPod vpa.VerticalPod) {
	patch.ApplyFunc(vpa.NewVpaPausePod, func(opts ...vpa.Option) vpa.VerticalPod {
		return vpaPod
	})
}

func mockAddVpaPod(patch *gomonkey.Patches, err error) {
	patch.ApplyMethod(reflect.TypeOf(&vpa.VerticalPausePod{}), "AddVpaPod",
		func(_ *vpa.VerticalPausePod, ctx context.Context, nodeName string, req *corev1.ResourceRequirements) error {
			return err
		})
}

func TestUpdateWorkerInsInfo(t *testing.T) {
	convey.Convey("TestUpdateWorkerInsInfo", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc((*podfetcher.CREvent).GetFuncMeta, func() *faasfunction.FunctionService {
				return &faasfunction.FunctionService{
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							BusinessType:    "BusinessType",
							BusinessID:      "BusinessID",
							TenantID:        "TenantID",
							FunctionName:    "FunctionName",
							FunctionVersion: "FunctionVersion",
						},
					},
				}
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		workerIns := v1alpha1.WorkerInstance{}
		fillWorkerInsInfo(&workerIns, "TestUpdateWorkerInsInfo", podfetcher.CREvent{})
		convey.So(len(workerIns.BusinessType) != 0, convey.ShouldBeTrue)
		convey.So(len(workerIns.BusinessID) != 0, convey.ShouldBeTrue)
		convey.So(len(workerIns.TenantID) != 0, convey.ShouldBeTrue)
		convey.So(len(workerIns.FuncName) != 0, convey.ShouldBeTrue)
		convey.So(len(workerIns.Version) != 0, convey.ShouldBeTrue)
	})
}

func Test_isNeedDeletePod(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		expected bool
	}{
		{
			name: "no worker container",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{},
			}},
			expected: true,
		},
		{
			name: "runtime not ready, but worker ready",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: utils.WorkerContainerName, Ready: true},
					{Name: utils.RuntimeContainerName, Ready: false},
				},
			}},
			expected: false,
		},
		{
			name: "runtime ready, but worker not ready",
			pod: &corev1.Pod{Status: corev1.PodStatus{
				ContainerStatuses: []corev1.ContainerStatus{
					{Name: utils.WorkerContainerName, Ready: false},
					{Name: utils.RuntimeContainerName, Ready: true},
				},
			}},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, isNeedDeletePod(tt.pod), tt.name)
		})
	}
}

func Test_parseCAERouterDeleteDelay(t *testing.T) {
	defer os.Unsetenv("CAE_ROUTER_DELETE_DELAY")

	tests := []struct {
		name     string
		env      string
		expected time.Duration
	}{
		{
			name:     "empty",
			expected: defaultCAERouterDeleteDelay,
		},
		{
			name:     "invalid num",
			env:      "aaaa",
			expected: defaultCAERouterDeleteDelay,
		},
		{
			name:     "valid num",
			env:      "900",
			expected: 900 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("CAE_ROUTER_DELETE_DELAY", tt.env)
			assert.Equal(t, parseCAERouterDeleteDelay(), tt.expected, tt.name)
		})
	}
}

func Test_calCurrentCPUUsage(t *testing.T) {
	convey.Convey("test calCurrentCPUUsage", t, func() {
		config.EnableHCSLicense = true
		convey.Convey("case1: pod not owned by function service", func() {
			isCalled := false
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
					return "", false
				}),
				gomonkey.ApplyFunc(utils.ReleaseCPUResource, func(cpu int64) {
					isCalled = true
				}),
			})
			defer patches.ResetAll()
		
			pod := &corev1.Pod{}
			event := eventsource.Event{
				EventType: eventsource.Deleted,
			}
			calCurrentCPUUsage(pod, event)
			convey.So(isCalled, convey.ShouldBeFalse)
		})
		convey.Convey("case2: pod is destroyed to release cpu resources", func() {
			isCalled := false
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
					return "", true
				}),
				gomonkey.ApplyFunc(utils.ReleaseCPUResource, func(cpu int64) {
					isCalled = true
				}),
			})
			defer patches.ResetAll()
			
			pod := &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name: utils.RuntimeContainerName,
					}},
				},
			}
			event := eventsource.Event{
				EventType: eventsource.Deleted,
			}
			calCurrentCPUUsage(pod, event)
			convey.So(isCalled, convey.ShouldBeTrue)
		})
	})
}
