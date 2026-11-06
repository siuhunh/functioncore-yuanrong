/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package podhandler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"

	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func Test_customRuntimeXpu_createPod(t *testing.T) {
	tools.RetryPodOperationList = tools.NewPodOperationRetryList()
	type fields struct {
		nameSpace  string
		kubeClient kubernetes.Interface
		config     config.PoolMgrConfig
	}
	type args struct {
		req *PodCreateReq
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        *corev1.Pod
		wantErr     bool
	}{
		{
			name: "case_01 create pod failed",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{
				PoolList:     config.PoolList{NodeAffinity: &config.NodeAffinity{NodeAffinityKey: "node-role"}},
				GPUPodConfig: config.GPUPodConfig{WaitReadyTimeout: 100}}},
			args: args{req: &PodCreateReq{
				Ctx:    context.TODO(),
				Spec:   &faasfunction.FunctionServiceSpec{},
				PodReq: &common.PodRequest{}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(tools.IsVpaFeatureEnabled, func() bool {
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "case_02 failed to wait pod ready",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{
				PoolList:     config.PoolList{NodeAffinity: &config.NodeAffinity{NodeAffinityKey: "node-role"}},
				GPUPodConfig: config.GPUPodConfig{WaitReadyTimeout: 100}}},
			args: args{req: &PodCreateReq{
				Ctx:    context.TODO(),
				Spec:   &faasfunction.FunctionServiceSpec{},
				PodReq: &common.PodRequest{}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(tools.IsVpaFeatureEnabled, func() bool {
						return true
					}),
					gomonkey.ApplyFunc((*tools.RetryPodOperationList).AddPodOperation,
						func(podOpEle *tools.PodOperationEle) {
							return
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
						func(_ *fake.FakePods, ctx context.Context,
							name string, opts metav1.DeleteOptions) error {
							return fmt.Errorf("error")
						}),
					gomonkey.ApplyFunc((*tools.PodWaiter).WaitStatus,
						func(_ *tools.PodWaiter) <-chan tools.PodStatus {
							p := make(chan tools.PodStatus, 1)
							p <- tools.PodStatus{nil, errors.New("my error")}
							return p
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "case_03 request gpu memory exceeded",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{
				PoolList:     config.PoolList{NodeAffinity: &config.NodeAffinity{NodeAffinityKey: "node-role"}},
				GPUPodConfig: config.GPUPodConfig{WaitReadyTimeout: 100, GPUMemoryCapacity: map[string]int64{"nvidia-a30": 24}}}},
			args: args{req: &PodCreateReq{
				Ctx:  context.TODO(),
				Spec: &faasfunction.FunctionServiceSpec{},
				PodReq: &common.PodRequest{
					GpuMemory: 25000,
					GpuType:   "nvidia-a30",
				}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(tools.IsVpaFeatureEnabled, func() bool {
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		}, {
			name: "case_03 succeed to wait pod ready",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{
				PoolList:     config.PoolList{NodeAffinity: &config.NodeAffinity{NodeAffinityKey: "node-role"}},
				GPUPodConfig: config.GPUPodConfig{WaitReadyTimeout: 100}}},
			args: args{req: &PodCreateReq{
				Ctx:    context.TODO(),
				Spec:   &faasfunction.FunctionServiceSpec{},
				PodReq: &common.PodRequest{}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(tools.IsVpaFeatureEnabled, func() bool {
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) v1.PodInterface {
							return &fake.FakePods{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
						func(_ *fake.FakePods, ctx context.Context,
							pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
							return &corev1.Pod{}, nil
						}),
					gomonkey.ApplyFunc((*tools.PodWaiter).WaitStatus,
						func(_ *tools.PodWaiter) <-chan tools.PodStatus {
							p := make(chan tools.PodStatus, 1)
							p <- tools.PodStatus{&corev1.Pod{}, nil}
							return p
						}),
				})
				return patches
			},
			want:    &corev1.Pod{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := createCustomRuntimeXpuPodHandler(tt.fields.kubeClient, tt.fields.config)
			got, err := c.CreatePod(tt.args.req, &PodTemplate{
				Spec: &corev1.PodSpec{
					Affinity: &corev1.Affinity{NodeAffinity: nil},
					Containers: []corev1.Container{{Name: tools.RuntimeContainerName, Resources: corev1.ResourceRequirements{
						Limits:   make(map[corev1.ResourceName]resource.Quantity, 0),
						Requests: make(map[corev1.ResourceName]resource.Quantity, 0)}}},
				},
				Meta: &metav1.ObjectMeta{
					Annotations:     make(map[string]string),
					Labels:          make(map[string]string),
					OwnerReferences: make([]metav1.OwnerReference, 1),
				},
			})
			if (err != nil) != tt.wantErr {
				t.Errorf("createPod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createPod() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_customRuntimeXpu_updatePod(t *testing.T) {
	var m = sync.Map{}
	m.Store("test", tools.NewPodWaiter())
	var unknownMap = sync.Map{}
	unknownMap.Store("test", "aaa")
	type fields struct {
		waitMap sync.Map
	}
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case_01 unready pod",
			fields: fields{sync.Map{}},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return false
						}),
				})
				return patches
			},
		},
		{
			name:   "case_02 ready but not exist pod",
			fields: fields{sync.Map{}},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
		},
		{
			name:   "case_03 ready but assert error pod status",
			fields: fields{unknownMap},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
		},
		{
			name:   "case_04 ready and notify pod status",
			fields: fields{m},
			args:   args{&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady,
						func(pod *corev1.Pod) bool {
							return true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &customRuntimeXpuPodHandler{
				waitMap: tt.fields.waitMap,
			}
			c.UpdatePod(tt.args.pod)
			patches.ResetAll()
		})
	}
}

func Test_customRuntimeXpuPodHandler_CreatePod_retryWhenStartError(t *testing.T) {
	createCount := uint32(0)
	deleteCount := uint32(0)
	c := &customRuntimeXpuPodHandler{
		kubeClient: &kubernetes.Clientset{},
		config: config.PoolMgrConfig{
			PoolList:     config.PoolList{NodeAffinity: &config.NodeAffinity{NodeAffinityKey: "node-role"}},
			GPUPodConfig: config.GPUPodConfig{WaitReadyTimeout: 100},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
		gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1", func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
			return &fake.FakeCoreV1{}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods", func(_ *fake.FakeCoreV1, _ string) typecorev1.PodInterface {
			return &fake.FakePods{}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete", func(_ *fake.FakePods, _ context.Context, _ string, _ metav1.DeleteOptions) error {
			atomic.AddUint32(&deleteCount, 1)
			return nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create", func(_ *fake.FakePods, ctx context.Context, pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
			go func() {
				for {
					<-time.After(50 * time.Millisecond)
					_, ok := c.waitMap.Load(pod.Name)
					if !ok {
						continue
					}
					if atomic.AddUint32(&createCount, 1) < 3 {
						c.UpdatePod(&corev1.Pod{
							ObjectMeta: metav1.ObjectMeta{Name: pod.Name},
							Status: corev1.PodStatus{
								ContainerStatuses: []corev1.ContainerStatus{
									{
										Name: "runtime",
										State: corev1.ContainerState{
											Terminated: &corev1.ContainerStateTerminated{
												Reason:  "StartError",
												Message: `failed to create containerd task: failed to create shim task: Others("other error: container_linux.go:340: starting container process caused \"process_linux.go:380: container init caused \\\"process_linux.go:363: running prestart hook 0:/usr/bin/nvidia-container-runtime-hook,/usr/bin/nvidia-container-runtime-hook,prestart, ContainerID: 116d2bb9094ba9efcc866c6b652d93db24b0b3fe61b8037d35a1cc57cf85a72a caused \\\\\\\"error running hook: exit status 1, stdout: , stderr: Auto-detected mode as ''legacy''\\\\\\\\nnvidia-container-cli: create xgpu container error: create container failed, errno=-1\\\\\\\\n\\\\\\\"\\\"\""): unknown`,
											},
										},
									},
								},
							},
						})
						return
					}
					cancel()
					return
				}
			}()
			return pod.DeepCopy(), nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	_, _ = c.CreatePod(&PodCreateReq{
		Spec:   &faasfunction.FunctionServiceSpec{},
		PodReq: &common.PodRequest{},
		Ctx:    ctx,
	}, &PodTemplate{
		Spec: &corev1.PodSpec{
			Affinity: &corev1.Affinity{NodeAffinity: nil},
			Containers: []corev1.Container{{Name: tools.RuntimeContainerName, Resources: corev1.ResourceRequirements{
				Limits:   make(map[corev1.ResourceName]resource.Quantity, 0),
				Requests: make(map[corev1.ResourceName]resource.Quantity, 0)}}},
		},
		Meta: &metav1.ObjectMeta{
			Annotations:     make(map[string]string),
			Labels:          make(map[string]string),
			OwnerReferences: make([]metav1.OwnerReference, 1),
		},
	})
	assert.Equal(t, uint32(3), atomic.LoadUint32(&createCount))
	assert.Equal(t, uint32(3), atomic.LoadUint32(&deleteCount))
}

func Test_customRuntimeXpu_filter(t *testing.T) {
	mgr := &customRuntimeXpuPodHandler{}
	{
		req := &PodCreateReq{Spec: &faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{}},
			PodReq: &common.PodRequest{}}
		req.Spec.Configuration.Runtime.Language = "java"
		res := mgr.FilterByPodCreateReq(req)
		assert.Equal(t, false, res)
	}
	{
		req := &PodCreateReq{Spec: &faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{}},
			PodReq: &common.PodRequest{}}
		req.Spec.Configuration.Runtime.Language = "custom-runtime"
		req.PodReq.GpuMemory = 0
		res := mgr.FilterByPodCreateReq(req)
		assert.Equal(t, false, res)
	}
	{
		req := &PodCreateReq{Spec: &faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{}},
			PodReq: &common.PodRequest{}}
		req.Spec.Configuration.Runtime.Language = "custom-runtime"
		req.PodReq.GpuMemory = 1024
		res := mgr.FilterByPodCreateReq(req)
		assert.Equal(t, true, res)
	}
}
