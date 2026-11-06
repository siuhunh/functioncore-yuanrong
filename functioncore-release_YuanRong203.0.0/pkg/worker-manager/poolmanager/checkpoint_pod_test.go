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

package poolmanager

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"

	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/common"
	"functioncore/pkg/worker-manager/config"
	tools "functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func Test_checkpointPodMGR_createPod(t *testing.T) {
	tools.RetryPodOperationList = tools.NewPodOperationRetryList()
	type fields struct {
		nameSpace  string
		kubeClient kubernetes.Interface
		config     config.PoolMgrConfig
	}
	type args struct {
		ctx context.Context
		req common.PodRequest
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
			name:   "case_01 create pod failed",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{CheckpointPodConfig: config.CheckpointPodConfig{WaitReadyTimeout: 100}}},
			args:   args{ctx: context.TODO(), req: common.PodRequest{}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyFunc(tools.IsVpaFeatureEnabled, func() bool {
						return true
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
			name:   "case_02 failed to wait pod ready",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: nil}}},
			args:   args{ctx: context.TODO(), req: common.PodRequest{ForceDeploy: true}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*tools.RetryPodOperationList).AddPodOperation,
						func(podOpEle *tools.PodOperationEle) {
							return
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyFunc(tools.IsVpaFeatureEnabled, func() bool {
						return true
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
			name:   "case_03 succeed to wait pod ready",
			fields: fields{nameSpace: "default", kubeClient: &kubernetes.Clientset{}, config: config.PoolMgrConfig{PoolList: config.PoolList{NodeAffinity: nil}}},
			args:   args{ctx: context.TODO(), req: common.PodRequest{ForceDeploy: true}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					gomonkey.ApplyFunc(tools.IsVpaFeatureEnabled, func() bool {
						return true
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
			c := newCheckpointPodMGR(tt.fields.kubeClient, tt.fields.config)
			got, err := c.createPod(tt.args.ctx, &Pool{Deployment: &appsv1.Deployment{
				Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{corev1.Container{Env: make([]corev1.EnvVar, 0)}},
						},
						ObjectMeta: metav1.ObjectMeta{
							Annotations:     make(map[string]string),
							Labels:          make(map[string]string),
							OwnerReferences: make([]metav1.OwnerReference, 1),
						},
					},
				},
			}}, tt.args.req, "")
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

func Test_checkpointPodMGR_updatePod(t *testing.T) {
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
			c := &checkpointPodMGR{
				waitMap: tt.fields.waitMap,
			}
			c.updatePod(tt.args.pod)
			patches.ResetAll()
		})
	}
}

func Test_checkpointPodMGR_filter(t *testing.T) {
	mgr := &checkpointPodMGR{}
	{
		req := common.PodRequest{}
		res := mgr.filter(req)
		assert.Equal(t, res, false)
	}
	{
		req := common.PodRequest{SnapshotDeployment: &types.SnapshotDeploymentSpec{BucketID: "id"}}
		res := mgr.filter(req)
		assert.Equal(t, res, true)
	}
	{
		snapshotmanager.SetSnapshotManagerMode(true)
		req := common.PodRequest{SnapshotDeployment: &types.SnapshotDeploymentSpec{BucketID: "id", SnapshotSize: 123123}}
		res := mgr.filter(req)
		assert.Equal(t, res, true)
		snapshotmanager.SetSnapshotManagerMode(false)
	}
}
