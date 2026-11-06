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

package utils

import (
	"container/list"
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"

	mockUtils "functioncore/test/utils"
)

func TestRetryPodOperationListNotNil(t *testing.T) {
	assert.NotNil(t, RetryPodOperationList, "RetryPodOperationList should not be nil")
}

func TestPodOperationRetryList_AddPodOperation(t *testing.T) {
	type fields struct {
		podOpRetryList *list.List
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{
			name: "case 1 test adding element to retry list",
			fields: fields{
				podOpRetryList: list.New(),
			},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			retryList := &PodOperationRetryList{
				podOpRetryList: tt.fields.podOpRetryList,
				mutex:          sync.Mutex{},
			}
			retryList.AddPodOperation(&PodOperationEle{})
			retryList.AddPodOperation(&PodOperationEle{})
			assert.Equalf(t, tt.want, retryList.podOpRetryList.Len(), "test adding element to retry list")
		})
	}
}

func TestPodOperationRetryList_copyPodOperationList(t *testing.T) {
	type fields struct {
		podOpRetryList *list.List
	}
	type want struct {
		old int
		new int
	}
	tests := []struct {
		name   string
		fields fields
		want   want
	}{
		{
			name: "case 1 test copy retry list",
			fields: fields{
				podOpRetryList: list.New(),
			},
			want: want{
				new: 0,
				old: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RetryPodOperationList := &PodOperationRetryList{
				podOpRetryList: tt.fields.podOpRetryList,
				mutex:          sync.Mutex{},
			}
			RetryPodOperationList.podOpRetryList.PushBack(&PodOperationEle{})
			assert.Equalf(t, tt.want.old, RetryPodOperationList.copyPodOperationList().Len(), "copyPodOperationList()")
			assert.Equalf(t, tt.want.new, RetryPodOperationList.podOpRetryList.Len(), "copyPodOperationList()")
		})
	}
}

func TestStartRetryFailedPodOperation(t *testing.T) {
	type args struct {
		stopCh              chan struct{}
		receiveChanIsClosed bool
	}
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		args        args
	}{
		{
			name: "case 1 goroutine return when stop chan is nil",
			args: args{stopCh: nil},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 2 goroutine return when receive stopChan is closed",
			args: args{
				stopCh:              make(chan struct{}),
				receiveChanIsClosed: true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 3 start retry func after ticker triggered",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					ApplyFunc(retryPodOperationList, func() {
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
			go StartRetryFailedPodOperation(tt.args.stopCh)
			if tt.args.stopCh != nil {
				if tt.args.receiveChanIsClosed {
					timer := time.NewTimer(time.Millisecond * 500)
					<-timer.C
					close(tt.args.stopCh)
					return
				}
				timer := time.NewTimer(time.Second * 1)
				go func(stopCh chan struct{}) {
					<-timer.C
					close(stopCh)
				}(tt.args.stopCh)
				time.Sleep(time.Second * 1)
			}
			patches.ResetAll()
		})
	}
}

func Test_retryPodOperationList(t *testing.T) {
	mockList := &PodOperationRetryList{
		podOpRetryList: list.New(),
		mutex:          sync.Mutex{},
	}
	mockPatchPodOpEle := &PodOperationEle{
		Namespace:               "test-nm",
		PodName:                 "test-pod",
		OperationType:           PatchType,
		StrategicMergePatchType: types.StrategicMergePatchType,
		PatchBytes:              []byte(`{"metadata":{"annotations":{"used":"true"}}`),
		OperationOptions:        metav1.PatchOptions{},
	}
	mockDeletePodOpEle := &PodOperationEle{
		Namespace:        "test-nm",
		OperationType:    DeleteType,
		PodName:          "test-pod",
		OperationOptions: metav1.DeleteOptions{},
	}
	mockPodOpEleErr := &struct{}{}
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1 when kubernetes client is invalid",
			patchesFunc: func() mockUtils.PatchSlice {
				RetryPodOperationList = mockList
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return nil
					}),
				})
				return patches
			},
		}, {
			name: "case 2 if RetryPodOperationList is not initialized",
			patchesFunc: func() mockUtils.PatchSlice {
				RetryPodOperationList = nil
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
				})
				return patches
			},
		}, {
			name: "case 3 if pod operation list has abnormal element",
			patchesFunc: func() mockUtils.PatchSlice {
				RetryPodOperationList = mockList
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					ApplyFunc(retryOperation, func(podEle *PodOperationEle, kubeClient kubernetes.Interface) {
						return
					}),
					ApplyFunc((*PodOperationRetryList).copyPodOperationList, func(_ *PodOperationRetryList) *list.List {
						l := list.New()
						l.PushBack(mockPatchPodOpEle)
						l.PushBack(mockDeletePodOpEle)
						l.PushBack(mockPodOpEleErr)
						return l
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			retryPodOperationList()
			patches.ResetAll()
		})
	}
}

func Test_retryOperation(t *testing.T) {
	mockPatchPodOpEle := &PodOperationEle{
		Namespace:               "test-nm",
		PodName:                 "test-pod",
		OperationType:           PatchType,
		StrategicMergePatchType: types.StrategicMergePatchType,
		PatchBytes:              []byte(`{"metadata":{"annotations":{"used":"true"}}`),
		OperationOptions:        metav1.PatchOptions{},
		Ctx:                     context.TODO(),
	}
	mockDeletePodOpEle := &PodOperationEle{
		Namespace:        "test-nm",
		OperationType:    DeleteType,
		PodName:          "test-pod",
		OperationOptions: metav1.DeleteOptions{},
	}
	type args struct {
		podEle     *PodOperationEle
		kubeClient kubernetes.Interface
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case 1",
			args: args{
				kubeClient: &kubernetes.Clientset{},
				podEle:     mockDeletePodOpEle,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
						func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
							return nil
						}),
					ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
				})
				return patches
			},
		}, {
			name: "case 2",
			args: args{
				kubeClient: &kubernetes.Clientset{},
				podEle:     mockPatchPodOpEle,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Patch",
						func(_ *fake.FakePods, ctx context.Context, name string, pt types.PatchType,
							data []byte, opts metav1.PatchOptions, subResources ...string) (*corev1.Pod, error) {
							return nil, nil
						}),
					ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &fake.FakeCoreV1{}
						}),
					ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
						func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
							return &fake.FakePods{}
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			retryOperation(tt.args.podEle, tt.args.kubeClient)
			patches.ResetAll()
		})
	}
}
