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
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"

	"functioncore/pkg/common/constants"
	mockutils "functioncore/test/utils"
)

func TestCreatePod(t *testing.T) {
	RetryPodOperationList = NewPodOperationRetryList()
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
		want        *corev1.Pod
		wantErr     bool
	}{
		{
			name: "case_01 create pod failed",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
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
		},
		{
			name: "case_02 create pod failed due to an error was encountered while waiting for the pod to be ready",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*RetryPodOperationList).AddPodOperation,
						func(podOpEle *PodOperationEle) {
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
					gomonkey.ApplyFunc((*PodWaiter).WaitStatus,
						func(_ *PodWaiter) <-chan PodStatus {
							p := make(chan PodStatus, 1)
							p <- PodStatus{nil, errors.New("wait ready error")}
							return p
						}),
				})
				return patches
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "case_03 pod is successfully created and the pod is ready to start",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
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
					gomonkey.ApplyFunc((*PodWaiter).WaitStatus,
						func(_ *PodWaiter) <-chan PodStatus {
							p := make(chan PodStatus, 1)
							p <- PodStatus{&corev1.Pod{}, nil}
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
			podTemplate := &CreatePodTemplate{
				TraceID:          "mock-traceID",
				WaitReadyTimeout: 10,
				Spec: &corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  RuntimeContainerName,
							Image: "mock-runtime-image",
						},
						{
							Name:  WorkerContainerName,
							Image: "mock-worker-image",
						},
					},
					InitContainers: []corev1.Container{
						{
							Name:  WorkerInitContainerName,
							Image: "init-image",
						},
					},
				},
				ObjectMeta: &metav1.ObjectMeta{
					Name:      "mock-pod-name",
					Namespace: "mock-namespace",
					Labels: map[string]string{
						"mock-key": "mock-value",
					},
				},
			}
			ctx := context.TODO()
			pod, err := CreatePod(ctx, podTemplate)
			if (err != nil) != tt.wantErr {
				t.Errorf("createPod() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(pod, tt.want) {
				t.Errorf("createPod() got = %v, want %v", pod, tt.want)
			}
			patches.ResetAll()
		})
	}

	testContextDone := struct {
		name        string
		patchesFunc mockutils.PatchesFunc
		want        *corev1.Pod
		wantErr     bool
	}{
		name: "case_04 create pod failed due to timeout",
		patchesFunc: func() mockutils.PatchSlice {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
					return &kubernetes.Clientset{}
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
						return nil
					}),
			})
			return patches
		},
		want:    nil,
		wantErr: true,
	}
	t.Run(testContextDone.name, func(t *testing.T) {
		patches := testContextDone.patchesFunc()
		podTemplate := &CreatePodTemplate{
			Spec: &corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  RuntimeContainerName,
						Image: "mock-runtime-image",
					},
					{
						Name:  WorkerContainerName,
						Image: "mock-worker-image",
					},
				},
				InitContainers: []corev1.Container{
					{
						Name:  WorkerInitContainerName,
						Image: "init-image",
					},
				},
			},
			ObjectMeta: &metav1.ObjectMeta{
				Name:      "mock-pod-name",
				Namespace: "mock-namespace",
				Labels: map[string]string{
					"mock-key": "mock-value",
				},
			},
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		pod, err := CreatePod(ctx, podTemplate)
		if (err != nil) != testContextDone.wantErr {
			t.Errorf("createPod() error = %v, wantErr %v", err, testContextDone.wantErr)
			return
		}
		if !reflect.DeepEqual(pod, testContextDone.want) {
			t.Errorf("createPod() got = %v, want %v", pod, testContextDone.want)
		}
		patches.ResetAll()
	})
}

func TestDeletePod(t *testing.T) {
	type args struct {
		pod *corev1.Pod
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "mock-pod-name",
						Labels: map[string]string{LabelShortFuncName: "mock-func-name"},
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				kubeClient := &kubernetes.Clientset{}
				corev1Client := &v1.CoreV1Client{}
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(FunctionNameSpace, func() string { return "default" }),
					gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return corev1Client
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
						func(_ *v1.CoreV1Client, namespace string) v1.PodInterface {
							return mockutils.NewK8sPodImp(namespace)
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
						ctx context.Context, name string, opts metav1.DeleteOptions) error {
						return nil
					}),
					gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return kubeClient
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test success
		{
			name: "test success",
			args: args{
				pod: &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:   "mock-pod-name",
						Labels: map[string]string{LabelShortFuncName: "mock-func-name"},
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				kubeClient := &kubernetes.Clientset{}
				corev1Client := &v1.CoreV1Client{}
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(FunctionNameSpace, func() string { return "default" }),
					gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
						func(_ *kubernetes.Clientset) v1.CoreV1Interface {
							return corev1Client
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
						func(_ *v1.CoreV1Client, namespace string) v1.PodInterface {
							return mockutils.NewK8sPodImp(namespace)
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
						ctx context.Context, name string, opts metav1.DeleteOptions) error {
						return errors.New("mock delete pod error")
					}),
					gomonkey.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
						return kubeClient
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test failed with delete error
		{
			name: "test success with nil pod",
			args: args{},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		}, // test success with nil pod
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			if err := DeletePod(tt.args.pod); (err != nil) != tt.wantErr {
				t.Errorf("expect %v but found %v", tt.wantErr, err)
			}
		})
		patches.ResetAll()
	}
}

// TestForciblyDeletePod -
func TestForciblyDeletePod(t *testing.T) {
	convey.Convey("test: test forcibly delete pod", t, func() {
		patches := initPodsPatches()
		defer patches.Reset()
		convey.Convey("test: err", func() {
			patch := gomonkey.ApplyFunc((*mockutils.K8sPodImp).Delete, func(_ *mockutils.K8sPodImp,
				ctx context.Context, name string, opts metav1.DeleteOptions) error {
				return errors.New("test err")
			})
			defer patch.Reset()
			err := ForciblyDeletePod(&corev1.Pod{})
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("test: success", func() {
			err := ForciblyDeletePod(&corev1.Pod{})
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func initPodsPatches() *gomonkey.Patches {
	kubeClient := &kubernetes.Clientset{}
	corev1Client := &v1.CoreV1Client{}
	patch := gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
		func(_ *kubernetes.Clientset) v1.CoreV1Interface {
			return corev1Client
		})
	patch.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
		func(_ *v1.CoreV1Client, namespace string) v1.PodInterface {
			return mockutils.NewK8sPodImp(namespace)
		})
	patch.ApplyFunc(GetKubernetesClient, func() kubernetes.Interface {
		return kubeClient
	})
	return patch
}

func TestNotifyPodReadyStatus(t *testing.T) {
	convey.Convey("case01: pod is not ready", t, func() {
		pod := &corev1.Pod{}
		patch := gomonkey.ApplyFunc(IsPodReady, func(pod *corev1.Pod) bool {
			return false
		})
		defer patch.Reset()
		NotifyPodReadyStatus(pod)
	})

	convey.Convey("case02: pod is ready", t, func() {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "mock-pod"}}
		patch := gomonkey.ApplyFunc(IsPodReady, func(pod *corev1.Pod) bool {
			return true
		})
		defer patch.Reset()
		convey.Convey("when the pod ready status has been notified, it will not exist in the waiter map",
			func() {
				waiter := NewPodWaiter()
				waitMap.Store("not-exist-pod", waiter)
				NotifyPodReadyStatus(pod)
				waitMap.LoadAndDelete("not-exist-pod")
			})

		convey.Convey("when the pod ready status not be notified, it exists in the waiter map", func() {

			convey.Convey("podWaiter does not exist, assertion failed", func() {
				waitMap.Store("mock-pod", "not-exist-waiter")
				NotifyPodReadyStatus(pod)
				waitMap.Delete("mock-pod")
			})

			convey.Convey("podWaiter does exist, the pod ready status notification is successful", func() {
				waitMap.Store("mock-pod", NewPodWaiter())
				NotifyPodReadyStatus(pod)
				waitMap.Delete("mock-pod")
			})
		})
	})
}

func TestIsPodStuckTerminating(t *testing.T) {
	tests := []struct {
		name     string
		pod      *corev1.Pod
		excepted bool
	}{
		{
			name:     "empty pod",
			pod:      &corev1.Pod{},
			excepted: false,
		},
		{
			name: "restarted running pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 8*time.Second)},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 7*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 6*time.Second)},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "restarted initializing pod",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 8*time.Second)},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 7*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 6*time.Second)},
								},
							},
						},
					},
				},
			},
			excepted: true,
		},
		{
			name: "all terminated and reach stuckTerminatingTimeout",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 8*time.Second)},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 7*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 6*time.Second)},
								},
							},
						},
					},
				},
			},
			excepted: true,
		},
		{
			name: "no init container, all container running",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "only init container running",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "init container terminated, all container running",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "init container terminated, some containers running and some terminated",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "all terminated, but init not finish terminate",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									StartedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 8*time.Second)},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 7*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 6*time.Second)},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "all terminated, but not all finish terminate",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 8*time.Second)},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									StartedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 7*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 6*time.Second)},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "all terminated, but not reach stuckTerminatingTimeout",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "one container state waiting, last state terminating， one container state terminating and not reach stuckTerminatingTimeout",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "one container state waiting, last state terminating, one container state terminating and reach stuckTerminatingTimeout",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
					},
				},
			},
			excepted: true,
		},
		{
			name: "two container state waiting, last state terminating and two not reach stuckTerminatingTimeout",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "two container state waiting, last state terminating and one not reach stuckTerminatingTimeout",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
				},
			},
			excepted: false,
		},
		{
			name: "two container state waiting, last state terminating and double reach stuckTerminatingTimeout",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Waiting: &corev1.ContainerStateWaiting{
									Reason:  "test",
									Message: "test",
								},
							},
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now().Add(-stuckTerminatingTimeout - 5*time.Second)},
								},
							},
						},
					},
				},
			},
			excepted: true,
		},
		{
			name: "two container state terminating, and restart time is 1",
			pod: &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
							RestartCount: 1,
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
						{
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
							RestartCount: 1,
							LastTerminationState: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									FinishedAt: metav1.Time{Time: time.Now()},
								},
							},
						},
					},
				},
			},
			excepted: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.excepted, IsPodStuckTerminating(tt.pod))
		})
	}
}

func TestIsPodNotFound(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test error string contains not found",
			args: args{
				err: errors.New("pod mockPodName not found"),
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IsPodNotFound(tt.args.err), "IsPodNotFound(%v)", tt.args.err)
		})
	}
}

func TestGetTmpExpansionNodeSelectorTerm(t *testing.T) {
	type args struct {
		demandNodes []string
	}
	var a args
	var a2 args
	a2.demandNodes = []string{"127.0.0.1"}
	var emptyNodeSelectorTerms []corev1.NodeSelectorTerm
	var nodeSelectorTerms []corev1.NodeSelectorTerm
	nodeSelectorTerm := corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{{
			Key:      k8sHostNameKey,
			Operator: corev1.NodeSelectorOpIn,
			Values:   []string{"127.0.0.1"},
		}, {
			Key:      "node-role",
			Operator: corev1.NodeSelectorOpIn,
			Values:   constants.GetValidNodeRoleValuesWithGPU(),
		}},
	}
	nodeSelectorTerms = append(nodeSelectorTerms, nodeSelectorTerm)
	tests := []struct {
		name string
		args args
		want []corev1.NodeSelectorTerm
	}{
		{"GetEmptyTmpExpansionNodeSelectorTerm", a, emptyNodeSelectorTerms},
		{"GetTmpExpansionNodeSelectorTerm", a2, nodeSelectorTerms},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, GetTmpExpansionNodeSelectorTerm(tt.args.demandNodes), "GetTmpExpansionNodeSelectorTerm(%v)", tt.args.demandNodes)
		})
	}
}
