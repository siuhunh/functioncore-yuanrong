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

// Package detector detect pod status
package detector

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	informercorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	k8cache "k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

var podName = ""

type fakePodLister struct{}

func (s *fakePodLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	return nil, nil
}

func (s *fakePodLister) Pods(namespace string) listerscorev1.PodNamespaceLister {
	return fakePodNamespaceLister{}
}

type fakePodNamespaceLister struct{}

func (s fakePodNamespaceLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	return nil, nil
}

func (s fakePodNamespaceLister) Get(name string) (*v1.Pod, error) {
	if podName == "" {
		return &v1.Pod{Status: v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{
			{State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{}}},
			{State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{}}},
		}}}, nil
	} else {
		return &v1.Pod{Status: v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{
			{State: v1.ContainerState{Terminated: nil}},
			{State: v1.ContainerState{Terminated: nil}},
		}}}, nil
	}

}

type fakePodInformer struct {
}

func (f fakePodInformer) Informer() k8cache.SharedIndexInformer {
	return nil
}

func (f fakePodInformer) Lister() listerscorev1.PodLister {
	return &fakePodLister{}
}

func TestInitPodDetector(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test normal",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
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
			patches := tt.patchesFunc()
			_, err := InitPodDetector()
			if (err != nil) != tt.wantErr {
				t.Errorf("InitPodDetector() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestPodDetector_MonitorRestartingPods(t *testing.T) {
	type fields struct {
	}
	type args struct {
		stopCh          chan struct{}
		monitorInterval time.Duration
	}
	isMocked := false
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		isMocked    bool
	}{
		{
			name:   "test normal",
			fields: fields{},
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockutils.PatchSlice {
				isMocked = false
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "1"
					}),
				})
				return patches
			},
			isMocked: false,
		},
		{
			name:   "test report abnormal alarm",
			fields: fields{},
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockutils.PatchSlice {
				isMocked = false
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*PodDetector).getRestartingPods, func(_ *PodDetector) []string {
						isMocked = true
						return nil
					}),
					gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
						func(a *alarm.Client, almType alarm.Type, specAddition string) { return }),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						ch := make(chan time.Time, 1)
						ch <- time.Time{}
						return &time.Ticker{C: ch}
					}),
				})
				return patches
			},
			isMocked: true,
		},
		{
			name:   "test with stop channel is nil",
			fields: fields{},
			args: args{
				stopCh: nil,
			},
			patchesFunc: func() mockutils.PatchSlice {
				isMocked = false
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
			isMocked: false,
		},
		{
			name:   "test monitor interval is 0",
			fields: fields{},
			args: args{
				stopCh: nil,
			},
			patchesFunc: func() mockutils.PatchSlice {
				isMocked = false
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "0"
					}),
				})
				return patches
			},
			isMocked: false,
		},
		{
			name:   "test monitor interval is not int",
			fields: fields{},
			args: args{
				stopCh: nil,
			},
			patchesFunc: func() mockutils.PatchSlice {
				isMocked = false
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
			isMocked: false,
		},
		{
			name:   "test monitor interval is nil",
			fields: fields{},
			args: args{
				stopCh: nil,
			},
			patchesFunc: func() mockutils.PatchSlice {
				isMocked = false
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
			isMocked: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pd := &PodDetector{}
			patches := tt.patchesFunc()
			go func() {
				time.Sleep(2 * time.Second)
				if tt.args.stopCh != nil {
					close(tt.args.stopCh)
				}
			}()
			pd.monitorRestartingPods(tt.args.stopCh)
			if tt.isMocked != isMocked {
				t.Errorf("expect %v but found %v", tt.isMocked, isMocked)
			}
			patches.ResetAll()
		})
	}
}

func TestPodDetector_deleteTimeoutTerminatingPods(t *testing.T) {
	convey.Convey("TestTerminatingPod", t, func() {
		kubeClient := &kubernetes.Clientset{}
		corev1Client := &typecorev1.CoreV1Client{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface { return &kubernetes.Clientset{} }),
			gomonkey.ApplyFunc(utils.FunctionNameSpace, func() string { return "default" }),
			gomonkey.ApplyMethod(reflect.TypeOf(kubeClient), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return corev1Client
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(corev1Client), "Pods",
				func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
					return mockutils.NewK8sPodImp(namespace)
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("deleteTimeoutTerminatingPodOK_test1", func() {
			pd := &PodDetector{}
			timeoutPodKey := "pod1"
			pd.terminatingPodMap.Store(timeoutPodKey, time.Now())
			pd.deleteTimeoutTerminatingPods()
			_, isLoad1 := pd.terminatingPodMap.Load(timeoutPodKey)
			convey.So(isLoad1, convey.ShouldBeTrue)
		})
		convey.Convey("deleteTimeoutTerminatingPodOK_test2", func() {
			pd := &PodDetector{}
			normalPodKey := "pod2"
			pd.terminatingPodMap.Store(normalPodKey, time.Date(2022, 1, 2, 3, 4, 5, 0, time.UTC))
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
					ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			pd.deleteTimeoutTerminatingPods()
			_, isLoad2 := pd.terminatingPodMap.Load(normalPodKey)
			convey.So(isLoad2, convey.ShouldBeFalse)
		})
		convey.Convey("deleteTerminatedPods_OK", func() {
			pd := &PodDetector{}
			podName = ""
			timeoutPodKey := "pod1"
			pd.terminatingPodMap.Store(timeoutPodKey, time.Now())
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
					ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return nil
				}),
				gomonkey.ApplyFunc(utils.GetPodInformer, func() informercorev1.PodInformer {
					return fakePodInformer{}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			pd.deleteTerminatedPods()
			_, isLoad := pd.terminatingPodMap.Load(timeoutPodKey)
			convey.So(isLoad, convey.ShouldBeFalse)
		})
		convey.Convey("deleteTerminatedPods_PASS", func() {
			pd := &PodDetector{}
			podName = "PASS"
			timeoutPodKey := "pod1"
			pd.terminatingPodMap.Store(timeoutPodKey, time.Now())
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&mockutils.K8sPodImp{}), "Delete", func(_ *mockutils.K8sPodImp,
					ctx context.Context, name string, opts metav1.DeleteOptions) error {
					return nil
				}),
				gomonkey.ApplyFunc(utils.GetPodInformer, func() informercorev1.PodInformer {
					return fakePodInformer{}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			pd.deleteTerminatedPods()
			_, isLoad := pd.terminatingPodMap.Load(timeoutPodKey)
			convey.So(isLoad, convey.ShouldBeTrue)
		})
	})
}

func TestPodDetector_checkTimeoutTerminatingPods(t *testing.T) {
	var pd *PodDetector
	convey.Convey("TestCheckTimeoutTerminatingPods_test1", t, func() {
		timerCh := make(chan time.Time, defaultCheckTerminatingPodInterval)
		isMock1 := false
		isMock2 := false
		ch := make(chan struct{}, 1)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return &time.Ticker{C: timerCh}
			}),
			gomonkey.ApplyFunc((*PodDetector).deleteTimeoutTerminatingPods, func(_ *PodDetector) {
				isMock1 = true
				close(ch)
			}),
			gomonkey.ApplyFunc((*PodDetector).deleteTerminatedPods, func(_ *PodDetector) {
				isMock2 = true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		for i := 0; i < defaultCheckTerminatingPodInterval; i++ {
			timerCh <- time.Time{}
		}

		pd.checkTimeoutTerminatingPods(ch)

		convey.So(isMock1, convey.ShouldBeTrue)
		convey.So(isMock2, convey.ShouldBeTrue)
	})
	convey.Convey("TestCheckTimeoutTerminatingPods_test2", t, func() {
		pd.checkTimeoutTerminatingPods(nil)
	})
}

func TestPodDetector_statisticsRestartingPods(t *testing.T) {
	convey.Convey("TestStatisticsRestartingPod_test1", t, func() {
		var pd PodDetector
		containerState1 := v1.ContainerStatus{
			State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
				ExitCode:    0,
				Signal:      0,
				Reason:      "",
				Message:     "",
				StartedAt:   metav1.Time{},
				FinishedAt:  metav1.Time{},
				ContainerID: "",
			}},
			Name:         "worker",
			RestartCount: 5,
		}
		containerStatus1 := v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{containerState1},
		}
		pod := &v1.Pod{
			TypeMeta:   metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{},
			Spec:       v1.PodSpec{},
			Status:     containerStatus1,
		}
		pd.statisticsRestartingPods(pod)
		pods := pd.getRestartingPods()
		convey.So(len(pods), convey.ShouldEqual, 1)
	})
	convey.Convey("TestStatisticsRestartingPod_test2", t, func() {
		var pd PodDetector
		containerState1 := v1.ContainerStatus{
			State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
				ExitCode:    0,
				Signal:      0,
				Reason:      "",
				Message:     "",
				StartedAt:   metav1.Time{},
				FinishedAt:  metav1.Time{},
				ContainerID: "",
			}},
			Name:         "worker",
			RestartCount: 5,
		}
		containerStatus1 := v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{containerState1},
		}
		pod := &v1.Pod{
			TypeMeta:   metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{},
			Spec:       v1.PodSpec{},
			Status:     containerStatus1,
		}
		pod.Name = "custom-xxx"
		pd.statisticsRestartingPods(pod)
		pods := pd.getRestartingPods()
		convey.So(len(pods), convey.ShouldEqual, 0)
	})
	convey.Convey("TestStatisticsRestartingPod_test3", t, func() {
		var pd PodDetector
		containerStatus1 := v1.PodStatus{
			ContainerStatuses: nil,
		}
		pod := &v1.Pod{
			TypeMeta:   metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{},
			Spec:       v1.PodSpec{},
			Status:     containerStatus1,
		}
		pod.Name = "custom-xxx"
		pd.statisticsRestartingPods(pod)
		pods := pd.getRestartingPods()
		convey.So(len(pods), convey.ShouldEqual, 0)
	})
	convey.Convey("TestStatisticsRestartingPod_test4", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.IsPoolPod, func(pod *v1.Pod) bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		var pd PodDetector
		containerState1 := v1.ContainerStatus{
			State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
				ExitCode:    0,
				Signal:      0,
				Reason:      "",
				Message:     "",
				StartedAt:   metav1.Time{},
				FinishedAt:  metav1.Time{},
				ContainerID: "",
			}},
			Name:         "worker",
			RestartCount: 5,
		}
		containerStatus1 := v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{containerState1},
		}
		pod := &v1.Pod{
			TypeMeta:   metav1.TypeMeta{},
			ObjectMeta: metav1.ObjectMeta{},
			Spec:       v1.PodSpec{},
			Status:     containerStatus1,
		}
		pd.statisticsRestartingPods(pod)
		pods := pd.getRestartingPods()
		convey.So(len(pods), convey.ShouldEqual, 0)
	})
}

func TestPodDetector_getRestartingPods(t *testing.T) {
	convey.Convey("TestGetRestartingPods_test1", t, func() {
		var pd PodDetector
		info := RestartingPodInfo{
			RestartCount: 5,
			NodeIP:       "1.1.1.1",
		}
		pd.RestartingPods.Store("aaa", info)
		pd.RestartingPods.Store("bbb", info)
		pd.RestartingPods.Store("ccc", info)
		pods := pd.getRestartingPods()
		convey.So(len(pods), convey.ShouldEqual, 3)
	})
	convey.Convey("TestGetRestartingPods_test2", t, func() {
		var pd PodDetector
		info := RestartingPodInfo{
			RestartCount: 2,
			NodeIP:       "1.1.1.1",
		}
		pd.RestartingPods.Store("aaa", info)
		pd.RestartingPods.Store("bbb", info)
		pd.RestartingPods.Store("ccc", info)
		pods := pd.getRestartingPods()
		convey.So(len(pods), convey.ShouldEqual, 0)
	})
}

func TestPodDetector_printRestartingPodsLog(t *testing.T) {
	convey.Convey("TestPrintRestartingPodsLog", t, func() {
		pods1 := make([]string, 11)
		printRestartingPodsLog(pods1)
		pods2 := make([]string, 1)
		printRestartingPodsLog(pods2)
	})
}

func TestPodDetector_Run(t *testing.T) {
	var pd *PodDetector
	convey.Convey("TestRun", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*PodDetector).monitorRestartingPods, func(_ *PodDetector, stopCh <-chan struct{}) {}),
			gomonkey.ApplyFunc((*PodDetector).checkTimeoutTerminatingPods, func(_ *PodDetector, stopCh <-chan struct{}) {}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		ch := make(chan struct{}, 1)
		close(ch)
		pd.Run(ch)
	})
}

func Test_checkPodAllContainerRunning(t *testing.T) {
	type args struct {
		pod *v1.Pod
	}
	var a1 args
	var a2 args
	containerState1 := v1.ContainerStatus{
		State: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{
			ExitCode:    0,
			Signal:      0,
			Reason:      "",
			Message:     "",
			StartedAt:   metav1.Time{},
			FinishedAt:  metav1.Time{},
			ContainerID: "",
		}},
	}
	containerStatus1 := v1.PodStatus{
		ContainerStatuses: []v1.ContainerStatus{containerState1},
	}
	pod1 := &v1.Pod{
		TypeMeta:   metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       v1.PodSpec{},
		Status:     containerStatus1,
	}
	a1.pod = pod1

	containerState2 := v1.ContainerStatus{
		State: v1.ContainerState{Terminated: nil},
	}
	containerStatus2 := v1.PodStatus{
		ContainerStatuses: []v1.ContainerStatus{containerState2},
	}
	pod2 := &v1.Pod{
		TypeMeta:   metav1.TypeMeta{},
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       v1.PodSpec{},
		Status:     containerStatus2,
	}
	a2.pod = pod2
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"Not All Container Running", a1, false},
		{"All Container Running", a2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := checkPodAllContainerRunning(tt.args.pod); got != tt.want {
				t.Errorf("checkPodAllContainerRunning() = %v, want %v", got, tt.want)
			}
		})
	}
}
