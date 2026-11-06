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

package vpa

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	v1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	k8cache "k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/utils"
)

type fakePodLister struct{}

func (s *fakePodLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (s *fakePodLister) Pods(namespace string) listerscorev1.PodNamespaceLister {
	return fakePodNamespaceLister{}
}

type fakePodNamespaceLister struct{}

func (s fakePodNamespaceLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (s fakePodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	return nil, nil
}

type fakePodInformer struct {
}

func (f fakePodInformer) Informer() k8cache.SharedIndexInformer {
	return nil
}

func (f fakePodInformer) Lister() listerscorev1.PodLister {
	return &fakePodLister{}
}

func TestVerticalPausePod_GetVpaPod(t *testing.T) {
	patch := gomonkey.NewPatches()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "worker",
					ContainerID: "docker://worker-id",
				},
				{
					Name:        "runtime",
					ContainerID: "docker://runtime-id",
				},
			},
			Phase: corev1.PodRunning,
		},
	}

	podName := "pause-pod-test"
	convey.Convey("test get vap pod", t, func() {
		convey.Convey("test get vap pod failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get pause pod failed"))

			_, err := vpaPod.GetVpaPod(context.Background(), podName)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "get pause pod failed")
		})

		convey.Convey("test get vap pod success by podName", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, pod, nil)
			pausePod, err := vpaPod.GetVpaPod(context.Background(), podName)
			convey.So(err, convey.ShouldBeNil)
			convey.So(pausePod.Status.Phase, convey.ShouldEqual, corev1.PodRunning)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestVerticalPausePod_GetVpaPodList(t *testing.T) {
	patch := gomonkey.NewPatches()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "worker",
					ContainerID: "docker://worker-id",
				},
				{
					Name:        "runtime",
					ContainerID: "docker://runtime-id",
				},
			},
			Phase: corev1.PodRunning,
		},
	}

	convey.Convey("test get vap pod list", t, func() {
		convey.Convey("test get vap pod list failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()

			var podList []*corev1.Pod
			mockPodListerList(patch, podList, errors.New("get vap pod list failed"))
			_, err := vpaPod.GetVpaPodList(context.Background())
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "get vap pod list failed")
		})

		convey.Convey("test get vap pod list success", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			var podList []*corev1.Pod
			podList = append(podList, pod)
			mockPodListerList(patch, podList, nil)
			_, err := vpaPod.GetVpaPodList(context.Background())
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestVerticalPausePod_AddVpaPod(t *testing.T) {
	patch := gomonkey.NewPatches()
	reqPodResource := corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "worker",
					ContainerID: "docker://worker-id",
				},
				{
					Name:        "runtime",
					ContainerID: "docker://runtime-id",
				},
			},
			Phase: corev1.PodRunning,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: reqPodResource,
				},
			},
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "test-node1",
			Labels: map[string]string{"node-role": "data-plane"},
		},
	}
	convey.Convey("test add vap pod", t, func() {
		convey.Convey("test vpa feature is disabled", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, false)
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test get node info by node name failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, nil, errors.New("test get node info by node name failed"))
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "test get node info by node name failed")
		})

		convey.Convey("test get node label is empty", func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-node1",
				},
			}
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, node, nil)
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, ErrUnScheduledForPausePod)
		})

		convey.Convey("test get node label is unscheduled", func() {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-node1",
					Labels: map[string]string{"test": "hi123"},
				},
			}
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, node, nil)
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, ErrUnScheduledForPausePod)
		})

		convey.Convey("test find the pod by name failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, node, nil)
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, nil, errors.New("get pod by name failed"))

			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "get pod by name failed")
		})

		convey.Convey("test the pod already exists", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, node, nil)
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, pod, nil)
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test create pod failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, node, nil)
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get the pod not found"))
			mockKubePodNotFound(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientCreate(patch, &corev1.Pod{}, errors.New("crate pod failed"))
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "crate pod failed")
		})

		convey.Convey("test create pod wait timeout", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, node, nil)
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get the pod not found"))
			mockKubePodNotFound(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientCreate(patch, pod, nil)
			mockWaitPodReady(patch, errors.New("create pod wait timeout"))
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "create pod wait timeout")
		})

		convey.Convey("test create pod success", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesGet(patch, node, nil)
			vpaPod := NewVpaPausePod(WithPodReadyTimeout(1 * time.Second))
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get the pod not found"))
			mockKubePodNotFound(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientCreate(patch, pod, nil)
			mockWaitPodReady(patch, nil)
			err := vpaPod.AddVpaPod(context.Background(), node.Name, &reqPodResource)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestVerticalPausePod_AddVpaPodFull(t *testing.T) {
	patch := gomonkey.NewPatches()
	reqPodResource := corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "worker",
					ContainerID: "docker://worker-id",
				},
				{
					Name:        "runtime",
					ContainerID: "docker://runtime-id",
				},
			},
			Phase: corev1.PodRunning,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Resources: reqPodResource,
				},
			},
		},
	}

	nodeList := &corev1.NodeList{Items: []corev1.Node{{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "test-node1",
			Labels: map[string]string{"node-role": "data-plane"}}},
	}}

	convey.Convey("test add vap pod full", t, func() {
		convey.Convey("test vpa feature is disabled", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, false)
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test get k8s node failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, &corev1.NodeList{}, errors.New("get k8s node failed"))

			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "get k8s node failed")
		})

		convey.Convey("test get label is nil", func() {
			nodeList := &corev1.NodeList{Items: []corev1.Node{{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-node1"}},
			}}

			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, nodeList, nil)
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test get label is unscheduled", func() {
			nodeList := &corev1.NodeList{Items: []corev1.Node{{
				ObjectMeta: metav1.ObjectMeta{
					Name:   "test-node1",
					Labels: map[string]string{"node-role": "data-plane"}}},
			}}
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, nodeList, nil)
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test find the pod by name failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, nodeList, nil)
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get pod by name failed"))
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "get pod by name failed")
		})

		convey.Convey("test the pod already exists", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, nodeList, nil)
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, pod, nil)
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test create pod failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, nodeList, nil)
			mockKubeClientPods(patch, &fake.FakePods{})
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get the pod not found"))
			mockKubePodNotFound(patch, true)
			mockKubeClientCreate(patch, &corev1.Pod{}, errors.New("create pod failed"))
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "create pod failed")
		})

		convey.Convey("test create pod wait timeout", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, nodeList, nil)
			mockKubeClientPods(patch, &fake.FakePods{})
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get the pod not found"))
			mockKubePodNotFound(patch, true)
			mockKubeClientCreate(patch, pod, nil)
			mockWaitPodReady(patch, errors.New("create pod wait timeout"))
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "create pod wait timeout")
		})

		convey.Convey("test create pod success", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockPodInformer(patch, &fakePodInformer{})
			vpaPod := NewVpaPausePod()
			mockIsVpaFeatureEnabled(patch, true)
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientNodes(patch, &fake.FakeNodes{})
			mockKubeClientNodesList(patch, nodeList, nil)
			mockKubeClientPods(patch, &fake.FakePods{})
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockPodNamespaceListerGet(patch, &corev1.Pod{}, errors.New("get the pod not found"))
			mockKubePodNotFound(patch, true)
			mockKubeClientCreate(patch, pod, nil)
			mockWaitPodReady(patch, nil)
			err := vpaPod.AddVpaPodFull(context.Background())
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestVerticalPausePod_DeleteVpaPod(t *testing.T) {
	patch := gomonkey.NewPatches()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "pause-pod",
					ContainerID: "docker://pause-id",
				},
			},
			Phase: corev1.PodRunning,
		},
	}

	convey.Convey("test delete vpa pod", t, func() {
		convey.Convey("test delete vap pod failed", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			vpaPod := NewVpaPausePod()
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientDelete(patch, errors.New("delete vpa pod failed"))
			utils.RetryPodOperationList = utils.NewPodOperationRetryList()
			err := vpaPod.DeleteVpaPod(context.Background(), pod)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "delete vpa pod failed")
		})

		convey.Convey("test delete vap pod success", func() {
			mockKubeClient(patch, &kubernetes.Clientset{})
			vpaPod := NewVpaPausePod()
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientDelete(patch, nil)

			err := vpaPod.DeleteVpaPod(context.Background(), pod)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestVerticalPausePod_waitPodReadyOrTimeout(t *testing.T) {
	patch := gomonkey.NewPatches()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "pause-pod",
					ContainerID: "docker://pause-id",
				},
			},
			Phase: corev1.PodRunning,
		},
	}

	convey.Convey("test wait vpa pod ready", t, func() {
		convey.Convey("test wait vpa pod pull up failed ", func() {
			vpaPod := VerticalPausePod{
				nameSpace:              utils.FunctionNameSpace(),
				kubeClient:             &kubernetes.Clientset{},
				waitReadyTimeout:       3 * time.Second,
				checkPodStatusInterval: 1 * time.Second,
				informer:               fakePodInformer{},
			}
			mockPodListerPods(patch, &fakePodNamespaceLister{})

			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			patch.ApplyMethodSeq(reflect.TypeOf(&fakePodNamespaceLister{}), "Get",
				[]gomonkey.OutputCell{
					{Values: gomonkey.Params{nil, errors.New("pods \"pause-pod-123\" not found")}},
					{Values: gomonkey.Params{nil, errors.New("pods \"pause-pod-123\" not found")}},
					{Values: gomonkey.Params{nil, errors.New("unexpected error")}},
				})

			patch.ApplyFunc(utils.IsPodNotFound, func(err error) bool {
				if strings.Contains(err.Error(), "not found") {
					return true
				}
				return false
			})

			err := vpaPod.waitPodReady(context.Background(), pod)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldEqual, ErrCreatePodPausePod)
		})

		convey.Convey("test wait vpa pod pull up success", func() {
			vpaPod := VerticalPausePod{
				nameSpace:              utils.FunctionNameSpace(),
				kubeClient:             &kubernetes.Clientset{},
				waitReadyTimeout:       3 * time.Second,
				checkPodStatusInterval: 1 * time.Second,
				informer:               fakePodInformer{},
			}
			mockPodListerPods(patch, &fakePodNamespaceLister{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			patch.ApplyMethodSeq(reflect.TypeOf(&fakePodNamespaceLister{}), "Get",
				[]gomonkey.OutputCell{
					{Values: gomonkey.Params{pod, nil}},
				})

			patch.ApplyFunc(utils.IsPodNotFound, func(err error) bool {
				if strings.Contains(err.Error(), "not found") {
					return true
				}
				return false
			})

			err := vpaPod.waitPodReady(context.Background(), pod)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func mockKubeClientCoreV1(patch *gomonkey.Patches, fakeCoreV1 *fake.FakeCoreV1) {
	patch.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
		func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
			return fakeCoreV1
		})
}

func mockKubeClientPods(patch *gomonkey.Patches, fakePods *fake.FakePods) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Pods",
		func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
			return fakePods
		})
}

func mockKubeClientPodsUpdate(patch *gomonkey.Patches, updatePod *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Update",
		func(_ *fake.FakePods, ctx context.Context, pod *corev1.Pod, opts metav1.UpdateOptions) (*corev1.Pod, error) {
			return updatePod, err
		})
}

func mockKubeClientNodes(patch *gomonkey.Patches, fakePods *fake.FakeNodes) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Nodes",
		func(_ *fake.FakeCoreV1) typecorev1.NodeInterface {
			return fakePods
		})
}

func mockKubeClientNodesGet(patch *gomonkey.Patches, node *corev1.Node, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakeNodes{}), "Get",
		func(_ *fake.FakeNodes, ctx context.Context, name string, options metav1.GetOptions) (*corev1.Node, error) {
			return node, err
		})
}

func mockKubeClientNodesList(patch *gomonkey.Patches, nodes *corev1.NodeList, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakeNodes{}), "List",
		func(_ *fake.FakeNodes, ctx context.Context, opts metav1.ListOptions) (*corev1.NodeList, error) {
			return nodes, err
		})
}

func mockKubeClientNodesUpdate(patch *gomonkey.Patches, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakeNodes{}), "Update",
		func(_ *fake.FakeNodes, ctx context.Context, node *corev1.Node, opts metav1.UpdateOptions) (*corev1.Node, error) {
			return &corev1.Node{}, err
		})
}

func mockKubeClientUpdate(patch *gomonkey.Patches, pausePod *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Update",
		func(_ *fake.FakePods, ctx context.Context, pod *corev1.Pod, opts metav1.UpdateOptions) (*corev1.Pod, error) {
			return pausePod, err
		})
}

func mockKubeClientGet(patch *gomonkey.Patches, pausePod *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Get",
		func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.GetOptions) (*corev1.Pod, error) {
			return pausePod, err
		})
}

func mockKubeClientDelete(patch *gomonkey.Patches, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Delete",
		func(_ *fake.FakePods, ctx context.Context, name string, opts metav1.DeleteOptions) error {
			return err
		})
}

func mockKubeClientCreate(patch *gomonkey.Patches, pausePod *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "Create",
		func(_ *fake.FakePods, ctx context.Context, pod *corev1.Pod, opts metav1.CreateOptions) (*corev1.Pod, error) {
			return pausePod, err
		})
}

func mockKubeClientList(patch *gomonkey.Patches, pausePodList *corev1.PodList, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "List",
		func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
			return pausePodList, err
		})
}

func mockKubeClient(patch *gomonkey.Patches, kubeClient kubernetes.Interface) {
	patch.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
		return kubeClient
	})
}

func mockWaitPodReady(patch *gomonkey.Patches, err error) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&VerticalPausePod{}), "waitPodReady",
		func(_ *VerticalPausePod, ctx context.Context, pod *corev1.Pod) error {
			return err
		})
}

func mockKubePodNotFound(patch *gomonkey.Patches, res bool) {
	patch.ApplyFunc(utils.IsPodNotFound, func(err error) bool {
		return res
	})
}

func mockIsVpaFeatureEnabled(patch *gomonkey.Patches, res bool) {
	patch.ApplyFunc(utils.IsVpaFeatureEnabled, func() bool {
		return res
	})
}

func mockPodInformer(patch *gomonkey.Patches, podInformer v1.PodInformer) {
	patch.ApplyFunc(utils.GetPodInformer, func() v1.PodInformer {
		return podInformer
	})
}

func mockPodListerList(patch *gomonkey.Patches, ret []*corev1.Pod, err error) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&fakePodLister{}), "List",
		func(_ *fakePodLister, selector labels.Selector) ([]*corev1.Pod, error) {
			return ret, err
		})
}

func mockPodListerPods(patch *gomonkey.Patches, pods listercorev1.PodNamespaceLister) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&fakePodLister{}), "List",
		func(_ *fakePodLister, namespace string) listercorev1.PodNamespaceLister {
			return pods
		})
}

func mockPodNamespaceListerGet(patch *gomonkey.Patches, ret *corev1.Pod, err error) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "Get",
		func(_ *fakePodNamespaceLister, name string) (*corev1.Pod, error) {
			return ret, err
		})
}

func mockPodNamespaceListerList(patch *gomonkey.Patches, ret []*corev1.Pod, err error) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "List",
		func(_ *fakePodNamespaceLister, selector labels.Selector) ([]*corev1.Pod, error) {
			return ret, err
		})
}

func TestVerticalPausePod_getPodSpec(t *testing.T) {
	vpaPod := VerticalPausePod{
		nameSpace:              utils.FunctionNameSpace(),
		kubeClient:             &kubernetes.Clientset{},
		waitReadyTimeout:       3 * time.Second,
		checkPodStatusInterval: 1 * time.Second,
		informer:               fakePodInformer{},
	}
	reqPodResource := &corev1.ResourceRequirements{
		Limits: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
		Requests: map[corev1.ResourceName]resource.Quantity{
			"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
			"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
		},
	}
	nodeSelectorTerms := utils.GetNodeSelectorTerm([]string{"nodeA"})
	var nodeSelector []corev1.NodeSelectorRequirement
	nodeSelector = append(nodeSelector, corev1.NodeSelectorRequirement{
		Key:      constants.NodeRoleLabelKey,
		Operator: corev1.NodeSelectorOpIn,
		Values:   constants.GetValidNodeRoleValues(),
	})
	for i := range nodeSelectorTerms {
		// NodeSelectorTerms are ORed, so we should append on-demand node expression for each term
		nodeSelectorTerms[i].MatchExpressions = append(nodeSelectorTerms[i].MatchExpressions, nodeSelector...)
	}
	convey.Convey("test get pod spec with ownerReference", t, func() {
		convey.Convey("test ", func() {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc(utils.GetWorkerManagerDeployUID, func() types.UID {
				return "1111"
			})
			pod := vpaPod.getPodSpec("pause-pod-nodeA", "nodeA", reqPodResource)
			convey.So(pod.OwnerReferences[0].Name, convey.ShouldEqual, utils.WorkerManagerDeployName)
			convey.So(pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
				convey.ShouldResemble, nodeSelectorTerms)
		})
	})
}

func mockGetVpaPodList(patch *gomonkey.Patches, pods []*corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&VerticalPausePod{}), "GetVpaPodList",
		func(_ *VerticalPausePod, ctx context.Context) ([]*corev1.Pod, error) {
			return pods, err
		})
}

func mockDelLabelNodeVpaPausePodFaults(patch *gomonkey.Patches, err error) {
	patch.ApplyMethod(reflect.TypeOf(&VerticalPausePod{}), "DelLabelNodeVpaPausePodFaults",
		func(_ *VerticalPausePod, ctx context.Context, nodeName string) error {
			return err
		})
}

func mockAddLabelNodeVpaPausePodFaults(patch *gomonkey.Patches, err error) {
	patch.ApplyMethod(reflect.TypeOf(&VerticalPausePod{}), "AddLabelNodeVpaPausePodFaults",
		func(_ *VerticalPausePod, ctx context.Context, podName string) error {
			return err
		})
}

func mockGetVpaPod(patch *gomonkey.Patches, pod *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&VerticalPausePod{}), "GetVpaPod",
		func(_ *VerticalPausePod, ctx context.Context, podName string) (*corev1.Pod, error) {
			return pod, err
		})
}

func mockReportAlarm(patch *gomonkey.Patches) {
	alarmClient := &alarm.Client{}
	patch.ApplyFunc(alarm.GetClient, func() *alarm.Client {
		return alarmClient
	})
	patch.ApplyMethod(reflect.TypeOf(alarmClient), "ReportAlarm",
		func(c *alarm.Client, almType alarm.Type, errMsg string) {})
}

func mockClearAlarm(patch *gomonkey.Patches) {
	alarmClient := &alarm.Client{}
	patch.ApplyFunc(alarm.GetClient, func() *alarm.Client {
		return alarmClient
	})
	patch.ApplyMethod(reflect.TypeOf(alarmClient), "ClearAlarm",
		func(c *alarm.Client, almType alarm.Type) {})
}

func TestCheckPausePodStatus(t *testing.T) {
	t.Run("failed to list vpa pause pod", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, nil, errors.New("failed"))
		defer patch.Reset()
		err := checkPausePodStatus()
		assert.Error(t, err)
	})
	t.Run("there is no pause pod", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, []*corev1.Pod{}, nil)
		defer patch.Reset()
		err := checkPausePodStatus()
		assert.Error(t, err)
		assert.EqualError(t, err, "there is no pause pod")
	})
	t.Run("all pause pod is running", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, []*corev1.Pod{
			{
				Status: corev1.PodStatus{
					Phase: "Running",
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{},
							},
						},
					},
				},
			},
		}, nil)
		mockDelLabelNodeVpaPausePodFaults(patch, nil)
		mockClearAlarm(patch)
		defer patch.Reset()
		err := checkPausePodStatus()
		assert.NoError(t, err)
	})
	t.Run("has pause pod is not running", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, []*corev1.Pod{
			{
				Status: corev1.PodStatus{
					Phase: "Running",
					ContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: nil,
							},
						},
					},
				},
			},
		}, nil)
		mockGetVpaPod(patch, &corev1.Pod{
			Status: corev1.PodStatus{
				Phase: "Running",
				ContainerStatuses: []corev1.ContainerStatus{
					{
						State: corev1.ContainerState{
							Running: nil,
						},
					},
				},
			},
		}, nil)
		mockAddLabelNodeVpaPausePodFaults(patch, nil)
		mockReportAlarm(patch)
		defer patch.Reset()
		err := checkPausePodStatus()
		assert.Error(t, err)
		assert.EqualError(t, err, "error msg=there is 1 pause pod is not running")
	})
}

func mockNodeCache(patch *gomonkey.Patches, node *corev1.Node) {
	patch.ApplyFunc(cache.GetK8sNodeCacheByDeepCopy, func(nodeName string) *corev1.Node {
		return node
	})
}

func TestVerticalPausePod_AddLabelNodeVpaPausePodFaults(t *testing.T) {
	t.Run("node not exists in nodeCache", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeCache(patch, nil)
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.AddLabelNodeVpaPausePodFaults(context.Background(), "test-node", false)
		assert.NoError(t, err)
	})
	t.Run("node already add label:vpa-pause-pod-faults:default", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeCache(patch, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: "default"},
			},
		})
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.AddLabelNodeVpaPausePodFaults(context.Background(), "test-node", false)
		assert.NoError(t, err)
	})
	t.Run("node update label:vpa-pause-pod-faults:resource-Insufficient", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: "default"},
			},
		}
		mockNodeCache(patch, node)
		mockKubeClient(patch, &kubernetes.Clientset{})
		mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
		mockKubeClientNodes(patch, &fake.FakeNodes{})
		mockKubeClientNodesUpdate(patch, nil)
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.AddLabelNodeVpaPausePodFaults(context.Background(), "test-node", true)
		assert.NoError(t, err)
	})
	t.Run("failed to add label:vpa-pause-pod-faults", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeCache(patch, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{},
			},
		})
		mockKubeClient(patch, &kubernetes.Clientset{})
		mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
		mockKubeClientNodes(patch, &fake.FakeNodes{})
		mockKubeClientNodesUpdate(patch, errors.New("update failed"))
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.AddLabelNodeVpaPausePodFaults(context.Background(), "test-node", false)
		assert.Error(t, err)
	})
	t.Run("succeed to add label:vpa-pause-pod-faults", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{},
			},
		}
		mockNodeCache(patch, node)
		mockKubeClient(patch, &kubernetes.Clientset{})
		mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
		mockKubeClientNodes(patch, &fake.FakeNodes{})
		mockKubeClientNodesUpdate(patch, nil)
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.AddLabelNodeVpaPausePodFaults(context.Background(), "test-node", false)
		assert.NoError(t, err)
		_, ok := node.Labels[constants.NodeVpaPausePodFaultsLabelKey]
		assert.True(t, ok)
	})
}

func TestVerticalPausePod_DelLabelNodeVpaPausePodFaults(t *testing.T) {
	t.Run("node not exists in nodeCache", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeCache(patch, nil)
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.DelLabelNodeVpaPausePodFaults(context.Background(), "test-node")
		assert.NoError(t, err)
	})
	t.Run("node not exists label:vpa-pause-pod-faults", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeCache(patch, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{},
			},
		})
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.DelLabelNodeVpaPausePodFaults(context.Background(), "test-node")
		assert.NoError(t, err)
	})
	t.Run("failed to del label:vpa-pause-pod-faults", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockNodeCache(patch, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: ""},
			},
		})
		mockKubeClient(patch, &kubernetes.Clientset{})
		mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
		mockKubeClientNodes(patch, &fake.FakeNodes{})
		mockKubeClientNodesUpdate(patch, errors.New("update failed"))
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.DelLabelNodeVpaPausePodFaults(context.Background(), "test-node")
		assert.Error(t, err)
	})
	t.Run("succeed to del label:vpa-pause-pod-faults", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "test-node",
				Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: ""},
			},
		}
		mockNodeCache(patch, node)
		mockKubeClient(patch, &kubernetes.Clientset{})
		mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
		mockKubeClientNodes(patch, &fake.FakeNodes{})
		mockKubeClientNodesUpdate(patch, nil)
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.DelLabelNodeVpaPausePodFaults(context.Background(), "test-node")
		assert.NoError(t, err)
		_, ok := node.Labels[constants.NodeVpaPausePodFaultsLabelKey]
		assert.False(t, ok)
	})
}

func TestVerticalPausePod_UpdateVpaPodImage(t *testing.T) {
	pausePod := &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: "Running",
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{
				Image: "old-image",
			},
		}},
	}
	t.Run("failed to get pause pod", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockPodInformer(patch, &fakePodInformer{})
		mockPodNamespaceListerGet(patch, nil, errors.New("get pod failed"))
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.UpdateVpaPodImage(context.Background(), &corev1.Pod{}, "test-image")
		assert.Error(t, err)
		assert.EqualError(t, err, "get pod failed")
	})
	t.Run("failed to update pause pod image", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockPodInformer(patch, &fakePodInformer{})
		mockPodNamespaceListerGet(patch, pausePod, nil)
		mockKubeClient(patch, &kubernetes.Clientset{})
		mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
		mockKubeClientPods(patch, &fake.FakePods{})
		mockKubeClientPodsUpdate(patch, nil, errors.New("failed to update pod"))
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.UpdateVpaPodImage(context.Background(), &corev1.Pod{}, "test-image")
		assert.Error(t, err)
		assert.EqualError(t, err, "failed to update pod")
	})
	t.Run("succeed to update pause pod image", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockPodInformer(patch, &fakePodInformer{})
		mockPodNamespaceListerGet(patch, pausePod, nil)
		mockKubeClient(patch, &kubernetes.Clientset{})
		mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
		mockKubeClientPods(patch, &fake.FakePods{})
		mockKubeClientPodsUpdate(patch, pausePod, nil)
		defer patch.Reset()
		vpaPod := NewVpaPausePod()
		err := vpaPod.UpdateVpaPodImage(context.Background(), &corev1.Pod{}, "test-image")
		assert.NoError(t, err)
	})
}

func TestSetPausePodResourceRequirements(t *testing.T) {
	SetPausePodResourceRequirements("test-pod", &corev1.ResourceRequirements{})
	_, ok := vpaPausePodResourceMap.Load("test-pod")
	assert.True(t, ok)

	_, ok = vpaPausePodResourceMap.Load("notExist-pod")
	assert.False(t, ok)
}

func TestRemovePausePodResourceRequirements(t *testing.T) {
	SetPausePodResourceRequirements("test-pod", &corev1.ResourceRequirements{})
	RemovePausePodResourceRequirements("test-pod")
	_, ok := vpaPausePodResourceMap.Load("test-pod")
	assert.False(t, ok)
}

func TestGetPausePodResourceRequirements(t *testing.T) {
	req := GetPausePodResourceRequirements("notExist-pod")
	assert.Nil(t, req)

	SetPausePodResourceRequirements("existed-pod", &corev1.ResourceRequirements{})
	req = GetPausePodResourceRequirements("existed-pod")
	assert.NotNil(t, req)

	vpaPausePodResourceMap.Store("not-ResourceRequirements", 1)
	req = GetPausePodResourceRequirements("not-ResourceRequirements")
	assert.Nil(t, req)
}

func TestUpdatePausePodImage(t *testing.T) {
	t.Run("failed to get pause pod list", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, nil, errors.New("get pod failed"))
		defer patch.Reset()
		err := updatePausePodImage()
		assert.Error(t, err)
		assert.EqualError(t, err, "get pod failed")
	})
	t.Run("there is no vpa pause pod", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, []*corev1.Pod{}, nil)
		defer patch.Reset()
		err := updatePausePodImage()
		assert.Error(t, err)
		assert.EqualError(t, err, "there is no vpa pause pod")
	})
	t.Run("no need to update pause pod image", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, []*corev1.Pod{{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: verticalPauseImageName}}}}}, nil)
		defer patch.Reset()
		err := updatePausePodImage()
		assert.NoError(t, err)
	})
	t.Run("need to update pause pod image", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		mockGetVpaPodList(patch, []*corev1.Pod{{Spec: corev1.PodSpec{Containers: []corev1.Container{{Image: "old-image"}}}}}, nil)
		mockUpdateVpaPodImage(patch, nil)
		defer patch.Reset()
		err := updatePausePodImage()
		assert.NoError(t, err)
	})
}

func mockUpdateVpaPodImage(patch *gomonkey.Patches, err error) {
	patch.ApplyMethod(reflect.TypeOf(&VerticalPausePod{}), "UpdateVpaPodImage",
		func(_ *VerticalPausePod, ctx context.Context, pod *corev1.Pod, image string) error {
			return err
		})
}

func TestGetVpaThresholdCPU(t *testing.T) {
	assert.Equal(t, defaultUpdateThreshold, GetVpaThresholdCPU())
	os.Setenv(envVpaThresholdCPU, "999")
	assert.Equal(t, int64(999), GetVpaThresholdCPU())
}
