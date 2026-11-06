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

// Package poolmanager manages pool
package poolmanager

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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	informerscorev1 "k8s.io/client-go/informers/core/v1"
	v1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"
	listerscorev1 "k8s.io/client-go/listers/core/v1"
	k8cache "k8s.io/client-go/tools/cache"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	tools "functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
	mockutils "functioncore/test/utils"
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

type fakeNodeInformer struct {
}

func (f fakeNodeInformer) Informer() k8cache.SharedIndexInformer {
	return nil
}

func (f fakeNodeInformer) Lister() listerscorev1.NodeLister {
	return &fakeNodeLister{}
}

type fakeNodeLister struct {
}

func (f fakeNodeLister) List(selector labels.Selector) (ret []*corev1.Node, err error) {
	return nil, nil
}

func (f fakeNodeLister) Get(name string) (*corev1.Node, error) {
	return nil, nil
}

func TestUpdateNode(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	node001 := &corev1.Node{}
	node002 := &corev1.Node{}
	node002.Labels = map[string]string{constants.NodeRoleLabelKey: "test002"}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "test001", args: args{node: node001}, wantErr: false},
		{name: "test002", args: args{node: node002}, wantErr: false},
	}
	RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
		return nil
	})
	patch := gomonkey.ApplyFunc(cache.GetNodeInfo,
		func(nodeID string) *cache.NodeInfo {
			return &cache.NodeInfo{
				Label: map[string]string{constants.NodeRoleLabelKey: "test001"},
			}
		})
	defer patch.Reset()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := UpdateNode(tt.args.node); (err != nil) != tt.wantErr {
				t.Errorf("UpdateNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeleteNode(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	node001 := &corev1.Node{}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{name: "test001", args: args{node: node001}, wantErr: false},
	}
	RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
		return nil
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := DeleteNode(tt.args.node); (err != nil) != tt.wantErr {
				t.Errorf("DeleteNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestUpdateNodeWithTaints(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	nodeWithTaints := &corev1.Node{}
	nodeWithTaints.Name = "mock-node-name"
	nodeWithTaints.Spec.Taints = []corev1.Taint{
		{
			Key: tools.MigrateTaintKey,
		},
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with success",
			args: args{node: nodeWithTaints},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, nil
					}),
					gomonkey.ApplyFunc(tools.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "mock-funcSvcID", true
					}),
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test with success
		{
			name: "test with success",
			args: args{node: nodeWithTaints},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, nil
					}),
					gomonkey.ApplyFunc(tools.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "mock-funcSvcID", true
					}),
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.IsPodReady, func(pod *corev1.Pod) bool {
						return false
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test failed with pod is not ready
		{
			name: "test with success",
			args: args{node: nodeWithTaints},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, nil
					}),
					gomonkey.ApplyFunc(tools.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "mock-funcSvcID", true
					}),
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
					gomonkey.ApplyFunc(tools.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "", false
					}),
					gomonkey.ApplyFunc(tools.ForciblyDeletePod, func(pod *corev1.Pod) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test failed with funcSvcID is empty
	}
	RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
		log.GetLogger().Infof("succeed to mock clean pods with labelSelector %s and traceID %s",
			labelSelector, traceID)
		return nil
	})
	RegisterDoActionsCallBack(func(funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
		log.GetLogger().Infof("succeed to do actions with funcSvcID %s for migrating", funcSvcID)
		return
	})
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			if err := UpdateNode(tt.args.node); (err != nil) != tt.wantErr {
				t.Errorf("UpdateNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		patches.ResetAll()
	}
}

func Test_needMigratePodOnNode(t *testing.T) {
	tests := []struct {
		name    string
		oldNode *cache.NodeInfo
		node    *corev1.Node

		expected bool
	}{
		{
			name:    "new node has no taints",
			oldNode: &cache.NodeInfo{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}}},
			node:    &corev1.Node{},

			expected: false,
		},
		{
			name:    "taints added",
			oldNode: &cache.NodeInfo{},
			node:    &corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}}}},

			expected: true,
		},
		{
			name:    "new node not ready",
			oldNode: &cache.NodeInfo{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}}},
			node:    &corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}}}},

			expected: false,
		},
		{
			name:    "new taints added",
			oldNode: &cache.NodeInfo{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}}},
			node: &corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}, {Key: corev1.TaintNodeUnschedulable}}},
				Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},

			expected: true,
		},
		{
			name:    "taints check interval fail",
			oldNode: &cache.NodeInfo{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}}},
			node: &corev1.Node{Spec: corev1.NodeSpec{Taints: []corev1.Taint{{Key: tools.MigrateTaintKey}}},
				Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},

			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, needMigratePodOnNode(tt.oldNode, tt.node), tt.name)
		})
	}
}

func TestUpdateNodeWithTaintsAndCleanPodsError(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	nodeWithTaints := &corev1.Node{}
	nodeWithTaints.Name = "mock-node-name"
	nodeWithTaints.Spec.Taints = []corev1.Taint{
		{
			Key: tools.MigrateTaintKey,
		},
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with clean pods error",
			args: args{node: nodeWithTaints},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test with clean pods error
	}
	RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
		log.GetLogger().Infof("mock clean pods failed with labelSelector %s and traceID %s",
			labelSelector, traceID)
		return errors.New("mock clean pods error")
	})
	RegisterDoActionsCallBack(func(funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
		log.GetLogger().Infof("succeed to do actions with funcSvcID %s for migrating", funcSvcID)
		return
	})
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			if err := UpdateNode(tt.args.node); (err != nil) != tt.wantErr {
				t.Errorf("UpdateNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		patches.ResetAll()
	}
}

func TestUpdateNodeWithTaintsAndK8sOperateError(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	nodeWithTaints := &corev1.Node{}
	nodeWithTaints.Name = "mock-node-name"
	nodeWithTaints.Spec.Taints = []corev1.Taint{
		{
			Key: tools.MigrateTaintKey,
		},
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with list pods error",
			args: args{node: nodeWithTaints},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, errors.New("mock list pods error")
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test with list pods error
	}
	RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
		log.GetLogger().Infof("succeed to mock clean pods with labelSelector %s and traceID %s",
			labelSelector, traceID)
		return nil
	})
	RegisterDoActionsCallBack(func(funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
		log.GetLogger().Infof("succeed to do actions with funcSvcID %s for migrating", funcSvcID)
		return
	})
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			if err := UpdateNode(tt.args.node); (err != nil) != tt.wantErr {
				t.Errorf("UpdateNode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		patches.ResetAll()
	}
}

func TestUpdateNodeWithDsWorkerTaints(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	nodeWithTaints := &corev1.Node{}
	nodeWithTaints.Name = "mock-node-name"
	nodeWithTaints.Spec.Taints = []corev1.Taint{
		{
			Key: tools.DsWorkerTaintKey,
		},
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test with list pods",
			args: args{node: nodeWithTaints},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, errors.New("mock list pods error")
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test with list pods error
	}
	RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
		log.GetLogger().Infof("succeed to mock clean pods with labelSelector %s and traceID %s",
			labelSelector, traceID)
		return nil
	})
	RegisterDoActionsCallBack(func(funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
		log.GetLogger().Infof("succeed to do actions with funcSvcID %s for migrating", funcSvcID)
		return
	})
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			if err := UpdateNode(tt.args.node); err != nil {
				t.Errorf("this is error")
			}
		})
		patches.ResetAll()
	}
}

func TestAddNode(t *testing.T) {
	type args struct {
		node *corev1.Node
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
				node: &corev1.Node{
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key: corev1.TaintNodeUnschedulable,
							},
						},
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test success
		{
			name: "test success with node has no taints",
			args: args{
				node: &corev1.Node{
					Spec: corev1.NodeSpec{},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return nil
					}),
					gomonkey.ApplyFunc(notifyMigratingReservedIns, func(node *corev1.Node) {
						return
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test success with node has no taints
		{
			name: "test with error",
			args: args{
				node: &corev1.Node{
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key: corev1.TaintNodeUnschedulable,
							},
						},
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return errors.New("mock prepareMigratingPods error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "test DsWorkerTaintKey taint",
			args: args{
				node: &corev1.Node{
					Spec: corev1.NodeSpec{
						Taints: []corev1.Taint{
							{
								Key: tools.DsWorkerTaintKey,
							},
						},
					},
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return errors.New("mock prepareMigratingPods error")
					}),
					gomonkey.ApplyFunc(notifyMigratingReservedIns, func(node *corev1.Node) {}),
				})
				return patches
			},
			wantErr: false,
		}, // test DsWorkerTaintKey taint
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			err := AddNode(tt.args.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
			patches.ResetAll()
		})
	}
}

func TestMigratePodToSpecificNode(t *testing.T) {
	type args struct {
		pod  corev1.Pod
		node string
	}
	tests := []struct {
		name        string
		args        args
		patcherFunc mockutils.PatchesFunc
	}{
		{
			"test migrate pod to specific node pod not ready",
			args{},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady, func(pod *corev1.Pod) bool {
						return false
					}),
				})
				return patches
			},
		},
		{
			"test migrate pod to specific node pod not ready",
			args{
				pod: corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"migrating": "true",
						},
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
					gomonkey.ApplyFunc(tools.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "", false
					}),
				})
				return patches
			},
		},
		{
			"test migrate pod to specific node pod not ready",
			args{
				pod: corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"migrating": "true",
						},
					},
				},
			},
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(tools.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
					gomonkey.ApplyFunc(tools.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
						return "test", false
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patcherFunc()
		MigratePodToSpecificNode(tt.args.pod, tt.args.node)
		patches.ResetAll()
	}
}

func TestUpdateNodeWithNotReadyNodes(t *testing.T) {
	type args struct {
		node *corev1.Node
	}
	notReadyEvent := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   "Ready",
					Status: "False",
				},
			},
		},
	}
	notReadyEvent.Name = "mock-not-ready-node-name-001"
	readyEvent := &corev1.Node{
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   "Ready",
					Status: "True",
				},
			},
		},
	}
	readyEvent.Name = "mock-not-ready-node-name-002"
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "test success with not ready nodes",
			args: args{
				node: notReadyEvent,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, nil
					}),
					gomonkey.ApplyFunc(tools.ForciblyDeletePod, func(pod *corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test success with not ready nodes
		{
			name: "test success with not ready nodes",
			args: args{
				node: notReadyEvent,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, errors.New("mock list pods error")
					}),
					gomonkey.ApplyFunc(tools.ForciblyDeletePod, func(pod *corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test success with not ready nodes and list pods error
		{
			name: "test success with not ready nodes",
			args: args{
				node: notReadyEvent,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &fake.FakeCoreV1{}
					}),
					gomonkey.ApplyFunc((*fake.FakeCoreV1).Pods, func(_ *fake.FakeCoreV1, namespace string) typecorev1.PodInterface {
						return &fake.FakePods{}
					}),
					gomonkey.ApplyFunc((*fake.FakePods).List, func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (result *corev1.PodList, err error) {
						mockPod1 := corev1.Pod{}
						mockPod1.Name = "mock-pod-1"
						mockPod1.Labels = map[string]string{}
						return &corev1.PodList{
							Items: []corev1.Pod{
								mockPod1,
							},
						}, nil
					}),
					gomonkey.ApplyFunc(tools.ForciblyDeletePod, func(pod *corev1.Pod) error {
						return errors.New("forcibly delete pod error")
					}),
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		}, // test success with not ready nodes and force delete pod error
		{
			name: "test success with ready nodes",
			args: args{
				node: readyEvent,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(prepareMigratingPods, func(node *corev1.Node) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		}, // test success with ready nodes
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			err = UpdateNode(tt.args.node)
			err = UpdateNode(tt.args.node)
			err = UpdateNode(tt.args.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("want %v but found %v", tt.wantErr, err)
			}
		})
	}
}

func Test_shortPodName(t *testing.T) {
	name := make([]byte, 64)
	shortName := shortPodName(string(name))
	assert.Equal(t, 63, len(shortName))

	name = make([]byte, 20)
	shortName = shortPodName(string(name))
	assert.Equal(t, 20, len(shortName))

}

func Test_isNodeTurnToSchedule(t *testing.T) {
	tests := []struct {
		name    string
		oldNode *cache.NodeInfo
		newNode *corev1.Node

		expected bool
	}{
		{
			name:    "old and new all can schedule",
			oldNode: &cache.NodeInfo{Label: map[string]string{constants.NodeRoleLabelKey: "data-plane"}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.NodeRoleLabelKey: "data-plane"}},
				Spec:       corev1.NodeSpec{},
			},

			expected: false,
		},
		{
			name:    "new node no label",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}},
				Spec:       corev1.NodeSpec{},
			},

			expected: false,
		},
		{
			name:    "new node has taints",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.NodeRoleLabelKey: "data-plane"}},
				Spec: corev1.NodeSpec{
					Taints: []corev1.Taint{{Key: corev1.TaintNodeUnschedulable}},
				},
			},

			expected: false,
		},
		{
			name:    "un-schedule to schedule, add label",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.NodeRoleLabelKey: "data-plane"}},
				Spec:       corev1.NodeSpec{},
			},

			expected: true,
		},
		{
			name:    "un-schedule to schedule, remove taints",
			oldNode: &cache.NodeInfo{Label: map[string]string{constants.NodeRoleLabelKey: "data-plane"}, Taints: []corev1.Taint{{Key: corev1.TaintNodeUnschedulable}}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{constants.NodeRoleLabelKey: "data-plane"}},
				Spec:       corev1.NodeSpec{},
			},

			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, isNodeTurnToSchedule(tt.oldNode, tt.newNode), tt.expected)
		})
	}
}

func Test_isVPAPausePodTurnToInsufficient(t *testing.T) {
	tests := []struct {
		name     string
		oldNode  *cache.NodeInfo
		newNode  *corev1.Node
		expected bool
	}{
		{
			name:    "old node cache is nil",
			oldNode: nil,
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{}},
				Spec:       corev1.NodeSpec{},
			},
			expected: false,
		},
		{
			name:    "new node has no vpa pause pod fault label",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{}},
				Spec:       corev1.NodeSpec{},
			},
			expected: false,
		},
		{
			name:    "new node has vpa pause pod fault label but not resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: "default"}},
				Spec:       corev1.NodeSpec{},
			},
			expected: false,
		},
		{
			name:    "new node has vpa pause pod fault label and is resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
				Spec:       corev1.NodeSpec{},
			},

			expected: true,
		},
		{
			name:    "node vpa pause pod fault label turn to resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: "default"}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
				Spec:       corev1.NodeSpec{},
			},

			expected: true,
		},
		{
			name:    "old and new node both has vpa pause pod fault label and is resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
				Spec:       corev1.NodeSpec{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
				return tt.oldNode
			})
			defer patch.Reset()
			assert.Equal(t, isVPAPausePodTurnToInsufficient(tt.newNode), tt.expected)
		})
	}
}

func Test_sortMigratePod(t *testing.T) {
	var (
		patch   = gomonkey.NewPatches()
		node    *corev1.Node
		podList *corev1.PodList
		traceID string
	)

	node = &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}
	podList = &corev1.PodList{Items: []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod1_valid",
				Namespace: "default",
				Annotations: map[string]string{
					"resource": " {\"worker\":{\"cpuLimit\":1000,\"cpuRequest\":100,\"memoryLimit\":3686,\"memoryRequest\":100},\"runtime\":{\"cpuLimit\":1000,\"cpuRequest\":100,\"memoryLimit\":3686,\"memoryRequest\":100}}",
				},
				Labels: map[string]string{"isPoolPod": "false", "funcName": "aaa"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod2_invalid",
				Namespace: "default",
				Annotations: map[string]string{
					"resource": "{\"worker\":{\"cpuLimit\":1000,\"cpuRequest\":100}",
				},
				Labels: map[string]string{"isPoolPod": "false", "funcName": "aaa"}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod3_valid",
				Namespace: "default",
				Labels:    map[string]string{"isPoolPod": "false", "funcName": "aaa"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{
					Name:  tools.RuntimeContainerName,
					Image: "mock-runtime-image",
					Resources: corev1.ResourceRequirements{
						Limits: map[corev1.ResourceName]resource.Quantity{
							"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
							"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
						},
						Requests: map[corev1.ResourceName]resource.Quantity{
							"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
							"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
						},
					},
				},
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod4_invalid",
				Namespace: "default",
				Labels:    map[string]string{"isPoolPod": "false", "funcName": "aaa"}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{
					Name:  tools.WorkerContainerName,
					Image: "mock-runtime-image",
					Resources: corev1.ResourceRequirements{
						Limits: map[corev1.ResourceName]resource.Quantity{
							"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
							"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
						},
						Requests: map[corev1.ResourceName]resource.Quantity{
							"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
							"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
						},
					},
				},
			}},
		},
	}}

	convey.Convey("test sortMigratePod", t, func() {
		convey.Convey("test sort migrate pod by cpu", func() {
			migratePodList, err := sortMigratePod(node, podList, traceID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(migratePodList), convey.ShouldEqual, 2)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})

}

func Test_selectMigrateNode(t *testing.T) {
	var (
		patch     = gomonkey.NewPatches()
		nodeList  []*corev1.Node
		funcSvcID string
		traceID   string
	)
	label := map[string]string{"ssd": "true"}
	nodeList = []*corev1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-node1",
				Namespace: "default",
				Labels:    label,
			},
			Status: corev1.NodeStatus{
				Allocatable: corev1.ResourceList{
					"cpu":     resource.MustParse("500"),
					"memory":  resource.MustParse("500"),
					"storage": resource.MustParse("500"),
				},
			},
		},
	}

	convey.Convey("test selectMigrateNode", t, func() {
		convey.Convey("test get node list error", func() {
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			flag := false
			patch.ApplyMethod(reflect.TypeOf(&fakeNodeLister{}), "List",
				func(_ *fakeNodeLister, selector labels.Selector) ([]*corev1.Node, error) {
					flag = true
					return nil, errors.New("get node list error")
				})
			_, err := selectMigrateNode(funcSvcID, traceID)
			if err != nil {
				convey.So(flag, convey.ShouldBeTrue)
			}
		})

		convey.Convey("test get node list is empty", func() {
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			mockNodeListerList(patch, nodeList, nil)
			_, err := selectMigrateNode(funcSvcID, traceID)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test get node is unscheduled", func() {
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			mockNodeListerList(patch, nodeList, nil)
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{})
			mockIsNodeNormalSchedule(patch, false)
			_, err := selectMigrateNode(funcSvcID, traceID)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test k8s unsupported metrics server", func() {
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			mockNodeListerList(patch, nodeList, nil)
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        0,
				AllocatableCPU:    0,
				RequestMemory:     0,
				AllocatableMemory: 0,
			})
			mockIsNodeNormalSchedule(patch, true)
			_, err := selectMigrateNode(funcSvcID, traceID)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test get node with lower pressure", func() {
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			mockNodeListerList(patch, nodeList, nil)
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        100,
				AllocatableCPU:    1000,
				RequestMemory:     200,
				AllocatableMemory: 1024,
			})
			mockIsNodeNormalSchedule(patch, true)
			chooseNode, err := selectMigrateNode(funcSvcID, traceID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(chooseNode.Name, convey.ShouldEqual, "test-node1")
		})

		convey.Convey("test get node with high pressure", func() {
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			mockNodeListerList(patch, nodeList, nil)
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        900,
				AllocatableCPU:    1000,
				RequestMemory:     900,
				AllocatableMemory: 1024,
			})
			mockIsNodeNormalSchedule(patch, true)
			_, err := selectMigrateNode(funcSvcID, traceID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "no available node for migrate")
		})

		convey.Convey("test get node pressure between low and high pressure", func() {
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			mockNodeListerList(patch, nodeList, nil)
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        700,
				AllocatableCPU:    1000,
				RequestMemory:     750,
				AllocatableMemory: 1024,
			})
			mockIsNodeNormalSchedule(patch, true)
			chooseNode, err := selectMigrateNode(funcSvcID, traceID)
			convey.So(err, convey.ShouldBeNil)
			convey.So(chooseNode.Name, convey.ShouldEqual, "test-node1")
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_isNodeNormalSchedule(t *testing.T) {
	var (
		patch     = gomonkey.NewPatches()
		cached    *cache.NodeInfo
		pod       *corev1.Pod
		funcSvcID string
		name      string
		traceID   string
	)
	pod = &corev1.Pod{
		Status: corev1.PodStatus{
			Phase: "Running",
		},
	}

	convey.Convey("test isNodeNormalSchedule", t, func() {
		convey.Convey("test node with gpu-node taints", func() {
			cached = &cache.NodeInfo{IsDefaultLabel: true, Taints: []corev1.Taint{{Key: "node-type", Value: "gpu"}}}
			res := isNodeNormalSchedule(cached, funcSvcID, name, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test node with other taints", func() {
			cached = &cache.NodeInfo{Taints: []corev1.Taint{{Key: "node-role.kubernetes.io/master"}}}
			res := isNodeNormalSchedule(cached, funcSvcID, name, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test node with default labels", func() {
			cached = &cache.NodeInfo{IsDefaultLabel: true}
			res := isNodeNormalSchedule(cached, funcSvcID, name, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test node with unhealthy vpa pod", func() {
			cached = &cache.NodeInfo{}
			mockGetVpaPod(patch, &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
				},
			}, nil)
			res := isNodeNormalSchedule(cached, funcSvcID, name, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test node with anti affinity nodes", func() {
			cached = &cache.NodeInfo{}
			mockGetVpaPod(patch, pod, nil)
			funcSvcID = "aaa"
			name = "node1"
			cache.WorkerCache.CreateFunctionCacheByFuncMeta(funcSvcID,
				&faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: funcSvcID}})
			cache.WorkerCache.UpdateFuncScheduleNodes(&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node1",
					Labels: map[string]string{
						"default": "default-NoSchedule",
						"xx":      "xxx-NoSchedule",
					},
				},
			})
			res := isNodeNormalSchedule(cached, funcSvcID, name, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test node with not in affinity nodes", func() {
			cached = &cache.NodeInfo{}
			mockGetVpaPod(patch, pod, nil)
			funcSvcID = "aaa"
			name = "node1"
			cache.WorkerCache.CreateFunctionCacheByFuncMeta(funcSvcID,
				&faasfunction.FunctionService{ObjectMeta: faasfunction.ObjectMeta{ID: funcSvcID}})
			cache.WorkerCache.UpdateFuncScheduleNodes(&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node2",
					Labels: map[string]string{
						"key2": "value2-Schedule",
					},
				},
			})
			res := isNodeNormalSchedule(cached, funcSvcID, name, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test success", func() {
			cached = &cache.NodeInfo{}
			mockGetVpaPod(patch, pod, nil)
			funcSvcID = "bbb"
			name = "node-bbb3"
			cache.WorkerCache.CreateFunctionCacheByFuncMeta(funcSvcID,
				&faasfunction.FunctionService{
					ObjectMeta: faasfunction.ObjectMeta{ID: funcSvcID},
					Spec: faasfunction.FunctionServiceSpec{
						Configuration: faasfunction.ConfigurationSpec{
							FgsTags: map[string]string{
								"key1": "value1",
								"key2": "value2",
							},
						},
					},
				})
			cache.WorkerCache.UpdateFuncScheduleNodes(&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-bbb3",
					Labels: map[string]string{
						"key1": "value1-Schedule",
					},
				},
			})
			res := isNodeNormalSchedule(cached, funcSvcID, name, traceID)
			convey.So(res, convey.ShouldBeTrue)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_prepareMigratingVpaPods(t *testing.T) {
	var (
		patch   = gomonkey.NewPatches()
		node    *corev1.Node
		podList *corev1.PodList
	)
	node = &corev1.Node{}
	pod1 := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod1_valid",
			Namespace: "default",
			Annotations: map[string]string{
				"resource": " {\"worker\":{\"cpuLimit\":1000,\"cpuRequest\":100,\"memoryLimit\":3686,\"memoryRequest\":100},\"runtime\":{\"cpuLimit\":1000,\"cpuRequest\":100,\"memoryLimit\":3686,\"memoryRequest\":100}}",
			},
			Labels: map[string]string{"isPoolPod": "false", "funcName": "aaa"},
		},
		Status: corev1.PodStatus{
			Phase: "Running",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	pod2 := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod2_valid",
			Namespace: "default",
			Labels:    map[string]string{"isPoolPod": "false", "funcName": "bbb"},
		},
		Status: corev1.PodStatus{
			Phase: "Running",
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{
				Name:  tools.RuntimeContainerName,
				Image: "mock-runtime-image",
				Resources: corev1.ResourceRequirements{
					Limits: map[corev1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
						"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
					},
					Requests: map[corev1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse(fmt.Sprintf("%dm", 500)),
						"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
					},
				},
			},
		}},
	}

	convey.Convey("test prepareMigratingVpaPods", t, func() {
		convey.Convey("test clear pods by node func error", func() {
			RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
				return errors.New("clear pods by node func error")
			})

			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test list pods by node name error", func() {
			mockRegisterCleanPodsCallBack(nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, nil, errors.New("clear pods by node func error"))
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test sort migrate pod error", func() {
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, &corev1.PodList{}, nil)
			mockSortMigratePod(patch, nil, errors.New("sort migrate pod error"))
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test list pods is empty", func() {
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, &corev1.PodList{}, nil)
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test list pods is not ready", func() {
			podList = &corev1.PodList{}
			podList.Items = append(podList.Items, pod1, pod2)
			podList.Items[0].DeletionTimestamp = &metav1.Time{}
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, podList, nil)
			mockIsPodReady(patch, false)
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test get full function name by pod is empty", func() {
			podList = &corev1.PodList{}
			podList.Items = append(podList.Items, pod1, pod2)
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, podList, nil)
			mockIsPodReady(patch, true)
			mockGetFullFuncNameByPod(patch, "")
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test get owned by function service is empty", func() {
			podList = &corev1.PodList{}
			podList.Items = append(podList.Items, pod1, pod2)
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, podList, nil)
			mockIsPodReady(patch, true)
			mockOwnedByFunctionService(patch, "", true)
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test get select migrate node error", func() {
			podList = &corev1.PodList{}
			podList.Items = append(podList.Items, pod1, pod2)
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, podList, nil)
			mockIsPodReady(patch, true)
			mockOwnedByFunctionService(patch, "test-func1", true)
			mockSelectMigrateNode(patch, corev1.Node{}, errors.New("select migrate node error"))
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test check cur node resource pressure failed", func() {
			podList = &corev1.PodList{}
			podList.Items = append(podList.Items, pod1, pod2)
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, podList, nil)
			mockIsPodReady(patch, true)
			mockOwnedByFunctionService(patch, "test-func1", true)
			mockSelectMigrateNode(patch, corev1.Node{}, nil)
			mockMigratePodToSpecificNode(patch)
			mockWaitPodDeletedOrTimeout(patch)
			mockCheckCurNodeResourcePressure(patch, false)
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test check cur node resource pressure success", func() {
			podList = &corev1.PodList{}
			podList.Items = append(podList.Items, pod1, pod2)
			mockCleanPodsByNodeFunc(patch, nil)
			mockKubeClient(patch, &kubernetes.Clientset{})
			mockKubeClientCoreV1(patch, &fake.FakeCoreV1{})
			mockKubeClientPods(patch, &fake.FakePods{})
			mockKubeClientPodsList(patch, podList, nil)
			mockIsPodReady(patch, true)
			mockOwnedByFunctionService(patch, "test-func1", true)
			mockSelectMigrateNode(patch, corev1.Node{}, nil)
			mockMigratePodToSpecificNode(patch)
			mockWaitPodDeletedOrTimeout(patch)
			mockCheckCurNodeResourcePressure(patch, true)
			err := prepareMigratingVpaPods(node)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_waitPodDeletedOrTimeout(t *testing.T) {
	var (
		patch   = gomonkey.NewPatches()
		podName string
		traceID string
	)
	convey.Convey("test waitPodDeletedOrTimeout", t, func() {
		convey.Convey("test get pod is exist", func() {
			patch.ApplyFunc(tools.GetPodInformer, func() informerscorev1.PodInformer {
				return mockutils.InformerFakeFactory.Core().V1().Pods()
			})

			mockGetPodInformer(patch, &fakePodInformer{})
			flag := false
			patch.ApplyMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "Get",
				func(_ *fakePodNamespaceLister, name string) (*corev1.Pod, error) {
					flag = true
					return &corev1.Pod{}, nil
				})
			mockTimeSleep(patch)
			waitPodDeletedOrTimeout(podName, traceID)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test get pod is not found", func() {
			mockGetPodInformer(patch, &fakePodInformer{})
			mockPodNamespaceListerGet(patch, nil, errors.New("not found"))
			times := 0
			patch.ApplyFunc(tools.IsPodNotFound, func(err error) bool {
				times++
				return true
			})

			mockTimeSleep(patch)
			waitPodDeletedOrTimeout(podName, traceID)
			convey.So(times, convey.ShouldEqual, 1)
		})

		convey.Convey("test get pod is other error", func() {
			mockGetPodInformer(patch, &fakePodInformer{})
			mockPodNamespaceListerGet(patch, nil, errors.New("other error"))
			times := 0
			patch.ApplyFunc(tools.IsPodNotFound, func(err error) bool {
				times++
				return false
			})

			mockTimeSleep(patch)
			waitPodDeletedOrTimeout(podName, traceID)
			convey.So(times, convey.ShouldEqual, 10)
		})
		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_checkCurNodeResourcePressure(t *testing.T) {
	var (
		patch   = gomonkey.NewPatches()
		curNode *corev1.Node
		traceID string
	)
	curNode = &corev1.Node{}

	convey.Convey("test checkCurNodeResourcePressure", t, func() {
		convey.Convey("test compute node resource error", func() {
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        0,
				AllocatableCPU:    0,
				RequestMemory:     0,
				AllocatableMemory: 0,
			})
			res := checkCurNodeResourcePressure(curNode, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test node resource is high pressure", func() {
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        900,
				AllocatableCPU:    1000,
				RequestMemory:     950,
				AllocatableMemory: 1024,
			})
			res := checkCurNodeResourcePressure(curNode, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test node vpa pod is unhealthy", func() {
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        700,
				AllocatableCPU:    1000,
				RequestMemory:     750,
				AllocatableMemory: 1024,
			})

			mockGetVpaPod(patch, &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: "Pending",
				},
			}, nil)
			res := checkCurNodeResourcePressure(curNode, traceID)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test cur node is normal", func() {
			mockCacheGetNodeInfo(patch, &cache.NodeInfo{
				RequestCPU:        700,
				AllocatableCPU:    1000,
				RequestMemory:     750,
				AllocatableMemory: 1024,
			})

			mockGetVpaPod(patch, &corev1.Pod{
				Status: corev1.PodStatus{
					Phase: "Running",
				},
			}, nil)
			res := checkCurNodeResourcePressure(curNode, traceID)
			convey.So(res, convey.ShouldBeTrue)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestMigrateVpaPodsOnNode(t *testing.T) {
	var (
		patch = gomonkey.NewPatches()
		node  *corev1.Node
	)
	node = &corev1.Node{}
	convey.Convey("test MigrateVpaPodsOnNode", t, func() {
		convey.Convey("test get config error", func() {
			flag := false
			patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				flag = true
				return nil, errors.New("get config error")
			})
			MigrateVpaPodsOnNode(node)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test migrate vpa pods error", func() {
			flag := false
			node.Labels = map[string]string{
				constants.NodeVpaPausePodFaultsLabelKey: "test",
			}
			mockGetConfig(patch, &config.FaasConfig{}, nil)
			mockTimeSleep(patch)
			mockNodeInformer(patch, fakeNodeInformer{})
			mockNodeLister(patch, fakeNodeLister{})
			mockNodeListerGet(patch, node, nil)
			patch.ApplyFunc(prepareMigratingVpaPods, func(node *corev1.Node) error {
				flag = true
				return errors.New("migrate vpa pods error")
			})
			MigrateVpaPodsOnNode(node)
			convey.So(flag, convey.ShouldBeTrue)
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

func mockKubeClientPodsList(patch *gomonkey.Patches, pausePodList *corev1.PodList, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fake.FakePods{}), "List",
		func(_ *fake.FakePods, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
			return pausePodList, err
		})
}

func mockSortMigratePod(patch *gomonkey.Patches, ret []migratePod, err error) {
	patch.ApplyFunc(sortMigratePod, func(node *corev1.Node, podList *corev1.PodList, traceID string) ([]migratePod,
		error) {
		return ret, err
	})
}

func mockOwnedByFunctionService(patch *gomonkey.Patches, ref string, res bool) {
	patch.ApplyFunc(tools.OwnedByFunctionService, func(pod *corev1.Pod) (string, bool) {
		return ref, res
	})
}

func mockSelectMigrateNode(patch *gomonkey.Patches, ret corev1.Node, err error) {
	patch.ApplyFunc(selectMigrateNode, func(funcSvcID, traceID string) (corev1.Node, error) {
		return ret, err
	})
}

func mockMigratePodToSpecificNode(patch *gomonkey.Patches) {
	patch.ApplyFunc(MigratePodToSpecificNode, func(pod corev1.Pod, node string) {
		return
	})
}

func mockWaitPodDeletedOrTimeout(patch *gomonkey.Patches) {
	patch.ApplyFunc(waitPodDeletedOrTimeout, func(podName string, traceID string) {
		return
	})
}

func mockCheckCurNodeResourcePressure(patch *gomonkey.Patches, res bool) {
	patch.ApplyFunc(checkCurNodeResourcePressure, func(curNode *corev1.Node, traceID string) bool {
		return res
	})
}

func mockRegisterCleanPodsCallBack(err error) {
	RegisterCleanPodsCallBack(func(node *corev1.Node, labelSelector, traceID string) error {
		return err
	})
}

func mockIsPodReady(patch *gomonkey.Patches, res bool) {
	patch.ApplyFunc(tools.IsPodReady, func(pod *corev1.Pod) bool {
		return res
	})
}

func mockGetFullFuncNameByPod(patch *gomonkey.Patches, name string) {
	patch.ApplyFunc(tools.GetFullFuncNameByPod, func(pod *corev1.Pod) string {
		return name
	})
}

func mockPodNamespaceListerGet(patch *gomonkey.Patches, ret *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fakePodNamespaceLister{}), "Get",
		func(_ *fakePodNamespaceLister, name string) (*corev1.Pod, error) {
			return ret, err
		})
}

func mockTimeSleep(patch *gomonkey.Patches) {
	patch.ApplyFunc(time.Sleep, func(_ time.Duration) {})
}

func mockGetPodInformer(patch *gomonkey.Patches, informer informerscorev1.PodInformer) {
	patch.ApplyFunc(tools.GetPodInformer, func() informerscorev1.PodInformer {
		return informer
	})
}
func mockNodeInformer(patch *gomonkey.Patches, nodeInformer v1.NodeInformer) {
	patch.ApplyFunc(tools.GetNodeInformer, func() v1.NodeInformer {
		return nodeInformer
	})
}

func mockNodeLister(patch *gomonkey.Patches, nodeLister listerscorev1.NodeLister) {
	patch.ApplyMethod(reflect.TypeOf(fakeNodeInformer{}), "Lister",
		func(_ fakeNodeInformer) listerscorev1.NodeLister {
			return nodeLister
		})
}

func mockNodeListerList(patch *gomonkey.Patches, ret []*corev1.Node, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fakeNodeLister{}), "List",
		func(_ *fakeNodeLister, selector labels.Selector) ([]*corev1.Node, error) {
			return ret, err
		})
}

func mockNodeListerGet(patch *gomonkey.Patches, ret *corev1.Node, err error) {
	patch.ApplyMethod(reflect.TypeOf(&fakeNodeLister{}), "Get",
		func(_ *fakeNodeLister, name string) (*corev1.Node, error) {
			return ret, err
		})
}

func mockIsNodeNormalSchedule(patch *gomonkey.Patches, res bool) {
	patch.ApplyFunc(isNodeNormalSchedule, func(cached *cache.NodeInfo, funcSvcID, name, traceID string) bool {
		return res
	})
}

func mockCacheGetNodeInfo(patch *gomonkey.Patches, cached *cache.NodeInfo) {
	patch.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
		return cached
	})
}

func mockCleanPodsByNodeFunc(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc(cleanPodsByNodeFunc, func(node *corev1.Node, labelSelector string, traceID string) error {
		return err
	})
}

func mockKubeClient(patch *gomonkey.Patches, kubeClient kubernetes.Interface) {
	patch.ApplyFunc(tools.GetKubernetesClient, func() kubernetes.Interface {
		return kubeClient
	})
}

func mockGetVpaPod(patch *gomonkey.Patches, pod *corev1.Pod, err error) {
	patch.ApplyMethod(reflect.TypeOf(&vpa.VerticalPausePod{}), "GetVpaPod",
		func(_ *vpa.VerticalPausePod, ctx context.Context, podName string) (*corev1.Pod, error) {
			return pod, err
		})
}

func mockGetConfig(patch *gomonkey.Patches, cfg *config.FaasConfig, err error) {
	patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
		return cfg, err
	})
}

func mockPrepareMigratingVpaPods(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc(prepareMigratingVpaPods, func(node *corev1.Node) error {
		return err
	})
}

func Test_isVPAPausePodTurnToFaults(t *testing.T) {
	tests := []struct {
		name     string
		oldNode  *cache.NodeInfo
		newNode  *corev1.Node
		expected bool
	}{
		{
			name:    "old node cache is nil",
			oldNode: nil,
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{}},
				Spec:       corev1.NodeSpec{},
			},
			expected: false,
		},
		{
			name:    "new node has no vpa pause pod fault label",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{}},
				Spec:       corev1.NodeSpec{},
			},
			expected: false,
		},
		{
			name:    "new node has vpa pause pod fault label but not resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: "default"}},
				Spec:       corev1.NodeSpec{},
			},
			expected: true,
		},
		{
			name:    "new node has vpa pause pod fault label and is resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
				Spec:       corev1.NodeSpec{},
			},

			expected: true,
		},
		{
			name:    "node vpa pause pod fault label turn to resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: "default"}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
				Spec:       corev1.NodeSpec{},
			},

			expected: false,
		},
		{
			name:    "old and new node both has vpa pause pod fault label and is resource-Insufficient",
			oldNode: &cache.NodeInfo{Label: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
			newNode: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node", Labels: map[string]string{constants.NodeVpaPausePodFaultsLabelKey: constants.NodeVpaPausePodResourceInsufficientLabelValue}},
				Spec:       corev1.NodeSpec{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch := gomonkey.NewPatches()
			patch.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
				return tt.oldNode
			})
			defer patch.Reset()
			assert.Equal(t, isVPAPausePodTurnToFaults(tt.newNode), tt.expected)
		})
	}
}
