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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	tools "functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

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
			args{
				pod: corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"test": "aaaa"},
					},
				},
			},
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
						Labels: map[string]string{"test": "aaaa"},
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
						Labels: map[string]string{"test": "aaaa"},
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
		{
			"test not migrate mpc pod",
			args{
				pod: corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{tools.LabelMCPFunc: tools.TrueString},
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
		copyLabel := make(map[string]string, len(tt.args.pod.Labels))
		for k, v := range tt.args.pod.Labels {
			copyLabel[k] = v
		}
		MigratePodToSpecificNode(tt.args.pod, tt.args.node)
		assert.Equal(t, copyLabel, tt.args.pod.Labels, tt.name)
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
