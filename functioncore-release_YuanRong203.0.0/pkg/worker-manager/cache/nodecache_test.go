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

package cache

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/kubernetes/typed/core/v1/fake"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func TestAddNode(t *testing.T) {
	label := map[string]string{"ssd": "true"}
	nodeName := "node01"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
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
	}
	Convey("test add node", t, func() {
		AddNode(node)
		record := nodeCache.LabelRecord["ssd"]["true"]
		So(record[nodeName], ShouldNotEqual, nil)
		nodeInfo := GetNodeInfo(nodeName)
		So(len(nodeInfo.Label), ShouldEqual, len(label))
	})
}

func TestUpdateNode(t *testing.T) {
	label := map[string]string{"ssd": "true", "aaa": "bbb"}
	nodeName := "node01"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
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
	}
	Convey("test add node", t, func() {
		UpdateNode(node)
		record := nodeCache.LabelRecord["ssd"]["true"]
		So(record[nodeName], ShouldNotEqual, nil)
		record = nodeCache.LabelRecord["aaa"]["bbb"]
		So(record[nodeName], ShouldNotEqual, nil)
	})
}

func TestDeleteNode(t *testing.T) {
	label := map[string]string{"ssd": "true"}
	nodeName := "node01"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
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
	}
	Convey("test delete node", t, func() {
		DeleteNode(node)
		nodeList := GetNodesByLabel([]map[string]string{label})
		So(len(nodeList), ShouldEqual, 0)
	})
}

func TestMain(m *testing.M) {
	if nodeCache == nil {
		nodeCache = &NodeCache{
			NodeList:    make(map[string]*NodeInfo),
			LabelRecord: make(map[string]map[string]map[string]struct{}),
		}
	}
	m.Run()
}

func TestIsOverload(t *testing.T) {
	filters := []Filter{WithLabelValueIn(constants.NodeRoleLabelKey, constants.GetValidNodeRoleValues()...)}
	heterogeneousFilters := []Filter{WithLabelValueEq(map[string]string{
		constants.NodeRoleLabelKey: constants.NodeRoleLabelValue,
		utils.LabelNodeType:        utils.LowNodeType,
	})}
	type args struct {
		filters   []Filter
		threshold float64
	}
	{
		tests := []struct {
			name string
			args args
			want bool
		}{
			{name: "test001", args: args{filters: filters, threshold: 0.9}, want: true},
		}
		// node001
		nodeCache.NodeList["node001"] = &NodeInfo{}
		nodeCache.NodeList["node001"].Label = make(map[string]string)
		nodeCache.NodeList["node001"].Label[constants.NodeRoleLabelKey] = constants.NodeRoleCALabelValue
		nodeCache.NodeList["node001"].RequestCPU = 90
		nodeCache.NodeList["node001"].LimitCPU = 100
		nodeCache.NodeList["node001"].RequestMemory = 91
		nodeCache.NodeList["node001"].LimitMemory = 100
		// node002
		nodeCache.NodeList["node002"] = &NodeInfo{}
		nodeCache.NodeList["node001"].Label = make(map[string]string)
		nodeCache.NodeList["node001"].Label[constants.NodeRoleLabelKey] = "test"
		nodeCache.NodeList["node001"].RequestCPU = 90
		nodeCache.NodeList["node001"].LimitCPU = 100
		nodeCache.NodeList["node001"].RequestMemory = 91
		nodeCache.NodeList["node001"].LimitMemory = 100
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := IsOverload(tt.args.threshold, tt.args.filters...); got != tt.want {
					t.Errorf("IsOverload() = %v, want %v", got, tt.want)
				}
			})
		}
	}
	{
		tests := []struct {
			name string
			args args
			want bool
		}{
			{name: "test002", args: args{filters: filters, threshold: 0.9}, want: true},
		}
		nodeCache.NodeList["node001"] = &NodeInfo{}
		nodeCache.NodeList["node001"].Label = make(map[string]string)
		nodeCache.NodeList["node001"].Label[constants.NodeRoleLabelKey] = constants.NodeRoleLabelValue
		nodeCache.NodeList["node001"].RequestCPU = 91
		nodeCache.NodeList["node001"].LimitCPU = 100
		nodeCache.NodeList["node001"].RequestMemory = 90
		nodeCache.NodeList["node001"].LimitMemory = 100
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := IsOverload(tt.args.threshold, tt.args.filters...); got != tt.want {
					t.Errorf("IsOverload() = %v, want %v", got, tt.want)
				}
			})
		}
	}
	{
		tests := []struct {
			name string
			args args
			want bool
		}{
			{name: "test003", args: args{filters: filters, threshold: 0.9}, want: false},
		}
		nodeCache.NodeList["node001"] = &NodeInfo{}
		nodeCache.NodeList["node001"].Label = make(map[string]string)
		nodeCache.NodeList["node001"].Label[constants.NodeRoleLabelKey] = constants.NodeRoleLabelValue
		nodeCache.NodeList["node001"].RequestCPU = 90
		nodeCache.NodeList["node001"].LimitCPU = 100
		nodeCache.NodeList["node001"].RequestMemory = 90
		nodeCache.NodeList["node001"].LimitMemory = 100
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := IsOverload(tt.args.threshold, tt.args.filters...); got != tt.want {
					t.Errorf("IsOverload() = %v, want %v", got, tt.want)
				}
			})
		}
	}
	{
		// test zero
		tests := []struct {
			name string
			args args
			want bool
		}{
			{name: "test004", args: args{filters: filters, threshold: 0.9}, want: true},
		}
		nodeCache.NodeList["node001"] = &NodeInfo{}
		nodeCache.NodeList["node001"].Label = make(map[string]string)
		nodeCache.NodeList["node001"].Label[constants.NodeRoleLabelKey] = constants.NodeRoleLabelValue
		nodeCache.NodeList["node001"].RequestCPU = 0
		nodeCache.NodeList["node001"].LimitCPU = 0
		nodeCache.NodeList["node001"].RequestMemory = 90
		nodeCache.NodeList["node001"].LimitMemory = 100
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := IsOverload(tt.args.threshold, tt.args.filters...); got != tt.want {
					t.Errorf("IsOverload() = %v, want %v", got, tt.want)
				}
			})
		}
	}
	{
		tests := []struct {
			name string
			args args
			want bool
		}{
			{name: "test005 heterogeneous scheduling", args: args{filters: heterogeneousFilters, threshold: 0.9}, want: true},
		}
		// node001
		nodeCache.NodeList["node001"] = &NodeInfo{}
		nodeCache.NodeList["node001"].Label = make(map[string]string)
		nodeCache.NodeList["node001"].Label[constants.NodeRoleLabelKey] = constants.NodeRoleLabelValue
		nodeCache.NodeList["node001"].Label[utils.LabelNodeType] = utils.HighNodeType
		nodeCache.NodeList["node001"].RequestCPU = 90
		nodeCache.NodeList["node001"].LimitCPU = 100
		nodeCache.NodeList["node001"].RequestMemory = 90
		nodeCache.NodeList["node001"].LimitMemory = 100
		// node002
		nodeCache.NodeList["node001"] = &NodeInfo{}
		nodeCache.NodeList["node001"].Label = make(map[string]string)
		nodeCache.NodeList["node001"].Label[constants.NodeRoleLabelKey] = constants.NodeRoleLabelValue
		nodeCache.NodeList["node001"].Label[utils.LabelNodeType] = utils.LowNodeType
		nodeCache.NodeList["node001"].RequestCPU = 91
		nodeCache.NodeList["node001"].LimitCPU = 100
		nodeCache.NodeList["node001"].RequestMemory = 91
		nodeCache.NodeList["node001"].LimitMemory = 100
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := IsOverload(tt.args.threshold, tt.args.filters...); got != tt.want {
					t.Errorf("IsOverload() = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

// TestInitNodeCache_ListNodeErr -
func TestInitNodeCache_ListNodeErr(t *testing.T) {
	Convey("test: list node err", t, func() {

		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &fake.FakeCoreV1{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Nodes",
				func(_ *fake.FakeCoreV1) typecorev1.NodeInterface {
					return &fake.FakeNodes{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeNodes{}), "List",
				func(_ *fake.FakeNodes, ctx context.Context, opts metav1.ListOptions) (*corev1.NodeList, error) {
					return nil, errors.New("test err")
				}),
		})
		defer patches.ResetAll()
		err := InitNodeCache([]corev1.Pod{})
		So(err, ShouldNotBeNil)
	})
}

// TestInitNodeCache
func TestInitNodeCache(t *testing.T) {
	Convey("test: init node cache", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
				return &kubernetes.Clientset{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
				func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
					return &fake.FakeCoreV1{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeCoreV1{}), "Nodes",
				func(_ *fake.FakeCoreV1) typecorev1.NodeInterface {
					return &fake.FakeNodes{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeNodes{}), "List",
				func(_ *fake.FakeNodes, ctx context.Context, opts metav1.ListOptions) (*corev1.NodeList, error) {
					return &corev1.NodeList{
						Items: []corev1.Node{{}},
					}, nil
				}),
		})
		defer patches.ResetAll()
		podList := &corev1.PodList{Items: make([]corev1.Pod, 3)}
		// is not selected
		podList.Items[0].Labels = map[string]string{utils.LabelEnableTMPExpansion: utils.FalseString}
		podList.Items[1].Labels = map[string]string{utils.LabelEnableTMPExpansion: utils.TrueString}
		// is not selected
		podList.Items[1].Spec.NodeName = "test-not-exist"
		podList.Items[2].Labels = map[string]string{utils.LabelEnableTMPExpansion: utils.TrueString}
		// is selected
		GB := 1024 * 1024 * 1024
		podList.Items[2].Spec.NodeName = "test-exist"
		nodeCache.NodeList["test-exist"] = &NodeInfo{
			remainingCapacityStorage: 30 * int64(GB),
		}
		err := initNodeCache(podList.Items)
		So(err, ShouldBeNil)
		So(nodeCache.NodeList["test-exist"].remainingCapacityStorage, ShouldEqual, 20*GB)
		delete(nodeCache.NodeList, "test-exist")
	})
}

func TestWithLabelValueEq(t *testing.T) {
	tests := []struct {
		name       string
		labels     map[string]string
		nodeLabels map[string]string
		match      bool
	}{
		{
			name:       "filter labels empty",
			labels:     nil,
			nodeLabels: map[string]string{"a": "b", "c": "d"},
			match:      true,
		},
		{
			name:       "node labels empty",
			labels:     map[string]string{"a": "b"},
			nodeLabels: nil,
			match:      false,
		},
		{
			name:       "all nil",
			labels:     nil,
			nodeLabels: nil,
			match:      true,
		},
		{
			name:       "node labels contains labels",
			labels:     map[string]string{"a": "b"},
			nodeLabels: map[string]string{"a": "b", "c": "d"},
			match:      true,
		},
		{
			name:       "labels contains node labels",
			labels:     map[string]string{"a": "b", "c": "d"},
			nodeLabels: map[string]string{"a": "b"},
			match:      false,
		},
		{
			name:       "labels value mismatch",
			labels:     map[string]string{"a": "a"},
			nodeLabels: map[string]string{"a": "b"},
			match:      false,
		},
		{
			name:       "some label match",
			labels:     map[string]string{"a": "b", "x": "y"},
			nodeLabels: map[string]string{"c": "d", "e": "f"},
			match:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := WithLabelValueEq(tt.labels)
			node := &NodeInfo{Label: tt.nodeLabels}
			assert.Equal(t, tt.match, filter.match(node))
		})
	}
}

func TestWithLabelValueIn(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		values     []string
		nodeLabels map[string]string
		match      bool
	}{
		{
			name:       "no values",
			key:        "a",
			values:     nil,
			nodeLabels: map[string]string{"a": "b"},
			match:      true,
		},
		{
			name:       "no such key",
			key:        "x",
			values:     []string{"y"},
			nodeLabels: map[string]string{"a": "b"},
			match:      false,
		},
		{
			name:       "value match",
			key:        "a",
			values:     []string{"a", "b", "c"},
			nodeLabels: map[string]string{"a": "c"},
			match:      true,
		},
		{
			name:       "value mismatch",
			key:        "a",
			values:     []string{"a", "b", "c"},
			nodeLabels: map[string]string{"a": "x"},
			match:      false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := WithLabelValueIn(tt.key, tt.values...)
			node := &NodeInfo{Label: tt.nodeLabels}
			assert.Equal(t, tt.match, filter.match(node))
		})
	}
}

func TestGetNodeNameList(t *testing.T) {

	Convey("case01: When the node map is not empty, the return node list is not empty", t, func() {
		nodeMap := make(map[string]*NodeInfo)
		nodeMap["mock-node-name-01"] = &NodeInfo{}
		nodeMap["mock-node-name-02"] = &NodeInfo{}
		nodeNameList := GetNodeNameList(nodeMap)
		So(len(nodeNameList), ShouldEqual, 2)
	})

	Convey("case02: When the node map is empty, the return node list is empty", t, func() {
		nodeMap := make(map[string]*NodeInfo)
		nodeNameList := GetNodeNameList(nodeMap)
		So(len(nodeNameList), ShouldEqual, 0)
	})
}

func TestRecoverNodeRemainCapacityStorage(t *testing.T) {
	GB := 1024 * 1024 * 1024
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "exist-node",
			Namespace: "default",
		},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				"cpu":    resource.MustParse("500"),
				"memory": resource.MustParse("500"),
			},
		},
	}
	AddNode(node)
	Convey("case01: node not exist, won't restore 10g", t, func() {
		RecoverNodeRemainCapacityStorage("not-exist-node", "not-exist-pod")
	})
	Convey("case02: node exist, will recover 10g", t, func() {
		RecoverNodeRemainCapacityStorage("exist-node", "exist-pod")
		So(GetNodeInfo(node.ObjectMeta.Name).remainingCapacityStorage, ShouldEqual,
			10*int64(GB)-normalInsStorageCapacity)
	})
}

func TestGetAvailableNodesForTMPExpansion(t *testing.T) {
	Convey("test: get available nodes for tmp expansion option", t, func() {
		// initialize three nodes, two nodes are selected and one node is not selected
		keysList := []string{"node1", "node2", "node3"}
		GB := 1024 * 1024 * 1024
		for _, key := range keysList {
			nodeCache.NodeList[key] = &NodeInfo{
				// initialization status: 50 GB
				remainingCapacityStorage: 50 * int64(GB),
			}
		}
		// node3: 5GB
		nodeCache.NodeList["node3"].remainingCapacityStorage = 5 * int64(GB)
		// recover
		defer func() {
			for key := range nodeCache.NodeList {
				delete(nodeCache.NodeList, key)
			}
		}()
		nodes, _ := GetAvailableNodesForTMPExpansion()
		So(len(nodes), ShouldEqual, 2)
		So(nodeCache.NodeList["node3"].remainingCapacityStorage, ShouldEqual, 5*int64(GB))
		So(nodeCache.NodeList["node2"].remainingCapacityStorage, ShouldEqual, 50*int64(GB))
		So(nodeCache.NodeList["node1"].remainingCapacityStorage, ShouldEqual, 50*int64(GB))
		So(nodeCache.needReserveStorageNums, ShouldEqual, 1)
	})
}

func TestReduceNodeStorage(t *testing.T) {
	Convey("test: reduce node storage", t, func() {
		// the pod selected is on the node1, other nodes will recover
		GB := 1024 * 1024 * 1024
		node1 := &NodeInfo{remainingCapacityStorage: 10 * int64(GB)}
		node2 := &NodeInfo{remainingCapacityStorage: 10 * int64(GB)}
		node3 := &NodeInfo{remainingCapacityStorage: 10 * int64(GB)}
		nodesMap := map[string]*NodeInfo{
			"node1": node1,
			"node2": node2,
			"node3": node3,
		}
		ReduceNodeStorage(nodesMap, "node1")
		So(node1.remainingCapacityStorage, ShouldEqual, 0)
		So(node2.remainingCapacityStorage, ShouldEqual, 10*int64(GB))
		So(node3.remainingCapacityStorage, ShouldEqual, 10*int64(GB))
	})
}

func TestGetNodeInfoCopy(t *testing.T) {
	a := &NodeInfo{
		NodeIP: "127.0.0.1",
	}
	nodeCache.mux.Lock()
	nodeCache.NodeList["test_copy_node"] = a
	nodeCache.mux.Unlock()

	b := GetNodeInfo("test_copy_node")

	// assert a and b are equal but with different pointer
	assert.Equal(t, a, b)
	assert.NotSame(t, a, b)
}

func Test_isMatchedNode(t *testing.T) {
	type args struct {
		node   *NodeInfo
		labels []map[string]string
	}
	var a args
	label := map[string]string{}
	label["test"] = "test"
	labe2 := map[string]string{}
	labe2["test"] = "test1"
	labels := []map[string]string{}
	labels = append(labels, label)
	node := &NodeInfo{Label: label}
	a.node = node
	a.labels = labels

	var a2 args
	node2 := &NodeInfo{Label: labe2}
	a2.node = node2
	a2.labels = labels
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"case1", a, true},
		{"case2", a2, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isMatchedNode(tt.args.node, tt.args.labels), "isMatchedNode(%v, %v)", tt.args.node, tt.args.labels)
		})
	}
}

func Test_GetK8sNodeCacheByDeepCopy(t *testing.T) {
	Convey("test: Load !ok", t, func() {
		nc := GetK8sNodeCacheByDeepCopy("podName")
		if nc != nil {
			t.Errorf("this is error")
		}
	})
	Convey("test: unmarshal !ok", t, func() {
		NodeK8sCache.Store("podName", 1)
		defer func() { NodeK8sCache.Delete("podName") }()
		nc := GetK8sNodeCacheByDeepCopy("podName")
		if nc != nil {
			t.Errorf("this is error")
		}
	})
	Convey("test: clear", t, func() {
		NodeK8sCache.Store("podName", &corev1.Node{})
		defer func() { NodeK8sCache.Delete("podName") }()
		nc := GetK8sNodeCacheByDeepCopy("podName")
		if nc == nil {
			t.Errorf("this is error")
		}
	})
}

func TestGetNodeNameByNodeIP(t *testing.T) {
	type args struct {
		nodeIP string
	}
	tests := []struct {
		name     string
		args     args
		nodeList map[string]*NodeInfo
		want     string
	}{
		{
			"case 1 test return value",
			args{
				"127.0.0.1",
			},
			map[string]*NodeInfo{
				"node1": {
					NodeIP: "127.0.0.1",
				},
			},
			"node1",
		},
		{
			"case 1 test return empty",
			args{
				"127.0.0",
			},
			map[string]*NodeInfo{
				"node1": {
					NodeIP: "127.0.0.1",
				},
			},
			"",
		},
	}
	for _, tt := range tests {
		temp := nodeCache.NodeList
		nodeCache.NodeList = tt.nodeList

		if tt.want != GetNodeNameByNodeIP(tt.args.nodeIP) {
			t.Errorf("failed to test TestGetNodeNameByNodeIP")
		}
		nodeCache.NodeList = temp
	}
}

func TestConvertNodeMirroredDiskCacheToBytes(t *testing.T) {
	type args struct {
		nodeMirroredDiskStr string
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			"case 1",
			args{
				"10G",
			},
			10737418240,
		},
		{
			"case 2",
			args{
				"10B",
			},
			10,
		},
		{
			"case 3",
			args{
				"1000",
			},
			0,
		},
		{
			"case 4",
			args{
				"",
			},
			0,
		},
		{
			"case 5",
			args{
				"0",
			},
			0,
		},
		{
			"case 6",
			args{
				"100K",
			},
			102400,
		},
		{
			"case 7",
			args{
				"100M",
			},
			104857600,
		},
		{
			"case 8",
			args{
				"100T",
			},
			109951162777600,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := ConvertNodeMirroredDiskCacheToBytes(tt.args.nodeMirroredDiskStr)
			if tt.want != res {
				t.Errorf("failed to test TestConvertNodeMirroredDiskCacheToBytes, want: %d, res: %d", tt.want, res)
			}
		})
	}
}

func TestObtainingNodesSuitableForCache(t *testing.T) {
	type args struct {
		cacheNodesPercentage float64
		cacheNodesNumber     int
	}
	var a args
	NodeMirroredDiskCache.Store(0, "")
	NodeMirroredDiskCache.Store("", 0)

	NodeMirroredDiskCache.Store("127.0.0.1", "")
	NodeMirroredDiskCache.Store("127.0.0.2", "1G")
	NodeMirroredDiskCache.Store("127.0.0.3", "2G")

	result := []string{"127.0.0.3", "127.0.0.2"}
	tests := []struct {
		name        string
		args        args
		want        []string
		patchesFunc mockUtils.PatchesFunc
	}{
		{"Obtaining Nodes Suitable For Cache", a, result, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, ObtainingNodesSuitableForCache(tt.args.cacheNodesPercentage, tt.args.cacheNodesNumber), "ObtainingNodesSuitableForCache(%v, %v)", tt.args.cacheNodesPercentage, tt.args.cacheNodesNumber)
		})
		patches.ResetAll()
	}
}

func TestIsNoDefaultScheduleNode(t *testing.T) {
	type args struct {
		labels map[string]string
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "IsDefaultScheduleNode",
			args: args{
				labels: map[string]string{
					"default": "default-NoSchedule",
					"key2":    "value2-NoSchedule",
				},
			},
			want: true,
		},
		{
			name: "NotDefaultScheduleNode",
			args: args{
				labels: map[string]string{
					"default": "default-NoSche",
					"key2":    "value2-NoSchedule",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isNoDefaultScheduleNode(tt.args.labels), "IsNoDefaultScheduleNode(%v)", tt.args.labels)
		})
	}
}

func TestIsHaveNoScheduleLabels(t *testing.T) {
	type args struct {
		labels map[string]string
	}

	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "IncludeNoScheduleLabels",
			args: args{
				labels: map[string]string{
					"key1": "value1",
					"key2": "value2-NoSchedule",
				},
			},
			want: true,
		},
		{
			name: "NotIncludeNoScheduleLabels",
			args: args{
				labels: map[string]string{
					"key1": "value1",
					"key2": "value2",
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, IncludeNoScheduleLabels(tt.args.labels), "IsHaveNoScheduleLabels(%v)", tt.args.labels)
		})
	}
}

func TestGetRandomNodeFromNodeList(t *testing.T) {
	tests := []struct {
		name     string
		nodeList map[string]*NodeInfo
		want     string
	}{
		{
			"case 1 test nodeList empty",
			map[string]*NodeInfo{},
			"",
		},
		{
			"case 2 test return nodeID",
			map[string]*NodeInfo{
				"node1": {
					NodeIP: "127.0.0.1",
				},
				"node2": {
					NodeIP: "127.0.0.2",
					Label:  map[string]string{"node-role": "xxxx"},
				},
				"node3": {
					NodeIP: "127.0.0.3",
					Label:  map[string]string{"node-role": "data-plane"},
				},
			},
			"node3",
		},
	}
	for _, tt := range tests {
		temp := nodeCache.NodeList
		nodeCache.NodeList = tt.nodeList

		if tt.want != GetRandomNodeFromNodeList() {
			t.Errorf("failed to test TestGetRandomNodeFromNodeList")
		}
		nodeCache.NodeList = temp
	}
}
