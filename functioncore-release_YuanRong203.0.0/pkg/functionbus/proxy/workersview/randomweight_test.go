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

package workersview

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"functioncore/pkg/functionbus/servicediscovery"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func Test_getRandomWeightMap(t *testing.T) {
	if getRandomWeightMap() == nil {
		t.Error("getRandomWeightMap error")
	}
}

type _workerIns struct {
	FuncKey string
	Node    string
	Pod     string
}

func TestRandomWeightMap_addFunctionInstance(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	functionKeyA := "testA"
	nodeA := "nodeA"
	functionKeyB := "testB"
	nodeB := "nodeB"

	workers := []_workerIns{
		{FuncKey: functionKeyA, Node: nodeA, Pod: "pod1"},
		{FuncKey: functionKeyB, Node: nodeA, Pod: "pod2"},
		{FuncKey: functionKeyB, Node: nodeB, Pod: "pod3"},
	}
	for _, w := range workers {
		rwm.addWorker(w.FuncKey, w.Pod, &servicediscovery.Provider{OwnerIP: w.Node})
	}

	objA, _ := getRandomWeightMap().randomWeights.Load(functionKeyA)
	if len(objA.(*RandomWeight).nodes) != 1 || objA.(*RandomWeight).nodes[nodeA] != 1 {
		t.Error("add weight to randomweight error")
	}
	objB, _ := getRandomWeightMap().randomWeights.Load(functionKeyB)
	if len(objB.(*RandomWeight).nodes) != 2 || objB.(*RandomWeight).nodes[nodeA] != 1 || objB.(*RandomWeight).nodes[nodeB] != 1 {
		t.Error("add weight to randomweight error")
	}
	workers = append(workers, _workerIns{FuncKey: functionKeyB, Node: nodeB, Pod: "pod4"})
	for _, w := range workers {
		rwm.addWorker(w.FuncKey, w.Pod, &servicediscovery.Provider{OwnerIP: w.Node})
	}
	objB, _ = getRandomWeightMap().randomWeights.Load(functionKeyB)
	if objB.(*RandomWeight).nodes[nodeB] != 2 {
		t.Error("add weight to randomweight error")
	}
	for _, w := range workers {
		rwm.delWorker(w.FuncKey, w.Pod, w.Node)
	}
}

func TestRandomWeightMap_delFunctionInstance(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	functionKeyA := "testA"
	nodeA := "nodeA"
	functionKeyB := "testB"
	nodeB := "nodeB"

	rwm.addWorker(functionKeyA, "pod1", &servicediscovery.Provider{OwnerIP: nodeA})
	rwm.delWorker(functionKeyA, "pod1", nodeA)
	_, ok := getRandomWeightMap().randomWeights.Load(functionKeyA)
	if ok {
		t.Error("values in randomWeights error")
	}

	workers := []_workerIns{
		{FuncKey: functionKeyA, Node: nodeA, Pod: "pod1"},
		{FuncKey: functionKeyA, Node: nodeB, Pod: "pod2"},
		{FuncKey: functionKeyB, Node: nodeA, Pod: "pod3"},
		{FuncKey: functionKeyB, Node: nodeB, Pod: "pod4"},
	}

	for _, w := range workers {
		rwm.addWorker(w.FuncKey, w.Pod, &servicediscovery.Provider{OwnerIP: w.Node})
	}

	rwm.delWorker(workers[0].FuncKey, workers[0].Pod, workers[0].Node)

	objA, _ := getRandomWeightMap().randomWeights.Load(functionKeyA)
	if _, ok := objA.(*RandomWeight).nodes[nodeA]; ok {
		t.Error("values in randomweight error")
	}
	if objA.(*RandomWeight).nodes[nodeB] != 1 {
		t.Error("values in randomweight error")
	}

	objB, _ := getRandomWeightMap().randomWeights.Load(functionKeyB)
	if objB.(*RandomWeight).nodes[nodeA] != 1 {
		t.Error("values in randomweight error")
	}
	if objB.(*RandomWeight).nodes[nodeB] != 1 {
		t.Error("values in randomweight error")
	}

	for _, w := range workers {
		rwm.delWorker(w.FuncKey, w.Pod, w.Node)
	}

}

func TestRandomWeightMap_chooseRemoteNodes(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	functionKeyA := "testA"
	nodeA := "nodeA"
	rwm.addWorker(functionKeyA, "pod1", &servicediscovery.Provider{OwnerIP: nodeA})
	GetWorkersView().EnableNode(nodeA)

	nodeif := ChooseRemoteNode(functionKeyA, nil)
	if nodeif == nil || nodeif.(string) != nodeA {
		t.Error("chooseRemoteNode error")
	}

	nodeif = ChooseRemoteNode(functionKeyA, map[string]struct{}{"nodeA": {}})
	if nodeif != nil {
		t.Error("chooseRemoteNode error, should get empty node")
	}

	GetWorkersView().DisableNode(nodeA)
	nodeif = ChooseRemoteNode(functionKeyA, nil)
	if nodeif != nil {
		t.Error("chooseRemoteNode error, get worker on disabled node")
	}

	nodeif = ChooseRemoteNode("", nil)
	if nodeif != nil {
		t.Error("chooseRemoteNode error, get empty node")
	}

	rwm.delWorker(functionKeyA, "pod1", nodeA)
}

func TestRandomWeightMap_updateOwnerIP(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	functionKey := "functionA"
	oldNode := "nodeA"
	newNode := "nodeB"
	pod := "pod1"

	rwm.addWorker(functionKey, pod, &servicediscovery.Provider{OwnerIP: oldNode})
	objA, _ := getRandomWeightMap().randomWeights.Load(functionKey)
	if objA.(*RandomWeight).nodes[oldNode] != 1 {
		t.Error("values in randomweight error")
	}
	if _, ok := objA.(*RandomWeight).nodes[newNode]; ok {
		t.Error("values in randomweight error")
	}

	rwm.addWorker(functionKey, pod, &servicediscovery.Provider{OwnerIP: newNode})
	objB, _ := getRandomWeightMap().randomWeights.Load(functionKey)
	if objB.(*RandomWeight).nodes[newNode] != 1 {
		t.Error("values in randomweight error")
	}
	if _, ok := objB.(*RandomWeight).nodes[oldNode]; ok {
		t.Error("values in randomweight error")
	}

	rwm.addWorker(functionKey, pod, &servicediscovery.Provider{OwnerIP: newNode})
	objC, _ := getRandomWeightMap().randomWeights.Load(functionKey)
	if objC.(*RandomWeight).nodes[newNode] != 1 {
		t.Error("values in randomweight error")
	}
	if _, ok := objC.(*RandomWeight).nodes[oldNode]; ok {
		t.Error("values in randomweight error")
	}

	rwm.delWorker(functionKey, pod, newNode)
}

func TestRandomWeightMap_ChooseRemoteInstanceSkipUsed(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	funcKey := "testFuncSkip"
	testPod := "testPod1"
	testNode := "testNode"

	GetWorkersView()

	rwm.addWorker(funcKey, testPod, &servicediscovery.Provider{OwnerIP: testNode})
	defer rwm.delWorker(funcKey, testPod, testNode)

	pod, node := ChooseRemoteInstanceSkipUsed(funcKey, nil)
	assert.Empty(t, pod)
	assert.Empty(t, node)

	GetWorkersView().EnableNode(testNode)
	defer GetWorkersView().DisableNode(testNode)

	pod, node = ChooseRemoteInstanceSkipUsed(funcKey, nil)
	assert.Equal(t, testPod, pod)
	assert.Equal(t, testNode, node)

	pod, node = ChooseRemoteInstanceSkipUsed("", nil)
	assert.Empty(t, pod)
	assert.Empty(t, node)

	pod, node = ChooseRemoteInstanceSkipUsed(funcKey, map[string]struct{}{testPod: {}})
	assert.Empty(t, pod)
	assert.Empty(t, node)
}

func TestChooseSpecificRemoteInstance(t *testing.T) {
	convey.Convey("TestChooseSpecificRemoteInstance load randomWeight failed", t, func() {
		workerID, nodeIP := ChooseSpecificRemoteInstance("function1", "instanceID1")
		convey.So(workerID, convey.ShouldEqual, "")
		convey.So(nodeIP, convey.ShouldEqual, "")
	})
	convey.Convey("TestChooseSpecificRemoteInstance assert RandomWeight failed", t, func() {
		getRandomWeightMap().randomWeights.Store("function1", 123)
		workerID, nodeIP := ChooseSpecificRemoteInstance("function1", "instanceID1")
		convey.So(workerID, convey.ShouldEqual, "")
		convey.So(nodeIP, convey.ShouldEqual, "")
	})
	convey.Convey("TestChooseSpecificRemoteInstance can't find worker", t, func() {
		getRandomWeightMap().randomWeights.Store("function1", &RandomWeight{
			randomPods: []RandomPod{{nodeIP: "172.28.0.80", podName: "instanceIDx"}},
		})
		workerID, nodeIP := ChooseSpecificRemoteInstance("function1", "instanceID1")
		convey.So(workerID, convey.ShouldEqual, "")
		convey.So(nodeIP, convey.ShouldEqual, "")
	})
	convey.Convey("TestChooseSpecificRemoteInstance node ip is disable", t, func() {
		workersview = &WorkersView{enabledNodes: sync.Map{}}
		getRandomWeightMap().randomWeights.Store("function1", &RandomWeight{
			randomPods: []RandomPod{{nodeIP: "172.28.0.80", podName: "defaultaz-#-instanceID1"}},
		})
		workerID, nodeIP := ChooseSpecificRemoteInstance("function1", "instanceID1")
		convey.So(workerID, convey.ShouldEqual, "")
		convey.So(nodeIP, convey.ShouldEqual, "")
	})
	convey.Convey("TestChooseSpecificRemoteInstance ok", t, func() {
		nodes := sync.Map{}
		nodes.Store("172.28.0.81", nil)
		workersview = &WorkersView{enabledNodes: nodes}
		getRandomWeightMap().randomWeights.Store("function1", &RandomWeight{
			randomPods: []RandomPod{{nodeIP: "172.28.0.81", podName: "defaultaz-#-instanceID1"}},
		})
		workerID, nodeIP := ChooseSpecificRemoteInstance("function1", "instanceID1")
		convey.So(workerID, convey.ShouldEqual, "defaultaz-#-instanceID1")
		convey.So(nodeIP, convey.ShouldEqual, "172.28.0.81")
	})
}

func TestRandomWeight_nextSkipUsed(t *testing.T) {

	rw := &RandomWeight{
		nodes:               map[string]int64{"node1": 2, "node2": 3},
		mutex:               sync.RWMutex{},
		funcKeyWithResource: "funcA",
		randomPods: RandomPods{
			{nodeIP: "101.0.0.0", podName: "podA", willDelete: false},
			{nodeIP: "101.0.0.1", podName: "podB", willDelete: false},
			{nodeIP: "101.0.0.2", podName: "podC", willDelete: false},
		},
		index: -1,
	}
	res := make(map[string]int)
	for i := 0; i < 999; i++ {
		podName, nodeIP := rw.nextSkipUsed(map[string]struct{}{})
		res[podName]++
		res[nodeIP]++
	}

	if res["podA"] != 333 || res["podB"] != 333 || res["podC"] != 333 || res["101.0.0.0"] != 333 || res["101.0.0.1"] != 333 || res["101.0.0.2"] != 333 {
		t.Error("failed to test random weight next skip used")
	}

	res = make(map[string]int)
	for i := 0; i < 1000; i++ {
		podName, nodeIP := rw.nextSkipUsed(map[string]struct{}{"podA": {}})
		res[podName]++
		res[nodeIP]++
	}
	fmt.Println(res)
	if res["podA"] != 0 || res["podB"] != 500 || res["podC"] != 500 || res["101.0.0.0"] != 0 || res["101.0.0.1"] != 500 || res["101.0.0.2"] != 500 {
		t.Error("failed to test random weight next skip used")
	}

	ip, exist := rw.randomPods.getNodeIPByPodName("podA")
	if ip != "101.0.0.0" || !exist {
		t.Error("failed to get node ip by pod name")
	}

	ip, exist = rw.randomPods.getNodeIPByPodName("podD")
	if ip != "" || exist {
		t.Error("failed to get node ip by pod name")
	}

	rw.randomPods = rw.randomPods.deleteRandomPodsByPodName("podA")
	if len(rw.randomPods) != 2 {
		t.Error("failed to test delete random pod")
	}

	rw.randomPods = rw.randomPods.deleteRandomPodsByPodName("podD")
	if len(rw.randomPods) != 2 {
		t.Error("failed to test delete random pod")
	}

	rw.randomPods = rw.randomPods.updatePodInfo("podD", &servicediscovery.Provider{NodeIP: "101.0.0.3", WillDelete: false, Applier: ""})
	if len(rw.randomPods) != 3 {
		t.Error("failed to update node ip")
	}

	rw.randomPods = rw.randomPods.updatePodInfo("podD", &servicediscovery.Provider{NodeIP: "101.0.0.3", WillDelete: false, Applier: ""})
	if len(rw.randomPods) != 3 {
		t.Error("failed to update node ip")
	}

}

func TestGetAllRemoteInstances(t *testing.T) {
	patch := gomonkey.ApplyFunc((*RandomWeightMap).getInstances, func(m *RandomWeightMap,
		funcKeyWithResource string) map[string]string {
		if funcKeyWithResource == "test_func_key" {
			return map[string]string{"worker1": "192.168.1.1", "worker2": "192.168.1.2"}
		}
		return nil
	})
	defer patch.Reset()

	testCases := []struct {
		name           string
		funcKey        string
		expectedResult map[string]string
	}{
		{"Empty FuncKey", "", nil},
		{"Valid FuncKey", "test_func_key", map[string]string{"worker1": "192.168.1.1", "worker2": "192.168.1.2"}},
		{"NonExistent FuncKey", "unknown_func_key", nil},
	}

	// Run test cases.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := GetAllRemoteInstances(tc.funcKey)
			if !reflect.DeepEqual(result, tc.expectedResult) {
				t.Errorf("GetAllRemoteInstances(%q) = %v; want %v", tc.funcKey, result, tc.expectedResult)
			}
		})
	}
}

func TestRandomWeightMap_GetInstances(t *testing.T) {
	rwm := &RandomWeightMap{}
	rw := new(RandomWeight)
	rwm.randomWeights.Store("test_func_key", rw)

	patchGetInstances := gomonkey.ApplyPrivateMethod(reflect.TypeOf(rw), "getInstances",
		func(rw *RandomWeight) map[string]string {
			return map[string]string{"worker1": "192.168.1.1", "worker2": "192.168.1.2"}
		})
	defer patchGetInstances.Reset()

	testCases := []struct {
		name           string
		funcKey        string
		expectedResult map[string]string
	}{
		{"Existing FuncKey", "test_func_key", map[string]string{"worker1": "192.168.1.1", "worker2": "192.168.1.2"}},
		{"NonExistent FuncKey", "unknown_func_key", nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := rwm.getInstances(tc.funcKey)
			if !reflect.DeepEqual(result, tc.expectedResult) {
				t.Errorf("getInstances(%q) = %v; want %v", tc.funcKey, result, tc.expectedResult)
			}
		})
	}
}

func TestRandomWeight_GetInstances(t *testing.T) {
	validRw := &RandomWeight{
		randomPods: []RandomPod{
			{
				podName: "pod1",
				nodeIP:  "192.168.1.1",
			},
			{
				podName: "pod2",
				nodeIP:  "192.168.1.2",
			},
		},
	}

	invalidRw := &RandomWeight{
		randomPods: []RandomPod{
			{
				podName: "pod3",
				nodeIP:  "192.168.1.3",
			},
			{
				podName: "pod4",
				nodeIP:  "192.168.1.4",
			},
		},
	}

	workersview.enabledNodes.Store("192.168.1.1", true)
	workersview.enabledNodes.Store("192.168.1.2", true)

	testCases := []struct {
		name         string
		expected     map[string]string
		randomWeight *RandomWeight
	}{
		{
			"Valid Pods",
			map[string]string{"pod1": "192.168.1.1", "pod2": "192.168.1.2"},
			validRw,
		},
		{
			"Empty Pods",
			make(map[string]string),
			invalidRw,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.randomWeight.getInstances()
			if !reflect.DeepEqual(result, tc.expected) {
				t.Errorf("getInstances() = %v; want %v", result, tc.expected)
			}
		})
	}
}

func TestRandomWeightNext(t *testing.T) {
	rw := &RandomWeight{
		nodes: make(map[string]int64),
	}
	rw.nodes["ip002"] = -1
	used := make(map[string]struct{})
	used["ip001"] = struct{}{}
	res := rw.next(used)
	assert.Nil(t, res)
}

func TestRandomWeightMap_checkOnlyOneInstanceLeft(t *testing.T) {
	type fields struct {
		randomWeights sync.Map
	}
	type args struct {
		funcKeyWithResource string
		podName             string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		val    *RandomWeight
		want   bool
	}{
		{
			"test checkOnlyOneInstanceLeft ok",
			fields{
				randomWeights: sync.Map{},
			},
			args{
				"test",
				"podA",
			},
			&RandomWeight{
				funcKeyWithResource: "test",
				randomPods:          []RandomPod{{podName: "podA", Applier: "funtiontask-aaa"}},
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rwm := &RandomWeightMap{
				randomWeights: tt.fields.randomWeights,
			}
			rwm.randomWeights.Store(tt.val.funcKeyWithResource, tt.val)
			assert.Equalf(t, tt.want, rwm.checkOnlyOneInstanceLeft(tt.args.funcKeyWithResource, tt.args.podName), "checkOnlyOneInstanceLeft(%v, %v)", tt.args.funcKeyWithResource, tt.args.podName)
		})
	}
}

func TestRandomPods_checkOnlyOneInstanceLeft(t *testing.T) {
	type args struct {
		podName string
	}

	clusterID = "cluster001"

	tests := []struct {
		name       string
		randomPods RandomPods
		args       args
		want       bool
	}{
		{
			"test only one this cluster instance",
			[]RandomPod{
				{
					podName: "podA",
					Cluster: "cluster001",
					Applier: "functiontask1",
				},
			},
			args{
				"podA",
			},
			true,
		},
		{
			"test with other cluster instance",
			[]RandomPod{
				{
					podName: "podA",
					Cluster: "cluster001",
					Applier: "functiontask1",
				},
				{
					podName: "podA",
					Cluster: "cluster002",
					Applier: "functiontask1",
				},
			},
			args{
				"podA",
			},
			true,
		},
		{
			"test with other cluster reversed instance",
			[]RandomPod{
				{
					podName: "podA",
					Cluster: "cluster001",
					Applier: "functiontask1",
				},
				{
					podName: "podA",
					Cluster: "cluster002",
					Applier: "worker-manager",
				},
			},
			args{
				"podA",
			},
			true,
		},
		{
			"test with this cluster reversed instance",
			[]RandomPod{
				{
					podName: "podA",
					Cluster: "cluster001",
					Applier: "functiontask1",
				},
				{
					podName: "podA",
					Cluster: "cluster001",
					Applier: "worker-manager",
				},
			},
			args{
				"podA",
			},
			false,
		},
		{
			"test podName not right instance",
			[]RandomPod{
				{
					podName: "podB",
					Cluster: "cluster001",
					Applier: "functiontask1",
				},
			},
			args{
				"podA",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.randomPods.checkOnlyOneInstanceLeft(tt.args.podName), "checkOnlyOneInstanceLeft(%v)", tt.args.podName)
		})
	}
}
