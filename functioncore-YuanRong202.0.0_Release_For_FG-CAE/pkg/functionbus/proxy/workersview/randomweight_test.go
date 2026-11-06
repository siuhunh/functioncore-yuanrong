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
	"sync"
	"testing"

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
		rwm.addWorker(w.FuncKey, w.Pod, w.Node)
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
		rwm.addWorker(w.FuncKey, w.Pod, w.Node)
	}
	objB, _ = getRandomWeightMap().randomWeights.Load(functionKeyB)
	if objB.(*RandomWeight).nodes[nodeB] != 2 {
		t.Error("add weight to randomweight error")
	}
	for _, w := range workers {
		rwm.delRemoteWorker(w.FuncKey, w.Pod, w.Node)
	}
}

func TestRandomWeightMap_delFunctionInstance(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	functionKeyA := "testA"
	nodeA := "nodeA"
	functionKeyB := "testB"
	nodeB := "nodeB"

	rwm.addWorker(functionKeyA, "pod1", nodeA)
	rwm.delRemoteWorker(functionKeyA, "pod1", nodeA)
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
		rwm.addWorker(w.FuncKey, w.Pod, w.Node)
	}

	rwm.delRemoteWorker(workers[0].FuncKey, workers[0].Pod, workers[0].Node)

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
		rwm.delRemoteWorker(w.FuncKey, w.Pod, w.Node)
	}

}

func TestRandomWeightMap_chooseRemoteNodes(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	functionKeyA := "testA"
	nodeA := "nodeA"
	rwm.addWorker(functionKeyA, "pod1", nodeA)
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

	rwm.delRemoteWorker(functionKeyA, "pod1", nodeA)
}

func TestRandomWeightMap_updateOwnerIP(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	functionKey := "functionA"
	oldNode := "nodeA"
	newNode := "nodeB"
	pod := "pod1"

	rwm.addWorker(functionKey, pod, oldNode)
	objA, _ := getRandomWeightMap().randomWeights.Load(functionKey)
	if objA.(*RandomWeight).nodes[oldNode] != 1 {
		t.Error("values in randomweight error")
	}
	if _, ok := objA.(*RandomWeight).nodes[newNode]; ok {
		t.Error("values in randomweight error")
	}

	rwm.addWorker(functionKey, pod, newNode)
	objB, _ := getRandomWeightMap().randomWeights.Load(functionKey)
	if objB.(*RandomWeight).nodes[newNode] != 1 {
		t.Error("values in randomweight error")
	}
	if _, ok := objB.(*RandomWeight).nodes[oldNode]; ok {
		t.Error("values in randomweight error")
	}

	rwm.addWorker(functionKey, pod, newNode)
	objC, _ := getRandomWeightMap().randomWeights.Load(functionKey)
	if objC.(*RandomWeight).nodes[newNode] != 1 {
		t.Error("values in randomweight error")
	}
	if _, ok := objC.(*RandomWeight).nodes[oldNode]; ok {
		t.Error("values in randomweight error")
	}

	rwm.delRemoteWorker(functionKey, pod, newNode)
}

func TestRandomWeightMap_ChooseRemoteInstanceSkipUsed(t *testing.T) {
	MockLocalNodeIP()
	rwm := getRandomWeightMap()
	funcKey := "testFuncSkip"
	testPod := "testPod1"
	testNode := "testNode"

	GetWorkersView()

	rwm.addWorker(funcKey, testPod, testNode)
	defer rwm.delRemoteWorker(funcKey, testPod, testNode)

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

func TestRandomWeight_nextSkipUsed(t *testing.T) {

	rw := &RandomWeight{
		nodes:               map[string]int64{"node1": 2, "node2": 3},
		mutex:               sync.RWMutex{},
		funcKeyWithResource: "funcA",
		randomPods: RandomPods{
			{"101.0.0.0", "podA"},
			{"101.0.0.1", "podB"},
			{"101.0.0.2", "podC"},
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

	rw.randomPods = rw.randomPods.updateNodeIPByPodName("podD", "101.0.0.3")
	if len(rw.randomPods) != 3 {
		t.Error("failed to update node ip")
	}

	rw.randomPods = rw.randomPods.updateNodeIPByPodName("podD", "101.0.0.3")
	if len(rw.randomPods) != 3 {
		t.Error("failed to update node ip")
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
