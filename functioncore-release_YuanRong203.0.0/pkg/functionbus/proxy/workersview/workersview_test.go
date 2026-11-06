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
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/servicediscovery"
	testutils "functioncore/test/utils"
)

var (
	patches testutils.PatchSlice
)

func workersviewSetup() {
	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(workerbroker.GetRequestTimeout, func(funcKeyWithResource string) time.Duration {
			return time.Duration(5) * time.Second
		}),
	})
}

func workersviewTearDown() {
	patches.ResetAll()
}

func TestGetWorkersView(t *testing.T) {
	if GetWorkersView() == nil {
		t.Error("get workersview instance failed")
	}
}

func MockLocalNodeIP() {
	localNodeIP = "127.0.0.1"
}

func TestWorkersView_AddWorker(t *testing.T) {
	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&workerbroker.Manager{}), "DeleteRemoteBroker", func(_ *workerbroker.Manager, _, _ string) {}),
	})
	defer patches.ResetAll()

	MockLocalNodeIP()
	functionKeyA := "testA"
	instanceA1 := "instanceA1"
	instanceA2 := "instanceA2"
	functionKeyB := "testB"
	instanceB1 := "instanceB1"
	instanceB2 := "instanceB2"
	provider1 := &servicediscovery.Provider{
		IP:      "10.0.1.1",
		NodeIP:  localNodeIP,
		OwnerIP: localNodeIP,
	}
	provider2 := &servicediscovery.Provider{
		IP:      "10.0.1.2",
		NodeIP:  "10.0.0.1",
		OwnerIP: "",
	}
	provider3 := &servicediscovery.Provider{
		IP:      "10.0.2.1",
		NodeIP:  "10.0.0.1",
		OwnerIP: "10.0.0.1",
	}
	provider4 := &servicediscovery.Provider{
		IP:      "10.0.2.2",
		NodeIP:  "10.0.0.2",
		OwnerIP: "10.0.0.2",
	}

	GetWorkersView().AddWorker(functionKeyA, instanceA1, provider1, false) // local pod
	GetWorkersView().AddWorker(functionKeyA, instanceA2, provider2, false) // remote worker-manager pod
	GetWorkersView().AddWorker(functionKeyB, instanceB1, provider3, false) // remote proxy pod 10.0.0.1
	GetWorkersView().AddWorker(functionKeyB, instanceB2, provider4, false) // remote proxy pod 10.0.0.2

	if _, ok := getRandomWeightMap().randomWeights.Load(functionKeyA); !ok {
		t.Errorf("should be functionKey:%s in workersView", functionKeyA)
	}

	obj, ok := getRandomWeightMap().randomWeights.Load(functionKeyB)
	if !ok {
		t.Errorf("should has functionKey:%s in workersView", functionKeyB)
		return
	}

	if len(obj.(*RandomWeight).nodes) != 2 {
		t.Errorf("size of function:%s instance error", functionKeyB)
	}

	if obj.(*RandomWeight).nodes["10.0.0.1"] != 1 {
		t.Errorf("size of node:%s instance error, functionKey:%s", "10.0.0.1", functionKeyB)
		return
	}
	if obj.(*RandomWeight).nodes["10.0.0.2"] != 1 {
		t.Errorf("size of node:%s instance error, functionKey:%s", "10.0.0.1", functionKeyB)
		return
	}

	GetWorkersView().DeleteWorker(functionKeyB, instanceB1, provider3, false)
	GetWorkersView().DeleteWorker(functionKeyB, instanceB2, provider4, false)
	GetWorkersView().DeleteWorker("", instanceB2, provider4, false)
}

func TestWorkersView_Delete(t *testing.T) {
	workerevaluator.CreateInstance(make(<-chan struct{}))
	patches = testutils.InitPatchSlice()
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&workerbroker.Manager{}), "DeleteRemoteBroker", func(_ *workerbroker.Manager, _, _ string) {}),
	})
	defer patches.ResetAll()

	MockLocalNodeIP()
	functionKeyA := "testA"
	instanceA1 := "instanceA1"
	functionKeyB := "testB"
	instanceB1 := "instanceB1"
	instanceB2 := "instanceB2"
	provider1 := &servicediscovery.Provider{
		IP:      "10.0.1.1",
		NodeIP:  "10.0.0.1",
		OwnerIP: "10.0.0.1",
	}
	provider2 := &servicediscovery.Provider{
		IP:      "10.0.1.1",
		NodeIP:  "10.0.0.1",
		OwnerIP: "",
	}
	provider3 := &servicediscovery.Provider{
		IP:      "10.0.2.1",
		NodeIP:  "10.0.0.1",
		OwnerIP: "10.0.0.1",
	}
	provider4 := &servicediscovery.Provider{
		IP:      "10.0.2.2",
		NodeIP:  "10.0.0.1",
		OwnerIP: "10.0.0.1",
	}
	wv := GetWorkersView()

	// add delete add(wm)
	wv.AddWorker(functionKeyA, instanceA1, provider1, false)    // add remote proxy 10.0.0.1
	wv.DeleteWorker(functionKeyA, instanceA1, provider1, false) // delete remote proxy 10.0.01
	wv.AddWorker(functionKeyA, instanceA1, provider2, false)    // add remote wm 10.0.0.1
	if _, ok := getRandomWeightMap().randomWeights.Load(functionKeyA); ok {
		t.Error("delete function:testA instance:instanceA1 failed")
	}

	// add delete
	wv.AddWorker(functionKeyB, instanceB1, provider3, false)
	wv.DeleteWorker(functionKeyB, instanceB1, provider3, false)
	if _, ok := getRandomWeightMap().randomWeights.Load(functionKeyB); ok {
		t.Error("delete function:testB instance:instanceB1 failed")
	}

	// add add delete
	wv.AddWorker(functionKeyB, instanceB1, provider3, false)
	wv.AddWorker(functionKeyB, instanceB2, provider4, false)
	wv.DeleteWorker(functionKeyB, instanceB1, provider3, false)
	obj, ok := getRandomWeightMap().randomWeights.Load(functionKeyB)
	if !ok {
		t.Error("delete function:testB instance:instanceB1 failed")
	}
	if _, ok := obj.(*RandomWeight).nodes["10.0.0.1"]; !ok {
		t.Error("no value in function:functionKeyB node:10.0.0.1")
	}
	if obj.(*RandomWeight).nodes["10.0.0.1"] != 1 {
		t.Error("value in function:functionKeyB node:10.0.0.1 error")
	}

	provider5 := &servicediscovery.Provider{
		IP:      "10.0.9.9",
		NodeIP:  "127.0.0.1",
		OwnerIP: "127.0.0.1",
	}

	functionKey5 := "func5"
	instance5 := "instance5"
	wv.AddWorker(functionKey5, instance5, provider5, false)
	func5RandomWeight, ok := getRandomWeightMap().randomWeights.Load(functionKey5)
	if !ok {
		t.Errorf("failed to test add local worker")
	}
	if _, ok := func5RandomWeight.(*RandomWeight).randomPods.getNodeIPByPodName(instance5); !ok {
		t.Error("no value in function:functionKey5 node local")
	}
	wv.DeleteWorker(functionKey5, instance5, provider5, false)
	_, ok = getRandomWeightMap().randomWeights.Load(functionKey5)
	if ok {
		t.Errorf("failed to test delete local worker")
	}
}

func TestLBFactory_Next(t *testing.T) {
	workersviewSetup()
	defer workersviewTearDown()
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&servicediscovery.Registry{}), "GetFunctionMetadata", func(*servicediscovery.Registry, string) (*servicediscovery.FunctionMetaInfo, error) {
		return nil, errors.New("func meta not found")
	})
	defer func() {
		time.Sleep(100 * time.Millisecond)
		patch.Reset()
	}()
	// 概率
	MockLocalNodeIP()
	functionKey := "testA"
	nodeA := "10.0.0.1"
	instanceA1 := "instanceA1"
	providerA1 := &servicediscovery.Provider{
		IP:      "10.0.1.1",
		NodeIP:  "10.0.0.1",
		OwnerIP: "10.0.0.1",
	}
	nodeB := "10.0.0.2"
	instanceB1 := "instanceB1"
	providerB1 := &servicediscovery.Provider{
		IP:      "10.0.2.1",
		NodeIP:  nodeB,
		OwnerIP: nodeB,
	}
	instanceB2 := "instanceB2"
	providerB2 := &servicediscovery.Provider{
		IP:      "10.0.2.2",
		NodeIP:  nodeB,
		OwnerIP: nodeB,
	}
	nodeC := "10.0.0.3"
	instanceC1 := "instanceC1"
	providerC1 := &servicediscovery.Provider{
		IP:      "10.0.3.1",
		NodeIP:  nodeC,
		OwnerIP: nodeC,
	}
	instanceC2 := "instanceC2"
	providerC2 := &servicediscovery.Provider{
		IP:      "10.0.3.2",
		NodeIP:  nodeC,
		OwnerIP: nodeC,
	}
	instanceC3 := "instanceC3"
	providerC3 := &servicediscovery.Provider{
		IP:      "10.0.3.3",
		NodeIP:  nodeC,
		OwnerIP: nodeC,
	}
	nodeD := "10.0.0.4"
	instanceD1 := "instanceD1"
	providerD1 := &servicediscovery.Provider{
		IP:      "10.0.4.1",
		NodeIP:  nodeD,
		OwnerIP: nodeD,
	}
	instanceD2 := "instanceD2"
	providerD2 := &servicediscovery.Provider{
		IP:      "10.0.4.2",
		NodeIP:  nodeD,
		OwnerIP: nodeD,
	}
	instanceD3 := "instanceD3"
	providerD3 := &servicediscovery.Provider{
		IP:      "10.0.4.3",
		NodeIP:  nodeD,
		OwnerIP: nodeD,
	}
	instanceD4 := "instanceD4"
	providerD4 := &servicediscovery.Provider{
		IP:      "10.0.4.4",
		NodeIP:  nodeD,
		OwnerIP: nodeD,
	}
	wv := GetWorkersView()
	wv.EnableNode(nodeA)
	wv.EnableNode(nodeB)
	wv.EnableNode(nodeC)
	wv.EnableNode(nodeD)

	wv.AddWorker(functionKey, instanceA1, providerA1, false)

	wv.AddWorker(functionKey, instanceB1, providerB1, false)
	wv.AddWorker(functionKey, instanceB2, providerB2, false)

	wv.AddWorker(functionKey, instanceC1, providerC1, false)
	wv.AddWorker(functionKey, instanceC2, providerC2, false)
	wv.AddWorker(functionKey, instanceC3, providerC3, false)

	wv.AddWorker(functionKey, instanceD1, providerD1, false)
	wv.AddWorker(functionKey, instanceD2, providerD2, false)
	wv.AddWorker(functionKey, instanceD3, providerD3, false)
	wv.AddWorker(functionKey, instanceD4, providerD4, false)

	result := make(map[string]int, 4)

	for i := 0; i < 10000; i++ {
		node := ChooseRemoteNode(functionKey, nil).(string)
		if _, ok := result[node]; !ok {
			result[node] = 0
		}
		result[node]++
	}
	if len(result) != 4 {
		t.Error("next function failed")
	}
	if _, ok := result[nodeA]; !ok {
		t.Error("value error")
	}
	if _, ok := result[nodeB]; !ok {
		t.Error("value error")
	}
	if _, ok := result[nodeC]; !ok {
		t.Error("value error")
	}
	if _, ok := result[nodeD]; !ok {
		t.Error("value error")
	}
	resultA := result[nodeA]
	resultB := result[nodeB]
	resultC := result[nodeC]
	resultD := result[nodeD]

	if resultA < 800 || resultA > 1200 {
		t.Errorf("resultA error, resultA is %d", resultA)
	}
	if resultB < 1600 || resultB > 2400 {
		t.Errorf("resultB error, resultB is %d", resultB)
	}
	if resultC < 2400 || resultC > 3600 {
		t.Errorf("resultC error, resultC is %d", resultC)
	}
	if resultD < 3200 || resultD > 8480 {
		t.Errorf("resultD error, resultD is %d", resultD)
	}
}

func TestLBFactory_Next2(t *testing.T) {
	// 概率
	MockLocalNodeIP()

	for i := 0; i < 10000; i++ {
		node := ChooseRemoteNode("functionKey", nil)
		if node != nil {
			t.Error("next value error")
		}
	}
}

func TestStart(t *testing.T) {
	p := gomonkey.NewPatches()
	c := make(chan time.Time)
	p.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
		return &time.Ticker{C: c}
	})
	var evaluator *workerevaluator.WorkerEvaluator
	p.ApplyMethod(reflect.TypeOf(evaluator), "IsExist", func(_ *workerevaluator.WorkerEvaluator,
		workerID string) bool {
		return workerID == "instanceA" || workerID == "instanceB"
	})
	p.ApplyFunc(action.ScaleDown, func(scaleDownParam action.ScaleDownParam, functionKey string,
		isBrokenConnection bool) (bool, uint32, string) {
		return false, 0, ""
	})
	defer p.Reset()

	wv := &WorkersView{}
	internal.SaveOwnedInstances("instanceA", "funcKeyA", false)
	internal.SaveOwnedInstances("instanceB", "funcKeyB", false)
	internal.SaveOwnedInstances("instanceC", "funcKeyC", false)
	wv.Start(nil)

	var wg sync.WaitGroup
	wg.Add(1)
	stopCh := make(chan struct{})
	go func() {
		defer wg.Done()
		wv.Start(stopCh)
	}()
	c <- time.Now()
	wv.toDeleteInstances = append(wv.toDeleteInstances, toDeleteInstance{
		id: "instance0",
		instanceStatus: internal.InstanceStatus{
			FuncKeyWithResource: "funcKey0",
			IsBrokenConnection:  true,
		},
	})
	c <- time.Now()
	close(stopCh)
	wg.Wait()

	time.Sleep(50 * time.Millisecond)
}
