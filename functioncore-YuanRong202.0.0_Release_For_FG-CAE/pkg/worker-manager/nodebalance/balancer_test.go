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

package nodebalance

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/clientset/versioned"
	"k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1/fake"

	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

func Test_hotLoadNodeBalance(t *testing.T) {
	defaultBalancer = &Balancer{ticker: time.NewTicker(time.Hour)}
	oldConfig := &config.FaasConfig{}
	newConfig := &config.HotLoadConfig{NodeBalance: config.NodeBalance{
		Disabled: 1,
	}}
	assert.NotEqual(t, newConfig.NodeBalance.Disabled, oldConfig.NodeBalance.Disabled)

	hotLoadNodeBalance(oldConfig, newConfig)

	assert.Equal(t, int64(defaultCPUThreshold), oldConfig.NodeBalance.CPUPercentThreshold)
	assert.Equal(t, int64(defaultCPUThreshold), defaultBalancer.cpuThreshold)

	assert.Equal(t, int64(defaultPressureCountThreshold), oldConfig.NodeBalance.NodePressureThreshold)
	assert.Equal(t, int64(defaultPressureCountThreshold), defaultBalancer.pressureCountThreshold)

	assert.Equal(t, int64(defaultRefreshTime), oldConfig.NodeBalance.RefreshInterval)

	assert.Equal(t, newConfig.NodeBalance.Disabled, oldConfig.NodeBalance.Disabled)
	assert.Equal(t, newConfig.NodeBalance.Disabled, defaultBalancer.disabled)
}

func Test_sortByPodType(t *testing.T) {
	migrateNode := &nodeState{
		name:    "migratedNode",
		allCPU:  16000,
		usedCPU: 8500,
		pods: []podState{
			{
				name:    "PodTypeDefault",
				usedCPU: 300,
				podType: utils.PodTypeDefault,
			},
			{
				name:    "podCAEReserve",
				usedCPU: 2000,
				podType: utils.PodTypeCAEReserve,
			},
			{
				name:    "PodTypeCAE",
				usedCPU: 300,
				podType: utils.PodTypeCAE,
			},
			{
				name:    "PodTypeFaaSReserve",
				usedCPU: 200,
				podType: utils.PodTypeFaaSReserve,
			},
			{
				name:    "PodTypeFaaS",
				usedCPU: 100,
				podType: utils.PodTypeFaaS,
			},
		},
		nodeType: utils.CAENodeAntiAffinityValue,
	}
	sortByPodType(migrateNode)
	for index, pod := range migrateNode.pods {
		if index == 0 {
			assert.Equal(t, "podCAEReserve", pod.name)
		} else if index == 1 {
			assert.Equal(t, "PodTypeCAE", pod.name)
		} else if index == 2 {
			assert.Equal(t, "PodTypeFaaSReserve", pod.name)
		} else if index == 3 {
			assert.Equal(t, "PodTypeFaaS", pod.name)
		} else {
			assert.Equal(t, "PodTypeDefault", pod.name)
		}
	}
	migrateNode.nodeType = utils.CAENodeAffinityValue
	sortByPodType(migrateNode)
	for index, pod := range migrateNode.pods {
		if index == 0 {
			assert.Equal(t, "PodTypeFaaS", pod.name)
		} else if index == 1 {
			assert.Equal(t, "PodTypeFaaSReserve", pod.name)
		} else if index == 2 {
			assert.Equal(t, "PodTypeCAE", pod.name)
		} else if index == 3 {
			assert.Equal(t, "podCAEReserve", pod.name)
		} else {
			assert.Equal(t, "PodTypeDefault", pod.name)
		}
	}
}

func TestBalancer_Run(t *testing.T) {
	var count int64
	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeNodeMetricses{}), "List", func(_ *fake.FakeNodeMetricses, _ context.Context, _ v1.ListOptions) (*v1beta1.NodeMetricsList, error) {
			atomic.AddInt64(&count, 1)
			return nil, errors.New("no metrics for nodes")
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	stopCh := make(chan struct{})
	balancer := NewBalancer(Option{
		MetricsClient:          mockutils.K8sMetricsFakeClient,
		PodLister:              mockutils.InformerFakeFactory.Core().V1().Pods().Lister(),
		StopCh:                 stopCh,
		CPUThreshold:           defaultCPUThreshold,
		PressureCountThreshold: defaultPressureCountThreshold,
		RefreshTime:            1,
	})
	balancer.ticker.Reset(100 * time.Millisecond)
	go balancer.Run()

	time.Sleep(200 * time.Millisecond)
	// assert balancer is running
	assert.True(t, atomic.LoadInt64(&count) > 0)

	balancer.UpdateDisableStatus(1)

	time.Sleep(1 * time.Second)
	// assert balancer is disabled
	assert.True(t, atomic.LoadInt64(&count) < 5)

	close(stopCh)

	time.Sleep(100 * time.Millisecond)
}

func Test_GetNodesState(t *testing.T) {
	mutex := sync.RWMutex{}
	nodes := map[string]*nodeState{}
	state := nodeState{}
	nodes["test"] = &state
	balance := &Balancer{
		nodesMutex: &mutex,
		nodes:      nodes,
	}
	tests := []struct {
		name        string
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "case1",
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyGlobalVar(&defaultBalancer, balance),
				})
				return patches
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			GetNodesState("test")
			patches.ResetAll()
		})
	}
}

var fakePodList = map[string]*corev1.Pod{
	"pool22-300-128-1_node1":       {ObjectMeta: v1.ObjectMeta{Labels: map[string]string{"funcName": "funcA"}}, Spec: corev1.PodSpec{NodeName: "node1"}},
	"pool22-300-128-nolabel_node1": {ObjectMeta: v1.ObjectMeta{Labels: map[string]string{}}, Spec: corev1.PodSpec{NodeName: "node1"}},
	"pool22-300-128-2_node2":       {ObjectMeta: v1.ObjectMeta{Labels: map[string]string{"funcName": "funcA"}}, Spec: corev1.PodSpec{NodeName: "node2"}},
	"pool22-300-128_node9":         {ObjectMeta: v1.ObjectMeta{Labels: map[string]string{"funcName": "funcA"}}, Spec: corev1.PodSpec{NodeName: "node9"}},
	"pool22-300-128-3_node2":       {ObjectMeta: v1.ObjectMeta{Labels: map[string]string{"funcName": "funcB"}}, Spec: corev1.PodSpec{NodeName: "node2"}},
	"pool22-300-128-stateful_node2": {ObjectMeta: v1.ObjectMeta{Labels: map[string]string{"funcName": "stateful", utils.LabelStateID: "stateID"}},
		Spec: corev1.PodSpec{NodeName: "node2"}},
}

type fakePodLister struct{}

func (s *fakePodLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (s *fakePodLister) Pods(namespace string) listercorev1.PodNamespaceLister {
	return fakePodNamespaceLister{}
}

type fakePodNamespaceLister struct{}

func (s fakePodNamespaceLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return nil, nil
}

func (s fakePodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	if name == "no_pod" {
		return nil, errors.New("no such pod")
	}
	if p, ok := fakePodList[name]; ok {
		return p, nil
	}
	node := ""
	if strings.Contains(name, "_") {
		node = strings.Split(name, "_")[1]
	}

	return &corev1.Pod{Spec: corev1.PodSpec{NodeName: node}}, nil
}

func TestBalancer_migrate(t *testing.T) {
	balancer := &Balancer{
		podLister:    &fakePodLister{},
		cpuThreshold: 50,
		migratePodToSpecificNodeFunc: func(pod corev1.Pod, node string) {
			return
		},
		isMatchMigrationWithLabelFunc: func(tarGetNodeInfo *nodeState, podInfo podState) bool {
			return true
		},
	}

	migrateNode := &nodeState{
		name:    "migratedNode",
		allCPU:  16000,
		usedCPU: 8500,
		pods: []podState{
			{
				name:    "no_pod",
				usedCPU: 300,
			},
			{
				name:    "pod1",
				usedCPU: 2000,
			},
			{
				name:    "pod2",
				usedCPU: 300,
			},
			{
				name:    "pod3",
				usedCPU: 200,
			},
			{
				name:    "pod4",
				usedCPU: 100,
			},
			{
				name:    "pod5",
				usedCPU: 50,
			},
		},
	}

	targets := []*nodeState{
		{
			name:          "node2",
			allCPU:        16000,
			usedCPU:       7800,
			usedPods:      10,
			allocatePods:  100,
			requestCPU:    100,
			allMemory:     10000,
			requestMemory: 100,
		},
		{
			name:          "node1",
			allCPU:        16000,
			usedCPU:       7450,
			usedPods:      10,
			allocatePods:  100,
			requestCPU:    100,
			allMemory:     10000,
			requestMemory: 100,
		},
	}

	balancer.migrate(migrateNode, targets, 100)

	assert.Equal(t, int64(6500), migrateNode.usedCPU)
	// after sort, targets[0] is node1, pod2 and pod3 is migrated to this node
	assert.Equal(t, int64(7800), targets[0].usedCPU)
	// pod4 is migrated to node2
	assert.Equal(t, int64(9450), targets[1].usedCPU)
}

func TestBalancer_migrateByNodeLabel(t *testing.T) {
	balancer := &Balancer{
		podLister:    &fakePodLister{},
		cpuThreshold: 50,
		migratePodToSpecificNodeFunc: func(pod corev1.Pod, node string) {
			return
		},
		isMatchMigrationWithLabelFunc: func(tarGetNodeInfo *nodeState, podInfo podState) bool {
			return true
		},
	}

	migrateNode := &nodeState{
		name:    "migratedNode",
		allCPU:  16000,
		usedCPU: 8500,
		pods:    []podState{},

		antiAffinityPods: []podState{
			{
				name:    "pod1",
				usedCPU: 300,
			},
			{
				name:    "pod2",
				usedCPU: 2000,
			},
		},
	}

	targets := []*nodeState{
		{
			name:          "node2",
			allCPU:        16000,
			usedCPU:       7800,
			usedPods:      10,
			allocatePods:  100,
			requestCPU:    100,
			allMemory:     10000,
			requestMemory: 100,
		},
		{
			name:          "node1",
			allCPU:        16000,
			usedCPU:       7450,
			usedPods:      10,
			allocatePods:  100,
			requestCPU:    100,
			allMemory:     10000,
			requestMemory: 100,
		},
	}

	balancer.migrateByNodeLabel(migrateNode, targets, 100)

	assert.Equal(t, int64(6200), migrateNode.usedCPU)
	// after sort, targets[0] is node1, pod2 and pod3 is migrated to this node
	assert.Equal(t, int64(7800), targets[0].usedCPU)
	// pod4 is migrated to node2
	assert.Equal(t, int64(9750), targets[1].usedCPU)
}

func TestBalancer_refreshPodState_listFail(t *testing.T) {
	balancer := &Balancer{
		mc: mockutils.K8sMetricsFakeClient,
	}

	patch := gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePodMetricses{}), "List", func(_ *fake.FakePodMetricses, _ context.Context, _ v1.ListOptions) (*v1beta1.PodMetricsList, error) {
		return nil, errors.New("no metrics for pods")
	})
	defer func() {
		patch.Reset()
	}()

	assert.Error(t, balancer.refreshPodState())
}

func TestBalancer_refreshPodState(t *testing.T) {
	balancer := &Balancer{
		mc:        mockutils.K8sMetricsFakeClient,
		podLister: &fakePodLister{},
		nodes: map[string]*nodeState{
			"node1": {name: "node1"},
			"node2": {name: "node2"},
			"node3": {name: "node3"},
		},
	}

	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePodMetricses{}), "List", func(_ *fake.FakePodMetricses, _ context.Context, _ v1.ListOptions) (*v1beta1.PodMetricsList, error) {
			return &v1beta1.PodMetricsList{Items: []v1beta1.PodMetrics{
				{ObjectMeta: v1.ObjectMeta{DeletionTimestamp: &v1.Time{}, Labels: map[string]string{"funcName": "funcA"}}},
				{ObjectMeta: v1.ObjectMeta{Name: "no_pod", Labels: map[string]string{"funcName": "funcA"}}},
				{ObjectMeta: v1.ObjectMeta{Name: "pool22-300-128-1_node1", Labels: map[string]string{"funcName": "funcA"}}, Containers: []v1beta1.ContainerMetrics{
					{Name: "worker", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("10m")}},
					{Name: "runtime", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("80m")}},
				}},
				{ObjectMeta: v1.ObjectMeta{Name: "pool22-300-128-nolabel_node1"}, Containers: []v1beta1.ContainerMetrics{
					{Name: "worker", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("10m")}},
					{Name: "runtime", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("80m")}},
				}},
				{ObjectMeta: v1.ObjectMeta{Name: "pool22-300-128-2_node2", Labels: map[string]string{"funcName": "funcA"}}, Containers: []v1beta1.ContainerMetrics{
					{Name: "worker", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("50m")}},
					{Name: "runtime", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("400m")}},
				}},
				{ObjectMeta: v1.ObjectMeta{Name: "pool22-300-128_node9", Labels: map[string]string{"funcName": "funcA"}}, Containers: []v1beta1.ContainerMetrics{
					{Name: "worker", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("10m")}},
					{Name: "runtime", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("80m")}},
				}},
				// stateful function will not be calculated
				{ObjectMeta: v1.ObjectMeta{Name: "pool22-300-128-stateful_node2", Labels: map[string]string{"funcName": "stateful", utils.LabelStateID: "stateID"}},
					Containers: []v1beta1.ContainerMetrics{
						{Name: "worker", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("10m")}},
						{Name: "runtime", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("80m")}},
					}},
			}}, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	assert.Nil(t, balancer.refreshPodState())
	assert.Equal(t, int64(100), balancer.nodes["node1"].pods[0].usedCPU)
	assert.Equal(t, int64(500), balancer.nodes["node2"].pods[0].usedCPU)
}

func TestBalancer_calculateMigrate(t *testing.T) {
	balancer := &Balancer{
		podLister:              &fakePodLister{},
		mc:                     mockutils.K8sMetricsFakeClient,
		cpuThreshold:           defaultCPUThreshold,
		cpuLowThreshold:        defaultCPULowThreshold,
		pressureCountThreshold: defaultPressureCountThreshold,
		nodes: map[string]*nodeState{
			"node1": {
				allCPU:           16000,
				usedCPU:          7450,
				usedPods:         10,
				allocatePods:     100,
				requestCPU:       100,
				allMemory:        10000,
				requestMemory:    100,
				pressureCount:    1,
				lowPressureCount: 2,
			},
			"node2": {
				allCPU:           16000,
				usedCPU:          10,
				usedPods:         10,
				allocatePods:     100,
				requestCPU:       100,
				allMemory:        10000,
				requestMemory:    100,
				pressureCount:    5,
				lowPressureCount: 5,
			},
			"node3": {
				allCPU:        16000,
				usedCPU:       50,
				usedPods:      10,
				allocatePods:  100,
				requestCPU:    100,
				allMemory:     10000,
				requestMemory: 100, pressureCount: 6,
				lowPressureCount: 10,
			},
			"node4": {
				allCPU:           16000,
				usedCPU:          7450,
				usedPods:         10,
				allocatePods:     100,
				requestCPU:       100,
				allMemory:        10000,
				requestMemory:    100,
				pressureCount:    7,
				lowPressureCount: 5,
			},
			"node5": {
				allCPU:           16000,
				usedCPU:          1000,
				usedPods:         10,
				allocatePods:     100,
				requestCPU:       100,
				allMemory:        10000,
				requestMemory:    100,
				pressureCount:    7,
				lowPressureCount: 0,
			},
			"node6": {
				allCPU:           16000,
				usedCPU:          7450,
				usedPods:         10,
				allocatePods:     100,
				requestCPU:       100,
				allMemory:        10000,
				requestMemory:    100,
				pressureCount:    0,
				lowPressureCount: 0,
			},
		},
		migratePodToSpecificNodeFunc: func(pod corev1.Pod, node string) {
			return
		},
		isMatchMigrationWithLabelFunc: func(tarGetNodeInfo *nodeState, podInfo podState) bool {
			return true
		},
		nodesMutex: &sync.RWMutex{},
	}

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
			return &cache.NodeInfo{CapacityCPU: 16000, RequestCPU: 100, RequestMemory: 100, UsedPods: 10, AllocatablePods: 100, CapacityMemory: 1000}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeNodeMetricses{}), "List", func(_ *fake.FakeNodeMetricses, _ context.Context, _ v1.ListOptions) (*v1beta1.NodeMetricsList, error) {
			return &v1beta1.NodeMetricsList{Items: []v1beta1.NodeMetrics{
				{ObjectMeta: v1.ObjectMeta{Name: "node1"}, Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("8100m")}},
				{ObjectMeta: v1.ObjectMeta{Name: "node2"}, Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("8400m")}},
				{ObjectMeta: v1.ObjectMeta{Name: "node3"}, Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("8400m")}},
				{ObjectMeta: v1.ObjectMeta{Name: "node4"}, Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("100m")}},
				{ObjectMeta: v1.ObjectMeta{Name: "node7"}, Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("10000m")}},
				{ObjectMeta: v1.ObjectMeta{Name: "node8"}, Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("200m")}},
			}}, nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakePodMetricses{}), "List", func(_ *fake.FakePodMetricses, _ context.Context, _ v1.ListOptions) (*v1beta1.PodMetricsList, error) {
			return &v1beta1.PodMetricsList{Items: []v1beta1.PodMetrics{
				{ObjectMeta: v1.ObjectMeta{Name: "pool22-300-128-1_node1", Labels: map[string]string{"funcName": "funcA"}}, Containers: []v1beta1.ContainerMetrics{
					{Name: "worker", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("10m")}},
					{Name: "runtime", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("80m")}},
				}},
				{ObjectMeta: v1.ObjectMeta{Name: "pool22-300-128-3_node2", Labels: map[string]string{"funcName": "funcB"}}, Containers: []v1beta1.ContainerMetrics{
					{Name: "worker", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("50m")}},
					{Name: "runtime", Usage: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse("400m")}},
				}},
			}}, nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	balancer.calculateMigrate()

	// assert pod2 is migrated from node2 to node4
	assert.Equal(t, int64(7900), balancer.nodes["node2"].usedCPU)
	assert.Equal(t, int64(600), balancer.nodes["node4"].usedCPU)
	assert.Equal(t, int64(0), balancer.nodes["node2"].pressureCount)
}

func TestBalancer_migrateWithScore(t *testing.T) {
	type fields struct {
		ticker                       *time.Ticker
		mc                           versioned.Interface
		podLister                    listercorev1.PodLister
		stopCh                       <-chan struct{}
		cpuThreshold                 int64
		pressureCountThreshold       int64
		nodes                        map[string]*nodeState
		nodesMutex                   *sync.RWMutex
		disabled                     uint32
		migratePodToSpecificNodeFunc func(pod corev1.Pod, node string)
		scoreFunc                    func(node string, profilingContent *monitoring.ProfilingContent, podMemory, podCPU int64) float64
	}
	type args struct {
		migrateNode *nodeState
		targets     []*nodeState
		threshold   int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test migrateWithScore",
			fields{
				scoreFunc: func(node string, profilingContent *monitoring.ProfilingContent, podMemory, podCPU int64) float64 {
					return 0
				},
			},
			args{
				migrateNode: &nodeState{
					pods: []podState{
						{
							name:      "test",
							usedCPU:   1,
							funcName:  "",
							podMemory: 1,
							podCPU:    1,
						},
					},
				},
				targets: []*nodeState{
					{
						pods: []podState{
							{
								name:      "1",
								usedCPU:   1,
								funcName:  "",
								podMemory: 1,
								podCPU:    1,
							},
						},
					},
				},
				threshold: 100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Balancer{
				ticker:                       tt.fields.ticker,
				mc:                           tt.fields.mc,
				podLister:                    tt.fields.podLister,
				stopCh:                       tt.fields.stopCh,
				cpuThreshold:                 tt.fields.cpuThreshold,
				pressureCountThreshold:       tt.fields.pressureCountThreshold,
				nodes:                        tt.fields.nodes,
				nodesMutex:                   tt.fields.nodesMutex,
				disabled:                     tt.fields.disabled,
				migratePodToSpecificNodeFunc: tt.fields.migratePodToSpecificNodeFunc,
				scoreFunc:                    tt.fields.scoreFunc,
				isMatchMigrationWithLabelFunc: func(tarGetNodeInfo *nodeState, podInfo podState) bool {
					return true
				},
			}
			b.migrateWithScore(tt.args.migrateNode, tt.args.targets, 100)
		})
	}
}

func TestBalancer_migrateWithScoreByNodeLabel(t *testing.T) {
	type fields struct {
		ticker                       *time.Ticker
		mc                           versioned.Interface
		podLister                    listercorev1.PodLister
		stopCh                       <-chan struct{}
		cpuThreshold                 int64
		pressureCountThreshold       int64
		nodes                        map[string]*nodeState
		nodesMutex                   *sync.RWMutex
		disabled                     uint32
		migratePodToSpecificNodeFunc func(pod corev1.Pod, node string)
		scoreFunc                    func(node string, profilingContent *monitoring.ProfilingContent, podMemory, podCPU int64) float64
	}
	type args struct {
		migrateNode *nodeState
		targets     []*nodeState
		threshold   int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test migrateWithScore",
			fields{
				scoreFunc: func(node string, profilingContent *monitoring.ProfilingContent, podMemory, podCPU int64) float64 {
					return 0
				},
			},
			args{
				migrateNode: &nodeState{
					antiAffinityPods: []podState{
						{
							name:      "test",
							usedCPU:   1,
							funcName:  "",
							podMemory: 1,
							podCPU:    1,
						},
					},
				},
				targets: []*nodeState{
					{
						pods: []podState{
							{
								name:      "1",
								usedCPU:   1,
								funcName:  "",
								podMemory: 1,
								podCPU:    1,
							},
						},
					},
				},
				threshold: 100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &Balancer{
				ticker:                       tt.fields.ticker,
				mc:                           tt.fields.mc,
				podLister:                    tt.fields.podLister,
				stopCh:                       tt.fields.stopCh,
				cpuThreshold:                 tt.fields.cpuThreshold,
				pressureCountThreshold:       tt.fields.pressureCountThreshold,
				nodes:                        tt.fields.nodes,
				nodesMutex:                   tt.fields.nodesMutex,
				disabled:                     tt.fields.disabled,
				migratePodToSpecificNodeFunc: tt.fields.migratePodToSpecificNodeFunc,
				scoreFunc:                    tt.fields.scoreFunc,
				isMatchMigrationWithLabelFunc: func(tarGetNodeInfo *nodeState, podInfo podState) bool {
					return true
				},
			}
			b.migrateWithScoreByNodeLabel(tt.args.migrateNode, tt.args.targets, 100)
		})
	}
}

func Test_nodeState_canMigrate(t *testing.T) {
	type fields struct {
		name             string
		allCPU           int64
		usedCPU          int64
		allMemory        int64
		requestMemory    int64
		requestCPU       int64
		usedPods         int64
		allocatePods     int64
		pressureCount    int64
		lowPressureCount int64
		pods             []podState
		score            float64
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			"test can migrate",
			fields{
				usedPods:      10,
				allocatePods:  20,
				requestCPU:    10,
				allCPU:        20,
				requestMemory: 10,
				allMemory:     20,
			},
			true,
		},
		{
			"test can't migrate",
			fields{
				usedPods:      86,
				allocatePods:  100,
				requestCPU:    10,
				allCPU:        20,
				requestMemory: 10,
				allMemory:     20,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &nodeState{
				name:             tt.fields.name,
				allCPU:           tt.fields.allCPU,
				usedCPU:          tt.fields.usedCPU,
				allMemory:        tt.fields.allMemory,
				requestMemory:    tt.fields.requestMemory,
				requestCPU:       tt.fields.requestCPU,
				usedPods:         tt.fields.usedPods,
				allocatePods:     tt.fields.allocatePods,
				pressureCount:    tt.fields.pressureCount,
				lowPressureCount: tt.fields.lowPressureCount,
				pods:             tt.fields.pods,
				score:            tt.fields.score,
			}
			assert.Equalf(t, tt.want, n.canMigrate(), "canMigrate()")
		})
	}
}

func TestUpdateNodeState(t *testing.T) {
	type args struct {
		nodeName       string
		usedCPU        int64
		requestsMemory int64
	}
	tests := []struct {
		name    string
		args    args
		balance *Balancer
	}{
		{
			name: "test balance is nil",
			args: args{
				nodeName:       "node1",
				usedCPU:        1,
				requestsMemory: 1,
			},
			balance: nil,
		},
		{
			name: "test balance not nil",
			args: args{
				nodeName:       "node1",
				usedCPU:        1,
				requestsMemory: 1,
			},
			balance: &Balancer{
				nodes: map[string]*nodeState{
					"node1": {
						usedCPU:       1,
						requestMemory: 1,
					},
				},
				nodesMutex: &sync.RWMutex{},
			},
		},
	}
	for _, tt := range tests {
		tmpDefaultBalance := defaultBalancer

		defaultBalancer = tt.balance

		t.Run(tt.name, func(t *testing.T) {
			UpdateNodeState(tt.args.nodeName, tt.args.usedCPU, tt.args.requestsMemory)
		})
		defaultBalancer = tmpDefaultBalance
	}
}

func Test_hasTaints(t *testing.T) {
	tests := []struct {
		name     string
		node     *cache.NodeInfo
		expected bool
	}{
		{
			name:     "node with no taints",
			node:     &cache.NodeInfo{},
			expected: false,
		},
		{
			name:     "node with gpu-node taints",
			node:     &cache.NodeInfo{Taints: []corev1.Taint{{Key: "node-type", Value: "gpu"}}},
			expected: false,
		},
		{
			name:     "node with other taints",
			node:     &cache.NodeInfo{Taints: []corev1.Taint{{Key: "node-role.kubernetes.io/master"}}},
			expected: true,
		},
		{
			name:     "has gpu-node taints and other taints",
			node:     &cache.NodeInfo{Taints: []corev1.Taint{{Key: "node-type", Value: "gpu"}, {Key: "node-role.kubernetes.io/master"}}},
			expected: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, hasTaints(tt.node))
		})
	}
}

func Test_nodeState_tryAddPods(t *testing.T) {
	tests := []struct {
		name     string
		node     nodeState
		pod      podState
		expected bool
	}{
		{
			name:     "normal",
			node:     nodeState{capacityNvidiaGPU: 1, capacityGPUMem128: 192, capacityGPUPercentage: 100, allCPU: 16000, allMemory: 32 * 1024 * 1024 * 1024},
			pod:      podState{podCPU: 300, podMemory: 128 * 1024 * 1024, requestNvidiaGPU: 1},
			expected: true,
		},
		{
			name:     "no available nvidia GPU",
			node:     nodeState{requestNvidiaGPU: 1, capacityNvidiaGPU: 1, capacityGPUMem128: 192, capacityGPUPercentage: 100, allCPU: 16000, allMemory: 32 * 1024 * 1024 * 1024},
			pod:      podState{podCPU: 300, podMemory: 128 * 1024 * 1024, requestNvidiaGPU: 1},
			expected: false,
		},
		{
			name:     "no available GPU percentage",
			node:     nodeState{requestGPUPercentage: 90, capacityNvidiaGPU: 1, capacityGPUMem128: 192, capacityGPUPercentage: 100, allCPU: 16000, allMemory: 32 * 1024 * 1024 * 1024},
			pod:      podState{podCPU: 300, podMemory: 128 * 1024 * 1024, requestGPUPercentage: 15},
			expected: false,
		},
		{
			name:     "both nvidia GPU and GPU percentage requested",
			node:     nodeState{requestGPUPercentage: 10, capacityNvidiaGPU: 1, capacityGPUMem128: 192, capacityGPUPercentage: 100, allCPU: 16000, allMemory: 32 * 1024 * 1024 * 1024},
			pod:      podState{podCPU: 300, podMemory: 128 * 1024 * 1024, requestNvidiaGPU: 1},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.node.tryAddPods(tt.pod))
		})
	}
}

func TestBalancer_migrateByPressureNodes(t *testing.T) {
	balancer := &Balancer{
		podLister: &fakePodLister{},
		migratePodToSpecificNodeFunc: func(pod corev1.Pod, node string) {
			return
		},
		nodesMutex: &sync.RWMutex{},
		isMatchMigrationWithLabelFunc: func(tarGetNodeInfo *nodeState, podInfo podState) bool {
			return true
		},
	}

	tests := []struct {
		name                 string
		highPressure         []*nodeState
		lowPressure          []*nodeState
		expectedHighPressure []*nodeState
		expectedLowPressure  []*nodeState
	}{
		{
			name: "empty",
		},
		{
			name: "GPU case: must match GPU type",
			highPressure: []*nodeState{
				{
					// can't migrate
					name:                  "GPU-node-a30-1",
					gpuType:               "a30",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					requestCPU:            800,
					requestMemory:         600,
					usedCPU:               5000,
					usedPods:              1,
					capacityNvidiaGPU:     1,
					capacityGPUMem128:     192,
					capacityGPUPercentage: 100,
					requestGPUPercentage:  5,
					requestGPUMem128:      8,
					pressureCount:         10,
					pods: []podState{
						{
							requestGPUPercentage: 5,
							requestGPUMem128:     8,
							usedCPU:              800,
							podCPU:               800,
							podMemory:            600,
						},
					},
				},
				{
					// can migrate
					name:                  "GPU-node-a40-1",
					gpuType:               "a40",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					requestCPU:            600,
					requestMemory:         600,
					usedCPU:               4500,
					usedPods:              1,
					capacityGPUMem128:     192,
					capacityGPUPercentage: 100,
					requestGPUPercentage:  5,
					requestGPUMem128:      8,
					pressureCount:         10,
					pods: []podState{
						{
							requestGPUPercentage: 5,
							requestGPUMem128:     8,
							usedCPU:              600,
							podCPU:               600,
							podMemory:            600,
						},
					},
				},
			},
			lowPressure: []*nodeState{
				{
					// no available GPU resource
					name:                  "GPU-node-a30-2",
					gpuType:               "a30",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					capacityNvidiaGPU:     1,
					capacityGPUPercentage: 100,
					capacityGPUMem128:     192,
					requestGPUPercentage:  100,
					requestGPUMem128:      100,
				},
				{
					// node is alloc by nvidia GPU
					name:                  "GPU-node-a30-3",
					gpuType:               "a30",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					capacityNvidiaGPU:     1,
					capacityGPUPercentage: 100,
					capacityGPUMem128:     192,
					requestNvidiaGPU:      1,
				},
				{
					// different GPU card
					name:                  "GPU-node-a40-2",
					gpuType:               "a40",
					allocatePods:          100,
					capacityNvidiaGPU:     1,
					capacityGPUPercentage: 100,
					capacityGPUMem128:     192,
					allCPU:                8000,
					allMemory:             64000,
				},
				{
					// no GPU card
					name:         "normal node",
					allocatePods: 100,
					allCPU:       64000,
					allMemory:    64000,
				},
			},
			expectedHighPressure: []*nodeState{
				{
					name:                  "GPU-node-a30-1",
					gpuType:               "a30",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					requestCPU:            800,
					requestMemory:         600,
					usedCPU:               5000,
					usedPods:              1,
					capacityNvidiaGPU:     1,
					capacityGPUMem128:     192,
					capacityGPUPercentage: 100,
					requestGPUPercentage:  5,
					requestGPUMem128:      8,
					pods: []podState{
						{
							requestGPUPercentage: 5,
							requestGPUMem128:     8,
							usedCPU:              800,
							podCPU:               800,
							podMemory:            600,
						},
					},
				},
				{
					name:                  "GPU-node-a40-1",
					gpuType:               "a40",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					usedCPU:               3900,
					usedPods:              0,
					capacityGPUMem128:     192,
					capacityGPUPercentage: 100,
					pods: []podState{
						{
							requestGPUPercentage: 5,
							requestGPUMem128:     8,
							usedCPU:              600,
							podCPU:               600,
							podMemory:            600,
						},
					},
				},
			},
			expectedLowPressure: []*nodeState{
				{
					name:                  "GPU-node-a30-2",
					gpuType:               "a30",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					capacityNvidiaGPU:     1,
					capacityGPUPercentage: 100,
					capacityGPUMem128:     192,
					requestGPUPercentage:  100,
					requestGPUMem128:      100,
				},
				{
					name:                  "GPU-node-a30-3",
					gpuType:               "a30",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					capacityNvidiaGPU:     1,
					capacityGPUPercentage: 100,
					capacityGPUMem128:     192,
					requestNvidiaGPU:      1,
				},
				{
					// pod migrated from GPU-node-a40-1 to GPU-node-a40-2
					name:                  "GPU-node-a40-2",
					gpuType:               "a40",
					allocatePods:          100,
					allCPU:                8000,
					allMemory:             64000,
					capacityNvidiaGPU:     1,
					capacityGPUPercentage: 100,
					capacityGPUMem128:     192,
					requestCPU:            600,
					usedCPU:               600,
					requestMemory:         600,
					usedPods:              1,
					requestGPUPercentage:  5,
					requestGPUMem128:      8,
				},
				{
					name:         "normal node",
					allocatePods: 100,
					allCPU:       64000,
					allMemory:    64000,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			balancer.migrateByPressureNodes(tt.highPressure, tt.lowPressure, false, 50)
			sort.Slice(tt.lowPressure, func(i, j int) bool {
				return tt.lowPressure[i].name < tt.lowPressure[j].name
			})
			sort.Slice(tt.expectedLowPressure, func(i, j int) bool {
				return tt.expectedLowPressure[i].name < tt.expectedLowPressure[j].name
			})
			sort.Slice(tt.highPressure, func(i, j int) bool {
				return tt.highPressure[i].name < tt.highPressure[j].name
			})
			sort.Slice(tt.expectedHighPressure, func(i, j int) bool {
				return tt.expectedHighPressure[i].name < tt.expectedHighPressure[j].name
			})

			for i := range tt.expectedLowPressure {
				assert.Equal(t, tt.expectedLowPressure[i], tt.lowPressure[i])
			}
			for i := range tt.expectedHighPressure {
				assert.Equal(t, tt.expectedHighPressure[i], tt.highPressure[i])
			}
		})
	}
}

func Test_isMatchMigrationWithLabel(t *testing.T) {
	tests := []struct {
		name        string
		nodeInfo    *nodeState
		podInfo     podState
		want        bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "MatchMigrationWithDefaultLabel 1",
			nodeInfo: &nodeState{
				name: "node1",
			},
			podInfo: podState{
				podScheduleInfo: cache.PodScheduleInfo{
					TaggedScheduleLabel: false,
					AffinityNodes:       []string{"node2"},
					AntiAffinityNodes:   []string{"node3"},
				},
			},
			want: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						return &cache.NodeInfo{
							IsDefaultLabel: false,
						}
					}),
				})
				return patches
			},
		},
		{
			name: "MatchMigrationWithLabel 2",
			nodeInfo: &nodeState{
				name: "node1",
			},
			podInfo: podState{
				podScheduleInfo: cache.PodScheduleInfo{
					TaggedScheduleLabel: true,
					AffinityNodes:       []string{"node2"},
					AntiAffinityNodes:   []string{"node3"},
				},
			},
			want: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						return &cache.NodeInfo{
							IsDefaultLabel: false,
						}
					}),
				})
				return patches
			},
		},
		{
			name: "MatchMigrationWithLabel 3",
			nodeInfo: &nodeState{
				name: "node1",
			},
			podInfo: podState{
				podScheduleInfo: cache.PodScheduleInfo{
					TaggedScheduleLabel: true,
					AffinityNodes:       []string{"node1"},
					AntiAffinityNodes:   []string{"node3"},
				},
			},
			want: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						return &cache.NodeInfo{
							IsDefaultLabel: true,
						}
					}),
				})
				return patches
			},
		},
		{
			name: "MatchMigrationWithLabel 4",
			nodeInfo: &nodeState{
				name: "node3",
			},
			podInfo: podState{
				podScheduleInfo: cache.PodScheduleInfo{
					TaggedScheduleLabel: true,
					AffinityNodes:       []string{"node1"},
					AntiAffinityNodes:   []string{"node3"},
				},
			},
			want: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						return &cache.NodeInfo{
							IsDefaultLabel: true,
						}
					}),
				})
				return patches
			},
		},
		{
			name: "MatchMigrationWithLabel 5",
			nodeInfo: &nodeState{
				name: "node1",
			},
			podInfo: podState{
				podScheduleInfo: cache.PodScheduleInfo{
					TaggedScheduleLabel: true,
					AffinityNodes:       []string{"node2"},
					AntiAffinityNodes:   []string{"node3"},
				},
			},
			want: false,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						return &cache.NodeInfo{
							IsDefaultLabel: true,
						}
					}),
				})
				return patches
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			assert.Equal(t, isMatchMigrationWithLabel(tt.nodeInfo, tt.podInfo), tt.want, tt.name)
			patches.ResetAll()
		})
	}
}

func TestBalancer_scaleDefaultLabelsMigratePod(t *testing.T) {
	balancer := &Balancer{
		podLister: &fakePodLister{},
		migratePodToSpecificNodeFunc: func(pod corev1.Pod, node string) {
			return
		},
		isMatchMigrationWithLabelFunc: isMatchMigrationWithLabel,
		nodesMutex:                    &sync.RWMutex{},
		nodes: map[string]*nodeState{
			"node1": &nodeState{
				name:          "node1",
				requestCPU:    500,
				requestMemory: 500,
				allocatePods:  1000,
				allCPU:        1000,
				allMemory:     1000,
				usedPods:      2,
				pods: []podState{
					{
						name:     "node1-pod1",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: true,
							AffinityNodes:       []string{"node1"},
						},
					},

					{
						name:     "node1-pod1",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
					},
				},
			},

			"node2": &nodeState{
				name:          "node2",
				requestCPU:    500,
				requestMemory: 500,
				allocatePods:  1000,
				allCPU:        1000,
				allMemory:     1000,
				usedPods:      2,
				pods: []podState{
					{
						name:     "node2-pod1",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: true,
							AffinityNodes:       []string{"node1", "node2"},
							AntiAffinityNodes:   []string{"node3"},
						},
					},
					{
						name:     "node2-pod2",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: true,
							AffinityNodes:       []string{"node3"},
							AntiAffinityNodes:   []string{"node2"},
						},
					},
				},
				antiAffinityPods: []podState{
					{
						name:     "node2-pod2",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: true,
							AffinityNodes:       []string{"node1"},
							AntiAffinityNodes:   []string{"node2"},
						},
					},
				},
			},

			"node3": &nodeState{
				name:          "node3",
				requestCPU:    500,
				requestMemory: 500,
				allocatePods:  1000,
				allCPU:        1000,
				allMemory:     1000,
				usedPods:      3,
				pods: []podState{
					{
						name:     "node3-pod1",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   40,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: true,
							AntiAffinityNodes:   []string{"node3"},
						},
					},
					{
						name:     "node3-pod2",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: false,
							AntiAffinityNodes:   []string{"node3"},
						},
					},
					{
						name:     "node3-pod3",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
					},
				},
				antiAffinityPods: []podState{
					{
						name:     "node3-pod1",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   40,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: true,
							AntiAffinityNodes:   []string{"node3"},
						},
					},
					{
						name:     "node3-pod2",
						usedCPU:  10,
						funcName: "test1",
						podCPU:   20,
						podScheduleInfo: cache.PodScheduleInfo{
							TaggedScheduleLabel: false,
							AntiAffinityNodes:   []string{"node3"},
						},
					},
				},
			},
		},
		cpuThreshold:           70,
		cpuLowThreshold:        30,
		pressureCountThreshold: 50,
	}

	convey.Convey("Test scaleDefaultLabelsMigratePod not exist node", t, func() {
		patch := gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
			return &cache.NodeInfo{}
		})

		defer func() {
			patch.Reset()
		}()
		balancer.scaleNoScheduleLabelsMigratePod([]string{"node5"})
		convey.So(int(balancer.nodes["node1"].usedPods), convey.ShouldEqual, 2)
		convey.So(int(balancer.nodes["node2"].usedPods), convey.ShouldEqual, 2)
		convey.So(int(balancer.nodes["node3"].usedPods), convey.ShouldEqual, 3)
	})

	convey.Convey("Test scaleDefaultLabelsMigratePod not need migrate with Anti-AffinityNodes 1", t, func() {
		patch := gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
			if nodeID == "node1" {
				return &cache.NodeInfo{
					IsDefaultLabel: true,
				}
			}
			return &cache.NodeInfo{}
		})
		defer func() {
			patch.Reset()
		}()
		balancer.scaleNoScheduleLabelsMigratePod([]string{"node1"})
		convey.So(int(balancer.nodes["node1"].usedPods), convey.ShouldEqual, 2)
		convey.So(int(balancer.nodes["node1"].requestCPU), convey.ShouldEqual, 500)
	})

	convey.Convey("Test scaleDefaultLabelsMigratePod succeed to migrate with Anti-AffinityNodes 2", t, func() {
		patch := gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
			return &cache.NodeInfo{}
		})
		defer func() {
			patch.Reset()
		}()
		balancer.scaleNoScheduleLabelsMigratePod([]string{"node2"})
		convey.So(int(balancer.nodes["node2"].usedPods), convey.ShouldEqual, 1)
		convey.So(int(balancer.nodes["node2"].requestCPU), convey.ShouldEqual, 480)
	})

	convey.Convey("Test scaleDefaultLabelsMigratePod succeed to migrate with Anti-AffinityNodes 3", t, func() {
		patch := gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeID string) *cache.NodeInfo {
			if nodeID == "node3" {
				return &cache.NodeInfo{
					IsDefaultLabel: true,
				}
			}
			return &cache.NodeInfo{}
		})
		defer func() {
			patch.Reset()
		}()
		balancer.scaleNoScheduleLabelsMigratePod([]string{"node3"})
		convey.So(int(balancer.nodes["node3"].usedPods), convey.ShouldEqual, 1)
		convey.So(int(balancer.nodes["node3"].requestCPU), convey.ShouldEqual, 440)
	})
}
