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
	"fmt"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/metrics"
	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	tools "functioncore/pkg/worker-manager/utils"
	mockutils "functioncore/test/utils"
)

func assertActions(actions []Action, needPull, needReduce int) error {
	for _, action := range actions {
		if action.Type == ActionPull {
			if action.NeedPull != needPull {
				return fmt.Errorf("action need pull = %v, want %v", action.NeedPull, needPull)
			}
		}
		if action.Type == ActionReduce {
			if len(action.NeedReduce) != needReduce {
				return fmt.Errorf("action need reduce len = %v, want %v", len(action.NeedReduce), needReduce)
			}
		}
	}
	return nil
}

func Test_parseCAERollingPercent(t *testing.T) {
	defer os.Unsetenv("CAERollingPercent")
	tests := []struct {
		name     string
		val      string
		expected int
	}{
		{
			name:     "empty env",
			val:      "",
			expected: defaultRollingPercentage,
		},
		{
			name:     "invalid int",
			val:      "some value",
			expected: defaultRollingPercentage,
		},
		{
			name:     "too small",
			val:      "0",
			expected: defaultRollingPercentage,
		},
		{
			name:     "too large",
			val:      "101",
			expected: defaultRollingPercentage,
		},
		{
			name:     "valid value",
			val:      "66",
			expected: 66,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv("CAERollingPercent", tt.val)
			assert.Equal(t, tt.expected, parseCAERollingPercent())
		})
	}
}

func Test_workerCache_GetAndClearRollingInstance(t *testing.T) {
	wc := workerCache{}
	functions := map[string]*functionCache{
		"function1": {
			crSignature:      "1",
			rollingInstances: map[string]v1alpha1.WorkerInstance{"pod1": {InstanceID: "pod1"}, "pod2": {InstanceID: "pod2"}},
		},
	}
	for k, v := range functions {
		wc.functions.Store(k, v)
	}

	tests := []struct {
		name        string
		funcSvcID   string
		crSignature string

		expected []v1alpha1.WorkerInstance
	}{
		{
			name:      "function not exits",
			funcSvcID: "some function",

			expected: nil,
		},
		{
			name:        "signature mismatched",
			funcSvcID:   "function1",
			crSignature: "2",
			expected:    nil,
		},
		{
			name:        "success get",
			funcSvcID:   "function1",
			crSignature: "1",
			expected:    []v1alpha1.WorkerInstance{{InstanceID: "pod1"}, {InstanceID: "pod2"}},
		},
		{
			name:        "multi get return nil",
			funcSvcID:   "function1",
			crSignature: "1",
			expected:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := wc.GetAndClearRollingInstance(tt.funcSvcID, tt.crSignature)
			sort.Slice(result, func(i, j int) bool {
				return result[i].InstanceID < result[j].InstanceID
			})
			assert.Equal(t, tt.expected, result, tt.name)
		})
	}
}

func Test_workerCache_GenerateInitRollingCAEFuncActions(t *testing.T) {
	wc := workerCache{}
	functions := map[string]*functionCache{
		"meta_nil":     {},
		"not_cae_func": {funcMeta: &faasfunction.FunctionService{}},
		"no_rolling_instance": {
			funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}}},
		},
		"func_1": {
			funcMeta:    &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}}},
			crSignature: "1",
			rollingInstances: map[string]v1alpha1.WorkerInstance{
				"pod1": {PodName: "pod1", Applier: constants.WorkerManagerApplier},
				"pod2": {PodName: "pod2", Applier: constants.WorkerManagerApplier},
				"pod3": {PodName: "pod3", Applier: constants.ASBResApplier},
			},
		},
		"func_2": {
			funcMeta:    &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}}},
			crSignature: "2",
			rollingInstances: map[string]v1alpha1.WorkerInstance{
				"pod4": {PodName: "pod4", Applier: constants.ASBResApplier},
				"pod5": {PodName: "pod5", Applier: constants.ASBResApplier},
			},
		},
	}
	for k, v := range functions {
		wc.functions.Store(k, v)
	}
	wc.functions.Store(123, &functionCache{})
	wc.functions.Store("123", 123)

	expected := []FuncActions{
		{
			FuncSvcID: "func_1",
			Actions: []Action{
				{Type: ActionRollingUpdate, RollingReserved: 2, RollingOnDemand: 1, RollingPercentage: caeRollingPercentage},
			},
			Metadata: Metadata{FuncMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}}},
				CRSignature: "1", Status: IsChanged},
		},
		{
			FuncSvcID: "func_2",
			Actions: []Action{
				{Type: ActionRollingUpdate, RollingReserved: 0, RollingOnDemand: 2, RollingPercentage: caeRollingPercentage},
			},
			Metadata: Metadata{FuncMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{BusinessType: "CAE"}}},
				CRSignature: "2", Status: IsChanged},
		},
	}

	result := wc.GenerateInitRollingCAEFuncActions()

	sort.Slice(result, func(i, j int) bool {
		return result[i].FuncSvcID < result[j].FuncSvcID
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].FuncSvcID < expected[j].FuncSvcID
	})
	assert.Equal(t, expected, result)
}

func Test_workerCache_handleCAESignatureChanged(t *testing.T) {
	tests := []struct {
		name  string
		fc    *functionCache
		event *FunctionMeta

		expected []Action
	}{
		{
			name: "normal case",
			fc: &functionCache{
				minScale:         5,
				rollingInstances: map[string]v1alpha1.WorkerInstance{},
				instances: map[string]v1alpha1.WorkerInstance{
					"pod1": {Applier: constants.WorkerManagerApplier},
					"pod2": {Applier: constants.WorkerManagerApplier},
					"pod3": {Applier: constants.ASBResApplier},
					"pod4": {Applier: constants.ASBResApplier},
					"pod5": {Applier: "functiontask-xxx"},
				},
			},
			event: &FunctionMeta{},

			expected: []Action{
				{Type: ActionRollingUpdate, RollingReserved: 5, RollingOnDemand: 3, RollingPercentage: caeRollingPercentage},
				{Type: ActionReducePrewarm, PrewarmPod: []*corev1.Pod{}},
				{Type: ActionPullPrewarm, ScaleUpPrewarmPodNum: 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instanceNum := len(tt.fc.instances)
			assert.Equal(t, tt.expected, (&workerCache{}).handleCAESignatureChanged(tt.fc, tt.event), tt.name)
			assert.Equal(t, 0, len(tt.fc.instances), tt.name)
			assert.Equal(t, instanceNum, len(tt.fc.rollingInstances), tt.name)
		})
	}
}

func Test_reduceRollingInstance(t *testing.T) {
	tests := []struct {
		name  string
		fc    *functionCache
		event *InstanceMeta

		expected []Action
	}{
		{
			name: "no init or rolling instances",
			fc: &functionCache{
				initInstances:    map[string]struct{}{},
				rollingInstances: map[string]v1alpha1.WorkerInstance{},
			},
			event: &InstanceMeta{Instance: v1alpha1.WorkerInstance{PodName: "pod1", Applier: constants.WorkerManagerApplier}},

			expected: nil,
		},
		{
			name: "init reserved instances",
			fc: &functionCache{
				initInstances:    map[string]struct{}{"pod1": {}},
				rollingInstances: map[string]v1alpha1.WorkerInstance{"old_pod1": {PodName: "old_pod1", Applier: constants.WorkerManagerApplier}},
			},
			event: &InstanceMeta{Instance: v1alpha1.WorkerInstance{PodName: "pod1", Applier: constants.WorkerManagerApplier}},

			expected: nil,
		},
		{
			name: "reserved instances reduce reserved instances",
			fc: &functionCache{
				initInstances:    map[string]struct{}{},
				rollingInstances: map[string]v1alpha1.WorkerInstance{"old_pod1": {PodName: "old_pod1", Applier: constants.WorkerManagerApplier}},
			},
			event: &InstanceMeta{Instance: v1alpha1.WorkerInstance{PodName: "pod1", Applier: constants.WorkerManagerApplier}},

			expected: []Action{{Type: ActionReduce, NeedReduce: []v1alpha1.WorkerInstance{{PodName: "old_pod1", Applier: constants.WorkerManagerApplier}}}},
		},
		{
			name: "reserved instances cannot reduce normal instances",
			fc: &functionCache{
				initInstances: map[string]struct{}{},
				rollingInstances: map[string]v1alpha1.WorkerInstance{"old_pod1": {PodName: "old_pod1", Applier: constants.ASBResApplier},
					"old_pod2": {PodName: "old_pod2", Applier: "functiontask-xxx"}},
			},
			event: &InstanceMeta{Instance: v1alpha1.WorkerInstance{PodName: "pod1", Applier: constants.WorkerManagerApplier}},

			expected: nil,
		},
		{
			name: "asb instances reduce normal instances",
			fc: &functionCache{
				initInstances:    map[string]struct{}{},
				rollingInstances: map[string]v1alpha1.WorkerInstance{"old_pod1": {PodName: "old_pod1", Applier: "functiontask-xxx"}},
			},
			event: &InstanceMeta{Instance: v1alpha1.WorkerInstance{PodName: "pod1", Applier: constants.ASBResApplier}},

			expected: []Action{{Type: ActionReduce, NeedReduce: []v1alpha1.WorkerInstance{{PodName: "old_pod1", Applier: "functiontask-xxx"}}}},
		},
		{
			name: "asb instances cannot reduce reserved instances",
			fc: &functionCache{
				initInstances:    map[string]struct{}{},
				rollingInstances: map[string]v1alpha1.WorkerInstance{"old_pod1": {PodName: "old_pod1", Applier: constants.WorkerManagerApplier}},
			},
			event: &InstanceMeta{Instance: v1alpha1.WorkerInstance{PodName: "pod1", Applier: constants.ASBResApplier}},

			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, reduceRollingInstance("", tt.fc, tt.event), tt.name)
		})
	}
}

func Test_workerCache_GenerateActions_All(t *testing.T) {
	mockFuncSvcID := "mockFuncSvcID"
	mockSignature := "mockSignature"
	mockPodName := "mockPodName"
	w := &workerCache{}

	FuncMetaAdd := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Added,
		FuncMeta:    &faasfunction.FunctionService{},
	}
	FuncMetaAdd.FuncMeta.Spec.Configuration.AutoScalerConfig.MaximumScale = 100

	InsMetaAdd := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
	}, eventsource.Added)

	predictionInsMetaAdd := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName,
		Applier:   "prediction",
		Signature: mockSignature,
	}, eventsource.Added)

	InsMetaDel := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
	}, eventsource.Deleted)

	FuncMetaDel := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Deleted,
	}

	actions, _ := w.HandleFunctionRepo(mockFuncSvcID, FuncMetaAdd)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInsMetadata(mockFuncSvcID, config.InstanceMetaData{
		MinInstance: 1, MaxInstance: 10, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})
	if err := assertActions(actions, 1, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaAdd)
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaDel)
	if err := assertActions(actions, 1, 1); err != nil {
		t.Errorf(err.Error())
	}

	utils.SetSmartWarmerEnable(true)
	actions, _ = w.HandleInstance(mockFuncSvcID, predictionInsMetaAdd)
	assert.Equal(t, true, actions == nil)

	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaAdd)
	actions, _ = w.HandleFunctionRepo(mockFuncSvcID, FuncMetaDel)
	if err := assertActions(actions, 0, 1); err != nil {
		t.Errorf(err.Error())
	}
}

func TestWorkerCache_HandleInstance_PreferDeleteMigratingIns(t *testing.T) {
	mockFuncSvcID := "mock-funcSvcID-migrating"
	mockSignature := "mockSignature-migrating"
	mockPodName1 := "mock-pod-with-migrating-label-1"
	mockPodName2 := "mock-pod-with-migrating-label-2"
	nodeName1 := "mock-nodeName1"
	nodeName2 := "mock-nodeName2"
	w := &workerCache{}

	FuncMetaAdd := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Added,
		FuncMeta:    &faasfunction.FunctionService{},
	}
	FuncMetaAdd.FuncMeta.Spec.Configuration.AutoScalerConfig.MaximumScale = 100

	InsMetaUpdate := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName1,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
		NodeName:  nodeName1,
	}, eventsource.Updated)
	InsMetaUpdateWithMigrating := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:     mockPodName1,
		Applier:     constants.WorkerManagerApplier,
		Signature:   mockSignature,
		IsMigrating: true,
		NodeName:    nodeName1,
	}, eventsource.Updated)
	Ins2MetaUpdate := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName2,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
		NodeName:  nodeName2,
	}, eventsource.Updated)

	var patchesFunc mockutils.PatchesFunc
	patchesFunc = func() mockutils.PatchSlice {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{
			gomonkey.ApplyFunc(GetNodeInfo, func(nodeID string) *NodeInfo {
				if nodeID == nodeName1 {
					return &NodeInfo{Taints: []corev1.Taint{
						{
							Key: corev1.TaintNodeUnschedulable,
						},
					}}
				}
				return &NodeInfo{}
			}),
		})
		return patches
	}
	patches := patchesFunc()
	defer patches.ResetAll()

	actions, _ := w.HandleFunctionRepo(mockFuncSvcID, FuncMetaAdd)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInsMetadata(mockFuncSvcID, config.InstanceMetaData{
		MinInstance: 1, MaxInstance: 10, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})
	if err := assertActions(actions, 1, 0); err != nil {
		t.Errorf(err.Error())
	}

	// begin to test prefer to delete migrated instance
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaUpdate)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	// pod is changed to migrating pod
	actions, _ = w.HandleMigratingInstance(mockFuncSvcID, InsMetaUpdateWithMigrating, nil)
	w.HandleMigratingInstanceFailed(mockFuncSvcID, InsMetaUpdateWithMigrating)
	actions, _ = w.HandleMigratingInstance(mockFuncSvcID, InsMetaUpdateWithMigrating, nil)
	// add a new reserved instance, it will prefer to scale down migrated pod
	log.GetLogger().Infof("expect scale down a migrating reserved instance")
	actions, _ = w.HandleInstance(mockFuncSvcID, Ins2MetaUpdate)
	// it will reduce a reserved instance that is migrated pod
	if err := assertActions(actions, 0, 1); err != nil {
		t.Errorf(err.Error())
	}
	// end to test prefer to delete migrating instance
}

func TestWorkerCache_HandleInstance_ReachMaxInstance(t *testing.T) {
	mockFuncSvcID := "mock-funcSvcID-migrating"
	mockSignature := "mockSignature-migrating"
	mockPodName := "mock-pod-with-migrating-label"
	nodeName := "mock-nodeName"
	w := &workerCache{}
	fc := w.CreateFunctionCacheByFuncMeta(mockFuncSvcID, nil)
	fc.maxScale = 1
	fc.crSignature = mockSignature
	InsMetaUpdateWithMigrating := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:     mockPodName,
		Applier:     "-",
		Signature:   mockSignature,
		IsMigrating: true,
		NodeName:    nodeName,
	}, eventsource.Updated)
	// test migrating when reaching maxScale
	patch := gomonkey.ApplyFunc((*functionCache).getCurOnDemandNum, func(fc *functionCache) int64 {
		return 2
	})
	defer patch.Reset()
	actions, _ := w.HandleInstance(mockFuncSvcID, InsMetaUpdateWithMigrating)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	assert.Nil(t, actions)
}

func Test_workerCache_GenerateActions_Failure(t *testing.T) {
	mockFuncSvcID := "mockFuncSvcID"
	mockSignature := "mockSignature"
	w := &workerCache{}

	FuncMetaAdd := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Added,
		FuncMeta:    &faasfunction.FunctionService{},
	}
	FuncMetaAdd.FuncMeta.Spec.Configuration.AutoScalerConfig.MaximumScale = 100

	PullFailureMeta := NewPullFailureMeta(6, mockSignature, 10*time.Second)

	actions, _ := w.HandleFunctionRepo(mockFuncSvcID, FuncMetaAdd)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInsMetadata(mockFuncSvcID, config.InstanceMetaData{
		MinInstance: 6, MaxInstance: 10, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})
	if err := assertActions(actions, 6, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleFailure(mockFuncSvcID, PullFailureMeta)
	if err := assertActions(actions, 6, 0); err != nil {
		t.Errorf(err.Error())
	}
}

func Test_workerCache_GenerateActions_AutoScaleDown(t *testing.T) {
	mockFuncSvcID := "mockFuncSvcID"
	mockSignature := "mockSignature"
	mockPodName := "mockPodName"
	mockPodName2 := "mockPodName2"
	mockNodeName := "mockNodeName"
	mockNodeName2 := "mockNodeName2"
	w := &workerCache{}

	FuncMetaAdd := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Added,
		FuncMeta:    &faasfunction.FunctionService{},
	}
	FuncMetaAdd.FuncMeta.Spec.Configuration.AutoScalerConfig.MaximumScale = 100

	InsMetaAdd := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
	}, eventsource.Added)

	InsMetaAdd2 := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName2,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
	}, eventsource.Added)

	autoScaleDownMeta := &AutoScaleDownMeta{
		Instances: []v1alpha1.WorkerInstance{
			{
				PodName:  mockPodName,
				NodeName: mockNodeName,
			},
			{
				PodName:  mockPodName2,
				NodeName: mockNodeName2,
			},
		},
	}

	actions, _ := w.HandleFunctionRepo(mockFuncSvcID, FuncMetaAdd)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInsMetadata(mockFuncSvcID, config.InstanceMetaData{
		MinInstance: 1, MaxInstance: 10, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})
	if err := assertActions(actions, 1, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaAdd)
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaAdd2)
	w.HandleInstanceAutoScaleDown(mockFuncSvcID, autoScaleDownMeta)
	if actions != nil {
		t.Errorf("actions actual %v, want nil", actions)
	}
}

func Test_workerCache_GenerateActions_SignatureChanged(t *testing.T) {
	mockFuncSvcID := "mockFuncSvcID"
	mockSignature := "mockSignature"
	mockChangedSignature := "mockChangedSignature"
	mockPodName := "mockPodName"
	w := &workerCache{}

	FuncMetaAdd := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Added,
		FuncMeta:    &faasfunction.FunctionService{},
	}

	FuncMetaAdd.FuncMeta.Spec.Configuration.AutoScalerConfig.MaximumScale = 100

	changedFuncMeta := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockChangedSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Updated,
		FuncMeta:    &faasfunction.FunctionService{},
	}

	InsMetaAdd := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
	}, eventsource.Added)

	InsMetaDel := NewInstanceMeta(v1alpha1.WorkerInstance{
		PodName:   mockPodName,
		Applier:   constants.WorkerManagerApplier,
		Signature: mockSignature,
	}, eventsource.Deleted)

	FuncMetaDel := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Deleted,
	}
	actions, _ := w.HandleFunctionRepo(mockFuncSvcID, FuncMetaAdd)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleFunctionRepo(mockFuncSvcID, changedFuncMeta)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInsMetadata(mockFuncSvcID, config.InstanceMetaData{
		MinInstance: 1, MaxInstance: 10, IdleMode: false}, types.ProfileMetaData{}, tools.FunctionInfo{})
	// min scale update will pull up reserved instance whatever signature changed
	if err := assertActions(actions, 1, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaAdd)
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaDel)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}
	actions, _ = w.HandleInstance(mockFuncSvcID, InsMetaAdd)
	actions, _ = w.HandleFunctionRepo(mockFuncSvcID, FuncMetaDel)
	if err := assertActions(actions, 0, 0); err != nil {
		t.Errorf(err.Error())
	}

	fm := &faasfunction.FunctionService{
		Spec: faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{
				IsStatefulFunction: true,
			},
		},
	}

	w.functions.Store("testFuncSvcID",
		&functionCache{
			funcMeta: fm,
			instances: map[string]v1alpha1.WorkerInstance{
				"testStateID": {PodName: "testPodName"},
			},
			crSignature: "123456",
		})

	StatefulInsMetaAdd := &FunctionMeta{
		FuncInfo:    metrics.FunctionInfo{},
		CRSignature: mockSignature,
		EventKind:   utils.InstanceMetadataEventKind,
		EventType:   eventsource.Added,
		FuncMeta:    &faasfunction.FunctionService{},
	}

	actions, _ = w.HandleFunctionRepo("testFuncSvcID", StatefulInsMetaAdd)
	if err := assertActions(actions, 0, 1); err != nil {
		t.Errorf(err.Error())
	}
}

// TestWorkerCache_calActions -
func TestWorkerCache_calActions(t *testing.T) {
	convey.Convey("TestWorkerCache_calActions", t, func() {
		WorkerCache.functions = sync.Map{}
		convey.Convey("calActionsOK", func() {
			needPullNum, needReduceNum, _ := WorkerCache.calActions(&functionCache{
				funcMeta:        &faasfunction.FunctionService{},
				scalingReversed: 2,
				minScale:        1,
				maxScale:        3,
				instances: map[string]v1alpha1.WorkerInstance{
					"reserved1": {
						Applier: constants.WorkerManagerApplier,
					},
					"normal1": {
						Applier: "functiontask-test1",
					},
					"normal2": {
						Applier: "functiontask-test2",
					},
					"normal3": {
						Applier: "functiontask-test3",
					},
				},
			})
			convey.So(needPullNum, convey.ShouldBeZeroValue)
			convey.So(needReduceNum, convey.ShouldEqual, 0)
		})
		convey.Convey("minScaleIsGreaterThanMaxScale", func() {
			needPullNum, needReduceNum, _ := WorkerCache.calActions(&functionCache{
				minScale: 2,
				maxScale: 1,
				instances: map[string]v1alpha1.WorkerInstance{
					"normal1": {},
				},
			})
			convey.So(needPullNum, convey.ShouldEqual, 2)
			convey.So(needReduceNum, convey.ShouldEqual, 0)
		})
		convey.Convey("snapshotManager mode skip calActions", func() {
			snapshotmanager.SetSnapshotManagerMode(true)
			needPullNum, needReduceNum, _ := WorkerCache.calActions(&functionCache{
				minScale: 2,
				maxScale: 1,
				instances: map[string]v1alpha1.WorkerInstance{
					"normal1": {},
				},
			})
			convey.So(needPullNum, convey.ShouldEqual, 0)
			convey.So(needReduceNum, convey.ShouldEqual, 0)
			snapshotmanager.SetSnapshotManagerMode(false)
		})

		convey.Convey("calActions", func() {
			needPullNum, needReduceNum, _ := WorkerCache.calActions(&functionCache{
				minScale:        2,
				scalingReversed: 3,
				maxScale:        1,
				instances: map[string]v1alpha1.WorkerInstance{
					"reserved1": {
						Applier: constants.WorkerManagerApplier,
					},
					"reserved2": {
						Applier: constants.WorkerManagerApplier,
					},
					"reserved3": {
						Applier: constants.WorkerManagerApplier,
					},
					"reserved4": {
						Applier: constants.WorkerManagerApplier,
					},
				},
			})
			convey.So(needPullNum, convey.ShouldEqual, 0)
			convey.So(needReduceNum, convey.ShouldEqual, 2)
		})
	})
}

func Test_handleReservedInstanceUpdate_migrated(t *testing.T) {
	fc := &functionCache{
		reservedInsNodePreDis: map[string]int{"pod1": 1},
		scalingDone:           map[string]int64{},
		minScale:              1,
		instances: map[string]v1alpha1.WorkerInstance{
			"pod2": {PodName: "pod2"},
		},
	}
	tests := []struct {
		name     string
		instance v1alpha1.WorkerInstance

		excepted string
	}{
		{
			name:     "migrated instance existed",
			instance: v1alpha1.WorkerInstance{PodName: "pod3-migrated", MigratedFrom: "pod2"},
			excepted: "pod2",
		},
		{
			name:     "migrated instance not existed",
			instance: v1alpha1.WorkerInstance{PodName: "pod4-migrated", MigratedFrom: "pod0"},
			excepted: "pod4-migrated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fc.scalingReversed = 3
			actions := fc.handleReservedInstanceUpdate(&InstanceMeta{Instance: tt.instance})
			assert.Equal(t, tt.excepted, actions[0].NeedReduce[0].PodName)
		})
	}
}

func TestWorkerCache_GetAndUpdateAPIGIP(t *testing.T) {
	w := &workerCache{
		apigIP: apigIPCache{
			ips: []string{"192.168.0.1", "192.168.0.2"},
		},
	}
	ips := w.GetAPIGIP()
	assert.Equal(t, 2, len(ips))
	assert.Equal(t, "192.168.0.1", ips[0])
	assert.Equal(t, "192.168.0.2", ips[1])

	w.UpdateAPIGIP([]string{"172.168.0.1"})
	newIps := w.GetAPIGIP()
	assert.Equal(t, 1, len(newIps))
	assert.Equal(t, "172.168.0.1", newIps[0])

	w.DeleteAPIGIP()
	tmpIps := w.GetAPIGIP()
	assert.Equal(t, 0, len(tmpIps))

	w.UpdateAPIGIP([]string{"172.168.0.3", "172.168.0.4"})
	apigIps := w.GetAPIGIP()
	assert.Equal(t, 2, len(apigIps))
	assert.Equal(t, "172.168.0.3", apigIps[0])
	assert.Equal(t, "172.168.0.4", apigIps[1])
}

func Test_UpdateFuncMinScaleUpdatedTime(t *testing.T) {
	type args struct {
		functions            sync.Map
		err                  error
		funcSvcID            string
		isUserErrorAlarmSign bool
	}
	var fm sync.Map
	fm.Store("aaa", &functionCache{
		funcMeta:             &faasfunction.FunctionService{},
		minScale:             1,
		maxScale:             2,
		isUserErrorAlarmSign: true,
		minScaleUpdatedTime:  time.Unix(1500000000, 0),
	})
	fm.Store("bbb", &functionCache{
		funcMeta:             &faasfunction.FunctionService{},
		minScale:             1,
		maxScale:             2,
		isUserErrorAlarmSign: false,
		minScaleUpdatedTime:  time.Unix(1500000000, 0),
	})

	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		want        int64
	}{
		{
			name: "case1",
			args: args{
				functions:            fm,
				err:                  snerror.New(150500, "system error"),
				funcSvcID:            "aaa",
				isUserErrorAlarmSign: true,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
			want: 0,
		},
		{
			name: "case2",
			args: args{
				functions:            fm,
				err:                  snerror.New(4009, "user error"),
				funcSvcID:            "bbb",
				isUserErrorAlarmSign: false,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			w := &workerCache{
				functions: tt.args.functions,
			}
			w.UpdateFuncMinScaleUpdatedTime(tt.args.err, tt.args.funcSvcID)
			cache, _ := w.functions.Load(tt.args.funcSvcID)
			fci, _ := cache.(*functionCache)
			assert.Equal(t, !tt.args.isUserErrorAlarmSign, fci.isUserErrorAlarmSign)
			patches.ResetAll()
		})
	}
}

func Test_workerCache_HandleMigratingInstance(t *testing.T) {
	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID   string
		event       *InstanceMeta
		podTemplate *corev1.Pod
	}
	mockFuncSvcID := "mockFuncSvcID001"
	mockPodName := "mockPodName001"
	mockWorkerIns := v1alpha1.WorkerInstance{
		PodName: mockPodName,
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		instancesLen int
		expectLen    int64
	}{
		{
			name:   "test succeed to migrate instance when pod is in instance cache",
			fields: fields{},
			args: args{
				funcSvcID: mockFuncSvcID,
				event:     NewInstanceMeta(mockWorkerIns, eventsource.Updated),
			},
			expectLen: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			w.StoreWorkerInstance(mockFuncSvcID, mockWorkerIns)
			w.HandleMigratingInstance(tt.args.funcSvcID, tt.args.event, tt.args.podTemplate)
			actualLen, _ := w.GetInstanceNum(tt.args.funcSvcID)
			if actualLen != tt.expectLen {
				t.Errorf("expect %v but found %v", tt.expectLen, actualLen)
			}
		})
	}
}

func Test_workerCache_UpdateFuncMaxScale(t *testing.T) {
	w := &workerCache{
		functions: sync.Map{},
	}
	w.functions.Store("test", &functionCache{})
	clusterStatusMap := make(map[string]clusterstatusutil.ClusterFlowState)
	clusterStatusMap["az101"] = clusterstatusutil.ClusterFlowState{State: "running"}
	w.UpdateFuncMaxScale()
}

func Test_getCurrentAvailableClusters(t *testing.T) {
	clusterstatusutil.ClusterStatusCache.ClusterStatusMap = make(map[string]clusterstatusutil.ClusterFlowState)
	clusterstatusutil.ClusterStatusCache.ClusterStatusMap["cluster"] = clusterstatusutil.ClusterFlowState{State: "running"}
	tests := []struct {
		name string
		want []string
	}{
		{"case1", []string{"cluster"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, getCurrentAvailableClusters(), "getCurrentAvailableClusters")
		})
	}
}

func Test_HandlePrediction(t *testing.T) {
	convey.Convey("Test_HandlePrediction", t, func() {
		convey.Convey("scaleLen > 0", func() {
			w := workerCache{
				functions: sync.Map{},
			}
			scaleLen := 1
			w.functions.Store("funcSvcID", &functionCache{})
			actions := w.HandlePrediction("funcSvcID", scaleLen, "funcName")
			convey.So(actions[0].Type, convey.ShouldEqual, ActionPullPrewarm)
		})
		convey.Convey("scaleLen < 0", func() {
			w := workerCache{
				functions: sync.Map{},
			}
			scaleLen := -1
			w.functions.Store("funcSvcID", &functionCache{})
			actions := w.HandlePrediction("funcSvcID", scaleLen, "funcName")
			convey.So(actions[0].Type, convey.ShouldEqual, ActionReducePrewarm)
		})
		convey.Convey("scaleLen == 0", func() {
			w := workerCache{
				functions: sync.Map{},
			}
			scaleLen := 0
			w.functions.Store("funcSvcID", &functionCache{})
			actions := w.HandlePrediction("funcSvcID", scaleLen, "funcName")
			convey.So(actions, convey.ShouldBeNil)
		})
	})
}

func Test_needPullUpMicroServiceInstance(t *testing.T) {
	convey.Convey("Test_needPullUpMicroServiceInstance", t, func() {
		fc := functionCache{
			expectedNum: 4,
			minScale:    2,
			maxScale:    4,
		}
		convey.Convey("test lack of reserved instance", func() {
			fc.instances = make(map[string]v1alpha1.WorkerInstance, initCacheSize)
			fc.instances["testReserved"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testASBRes1"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			fc.instances["testASBRes2"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			convey.ShouldBeFalse(needPullUpMicroServiceInstance(&fc))
		})
		convey.Convey("test lack of ASBRes instance", func() {
			fc.instances = make(map[string]v1alpha1.WorkerInstance, initCacheSize)
			fc.instances["testReserved"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testReserved2"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testASBRes1"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			convey.ShouldBeTrue(needPullUpMicroServiceInstance(&fc))
		})
		convey.Convey("test unnecessary ASBRes instance", func() {
			fc.instances = make(map[string]v1alpha1.WorkerInstance, initCacheSize)
			fc.instances["testReserved"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testReserved2"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testASBRes1"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			fc.instances["testASBRes2"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			fc.instances["testASBRes3"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			convey.ShouldBeFalse(needPullUpMicroServiceInstance(&fc))
		})
		convey.Convey("test unnecessary reserved instance but lack of ASBRes instance", func() {
			fc.instances = make(map[string]v1alpha1.WorkerInstance, initCacheSize)
			fc.instances["testReserved"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testReserved2"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testReserved3"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testASBRes1"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			convey.ShouldBeTrue(needPullUpMicroServiceInstance(&fc))
		})
		convey.Convey("test expected num larger than max instance", func() {
			fc := functionCache{
				expectedNum: 5,
				minScale:    1,
				maxScale:    3,
			}
			fc.instances = make(map[string]v1alpha1.WorkerInstance, initCacheSize)
			fc.instances["testReserved"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testASBRes1"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			fc.instances["testASBRes2"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			fc.instances["testASBRes3"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			convey.ShouldBeFalse(needPullUpMicroServiceInstance(&fc))

			fc.instances = make(map[string]v1alpha1.WorkerInstance, initCacheSize)
			fc.instances["testReserved"] = v1alpha1.WorkerInstance{
				Applier: constants.WorkerManagerApplier,
			}
			fc.instances["testASBRes1"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			fc.instances["testASBRes2"] = v1alpha1.WorkerInstance{
				Applier: constants.ASBResApplier,
			}
			convey.ShouldBeTrue(needPullUpMicroServiceInstance(&fc))
		})
	})
}

func Test_workerCache_handleInstanceUpdate(t *testing.T) {
	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
		event     *InstanceMeta
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantActionLen int
		patches       mockutils.PatchesFunc
	}{
		{
			name: "test get empty action",
			args: args{
				funcSvcID: "",
				event: &InstanceMeta{
					Instance: v1alpha1.WorkerInstance{
						Signature: "1",
						Applier:   constants.WorkerManagerApplier,
						PodName:   "podA",
					},
				},
			},
			fields: fields{
				functions: sync.Map{},
			},
			wantActionLen: 0,
			patches: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load",
						func(_ *sync.Map, key any) (value any, ok bool) {
							return &functionCache{
								RWMutex:     sync.RWMutex{},
								crSignature: "1",
								scalingDone: map[string]int64{"podA": 10},
								instances:   map[string]v1alpha1.WorkerInstance{},
							}, true
						}),
				})
				return patches
			},
		},
		{
			name: "test get podA",
			args: args{
				funcSvcID: "",
				event: &InstanceMeta{
					Instance: v1alpha1.WorkerInstance{
						Signature: "1",
						Applier:   constants.WorkerManagerApplier,
						PodName:   "podA",
					},
				},
			},
			fields: fields{
				functions: sync.Map{},
			},
			wantActionLen: 1,
			patches: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&sync.Map{}), "Load",
						func(_ *sync.Map, key any) (value any, ok bool) {
							return &functionCache{
								RWMutex:     sync.RWMutex{},
								crSignature: "1",
								scalingDone: map[string]int64{},
								instances:   map[string]v1alpha1.WorkerInstance{},
							}, true
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patches()
		w := &workerCache{
			functions: tt.fields.functions,
			apigIP:    tt.fields.apigIP,
		}
		action, _ := w.handleInstanceUpdate(tt.args.funcSvcID, tt.args.event)
		if len(action) != tt.wantActionLen {
			t.Errorf("failed to test handleInstanceUpdate, action name: %s, action: %+v", tt.name, action)
		}
		patches.ResetAll()
	}
}

func TestCurrentClustersStatus(t *testing.T) {
	envValClusterID := clusterID
	envValClusterStatusCache := clusterstatusutil.ClusterStatusCache
	clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterRunningStatus}}}
	clusterID = "testCluster"
	if !IsCurrentClustersStatusRunning() {
		t.Errorf("this is true")
	}
	clusterstatusutil.ClusterStatusCache = clusterstatusutil.ClusterStatus{ClusterStatusMap: map[string]clusterstatusutil.ClusterFlowState{"testCluster": {State: clusterstatusutil.ClusterFlowOutingStatus}}}
	if IsCurrentClustersStatusRunning() {
		t.Errorf("this is true")
	}
	clusterID = envValClusterID
	clusterstatusutil.ClusterStatusCache = envValClusterStatusCache
}

func Test_isNeedReloadStatefulFunction(t *testing.T) {
	convey.Convey("Test isNeedReloadStatefulFunction", t, func() {
		fc := &functionCache{}
		event := &InstanceMeta{
			Instance: v1alpha1.WorkerInstance{
				PodName: "pod1",
			},
		}
		convey.Convey("When stateID is empty", func() {
			event.Instance.StateID = ""
			result := isNeedReloadStatefulFunction(fc, event)
			convey.So(result, convey.ShouldBeFalse)
		})
		convey.Convey("checkStateIDKeyExist false", func() {
			event.Instance.StateID = "ss"
			fc.stateMap.Store("ss", "")
			fc.stateFulEventCheckRepeat.Store("ss", "pod1")
			result := isNeedReloadStatefulFunction(fc, event)
			convey.So(result, convey.ShouldBeTrue)
		})
	})
}

func Test_refreshMetadataWhenHandleInstanceDelete(t *testing.T) {
	convey.Convey("Given a metadata, function cache, and event instance", t, func() {
		metadata := &Metadata{}
		fc := &functionCache{}
		event := &InstanceMeta{
			Instance: v1alpha1.WorkerInstance{
				Creator: "TestCreator",
				StateID: "123",
				CPU:     2,
				Memory:  4096,
			},
		}
		convey.Convey("When calling refreshMetadataWhenHandleInstanceDelete", func() {
			refreshMetadataWhenHandleInstanceDelete(metadata, fc, event)
			convey.Convey("It should update metadata correctly", func() {
				convey.So(metadata.Creator, convey.ShouldEqual, "TestCreator")
				convey.So(metadata.FuncMeta, convey.ShouldEqual, fc.funcMeta)
				convey.So(metadata.CRSignature, convey.ShouldEqual, fc.crSignature)
				convey.So(metadata.CreateOptions.CPU, convey.ShouldEqual, 2)
				convey.So(metadata.CreateOptions.Memory, convey.ShouldEqual, 4096)
			})
		})
		convey.Convey("When StateID is empty in the event instance", func() {
			event.Instance.StateID = ""
			refreshMetadataWhenHandleInstanceDelete(metadata, fc, event)
			convey.Convey("It should not set CreateOptions", func() {
				convey.So(metadata.CreateOptions, convey.ShouldBeNil)
			})
		})
	})
}

func Test_handleInstanceDelete(t *testing.T) {
	convey.Convey("test handleInstanceDelete", t, func() {
		convey.Convey("get functionCache error", func() {
			wc := workerCache{}
			ins := &InstanceMeta{}
			actions, metadata := wc.handleInstanceDelete(funcSvcID, ins)
			if actions != nil && metadata.Status != NotChanged {
				t.Errorf("this is error")
			}
		})

		convey.Convey("event.Instance.Signature != fc.crSignature && fc.metadata is nil", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(refreshMetadataWhenHandleInstanceDelete, func(metadata *Metadata, fc *functionCache, event *InstanceMeta) {}),
			})
			defer patches.ResetAll()
			wc := workerCache{}
			ins := &InstanceMeta{Instance: v1alpha1.WorkerInstance{Signature: "testSignature"}}
			fc := &functionCache{}
			wc.functions.Store(funcSvcID, fc)
			actions, metadata := wc.handleInstanceDelete(funcSvcID, ins)
			if actions != nil && metadata.Status != NotChanged {
				t.Errorf("this is error")
			}
		})

		convey.Convey("event.Instance.Signature != fc.crSignature && !fc.funcMeta.Spec.Configuration.IsStatefulFunction", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(refreshMetadataWhenHandleInstanceDelete, func(metadata *Metadata, fc *functionCache, event *InstanceMeta) {}),
			})
			defer patches.ResetAll()
			wc := workerCache{}
			ins := &InstanceMeta{Instance: v1alpha1.WorkerInstance{Signature: "testSignature"}}
			fc := &functionCache{funcMeta: &faasfunction.FunctionService{}}
			wc.functions.Store(funcSvcID, fc)
			actions, metadata := wc.handleInstanceDelete(funcSvcID, ins)
			if actions != nil && metadata.Status != NotChanged {
				t.Errorf("this is error")
			}
		})

		convey.Convey("fc.instances[event.Instance.PodName], existed", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc((*functionCache).deleteInstance, func(_ *functionCache, podName string, isReversed bool) {}),
				gomonkey.ApplyFunc(isNeedReloadStatefulFunction, func(fc *functionCache, event *InstanceMeta) bool {
					return true
				}),
			})
			defer patches.ResetAll()
			wc := workerCache{}
			ins := &InstanceMeta{Instance: v1alpha1.WorkerInstance{Signature: "testSignature"}}
			fc := &functionCache{funcMeta: &faasfunction.FunctionService{Spec: faasfunction.FunctionServiceSpec{Configuration: faasfunction.ConfigurationSpec{IsStatefulFunction: true}}},
				instances: map[string]v1alpha1.WorkerInstance{"podName": {}}}
			wc.functions.Store(funcSvcID, fc)
			actions, metadata := wc.handleInstanceDelete(funcSvcID, ins)
			if actions == nil && metadata.Status != NotChanged {
				t.Errorf("this is error")
			}
		})
	})
}

func Test_workerCache_GetPodInMap(t *testing.T) {
	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
		stateID   string
	}
	var f fields
	var a args
	a.stateID = "stateID"
	var a2 args
	a2.stateID = "stateID_2"
	var stateFulEventCheckRepeatMap sync.Map
	stateFulEventCheckRepeatMap.Store("stateID", "podName")
	stateFulEventCheckRepeatMap.Store("stateID_2", 5)
	tests := []struct {
		name        string
		fields      fields
		args        args
		want        string
		want1       bool
		patchesFunc mockutils.PatchesFunc
	}{
		{"stateFulEventCheckRepeat load failed",
			f, a, "", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{}, true
					}),
				})
				return patches
			}},
		{"stateFulEventCheckRepeat load success",
			f, a, "podName", true, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{stateFulEventCheckRepeat: stateFulEventCheckRepeatMap}, true
					}),
				})
				return patches
			}},
		{"stateFulEventCheckRepeat load success and convert fail",
			f, a2, "", false, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{stateFulEventCheckRepeat: stateFulEventCheckRepeatMap}, true
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			got, got1 := w.GetPodInMap(tt.args.funcSvcID, tt.args.stateID)
			assert.Equalf(t, tt.want, got, "GetPodInMap(%v, %v)", tt.args.funcSvcID, tt.args.stateID)
			assert.Equalf(t, tt.want1, got1, "GetPodInMap(%v, %v)", tt.args.funcSvcID, tt.args.stateID)
		})
		patches.ResetAll()
	}
}

func Test_workerCache_GetStatePodActionWithoutReload(t *testing.T) {
	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
		creator   string
		stateID   string
	}
	var f fields
	var a args
	funcMeta := &faasfunction.FunctionService{}
	instances := map[string]v1alpha1.WorkerInstance{}
	instance := v1alpha1.WorkerInstance{}
	instances["test"] = instance
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{"GetStatePodActionWithoutReload get function cache fail",
			f, a, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{}, false
					}),
				})
				return patches
			}},
		{"GetStatePodActionWithoutReload get function cache success",
			f, a, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{funcMeta: funcMeta, instances: instances}, true
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			_, _ = w.GetStatePodActionWithoutReload(tt.args.funcSvcID, tt.args.creator, tt.args.stateID)
		})
		patches.ResetAll()
	}
}

func Test_workerCache_GetStatePodActionPullUp(t *testing.T) {
	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
		creator   string
		stateID   string
		cpu       int64
		memory    int64
	}
	var f fields
	var a args
	a.stateID = "stateID"
	funcMeta := &faasfunction.FunctionService{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantedErr   bool
	}{
		{"GetStatePodActionPullUp get function cache success",
			f, a, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{funcMeta: funcMeta}, true
					}),
				})
				return patches
			}, false},
		{"GetStatePodActionPullUp get function cache error",
			f, a, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return nil, false
					}),
				})
				return patches
			}, true},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			_, err := w.GetStatePodActionPullUp(tt.args.funcSvcID, tt.args.creator, tt.args.stateID, tt.args.cpu, tt.args.memory)
			assert.Equalf(t, tt.wantedErr, err != nil, "Test %s failed", tt.name)
		})
		patches.ResetAll()
	}
}

func Test_workerCache_GetStatePodActionReduce(t *testing.T) {
	type fields struct {
		functions sync.Map
		apigIP    apigIPCache
	}
	type args struct {
		funcSvcID string
		creator   string
		stateID   string
	}
	var f fields
	var a args
	funcMeta := &faasfunction.FunctionService{}
	instances := map[string]v1alpha1.WorkerInstance{}
	instance := v1alpha1.WorkerInstance{}
	instances["test"] = instance
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{"GetStatePodActionReduce get function cache success",
			f, a, func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*workerCache).getFunctionCache, func(_ *workerCache, funcSvcID string) (*functionCache, bool) {
						return &functionCache{funcMeta: funcMeta, instances: instances}, true
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			w := &workerCache{
				functions: tt.fields.functions,
				apigIP:    tt.fields.apigIP,
			}
			_, _ = w.GetStatePodActionReduce(tt.args.funcSvcID, tt.args.creator, tt.args.stateID)
		})
		patches.ResetAll()
	}
}

func Test_workerCache_NeedReRolling(t *testing.T) {
	w := workerCache{}
	functions := map[string]*functionCache{
		"function1": {
			crSignature: "1",
			minScale:    2,
			maxScale:    10,
		},
		"function2": {
			crSignature: "1",
			minScale:    0,
			maxScale:    10,
		},
		"function3": {
			crSignature: "1",
			minScale:    3,
			maxScale:    0,
		},
	}
	for k, v := range functions {
		w.functions.Store(k, v)
	}
	tests := []struct {
		name      string
		funcSvcID string
		event     *PullFailureMeta

		expected bool
	}{
		{
			name:      "function not exits",
			funcSvcID: "some func",
			event:     &PullFailureMeta{CRSignature: "1", FailedNums: 2},

			expected: false,
		},
		{
			name:      "signature mismatched",
			funcSvcID: "function1",
			event:     &PullFailureMeta{CRSignature: "22", FailedNums: 2},

			expected: false,
		},
		{
			name:      "reserved and minScale is 0",
			funcSvcID: "function2",
			event:     &PullFailureMeta{CRSignature: "1", FailedNums: 2, IsRollingReserved: true},

			expected: false,
		},
		{
			name:      "not reserved and maxScale is 0",
			funcSvcID: "function3",
			event:     &PullFailureMeta{CRSignature: "1", FailedNums: 2, IsRollingReserved: false},

			expected: false,
		},
		{
			name:      "reserved and minScale is not 0",
			funcSvcID: "function3",
			event:     &PullFailureMeta{CRSignature: "1", FailedNums: 2, IsRollingReserved: true},

			expected: true,
		},
		{
			name:      "not reserved and maxScale is not 0",
			funcSvcID: "function2",
			event:     &PullFailureMeta{CRSignature: "1", FailedNums: 2, IsRollingReserved: false},

			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, w.NeedReRolling(tt.funcSvcID, tt.event), tt.name)
		})
	}
}

func Test_reduceInstance(t *testing.T) {
	event := &InstanceMeta{
		Instance: v1alpha1.WorkerInstance{
			Applier: constants.PredictionApplier,
			PodName: "a",
		},
	}
	reduceInstance(event)

	event2 := &InstanceMeta{
		Instance: v1alpha1.WorkerInstance{
			Applier: "",
			PodName: "a",
		},
	}
	reduceInstance(event2)

}

func Test_workerCache_microServiceFunctionReload(t *testing.T) {
	w := workerCache{}
	actions, _ := w.microServiceFunctionReload(Metadata{})
	if actions[0].NeedPull != 1 {
		t.Errorf("failed to test microServiceFunctionReload")
	}
}
