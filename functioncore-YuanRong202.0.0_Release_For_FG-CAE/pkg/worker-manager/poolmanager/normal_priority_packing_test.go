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

package poolmanager

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"

	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/nodebalance"
	utils2 "functioncore/pkg/worker-manager/utils"
	"functioncore/test/utils"
)

func TestNormalPriorityPacking_GetEnableTMPExpansion(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			"get enabTMPxpansion",
			fields{
				enableTMPExpansion: true,
				funcName:           "test",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NormalPriorityPacking{
				enableTMPExpansion: tt.fields.enableTMPExpansion,
				funcName:           tt.fields.funcName,
			}
			if tt.want != n.GetEnableTMPExpansion() {
				t.Errorf("get enable tmp expansion error")
			}
		})
	}
}

func TestNormalPriorityPacking_GetFuncName(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			"test get funcName",
			fields{
				enableTMPExpansion: true,
				funcName:           "test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NormalPriorityPacking{
				enableTMPExpansion: tt.fields.enableTMPExpansion,
				funcName:           tt.fields.funcName,
			}
			if n.GetFuncName() != n.funcName {
				t.Errorf("get func name error")
			}
		})
	}
}

func TestNormalPriorityPacking_Recover(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			"test recover",
			fields{
				true,
				"test",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NormalPriorityPacking{
				enableTMPExpansion: tt.fields.enableTMPExpansion,
				funcName:           tt.fields.funcName,
			}
			n.Recover()
			if n.funcName != "" || n.enableTMPExpansion != false {
				t.Errorf("recover error")
			}
		})
	}
}

func TestNormalPriorityPacking_SelectPod(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	type args struct {
		p *Pool
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      *corev1.Pod
		wantNil   bool
		patchFunc utils.PatchesFunc
	}{
		{
			name: "test selectPod",
			fields: fields{
				false, "test",
			},
			args: args{
				p: &Pool{
					mux: &sync.Mutex{},
					PodList: map[nodeName]*NodePod{
						"1": {
							podsQueue: podsObjQueue{
								items: []*podObj{
									{
										pod: &corev1.Pod{
											ObjectMeta: metav1.ObjectMeta{Name: "testPod1"},
											Spec: corev1.PodSpec{
												NodeName: "1",
											},
										},
										usedFlag: false,
									}},
							},
							readyPodNum: 1,
						},
					},
				},
			},
			want: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "testPod1"},
			},
			patchFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(cache.GetNodeInfo, func(nodeName string) *cache.NodeInfo {
						return &cache.NodeInfo{}
					}),
					gomonkey.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
					gomonkey.ApplyFunc(getPoolMemoryInNode, func(node nodeName) int64 {
						return 0
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		n := &NormalPriorityPacking{
			enableTMPExpansion: tt.fields.enableTMPExpansion,
			funcName:           tt.fields.funcName,
		}
		pod := n.SelectPod(tt.args.p)
		if tt.wantNil {
			if pod != nil {
				t.Errorf("err when selete pod, want nil, get %s", pod.Name)
			}
		} else if !tt.wantNil && (pod.Name != tt.want.Name) {
			t.Errorf("err when selete pod, want %s, get %s", tt.want.Name, pod.Name)
		}
		patches.ResetAll()
	}
}

func TestNormalPriorityPacking_WithEnableTMPExpansion(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	type args struct {
		enableTMPExpansion bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test with enable tmp expansion",
			fields{
				enableTMPExpansion: true,
			},
			args{
				enableTMPExpansion: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NormalPriorityPacking{
				enableTMPExpansion: tt.fields.enableTMPExpansion,
				funcName:           tt.fields.funcName,
			}
			n.WithEnableTMPExpansion(tt.args.enableTMPExpansion)
			if n.enableTMPExpansion != tt.args.enableTMPExpansion {
				t.Errorf("with enable tmp expansion error")
			}
		})
	}
}

func TestNormalPriorityPacking_WithFuncName(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	type args struct {
		funcName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			"test with func name",
			fields{
				funcName: "test",
			},
			args{
				funcName: "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &NormalPriorityPacking{
				enableTMPExpansion: tt.fields.enableTMPExpansion,
				funcName:           tt.fields.funcName,
			}
			n.WithFuncName(tt.args.funcName)
			if tt.args.funcName != n.funcName {
				t.Errorf("test with func name err")
			}
		})
	}
}

func TestNormalPriorityPacking_scoreNodesAndSelectPod(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	type args struct {
		availableNodeList map[nodeName][]*corev1.Pod
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantPodName string
		patcherFunc utils.PatchesFunc
	}{
		{
			"test scoreNode and select pod",
			fields{
				enableTMPExpansion: false,
				funcName:           "test",
			},
			args{
				availableNodeList: map[nodeName][]*corev1.Pod{
					nodeName("1"): {
						&corev1.Pod{},
						&corev1.Pod{},
					},
				},
			},
			"",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(monitoring.GetFunctionProfilingDataByFuncName, func(
						funcName string) *monitoring.ProfilingContent {
						return &monitoring.ProfilingContent{
							CPU: &monitoring.ProfilingResourceItem{
								Average: 0,
							},
						}
					}),
					gomonkey.ApplyFunc(resourcehelper.PodRequestsAndLimits, func(
						pod *corev1.Pod) (reqs, limits corev1.ResourceList) {
						return corev1.ResourceList{}, nil
					}),
					gomonkey.ApplyFunc(GetPoolManager, func() *PoolMgr {
						return &PoolMgr{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&PoolMgr{}), "GetPoolList",
						func(*PoolMgr) map[standard]*Pool {
							poolList := make(map[standard]*Pool)
							poolList["pool22"] = &Pool{
								PodList: map[nodeName]*NodePod{
									"1": {
										podsQueue: podsObjQueue{},
									},
								},
							}
							return poolList
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patcherFunc()
		n := &NormalPriorityPacking{
			enableTMPExpansion: tt.fields.enableTMPExpansion,
			funcName:           tt.fields.funcName,
		}
		n.scoreNodesAndSelectPod(tt.args.availableNodeList)
		patches.ResetAll()
	}
}

func TestNormalPriorityPacking_selectAvailableNodeList(t *testing.T) {
	type fields struct {
		enableTMPExpansion bool
		funcName           string
	}
	type args struct {
		nodeList          []nodeName
		tmpExpansionNodes map[string]*cache.NodeInfo
		podList           map[nodeName]*NodePod
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		want      map[nodeName][]*corev1.Pod
		patchFunc utils.PatchesFunc
	}{
		{
			"test select available node",
			fields{
				enableTMPExpansion: false,
			},
			args{
				nodeList:          []nodeName{"1", "2"},
				tmpExpansionNodes: map[string]*cache.NodeInfo{},
				podList: map[nodeName]*NodePod{
					"1": {
						readyPodNum: 0,
					},
					"2": {
						podsQueue: podsObjQueue{
							items: []*podObj{
								{
									pod: &corev1.Pod{
										ObjectMeta: metav1.ObjectMeta{Name: "a"},
									},
									usedFlag: false,
								}},
						},
						readyPodNum: 1,
					},
				},
			},
			map[nodeName][]*corev1.Pod{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		n := &NormalPriorityPacking{
			enableTMPExpansion: tt.fields.enableTMPExpansion,
			funcName:           tt.fields.funcName,
		}
		availableNodeList := n.selectAvailableNodeList(tt.args.nodeList, tt.args.tmpExpansionNodes, tt.args.podList)
		if len(availableNodeList) == 0 {
			t.Errorf("get availableNodeList error")
		}
		patches.ResetAll()
	}
}

func Test_getPoolMemoryInNode(t *testing.T) {
	type args struct {
		node nodeName
	}
	tests := []struct {
		name      string
		args      args
		want      int64
		patchFunc utils.PatchesFunc
	}{
		{
			"test get pool memory in node",
			args{
				node: nodeName("1"),
			},
			0,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.IsPodReady, func(pod *corev1.Pod) bool {
						return true
					}),
					gomonkey.ApplyFunc(GetPoolManager, func() *PoolMgr {
						return &PoolMgr{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&PoolMgr{}), "GetPoolList",
						func(*PoolMgr) map[standard]*Pool {
							poolList := make(map[standard]*Pool)
							poolList["pool22"] = &Pool{
								PodList: map[nodeName]*NodePod{
									"1": {},
								},
							}
							return poolList
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		poolMemory := getPoolMemoryInNode(tt.args.node)
		if poolMemory != tt.want {
			t.Errorf("get pool memory error, want %d, get %d", tt.want, poolMemory)
		}
		patches.ResetAll()
	}
}

func Test_getScoreByNodeAndProfiling(t *testing.T) {
	type args struct {
		node             string
		profilingContent *monitoring.ProfilingContent
		podMemory        int64
		podCPU           int64
	}
	tests := []struct {
		name       string
		args       args
		want       float64
		patcheFunc utils.PatchesFunc
	}{
		{
			"test get score with get node status empty",
			args{
				"1",
				nil,
				1,
				1,
			},
			0,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(nodebalance.GetNodesState, func(nodeName string) *nodebalance.NodeStateForPacking {
						return &nodebalance.NodeStateForPacking{
							UsedCPU:       0,
							AllCPU:        2,
							AllMemory:     1000,
							RequestMemory: 500,
						}
					},
					),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("can't get config")
					}),
					gomonkey.ApplyFunc(getPoolMemoryInNode, func(node nodeName) int64 {
						return 0
					}),
				})
				return patches
			},
		},
		{
			"test get score with empty profilinContext",
			args{
				"1",
				nil,
				1,
				1,
			},
			0,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(nodebalance.GetNodesState, func(nodeName string) *nodebalance.NodeStateForPacking {
						return &nodebalance.NodeStateForPacking{
							UsedCPU:       1,
							AllCPU:        1,
							AllMemory:     1,
							RequestMemory: 1,
						}
					},
					),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("can't get config")
					}),
					gomonkey.ApplyFunc(getPoolMemoryInNode, func(node nodeName) int64 {
						return 0
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patcheFunc()
		score := GetScoreByNodeAndProfiling(tt.args.node, tt.args.profilingContent, tt.args.podMemory, tt.args.podCPU)
		if score != tt.want {
			t.Errorf("get score error! want %f, get %f", tt.want, score)
		}
		patches.ResetAll()
	}
}

func Test_getScoreByRatio(t *testing.T) {
	type args struct {
		currentRatio float64
		afterRatio   float64
		exceptRatio  float64
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			"test param is 0",
			args{
				0, 0, 0,
			},
			0,
		},
		{
			"test param calculate",
			args{
				3.5, 2.1, 4,
			},
			-0.35,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			currentScore := getScoreByRatio(tt.args.currentRatio, tt.args.afterRatio, tt.args.exceptRatio)
			if currentScore != tt.want {
				t.Errorf("get score by ratio error: currentScore is %f, want socre is %f", currentScore, tt.want)
			}
		})
	}
}
