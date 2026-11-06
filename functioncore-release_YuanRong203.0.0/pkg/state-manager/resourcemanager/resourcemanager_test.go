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

// Package resourcemanager implements methods
// to manager nodes in cluster.
package resourcemanager

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	v1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
	metricsv1beta1 "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1/fake"

	"functioncore/pkg/common/k8sclient"
	"functioncore/pkg/state-manager/config"
	"functioncore/test/utils"
)

func TestGetResourceValid(t *testing.T) {
	nodesMap = map[string]*nodeInfo{
		"hghphicprd19359": {
			nodeName: "hghphicprd19359",
			ready:    true,
			NodeResourceInfo: NodeResourceInfo{
				CPU:            100,
				Memory:         200,
				CPUCapacity:    8000,
				MemoryCapacity: 65536,
				CPURemain:      7990,
				MemoryRemain:   65336,
				NetBandwidth:   0,
			},
		},
		"hghphicprd19360": {
			nodeName: "hghphicprd19360",
			ready:    true,
			NodeResourceInfo: NodeResourceInfo{
				CPU:            300,
				Memory:         400,
				CPUCapacity:    32000,
				MemoryCapacity: 65536,
				CPURemain:      31700,
				MemoryRemain:   65136,
				NetBandwidth:   0,
			},
		},
	}

	if nodes, err := GetAllNodes(); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetAllNodes result = %v", nodes)
	}

	if cpu, err := GetCPU("hghphicprd19359"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetCPU(\"hghphicprd19359\") result = %v", cpu)
	}

	if memory, err := GetMemory("hghphicprd19360"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetMemory(\"hghphicprd19360\") result = %v", memory)
	}

	if cpuCap, err := GetCPUCapacity("hghphicprd19359"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetCPUCapacity(\"hghphicprd19359\") result = %v", cpuCap)
	}

	if memoryCap, err := GetMemoryCapacity("hghphicprd19360"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetMemoryCapacity(\"hghphicprd19360\") result = %v", memoryCap)
	}

	if cpuRemain, err := GetCPURemain("hghphicprd19359"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetCPURemain(\"hghphicprd19359\") result = %v", cpuRemain)
	}

	if memoryRemain, err := GetMemoryRemain("hghphicprd19360"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetMemoryRemain(\"hghphicprd19360\") result = %v", memoryRemain)
	}

	if bandWidth, err := GetNetBandwidth("hghphicprd19360"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetNetBandwidth(\"hghphicprd19360\") result = %v", bandWidth)
	}

	if nodeResource, err := GetAllResource("hghphicprd19360"); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	} else {
		t.Logf("GetAllResource(\"hghphicprd19360\") result = %v", *nodeResource)
	}
}

func TestGetResourceInvalid(t *testing.T) {
	nodesMap = map[string]*nodeInfo{
		"hghphicprd19359": {
			nodeName: "hghphicprd19359",
			ready:    true,
			NodeResourceInfo: NodeResourceInfo{
				CPU:            100,
				Memory:         200,
				CPUCapacity:    8000,
				MemoryCapacity: 65536,
				CPURemain:      7990,
				MemoryRemain:   65336,
				NetBandwidth:   0,
			},
		},
		"hghphicprd19360": {
			nodeName: "hghphicprd19360",
			ready:    true,
			NodeResourceInfo: NodeResourceInfo{
				CPU:            300,
				Memory:         400,
				CPUCapacity:    32000,
				MemoryCapacity: 65536,
				CPURemain:      31700,
				MemoryRemain:   65136,
				NetBandwidth:   0,
			},
		},
	}

	if _, err := GetCPU("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetCPU(\"hghphicprd19370\") err = %v", err)
	}

	if _, err := GetMemory("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetMemory(\"hghphicprd19370\") err = %v", err)
	}

	if _, err := GetCPUCapacity("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetCPUCapacity(\"hghphicprd19370\") err = %v", err)
	}

	if _, err := GetMemoryCapacity("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetMemoryCapacity(\"hghphicprd19370\") err = %v", err)
	}

	if _, err := GetCPURemain("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetCPURemain(\"hghphicprd19370\") err = %v", err)
	}

	if _, err := GetMemoryRemain("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetMemoryRemain(\"hghphicprd19370\") err = %v", err)
	}

	if _, err := GetNetBandwidth("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetNetBandwidth(\"hghphicprd19370\") err = %v", err)
	}

	if _, err := GetAllResource("hghphicprd19370"); err == nil {
		t.Errorf("Expected non nil but got nil")
	} else {
		t.Logf("GetAllResource(\"hghphicprd19370\") err = %v", err)
	}
}

func TestRegisterNodeEvent(t *testing.T) {
	nodeSubscribers = map[NodeEvent][]nodeEventCB{
		NodeOffline: []nodeEventCB{func(interface{}) {}},
		NodeOnline:  []nodeEventCB{func(interface{}) {}},
	}
	if err := RegisterNodeEvent(NodeOffline, func(interface{}) {}); err != nil {
		t.Errorf("Expected nil but got non nil, err = %v", err)
	}
}

// func TestStart(t *testing.T) {
// 	Convey("TestStart", t, func() {
// 		Convey("success", func() {
// 			patch1 := ApplyFunc(rest.InClusterConfig, func() (*rest.Config, error) {
// 				return &rest.Config{
// 					Host: "https://127.0.0.1",
// 				}, nil
// 			})
// 			patch2 := ApplyFunc(kubernetes.NewForConfig, func(c *rest.Config) (*kubernetes.Clientset, error) {
// 				return &kubernetes.Clientset{}, nil
// 			})
// 			patch3 := ApplyFunc(metrics.NewForConfig, func(c *rest.Config) (*metrics.Clientset, error) {
// 				return &metrics.Clientset{}, nil
// 			})
// 			patch4 := ApplyFunc(cache.WaitForCacheSync, func(stopCh <-chan struct{}, cacheSyncs ...cache.InformerSynced) bool {
// 				return true
// 			})
// 			var _ = ApplyFunc(monitorNodes, func(mc *metrics.Clientset, stopChan <-chan struct{}) {
// 				return
// 			})
// 			defer patch1.Reset()
// 			defer patch2.Reset()
// 			defer patch3.Reset()
// 			defer patch4.Reset()
//
// 			stopper := make(chan struct{})
// 			defer close(stopper)
// 			err := Start(stopper)
// 			if err != nil {
// 				t.Errorf("Expected nil but got non nil, err = %v", err)
// 			}
// 		})
// 		Convey("fail branch 1", func() {
// 			patch1 := ApplyFunc(rest.InClusterConfig, func() (*rest.Config, error) {
// 				return nil, errors.New("InClusterConfig fail")
// 			})
// 			defer patch1.Reset()
//
// 			stopper := make(chan struct{})
// 			defer close(stopper)
// 			err := Start(stopper)
// 			if err == nil {
// 				t.Errorf("Expected non nil but got nil")
// 			}
// 		})
// 		Convey("fail branch 2", func() {
// 			patch1 := ApplyFunc(rest.InClusterConfig, func() (*rest.Config, error) {
// 				return &rest.Config{
// 					Host: "https://127.0.0.1",
// 				}, nil
// 			})
// 			patch2 := ApplyFunc(kubernetes.NewForConfig, func(c *rest.Config) (*kubernetes.Clientset, error) {
// 				return nil, errors.New("kubernetes NewForConfig fail")
// 			})
// 			defer patch1.Reset()
// 			defer patch2.Reset()
//
// 			stopper := make(chan struct{})
// 			defer close(stopper)
// 			err := Start(stopper)
// 			if err == nil {
// 				t.Errorf("Expected non nil but got nil")
// 			}
// 		})
// 		Convey("fail branch 3", func() {
// 			patch1 := ApplyFunc(rest.InClusterConfig, func() (*rest.Config, error) {
// 				return &rest.Config{
// 					Host: "https://127.0.0.1",
// 				}, nil
// 			})
// 			patch2 := ApplyFunc(kubernetes.NewForConfig, func(c *rest.Config) (*kubernetes.Clientset, error) {
// 				return &kubernetes.Clientset{}, nil
// 			})
// 			patch3 := ApplyFunc(metrics.NewForConfig, func(c *rest.Config) (*metrics.Clientset, error) {
// 				return nil, errors.New("metrics NewForConfig fail")
// 			})
// 			defer patch1.Reset()
// 			defer patch2.Reset()
// 			defer patch3.Reset()
//
// 			stopper := make(chan struct{})
// 			defer close(stopper)
// 			err := Start(stopper)
// 			if err == nil {
// 				t.Errorf("Expected non nil but got nil")
// 			}
// 		})
// 	})
// }
//
// func TestOnAdd(t *testing.T) {
// 	Convey("TestOnAdd", t, func() {
// 		Convey("success", func() {
// 			onAdd(&corev1.Node{})
// 		})
// 		Convey("fail", func() {
// 			onAdd("hghphicprd19359")
// 		})
// 	})
// }
//
// func TestOnDelete(t *testing.T) {
// 	Convey("TestOnDelete", t, func() {
// 		Convey("success", func() {
// 			onDelete(&corev1.Node{})
// 		})
// 		Convey("fail", func() {
// 			onDelete("hghphicprd19359")
// 		})
// 	})
// }

func Test_updateNodesMap(t *testing.T) {
	called := false
	type args struct {
		mc *metrics.Clientset
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		// test list error
		{
			"case1",
			args{
				&metrics.Clientset{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&metrics.Clientset{}), "MetricsV1beta1",
						func(_ *metrics.Clientset) metricsv1beta1.MetricsV1beta1Interface {
							return &fake.FakeMetricsV1beta1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeNodeMetricses{}), "List",
						func(_ *fake.FakeNodeMetricses, ctx context.Context,
							opts v12.ListOptions) (*v1beta1.NodeMetricsList, error) {
							return &v1beta1.NodeMetricsList{}, errors.New("list error")
						}),
				})
				return patches
			},
			false,
		},
		// test list error
		{
			"case2",
			args{
				&metrics.Clientset{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&metrics.Clientset{}), "MetricsV1beta1",
						func(_ *metrics.Clientset) metricsv1beta1.MetricsV1beta1Interface {
							return &fake.FakeMetricsV1beta1{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&fake.FakeNodeMetricses{}), "List",
						func(_ *fake.FakeNodeMetricses, ctx context.Context,
							opts v12.ListOptions) (*v1beta1.NodeMetricsList, error) {
							return &v1beta1.NodeMetricsList{
								Items: []v1beta1.NodeMetrics{
									{
										ObjectMeta: v12.ObjectMeta{
											Name: "test",
										},
									},
								},
							}, nil
						}),
					gomonkey.ApplyFunc(getNodeReadyStatus, func(nodeID string) bool {
						return true
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			temp := nodesMap
			nodesMap = map[string]*nodeInfo{
				"test": &nodeInfo{},
			}
			patches := tt.patchesFunc()
			updateNodesMap(tt.args.mc)
			if called != tt.calledWant {
				t.Errorf("updateNodesMap() called = %v, calledWant %v", called, tt.calledWant)
			}
			patches.ResetAll()
			nodesMap = temp
		})
	}
}

// TestStart test start function
func TestStart(t *testing.T) {
	convey.Convey("TestStart", t, func() {
		patch := gomonkey.ApplyFunc(monitorNodes, func(mc *metrics.Clientset, stopChan <-chan struct{}) { return })
		defer func() {
			patch.Reset()
		}()
		convey.Convey("TestGetConfigErr", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return nil, errors.New("test err")
			})
			convey.So(Start(make(chan struct{})), convey.ShouldBeError)
			patch.Reset()
		})
		convey.Convey("GetKubeConfigErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{}, nil
				}),
				gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
					return nil, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(Start(make(chan struct{})), convey.ShouldBeError)
		})
		convey.Convey("KubeNewForConfigErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						KubeCfgTimeout: 10,
					}, nil
				}),
				gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
					return &rest.Config{}, nil
				}),
				gomonkey.ApplyFunc(kubernetes.NewForConfig, func(cfg *rest.Config) (*kubernetes.Clientset, error) {
					return nil, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(Start(make(chan struct{})), convey.ShouldBeError)
		})
		convey.Convey("MetricsNewForConfigErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						KubeCfgTimeout: 10,
					}, nil
				}),
				gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
					return &rest.Config{}, nil
				}),
				gomonkey.ApplyFunc(kubernetes.NewForConfig, func(cfg *rest.Config) (*kubernetes.Clientset, error) {
					return &kubernetes.Clientset{}, nil
				}),
				gomonkey.ApplyFunc(metrics.NewForConfig, func(cfg *rest.Config) (*metrics.Clientset, error) {
					return nil, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(Start(make(chan struct{})), convey.ShouldBeError)
		})
		convey.Convey("WaitForCacheSyncNotOK", func() {
			nodesMap = nil
			nodeSubscribers = nil
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						KubeCfgTimeout: 10,
					}, nil
				}),
				gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
					return &rest.Config{}, nil
				}),
				gomonkey.ApplyFunc(kubernetes.NewForConfig, func(cfg *rest.Config) (*kubernetes.Clientset, error) {
					return &kubernetes.Clientset{}, nil
				}),
				gomonkey.ApplyFunc(metrics.NewForConfig, func(cfg *rest.Config) (*metrics.Clientset, error) {
					return &metrics.Clientset{}, nil
				}),
				gomonkey.ApplyFunc(informers.NewSharedInformerFactory, func(client kubernetes.Interface,
					defaultResync time.Duration) informers.SharedInformerFactory {
					return utils.InformerFakeFactory
				}),
				gomonkey.ApplyFunc(runtime.HandleCrash, func(additionalHandlers ...func(interface{})) { return }),
				gomonkey.ApplyFunc(cache.WaitForCacheSync, func(stopCh <-chan struct{},
					cacheSyncs ...cache.InformerSynced) bool {
					return false
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(Start(make(chan struct{})), convey.ShouldBeError)
		})
		convey.Convey("GetNodeInfoListErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						KubeCfgTimeout: 10,
					}, nil
				}),
				gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
					return &rest.Config{}, nil
				}),
				gomonkey.ApplyFunc(kubernetes.NewForConfig, func(cfg *rest.Config) (*kubernetes.Clientset, error) {
					return &kubernetes.Clientset{}, nil
				}),
				gomonkey.ApplyFunc(metrics.NewForConfig, func(cfg *rest.Config) (*metrics.Clientset, error) {
					return &metrics.Clientset{}, nil
				}),
				gomonkey.ApplyFunc(informers.NewSharedInformerFactory, func(client kubernetes.Interface,
					defaultResync time.Duration) informers.SharedInformerFactory {
					return utils.InformerFakeFactory
				}),
				gomonkey.ApplyFunc(runtime.HandleCrash, func(additionalHandlers ...func(interface{})) { return }),
				gomonkey.ApplyFunc(cache.WaitForCacheSync, func(stopCh <-chan struct{},
					cacheSyncs ...cache.InformerSynced) bool {
					return true
				}),
				gomonkey.ApplyFunc(getNodeInfoList, func(nodeInformer v1.NodeInformer) error {
					return errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(Start(make(chan struct{})), convey.ShouldBeError)
		})
		convey.Convey("StartOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						KubeCfgTimeout: 10,
					}, nil
				}),
				gomonkey.ApplyFunc(k8sclient.GetKubeConfig, func() (*rest.Config, error) {
					return &rest.Config{}, nil
				}),
				gomonkey.ApplyFunc(kubernetes.NewForConfig, func(cfg *rest.Config) (*kubernetes.Clientset, error) {
					return &kubernetes.Clientset{}, nil
				}),
				gomonkey.ApplyFunc(metrics.NewForConfig, func(cfg *rest.Config) (*metrics.Clientset, error) {
					return &metrics.Clientset{}, nil
				}),
				gomonkey.ApplyFunc(informers.NewSharedInformerFactory, func(client kubernetes.Interface,
					defaultResync time.Duration) informers.SharedInformerFactory {
					return utils.InformerFakeFactory
				}),
				gomonkey.ApplyFunc(runtime.HandleCrash, func(additionalHandlers ...func(interface{})) { return }),
				gomonkey.ApplyFunc(cache.WaitForCacheSync, func(stopCh <-chan struct{},
					cacheSyncs ...cache.InformerSynced) bool {
					return true
				}),
				gomonkey.ApplyFunc(getNodeInfoList, func(nodeInformer v1.NodeInformer) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(Start(make(chan struct{})), convey.ShouldBeNil)
		})
	})
}

// TestStart_monitorNodes -
func TestStart_monitorNodes(t *testing.T) {
	convey.Convey("TestMonitorNodes", t, func() {
		mc := &metrics.Clientset{}
		monitorNodes(mc, nil)
		checkChan := make(chan bool, 1)
		once := sync.Once{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(updateNodesMap, func(mc *metrics.Clientset) {
				once.Do(func() {
					checkChan <- true
				})
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&time.Ticker{}), "Stop", func(t *time.Ticker) {
				checkChan <- true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		stopCh := make(chan struct{})
		go monitorNodes(mc, stopCh)
		ok := <-checkChan
		convey.So(ok, convey.ShouldBeTrue)
		close(stopCh)
		ok = <-checkChan
		convey.So(ok, convey.ShouldBeTrue)
		close(checkChan)
	})
}

// TestClearNodeMap -
func TestClearNodeMap(t *testing.T) {
	convey.Convey("TestClearNodeMap", t, func() {
		nodesMap = map[string]*nodeInfo{
			"test": {},
		}
		ClearNodeMap("test")
		_, ok := nodesMap["test"]
		convey.So(ok, convey.ShouldBeFalse)
	})
}

// TestOnUpdate -
func TestOnUpdate(t *testing.T) {
	convey.Convey("TestOnUpdate", t, func() {
		convey.Convey("IsNotDataPlaneNode", func() {
			checkFlag := false
			patch := gomonkey.ApplyFunc(isDataPlaneNode, func(node *corev1.Node) bool {
				checkFlag = true
				return false
			})
			onUpdate(&corev1.Node{}, &corev1.Node{})
			convey.So(checkFlag, convey.ShouldBeTrue)
			patch.Reset()
		})
		convey.Convey("TestOnUpdateOk", func() {
			checkFlag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(isDataPlaneNode, func(node *corev1.Node) bool {
					return true
				}),
				gomonkey.ApplyFunc(doUpdateCallback, func(oldNode, newNode *corev1.Node) {
					checkFlag = true
					return
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			onUpdate("", &corev1.Node{})
			onUpdate(&corev1.Node{}, "")
			onUpdate(&corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test1",
				}}, &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test2",
				}})
			onUpdate(&corev1.Node{}, &corev1.Node{})
			convey.So(checkFlag, convey.ShouldBeTrue)
		})
	})
}

// TestDoUpdateCallback -
func TestDoUpdateCallback(t *testing.T) {
	convey.Convey("TestDoUpdateCallback", t, func() {
		addFlag := false
		deleteFlag := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(onAdd, func(obj interface{}) {
				addFlag = true
				return
			}),
			gomonkey.ApplyFunc(onDelete, func(obj interface{}) {
				deleteFlag = true
				return
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		// test del
		doUpdateCallback(
			&corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}},
			&corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{}}},
		)
		convey.So(deleteFlag, convey.ShouldBeTrue)
		// test add
		doUpdateCallback(&corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{}}},
			&corev1.Node{Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{
				Type: corev1.NodeReady, Status: corev1.ConditionTrue}}}})
		convey.So(addFlag, convey.ShouldBeTrue)
	})
}

// TestOnAdd -
func TestOnAdd(t *testing.T) {
	convey.Convey("TestOnAdd", t, func() {
		convey.Convey("IsNotDataPlaneNode", func() {
			checkFlag := false
			patch := gomonkey.ApplyFunc(isDataPlaneNode, func(node *corev1.Node) bool {
				checkFlag = true
				return false
			})
			onAdd(&corev1.Node{})
			convey.So(checkFlag, convey.ShouldBeTrue)
			patch.Reset()
		})
		convey.Convey("TestOnAddOK", func() {
			nodesMap = nil
			nodeSubscribers[NodeOnline] = []nodeEventCB{func(i interface{}) {}}
			checkFlag := false
			patch := gomonkey.ApplyFunc(isDataPlaneNode, func(node *corev1.Node) bool {
				checkFlag = true
				return true
			})
			onAdd("")
			onAdd(&corev1.Node{Status: corev1.NodeStatus{Capacity: corev1.ResourceList{}}})
			convey.So(checkFlag, convey.ShouldBeTrue)
			patch.Reset()
		})
	})
}

// TestOnDelete -
func TestOnDelele(t *testing.T) {
	convey.Convey("TestOnDelete", t, func() {
		convey.Convey("IsNotDataPlaneNode", func() {
			checkFlag := false
			patch := gomonkey.ApplyFunc(isDataPlaneNode, func(node *corev1.Node) bool {
				checkFlag = true
				return false
			})
			onDelete(&corev1.Node{})
			convey.So(checkFlag, convey.ShouldBeTrue)
			patch.Reset()
		})
		convey.Convey("TestOnDeleteOK", func() {
			checkFlag := false
			nodeSubscribers[NodeOnline] = []nodeEventCB{func(i interface{}) {}}
			patch := gomonkey.ApplyFunc(isDataPlaneNode, func(node *corev1.Node) bool {
				checkFlag = true
				return true
			})
			onDelete("")
			onDelete(&corev1.Node{})
			convey.So(checkFlag, convey.ShouldBeTrue)
			patch.Reset()
		})
	})
}

// TestGetNodeInfoList -
func TestGetNodeInfoList(t *testing.T) {
	convey.Convey("TestGetNodeInfoList", t, func() {
		nodeInformer := utils.InformerFakeFactory.Core().V1().Nodes()
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(getNodeReadyStatus, func(nodeID string) bool {
				return true
			}),
			gomonkey.ApplyFunc(isDataPlaneNode, func(node *corev1.Node) bool { return true }),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.So(getNodeInfoList(nodeInformer), convey.ShouldBeNil)
	})
}

// TestIsDataPlaneNode -
func TestIsDataPlaneNode(t *testing.T) {
	convey.Convey("TestIsDataPlaneNode", t, func() {
		convey.Convey("TestGetConfigErr", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return nil, errors.New("test err")
			})
			convey.So(isDataPlaneNode(&corev1.Node{}), convey.ShouldBeFalse)
			patch.Reset()
		})
		convey.Convey("NodeAffinityIsNil", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return &config.FConfig{ScheCfg: config.SchedulerConfig{}}, nil
			})
			convey.So(isDataPlaneNode(&corev1.Node{}), convey.ShouldBeTrue)
			patch.Reset()
		})
		convey.Convey("isNotDataPlaneNode", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return &config.FConfig{ScheCfg: config.SchedulerConfig{
					NodeAffinity: config.NodeAffinity{
						NodeAffinityKey: "key",
						NodeAffinityValue: "value",
					},
				}}, nil
			})
			convey.So(isDataPlaneNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}),
				convey.ShouldBeFalse)
			patch.Reset()
		})
		convey.Convey("isDataPlaneNode", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return &config.FConfig{ScheCfg: config.SchedulerConfig{
					NodeAffinity: config.NodeAffinity{NodeAffinityKey: "node-role", NodeAffinityValue: "data-plane"},
				}}, nil
			})
			convey.So(isDataPlaneNode(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
				"node-role": "data-plane",
			}}}),
				convey.ShouldBeTrue)
			patch.Reset()
		})
	})
}
