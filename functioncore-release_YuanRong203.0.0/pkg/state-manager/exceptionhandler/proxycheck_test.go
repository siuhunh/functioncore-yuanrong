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

// Package exceptionhandler handling exceptions
package exceptionhandler

import (
	"context"
	"encoding/json"
	"errors"
	"functioncore/pkg/common/clusterstatusutil"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/state-manager/resourcemanager"
	"functioncore/pkg/state-manager/scheduler"
	"functioncore/pkg/state-manager/state/instancemanager"
	utils2 "functioncore/pkg/state-manager/utils"
	"functioncore/test/utils"
)

const (
	busKey    = "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/fake_node"
	workerKey = "/sn/workers/business/yrk/tenant/i1fe539427b24702acc11fbb4e134e17/function/function-task/version/$latest/defaultaz/fake_node"
	wrongKey  = "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz"
)

func TestInitProxyChecker(t *testing.T) {
	tests := []struct {
		name        string
		stopChan    chan struct{}
		patchedFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test initAvailableNodeMap error
		{
			"case1",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(initAvailableNodeMap, func() error {
						return errors.New("initAvailableNodeMap error")
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case2",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(initAvailableNodeMap, func() error {
						return errors.New("initAvailableNodeMap error")
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			err := InitProxyChecker(tt.stopChan)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRealTimeMetricsServerAddr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestInitAvailableNodeMap(t *testing.T) {
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test GetResponse error
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{}, errors.New("get response error")
						}),
				})
				return patches
			},
			false,
		},
		// test success
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{
								Kvs: []*mvccpb.KeyValue{
									{Key: []byte("test")},
									{Key: []byte("/a/a/a/a/tenant/a/function/a/version/a/a/a")},
								},
							}, nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			err := initAvailableNodeMap()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetRealTimeMetricsServerAddr() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestIsBusProxyInstance(t *testing.T) {
	convey.Convey(" test isBusProxyInstance", t, func() {
		convey.Convey("success", func() {
			res := isBusProxyInstance(busKey)
			convey.So(res, convey.ShouldEqual, true)
		})
		convey.Convey("fail", func() {
			res := isBusProxyInstance(workerKey)
			convey.So(res, convey.ShouldEqual, false)
		})
	})
}

func TestExtractNodeID(t *testing.T) {
	convey.Convey(" test extractNodeID", t, func() {
		rightEvent := &etcd3.Event{
			Type:      0,
			Key:       busKey,
			Value:     nil,
			PrevValue: nil,
			Rev:       0,
		}
		wrongEvent := &etcd3.Event{
			Type:      0,
			Key:       wrongKey,
			Value:     nil,
			PrevValue: nil,
			Rev:       0,
		}

		convey.Convey("success", func() {
			nodeID, err := extractNodeID(rightEvent)
			convey.So(nodeID, convey.ShouldEqual, "fake_node")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("fail", func() {
			_, err := extractNodeID(wrongEvent)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
	})
}

func TestProcessAddEvent(t *testing.T) {
	convey.Convey(" test processAddEvent", t, func() {
		AddEvent := &etcd3.Event{
			Type:      0,
			Key:       busKey,
			Value:     nil,
			PrevValue: nil,
			Rev:       0,
		}

		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(resourcemanager.GetNodeCPUMilliValue,
				func(nodeID string) int64 {
					return 0
				}),
			gomonkey.ApplyFunc(resourcemanager.GetNodeMemoryMilliValue,
				func(nodeID string) int64 {
					return 0
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()

		convey.Convey("success", func() {
			processAddEvent(AddEvent)
			nodeID, _ := extractNodeID(AddEvent)
			cpu, _ := resourcemanager.GetCPUCapacity(nodeID)
			convey.So(cpu, convey.ShouldEqual, 0)
		})
	})
}

func TestProcessDeleteEvent(t *testing.T) {
	test := []struct {
		name        string
		event       *etcd3.Event
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test extractNodeID return error
		{
			"case1",
			&etcd3.Event{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test extractFunctionName return error
		{
			"case2",
			&etcd3.Event{
				Key: busKey,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(extractFunctionName, func(event *etcd3.Event) (string, error) {
						return "", errors.New("extract function name error")
					}),
				})
				return patches
			},
			true,
		},
		// test processFNTaskOffline return error
		{
			"case3",
			&etcd3.Event{
				Key: busKey,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						if nodeName == "fake_node" {
							return 10, nil
						}
						return 0, errors.New("No valide node")
					}),
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusRunning, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(processFNTaskOffline, func(nodeID string) error {
						return errors.New("task off line error")
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case4",
			&etcd3.Event{
				Key: busKey,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						if nodeName == "fake_node" {
							return 10, nil
						}
						return 0, errors.New("No valide node")
					}),
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusRunning, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(processFNTaskOffline, func(nodeID string) error {
						return nil
					}),
				})
				return patches
			},
			false,
		},
		// test cluster not running
		{
			"case5",
			&etcd3.Event{
				Key: busKey,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						if nodeName == "fake_node" {
							return 10, nil
						}
						return 0, errors.New("No valide node")
					}),
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusRunning, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(processFNTaskOffline, func(nodeID string) error {
						return nil
					}),
				})
				return patches
			},
			false,
		},
		// test node is not in this cluster, GetNodeStateID error
		{
			"case6",
			&etcd3.Event{
				Key: busKey,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						return 0, errors.New("No valide node")
					}),
					gomonkey.ApplyFunc(instancemanager.GetNodeStateID, func(nodeName string) (map[string]struct{}, error) {
						return nil, errors.New("Test error")
					}),
				})
				return patches
			},
			false,
		},
		// test node is not in this cluster, with no states
		{
			"case7",
			&etcd3.Event{
				Key: busKey,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						return 0, errors.New("No valide node")
					}),
					gomonkey.ApplyFunc(instancemanager.GetNodeStateID, func(nodeName string) (map[string]struct{}, error) {
						stateIDMap := make(map[string]struct{}, 0)
						return stateIDMap, nil
					}),
				})
				return patches
			},
			false,
		},
		// test node is not in this cluster, with states
		{
			"case8",
			&etcd3.Event{
				Key: busKey,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						return 0, errors.New("No valide node")
					}),
					gomonkey.ApplyFunc(instancemanager.GetNodeStateID, func(nodeName string) (map[string]struct{}, error) {
						stateIDMap := make(map[string]struct{}, 2)
						stateIDMap["stateID1"] = struct{}{}
						stateIDMap["stateID2"] = struct{}{}
						return stateIDMap, nil
					}),
					gomonkey.ApplyFunc(instancemanager.ConstructTimerForCompetitionOfStateID, func(stateID string, nodeID string) {
						if stateID != "stateID1" && stateID != "stateID2" {
							t.Error("Test mock failed")
						}
						return
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range test {
		patches := tt.patchesFunc()
		err := processDeleteEvent(tt.event)
		if (err != nil) != tt.wantErr {
			t.Errorf("process delete event %s error, err %v , wantErr %v", tt.name, err, tt.wantErr)
		}
		patches.ResetAll()
	}
}

func TestProcessBusproxyEvent(t *testing.T) {
	test := []struct {
		name        string
		event       *etcd3.Event
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test put error
		{
			"case1",
			&etcd3.Event{
				Type: etcd3.PUT,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test delete success
		{
			"case2",
			&etcd3.Event{
				Key:  busKey,
				Type: etcd3.DELETE,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(processFNTaskOffline, func(nodeID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						if nodeName == "fake_node" {
							return 10, nil
						}
						return 0, errors.New("No valide node")
					}),
				})
				return patches
			},
			false,
		},
		// test delete error
		{
			"case3",
			&etcd3.Event{
				Key:  busKey,
				Type: etcd3.DELETE,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(processFNTaskOffline, func(nodeID string) error {
						return errors.New("test err")
					}),
					gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
						if nodeName == "fake_node" {
							return 10, nil
						}
						return 0, errors.New("No valide node")
					}),
				})
				return patches
			},
			true,
		},
		// test type error
		{
			"case4",
			&etcd3.Event{
				Key:  busKey,
				Type: etcd3.ERROR,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		// test default
		{
			"case5",
			&etcd3.Event{
				Type: etcd3.SYNCED,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
	}
	for _, tt := range test {
		patches := tt.patchesFunc()
		err := processBusProxyEvent(tt.event)
		if (err != nil) != tt.wantErr {
			t.Errorf("process delete event %s error, err %v , wantErr %v", tt.name, err, tt.wantErr)
		}
		patches.ResetAll()
	}
}

func TestWatchBusProxy(t *testing.T) {
	test := []struct {
		name       string
		metaClient *etcd3.EtcdClient
		stopChan   chan struct{}
		patchFunc  utils.PatchesFunc
		extFunc    func(*etcd3.EtcdClient, chan struct{})
		wantErr    bool
	}{
		// test result chan close
		{
			"case1",
			&etcd3.EtcdClient{
				Client: &clientv3.Client{},
			},
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
				})
				return patches
			},
			func(cl *etcd3.EtcdClient, c chan struct{}) {
			},
			false,
		},
		// test isEventKeyValid
		{
			"case2",
			&etcd3.EtcdClient{
				Client: &clientv3.Client{},
			},
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
				})
				return patches
			},
			func(cl *etcd3.EtcdClient, c chan struct{}) {
			},
			false,
		},
		// test success
		{
			"case2",
			&etcd3.EtcdClient{
				Client: &clientv3.Client{},
			},
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(
						reflect.TypeOf(&etcd3.EtcdClient{}),
						"Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
					gomonkey.ApplyFunc(processBusProxyEvent, func(event *etcd3.Event) error {
						return errors.New("process bus proxy event error")
					}),
				})
				return patches
			},
			func(cl *etcd3.EtcdClient, c chan struct{}) {
			},
			false,
		},
	}

	for _, tt := range test {
		patches := tt.patchFunc()
		if tt.stopChan != nil {
			go func(stopChan chan struct{}) {
				<-time.After(time.Millisecond * 1)
				close(stopChan)
			}(tt.stopChan)
		}
		go tt.extFunc(tt.metaClient, tt.stopChan)
		watchBusProxy(tt.metaClient, tt.stopChan)
		patches.ResetAll()
	}
}

func Test_updateFunctionInstanceMap(t *testing.T) {
	called := false
	tests := []struct {
		name        string
		event       *etcd3.Event
		patchesFunc utils.PatchesFunc
	}{
		// test parse node id error
		{
			"case1",
			&etcd3.Event{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(
						parseNodeID, func(eventValue []byte) (string, error) {
							called = true
							return "", errors.New("error")
						}),
				})
				return patches
			},
		},
		// test put
		{
			"case2",
			&etcd3.Event{
				Type: etcd3.PUT,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(
						parseNodeID, func(eventValue []byte) (string, error) {
							called = true
							return "", nil
						}),
				})
				return patches
			},
		},
		// test delete
		{
			"case2",
			&etcd3.Event{
				Type: etcd3.DELETE,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(
						parseNodeID, func(eventValue []byte) (string, error) {
							called = true
							return "", nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			patches := tt.patchesFunc()
			updateFunctionInstanceMap(tt.event)
			if called != true {
				t.Errorf("updateFunctionInstanceMap() error")
			}
			patches.ResetAll()
		})
	}
}

func TestProcessDeleteEvent_processFNTaskOffline(t *testing.T) {
	convey.Convey("TestProcessDeleteEvent_processFNTaskOffline", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(extractNodeID, func(event *etcd3.Event) (string, error) {
				return "test", nil
			}),
			gomonkey.ApplyFunc(scheduler.DelAvailableNodes, func(nodeID string) {}),
			gomonkey.ApplyFunc(resourcemanager.ClearNodeMap, func(nodeID string) {}),
			gomonkey.ApplyFunc(extractFunctionName, func(event *etcd3.Event) (string, error) {
				return functiontaskSign, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("GetNodeStateIDErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(instancemanager.GetNodeStateID, func(nodeID string) (map[string]struct{}, error) {
					return nil, errors.New("test")
				}),
				gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
					if nodeName == "test" {
						return 10, nil
					}
					return 0, errors.New("No valide node")
				}),
				gomonkey.ApplyFunc(utils2.GetClusterID, func() string {
					return "runningCluster"
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(processDeleteEvent(&etcd3.Event{}), convey.ShouldBeError)
		})
		convey.Convey("competeStateAndTryToUpdateErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(instancemanager.GetNodeStateID, func(nodeID string) (map[string]struct{}, error) {
					return map[string]struct{}{
						"testStateID1": {},
					}, nil
				}),
				gomonkey.ApplyFunc(instancemanager.CompeteStateAndTryToUpdate, func(stateID string, oldNodeID string) error {
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
					if nodeName == "test" {
						return 10, nil
					}
					return 0, errors.New("No valide node")
				}),
				gomonkey.ApplyFunc(utils2.GetClusterID, func() string {
					return "runningCluster"
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(processDeleteEvent(&etcd3.Event{}), convey.ShouldBeError)
		})
		convey.Convey("processFNTaskOfflineOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(instancemanager.GetNodeStateID, func(nodeID string) (map[string]struct{}, error) {
					return map[string]struct{}{
						"testStateID1": {},
					}, nil
				}),
				gomonkey.ApplyFunc(resourcemanager.GetCPU, func(nodeName string) (int64, error) {
					if nodeName == "test" {
						return 10, nil
					}
					return 0, errors.New("No valide node")
				}),
				gomonkey.ApplyFunc(utils2.GetClusterID, func() string {
					return "runningCluster"
				}),
				gomonkey.ApplyFunc(instancemanager.CompeteStateAndTryToUpdate, func(stateID string, oldNodeID string) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			convey.So(processDeleteEvent(&etcd3.Event{}), convey.ShouldBeNil)
		})
	})
}

func Test_judgeFNTaskOffline(t *testing.T) {
	type args struct {
		event *etcd3.Event
	}
	event1 := &etcd3.Event{
		Type:      0,
		Key:       "1/2/3/4/5/6/7/8/9/10/11/12/13",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	event2 := &etcd3.Event{
		Type:      0,
		Key:       "1/2/3/4/5/6/7/8/9/10/11/12",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	var a args
	var b args
	a.event = event1
	b.event = event2
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"case1", a, "", false},
		{"case2", b, "9", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := judgeFNTaskOffline(tt.args.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("judgeFNTaskOffline() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_parseNodeID(t *testing.T) {
	type args struct {
		eventValue []byte
	}
	value1 := 0
	v1, _ := json.Marshal(value1)
	var a1 args

	value2 := make(map[string]string)
	value2["nodeName"] = ""
	v2, _ := json.Marshal(value2)
	var a2 args

	value3 := make(map[string]string)
	value3["nodeName"] = "nodeName"
	v3, _ := json.Marshal(value3)
	var a3 args

	a1.eventValue = v1
	a2.eventValue = v2
	a3.eventValue = v3
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"case1", a1, "", true},
		{"case2", a2, "", false},
		{"case3", a3, "nodeName", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseNodeID(tt.args.eventValue)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseNodeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseNodeID() got = %v, want %v", got, tt.want)
			}
		})
	}
}
