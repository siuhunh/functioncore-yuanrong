/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package stateworkershandler -
package main

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/common/workermanagerutils"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/nodemgr"
	bridgemanagerutils "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	"functioncore/pkg/state-manager/config"
	statemanagerutils "functioncore/pkg/state-manager/utils"
	"functioncore/test/utils"
)

func TestLoadStateWorkersFromEtcd(t *testing.T) {
	convey.Convey("Test loadStateWorkersFromEtcd", t, func() {
		etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
		etcdMap := make(map[string]string)
		etcdMap["/sn/stateworkers/business/yrk/tenant/0/function/function-task/version"+
			"/$latest/defaultaz/2d5b759f-b3c6-58cd-97a3-1dc0536a9371"] = "{ \"cluster\": \"cluster001\"}"
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetStateEtcdClient,
				func() *etcd3.EtcdClient {
					return etcdClient
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse",
				func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					kvs := make([]*mvccpb.KeyValue, 0)
					for k, v := range etcdMap {
						kvs = append(kvs, &mvccpb.KeyValue{
							Key:   []byte(k),
							Value: []byte(v),
						})
					}
					resp := &clientv3.GetResponse{
						Kvs: kvs,
					}
					return resp, nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		stateWorkers := loadStateWorkersFromEtcd()
		convey.So(stateWorkers["2d5b759f-b3c6-58cd-97a3-1dc0536a9371"], convey.ShouldNotBeNil)
	})
}

func TestLoadStateWorkersFromEtcdWithError(t *testing.T) {
	convey.Convey("Test state client nil", t, func() {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		stateWorkers := loadStateWorkersFromEtcd()
		convey.So(len(stateWorkers) == 0, convey.ShouldBeTrue)
	})
	convey.Convey("Test state client get response failed", t, func() {
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
					return nil, errors.New("Test Error")
				}),
		})
		stateWorkers := loadStateWorkersFromEtcd()
		convey.So(len(stateWorkers) == 0, convey.ShouldBeTrue)
	})
}
func TestClearIllegalBridgeIDRef(t *testing.T) {
	convey.Convey("Test clearIllegalBridgeIDRef delete illegal bridgeid", t, func() {
		stateWorkers := make(map[string]map[string]string)
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", stateInfo)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		clearIllegalBridgeIDRef(stateWorkers)
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
	})
	convey.Convey("Test clearIllegalBridgeIDRef update cluster ok", t, func() {
		stateWorkers := make(map[string]map[string]string)
		stateWorkers["2d5b759f-b3c6-58cd-97a3-1dc0536a9371"] = map[string]string{"cluster": "cluster002"}
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", stateInfo)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(bridgeCache, "StoreSpecificStateIDInfoToEtcd",
				func(b *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		clearIllegalBridgeIDRef(stateWorkers)
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster002")
	})
	convey.Convey("Test clearIllegalBridgeIDRef update cluster nok", t, func() {
		stateWorkers := make(map[string]map[string]string)
		stateWorkers["2d5b759f-b3c6-58cd-97a3-1dc0536a9371"] = map[string]string{"cluster": "cluster002"}
		bridgeCache := cache.Instance()
		stateInfo := &cache.StateIDInfo{
			BelongCluster: "cluster001",
		}
		bridgeCache.StateIDInfoCache.Store("2d5b759f-b3c6-58cd-97a3-1dc0536a9371", stateInfo)
		patches := []*gomonkey.Patches{
			gomonkey.ApplyMethod(bridgeCache, "StoreSpecificStateIDInfoToEtcd",
				func(b *cache.BridgeCache, stateID string, traceID string) error {
					return errors.New("StoreSpecificStateIDInfoToEtcd failed")
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		clearIllegalBridgeIDRef(stateWorkers)
		convey.So(stateInfo.BelongCluster, convey.ShouldEqual, "cluster001")
	})
}

func TestInitBridgeManager(t *testing.T) {
	convey.Convey("Test InitBridgeManager ok", t, func() {
		bridgeCache := cache.Instance()
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(startWatch, func() error {
				return nil
			}),
			gomonkey.ApplyFunc(loadStateWorkersFromEtcd, func() map[string]map[string]string {
				return map[string]map[string]string{}
			}),
			gomonkey.ApplyFunc(clearIllegalBridgeIDRef, func(stateWorkers map[string]map[string]string) {
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(bridgeCache), "LoadCacheFromEtcd",
				func(b *cache.BridgeCache) error {
					return nil
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(bridgeCache), "LoadClientCiteBridgeInfo",
				func(b *cache.BridgeCache) {
				}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return &config.FConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: false,
					},
					LocalAuth: localauth.AuthConfig{
						AKey:     "akey",
						SKey:     "skey",
						Duration: 1,
					},
				}, nil
			}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		err := InitBridgeManager()
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestApplyStateKey(t *testing.T) {
	convey.Convey("Test ApplyStateKey ok", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ApplyStateKey,
				func(client *bgtypes.ClientInstance, traceID string) (string, bool, error) {
					return "statekey", false, nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		clientIns := &bgtypes.ClientInstance{}
		key, isNew, err := ApplyStateKey(clientIns, "traceID")
		convey.So(key, convey.ShouldEqual, "statekey")
		convey.So(isNew, convey.ShouldBeFalse)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestReleaseBridge(t *testing.T) {
	convey.Convey("Test ApplyStateKey ok", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(action.ReleaseBridge,
				func(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
					return true, nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		clientIns := &bgtypes.ClientInstance{}
		ok, err := ReleaseBridge(clientIns, "traceID")
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestCheckFuncInstances(t *testing.T) {
	convey.Convey("Test check function instances", t, func() {
		convey.Convey("Case1: success", func() {
			clientCallCounts := 0
			bridgeCallCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(loadClusterInfoFromEtcd, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(checkClientInstances, func() {
					clientCallCounts++
				}),
				gomonkey.ApplyFunc(checkBridgeInstances, func() {
					bridgeCallCounts++
				}),
				gomonkey.ApplyFunc(bridgemanagerutils.GetCheckInstancesInterval, func() int {
					return 1
				}),
				gomonkey.ApplyFunc((*nodemgr.Manager).GetNode, func() string {
					return "1.2.3.4"
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			go checkFuncInstances()
			time.Sleep(2 * time.Second)
			convey.ShouldBeTrue(clientCallCounts >= 2)
			convey.ShouldBeTrue(bridgeCallCounts >= 2)
		})
		convey.Convey("Case2: stopCh is nil", func() {
			clientCallCounts := 0
			bridgeCallCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(signals.WaitForSignal, func() <-chan struct{} {
					return nil
				}),
				gomonkey.ApplyFunc(checkClientInstances, func() {
					clientCallCounts++
				}),
				gomonkey.ApplyFunc(checkBridgeInstances, func() {
					bridgeCallCounts++
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			go checkFuncInstances()
			time.Sleep(2 * time.Second)
			convey.So(clientCallCounts, convey.ShouldEqual, 0)
			convey.So(bridgeCallCounts, convey.ShouldEqual, 0)
		})
		convey.Convey("Case3: failed to load cluster info from etcd, nodeIP is empty", func() {
			clientCallCounts := 0
			bridgeCallCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(loadClusterInfoFromEtcd, func() error {
					return errors.New("error")
				}),
				gomonkey.ApplyFunc(checkClientInstances, func() {
					clientCallCounts++
				}),
				gomonkey.ApplyFunc(checkBridgeInstances, func() {
					bridgeCallCounts++
				}),
				gomonkey.ApplyFunc(bridgemanagerutils.GetCheckInstancesInterval, func() int {
					return 1
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			go checkFuncInstances()
			time.Sleep(2 * time.Second)
			convey.So(clientCallCounts, convey.ShouldEqual, 0)
			convey.So(bridgeCallCounts, convey.ShouldEqual, 0)
		})
		convey.Convey("Case4: failed to load cluster info from etcd, nodeIP is not empty", func() {
			clientCallCounts := 0
			bridgeCallCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(loadClusterInfoFromEtcd, func() error {
					return errors.New("error")
				}),
				gomonkey.ApplyFunc(checkClientInstances, func() {
					clientCallCounts++
				}),
				gomonkey.ApplyFunc(checkBridgeInstances, func() {
					bridgeCallCounts++
				}),
				gomonkey.ApplyFunc(bridgemanagerutils.GetCheckInstancesInterval, func() int {
					return 1
				}),
				gomonkey.ApplyFunc((*nodemgr.Manager).GetNode, func() string {
					return "1.2.3.4"
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			go checkFuncInstances()
			time.Sleep(2 * time.Second)
			convey.ShouldBeTrue(clientCallCounts >= 1)
			convey.ShouldBeTrue(bridgeCallCounts >= 1)
		})
	})
}

func TestLoadClusterInfoFromEtcd(t *testing.T) {
	convey.Convey("Test load cluster info from etcd", t, func() {
		convey.Convey("Case1: success", func() {
			etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
			etcdMap := make(map[string]string)
			etcdMap["/sn/worker-manager/instances/cluster001/1.2.3.4/worker-manager-xxx"] = "active"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return etcdClient
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse",
					func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						kvs := make([]*mvccpb.KeyValue, 0)
						for k, v := range etcdMap {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc(statemanagerutils.GetClusterID, func() string {
					return "cluster001"
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			err := loadClusterInfoFromEtcd()
			convey.So(err, convey.ShouldBeNil)
			nodeIP := nodemgr.Instance().GetNode()
			convey.So(nodeIP, convey.ShouldEqual, "1.2.3.4")
			nodemgr.Instance().RemoveNode()
		})
		convey.Convey("Case2: failed to get router etcd client", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			err := loadClusterInfoFromEtcd()
			convey.So(err, convey.ShouldNotBeNil)
			convey.ShouldBeTrue(strings.Contains(err.Error(), "failed to get router etcd client"))
		})
		convey.Convey("Case3: failed to get response from etcd", func() {
			etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return etcdClient
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse",
					func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return nil, errors.New("error")
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			err := loadClusterInfoFromEtcd()
			convey.So(err, convey.ShouldNotBeNil)
			convey.ShouldBeTrue(strings.Contains(err.Error(), "failed to get worker-manager instances from etcd"))
		})
		convey.Convey("Case4: failed to parse etcd key", func() {
			etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
			etcdMap := make(map[string]string)
			etcdMap["error"] = "active"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return etcdClient
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse",
					func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						kvs := make([]*mvccpb.KeyValue, 0)
						for k, v := range etcdMap {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			err := loadClusterInfoFromEtcd()
			convey.So(err, convey.ShouldBeNil)
			nodeIP := nodemgr.Instance().GetNode()
			convey.So(nodeIP, convey.ShouldEqual, "")
		})
		convey.Convey("Case5: clusterID not match", func() {
			etcdClient := &etcd3.EtcdClient{Client: &clientv3.Client{}}
			etcdMap := make(map[string]string)
			etcdMap["/sn/worker-manager/instances/cluster001/1.2.3.4/worker-manager-xxx"] = "active"
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return etcdClient
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse",
					func(w *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						kvs := make([]*mvccpb.KeyValue, 0)
						for k, v := range etcdMap {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
				gomonkey.ApplyFunc(statemanagerutils.GetClusterID, func() string {
					return "cluster002"
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			err := loadClusterInfoFromEtcd()
			convey.So(err, convey.ShouldBeNil)
			nodeIP := nodemgr.Instance().GetNode()
			convey.So(nodeIP, convey.ShouldEqual, "")
		})
	})
}

func TestCheckClientInstances(t *testing.T) {
	convey.Convey("Test check client instances", t, func() {
		clientInstanceKey := "client-pod_tenant_bridge"
		clientInstance := &bgtypes.ClientInstance{
			PodName:  "client-pod",
			FuncName: "client",
			Version:  "latest",
			TenantID: "tenant",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant",
				ServiceID:      "service",
				BridgeFuncName: "bridge",
			},
		}
		convey.Convey("Case1: client instance exists in worker-manager", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
					func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
						return &types.QueryInstance{
							PodsName: []string{"client-pod"},
						}, nil
					}),
				gomonkey.ApplyFunc(action.ProcessRelatedInfo,
					func(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
						callCounts++
						return true, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().ClientPodClientInstCache.Store(clientInstanceKey, clientInstance)
			checkClientInstances()
			convey.So(callCounts, convey.ShouldEqual, 0)
			cache.Instance().ClientPodClientInstCache.Delete(clientInstanceKey)
		})
		convey.Convey("Case2: client instance does not exist in worker-manager", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
					func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
						return &types.QueryInstance{
							PodsName: []string{"other-pod"},
						}, nil
					}),
				gomonkey.ApplyFunc(action.ProcessRelatedInfo,
					func(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
						callCounts++
						return true, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().ClientPodClientInstCache.Store(clientInstanceKey, clientInstance)
			checkClientInstances()
			convey.So(callCounts, convey.ShouldEqual, 1)
			cache.Instance().ClientPodClientInstCache.Delete(clientInstanceKey)
		})
		convey.Convey("Case3: failed to clear client instance", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
					func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
						return &types.QueryInstance{
							PodsName: []string{"other-pod"},
						}, nil
					}),
				gomonkey.ApplyFunc(action.ProcessRelatedInfo,
					func(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
						callCounts++
						return false, errors.New("error")
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().ClientPodClientInstCache.Store(clientInstanceKey, clientInstance)
			checkClientInstances()
			convey.So(callCounts, convey.ShouldEqual, 1)
			cache.Instance().ClientPodClientInstCache.Delete(clientInstanceKey)
		})
		convey.Convey("Case4: failed to convert interface to ClientInstance", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(action.ProcessRelatedInfo,
					func(client *bgtypes.ClientInstance, isCrashScenario bool, traceID string) (bool, error) {
						callCounts++
						return true, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().ClientPodClientInstCache.Store(clientInstanceKey, "error")
			checkClientInstances()
			convey.So(callCounts, convey.ShouldEqual, 0)
			cache.Instance().ClientPodClientInstCache.Delete(clientInstanceKey)
		})
	})
}

func TestGetFunctionInstances(t *testing.T) {
	convey.Convey("Test get function instances", t, func() {
		convey.Convey("Case1: success", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						HTTPSConfig: tls.InternalHTTPSConfig{
							HTTPSEnable: false,
						},
						LocalAuth: localauth.AuthConfig{
							AKey:     "akey",
							SKey:     "skey",
							Duration: 1,
						},
					}, nil
				}),
				gomonkey.ApplyFunc((*nodemgr.Manager).GetNode, func() string {
					return "1.2.3.4"
				}),
				gomonkey.ApplyFunc(workermanagerutils.QueryInstancesFromWmByIP, func(funcSvc string, nodeIP string,
					enableHTTPS bool, auth *localauth.AuthConfig) (types.QueryInstanceResponse, error) {
					return types.QueryInstanceResponse{
						Code: 200,
						Instances: types.QueryInstance{
							ClusterID: "cluster",
							PodsName:  []string{"client-pod"},
						},
					}, nil
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			ins, err := nodemgr.Instance().GetFuncInstances("funcSvc")
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(ins.PodsName), convey.ShouldEqual, 1)
			convey.So(ins.PodsName[0], convey.ShouldEqual, "client-pod")
		})
		convey.Convey("Case2: failed to get config", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return nil, errors.New("failed to get config")
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			_, err := nodemgr.Instance().GetFuncInstances("funcSvc")
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to get config")
		})
		convey.Convey("Case3: failed to query function instances", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						HTTPSConfig: tls.InternalHTTPSConfig{
							HTTPSEnable: false,
						},
						LocalAuth: localauth.AuthConfig{
							AKey:     "akey",
							SKey:     "skey",
							Duration: 1,
						},
					}, nil
				}),
				gomonkey.ApplyFunc((*nodemgr.Manager).GetNode, func() string {
					return "1.2.3.4"
				}),
				gomonkey.ApplyFunc(workermanagerutils.QueryInstancesFromWmByIP, func(funcSvc string, nodeIP string,
					enableHTTPS bool, auth *localauth.AuthConfig) (types.QueryInstanceResponse, error) {
					return types.QueryInstanceResponse{}, errors.New("failed to query")
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			_, err := nodemgr.Instance().GetFuncInstances("funcSvc")
			convey.So(err, convey.ShouldNotBeNil)
			convey.ShouldBeTrue(strings.Contains(err.Error(), "failed to query"))
		})
		convey.Convey("Case4: response code is not OK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						HTTPSConfig: tls.InternalHTTPSConfig{
							HTTPSEnable: false,
						},
						LocalAuth: localauth.AuthConfig{
							AKey:     "akey",
							SKey:     "skey",
							Duration: 1,
						},
					}, nil
				}),
				gomonkey.ApplyFunc((*nodemgr.Manager).GetNode, func() string {
					return "1.2.3.4"
				}),
				gomonkey.ApplyFunc(workermanagerutils.QueryInstancesFromWmByIP, func(funcSvc string, nodeIP string,
					enableHTTPS bool, auth *localauth.AuthConfig) (types.QueryInstanceResponse, error) {
					return types.QueryInstanceResponse{
						Code: 500,
					}, nil
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			ins, err := nodemgr.Instance().GetFuncInstances("funcSvc")
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(ins.PodsName), convey.ShouldEqual, 0)
		})
		convey.Convey("Case5: failed to get function cache", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
					return &config.FConfig{
						HTTPSConfig: tls.InternalHTTPSConfig{
							HTTPSEnable: false,
						},
						LocalAuth: localauth.AuthConfig{
							AKey:     "akey",
							SKey:     "skey",
							Duration: 1,
						},
					}, nil
				}),
				gomonkey.ApplyFunc((*nodemgr.Manager).GetNode, func() string {
					return "1.2.3.4"
				}),
				gomonkey.ApplyFunc(workermanagerutils.QueryInstancesFromWmByIP, func(funcSvc string, nodeIP string,
					enableHTTPS bool, auth *localauth.AuthConfig) (types.QueryInstanceResponse, error) {
					return types.QueryInstanceResponse{}, errors.New("failed to get function cache")
				}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			ins, err := nodemgr.Instance().GetFuncInstances("funcSvc")
			convey.So(err, convey.ShouldBeNil)
			convey.So(len(ins.PodsName), convey.ShouldEqual, 0)
		})
	})
}

func TestCheckBridgeInstances(t *testing.T) {
	convey.Convey("Test check bridge instances", t, func() {
		stateID := "state"
		bridgePodName := "bridge-pod"
		bridgeInstance := &cache.StateIDInfo{
			PodName:       bridgePodName,
			BridgeFuncURN: "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@default@bridge",
		}
		convey.Convey("Case1: bridge instance is correct", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
					func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
						return &types.QueryInstance{
							BridgePodsInfo: map[string]string{stateID: bridgePodName},
						}, nil
					}),
				gomonkey.ApplyFunc(updateBridgeInstance,
					func(stateID string, newPodName string, bridgeInstance *cache.StateIDInfo) {
						callCounts++
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
			checkBridgeInstances()
			convey.So(callCounts, convey.ShouldEqual, 0)
			cache.Instance().StateIDInfoCache.Delete(stateID)
		})
		convey.Convey("Case2: bridge instance is incorrect", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
					func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
						return &types.QueryInstance{
							BridgePodsInfo: map[string]string{stateID: "other-pod"},
						}, nil
					}),
				gomonkey.ApplyFunc(updateBridgeInstance,
					func(stateID string, newPodName string, bridgeInstance *cache.StateIDInfo) {
						callCounts++
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
			checkBridgeInstances()
			convey.So(callCounts, convey.ShouldEqual, 1)
			cache.Instance().StateIDInfoCache.Delete(stateID)
		})
		convey.Convey("Case3: failed to convert interface to string", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(updateBridgeInstance,
					func(stateID string, newPodName string, bridgeInstance *cache.StateIDInfo) {
						callCounts++
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().StateIDInfoCache.Store(true, bridgeInstance)
			checkBridgeInstances()
			convey.So(callCounts, convey.ShouldEqual, 0)
			cache.Instance().StateIDInfoCache.Delete(true)
		})
		convey.Convey("Case4: failed to convert interface to StateIDInfo", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(updateBridgeInstance,
					func(stateID string, newPodName string, bridgeInstance *cache.StateIDInfo) {
						callCounts++
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().StateIDInfoCache.Store(stateID, "error")
			checkBridgeInstances()
			convey.So(callCounts, convey.ShouldEqual, 0)
			cache.Instance().StateIDInfoCache.Delete(stateID)
		})
		convey.Convey("Case5: bridge pods info is nil", func() {
			callCounts := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(nodemgr.Instance()), "GetFuncInstances",
					func(_ *nodemgr.Manager, funcSvc string) (*types.QueryInstance, error) {
						return &types.QueryInstance{}, nil
					}),
				gomonkey.ApplyFunc(updateBridgeInstance,
					func(stateID string, newPodName string, bridgeInstance *cache.StateIDInfo) {
						callCounts++
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
			checkBridgeInstances()
			convey.So(callCounts, convey.ShouldEqual, 0)
			cache.Instance().StateIDInfoCache.Delete(stateID)
		})
	})
}

func TestUpdateBridgeInstance(t *testing.T) {
	convey.Convey("Test update bridge instance", t, func() {
		stateID := "state"
		newPodName := "new-bridge-pod"
		bridgeInstance := &cache.StateIDInfo{
			PodName: "old-bridge-pod",
		}
		convey.Convey("Case1: success", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
					func(_ *cache.BridgeCache, stateID string, traceID string) error {
						return nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
			updateBridgeInstance(stateID, newPodName, bridgeInstance)
			newBridgeInstanceIf, _ := cache.Instance().StateIDInfoCache.Load(stateID)
			newBridgeInstance, _ := newBridgeInstanceIf.(*cache.StateIDInfo)
			convey.So(newBridgeInstance.PodName, convey.ShouldEqual, "new-bridge-pod")
			cache.Instance().StateIDInfoCache.Delete(stateID)
			cache.Instance().StateIDLockMap.Delete(stateID)
		})
		convey.Convey("Case2: failed to store StateIDInfo to etcd", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
					func(_ *cache.BridgeCache, stateID string, traceID string) error {
						return errors.New("error")
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
			updateBridgeInstance(stateID, newPodName, bridgeInstance)
			newBridgeInstanceIf, _ := cache.Instance().StateIDInfoCache.Load(stateID)
			newBridgeInstance, _ := newBridgeInstanceIf.(*cache.StateIDInfo)
			convey.So(newBridgeInstance.PodName, convey.ShouldEqual, "old-bridge-pod")
			cache.Instance().StateIDInfoCache.Delete(stateID)
			cache.Instance().StateIDLockMap.Delete(stateID)
		})
		convey.Convey("Case3: failed to convert interface to Mutex", func() {
			cache.Instance().StateIDInfoCache.Store(stateID, bridgeInstance)
			cache.Instance().StateIDLockMap.Store(stateID, "")
			updateBridgeInstance(stateID, newPodName, bridgeInstance)
			newBridgeInstanceIf, _ := cache.Instance().StateIDInfoCache.Load(stateID)
			newBridgeInstance, _ := newBridgeInstanceIf.(*cache.StateIDInfo)
			convey.So(newBridgeInstance.PodName, convey.ShouldEqual, "old-bridge-pod")
			cache.Instance().StateIDInfoCache.Delete(stateID)
			cache.Instance().StateIDLockMap.Delete(stateID)
		})
	})
}
