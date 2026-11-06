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
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/action"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
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
