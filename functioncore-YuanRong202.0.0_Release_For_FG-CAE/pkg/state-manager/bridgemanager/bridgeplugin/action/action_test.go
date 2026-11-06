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

package action

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/cache"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	"functioncore/pkg/state-manager/state/instancemanager"
	stateutils "functioncore/pkg/state-manager/utils"
	mockUtils "functioncore/test/utils"
)

func TestApplyStateKey(t *testing.T) {
	Convey("Test apply state key entry with assigned stateID", t, func() {
		patch := gomonkey.ApplyFunc(registerAssignStateKey, func(client *bgtypes.ClientInstance, traceID string) (string, bool, error) {
			return "stateKey", true, nil
		})
		clientInstance := &bgtypes.ClientInstance{
			AssignStateID: "exist",
		}
		key, isNewStateKeyScenario, err := ApplyStateKey(clientInstance, "")
		So(isNewStateKeyScenario, ShouldEqual, true)
		So(key, ShouldEqual, "stateKey")
		So(err, ShouldBeNil)

		patch.Reset()
	})
	Convey("Test apply state key entry with assigned stateID", t, func() {
		patch := gomonkey.ApplyFunc(applyStateKey, func(client *bgtypes.ClientInstance, traceID string) (string, bool, error) {
			return "stateKey", false, nil
		})
		clientInstance := &bgtypes.ClientInstance{}
		key, isNewStateKeyScenario, err := ApplyStateKey(clientInstance, "")
		So(key, ShouldEqual, "stateKey")
		So(err, ShouldBeNil)
		So(isNewStateKeyScenario, ShouldEqual, false)
		patch.Reset()
	})
}

func Test_applyStateKey(t *testing.T) {
	clientInstance := &bgtypes.ClientInstance{
		PodName: "test",
		FuncInfo: &bgtypes.BridgeFuncInfo{
			TenantID:       "tenant1",
			ServiceID:      "0@base",
			BridgeFuncName: "rds",
		},
	}
	Convey("Test apply new state key no available one and judge reach max failed", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "", &cache.StateIDInfo{PodName: "test"}, false
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).IsReachMaxStateKeyCount,
				func(_ *cache.BridgeCache, functionURN string, traceID string) (bool, error) {
					return true, errors.New("judge reach failed")
				}),
		})
		key, isNewStateKeyScenario, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err, ShouldNotBeNil)
		So(isNewStateKeyScenario, ShouldEqual, false)
		So(err.Error(), ShouldEqual, "judge reach failed")
		patches.ResetAll()
	})
	Convey("Test apply new state key no available one and reach max", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "", &cache.StateIDInfo{PodName: "test"}, false
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).IsReachMaxStateKeyCount,
				func(_ *cache.BridgeCache, functionURN string, traceID string) (bool, error) {
					return true, nil
				}),
		})
		key, _, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "apply stateKey failed, traceID: test, client: test")
		patches.ResetAll()
	})
	Convey("Test apply new state key no available one so generate new one but record failed", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "", &cache.StateIDInfo{PodName: "test"}, false
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).IsReachMaxStateKeyCount,
				func(_ *cache.BridgeCache, functionURN string, traceID string) (bool, error) {
					return false, nil
				}),
			gomonkey.ApplyFunc(recordStateIDInfo, func(stateInfo *bgtypes.StateIDInfo, functionURN string, traceID string) error {
				return errors.Errorf("recordStateIDInfo failed")
			}),
		})
		key, _, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "recordStateIDInfo failed")
		patches.ResetAll()
	})
	Convey("Test apply new state key available one but persist cache failed", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "", &cache.StateIDInfo{StateKey: "stateKey1", PodName: "test"}, true
				}),
			gomonkey.ApplyFunc(persistCacheInApply, func(client *bgtypes.ClientInstance, stateID string, traceID string) error {
				return errors.Errorf("persistCacheInApply failed")
			}),
			gomonkey.ApplyFunc((*cache.BridgeCache).GetRestrictByURN,
				func(_ *cache.BridgeCache, functionURN string) (int, int, error) {
					return 10, 10, nil
				}),
		})
		key, _, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "persistCacheInApply failed")
		patches.ResetAll()
	})
	Convey("Test getClientInsPodLock failed", t, func() {
		b := cache.Instance()
		b.ClientPodNameLockMap.Store(clientInstance.PodName, sync.Mutex{})
		key, _, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err.Error(), ShouldEqual, "[bridgeplugin] failed to convert an interface to a lock, traceID: test,"+
			" client podName: test, bridgeFuncName: rds")
	})
	Convey("Test getClientInsPodLock failed", t, func() {
		b := cache.Instance()
		b.ClientPodNameLockMap.Store(clientInstance.PodName, &sync.Mutex{})
		b.StateIDLockMap.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", sync.Mutex{})
		patches := mockUtils.InitPatchSlice()
		cache.Instance().StateWorkersCache["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = true
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "cf00203d-c4bd-52a9-adb5-d624349bffcf", &cache.StateIDInfo{StateKey: "stateKey1", PodName: "test"}, true
				}),
		})
		key, _, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err.Error(), ShouldEqual, "[bridgeplugin] failed to convert an interface to a lock, client pod: test, traceID: test")
		patches.ResetAll()
	})
	Convey("Test RegisterClientInsBridgeFunUseInfo failed", t, func() {
		b := cache.Instance()
		b.ClientPodNameLockMap.Store(clientInstance.PodName, &sync.Mutex{})
		b.StateIDLockMap.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", &sync.Mutex{})
		patches := mockUtils.InitPatchSlice()
		cache.Instance().StateWorkersCache["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = true
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).RegisterClientInsBridgeFunUseInfo,
				func(_ *cache.BridgeCache, client *bgtypes.ClientInstance, stateID string, changeCount int,
					traceID string) error {
					return errors.New("test")
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "cf00203d-c4bd-52a9-adb5-d624349bffcf", &cache.StateIDInfo{StateKey: "stateKey", PodName: "podName"}, true
				}),
		})
		key, _, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err.Error(), ShouldEqual, "test")
		patches.ResetAll()
	})
	Convey("Test RegisterClientInsBridgeFunUseInfo failed", t, func() {
		b := cache.Instance()
		funcUseInfo := make(map[string]int)
		funcUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 1
		clientInstance := &bgtypes.ClientInstance{
			PodName: "test",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funcUseInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance), clientInstance)
		b.ClientPodNameLockMap.Store(clientInstance.PodName, &sync.Mutex{})
		b.StateIDLockMap.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", &sync.Mutex{})
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", 0)
		patches := mockUtils.InitPatchSlice()
		cache.Instance().StateWorkersCache["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = true
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).GetClientInfoFromCache,
				func(_ *cache.BridgeCache, client *bgtypes.ClientInstance, traceID string) (*bgtypes.ClientInstance, bool) {
					return nil, false
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).GetRestrictByURN,
				func(_ *cache.BridgeCache, functionURN string) (int, int, error) {
					return 10, 10, nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "cf00203d-c4bd-52a9-adb5-d624349bffcf", &cache.StateIDInfo{StateKey: "stateKey", PodName: "podName"}, true
				}),
		})
		key, _, err := applyStateKey(clientInstance, "test")
		So(key, ShouldEqual, "")
		So(err.Error(), ShouldEqual, "[bridgeplugin] client instance info of podName: test is not exist, traceID: test")
		clientInst, ok := b.ClientPodClientInstCache.Load(utils.GetClientCacheInfoKey(clientInstance))
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo[clientInst.(*bgtypes.ClientInstance).AssignStateID], ShouldEqual, 1)
		So(ok, ShouldBeTrue)
		_, ok = b.StateIDInfoCache.Load(clientInst.(*bgtypes.ClientInstance).AssignStateID)
		So(ok, ShouldBeFalse)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInstance))
		patches.ResetAll()
	})
	Convey("Test apply an illegal stateID, release it and apply a new one", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "stateId", &cache.StateIDInfo{StateKey: "stateKey", PodName: "pool-xxx"}, true
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).IsReachMaxStateKeyCount,
				func(_ *cache.BridgeCache, functionURN string, traceID string) (bool, error) {
					return false, nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).GetClientInfoFromCache,
				func(b *cache.BridgeCache, client *bgtypes.ClientInstance,
					traceID string) (*bgtypes.ClientInstance, bool) {
					return &bgtypes.ClientInstance{}, true
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).RegisterClientInsBridgeFunUseInfo,
				func(b *cache.BridgeCache, client *bgtypes.ClientInstance, stateID string,
					changeCount int, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(recordStateIDInfo,
				func(stateInfo *bgtypes.StateIDInfo, functionURN string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(persistCacheInApply, func(client *bgtypes.ClientInstance,
				stateID string, traceID string) error {
				return nil
			}),
		})
		defer patches.ResetAll()
		key, isNewStateKeyScenario, err := applyStateKey(clientInstance, "test")
		So(key, ShouldNotEqual, "")
		So(err, ShouldBeNil)
		So(isNewStateKeyScenario, ShouldEqual, true)
	})
	Convey("Test apply stateID when checkpoint", t, func() {
		clientInstance.PodName = "checkpoint-xxxx"
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).PickAvailableStateKey,
				func(_ *cache.BridgeCache, bridgeFuncURN string, traceID string) (string, *cache.StateIDInfo, bool) {
					return "stateId", &cache.StateIDInfo{StateKey: "stateKey", PodName: "pool-xxx"}, true
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).IsReachMaxStateKeyCount,
				func(_ *cache.BridgeCache, functionURN string, traceID string) (bool, error) {
					return false, nil
				}),
		})
		defer patches.ResetAll()
		key, isNewStateKeyScenario, err := applyStateKey(clientInstance, "test")
		So(key, ShouldNotEqual, "")
		So(err, ShouldBeNil)
		So(isNewStateKeyScenario, ShouldEqual, true)
	})
}

func Test_recordStateIDInfo(t *testing.T) {
	Convey("Test recordStateIDInfo", t, func() {
		b := cache.Instance()
		b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
			RefCount: 5,
		})
		patch := gomonkey.ApplyFunc((*cache.BridgeCache).GetRestrictByURN,
			func(_ *cache.BridgeCache, functionURN string) (int, int, error) {
				return 10, 10, nil
			})
		Convey("Test record with existed stateID", func() {
			stateInfo := &bgtypes.StateIDInfo{StateID: "ID1", StateKey: "Key1"}
			err := recordStateIDInfo(stateInfo, "testURN", "test")
			getStateIDInfo, _ := b.GetStateIDInfoFromCache("ID1", "test")
			So(err, ShouldBeNil)
			So(getStateIDInfo.RefCount, ShouldEqual, 6)
			So(getStateIDInfo.MaxBgInstQua, ShouldEqual, 10)
			So(getStateIDInfo.MaxActiveQua, ShouldEqual, 10)
		})
		Convey("Test record with non-existed stateID", func() {
			stateInfo := &bgtypes.StateIDInfo{StateID: "ID2", StateKey: "Key1"}
			err := recordStateIDInfo(stateInfo, "testURN", "test")
			getStateIDInfo, _ := b.GetStateIDInfoFromCache("ID2", "test")
			So(err, ShouldBeNil)
			So(getStateIDInfo.RefCount, ShouldEqual, 1)
			So(getStateIDInfo.MaxBgInstQua, ShouldEqual, 10)
			So(getStateIDInfo.MaxActiveQua, ShouldEqual, 10)
		})
		patch.Reset()
	})

	Convey("Test recordStateIDInfo failed", t, func() {
		b := cache.Instance()
		b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
			RefCount: 5,
		})
		patch := gomonkey.ApplyFunc((*cache.BridgeCache).GetRestrictByURN,
			func(_ *cache.BridgeCache, functionURN string) (int, int, error) {
				return 10, 10, errors.New("error")
			})
		Convey("Test record with existed stateID", func() {
			stateInfo := &bgtypes.StateIDInfo{StateID: "ID1", StateKey: "Key1"}
			err := recordStateIDInfo(stateInfo, "testURN", "test")
			_, _ = b.GetStateIDInfoFromCache("ID1", "test")
			So(err, ShouldNotBeNil)
			assert.Equal(t, "error", err.Error())
		})
		patch.Reset()
	})
}

func Test_persistCacheInApply(t *testing.T) {
	Convey("Test StoreSpecificStateIDInfoCache failed", t, func() {
		b := cache.Instance()
		b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
			RefCount: 2,
		})
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return errors.New("store failed")
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).GetRestrictByURN,
				func(_ *cache.BridgeCache, functionURN string) (int, int, error) {
					return 10, 10, nil
				}),
		})
		Convey("Case1 availableExist is true", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName: "test",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant11",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			err := persistCacheInApply(clientInstance, "ID1", "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "store failed")
			getStateIDInfo, _ := b.GetStateIDInfoFromCache("ID1", "test")
			So(getStateIDInfo, ShouldNotBeNil)
			So(getStateIDInfo.RefCount, ShouldEqual, 1)
			_, ok := b.GetClientInfoFromCache(clientInstance, "test")
			So(ok, ShouldBeFalse)
		})
		patches.ResetAll()
	})
	Convey("Test StoreSpecificClientInstCache failed", t, func() {
		b := cache.Instance()
		clientInstance := &bgtypes.ClientInstance{
			PodName: "testPod",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance), clientInstance)
		patches := mockUtils.InitPatchSlice()
		patches = append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return errors.New("store failed")
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD, func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
				return nil
			}),
			gomonkey.ApplyFunc((*cache.BridgeCache).RollbackUpdateStateIDInfoToEtcd, func(_ *cache.BridgeCache, stateID string, traceID string) error {
				return nil
			}),
		})

		err := persistCacheInApply(&bgtypes.ClientInstance{
			PodName: "testPod",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}, "ID1", "test")
		So(err, ShouldNotBeNil)
		_, ok := b.GetClientInfoFromCache(clientInstance, "test")
		So(ok, ShouldBeTrue)
		_, ok = b.GetStateIDInfoFromCache("ID1", "test")
		So(ok, ShouldBeFalse)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInstance))
		patches.ResetAll()
	})
	Convey("Test StoreSpecificClientInstCache success", t, func() {
		b := cache.Instance()
		b.ClientPodClientInstCache.Store("test_tenant1_Pod", &bgtypes.ClientInstance{})
		patch1 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
			func(_ *cache.BridgeCache, stateID string, traceID string) error {
				return nil
			})
		patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
			func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
				return nil
			})
		err := persistCacheInApply(&bgtypes.ClientInstance{
			PodName: "testPod",
		}, "ID1", "test")
		_, ok := b.ClientPodClientInstCache.Load("test_tenant1_Pod")
		So(err, ShouldBeNil)
		So(ok, ShouldBeTrue)
		b.ClientPodClientInstCache.Delete("test_tenant1_Pod")
		patch1.Reset()
		patch2.Reset()
	})
}

func Test_registerAssignStateKey(t *testing.T) {
	Convey("Test register assigned state key", t, func() {
		Convey("case1 client have registered", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName:       "clientPod1",
				AssignStateID: "ID1",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant1",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			b := cache.Instance()
			b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance), clientInstance)
			b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
				RefCount: 5,
			})
			patch := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return errors.New("store failed")
				})
			_, isNewStateKeyScenario, err := registerAssignStateKey(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(isNewStateKeyScenario, ShouldEqual, false)
			b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInstance))
			patch.Reset()
		})
		Convey("case2 assigned ID not exist", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName:       "clientPod2",
				AssignStateID: "Not exist",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant1",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			b := cache.Instance()
			b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
				RefCount: 1,
			})
			patch := gomonkey.ApplyFunc(utils.GetValueFromMetaEtcd, func(funcKey string) ([]byte, error) {
				return make([]byte, 0), nil
			})
			_, isNewStateKeyScenario, err := registerAssignStateKey(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(isNewStateKeyScenario, ShouldEqual, false)
			So(err.Error(), ShouldEqual, "[bridgeplugin] failed to get function configuration, please check configuration")
			patch.Reset()
		})
		Convey("case3 StoreSpecificStateIDInfoCache failed", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName:       "clientPod3",
				AssignStateID: "ID3",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant1",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			b := cache.Instance()
			b.StateIDInfoCache.Store("ID3", &cache.StateIDInfo{
				RefCount: 1,
			})
			patch := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return errors.New("store failed")
				})
			_, isNewStateKeyScenario, err := registerAssignStateKey(clientInstance, "test")
			So(isNewStateKeyScenario, ShouldEqual, false)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "store failed")
			getStateIDInfo, _ := b.StateIDInfoCache.Load("ID3")
			So(getStateIDInfo.(*cache.StateIDInfo).RefCount, ShouldEqual, 1)
			patch.Reset()
		})
		Convey("case3 StoreSpecificClientInstCache failed", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName:       "clientPod4",
				AssignStateID: "ID4",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant1",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			b := cache.Instance()
			b.StateIDInfoCache.Store("ID4", &cache.StateIDInfo{
				RefCount: 1,
			})
			patch1 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				})
			patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return errors.New("store failed")
				})
			patch3 := gomonkey.ApplyFunc((*cache.BridgeCache).RollbackUpdateStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				})
			_, isNewStateKeyScenario, err := registerAssignStateKey(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(isNewStateKeyScenario, ShouldEqual, false)
			So(err.Error(), ShouldEqual, "store failed")
			getStateIDInfo, _ := b.StateIDInfoCache.Load("ID4")
			So(getStateIDInfo.(*cache.StateIDInfo).RefCount, ShouldEqual, 1)
			patch1.Reset()
			patch2.Reset()
			patch3.Reset()
		})
		Convey("case4 getClientInsPodLock failed", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName:       "clientPod4",
				AssignStateID: "ID4",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant1",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			b := cache.Instance()
			b.StateIDInfoCache.Store("ID4", &cache.StateIDInfo{
				RefCount: 1,
			})
			b.ClientPodNameLockMap.Store(clientInstance.PodName, sync.Mutex{})
			_, isNewStateKeyScenario, err := registerAssignStateKey(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(isNewStateKeyScenario, ShouldEqual, false)
			So(err.Error(), ShouldEqual, "[bridgeplugin] failed to convert an interface to a lock, traceID: test, client podName: clientPod4, bridgeFuncName: rds")
			getStateIDInfo, _ := b.StateIDInfoCache.Load("ID4")
			So(getStateIDInfo.(*cache.StateIDInfo).RefCount, ShouldEqual, 1)
		})
		Convey("case5 getStateIDLock failed", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName:       "clientPod4",
				AssignStateID: "ID4",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant1",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			b := cache.Instance()
			b.StateIDInfoCache.Store("ID4", &cache.StateIDInfo{
				RefCount: 1,
			})
			b.ClientPodNameLockMap.Store(clientInstance.PodName, &sync.Mutex{})
			b.StateIDLockMap.Store("ID4", sync.Mutex{})
			_, isNewStateKeyScenario, err := registerAssignStateKey(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(isNewStateKeyScenario, ShouldEqual, false)
			So(err.Error(), ShouldEqual, "[bridgeplugin] failed to convert an interface to a lock, client pod: clientPod4, traceID: test")
			getStateIDInfo, _ := b.StateIDInfoCache.Load("ID4")
			So(getStateIDInfo.(*cache.StateIDInfo).RefCount, ShouldEqual, 1)
		})
		Convey("case6 UpdateClientInsBridgeFunUseInfo failed", func() {
			clientInstance := &bgtypes.ClientInstance{
				PodName:       "clientPod4",
				AssignStateID: "ID4",
				FuncInfo: &bgtypes.BridgeFuncInfo{
					TenantID:       "tenant1",
					ServiceID:      "0@base",
					BridgeFuncName: "rds",
				},
			}
			b := cache.Instance()
			b.StateIDInfoCache.Store("ID4", &cache.StateIDInfo{
				RefCount: 1,
			})
			b.ClientPodNameLockMap.Store(clientInstance.PodName, &sync.Mutex{})
			b.StateIDLockMap.Store("ID4", &sync.Mutex{})
			patches := mockUtils.InitPatchSlice()
			patches = append(mockUtils.PatchSlice{gomonkey.ApplyFunc((*cache.BridgeCache).UpdateClientInsBridgeFunUseInfo, func(_ *cache.BridgeCache, client *bgtypes.ClientInstance, changeCount int,
				traceID string) error {
				return errors.New("error")
			}),
				gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd, func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				})},
				gomonkey.ApplyFunc((*cache.BridgeCache).RollbackUpdateStateIDInfoToEtcd, func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
			)
			_, isNewStateKeyScenario, err := registerAssignStateKey(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(isNewStateKeyScenario, ShouldEqual, false)
			So(err.Error(), ShouldEqual, "error")
			getStateIDInfo, _ := b.StateIDInfoCache.Load("ID4")
			So(getStateIDInfo.(*cache.StateIDInfo).RefCount, ShouldEqual, 1)
			patches.ResetAll()
		})
	})
}

func TestReleaseBridgeWhenSuicide_Concurrently(t *testing.T) {
	Convey("test release info when bridge suicide concurrently ", t, func(c C) {
		bridgeInstance := &bgtypes.BridgeInstance{
			PodName: "bridgePod",
		}
		b := cache.Instance()
		b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
			PodName: "pod1",
		})
		b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
			PodName: "pod2",
		})
		b.StateIDInfoCache.Store("ID3", &cache.StateIDInfo{
			PodName: "pod3",
		})
		b.StateIDInfoCache.Store("ID4", &cache.StateIDInfo{
			PodName: "pod4",
		})
		b.StateIDInfoCache.Store("ID5", &cache.StateIDInfo{
			PodName: "pod5",
		})
		b.StateIDInfoCache.Store("ID6", &cache.StateIDInfo{
			PodName: "pod6",
		})
		b.StateIDInfoCache.Store("ID7", &cache.StateIDInfo{
			PodName: "pod7",
		})
		b.StateIDInfoCache.Store("ID8", &cache.StateIDInfo{
			PodName: "pod8",
		})
		b.StateIDInfoCache.Store("ID9", &cache.StateIDInfo{
			PodName: "pod9",
		})
		b.StateIDInfoCache.Store("ID10", &cache.StateIDInfo{
			PodName: "pod10",
		})
		b.StateIDInfoCache.Store("ID11", &cache.StateIDInfo{
			PodName: "pod11",
		})
		quoteFuncUseInfo := make(map[string]int)
		quoteFuncUseInfo["ID1"] = 1
		quoteFuncUseInfo["ID2"] = 2
		quoteFuncUseInfo["ID3"] = 3
		quoteFuncUseInfo["ID4"] = 4
		quoteFuncUseInfo["ID5"] = 5
		quoteFuncUseInfo["ID6"] = 6
		quoteFuncUseInfo["ID7"] = 7
		quoteFuncUseInfo["ID8"] = 8
		quoteFuncUseInfo["ID9"] = 9
		quoteFuncUseInfo["ID10"] = 10
		quoteFuncUseInfo["ID11"] = 11
		clientInstance := &bgtypes.ClientInstance{
			PodName:       "pod1",
			AssignStateID: "ID1",
			FuncUseInfo:   quoteFuncUseInfo,
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance), clientInstance)

		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return errors.New("store failed")
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
				return nil
			}),
			gomonkey.ApplyFunc((*cache.BridgeCache).UpdateSpecificClientInstToEtcd, func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
				return nil
			}),
			gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD, func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
				return nil
			}),
			gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
		})
		defer patches.ResetAll()
		ch := make(chan bool, 10)
		for i := 1; i < 10; i++ {
			go func(c C, i int) {
				err := ReleaseBridgeWhenSuicide(bridgeInstance, fmt.Sprintf("%s%d", "ID", i), "test")
				if err != nil {
					fmt.Printf("ReleaseBridgeWhenSuicide index: %d, error: %s", i, err.Error())
				}
				ch <- true
			}(c, i)
		}
		for i := 1; i < 10; i++ {
			<-ch
		}
		clientInst, ok := b.ClientPodClientInstCache.Load(utils.GetClientCacheInfoKey(clientInstance))
		So(ok, ShouldBeTrue)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID1"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID2"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID3"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID4"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID5"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID6"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID7"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID8"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID9"], ShouldEqual, 0)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID10"], ShouldEqual, 10)
		So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID11"], ShouldEqual, 11)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInstance))
	})
}

func TestReleaseBridgeWhenSuicide(t *testing.T) {
	Convey("Test Release Bridge When Bridge func crash", t, func() {
		Convey("Test Release Bridge existed When Bridge func crash", func() {
			bridgeInstance := &bgtypes.BridgeInstance{
				PodName: "bridgePod",
			}
			patch := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
				func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
					return nil
				})
			defer patch.Reset()
			Convey("Case1 release state info to the specific stateId", func() {
				b := cache.Instance()
				b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
					PodName: "oldBridgePod",
				})
				err := ReleaseBridgeWhenSuicide(bridgeInstance, "ID1", "test")
				So(err, ShouldBeNil)
				_, ok := b.StateIDInfoCache.Load("ID1")
				So(ok, ShouldBeFalse)
			})
			Convey("Case2 release state info and client quoted info to the specific stateId when clientInstance quoted one BridgeInstance", func() {
				b := cache.Instance()
				b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
					PodName: "oldBridgePod",
				})
				quoteFuncUseInfo := make(map[string]int)
				quoteFuncUseInfo["ID1"] = 5
				b.ClientPodClientInstCache.Store("pod1_tenant1_rds", &bgtypes.ClientInstance{
					PodName:       "pod1",
					AssignStateID: "ID1",
					FuncUseInfo:   quoteFuncUseInfo,
					FuncInfo: &bgtypes.BridgeFuncInfo{
						TenantID:       "tenant1",
						ServiceID:      "0@base",
						BridgeFuncName: "rds",
					},
				})

				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return errors.New("store failed")
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return nil
					}),
				})
				defer patches.ResetAll()
				err := ReleaseBridgeWhenSuicide(bridgeInstance, "ID1", "test")
				So(err, ShouldBeNil)
				_, ok := b.ClientPodClientInstCache.Load("pod1_tenant1_rds")
				So(ok, ShouldBeFalse)
			})
			Convey("Case3 release state info and client quoted info to the specific stateId when clientInstance quoted two BridgeInstance", func() {
				b := cache.Instance()
				b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
					PodName: "oldBridgePod1",
				})
				b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
					PodName: "oldBridgePod2",
				})
				quoteFuncUseInfo := make(map[string]int)
				quoteFuncUseInfo["ID1"] = 5
				quoteFuncUseInfo["ID2"] = 1
				b.ClientPodClientInstCache.Store("pod1_tenant1_rds", &bgtypes.ClientInstance{
					PodName:       "pod1",
					AssignStateID: "ID1",
					FuncUseInfo:   quoteFuncUseInfo,
					FuncInfo: &bgtypes.BridgeFuncInfo{
						TenantID:       "tenant1",
						ServiceID:      "0@base",
						BridgeFuncName: "rds",
					},
				})

				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return nil
					}),
					gomonkey.ApplyFunc((*cache.BridgeCache).UpdateSpecificClientInstToEtcd, func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
						return nil
					}),
				})
				defer patches.ResetAll()
				err := ReleaseBridgeWhenSuicide(bridgeInstance, "ID1", "test")
				So(err, ShouldBeNil)
				clientInst, ok := b.ClientPodClientInstCache.Load("pod1_tenant1_rds")
				So(ok, ShouldBeTrue)
				So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID1"], ShouldEqual, 0)
				So(clientInst.(*bgtypes.ClientInstance).FuncUseInfo["ID2"], ShouldEqual, 1)
				b.ClientPodClientInstCache.Delete("pod1_tenant1_rds")
			})

			Convey("Case4 release state info and client quoted info to the specific stateId when two clientInstance quoted two BridgeInstance", func() {
				b := cache.Instance()
				b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
					PodName: "oldBridgePod1",
				})
				b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
					PodName: "oldBridgePod2",
				})
				quoteFuncUseInfo := make(map[string]int)
				quoteFuncUseInfo["ID1"] = 5
				quoteFuncUseInfo["ID2"] = 2
				b.ClientPodClientInstCache.Store("pod1_tenant1_rds", &bgtypes.ClientInstance{
					PodName:       "pod1",
					AssignStateID: "ID1",
					FuncUseInfo:   quoteFuncUseInfo,
					FuncInfo: &bgtypes.BridgeFuncInfo{
						TenantID:       "tenant1",
						ServiceID:      "0@base",
						BridgeFuncName: "rds",
					},
				})

				quoteFuncUseInfo2 := make(map[string]int)
				quoteFuncUseInfo2["ID1"] = 5
				quoteFuncUseInfo2["ID2"] = 1
				b.ClientPodClientInstCache.Store("pod2_tenant1_rds", &bgtypes.ClientInstance{
					PodName:       "pod2",
					AssignStateID: "ID1",
					FuncUseInfo:   quoteFuncUseInfo2,
					FuncInfo: &bgtypes.BridgeFuncInfo{
						TenantID:       "tenant1",
						ServiceID:      "0@base",
						BridgeFuncName: "rds",
					},
				})

				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return nil
					}),
					gomonkey.ApplyFunc((*cache.BridgeCache).UpdateSpecificClientInstToEtcd, func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
						return nil
					}),
				})
				defer patches.ResetAll()
				err := ReleaseBridgeWhenSuicide(bridgeInstance, "ID1", "test")
				So(err, ShouldBeNil)
				clientInst1, ok := b.ClientPodClientInstCache.Load("pod1_tenant1_rds")
				So(ok, ShouldBeTrue)
				So(clientInst1.(*bgtypes.ClientInstance).FuncUseInfo["ID1"], ShouldEqual, 0)
				So(clientInst1.(*bgtypes.ClientInstance).FuncUseInfo["ID2"], ShouldEqual, 2)

				clientInst2, ok := b.ClientPodClientInstCache.Load("pod2_tenant1_rds")
				So(ok, ShouldBeTrue)
				So(clientInst2.(*bgtypes.ClientInstance).FuncUseInfo["ID1"], ShouldEqual, 0)
				So(clientInst2.(*bgtypes.ClientInstance).FuncUseInfo["ID2"], ShouldEqual, 1)
				b.ClientPodClientInstCache.Delete("pod1_tenant1_rds")
				b.ClientPodClientInstCache.Delete("pod2_tenant1_rds")
			})
		})
	})
}

func TestReleaseBridgeWhenSuicideErrorScene(t *testing.T) {
	Convey("Test Release Bridge Error Scene When Bridge func crash", t, func() {
		Convey("Test delete StateInfo failed When Bridge func crash", func() {
			bridgeInstance := &bgtypes.BridgeInstance{
				PodName: "bridgePod",
			}

			patch := gomonkey.ApplyFunc(doReleaseStateInfo,
				func(stateID string, traceID string) error { return errors.New("doReleaseStateInfo failed") })
			defer patch.Reset()
			err := ReleaseBridgeWhenSuicide(bridgeInstance, "ID1", "test")
			So(err.Error(), ShouldEqual, "doReleaseStateInfo failed")
		})
	})
}

func TestNewBridge(t *testing.T) {
	Convey("Test new bridge", t, func() {
		Convey("Test nil bridge instance", func() {
			var bridgeInstance *bgtypes.BridgeInstance
			err := NewBridge(bridgeInstance, "ID1", "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] bridgeInstance is nil when NewBridge, stateID: ID1, traceID: test")
		})
		Convey("Test bridge instance exist", func() {
			bridgeInstance := &bgtypes.BridgeInstance{
				PodName: "bridgePod",
			}
			b := cache.Instance()
			b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
				PodName: "oldBridgePod",
			})
			Convey("Case1 stateID not exist", func() {
				err := NewBridge(bridgeInstance, "ID not exist", "test")
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "[bridgeplugin] UpdateBridgeFuncPodCache stateID: ID not exist does not exist, traceID: test, client: bridgePod")
			})
			Convey("Case2 stateID exist but store failed", func() {
				patch := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
					func(_ *cache.BridgeCache, stateID string, traceID string) error {
						return errors.New("store failed")
					})
				err := NewBridge(bridgeInstance, "ID1", "test")
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "store failed")
				getStateIDInfo, _ := b.GetStateIDInfoFromCache("ID1", "test")
				So(getStateIDInfo.PodName, ShouldEqual, "oldBridgePod")
				patch.Reset()
			})
			Convey("Case3 stateID exist and store successfully", func() {
				patch := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
					func(_ *cache.BridgeCache, stateID string, traceID string) error {
						return nil
					})
				err := NewBridge(bridgeInstance, "ID1", "test")
				So(err, ShouldBeNil)
				getStateIDInfo, _ := b.GetStateIDInfoFromCache("ID1", "test")
				So(getStateIDInfo.PodName, ShouldEqual, "bridgePod")
				patch.Reset()
			})
		})
	})
}

func Test_deletePersistentState(t *testing.T) {
	Convey("Test delete persistent state", t, func() {
		tenantID := "tenant1"
		serviceID := "0@base"
		functionName := "rds"
		stateID := "ID1"
		Convey("Test DeleteStateInRedis failed", func() {
			patch := gomonkey.ApplyFunc(utils.DeleteStateInRedis,
				func(tenantID string, serviceID string, functionName string, stateID string) (int, error) {
					return 0, errors.New("delete redis record failed")
				})
			err := deletePersistentState(tenantID, serviceID, functionName, stateID, "test")
			So(err, ShouldNotBeNil)
			patch.Reset()
		})
		Convey("Test DeleteStateIDHandlerByManager failed", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.DeleteStateInRedis,
					func(tenantID string, serviceID string, functionName string, stateID string) (int, error) {
						return 0, nil
					}),
				gomonkey.ApplyFunc(instancemanager.DeleteStateIDHandlerByManager,
					func(req *stateutils.Request) error {
						return errors.New("delete state failed")
					}),
			})
			err := deletePersistentState(tenantID, serviceID, functionName, stateID, "test")
			So(err, ShouldNotBeNil)
			patches.ResetAll()
		})
		Convey("Test successful", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.DeleteStateInRedis,
					func(tenantID string, serviceID string, functionName string, stateID string) (int, error) {
						return 0, nil
					}),
				gomonkey.ApplyFunc(instancemanager.DeleteStateIDHandlerByManager,
					func(req *stateutils.Request) error {
						return nil
					}),
			})
			err := deletePersistentState(tenantID, serviceID, functionName, stateID, "test")
			So(err, ShouldBeNil)
			patches.ResetAll()
		})
	})
}

func Test_doReleaseStateInfo(t *testing.T) {
	Convey("Test execute release", t, func() {
		b := cache.Instance()
		b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{})
		Convey("case1 stateID not exist", func() {
			err := doReleaseStateInfo("Not exist", "test")
			So(err, ShouldBeNil)
		})
		Convey("case2 stateID exist but type unmatched", func() {
			b.StateIDInfoCache.Store("ID2", "unmatched")
			err := doReleaseStateInfo("ID2", "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] Failed to get stateIDInfo, stateID: ID2, traceID: test")
		})
		Convey("case3 stateID exist but DeleteSpecificStateIDInfoETCD failed", func() {
			patch := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
				func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
					return errors.New("delete stateIDInfo failed")
				})
			err := doReleaseStateInfo("ID1", "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "delete stateIDInfo failed")
			patch.Reset()
		})
		Convey("Test whether crash scenario", func() {
			Convey("Case4 crash scenario but deletePersistentState failed", func() {
				patch1 := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
					func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
						return nil
					})
				patch2 := gomonkey.ApplyFunc(deletePersistentState,
					func(tenantID string, serviceID string, functionName string, stateID string, traceID string) error {
						return errors.New("delete persistent state failed")
					})
				err := doReleaseStateInfo("ID1", "test")
				So(err, ShouldBeNil)
				patch1.Reset()
				patch2.Reset()
			})
			Convey("Case4 not crash scenario", func() {
				patch := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
					func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
						return nil
					})
				err := doReleaseStateInfo("ID1", "test")
				_, ok := b.GetStateIDInfoFromCache("ID1", "test")
				So(err, ShouldBeNil)
				So(ok, ShouldBeFalse)
				patch.Reset()
			})
		})
	})
}

func TestReleaseBridge(t *testing.T) {
	Convey("Test release bridge delete failed", t, func() {
		b := cache.Instance()
		b.StateIDInfoCache.Store("ID1", &cache.StateIDInfo{
			RefCount: 1,
		})
		b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
			RefCount: 2,
		})
		funcInfo := &bgtypes.BridgeFuncInfo{
			TenantID:       "tenant1",
			ServiceID:      "0@base",
			BridgeFuncName: "rds",
		}
		funcUseInfo := make(map[string]int)
		funcUseInfo["ID1"] = 5
		clientInstance1 := &bgtypes.ClientInstance{
			PodName:       "clientPod1",
			AssignStateID: "ID1",
			FuncInfo:      funcInfo,
			FuncUseInfo:   funcUseInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance1), clientInstance1)
		funcUseInfo1 := make(map[string]int)
		funcUseInfo1["ID2"] = 5
		clientInstance2 := &bgtypes.ClientInstance{
			PodName:       "clientPod2",
			AssignStateID: "ID2",
			FuncInfo:      funcInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance2), clientInstance2)
		Convey("Case1 ReleaseBridge return false nil", func() {
			patch := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
				func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
					return errors.New("delete failed")
				})
			isNeedRelease, err := ReleaseBridge(clientInstance1, false, "")
			So(isNeedRelease, ShouldBeTrue)
			So(err.Error(), ShouldEqual, "delete failed")
			patch.Reset()
		})

		Convey("Test whether need release", func() {
			patch := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				})
			Convey("Test need release", func() {
				Convey("Case2 no need release but doReleaseStateInfo", func() {
					patch2 := gomonkey.ApplyFunc(doReleaseStateInfo,
						func(stateID string, traceID string) error {
							return errors.New("doReleaseStateInfo failed")
						})
					isNeedRelease, err := ReleaseBridge(clientInstance1, false, "")
					So(isNeedRelease, ShouldBeTrue)
					So(err, ShouldNotBeNil)
					patch2.Reset()
				})
				Convey("Case3 need release and do successfully", func() {
					patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						})
					patch3 := gomonkey.ApplyFunc((*cache.BridgeCache).UpdateSpecificClientInstToEtcd, func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
						return nil
					})
					patch4 := gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
						func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
							return nil
						})
					isNeedRelease, err := ReleaseBridge(clientInstance1, false, "")
					So(isNeedRelease, ShouldBeTrue)
					So(err, ShouldBeNil)
					_, ok := b.GetStateIDInfoFromCache(clientInstance1.AssignStateID, "test")
					So(ok, ShouldBeFalse)
					patch2.Reset()
					patch3.Reset()
					patch4.Reset()
				})
			})
			Convey("Test stateID not related with current client", func() {
				Convey("Case4 no need release but UpdateRefCountCache failed", func() {
					patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).UpdateRefCountCacheAndDeleteWhenRefZero,
						func(_ *cache.BridgeCache, stateID string, changeCount int, isDeleteEtcd bool, traceID string) error {
							return errors.New("update refCount failed")
						})
					isNeedRelease, err := ReleaseBridge(clientInstance2, false, "")
					So(isNeedRelease, ShouldBeFalse)
					So(err, ShouldBeNil)
					patch2.Reset()
				})

				funcUseInfo1 := make(map[string]int)
				funcUseInfo1["ID2"] = 5
				clientInstance2 := &bgtypes.ClientInstance{
					PodName:       "clientPod2",
					AssignStateID: "ID2",
					FuncInfo:      funcInfo,
					FuncUseInfo:   funcUseInfo1,
				}
				b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance2), clientInstance2)
				Convey("Case5 no need release but StoreSpecificStateIDInfoCache failed", func() {
					patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return errors.New("store failed")
						})
					isNeedRelease, err := ReleaseBridge(clientInstance2, false, "")
					So(isNeedRelease, ShouldBeFalse)
					So(err.Error(), ShouldEqual, "store failed")
					getStateIDInfo, _ := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
					So(getStateIDInfo.RefCount, ShouldEqual, 2)
					patch2.Reset()
				})
				Convey("Case6 no need release successfully", func() {
					patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						})
					patch3 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						})
					isNeedRelease, err := ReleaseBridge(clientInstance2, false, "")
					So(isNeedRelease, ShouldBeFalse)
					So(err, ShouldBeNil)
					getStateIDInfo, _ := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
					So(getStateIDInfo.RefCount, ShouldEqual, 1)
					patch2.Reset()
					patch3.Reset()
				})
				Convey("Case7 crash scene no need delete successfully", func() {
					patch2 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						})
					patch3 := gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						})
					patch4 := gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return errors.New("error")
					})
					isNeedRelease, err := ReleaseBridge(clientInstance2, true, "")
					So(isNeedRelease, ShouldBeFalse)
					So(err, ShouldBeNil)
					getStateIDInfo, _ := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
					So(getStateIDInfo.RefCount, ShouldEqual, -3)
					_, ok := b.GetClientInfoFromCache(clientInstance2, "test")
					So(ok, ShouldBeFalse)
					patch2.Reset()
					patch3.Reset()
					patch4.Reset()
				})
			})

			funcUseInfo1 := make(map[string]int)
			funcUseInfo1["ID2"] = 5
			clientInstance2 := &bgtypes.ClientInstance{
				PodName:       "clientPod2",
				AssignStateID: "ID2",
				FuncInfo:      funcInfo,
				FuncUseInfo:   funcUseInfo1,
			}
			b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance2), clientInstance2)
			b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
				RefCount: 5,
			})
			Convey("Case8 crash scene need delete failed", func() {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return errors.New("error")
					}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
						func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).RollbackUpdateStateIDInfoToEtcd, func(_ *cache.BridgeCache, stateID string, traceID string) error {
						return nil
					}),
				})
				isNeedRelease, err := ReleaseBridge(clientInstance2, true, "")
				So(isNeedRelease, ShouldBeTrue)
				So(err.Error(), ShouldEqual, "error")
				_, ok := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
				So(ok, ShouldBeFalse)
				getClientInst, ok := b.GetClientInfoFromCache(clientInstance2, "test")
				So(ok, ShouldBeTrue)
				So(getClientInst.FuncUseInfo[clientInstance2.AssignStateID], ShouldEqual, 5)
				b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInstance2))
				patches.ResetAll()
			})

			funcUseInfo1 = make(map[string]int)
			funcUseInfo1["ID2"] = 5
			clientInstance2 = &bgtypes.ClientInstance{
				PodName:       "clientPod2",
				AssignStateID: "ID2",
				FuncInfo:      funcInfo,
				FuncUseInfo:   funcUseInfo1,
			}
			b.ClientPodClientInstCache.Store("clientPod2_tenant1_rds", clientInstance2)
			b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
				RefCount: 5,
			})
			Convey("Case8 crash scene need delete successfully", func() {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
						func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return nil
					}),
				})
				isNeedRelease, err := ReleaseBridge(clientInstance2, true, "")
				So(isNeedRelease, ShouldBeTrue)
				So(err, ShouldBeNil)
				_, ok := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
				So(ok, ShouldBeFalse)
				_, ok = b.GetClientInfoFromCache(clientInstance2, "test")
				So(ok, ShouldBeFalse)
				patches.ResetAll()
			})

			funcUseInfo1 = make(map[string]int)
			funcUseInfo1["ID2"] = 5
			clientInstance2 = &bgtypes.ClientInstance{
				PodName:       "clientPod2",
				AssignStateID: "ID2",
				FuncInfo:      funcInfo,
				FuncUseInfo:   funcUseInfo1,
			}
			b.ClientPodClientInstCache.Store("clientPod2_tenant1_rds", clientInstance2)
			b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
				RefCount: 5,
			})
			Convey("Case9 crash scene", func() {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
						func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return nil
					}),
				})
				isNeedRelease, err := ReleaseBridge(clientInstance2, true, "")
				So(isNeedRelease, ShouldBeTrue)
				So(err, ShouldBeNil)
				_, ok := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
				So(ok, ShouldBeFalse)
				_, ok = b.GetClientInfoFromCache(clientInstance2, "test")
				So(ok, ShouldBeFalse)
				patches.ResetAll()
			})

			funcUseInfo1 = make(map[string]int)
			funcUseInfo1["ID2"] = 5
			clientInstance2 = &bgtypes.ClientInstance{
				PodName:       "clientPod2",
				AssignStateID: "ID2",
				FuncInfo:      funcInfo,
				FuncUseInfo:   funcUseInfo1,
			}
			b.ClientPodClientInstCache.Store("clientPod2_tenant1_rds", clientInstance2)
			b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
				RefCount: 5,
			})
			Convey("Case10 crash do deletePersistentState successfully", func() {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
						func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).RollbackUpdateStateIDInfoToEtcd, func(_ *cache.BridgeCache, stateID string, traceID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return nil
					}),
				})
				isNeedRelease, err := ReleaseBridge(clientInstance2, true, "")
				So(isNeedRelease, ShouldBeTrue)
				So(err, ShouldEqual, nil)
				getStateIDInfo, ok := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
				So(ok, ShouldBeFalse)
				So(getStateIDInfo, ShouldEqual, nil)
				getClientInstInfo, ok := b.GetClientInfoFromCache(clientInstance2, "test")
				So(ok, ShouldBeFalse)
				So(getClientInstInfo, ShouldEqual, nil)
				patches.ResetAll()
			})

			funcUseInfo1 = make(map[string]int)
			funcUseInfo1["ID2"] = 5
			clientInstance2 = &bgtypes.ClientInstance{
				PodName:       "clientPod2",
				AssignStateID: "ID2",
				FuncInfo:      funcInfo,
				FuncUseInfo:   funcUseInfo1,
			}
			b.ClientPodClientInstCache.Store("clientPod2_tenant1_rds", clientInstance2)
			b.StateIDInfoCache.Store("ID2", &cache.StateIDInfo{
				RefCount: 5,
			})
			Convey("Case11 crash do doReleaseClientInfo failed", func() {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
						func(_ *cache.BridgeCache, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
						func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
						func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*cache.BridgeCache).RollbackUpdateStateIDInfoToEtcd, func(_ *cache.BridgeCache, stateID string, traceID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(deletePersistentState, func(tenantID, serviceID, functionName, stateID string, traceID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(doReleaseClientInfo, func(clientInstInfo *bgtypes.ClientInstance, traceID string) error {
						return errors.New("error")
					}),
				})
				isNeedRelease, err := ReleaseBridge(clientInstance2, true, "")
				So(isNeedRelease, ShouldBeTrue)
				So(err, ShouldEqual, nil)
				getStateIDInfo, ok := b.GetStateIDInfoFromCache(clientInstance2.AssignStateID, "test")
				So(ok, ShouldBeFalse)
				So(getStateIDInfo, ShouldEqual, nil)
				getClientInstInfo, ok := b.GetClientInfoFromCache(clientInstance2, "test")
				So(ok, ShouldBeFalse)
				So(getClientInstInfo, ShouldEqual, nil)
				patches.ResetAll()
			})
			patch.Reset()
		})

		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInstance1))
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInstance2))
	})
}

func Test_ApplyNewStateKey_Concurrently(t *testing.T) {
	Convey("test apply new state key concurrently", t, func(c C) {
		b := cache.Instance()
		b.StateIDInfoCache.Range(func(key, value interface{}) bool {
			stateIDIf, _ := key.(string)
			b.StateIDInfoCache.Delete(stateIDIf)
			return true
		})
		b.StateIDInfoCache.Store("25e450fc-dabd-51d2-85ae-6dc5eede702d", getTestStateIDInfo())
		b.StateWorkersCache["25e450fc-dabd-51d2-85ae-6dc5eede702d"] = true
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(persistCacheInApply,
				func(client *bgtypes.ClientInstance, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).GetRestrictByURN,
				func(_ *cache.BridgeCache, functionURN string) (int, int, error) {
					return 20, 20, nil
				}),
		})
		defer patches.ResetAll()
		ch := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			go func(c C) {
				clientInstance := getTestClientInstance()
				key, _, err := ApplyStateKey(clientInstance, uuid.New().String())
				c.So(key, ShouldEqual, "stateKey1")
				c.So(err, ShouldBeNil)
				ch <- true
			}(c)
		}
		for i := 0; i < 10; i++ {
			<-ch
		}
		getStateIDInfo, _ := b.GetStateIDInfoFromCache("cf00203d-c4bd-52a9-adb5-d624349bffcf", "test")
		So(getStateIDInfo.RefCount, ShouldEqual, 10)
	})
}

func Test_registerAssignStateKey_Concurrently(t *testing.T) {
	Convey("test register assign state key concurrently", t, func(c C) {
		b := cache.Instance()
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", getTestStateIDInfo())
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
		})
		ch := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			index := i
			go func(c C) {
				clientInstance := getTestClientInstance()
				clientInstance.PodName = fmt.Sprintf("ClientPod_%d", index)
				clientInstance.AssignStateID = "cf00203d-c4bd-52a9-adb5-d624349bffcf"
				key, _, err := ApplyStateKey(clientInstance, "")
				c.So(key, ShouldEqual, "")
				c.So(err, ShouldBeNil)
				ch <- true
			}(c)
		}
		for i := 0; i < 10; i++ {
			<-ch
		}
		getStateIDInfo, _ := b.GetStateIDInfoFromCache("cf00203d-c4bd-52a9-adb5-d624349bffcf", "test")
		So(getStateIDInfo.RefCount, ShouldEqual, 11)
		patches.ResetAll()
	})
}

func Test_ReleaseBridge_Concurrently(t *testing.T) {
	Convey("Test release bridge concurrently", t, func(c C) {
		b := cache.Instance()
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", stateIDInfo)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificStateIDInfoETCD,
				func(_ *cache.BridgeCache, stateIDInfo *cache.StateIDInfo, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).UpdateSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
		})
		defer patches.ResetAll()
		ch := make(chan bool, 10)
		for i := 0; i < 10; i++ {
			index := i
			go func(c C) {
				clientInstance := getTestClientInstance()
				clientInstance.PodName = fmt.Sprintf("ClientPod_%d", index)
				clientInstance.AssignStateID = "cf00203d-c4bd-52a9-adb5-d624349bffcf"
				b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInstance), clientInstance)
				isNeedRelease, err := ReleaseBridge(clientInstance, false, "")
				c.So(err, ShouldBeNil)
				ch <- isNeedRelease
			}(c)
		}
		needReleaseCount := 0
		for i := 0; i < 10; i++ {
			v, _ := <-ch
			if v == true {
				needReleaseCount++
			}
		}
		So(needReleaseCount, ShouldEqual, 1)
		_, ok := b.GetStateIDInfoFromCache("cf00203d-c4bd-52a9-adb5-d624349bffcf", "test")
		So(ok, ShouldBeFalse)
		_, ok = b.StateIDLockMap.Load("cf00203d-c4bd-52a9-adb5-d624349bffcf")
		So(ok, ShouldBeFalse)
	})
}

func getTestStateIDInfo() *cache.StateIDInfo {
	return &cache.StateIDInfo{
		StateKey:      "stateKey1",
		PodName:       "Pod1",
		RefCount:      1,
		BridgeFuncURN: "sn:cn:yrk:tenant1:function:0@base@rds",
		MaxBgInstQua:  20,
		MaxActiveQua:  20,
	}
}

func getTestClientInstance() *bgtypes.ClientInstance {
	funUseInfo := make(map[string]int)
	funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 1
	return &bgtypes.ClientInstance{
		PodName: "ClientPod",
		FuncInfo: &bgtypes.BridgeFuncInfo{
			TenantID:       "tenant1",
			ServiceID:      "0@base",
			BridgeFuncName: "rds",
		},
		FuncUseInfo: funUseInfo,
	}
}

func Test_doUpdateCacheAndEtcdInfo(t *testing.T) {
	Convey("Test doUpdateCacheAndEtcdInfo successfully", t, func(c C) {
		b := cache.Instance()
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", stateIDInfo)

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 1
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funUseInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
		})
		err := doUpdateCacheAndEtcdInfo(clientInst, "test", 1, false)
		So(err, ShouldBeNil)
		stateIDInfo, ok := b.GetStateIDInfoFromCache(clientInst.AssignStateID, "test")
		So(ok, ShouldBeTrue)
		So(stateIDInfo.RefCount, ShouldEqual, 9)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInst))
		patches.ResetAll()
	})

	Convey("Test Test StoreSpecificStateIDInfoToEtcd failed", t, func(c C) {
		b := &cache.BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", stateIDInfo)

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 1
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funUseInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return errors.New("error")
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
		})

		err := doUpdateCacheAndEtcdInfo(clientInst, "test", 1, false)
		So(err, ShouldNotBeNil)
		stateIDInfo, ok := b.GetStateIDInfoFromCache(clientInst.AssignStateID, "test")
		So(ok, ShouldBeTrue)
		So(stateIDInfo.RefCount, ShouldEqual, 10)
		clientInstInfo, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeTrue)
		So(clientInstInfo.FuncUseInfo[clientInstInfo.AssignStateID], ShouldEqual, 1)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInst))
		patches.ResetAll()
	})
	Convey("Test Test StoreSpecificStateIDInfoToEtcd failed", t, func(c C) {
		b := &cache.BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", stateIDInfo)

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 1
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funUseInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).StoreSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return errors.New("error")
				}),
			gomonkey.ApplyFunc((*cache.BridgeCache).RollbackUpdateStateIDInfoToEtcd,
				func(_ *cache.BridgeCache, stateID string, traceID string) error {
					return nil
				}),
		})

		err := doUpdateCacheAndEtcdInfo(clientInst, "test", 1, false)
		So(err, ShouldNotBeNil)
		stateIDInfo, ok := b.GetStateIDInfoFromCache(clientInst.AssignStateID, "test")
		So(ok, ShouldBeTrue)
		So(stateIDInfo.RefCount, ShouldEqual, 10)
		clientInstInfo, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeTrue)
		So(clientInstInfo.FuncUseInfo[clientInstInfo.AssignStateID], ShouldEqual, 1)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInst))
		patches.ResetAll()
	})
}

func Test_doReleaseClientInfo(t *testing.T) {
	Convey("Test doReleaseClientInfo successfully", t, func(c C) {
		b := cache.Instance()
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", stateIDInfo)

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 1
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funUseInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).UpdateSpecificClientInstToEtcd,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
		})

		err := doReleaseClientInfo(clientInst, "test")
		So(err, ShouldBeNil)
		ClientInsInfo, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeTrue)
		So(ClientInsInfo.FuncUseInfo[ClientInsInfo.AssignStateID], ShouldEqual, 1)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInst))
		patches.ResetAll()
	})

	Convey("Test clientInstInfo.FuncUseInfo is nil when invoke doReleaseClientInfo", t, func(c C) {
		b := cache.Instance()
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", stateIDInfo)

		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
		})

		err := doReleaseClientInfo(clientInst, "test")
		So(err, ShouldBeNil)
		_, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeFalse)
		patches.ResetAll()
	})
	Convey("Test clientInstInfo.FuncUseInfo is nil and utils.IsClientInsFuncUseInfoEmpty is true", t, func(c C) {
		b := cache.Instance()
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10
		b.StateIDInfoCache.Store("cf00203d-c4bd-52a9-adb5-d624349bffcf", stateIDInfo)

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 0
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funUseInfo,
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*cache.BridgeCache).DeleteSpecificClientInfoETCD,
				func(_ *cache.BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return nil
				}),
		})

		err := doReleaseClientInfo(clientInst, "test")
		So(err, ShouldBeNil)
		_, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeFalse)
		patches.ResetAll()
	})
}

func Test_UpdateRefCountCache(t *testing.T) {
	Convey("Test UpdateRefCountCache when changeCount is negative", t, func(c C) {
		b := &cache.BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 10
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funUseInfo,
		}
		err := b.UpdateRefCountCache(clientInst.AssignStateID, -1, "test")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] Minus refCount not allow nonexistent stateID, stateID cf00203d-c4bd-52a9-adb5-d624349bffcf, traceID test")
	})
}

func Test_UpdateClientInsBridgeFunUseInfo(t *testing.T) {
	Convey("Test UpdateClientInsBridgeFunUseInfo when clientInstInfo.FuncUseInfo is nil", t, func(c C) {
		b := &cache.BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 10
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		err := b.UpdateClientInsBridgeFunUseInfo(clientInst, 1, "test")
		So(err, ShouldBeNil)
		getClientInst, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeTrue)
		So(getClientInst.FuncUseInfo[clientInst.AssignStateID], ShouldEqual, 1)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInst))
	})

	Convey("Test UpdateClientInsBridgeFunUseInfo when clientInst not exist in  ClientPodClientInstCache cache", t, func(c C) {
		b := &cache.BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 10
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod111",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}

		err := b.UpdateClientInsBridgeFunUseInfo(clientInst, -1, "test")
		So(err, ShouldNotBeNil)
		_, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeFalse)
		So(err.Error(), ShouldEqual, "[bridgeplugin] Minus refCount not allow unexisted PodName, traceID test, client ClientPod111")
	})
}

func Test_RegisterClientInsBridgeFunUseInfo(t *testing.T) {
	Convey("Test RegisterClientInsBridgeFunUseInfo when clientInstInfo.FuncUseInfo is nil", t, func(c C) {
		b := &cache.BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 10
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}
		b.ClientPodClientInstCache.Store(utils.GetClientCacheInfoKey(clientInst), clientInst)
		err := b.RegisterClientInsBridgeFunUseInfo(clientInst, "cf00203d-c4bd-52a9-adb5-d624349bffcf", 1, "test")
		So(err, ShouldBeNil)
		getClientInst, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeTrue)
		So(getClientInst.FuncUseInfo[clientInst.AssignStateID], ShouldEqual, 1)
		b.ClientPodClientInstCache.Delete(utils.GetClientCacheInfoKey(clientInst))
	})

	Convey("Test RegisterClientInsBridgeFunUseInfo when clientInst not exist in  ClientPodClientInstCache cache", t, func(c C) {
		b := &cache.BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		stateIDInfo.RefCount = 10

		funUseInfo := make(map[string]int)
		funUseInfo["cf00203d-c4bd-52a9-adb5-d624349bffcf"] = 10
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod111",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}

		err := b.RegisterClientInsBridgeFunUseInfo(clientInst, "cf00203d-c4bd-52a9-adb5-d624349bffcf", -1, "test")
		So(err, ShouldNotBeNil)
		_, ok := b.GetClientInfoFromCache(clientInst, "test")
		So(ok, ShouldBeFalse)
		So(err.Error(), ShouldEqual, "[bridgeplugin] Minus refCount not allow unexisted PodName, stateID: cf00203d-c4bd-52a9-adb5-d624349bffcf,client podName: ClientPod111, bridgeFuncName: rds, traceID: test")
	})
}

func Test_updateClientCiteBridgeFuncInfo(t *testing.T) {
	Convey("Test updateClientCiteBridgeFuncInfo when ClientCiteBridgeFuncMap is empty", t, func(c C) {
		b := &cache.BridgeCache{}
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "bridgeFuncName",
			},
		}
		updateClientCiteBridgeFuncInfo(clientInst, b, "test")
		v, ok := b.ClientCiteBridgeFuncMap.Load(clientInst.PodName)
		So(ok, ShouldBeTrue)
		citeBridgeInfo, ok := v.([]string)
		So(ok, ShouldBeTrue)
		for _, val := range citeBridgeInfo {
			So("bridgeFuncName", ShouldEqual, val)
		}
		b.ClientCiteBridgeFuncMap.Delete(clientInst.PodName)
	})

	Convey("Test updateClientCiteBridgeFuncInfo when ClientCiteBridgeFuncMap is not empty", t, func(c C) {
		b := &cache.BridgeCache{}
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "bridgeFuncName",
			},
		}
		citeBridgeInfo := make([]string, 0)
		citeBridgeInfo = append(citeBridgeInfo, "123")
		citeBridgeInfo = append(citeBridgeInfo, "456")
		b.ClientCiteBridgeFuncMap.Store(clientInst.PodName, citeBridgeInfo)
		updateClientCiteBridgeFuncInfo(clientInst, b, "test")
		v, ok := b.ClientCiteBridgeFuncMap.Load(clientInst.PodName)
		So(ok, ShouldBeTrue)
		citeBridgeInfo, ok = v.([]string)
		So(ok, ShouldBeTrue)
		So("123", ShouldEqual, citeBridgeInfo[0])
		So("456", ShouldEqual, citeBridgeInfo[1])
		So("bridgeFuncName", ShouldEqual, citeBridgeInfo[2])
	})
}

func Test_deleteClientCiteBridgeFuncInfo(t *testing.T) {
	Convey("Test deleteClientCiteBridgeFuncInfo when ClientCiteBridgeFuncMap is not empty", t, func(c C) {
		b := &cache.BridgeCache{}
		clientInst := &bgtypes.ClientInstance{
			PodName:       "ClientPod",
			AssignStateID: "cf00203d-c4bd-52a9-adb5-d624349bffcf",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "tenant1",
				ServiceID:      "0@base",
				BridgeFuncName: "bridgeFuncName",
			},
		}
		citeBridgeInfo := make([]string, 0)
		citeBridgeInfo = append(citeBridgeInfo, "123")
		citeBridgeInfo = append(citeBridgeInfo, "456")
		b.ClientCiteBridgeFuncMap.Store(clientInst.PodName, citeBridgeInfo)
		updateClientCiteBridgeFuncInfo(clientInst, b, "test")
		v, ok := b.ClientCiteBridgeFuncMap.Load(clientInst.PodName)
		deleteClientCiteBridgeFuncInfo(clientInst, "test", b)
		v, ok = b.ClientCiteBridgeFuncMap.Load(clientInst.PodName)
		So(ok, ShouldBeTrue)
		citeBridgeInfo, ok = v.([]string)
		So(ok, ShouldBeTrue)
		So(2, ShouldEqual, len(citeBridgeInfo))
		So("123", ShouldEqual, citeBridgeInfo[0])
		So("456", ShouldEqual, citeBridgeInfo[1])
		b.ClientCiteBridgeFuncMap.Delete(clientInst.PodName)
	})
}

func Test_updateClientCiteBridgeFunc(t *testing.T) {
	Convey("Test updateClientCiteBridgeFunc", t, func(c C) {
		bridgeCache := cache.Instance()
		bridgeCache.ClientCiteBridgeFuncMap.Store("pool23-400-256-fusion-794fc8f797-qp95v",
			[]string{"yzd-mqs-consumer-bridge", "yzd-mqs-consumer-bridge1"})
		value, _ := bridgeCache.ClientCiteBridgeFuncMap.Load("pool23-400-256-fusion-794fc8f797-qp95v")
		bridgeFuncs, _ := value.([]string)
		So(len(bridgeFuncs), ShouldEqual, 2)
		updateClientCiteBridgeFunc(bridgeCache,
			"pool23-400-256-fusion-794fc8f797-qp95v_c53626012ba84727b938ca8bf03108ef_yzd-mqs-consumer-bridge")
		value, _ = bridgeCache.ClientCiteBridgeFuncMap.Load("pool23-400-256-fusion-794fc8f797-qp95v")
		bridgeFuncs, _ = value.([]string)
		So(len(bridgeFuncs), ShouldEqual, 1)
	})
}

func Test_checkBridgeIDRefValidity(t *testing.T) {
	Convey("Test checkBridgeIDRefValidity ok", t, func(c C) {
		bridgeCache := cache.Instance()
		bridgeCache.StateIDInfoCache.Store("stateid", &cache.StateIDInfo{PodName: "podName"})
		bridgeCache.StateWorkersCache["stateid"] = true
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
			gomonkey.ApplyFunc(deletePersistentState,
				func(tenantID, serviceID, functionName, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(genStateIDByKey,
				func(client *bgtypes.ClientInstance, stateKey string, traceID string) string { return "stateid" }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		checkBridgeIDRefValidity(&bgtypes.ClientInstance{}, bridgeCache, "statekey", "traceid")
		_, ok := bridgeCache.StateIDInfoCache.Load("stateid")
		So(ok, ShouldBeTrue)
	})
	Convey("Test checkBridgeIDRefValidity state worker nok", t, func(c C) {
		bridgeCache := cache.Instance()
		bridgeCache.StateIDInfoCache.Store("stateid", &cache.StateIDInfo{PodName: "podName"})
		bridgeCache.StateWorkersCache["stateid"] = false
		flag := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
			gomonkey.ApplyFunc(deletePersistentState,
				func(tenantID, serviceID, functionName, stateID string, traceID string) error {
					flag = true
					return nil
				}),
			gomonkey.ApplyFunc(genStateIDByKey,
				func(client *bgtypes.ClientInstance, stateKey string, traceID string) string { return "stateid" }),
			gomonkey.ApplyFunc(ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					bridgeCache.StateIDInfoCache.Delete("stateid")
					return nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		So(flag, ShouldBeFalse)
		checkBridgeIDRefValidity(&bgtypes.ClientInstance{FuncInfo: &bgtypes.BridgeFuncInfo{
			TenantID:       "TenantID",
			ServiceID:      "ServiceID",
			BridgeFuncName: "BridgeFuncName",
		}}, bridgeCache, "statekey", "traceid")
		_, ok := bridgeCache.StateIDInfoCache.Load("stateid")
		So(ok, ShouldBeFalse)
		So(flag, ShouldBeTrue)
	})
	Convey("Test checkBridgeIDRefValidity stateinfo not found", t, func(c C) {
		bridgeCache := cache.Instance()
		bridgeCache.StateWorkersCache["stateid"] = true
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
			gomonkey.ApplyFunc(deletePersistentState,
				func(tenantID, serviceID, functionName, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(genStateIDByKey,
				func(client *bgtypes.ClientInstance, stateKey string, traceID string) string { return "stateid" }),
			gomonkey.ApplyFunc(ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					bridgeCache.StateIDInfoCache.Delete("stateid")
					return nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		checkBridgeIDRefValidity(&bgtypes.ClientInstance{}, bridgeCache, "statekey", "traceid")
		_, ok := bridgeCache.StateIDInfoCache.Load("stateid")
		So(ok, ShouldBeFalse)
	})
	Convey("Test checkBridgeIDRefValidity stateinfo assert failed", t, func(c C) {
		bridgeCache := cache.Instance()
		bridgeCache.StateIDInfoCache.Store("stateid", "")
		bridgeCache.StateWorkersCache["stateid"] = true
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
			gomonkey.ApplyFunc(deletePersistentState,
				func(tenantID, serviceID, functionName, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(genStateIDByKey,
				func(client *bgtypes.ClientInstance, stateKey string, traceID string) string { return "stateid" }),
			gomonkey.ApplyFunc(ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					bridgeCache.StateIDInfoCache.Delete("stateid")
					return nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		checkBridgeIDRefValidity(&bgtypes.ClientInstance{}, bridgeCache, "statekey", "traceid")
		_, ok := bridgeCache.StateIDInfoCache.Load("stateid")
		So(ok, ShouldBeFalse)
	})
	Convey("Test checkBridgeIDRefValidity bridge instance nok", t, func(c C) {
		bridgeCache := cache.Instance()
		bridgeCache.StateIDInfoCache.Store("stateid", &cache.StateIDInfo{})
		bridgeCache.StateWorkersCache["stateid"] = true
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
			gomonkey.ApplyFunc(deletePersistentState,
				func(tenantID, serviceID, functionName, stateID string, traceID string) error {
					return nil
				}),
			gomonkey.ApplyFunc(genStateIDByKey,
				func(client *bgtypes.ClientInstance, stateKey string, traceID string) string { return "stateid" }),
			gomonkey.ApplyFunc(ReleaseBridgeWhenSuicide,
				func(bgInst *bgtypes.BridgeInstance, stateID string, traceID string) error {
					bridgeCache.StateIDInfoCache.Delete("stateid")
					return nil
				}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		checkBridgeIDRefValidity(&bgtypes.ClientInstance{}, bridgeCache, "statekey", "traceid")
		_, ok := bridgeCache.StateIDInfoCache.Load("stateid")
		So(ok, ShouldBeFalse)
	})
}
