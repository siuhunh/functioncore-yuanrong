package cache

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	bgutils "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	"functioncore/test/utils"
)

func TestInstance(t *testing.T) {
	tests := []struct {
		name    string
		want    *BridgeCache
		wantErr bool
	}{
		{
			"test_case",
			&BridgeCache{
				StateWorkersCache: make(map[string]bool),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Instance(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Instance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_bridgeCache_DeleteETCD(t *testing.T) {
	Convey("Test delete cache info in etcd", t, func() {
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		patches := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		defer patch.Reset()
		defer patches.Reset()
		b := &BridgeCache{}
		clientInstance := getTestClientInstance()
		stateIDInfo := getTestStateIDInfo()
		Convey("Test delete client info without err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return nil
				})
			err := b.DeleteSpecificClientInfoETCD(clientInstance, "test")
			So(err, ShouldBeNil)
			patch1.Reset()
		})
		Convey("Test delete client info with err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete etcd failed")
				})
			err := b.DeleteSpecificClientInfoETCD(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "delete etcd failed")
			patch1.Reset()
		})
		Convey("Test delete stateID info without err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return nil
				})
			err := b.DeleteSpecificStateIDInfoETCD(stateIDInfo, "ID1", "test")
			So(err, ShouldBeNil)
			patch1.Reset()
		})
		Convey("Test delete stateID info with err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete etcd failed")
				})
			err := b.DeleteSpecificStateIDInfoETCD(stateIDInfo, "ID1", "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "delete etcd failed")
			patch1.Reset()
		})
	})
}

func Test_bridgeCache_GetRefCountFromCache(t *testing.T) {
	Convey("Test get refCount from cache", t, func() {
		b := &BridgeCache{}
		b.StateIDInfoCache.Store("ID1", getTestStateIDInfo())
		refCount := b.GetRefCountFromCache("ID1", "test")
		So(refCount, ShouldEqual, 1)
		refCount = b.GetRefCountFromCache("ID2", "test")
		So(refCount, ShouldEqual, -1)
	})
}

func Test_bridgeCache_IsReachMaxStateKeyCount(t *testing.T) {
	Convey("Test judge whether reach max state key count from cache", t, func() {
		patch := gomonkey.ApplyFunc((*BridgeCache).GetRestrictByURN,
			func(_ *BridgeCache, functionURN string) (BridgeConfigResp, *config.FunctionMetaInfo, error) {
				return BridgeConfigResp{15, 15}, nil, nil
			})
		defer patch.Reset()
		Convey("Test reach max state key count", func() {
			b := &BridgeCache{}
			for i := 1; i <= 20; i++ {
				// test stateIDInfo.MaxBgInstQua is 10
				stateIDInfo := getTestStateIDInfo()
				stateIDInfo.BridgeFuncURN = "rds"
				mapKey := "ID" + strconv.Itoa(i)
				b.StateIDInfoCache.Store(mapKey, stateIDInfo)
			}
			isReach, err := b.IsReachMaxStateKeyCount("rds", "test")
			So(isReach, ShouldBeTrue)
			stateIDInfoGet, _ := b.StateIDInfoCache.Load("ID20")
			// after IsReachMaxStateKeyCount method executed, stateIDInfo.MaxBgInstQua should be 15
			So(stateIDInfoGet.(*StateIDInfo).MaxBgInstQua, ShouldEqual, 15)
			So(err, ShouldBeNil)
		})
		Convey("Test unreach max state key count", func() {
			b := &BridgeCache{}
			stateIDInfo := getTestStateIDInfo()
			stateIDInfo.BridgeFuncURN = "rds"
			mapKey := "ID"
			b.StateIDInfoCache.Store(mapKey, stateIDInfo)
			isReach, err := b.IsReachMaxStateKeyCount("rds", "test")
			So(isReach, ShouldBeFalse)
			So(err, ShouldBeNil)
		})
	})
}

func Test_bridgeCache_LoadCacheFromEtcd(t *testing.T) {
	Convey("Test load persistent data from etcd to cache", t, func() {
		b := &BridgeCache{}
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		patches := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		defer patch.Reset()
		defer patches.Reset()
		Convey("Test etcd get response error", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return nil, errors.New("get response error")
				})
			err := b.LoadCacheFromEtcd()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "get response error")
			patch1.Reset()
		})
		Convey("Test no record in etcd", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return nil, nil
				})
			err := b.LoadCacheFromEtcd()
			So(err, ShouldBeNil)
			patch1.Reset()
		})
		Convey("Test stateIDInfo record exist in etcd", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
					"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
						opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{
							Kvs: []*mvccpb.KeyValue{
								{
									Key:   []byte("/sn/bridgemanager/business/yrk/tenant/a/function/0@base@rds/version/latest/defaultaz/ID1"),
									Value: []byte(`{"stateKey":"key1","bridgePodName":"pool0-xxxx","refCount":1,"bridgeFuncURN":"sn:cn:yrk:a:function:0@base@rds","maxBgInstQua":2,"maxActiveQua":200}`),
								},
								{
									Key:   []byte("/sn/bridgemanager/business/yrk/tenant/a/function/0@base@rds/version/latest/defaultaz/ID2"),
									Value: []byte(`{"stateKey":"key2","bridgePodName":"pool0-xxxx","refCount":1,"bridgeFuncURN":"sn:cn:yrk:a:function:0@base@rds","maxBgInstQua":2,"maxActiveQua":200}`),
								},
							},
						}, nil
					}),
				gomonkey.ApplyFunc((*BridgeCache).loadClientCacheFromEtcd, func(_ *BridgeCache) error {
					return nil
				}),
			})
			err := b.LoadCacheFromEtcd()
			So(err, ShouldBeNil)
			_, ok := b.StateIDInfoCache.Load("ID1")
			So(ok, ShouldBeTrue)
			_, ok = b.StateIDInfoCache.Load("ID2")
			So(ok, ShouldBeTrue)
			_, ok = b.FuncNameInfoCache.Load("sn:cn:yrk:a:function:0@base@rds")
			So(ok, ShouldBeTrue)
			patches.ResetAll()
		})
	})
}

func Test_bridgeCache_PickAvailableStateKey(t *testing.T) {
	Convey("Test pick existed stataKey", t, func() {
		b := &BridgeCache{}
		patch := gomonkey.ApplyGlobalVar(&bgutils.ClusterID, "cluster1")
		defer patch.Reset()
		stateIDInfo := getTestStateIDInfo()
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		_, stateInfo, ok := b.PickAvailableStateKey("sn:cn:yrk:xxxx:function:0@base@rds", "test")
		So(stateInfo.StateKey, ShouldEqual, "key")
		So(ok, ShouldBeTrue)
		stateIDInfo.RefCount = 10
		_, stateInfo, ok = b.PickAvailableStateKey("sn:cn:yrk:xxxx:function:0@base@rds", "test")
		So(stateInfo.StateKey, ShouldBeBlank)
		So(ok, ShouldBeFalse)
	})
}

func Test_bridgeCache_RegisterRefCount(t *testing.T) {
	Convey("Test register stateID", t, func() {
		b := &BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		Convey("test register existed stateID", func() {
			err := b.RegisterRefCount("ID1", "test")
			So(err, ShouldBeNil)
			stateIDInfoGet, _ := b.StateIDInfoCache.Load("ID1")
			So(stateIDInfoGet.(*StateIDInfo).RefCount, ShouldEqual, 2)
		})
		Convey("test register no existed stateID", func() {
			err := b.RegisterRefCount("ID2", "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] StateID: ID2 is not exist when register refCount, traceID: test")
		})
	})
}

func Test_bridgeCache_StoreETCD(t *testing.T) {
	Convey("Test store client info in etcd", t, func() {
		b := &BridgeCache{}
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		patches := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		defer patch.Reset()
		defer patches.Reset()
		stateIDInfo := getTestStateIDInfo()
		clientInstance := getTestClientInstance()
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		b.ClientPodClientInstCache.Store(bgutils.GetClientCacheInfoKey(clientInstance), clientInstance)
		Convey("Test store stateIDInfo to etcd with err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return errors.New("put error")
				})
			err := b.StoreSpecificStateIDInfoToEtcd("ID1", "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "put error")
			patch1.Reset()
		})
		Convey("Test store stateIDInfo to etcd without err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				})
			err := b.StoreSpecificStateIDInfoToEtcd("ID1", "test")
			So(err, ShouldBeNil)
			patch1.Reset()
		})
		Convey("Test store not exist stateID to etcd", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				})
			err := b.StoreSpecificStateIDInfoToEtcd("ID2", "test")
			So(err, ShouldBeNil)
			patch1.Reset()
		})
		Convey("Test store clientInstance to etcd with err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return errors.New("put error")
				})
			err := b.StoreSpecificClientInstToEtcd(clientInstance, "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "put error")
			patch1.Reset()
		})
		Convey("Test store clientInstance to etcd without err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
				func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
					return nil
				})
			err := b.StoreSpecificClientInstToEtcd(clientInstance, "test")
			So(err, ShouldBeNil)
			patch1.Reset()
		})

		b = &BridgeCache{}
		funcUseInfo := make(map[string]int)
		funcUseInfo["1111"] = 0
		clientInstance = &bgtypes.ClientInstance{
			PodName: "pool0-xxxx",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "a",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
			FuncUseInfo: funcUseInfo,
		}
		b.ClientPodClientInstCache.Store(bgutils.GetClientCacheInfoKey(clientInstance), clientInstance)
		Convey("Test store clientInstance to etcd with err1", func() {
			patch1 := gomonkey.ApplyFunc((*BridgeCache).UpdateSpecificClientInstToEtcd,
				func(_ *BridgeCache, clientInstance *bgtypes.ClientInstance, traceID string) error {
					return errors.New("error")
				})
			err := b.StoreSpecificClientInstToEtcd(clientInstance, "test")
			So(err, ShouldNotBeNil)
			patch1.Reset()
		})

		b = &BridgeCache{}
		funcUseInfo = make(map[string]int)
		funcUseInfo["1111"] = 1
		clientInstance = &bgtypes.ClientInstance{
			PodName: "pool0-xxxx",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "a",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}

		Convey("Test store clientInstance to etcd with err2", func() {
			err := b.StoreSpecificClientInstToEtcd(clientInstance, "test")
			So(err, ShouldBeNil)
		})
	})
}

func Test_bridgeCache_UpdateBridgeFuncPodCache(t *testing.T) {
	b := &BridgeCache{}
	stateIDInfo := getTestStateIDInfo()
	b.StateIDInfoCache.Store("ID1", stateIDInfo)
	Convey("Test update bridgePodName in cache with existed stateID", t, func() {
		cache, err := b.UpdateBridgeFuncPodCache("ID1", "NewPodName", "test")
		So(err, ShouldBeNil)
		So(cache, ShouldEqual, "pool0-xxxx")
	})
	Convey("Test update bridgePodName in cache with non-existed stateID", t, func() {
		cache, err := b.UpdateBridgeFuncPodCache("ID2", "NewPodName", "test")
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEqual, "[bridgeplugin] UpdateBridgeFuncPodCache stateID: ID2 does not exist, traceID: test, client: NewPodName")
		So(cache, ShouldEqual, "")
	})
}

func Test_bridgeCache_UpdateRefCountCacheAndDeleteWhenRefZero(t *testing.T) {
	Convey("Test update refCount in cache", t, func() {
		b := &BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		Convey("Test update refCount plus one with existed stateID", func() {
			err := b.UpdateRefCountCacheAndDeleteWhenRefZero("ID1", 1, false, "test")
			So(err, ShouldBeNil)
			stateIDInfoGet, _ := b.StateIDInfoCache.Load("ID1")
			So(stateIDInfoGet.(*StateIDInfo).RefCount, ShouldEqual, 2)
		})
		Convey("Test update refCount minus one with existed stateID", func() {
			err := b.UpdateRefCountCacheAndDeleteWhenRefZero("ID1", -1, false, "test")
			So(err, ShouldBeNil)
			stateIDInfoGet, _ := b.StateIDInfoCache.Load("ID1")
			So(stateIDInfoGet, ShouldBeNil)
		})
		Convey("Test update refCount positive with no existed stateID", func() {
			err := b.UpdateRefCountCacheAndDeleteWhenRefZero("ID2", 1, false, "test")
			So(err, ShouldBeNil)
			stateIDInfoGet, _ := b.StateIDInfoCache.Load("ID2")
			So(stateIDInfoGet.(*StateIDInfo).RefCount, ShouldEqual, 1)
		})
		Convey("Test update refCount negative with no existed stateID", func() {
			err := b.UpdateRefCountCacheAndDeleteWhenRefZero("ID2", -1, false, "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] Minus refCount not allow nonexistent stateID")
		})
		b = &BridgeCache{}
		stateIDInfo = &StateIDInfo{
			StateKey:      "key",
			PodName:       "pool0-xxxx",
			RefCount:      1,
			BridgeFuncURN: "sn:cn:yrk:xxxx:function:0@base@rds",
			MaxBgInstQua:  10,
			MaxActiveQua:  10,
		}
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		Convey("Test update refCount negative with no existed stateID1", func() {
			patch := gomonkey.ApplyFunc((*BridgeCache).DeleteSpecificStateIDInfoETCD, func(_ *BridgeCache, stateIDInfo *StateIDInfo, stateID string, traceID string) error {
				return errors.New("error")
			})
			err := b.UpdateRefCountCacheAndDeleteWhenRefZero("ID2", -1, true, "test")
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] Minus refCount not allow nonexistent stateID")
			patch.Reset()
		})
	})
}

func Test_bridgeCache_getStateIDInfoFromCache(t *testing.T) {
	Convey("Test update urn and relative max quantity in cache", t, func() {
		b := &BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		b.StateIDInfoCache.Store("ID2", "format invalid")
		Convey("Test get with existed stateID", func() {
			stateIDInfoGet, ok := b.GetStateIDInfoFromCache("ID1", "test")
			So(stateIDInfoGet, ShouldEqual, stateIDInfo)
			So(ok, ShouldBeTrue)
		})
		Convey("Test get with non-existed stateID", func() {
			stateIDInfoGet, ok := b.GetStateIDInfoFromCache("ID not exist", "test")
			So(stateIDInfoGet, ShouldBeNil)
			So(ok, ShouldBeFalse)
		})
		Convey("Test get with existed stateID but type invalid", func() {
			stateIDInfoGet, ok := b.GetStateIDInfoFromCache("ID2", "test")
			So(stateIDInfoGet, ShouldBeNil)
			So(ok, ShouldBeFalse)
		})
	})
}

func Test_bridgeCache_calcRestrict(t *testing.T) {
	Convey("Test calculate bridge restrictions", t, func() {
		b := &BridgeCache{}
		Convey("Test decryptEnv error", func() {
			patch := gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
				return nil, errors.New("decryptEnv failed")
			})
			maxStateKeyQua, maxActiveQua, err := b.calcRestrict(&config.FunctionMetaInfo{})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "decryptEnv failed")
			So(maxStateKeyQua, ShouldEqual, 0)
			So(maxActiveQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test decryptEnv without err but empty return", func() {
			patch := gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
				return nil, nil
			})
			maxStateKeyQua, maxActiveQua, err := b.calcRestrict(&config.FunctionMetaInfo{})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] envs is nil, calculate max number of stateKey failed")
			So(maxStateKeyQua, ShouldEqual, 0)
			So(maxActiveQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test decryptEnv without err but maxBaaSConnections configuration invalid", func() {
			patch := gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
				env := make(map[string]interface{})
				env[maxBaaSConnections] = "invalid"
				return env, nil
			})
			maxStateKeyQua, maxActiveQua, err := b.calcRestrict(&config.FunctionMetaInfo{})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] invalid key [maxBaaSConnections] of envs, type [string]")
			So(maxStateKeyQua, ShouldEqual, 0)
			So(maxActiveQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test decryptEnv without err but maxActive configuration invalid", func() {
			patch := gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
				env := make(map[string]interface{})
				env[maxBaaSConnections] = "10"
				env[maxActive] = "invalid"
				return env, nil
			})
			maxStateKeyQua, maxActiveQua, err := b.calcRestrict(&config.FunctionMetaInfo{})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] invalid key [maxActive] of envs, type [string]")
			So(maxStateKeyQua, ShouldEqual, 0)
			So(maxActiveQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test decryptEnv without err but maxBaaSConnections value is zero", func() {
			patch := gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
				env := make(map[string]interface{})
				env[maxBaaSConnections] = "0"
				env[maxActive] = "10"
				return env, nil
			})
			maxStateKeyQua, maxActiveQua, err := b.calcRestrict(&config.FunctionMetaInfo{})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] maxBaaSConnections is not a positive number")
			So(maxStateKeyQua, ShouldEqual, 0)
			So(maxActiveQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test decryptEnv without err but maxBaaSConnections less than maxActive", func() {
			patch := gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
				env := make(map[string]interface{})
				env[maxBaaSConnections] = "5"
				env[maxActive] = "10"
				return env, nil
			})
			maxStateKeyQua, maxActiveQua, err := b.calcRestrict(&config.FunctionMetaInfo{})
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] maxBaaSConnections is less than maxActive")
			So(maxStateKeyQua, ShouldEqual, 0)
			So(maxActiveQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test decryptEnv without err", func() {
			patch := gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
				env := make(map[string]interface{})
				env[maxBaaSConnections] = "20"
				env[maxActive] = "5"
				return env, nil
			})
			maxStateKeyQua, maxActiveQua, err := b.calcRestrict(&config.FunctionMetaInfo{})
			So(err, ShouldBeNil)
			So(maxStateKeyQua, ShouldEqual, 4)
			So(maxActiveQua, ShouldEqual, 5)
			patch.Reset()
		})
	})
}

func Test_bridgeCache_loadClientCacheFromEtcd(t *testing.T) {
	Convey("Test load client persistent data from etcd to cache", t, func() {
		b := &BridgeCache{}
		patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{Client: &clientv3.Client{}}
		})
		defer patch.Reset()
		Convey("Test get meta etcd client with nil", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return nil, errors.New("get response error")
				})
			patches := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			err := b.loadClientCacheFromEtcd()
			So(err, ShouldNotBeNil)
			patch1.Reset()
			patches.Reset()
		})
		Convey("Test get response with err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return nil, errors.New("get response error")
				})
			patches := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b.loadClientCacheFromEtcd()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "get response error")
			patch1.Reset()
			patches.Reset()
		})
		Convey("Test get response without err but resp is empty", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return nil, nil
				})
			patches := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b.loadClientCacheFromEtcd()
			So(err, ShouldBeNil)
			patch1.Reset()
			patches.Reset()
		})
		Convey("Test get response without err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return &clientv3.GetResponse{
						Kvs: []*mvccpb.KeyValue{
							{
								Key:   []byte("/sn/bridgeclient/business/yrk/tenant/a/function/0@base@rds01/version/latest/defaultaz/defaultaz-#-pool0-xxxx"),
								Value: []byte(`{"podName":"pool0-xxxx","assignStateID":"ID1","bridgeFuncInfo":{"tenantID":"a","serviceID":"0@base","bridgeFuncName":"rds01"}}`),
							},
							{
								Key:   []byte("/sn/bridgeclient/business/yrk/tenant/a/function/0@base@rds02/version/latest/defaultaz/defaultaz-#-pool1-xxxx"),
								Value: []byte(`{"podName":"pool1-xxxx","assignStateID":"ID1","bridgeFuncInfo":{"tenantID":"a","serviceID":"0@base","bridgeFuncName":"rds02"}}`),
							},
						},
					}, nil
				})
			patches := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b.loadClientCacheFromEtcd()
			So(err, ShouldBeNil)
			_, ok := b.ClientPodClientInstCache.Load("pool0-xxxx_a_rds01")
			So(ok, ShouldBeTrue)
			_, ok = b.ClientPodClientInstCache.Load("pool1-xxxx_a_rds02")
			So(ok, ShouldBeTrue)
			patch1.Reset()
			patches.Reset()
		})
	})
}

func Test_bridgeCache_updateRestrict(t *testing.T) {
	Convey("Test update restrict info", t, func() {
		b := &BridgeCache{}
		Convey("Test update restrict with positive newMaxStateKey", func() {
			newMaxStateKey := 10
			Convey("Test update restrict with existed functionURN", func() {
				b.FuncNameInfoCache.Store("urn1", &bridgeFuncRestrict{
					maxStateKeyQua: 2,
					maxActive:      2,
				})
				err := b.updateRestrict(&config.FunctionMetaInfo{FuncMetaData: config.FuncMetaData{FunctionURN: "urn1"}},
					newMaxStateKey, 3)
				So(err, ShouldBeNil)
				restriction, ok := b.FuncNameInfoCache.Load("urn1")
				So(ok, ShouldBeTrue)
				So(restriction.(*bridgeFuncRestrict).maxStateKeyQua, ShouldEqual, newMaxStateKey)
				So(restriction.(*bridgeFuncRestrict).maxActive, ShouldEqual, 3)
			})
			Convey("Test update restrict with non-existed functionURN", func() {
				err := b.updateRestrict(&config.FunctionMetaInfo{FuncMetaData: config.FuncMetaData{FunctionURN: "urn2"}},
					newMaxStateKey, 3)
				So(err, ShouldBeNil)
				restriction, ok := b.FuncNameInfoCache.Load("urn2")
				So(ok, ShouldBeTrue)
				So(restriction.(*bridgeFuncRestrict).maxStateKeyQua, ShouldEqual, newMaxStateKey)
				So(restriction.(*bridgeFuncRestrict).maxActive, ShouldEqual, 3)
			})
		})
		Convey("Test update restrict with zero or negative newMaxStateKey", func() {
			newMaxStateKey := 0
			Convey("Test update restrict with existed functionURN", func() {
				b.FuncNameInfoCache.Store(&config.FuncMetaData{FunctionURN: "urn1"}, &bridgeFuncRestrict{
					maxStateKeyQua: 2,
					maxActive:      2,
				})
				err := b.updateRestrict(&config.FunctionMetaInfo{FuncMetaData: config.FuncMetaData{FunctionURN: "urn1"}},
					newMaxStateKey, 3)
				So(err, ShouldBeNil)
				restriction, ok := b.FuncNameInfoCache.Load("urn1")
				So(ok, ShouldBeTrue)
				So(restriction.(*bridgeFuncRestrict).maxStateKeyQua, ShouldEqual, 1)
				So(restriction.(*bridgeFuncRestrict).maxActive, ShouldEqual, 3)
			})
			Convey("Test update restrict with non-existed functionURN", func() {
				err := b.updateRestrict(&config.FunctionMetaInfo{FuncMetaData: config.FuncMetaData{FunctionURN: "urn2"}},
					newMaxStateKey, 3)
				So(err, ShouldBeNil)
				restriction, ok := b.FuncNameInfoCache.Load("urn2")
				So(ok, ShouldBeTrue)
				So(restriction.(*bridgeFuncRestrict).maxStateKeyQua, ShouldEqual, 1)
				So(restriction.(*bridgeFuncRestrict).maxActive, ShouldEqual, 3)
			})
		})
	})
}

func Test_decryptEnv(t *testing.T) {
	Convey("Test decrypt environment", t, func() {
		info := &config.FunctionMetaInfo{
			FuncMetaData:     config.FuncMetaData{},
			S3MetaData:       config.S3MetaData{},
			EnvMetaData:      config.EnvMetaData{},
			ResourceMetaData: config.ResourceMetaData{},
			InstanceMetaData: config.InstanceMetaData{},
			ExtendedMetaData: config.ExtendedMetaData{},
		}
		Convey("Test environment is empty", func() {
			info.FuncMetaData.FuncName = "testfunc"
			info.EnvMetaData.Environment = ""
			env, err := decryptEnv(info)
			So(env, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] env of bridgefunc [testfunc] is empty")
		})
		Convey("Test environment is exist but decrypt with error", func() {
			info.FuncMetaData.FuncName = "testfunc"
			info.EnvMetaData.Environment = "xxx"
			patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return nil, errors.New("Decrypt a cypher text err")
			})
			env, err := decryptEnv(info)
			So(env, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] decrypt user's env error: Decrypt a cypher text err, funcName: testfunc")
			patch.Reset()
		})
		Convey("Test environment is exist but with unmarshal error", func() {
			info.FuncMetaData.FuncName = "testfunc"
			info.EnvMetaData.Environment = "xxx"
			patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte(`{"maxActive"}`), nil
			})
			env, err := decryptEnv(info)
			So(env, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] unmashal env error: invalid character '}' after object key, funcName: testfunc")
			patch.Reset()
		})
		Convey("Test environment is exist and decrypt successfully", func() {
			info.FuncMetaData.FuncName = "testfunc"
			info.EnvMetaData.Environment = "xxx"
			patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte(`{"maxActive":20}`), nil
			})
			env, err := decryptEnv(info)
			So(env, ShouldNotBeNil)
			So(err, ShouldBeNil)
			So(env["maxActive"], ShouldNotBeNil)
			patch.Reset()
		})
	})
}

func Test_bridgeCache_UpdateBridgeConfigByMeta(t *testing.T) {
	Convey("Test update bridge config", t, func() {
		b := &BridgeCache{}
		info := &config.FunctionMetaInfo{
			FuncMetaData:     config.FuncMetaData{},
			S3MetaData:       config.S3MetaData{},
			EnvMetaData:      config.EnvMetaData{},
			ResourceMetaData: config.ResourceMetaData{},
			InstanceMetaData: config.InstanceMetaData{},
			ExtendedMetaData: config.ExtendedMetaData{},
		}
		Convey("Test calcRestrict with err", func() {
			patch := gomonkey.ApplyFunc((*BridgeCache).calcRestrict, func(_ *BridgeCache, funcMeta *config.FunctionMetaInfo) (int, int, error) {
				return 0, 0, errors.New("calcRestrict error")
			})
			err := b.UpdateBridgeConfigByMeta(info)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "calcRestrict error")
			patch.Reset()
		})
		Convey("Test calcRestrict without err", func() {
			patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return []byte(`{"maxActive":"20","maxBaaSConnections":"400"}`), nil
			})
			info.FuncMetaData.FunctionURN = "testurn"
			info.EnvMetaData.Environment = "testenvironment"
			err := b.UpdateBridgeConfigByMeta(info)
			So(err, ShouldBeNil)
			bridgeRestrict, ok := b.FuncNameInfoCache.Load("testurn")
			So(ok, ShouldBeTrue)
			So(bridgeRestrict.(*bridgeFuncRestrict).maxActive, ShouldEqual, 20)
			So(bridgeRestrict.(*bridgeFuncRestrict).maxStateKeyQua, ShouldEqual, 20)
			patch.Reset()
		})
	})
}

func Test_bridgeCache_GetRestrictByURN(t *testing.T) {
	Convey("Test update urn and relative max quantity in cache", t, func() {
		b := &BridgeCache{}
		info := &config.FunctionMetaInfo{
			FuncMetaData:     config.FuncMetaData{},
			S3MetaData:       config.S3MetaData{},
			EnvMetaData:      config.EnvMetaData{},
			ResourceMetaData: config.ResourceMetaData{},
			InstanceMetaData: config.InstanceMetaData{},
			ExtendedMetaData: config.ExtendedMetaData{},
		}
		info.FuncMetaData.IsBridgeFunction = true
		stateIDInfo := getTestStateIDInfo()
		bridgeRestrict := getTestBridgeRestrict()
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		b.FuncNameInfoCache.Store("newURN", bridgeRestrict)
		Convey("Test update with existed URN", func() {
			bridgeConfigResp, _, err := b.GetRestrictByURN("newURN")
			maxBgInstQua, maxActiveQua := bridgeConfigResp.MaxStateKeyQua, bridgeConfigResp.MaxActiveQua
			So(err, ShouldBeNil)
			So(maxActiveQua, ShouldEqual, 1)
			So(maxBgInstQua, ShouldEqual, 1)
		})
		Convey("Test non-existed URN, get from etcd succeed", func() {
			info.FuncMetaData.FunctionURN = "sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@test-java-snapshot-restore-coldstart-001"

			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(bgutils.GetValueFromMetaEtcd, func(funcKey string) ([]byte, error) {
					if funcKey == "/sn/functions/business/yrk/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@test-java-snapshot-restore-coldstart-001/version/latest" {
						return json.Marshal(info)
					}
					return nil, errors.New("error occurs")
				}),
				gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
					env := make(map[string]interface{})
					env[maxBaaSConnections] = "10"
					env[maxActive] = "5"
					return env, nil
				}),
			})

			bridgeConfigResp, _, err := b.GetRestrictByURN("sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@test-java-snapshot-restore-coldstart-001")
			maxBgInstQua, maxActiveQua := bridgeConfigResp.MaxStateKeyQua, bridgeConfigResp.MaxActiveQua
			So(err, ShouldBeNil)
			So(maxActiveQua, ShouldEqual, 5)
			So(maxBgInstQua, ShouldEqual, 2)
			patches.ResetAll()
		})
		Convey("Test non-existed URN, get from etcd error", func() {
			patch := gomonkey.ApplyFunc(bgutils.GetValueFromMetaEtcd, func(funcKey string) ([]byte, error) {
				return nil, errors.New("error occurs")
			})
			bridgeConfigResp, _, err := b.GetRestrictByURN("sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@test-java-snapshot-restore-coldstart-001")
			maxBgInstQua, maxActiveQua := bridgeConfigResp.MaxStateKeyQua, bridgeConfigResp.MaxActiveQua
			So(err, ShouldNotBeNil)
			ShouldBeTrue(strings.Contains(err.Error(), "failed to get function configuration"))
			So(maxActiveQua, ShouldEqual, 0)
			So(maxBgInstQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test non-existed URN, get from etcd empty", func() {
			patch := gomonkey.ApplyFunc(bgutils.GetValueFromMetaEtcd, func(funcKey string) ([]byte, error) {
				return make([]byte, 0), nil
			})
			bridgeConfigResp, _, err := b.GetRestrictByURN("sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@test-java-snapshot-restore-coldstart-001")
			maxBgInstQua, maxActiveQua := bridgeConfigResp.MaxStateKeyQua, bridgeConfigResp.MaxActiveQua
			So(err, ShouldNotBeNil)
			ShouldBeTrue(strings.Contains(err.Error(), "failed to get function configuration"))
			So(maxActiveQua, ShouldEqual, 0)
			So(maxBgInstQua, ShouldEqual, 0)
			patch.Reset()
		})
		Convey("Test GetRestrictByURN concurrently, only query etcd once", func() {
			concurrentInfo := &config.FunctionMetaInfo{
				FuncMetaData:     config.FuncMetaData{},
				S3MetaData:       config.S3MetaData{},
				EnvMetaData:      config.EnvMetaData{},
				ResourceMetaData: config.ResourceMetaData{},
				InstanceMetaData: config.InstanceMetaData{},
				ExtendedMetaData: config.ExtendedMetaData{},
			}
			concurrentInfo.FuncMetaData.IsBridgeFunction = true
			concurrentInfo.FuncMetaData.FunctionURN = "sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@test-java-snapshot-restore-coldstart-002"
			// assert queryCount == 1
			queryCount := 0
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(bgutils.GetValueFromMetaEtcd, func(funcKey string) ([]byte, error) {
					if funcKey == "/sn/functions/business/yrk/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@test-java-snapshot-restore-coldstart-002/version/latest" {
						queryCount += 1
						return json.Marshal(concurrentInfo)
					}
					return nil, errors.New("error occurs")
				}),
				gomonkey.ApplyFunc(decryptEnv, func(funcMeta *config.FunctionMetaInfo) (map[string]interface{}, error) {
					env := make(map[string]interface{})
					env[maxBaaSConnections] = "20"
					env[maxActive] = "5"
					return env, nil
				}),
			})

			times := 10
			wg := sync.WaitGroup{}
			wg.Add(times)
			for i := 0; i < times; i++ {
				go func() {
					bridgeConfigResp, _, err := b.GetRestrictByURN("sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@test-java-snapshot-restore-coldstart-002")
					maxBgInstQua, maxActiveQua := bridgeConfigResp.MaxStateKeyQua, bridgeConfigResp.MaxActiveQua
					if maxActiveQua != 5 || maxBgInstQua != 4 || err != nil {
						t.Errorf("GetRestrictByURN error")
					}
					wg.Done()
				}()
			}
			wg.Wait()

			So(queryCount, ShouldEqual, 1)
			patches.ResetAll()
		})
	})
}

func TestBridgeCache_GetStateIDInfoFromCache(t *testing.T) {
	Convey("Test get stateIDInfo from cache", t, func() {
		b := &BridgeCache{}
		stateIDInfo := getTestStateIDInfo()
		b.StateIDInfoCache.Store("ID1", stateIDInfo)
		b.StateIDInfoCache.Store("ID2", "not match type")
		Convey("Test get successfully", func() {
			getStateIDInfo, ok := b.GetStateIDInfoFromCache("ID1", "test")
			So(getStateIDInfo, ShouldNotBeNil)
			So(ok, ShouldBeTrue)
		})
		Convey("Test get not exist stateID", func() {
			getStateIDInfo, ok := b.GetStateIDInfoFromCache("ID3", "test")
			So(getStateIDInfo, ShouldBeNil)
			So(ok, ShouldBeFalse)
		})
		Convey("Test get exist stateID but type no match", func() {
			getStateIDInfo, ok := b.GetStateIDInfoFromCache("ID2", "test")
			So(getStateIDInfo, ShouldBeNil)
			So(ok, ShouldBeFalse)
		})
	})
}

func getTestClientInstance() *bgtypes.ClientInstance {
	return &bgtypes.ClientInstance{
		PodName: "pool0-xxxx",
		FuncInfo: &bgtypes.BridgeFuncInfo{
			TenantID:       "a",
			ServiceID:      "0@base",
			BridgeFuncName: "rds",
		},
	}
}

func getTestStateIDInfo() *StateIDInfo {
	return &StateIDInfo{
		StateKey:      "key",
		PodName:       "pool0-xxxx",
		RefCount:      1,
		BridgeFuncURN: "sn:cn:yrk:xxxx:function:0@base@rds",
		MaxBgInstQua:  10,
		MaxActiveQua:  10,
		BelongCluster: "cluster1",
	}
}

func getTestBridgeRestrict() *bridgeFuncRestrict {
	return &bridgeFuncRestrict{
		maxStateKeyQua: 1,
		maxActive:      1,
	}
}

func TestIsStateIDRelatedWithCurrentClientIns(t *testing.T) {
	Convey("Test IsStateIDRelatedWithCurrentClientIns", t, func() {
		b := &BridgeCache{}
		clientInstInfo := getTestClientInstance()
		b.ClientPodClientInstCache.Store(bgutils.GetClientCacheInfoKey(clientInstInfo), clientInstInfo)
		Convey("Test get successfully", func() {
			ok := b.IsStateIDRelatedWithCurrentClientIns(clientInstInfo, "rds")
			So(ok, ShouldBeFalse)
		})
		b.ClientPodClientInstCache.Store("ID2", bgtypes.ClientInstance{})
		Convey("Test get not exist stateID", func() {
			ok := b.IsStateIDRelatedWithCurrentClientIns(clientInstInfo, "rds")
			So(ok, ShouldBeFalse)
		})
	})
}

func Test_bridgeCache_UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(t *testing.T) {
	Convey("Test update clientInsInfo in cache", t, func() {
		b := &BridgeCache{}
		clientInsInfo := &bgtypes.ClientInstance{
			PodName:       "pool0-xxxx",
			AssignStateID: "111",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "a",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}
		Convey("Test update clientInsInfo plus one with existed stateID", func() {
			err := b.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(clientInsInfo, 1, "test")
			So(err, ShouldBeNil)
			getClientInsInfo, _ := b.GetClientInfoFromCache(clientInsInfo, "test")
			So(getClientInsInfo.FuncUseInfo[clientInsInfo.AssignStateID], ShouldEqual, 1)
		})

		funcUseInfo := make(map[string]int)
		funcUseInfo["111"] = -1
		b = &BridgeCache{}
		clientInsInfo = &bgtypes.ClientInstance{
			PodName:       "pool0-xxxx",
			AssignStateID: "111",
			FuncInfo: &bgtypes.BridgeFuncInfo{
				TenantID:       "a",
				ServiceID:      "0@base",
				BridgeFuncName: "rds",
			},
		}
		Convey("Test update refCount negative with no existed stateID1", func() {
			err := b.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(clientInsInfo, 1, "test")
			So(err, ShouldBeNil)
			_, ok := b.GetClientInfoFromCache(clientInsInfo, "test")
			So(ok, ShouldBeTrue)
		})
		Convey("Test update refCount negative exist stateID1", func() {
			b.ClientPodClientInstCache.Store("pool0-xxxx_a_rds", &bgtypes.ClientInstance{})
			err := b.UpdateClientInsBridgeFunUseInfoAndDeleteWhenEmpty(clientInsInfo, 1, "test")
			So(err, ShouldBeNil)
			_, ok := b.GetClientInfoFromCache(clientInsInfo, "test")
			So(ok, ShouldBeTrue)
		})
	})
}

func TestBridgeCache_RefreshStateInfoCache(t *testing.T) {
	Convey("Test refresh state info", t, func() {
		stateIDInfo := getTestStateIDInfo()
		stateID := "testStateID"
		b := &BridgeCache{}
		Convey("Case1 marshal failed", func() {
			patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return nil, errors.New("failed")
			})
			err := b.RefreshStateInfoCache(stateID, stateIDInfo)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("Case2 refresh successfully", func() {
			err := b.RefreshStateInfoCache(stateID, stateIDInfo)
			So(err, ShouldBeNil)
			stateIDInfoGet, _ := b.StateIDInfoCache.Load("testStateID")
			So(stateIDInfoGet.(*StateIDInfo).StateKey, ShouldEqual, "key")
		})
	})
}

func TestBridgeCache_ClearStateInfoCache(t *testing.T) {
	Convey("Test clear state info", t, func() {
		stateIDInfo := getTestStateIDInfo()
		stateID := "testStateID"
		b := &BridgeCache{}
		b.StateIDInfoCache.Store(stateID, stateIDInfo)
		err := b.ClearStateInfoCache(stateID)
		So(err, ShouldBeNil)
		_, ok := b.StateIDInfoCache.Load(stateID)
		So(ok, ShouldBeFalse)
	})
}

func TestBridgeCache_RefreshClientCache(t *testing.T) {
	Convey("Test refresh client cache", t, func() {
		clientInstance := getTestClientInstance()
		podName := "pool0-xxxx"
		bridgeFuncName := "rds"
		tenantId := "a"
		b := &BridgeCache{}
		Convey("Case1 marshal failed", func() {
			patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return nil, errors.New("failed")
			})
			err := b.RefreshClientCache(podName, clientInstance)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "[bridgeplugin] fail to marshal clientInstance info")
			patch.Reset()
		})
		Convey("Case2 refresh successfully", func() {
			err := b.RefreshClientCache(podName, clientInstance)
			So(err, ShouldBeNil)
			clientInstanceGet, _ := b.ClientPodClientInstCache.Load(podName + "_" + tenantId + "_" + bridgeFuncName)
			So(clientInstanceGet.(*bgtypes.ClientInstance).PodName, ShouldEqual, podName)
		})
	})
}

func TestBridgeCache_ClearClientCache(t *testing.T) {
	Convey("Test clear client info", t, func() {
		clientInstance := getTestClientInstance()
		podName := "pool0-xxxx"
		b := &BridgeCache{}
		b.ClientPodClientInstCache.Store(bgutils.GetClientCacheInfoKey(clientInstance), clientInstance)
		err := b.ClearClientCache(podName, clientInstance.FuncInfo.BridgeFuncName, clientInstance.FuncInfo.TenantID)
		So(err, ShouldBeNil)
		_, ok := b.ClientPodClientInstCache.Load(podName)
		So(ok, ShouldBeFalse)
	})
}

func TestBridgeCache_LoadClientCiteBridgeInfo(t *testing.T) {
	Convey("Test load bridge func info", t, func() {
		clientInstance := getTestClientInstance()
		b := &BridgeCache{}
		b.ClientPodClientInstCache.Store(bgutils.GetClientCacheInfoKey(clientInstance), clientInstance)
		b.LoadClientCiteBridgeInfo()
		v, ok := b.ClientCiteBridgeFuncMap.Load(clientInstance.PodName)
		So(ok, ShouldBeTrue)
		citeBridgeInfo, ok := v.([]string)
		So(ok, ShouldBeTrue)
		for _, val := range citeBridgeInfo {
			So("rds", ShouldEqual, val)
		}
	})
}

func TestBridgeCache_DeleteStateIDInfoETCDByBridgeInfo(t *testing.T) {
	Convey("Test Delete State ID Info ETCD By Bridge Info", t, func() {
		type fields struct {
			ClientPodClientInstCache sync.Map
			FuncNameInfoCache        sync.Map
			StateIDInfoCache         sync.Map
			StateIDLockMap           sync.Map
			ClientPodNameLockMap     sync.Map
			ClientCiteBridgeFuncMap  sync.Map
		}
		type args struct {
			tenantID        string
			complexFuncName string
			stateID         string
			traceID         string
		}
		b := &BridgeCache{
			ClientPodClientInstCache: sync.Map{},
			FuncNameInfoCache:        sync.Map{},
			StateIDInfoCache:         sync.Map{},
			StateIDLockMap:           sync.Map{},
			ClientPodNameLockMap:     sync.Map{},
			ClientCiteBridgeFuncMap:  sync.Map{},
		}
		a := args{
			tenantID:        "tenantID",
			complexFuncName: "complexFuncName",
			stateID:         "stateID",
			traceID:         "traceID",
		}
		Convey("Test delete with err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete error")
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b.DeleteStateIDInfoETCDByBridgeInfo(a.tenantID, a.complexFuncName, a.stateID, a.tenantID)
			So(err, ShouldNotBeNil)
			patch1.Reset()
			patch.Reset()
		})
		Convey("Test get client err", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete error")
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			err := b.DeleteStateIDInfoETCDByBridgeInfo(a.tenantID, a.complexFuncName, a.stateID, a.tenantID)
			So(err, ShouldNotBeNil)
			patch1.Reset()
			patch.Reset()
		})
		Convey("Test delete with success", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return nil
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b.DeleteStateIDInfoETCDByBridgeInfo(a.tenantID, a.complexFuncName, a.stateID, a.tenantID)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch.Reset()
		})
	})
}

func TestBridgeCache_RollbackUpdateStateIDInfoToEtcd(t *testing.T) {
	Convey("Test Delete State ID Info ETCD By Bridge Info", t, func() {
		type fields struct {
			ClientPodClientInstCache sync.Map
			FuncNameInfoCache        sync.Map
			StateIDInfoCache         sync.Map
			StateIDLockMap           sync.Map
			ClientPodNameLockMap     sync.Map
			ClientCiteBridgeFuncMap  sync.Map
		}
		type args struct {
			stateID string
			traceID string
		}
		b := &BridgeCache{
			ClientPodClientInstCache: sync.Map{},
			FuncNameInfoCache:        sync.Map{},
			StateIDInfoCache:         sync.Map{},
			StateIDLockMap:           sync.Map{},
			ClientPodNameLockMap:     sync.Map{},
			ClientCiteBridgeFuncMap:  sync.Map{},
		}
		a := args{
			stateID: "stateID",
			traceID: "traceID",
		}
		Convey("Test load state info nil", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete error")
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b.RollbackUpdateStateIDInfoToEtcd(a.stateID, a.traceID)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch.Reset()
		})
		Convey("Test convert state info error", func() {
			b1 := &BridgeCache{
				ClientPodClientInstCache: sync.Map{},
				FuncNameInfoCache:        sync.Map{},
				StateIDInfoCache:         sync.Map{},
				StateIDLockMap:           sync.Map{},
				ClientPodNameLockMap:     sync.Map{},
				ClientCiteBridgeFuncMap:  sync.Map{},
			}
			info := &BridgeCache{
				ClientPodClientInstCache: sync.Map{},
				FuncNameInfoCache:        sync.Map{},
				StateIDInfoCache:         sync.Map{},
				StateIDLockMap:           sync.Map{},
				ClientPodNameLockMap:     sync.Map{},
				ClientCiteBridgeFuncMap:  sync.Map{},
			}
			b1.StateIDInfoCache.Store("stateID", info)
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete error")
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b1.RollbackUpdateStateIDInfoToEtcd(a.stateID, a.traceID)
			So(err, ShouldNotBeNil)
			patch1.Reset()
			patch.Reset()
		})
		Convey("Test json marshal error", func() {
			b1 := &BridgeCache{
				ClientPodClientInstCache: sync.Map{},
				FuncNameInfoCache:        sync.Map{},
				StateIDInfoCache:         sync.Map{},
				StateIDLockMap:           sync.Map{},
				ClientPodNameLockMap:     sync.Map{},
				ClientCiteBridgeFuncMap:  sync.Map{},
			}
			info := &StateIDInfo{
				StateKey:          "StateKey",
				PodName:           "PodName",
				RefCount:          0,
				BridgeFuncURN:     "BridgeFuncURN",
				MaxBgInstQua:      0,
				MaxActiveQua:      0,
				BelongCluster:     "BelongCluster",
				LastModifyCluster: "LastModifyCluster",
			}
			b1.StateIDInfoCache.Store("stateID", info)
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete error")
				})
			patch := gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return nil, errors.New("marshal failed")
			})
			err := b1.RollbackUpdateStateIDInfoToEtcd(a.stateID, a.traceID)
			So(err, ShouldNotBeNil)
			patch1.Reset()
			patch.Reset()
		})
		Convey("Test get client error", func() {
			b1 := &BridgeCache{
				ClientPodClientInstCache: sync.Map{},
				FuncNameInfoCache:        sync.Map{},
				StateIDInfoCache:         sync.Map{},
				StateIDLockMap:           sync.Map{},
				ClientPodNameLockMap:     sync.Map{},
				ClientCiteBridgeFuncMap:  sync.Map{},
			}
			info := &StateIDInfo{
				StateKey:          "StateKey",
				PodName:           "PodName",
				RefCount:          0,
				BridgeFuncURN:     "BridgeFuncURN",
				MaxBgInstQua:      0,
				MaxActiveQua:      0,
				BelongCluster:     "BelongCluster",
				LastModifyCluster: "LastModifyCluster",
			}
			b1.StateIDInfoCache.Store("stateID", info)
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) error {
					return errors.New("delete error")
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			err := b1.RollbackUpdateStateIDInfoToEtcd(a.stateID, a.traceID)
			So(err, ShouldNotBeNil)
			patch1.Reset()
			patch.Reset()
		})
		Convey("Test put etcd error", func() {
			b1 := &BridgeCache{
				ClientPodClientInstCache: sync.Map{},
				FuncNameInfoCache:        sync.Map{},
				StateIDInfoCache:         sync.Map{},
				StateIDLockMap:           sync.Map{},
				ClientPodNameLockMap:     sync.Map{},
				ClientCiteBridgeFuncMap:  sync.Map{},
			}
			info := &StateIDInfo{
				StateKey:          "StateKey",
				PodName:           "PodName",
				RefCount:          0,
				BridgeFuncURN:     "BridgeFuncURN",
				MaxBgInstQua:      0,
				MaxActiveQua:      0,
				BelongCluster:     "BelongCluster",
				LastModifyCluster: "LastModifyCluster",
			}
			b1.StateIDInfoCache.Store("stateID", info)
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Put", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
					opts ...clientv3.OpOption) error {
					return errors.New("put error")
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b1.RollbackUpdateStateIDInfoToEtcd(a.stateID, a.traceID)
			So(err, ShouldNotBeNil)
			patch1.Reset()
			patch.Reset()
		})
		Convey("Test put etcd success", func() {
			b1 := &BridgeCache{
				ClientPodClientInstCache: sync.Map{},
				FuncNameInfoCache:        sync.Map{},
				StateIDInfoCache:         sync.Map{},
				StateIDLockMap:           sync.Map{},
				ClientPodNameLockMap:     sync.Map{},
				ClientCiteBridgeFuncMap:  sync.Map{},
			}
			info := &StateIDInfo{
				StateKey:          "StateKey",
				PodName:           "PodName",
				RefCount:          0,
				BridgeFuncURN:     "BridgeFuncURN",
				MaxBgInstQua:      0,
				MaxActiveQua:      0,
				BelongCluster:     "BelongCluster",
				LastModifyCluster: "LastModifyCluster",
			}
			b1.StateIDInfoCache.Store("stateID", info)
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
				"Put", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string,
					opts ...clientv3.OpOption) error {
					return nil
				})
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			})
			err := b1.RollbackUpdateStateIDInfoToEtcd(a.stateID, a.traceID)
			So(err, ShouldBeNil)
			patch1.Reset()
			patch.Reset()
		})
	})
}

func TestBridgeCache_PullUpInstanceByBridgeManager(t *testing.T) {
	UpdateBridgeInstanceTimestamp("aprilyolies", "bridge-manager", time.Now().Second())
	assert.True(t, BridgeManagerCanPullUpInstance("aprilyolies"))
	DeleteBridgeInstanceTimestamp("aprilyolies")
	assert.True(t, len(pullUpBridgeInstanceTimestamp) == 0)
}
