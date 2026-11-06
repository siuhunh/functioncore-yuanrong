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

// Package servicediscovery is used for service discovery
package servicediscovery

import (
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/aliasroute"
	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/core-sdk/communication"
	"functioncore/pkg/core-sdk/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/remotescheduler"
	"functioncore/pkg/functionbus/utils"
	utils2 "functioncore/test/utils"
)

func TestWorkerObserver(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	GetRegistry().RegisterAddWorker(func(funcKeyWithResource, instanceID string, provider *Provider, isSynced bool) {})
	GetRegistry().RegisterDeleteWorker(func(funcKeyWithResource, instanceID string, provider *Provider, isSynced bool) {})
	GetRegistry().RegisterEnableNode(func(nodeIP string) {})
	GetRegistry().RegisterDisableNode(func(nodeIP string) {})
	common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	common.FConfig.LocalAuth = &localauth.AuthConfig{}

	workerObserver := &EtcdObserver{
		WatchPath:     common.WatchPathPrefixWithWorkers,
		Filter:        registry.WorkerPathFilter,
		SyncHandler:   registry.SyncedProviderOnObserve,
		UpdateHandler: registry.UpdateProviderOnObserve,
		DeleteHandler: registry.DeleteProviderOnObserve,
	}

	podKey := "/sn/workers/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/testFunc/version/$latest/defaultaz/defaultaz-#-pool1500500-5d9f678d8b-q79nv"
	podValue := `{
		"IP": "127.0.0.1",
		"Port": "8899",
		"status": "ready",
		"p2pPort": "8017",
		"nodeIP": "127.0.0.1",
		"nodePort": "8899",
        "businessType": "CAE"
	}`
	funcKey := "i1fe539427b24702acc11fbb4e134e17/testFunc/$latest"
	podName := "defaultaz-#-pool1500500-5d9f678d8b-q79nv"

	assert.Equal(t, common.WatchPathPrefixWithWorkers, workerObserver.Object())
	assert.Equal(t, true, workerObserver.Filter(podKey))

	event := &etcd3.Event{Key: podKey, Value: []byte(podValue)}
	workerObserver.UpdateHandler(event)
	time.Sleep(1 * time.Second)
	var exist bool
	provider, exist := registry.Providers[funcKey][podName]
	assert.Equal(t, true, exist)
	assert.Equal(t, "CAE", provider.BusinessType)

	workerObserver.DeleteHandler(event)
	time.Sleep(1 * time.Second)

	_, exist = registry.Providers[funcKey][podName]
	assert.Equal(t, false, exist)

	workerObserver.SyncHandler(event)
	assert.Equal(t, true, registry.ProvidersHasSynced)
}

func TestSchedulerObserver(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	GetRegistry().RegisterAddWorker(func(funcKeyWithResource, instanceID string, provider *Provider, isSynced bool) {})
	GetRegistry().RegisterDeleteWorker(func(funcKeyWithResource, instanceID string, provider *Provider, isSynced bool) {})
	GetRegistry().RegisterEnableNode(func(nodeIP string) {})
	GetRegistry().RegisterDisableNode(func(nodeIP string) {})
	common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	remotescheduler.CreateInstanceManager(make(chan struct{}))
	common.FConfig.LocalAuth = &localauth.AuthConfig{}

	result := make(map[string]*commontypes.FaaSServiceInfo)

	patches := []*Patches{
		gomonkey.ApplyFunc((*remotescheduler.Manager).AddScheduler,
			func(m *remotescheduler.Manager, key string, info *commontypes.FaaSServiceInfo) {
				result[key] = info
			}),
		gomonkey.ApplyFunc((*remotescheduler.Manager).DeleteScheduler, func(m *remotescheduler.Manager, key string) {
			delete(result, key)
		}),
	}
	defer func() {
		for _, patch := range patches {
			patch.Reset()
		}
	}()

	schedulerObserver := &EtcdObserver{
		WatchPath:     common.WatchPathSchedulerPrefix,
		Filter:        registry.FaaSSchedulerPathFilter,
		SyncHandler:   registry.UpdateSchedulerOnObserve,
		UpdateHandler: registry.UpdateSchedulerOnObserve,
		DeleteHandler: registry.DeleteSchedulerOnObserve,
	}

	schedulerKey := "/sn/faas-scheduler/instances/cluster001/10.28.88.205/faas-scheduler-869c9d8df9-fkt5w"
	schedulerValue := `{
		"instanceID": "faas-scheduler-869c9d8df9-fkt5w",
		"nodeIP": "10.28.88.205",
		"ip": "192.168.47.75",
		"port": "8889",
		"runningMode": ""
	}`

	assert.Equal(t, common.WatchPathSchedulerPrefix, schedulerObserver.Object())
	assert.Equal(t, true, schedulerObserver.Filter(schedulerKey))

	event := &etcd3.Event{Key: schedulerKey, PrevValue: []byte(schedulerValue), Value: []byte(schedulerValue)}

	schedulerObserver.SyncHandler(event)
	assert.Equal(t, 1, len(result))
	assert.NotNil(t, result["faas-scheduler-869c9d8df9-fkt5w"])

	schedulerObserver.UpdateHandler(event)
	assert.Equal(t, 1, len(result))
	assert.NotNil(t, result["faas-scheduler-869c9d8df9-fkt5w"])

	schedulerObserver.DeleteHandler(event)
	assert.Equal(t, 0, len(result))
	assert.Nil(t, result["faas-scheduler-869c9d8df9-fkt5w"])
}

func TestStateObserver(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})

	stateObserver := &EtcdObserver{
		WatchPath:     common.WatchPathPrefixWithStateWorkers,
		Filter:        registry.StatePathFilter,
		SyncHandler:   registry.SyncedProviderOnObserve,
		UpdateHandler: registry.UpdateStateOnObserve,
		DeleteHandler: registry.DeleteStateOnObserve,
	}

	stateKey := "/sn/stateworkers/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/testFunc/version/$latest/defaultaz/6c007fcd-107d-4561-938a-4af5750d865e"
	stateValue := `{"6c007fcd-107d-4561-938a-4af5750d865e":"defaultaz-#-pool1500500-5d9f678d8b-q79nv"}`
	stateID := "6c007fcd-107d-4561-938a-4af5750d865e"

	assert.Equal(t, common.WatchPathPrefixWithStateWorkers, stateObserver.Object())
	assert.Equal(t, true, stateObserver.Filter(stateKey))

	updateEvent := &etcd3.Event{Key: stateKey, Value: []byte(stateValue)}
	stateObserver.UpdateHandler(updateEvent)
	time.Sleep(1 * time.Second)
	var exist bool
	_, exist = registry.States.Load(stateID)
	assert.Equal(t, true, exist)
	deleteEvent := &etcd3.Event{Key: stateKey, PrevValue: []byte(stateValue)}
	stateObserver.DeleteHandler(deleteEvent)
	time.Sleep(1 * time.Second)
	_, exist = registry.States.Load(stateID)
	assert.Equal(t, false, exist)

	stateObserver.SyncHandler(updateEvent)
	assert.Equal(t, true, registry.ProvidersHasSynced)
}

func TestAliasObserver(t *testing.T) {
	aliasKey := "/sn/aliases/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/functionc/altest"
	aliasValue := `{"aliasUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:altest","description":"","functionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc","functionVersion":"$latest","functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:$latest","name":"altest","revisionId":"20200511112430505","routingconfig":[{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:1","weight":30},{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:$latest","weight":70}]}`
	aliasURN := "sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:altest"

	aliasObserver := &EtcdObserver{
		WatchPath:     common.WatchPathPrefixWithAliase,
		Filter:        aliasroute.IsAlias,
		UpdateHandler: CreateAliasOnObserve,
		DeleteHandler: DeleteAliasOnObserve,
	}

	assert.Equal(t, common.WatchPathPrefixWithAliase, aliasObserver.Object())

	updateEvent := &etcd3.Event{Value: []byte(aliasValue)}
	aliasObserver.UpdateHandler(updateEvent)
	time.Sleep(1 * time.Second)
	var exist bool
	// test add alias
	_, exist = aliasroute.AliasRoute.AliasMap.Load(aliasURN)
	assert.Equal(t, true, exist)
	if !exist {
		t.Errorf("alias URN should be created but not found: %+v", aliasroute.AliasRoute.AliasMap)
		aliasroute.AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
			t.Errorf("key: %v", k)
			t.Errorf("value: %+v", v)
			return true
		})
	}

	deleteEvent := &etcd3.Event{Key: aliasKey}
	aliasObserver.DeleteHandler(deleteEvent)
	time.Sleep(1 * time.Second)
	_, exist = aliasroute.AliasRoute.AliasMap.Load(aliasURN)
	assert.Equal(t, false, exist)
	if exist {
		t.Errorf("alias URN should be deleted but found")
		aliasroute.AliasRoute.AliasMap.Range(func(k, v interface{}) bool {
			t.Errorf("key: %v", k)
			t.Errorf("value: %+v", v)
			return true
		})
	}
}

func TestFunctionObserver(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})

	functionObserver := &EtcdObserver{
		WatchPath:     common.WatchPathPrefixWithFunction,
		Filter:        registry.FuncPathFilter,
		UpdateHandler: registry.UpdateFuncOnObserver,
		DeleteHandler: registry.DeleteFuncOnObserver,
	}
	functionKey := "/sn/functions/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/functionc/version/"
	functionURN := "i1fe539427b24702acc11fbb4e134e17/functionc/$latest"

	convey.Convey("TestFunctionObserver", t, func() {
		convey.Convey("URN invalid", func() {
			functionValue := `{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181"},"instanceMetaData":{"concurrentNum":9}}`
			updateEvent := &etcd3.Event{Key: functionKey, Value: []byte(functionValue)}
			functionObserver.UpdateHandler(updateEvent)
			time.Sleep(1 * time.Second)
			_, exist := registry.FuncMeta[functionURN]
			assert.Equal(t, false, exist)
		})
		convey.Convey("no version", func() {
			functionValue := `{"funcMetaData":{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181"},"instanceMetaData":{"concurrentNum":9}}`
			updateEvent := &etcd3.Event{Key: functionKey, Value: []byte(functionValue)}
			functionObserver.UpdateHandler(updateEvent)
			time.Sleep(1 * time.Second)
			_, exist := registry.FuncMeta[functionURN]
			assert.Equal(t, false, exist)
		})
		convey.Convey("no concurrentNum", func() {
			functionValue := `{"funcMetaData":{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181"}}`
			updateEvent := &etcd3.Event{Key: functionKey, Value: []byte(functionValue)}
			functionObserver.UpdateHandler(updateEvent)
			time.Sleep(1 * time.Second)
			_, exist := registry.FuncMeta[functionURN]
			assert.Equal(t, false, exist)
		})
		convey.Convey("success", func() {
			functionKey := "/sn/functions/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/functionc/version/$latest"
			functionValue := `{"funcMetaData":{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:$latest","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181"},"instanceMetaData":{"concurrentNum":9}}`
			assert.Equal(t, true, functionObserver.Filter(functionKey))

			updateEvent := &etcd3.Event{Key: functionKey, Value: []byte(functionValue)}
			functionObserver.UpdateHandler(updateEvent)
			time.Sleep(1 * time.Second)

			_, exist := registry.FuncMeta[functionURN]
			assert.Equal(t, true, exist)
			if !exist {
				t.Errorf("function mate should be create but not found in registry: %+v", registry.FuncMeta)
			}

			functionObserver.DeleteHandler(updateEvent)
			time.Sleep(1 * time.Second)
			_, exist = registry.FuncMeta[functionURN]
			assert.Equal(t, false, exist)
			if exist {
				t.Errorf("function mate should be delete but found in registry")
			}
		})
		convey.Convey("sync load functionMeta", func() {
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				functionKey := "/sn/functions/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/functionc/version/$latest"
				functionValue := `{"funcMetaData":{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:$latest","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181"},
				"instanceMetaData":{"concurrentNum":9},"resourceMetaData":{"cpu":1,"memory":256}}`
				assert.Equal(t, true, functionObserver.Filter(functionKey))

				updateEvent := &etcd3.Event{Key: functionKey, Value: []byte(functionValue)}
				time.Sleep(100 * time.Millisecond)
				functionObserver.UpdateHandler(updateEvent)
				heckedActual, exist := registry.FuncMeta[functionURN]
				assert.Equal(t, true, exist)
				if !exist {
					t.Errorf("function mate should be create but not found in registry: %+v", registry.FuncMeta)
				}
				if heckedActual.ResourceMetaData.CPU != 1 && heckedActual.ResourceMetaData.Memory != 256 {
					t.Errorf("checkSynced error ")
				}
				wg.Done()
			}()
			functionKey := "/sn/functions/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/functionc/version/$latest"
			functionValue := `{"funcMetaData":{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:$latest","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181"},"instanceMetaData":{"concurrentNum":9}}`
			assert.Equal(t, true, functionObserver.Filter(functionKey))

			updateEvent := &etcd3.Event{Key: functionKey, Value: []byte(functionValue)}
			functionObserver.UpdateHandler(updateEvent)
			_, exist := registry.FuncMeta[functionURN]
			assert.Equal(t, true, exist)
			if !exist {
				t.Errorf("function mate should be create but not found in registry: %+v", registry.FuncMeta)
			}
			wg.Wait()
			convey.Convey("success: cae", func() {
				functionKey := "/sn/functions/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/functionc/version/$latest"
				functionValue := `{"funcMetaData":{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:$latest","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181","businessType":"CAE"},"instanceMetaData":{"concurrentNum":9}}`
				assert.Equal(t, true, functionObserver.Filter(functionKey))

				updateEvent := &etcd3.Event{Key: functionKey, Value: []byte(functionValue)}
				functionObserver.UpdateHandler(updateEvent)
				time.Sleep(1 * time.Second)

				_, exist := registry.FuncMeta[functionURN]
				assert.Equal(t, true, exist)
				if !exist {
					t.Errorf("function mate should be create but not found in registry: %+v", registry.FuncMeta)
				}

				funcVersionKey := "i1fe539427b24702acc11fbb4e134e17/functionc"
				_, exist = registry.CAEFuncVersion.Load(funcVersionKey)
				assert.Equal(t, true, exist)
				if !exist {
					t.Errorf("function version should be create but not found in registry: %+v", registry.CAEFuncVersion)
				}

				functionKey2 := "/sn/functions/business/hag/tenant/i1fe539427b24702acc11fbb4e134e17/function/functionc/version/$xxx"
				functionValue2 := `{"funcMetaData":{"functionVersionUrn":"sn:cn:hag:i1fe539427b24702acc11fbb4e134e17:function:functionc:$xxx","created":"2020-03-06 08:24:19.181 UTC","revisionId":"20210306082419181","businessType":"CAE"},"instanceMetaData":{"concurrentNum":9}}`
				updateEvent2 := &etcd3.Event{Key: functionKey2, Value: []byte(functionValue2)}
				functionObserver.UpdateHandler(updateEvent2)
				time.Sleep(1 * time.Second)

				functionURN2 := "i1fe539427b24702acc11fbb4e134e17/functionc/$xxx"
				_, exist = registry.FuncMeta[functionURN2]
				assert.Equal(t, true, exist)
				if !exist {
					t.Errorf("function mate should be create but not found in registry: %+v", registry.FuncMeta)
				}

				_, exist = registry.CAEFuncVersion.Load(funcVersionKey)
				assert.Equal(t, true, exist)
				if !exist {
					t.Errorf("function version should be create but not found in registry: %+v", registry.CAEFuncVersion)
				}

				functionObserver.DeleteHandler(updateEvent)
				time.Sleep(1 * time.Second)
				_, exist = registry.FuncMeta[functionURN]
				assert.Equal(t, false, exist)
				if exist {
					t.Errorf("function mate should be delete but found in registry")
				}

				_, exist = registry.CAEFuncVersion.Load(funcVersionKey)
				assert.Equal(t, true, exist)
				if !exist {
					t.Errorf("function latest version should be not delete but version XXX still exist")
				}

				functionObserver.DeleteHandler(updateEvent2)
				time.Sleep(1 * time.Second)
				_, exist = registry.FuncMeta[functionURN2]
				if exist {
					t.Errorf("function mate should be delete but found in registry")
				}

				_, exist = registry.CAEFuncVersion.Load(funcVersionKey)
				assert.Equal(t, false, exist)
				if exist {
					t.Errorf("function version should be delete but found in registry")
				}
			})
		})
	})
}

func TestGetResourceValue(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	convey.Convey("TestGetResourceValue", t, func() {
		convey.Convey("load meta from cache", func() {
			info := FunctionMetaInfo{
				ResourceMetaData: ResourceMetaData{
					CPU:    100,
					Memory: 100,
				},
			}
			registry.FuncMeta["resourceTestKey_fromCache"] = &info
			value, err := registry.GetResourceValue("resourceTestKey_fromCache")
			assert.Nil(t, err)
			assert.Equal(t, 100, value.CPU)
			assert.Equal(t, 100, value.Memory)
		})
		convey.Convey("load meta from etcd", func() {
			convey.Convey("failed to load meta", func() {
				defer ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
					return nil, errors.New("")
				}).Reset()
				_, err := registry.GetResourceValue("resourceTestKey_fromETCD_error")
				assert.NotNil(t, err)
			})
			convey.Convey("store meta from etcd", func() {
				defer ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
					info := FunctionMetaInfo{
						ResourceMetaData: ResourceMetaData{
							CPU:    100,
							Memory: 100,
						},
					}
					return json.Marshal(info)
				}).Reset()
				value, err := registry.GetResourceValue("resourceTestKey_fromETCD_success")
				assert.Nil(t, err)
				assert.Equal(t, 100, value.CPU)
				assert.Equal(t, 100, value.Memory)
			})
		})
	})
}

func TestRegistry_GetStatefulFunctionConcurrentNum(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	convey.Convey("TestRegistry_GetStatefulFunctionConcurrentNum", t, func() {
		convey.Convey("json marshal error", func() {
			defer ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("json marshal error")
			}).Reset()
			value, err := registry.GetStatefulFunctionConcurrentNum("testTenantID", "testFuncName", "latest")
			convey.So(err, convey.ShouldBeError)
			assert.Equal(t, 1, value)
		})
		convey.Convey("load success", func() {
			info := FunctionMetaInfo{
				FuncMetaData:     FuncMetaData{},
				InstanceMetaData: InstanceMetaData{ConcurrentNum: 3},
			}
			registry.FuncMeta["testTenantID/testServiceID@testFuncName/latest"] = &info
			value, err := registry.GetStatefulFunctionConcurrentNum("testTenantID", "testFuncName", "testServiceID")
			assert.Nil(t, err)
			assert.Equal(t, 3, value)
		})
	})
}

func TestRegistry_GetFunctionInstanceMetadata(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	convey.Convey("TestRegistry_GetFunctionInstanceMetadata", t, func() {
		convey.Convey("json error", func() {
			defer ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				info := FunctionMetaInfo{
					FuncMetaData:     FuncMetaData{},
					InstanceMetaData: InstanceMetaData{ConcurrentNum: 9},
				}
				return json.Marshal(info)
			}).Reset()
			value, err := registry.GetFunctionInstanceMetadata("concurrencyKey_fromETCD")
			assert.Nil(t, err)
			assert.Equal(t, 9, value.ConcurrentNum)
		})
		convey.Convey("json marshal error", func() {
			path := ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("json marshal error")
			})
			defer path.Reset()
			_, err := registry.GetFunctionInstanceMetadata("concurrencyKey_fromETCD_test")
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("load FuncMeta", func() {
			info := FunctionMetaInfo{
				FuncMetaData:     FuncMetaData{},
				InstanceMetaData: InstanceMetaData{ConcurrentNum: 3},
			}
			registry.FuncMeta["concurrencyKey_fromCache"] = &info
			value, err := registry.GetFunctionInstanceMetadata("concurrencyKey_fromCache")
			assert.Nil(t, err)
			assert.Equal(t, 3, value.ConcurrentNum)
		})
	})
}

func TestRegistry_GetFunctionMetadata(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	convey.Convey("TestRegistry_GetFunctionMetadata", t, func() {
		convey.Convey("json error", func() {
			defer ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				info := FunctionMetaInfo{
					FuncMetaData:     FuncMetaData{},
					InstanceMetaData: InstanceMetaData{ConcurrentNum: 9},
				}
				return json.Marshal(info)
			}).Reset()
			value, err := registry.GetFunctionMetadata("concurrencyKey_fromETCD")
			assert.Nil(t, err)
			assert.Equal(t, 9, value.InstanceMetaData.ConcurrentNum)
		})
		convey.Convey("json marshal error", func() {
			path := ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("json marshal error")
			})
			defer path.Reset()
			_, err := registry.GetFunctionMetadata("concurrencyKey_fromETCD_test")
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("load FuncMeta", func() {
			info := FunctionMetaInfo{
				FuncMetaData:     FuncMetaData{},
				InstanceMetaData: InstanceMetaData{ConcurrentNum: 3},
			}
			registry.FuncMeta["concurrencyKey_fromCache"] = &info
			value, err := registry.GetFunctionMetadata("concurrencyKey_fromCache")
			assert.Nil(t, err)
			assert.Equal(t, 3, value.InstanceMetaData.ConcurrentNum)
		})
	})
}

func TestRegistry_GetDirectFunctionMetadata(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	convey.Convey("TestRegistry_GetDirectFunctionMetadata", t, func() {
		convey.Convey("json error", func() {
			defer ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				info := FunctionMetaInfo{
					FuncMetaData:     FuncMetaData{},
					InstanceMetaData: InstanceMetaData{ConcurrentNum: 9},
				}
				return json.Marshal(info)
			}).Reset()
			value, err := registry.GetDirectFunctionMetadata("concurrencyKey_fromETCD")
			assert.Nil(t, err)
			assert.Equal(t, 9, value.InstanceMetaData.ConcurrentNum)
		})
		convey.Convey("json marshal error", func() {
			path := ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("json marshal error")
			})
			defer path.Reset()
			_, err := registry.GetDirectFunctionMetadata("concurrencyKey_fromETCD_test")
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("load FuncMeta", func() {
			info := FunctionMetaInfo{
				FuncMetaData:     FuncMetaData{},
				InstanceMetaData: InstanceMetaData{ConcurrentNum: 3},
			}
			registry.FuncMeta["concurrencyKey_fromCache"] = &info
			value, err := registry.GetDirectFunctionMetadata("concurrencyKey_fromCache")
			assert.Nil(t, err)
			assert.Equal(t, 3, value.InstanceMetaData.ConcurrentNum)
		})
	})
}

func TestCheckWatchEvent(t *testing.T) {
	convey.Convey("TestCheckWatchEvent", t, func() {
		convey.Convey("success", func() {
			invalidIpInstance := InstanceEvent{}
			invalidPortInstance := InstanceEvent{NodeIP: "127.0.0.1", NodePort: "65536"}
			invalidP2PPortInstance := InstanceEvent{NodeIP: "127.0.0.1", NodePort: "8080", P2pPort: "65536"}
			invalidCPUInstance := InstanceEvent{NodeIP: "127.0.0.1", NodePort: "8080", P2pPort: "8080", CPU: -1}
			invalidMemoryInstance := InstanceEvent{
				NodeIP:   "127.0.0.1",
				NodePort: "8080",
				P2pPort:  "8080",
				CPU:      4,
				Memory:   -1,
			}
			successInstance := InstanceEvent{
				NodeIP:   "127.0.0.1",
				NodePort: "8080",
				P2pPort:  "8080",
				CPU:      4,
				Memory:   1024,
			}
			tests := []struct {
				name  string
				event InstanceEvent
				want  bool
			}{
				{"invalid ip", invalidIpInstance, false},
				{"invalid port", invalidPortInstance, false},
				{"invalid p2pPort", invalidP2PPortInstance, false},
				{"invalid cpu", invalidCPUInstance, false},
				{"invalid memory", invalidMemoryInstance, false},
				{"success", successInstance, true},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					actual := checkWatchEvent(tt.event)
					assert.Equal(t, tt.want, actual)
				})
			}
		})
	})
}

func TestFunctionCheck(t *testing.T) {
	r := GetRegistry()
	r.FuncMeta["tenant1/0@service1@funcname1/version1"] = &FunctionMetaInfo{
		FuncMetaData: FuncMetaData{
			IsStatefulFunction: true,
		},
	}
	r.States.Store("stateID1", "instance1")
	convey.Convey("TestFunctionCheck", t, func() {
		convey.Convey("TestIsFunctionExist", func() {
			convey.ShouldBeTrue(r.IsFunctionExist(
				"tenant1", "0@service1", "funcname1", "version1"))
		})
		convey.Convey("isStateFulFunction", func() {
			convey.ShouldBeTrue(r.IsStatefulFunction(
				"tenant1", "0@service1", "funcname1", "version1"))
			patch := gomonkey.ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("test err")
			})
			defer func() {
				patch.Reset()
			}()
			convey.ShouldBeFalse(r.IsStatefulFunction(
				"tenant3", "0@service3", "funcname3", "version3"))
		})
		convey.Convey("isStateExist", func() {
			convey.ShouldBeTrue(r.IsStateInsExist("stateID1"))
		})
		convey.Convey("test: get node id by state id", func() {
			convey.So(r.GetNodeIDByStateID("stateID1"), convey.ShouldEqual, "instance1")
		})
		convey.Convey("TestIsFunctionExist: cae", func() {
			convey.ShouldBeTrue(r.IsFunctionExist(
				"tenant1", "0@service1", "funcname1", "version1"))
		})
		convey.Convey("isStateFulFunction: cae", func() {
			convey.ShouldBeFalse(r.IsStatefulFunction(
				"tenant1", "0@service1", "funcname1", "version1"))

			versionMap := make(map[string]struct{})
			versionMap["version1"] = struct{}{}
			_, exist := r.selectCAEFuncVersion("tenant1", "funcname1")
			convey.ShouldBeFalse(exist)

			r.CAEFuncVersion.Store("tenant1/0@service1@funcname1", versionMap)
			convey.ShouldBeTrue(r.IsStatefulFunction(
				"tenant1", "0@service1", "funcname1", "version1"))
			versionMap["version1"] = struct{}{}
			delete(versionMap, "version1")
			r.CAEFuncVersion.Store("tenant1/0@service1@funcname1", versionMap)
			convey.ShouldBeFalse(r.IsStatefulFunction(
				"tenant1", "0@service1", "funcname1", "version1"))

			versionMap2 := make(map[string]string)
			versionMap2["version1"] = "str"
			r.CAEFuncVersion.Store("tenant1/0@service1@funcname1", versionMap2)
			convey.ShouldBeFalse(r.IsStatefulFunction(
				"tenant1", "0@service1", "funcname1", "version1"))
		})
		convey.Convey("TestIsFunctionExist: getMetaFromEtcd", func() {
			patch := gomonkey.ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("test err")
			})
			defer func() {
				patch.Reset()
			}()
			convey.ShouldBeFalse(r.IsFunctionExist(
				"tenant10", "0@service10", "funcname10", "version10"))
		})
	})
}

func Test_setInfoFromInstanceID(t *testing.T) {
	convey.Convey("setInfoFromInstanceID", t, func() {
		convey.Convey("cpu error", func() {
			instanceID := "x-xx-xxx-2cpu-256"
			p := &Provider{}
			setInfoFromInstanceID(p, instanceID)
			convey.So(p.CPU, convey.ShouldEqual, 0)
		})
		convey.Convey("memory error", func() {
			instanceID := "x-xx-xxx-2-256memory"
			p := &Provider{}
			setInfoFromInstanceID(p, instanceID)
			convey.So(p.Memory, convey.ShouldEqual, 0)
		})
		convey.Convey("set success", func() {
			instanceID := "x-xx-xxx-2-256"
			p := &Provider{}
			setInfoFromInstanceID(p, instanceID)
			convey.So(p.CPU, convey.ShouldEqual, 2)
			convey.So(p.Memory, convey.ShouldEqual, 256)
		})
	})
}

func Test_makeProvider(t *testing.T) {
	convey.Convey("makeProvider", t, func() {
		convey.Convey("without cpu and memory", func() {
			event := InstanceEvent{
				CPU: 1,
			}
			provider := makeProvider(event, "123", "0/123")
			convey.So(provider.CPU, convey.ShouldEqual, 0)
			convey.So(provider.Memory, convey.ShouldEqual, 0)
		})
		convey.Convey("with cpu and memeory", func() {
			event := InstanceEvent{
				CPU:    1,
				Memory: 256,
			}
			provider := makeProvider(event, "123", "123")
			convey.So(provider.CPU, convey.ShouldEqual, 1)
			convey.So(provider.Memory, convey.ShouldEqual, 256)
		})
	})
}

func TestWorkerPathFilter(t *testing.T) {
	r := newRegistry()
	result1 := r.WorkerPathFilter("should/be/false")
	assert.Equal(t, result1, false)

	result2 := r.WorkerPathFilter("1/2/3/4/5/6/7/8/9/10/11/12/13")
	assert.Equal(t, result2, false)
}

func TestStatePathFilter(t *testing.T) {
	r := newRegistry()
	result1 := r.StatePathFilter("should/be/false")
	assert.Equal(t, result1, false)

	result2 := r.StatePathFilter("1/2/3/4/5/6/7/8/9/10/11/12/13")
	assert.Equal(t, result2, false)
}

func TestFuncPathFilter(t *testing.T) {
	r := newRegistry()
	result1 := r.FuncPathFilter("should/be/false")
	assert.Equal(t, result1, false)

	result2 := r.FuncPathFilter("1/2/3/4/5/6/7/8/9/10/11/12/13")
	assert.Equal(t, result2, false)
}

func Test_checkSynced(t *testing.T) {
	synced := checkSynced("test assert", FunctionMetaInfo{})
	assert.Equal(t, synced, false)
}

func Test_updateProviders(t *testing.T) {
	r := newRegistry()
	data := []byte{123}
	item := []string{"test"}
	result := r.updateProviders(data, item, etcd3.Router)
	assert.Equal(t, result, false)
}

func Test_isCAERouterEtcdOnline(t *testing.T) {
	result := isCAERouterEtcdOnline()
	assert.Equal(t, result, false)
}

func Test_updateProvidersSkip(t *testing.T) {
	convey.Convey("Test_updateProvidersSkip", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			}),
			gomonkey.ApplyFunc(isCAERouterEtcdOnline, func() bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		r := newRegistry()

		event := InstanceEvent{BusinessType: "CAE"}
		item := []string{"test"}
		result := r._updateProviders(event, item, etcd3.Router)
		assert.Equal(t, result, true)
	})
}

func Test_deleteProvidersSkip(t *testing.T) {
	convey.Convey("Test_deleteProvidersSkip", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(etcd3.GetCaeRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			}),
			gomonkey.ApplyFunc(isCAERouterEtcdOnline, func() bool {
				return true
			}),
			gomonkey.ApplyFunc(getFuncKey, func(items []string) string {
				return "functionKey"
			}),
			gomonkey.ApplyFunc(extractInstanceFromPodPath, func(items []string) string {
				return "instanceID"
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		r := newRegistry()
		event := InstanceEvent{BusinessType: "CAE"}
		r.Providers["functionKey"] = make(map[string]*Provider, mapInitSize)
		provider := makeProvider(event, "instanceID", "functionKey")
		r.Providers["functionKey"]["instanceID"] = provider
		item := []string{"test"}
		r.deleteProviders(item, etcd3.Router)
		_, ok := r.Providers["functionKey"]["instanceID"]
		assert.Equal(t, ok, true)
	})
	convey.Convey("Test_deleteNoFuncKey", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(getFuncKey, func(items []string) string {
				return "functionKey"
			}),
			gomonkey.ApplyFunc(extractInstanceFromPodPath, func(items []string) string {
				return "instanceID"
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		r := newRegistry()
		item := []string{"test"}
		r.deleteProviders(item, etcd3.Router)
		_, ok := r.Providers["functionKey"]["instanceID"]
		assert.Equal(t, ok, false)
		r.Providers["functionKey"] = make(map[string]*Provider, mapInitSize)
		r.deleteProviders(item, etcd3.Router)
		_, ok = r.Providers["functionKey"]["instanceID"]
		assert.Equal(t, ok, false)
	})
}

func Test_convertEvent(t *testing.T) {
	bytes := []byte{111}
	instanceEvent, b := convertEvent(bytes)
	assert.NotNil(t, instanceEvent)
	assert.Equal(t, b, false)
}

func Test_getFuncKey(t *testing.T) {
	items := []string{"1", "2", "3"}
	key := getFuncKey(items)
	assert.Equal(t, key, "")
}

func Test_extractInstanceFromPodPath(t *testing.T) {
	items := []string{"1", "2", "3"}
	path := extractInstanceFromPodPath(items)
	assert.Equal(t, path, "")
}

func TestRegistry_StateRecovery(t *testing.T) {
	called := false
	type fields struct {
		Providers          map[string]map[string]*Provider
		ProvidersMu        sync.RWMutex
		ProvidersHasSynced bool
		States             sync.Map
		FuncMeta           map[string]*FunctionMetaInfo
		callBack           func(string, interface{}, bool)
		addWorker          []operateWorker
		deleteWorker       operateWorker
		enableNode         operateNode
		disableNode        operateNode
	}
	type args struct {
		stateRecoveryConfig types.StateRecoveryConfig
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils2.PatchesFunc
		callWant    bool
	}{
		// test localExistInETCD != realExistInETCD && realExistInETCD == false
		{
			"case1",
			fields{},
			args{
				stateRecoveryConfig: types.StateRecoveryConfig{
					LocalExistInETCD: true,
					RealExistInETCD:  false,
				},
			},
			func() utils2.PatchSlice {
				return utils2.InitPatchSlice()
			},
			false,
		},
		// test localExistInETCD != realExistInETCD && realExistInETCD == true
		{
			"case2",
			fields{},
			args{
				stateRecoveryConfig: types.StateRecoveryConfig{
					LocalExistInETCD: false,
					RealExistInETCD:  true,
				},
			},
			func() utils2.PatchSlice {
				return utils2.InitPatchSlice()
			},
			false,
		},
		// test localExistInETCD == realExistInETCD && realExistInETCD == true
		{
			"case3",
			fields{},
			args{
				stateRecoveryConfig: types.StateRecoveryConfig{
					LocalExistInETCD: true,
					RealExistInETCD:  true,
				},
			},
			func() utils2.PatchSlice {
				patches := utils2.InitPatchSlice()
				patches.Append(utils2.PatchSlice{
					ApplyFunc(communication.DeleteStateByHTTP,
						func(stateID string, tenantID, proxyFunction string) snerror.SNError {
							called = true
							return nil
						}),
				})
				return patches
			},
			false,
		},
		// test localExistInETCD == realExistInETCD && realExistInETCD == false
		{
			"case4",
			fields{},
			args{
				stateRecoveryConfig: types.StateRecoveryConfig{
					LocalExistInETCD: false,
					RealExistInETCD:  false,
				},
			},
			func() utils2.PatchSlice {
				patches := utils2.InitPatchSlice()
				patches.Append(utils2.PatchSlice{
					ApplyFunc(communication.CreateStateByHTTP,
						func(funcName string, stateID string, config types.CreateStateConfig) (string, string,
							snerror.SNError) {
							called = true
							return "", "", nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &Registry{
				Providers:          tt.fields.Providers,
				ProvidersMu:        tt.fields.ProvidersMu,
				ProvidersHasSynced: tt.fields.ProvidersHasSynced,
				States:             tt.fields.States,
				FuncMeta:           tt.fields.FuncMeta,
				callBack:           tt.fields.callBack,
				addWorker:          tt.fields.addWorker,
				deleteWorker:       tt.fields.deleteWorker,
				enableNode:         tt.fields.enableNode,
				disableNode:        tt.fields.disableNode,
			}
			patches := tt.patchesFunc()
			r.StateRecovery(tt.args.stateRecoveryConfig)
			if called != tt.callWant {
				t.Errorf("StateRecovery failed, called : %v, want : %v", called, tt.callWant)
			}
			patches.ResetAll()
		})
	}
}

func TestRegistry_GetFunctionFuncMetaData(t *testing.T) {
	GetRegistry().RegisterCallBack(func(string, interface{}, bool) {})
	convey.Convey("TestRegistry_GetFunctionFuncMetaData", t, func() {
		convey.Convey("json error", func() {
			defer ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				info := FunctionMetaInfo{
					FuncMetaData:     FuncMetaData{IsBridgeFunction: false},
					InstanceMetaData: InstanceMetaData{ConcurrentNum: 9},
				}
				return json.Marshal(info)
			}).Reset()
			value, err := registry.GetFunctionFuncMetaData("isBridgeFunction_fromETCD")
			assert.Nil(t, err)
			assert.Equal(t, false, value.IsBridgeFunction)
		})
		convey.Convey("json marshal error", func() {
			path := ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
				return nil, errors.New("json marshal error")
			})
			defer path.Reset()
			_, err := registry.GetFunctionFuncMetaData("isBridgeFunction_fromETCD_test")
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("load FuncMeta", func() {
			info := FunctionMetaInfo{
				FuncMetaData:     FuncMetaData{IsBridgeFunction: true},
				InstanceMetaData: InstanceMetaData{ConcurrentNum: 3},
			}
			registry.FuncMeta["isBridgeFunction_fromCache"] = &info
			value, err := registry.GetFunctionFuncMetaData("isBridgeFunction_fromCache")
			assert.Nil(t, err)
			assert.Equal(t, true, value.IsBridgeFunction)
		})
	})
}

func TestRecoverLongRunWithRetry(t *testing.T) {
	count := 0
	sleepCount := 0
	patches := []*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(&Registry{}), "RecoverLongRun", func(_ *Registry) error {
			defer func() {
				count++
			}()
			if count == 0 {
				return errors.New("failed to recover")
			}
			return nil
		}),
		gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
			sleepCount++
			return
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	r := &Registry{}
	r.RecoverLongRunWithRetry()
	assert.Equal(t, 2, count)
	assert.Equal(t, 1, sleepCount)
}

func TestRecoverLongRun(t *testing.T) {
	g := gin.New()
	g.GET("/worker-manager/v1/functions/longrun/:nodeIP", func(c *gin.Context) {
		if c.Param("nodeIP") == "invalid" {
			c.Status(http.StatusOK)
			return
		}
		c.JSON(http.StatusOK, map[string]commontypes.InstanceRouter{"a": {}})
	})
	ts := httptest.NewServer(g)

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(wmhealth.GetWorkerManagerBaseURL, func() string {
			return ts.URL
		}),
		gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(r *http.Request) {
			return
		}),
		gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return &http.Client{}
		}),
	}
	defer func() {
		ts.Close()
		for i := range patches {
			patches[i].Reset()
		}
	}()

	r := &Registry{}
	r.recoverLongRun = func(v commontypes.InstanceRouter) {
		return
	}

	tests := []struct {
		name   string
		hostIP string

		hasErr bool
	}{
		{
			name:   "unmarshal failed",
			hostIP: "invalid",
			hasErr: true,
		},
		{
			name:   "invalid status code",
			hasErr: true,
		},
		{
			name:   "normail request",
			hostIP: "127.0.0.1",
			hasErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selfNodeIP = tt.hostIP
			err := r.RecoverLongRun()
			if tt.hasErr {
				assert.Error(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
			}
		})
	}
}

func TestRegistry_FuncMeta_update_delete_MetaFirst(t *testing.T) {
	funcKey := "/sn/functions/business/yrk/tenant/123456/function/0@default@hello/version/latest"
	mapKey := "123456/0@default@hello/latest"
	metaVal := &FunctionMetaInfo{Source: etcd3.Meta, FuncMetaData: FuncMetaData{Timeout: 30}}
	caeMetaVal := &FunctionMetaInfo{Source: etcd3.CAEMeta, FuncMetaData: FuncMetaData{Timeout: 10}}

	r := newRegistry()

	tests := []struct {
		name   string
		source etcd3.EtcdType
		data   *FunctionMetaInfo
		event  int // 0 add 1 delete

		expectedInCache bool
		expectedTimeout int64
	}{
		{
			name:   "cae add event",
			source: etcd3.CAEMeta,
			data:   caeMetaVal,

			expectedInCache: true,
			expectedTimeout: 10,
		},
		{
			name:   "meta add event",
			source: etcd3.Meta,
			data:   metaVal,

			expectedInCache: true,
			expectedTimeout: 30,
		},
		{
			name:   "cae add event, skip",
			source: etcd3.CAEMeta,
			data:   caeMetaVal,

			expectedInCache: true,
			expectedTimeout: 30,
		},
		{
			name:   "cae add delete, skip",
			source: etcd3.CAEMeta,
			event:  1,

			expectedInCache: true,
			expectedTimeout: 30,
		},
		{
			name:            "meta delete",
			source:          etcd3.Meta,
			event:           1,
			expectedInCache: false,
		},
		{
			name:   "cae add event",
			source: etcd3.CAEMeta,
			data:   caeMetaVal,

			expectedInCache: true,
			expectedTimeout: 10,
		},
		{
			name:   "cae delete event",
			source: etcd3.CAEMeta,
			event:  1,

			expectedInCache: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.event == 0 {
				r.checkAndUpdate(tt.source, mapKey, tt.data)
			} else {
				r.DeleteFuncOnObserver(&etcd3.Event{
					Key:    funcKey,
					Source: tt.source,
				})
			}

			r.funcMetaLock.Lock()
			data, ok := r.FuncMeta[mapKey]
			r.funcMetaLock.Unlock()

			assert.Equal(t, tt.expectedInCache, ok, tt.name)
			if tt.expectedInCache {
				assert.Equal(t, tt.expectedTimeout, data.FuncMetaData.Timeout)
			}
		})
	}
}

func TestUpdateFuncAvailableCluster(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/function/available/clusters/urn:fss:cn-north-6:580943580943580943:function:debugservice:hello-world:latest",
		Value: []byte(`["cluster1", "cluster2"]`),
	}
	r.UpdateFuncAvailableCluster(ev)
	assert.Equal(t, len(r.FuncAvailableClusters), 1)

}

func TestUpdateFuncAvailableClusterMarshalErr(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/function/available/clusters/urn:fss:cn-north-6:580943580943580943:function:debugservice:hello-world:latest",
		Value: []byte(`x["cluster1", "cluster2"]`),
	}
	r.UpdateFuncAvailableCluster(ev)
	assert.Equal(t, len(r.FuncAvailableClusters), 0)
}

func TestUpdateFuncAvailableClusterKeyErr(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/function/available/clusters/sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world/latest",
		Value: []byte(`["cluster1", "cluster2"]`),
	}
	r.UpdateFuncAvailableCluster(ev)
	assert.Equal(t, len(r.FuncAvailableClusters), 0)
}

func TestUpdateFrontendInstances(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z",
		Value: []byte(`active`),
	}
	r.UpdateFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 1)
}

func TestGetAvailableClusters(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/function/available/clusters/urn:fss:cn-north-6:580943580943580943:function:debugservice:hello-world:latest",
		Value: []byte(`["cluster1", "cluster2"]`),
	}
	r.UpdateFuncAvailableCluster(ev)
	assert.Equal(t, len(r.GetAvailableClusters("sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world")), 2)
}

func TestUpdateFrontendInstancesStatusChange(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z",
		Value: []byte(`active`),
	}
	r.UpdateFrontendInstances(ev)
	ev = &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.121/frontend-768df8f66b-gvz4z",
		Value: []byte(`active`),
	}
	r.UpdateFrontendInstances(ev)
	ev = &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z",
		Value: []byte(`not active`),
	}
	r.UpdateFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 1)
}

func TestUpdateFrontendInstancesKeyErr(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z/xxx",
		Value: []byte(`active`),
	}
	r.UpdateFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 0)
}

func TestDeleteFuncAvailableCluster(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/function/available/clusters/urn:fss:cn-north-6:580943580943580943:function:debugservice:hello-world:latest",
		Value: []byte(`["cluster1", "cluster2"]`),
	}
	r.UpdateFuncAvailableCluster(ev)
	assert.Equal(t, len(r.FuncAvailableClusters), 1)
	r.DeleteFuncAvailableCluster(ev)
	assert.Equal(t, len(r.FuncAvailableClusters), 0)
}

func TestDeleteFuncAvailableClusterKeyErr(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/function/available/clusters/urn:fss:cn-north-6:580943580943580943:function:debugservice:hello-world:latest",
		Value: []byte(`["cluster1", "cluster2"]`),
	}
	r.UpdateFuncAvailableCluster(ev)
	assert.Equal(t, len(r.FuncAvailableClusters), 1)
	ev = &etcd3.Event{
		Key:   "/sn/function/available/clusters/sn:cn:yrk:580943580943580943:function:0@debugservice@hello-world/latest",
		Value: []byte(`["cluster1", "cluster2"]`),
	}
	r.DeleteFuncAvailableCluster(ev)
	assert.Equal(t, len(r.FuncAvailableClusters), 1)
}

func TestDeleteFrontendInstances(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z",
		Value: []byte(`active`),
	}
	r.UpdateFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 1)
	r.DeleteFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 0)
}

func TestDeleteFrontendInstances1(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z",
		Value: []byte(`active`),
	}
	ev1 := &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.121/frontend-768df8f66b-gvz4z",
		Value: []byte(`active`),
	}
	r.UpdateFrontendInstances(ev)
	r.UpdateFrontendInstances(ev1)
	assert.Equal(t, len(r.ClusterFrontends), 1)
	r.DeleteFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 1)
}

func TestDeleteFrontendInstancesKeyErr(t *testing.T) {
	r := newRegistry()
	ev := &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z",
		Value: []byte(`active`),
	}
	r.UpdateFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 1)
	ev = &etcd3.Event{
		Key:   "/sn/frontend/instances/cluster001/7.218.74.120/frontend-768df8f66b-gvz4z/latest",
		Value: []byte(`active`),
	}
	r.DeleteFrontendInstances(ev)
	assert.Equal(t, len(r.ClusterFrontends), 1)
}

func TestRegistry_getInstanceResource(t *testing.T) {
	r := newRegistry()

	tests := []struct {
		name     string
		provider *Provider

		expected *commontypes.InstanceResource
	}{
		{
			name:     "faas func",
			provider: &Provider{CPU: 300, Memory: 128},

			expected: &commontypes.InstanceResource{CPU: "300", Memory: "128"},
		},
		{
			name:     "cae func",
			provider: &Provider{BusinessType: "CAE", CPU: 300, Memory: 128},

			expected: &commontypes.InstanceResource{CPU: "0", Memory: "0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, r.getInstanceResource(tt.provider), tt.name)
		})
	}
}

type MockReadCloser struct{}

func (m MockReadCloser) Close() error {
	return nil
}

func (m MockReadCloser) Read([]byte) (int, error) {
	return 0, nil
}

func TestGetFuncMetaInfo(t *testing.T) {
	convey.Convey("TestGetFuncMetaInfo from local cache", t, func() {
		registry = newRegistry()
		registry.FuncMeta["7e1*86a/0@base@testsurgecaeinvokecangjienested-cae/latest"] = &FunctionMetaInfo{}
		funcMetaInfo, err := registry.GetFuncMetaInfo("7e1*86a/0@base@testsurgecaeinvokecangjienested-cae/latest/0-0")
		convey.So(funcMetaInfo, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("TestGetFuncMetaInfo from worker-manager", t, func() {
		client := &http.Client{}
		resp := &http.Response{
			StatusCode: 200,
			Body:       &MockReadCloser{},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(client), "Do",
				func(c *http.Client, req *http.Request) (*http.Response, error) {
					return resp, nil
				}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return json.Marshal(&workermanager.FuncMetaBaseInfoResp{})
			}),
			gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return client
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		registry = newRegistry()
		funcMetaInfo, err := registry.GetFuncMetaInfo("7e1*86a/0@base@testsurgecaeinvokecangjienested-cae/latest/0-0")
		convey.So(funcMetaInfo, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("TestGetFuncMetaInfo GetFuncMetaBaseInfoByFuncKeyWithResource error", t, func() {
		client := &http.Client{}
		resp := &http.Response{
			StatusCode: 200,
			Body:       &MockReadCloser{},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(client), "Do",
				func(c *http.Client, req *http.Request) (*http.Response, error) {
					return resp, nil
				}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return json.Marshal(&workermanager.FuncMetaBaseInfoResp{})
			}),
			gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return client
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		registry = newRegistry()
		funcMetaInfo, err := registry.GetFuncMetaInfo("7e1*86a/0@testsurgecaeinvokecangjienested-cae/latest/0-0")
		convey.So(funcMetaInfo, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestGetFuncMetaInfo from worker-manager ErrMsg is FuncMetaNotFound", t, func() {
		client := &http.Client{}
		resp := &http.Response{
			StatusCode: 200,
			Body:       &MockReadCloser{},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(client), "Do",
				func(c *http.Client, req *http.Request) (*http.Response, error) {
					return resp, nil
				}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return json.Marshal(&workermanager.FuncMetaBaseInfoResp{
					ErrMsg: snerror.ErrText(snerror.FuncMetaNotFound),
				})
			}),
			gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return client
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		registry = newRegistry()
		funcMetaInfo, err := registry.GetFuncMetaInfo("7e1*86a/0@base@testsurgecaeinvokecangjienested-cae/latest/0-0")
		convey.So(funcMetaInfo, convey.ShouldBeNil)
		convey.So(err.Error() == snerror.ErrText(snerror.FuncMetaNotFound), convey.ShouldBeTrue)
	})
	convey.Convey("TestGetFuncMetaInfo from worker-manager ErrMsg not FuncMetaNotFound", t, func() {
		client := &http.Client{}
		resp := &http.Response{
			StatusCode: 200,
			Body:       &MockReadCloser{},
		}
		registry = newRegistry()
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(client), "Do",
				func(c *http.Client, req *http.Request) (*http.Response, error) {
					return resp, nil
				}),
			gomonkey.ApplyPrivateMethod(reflect.TypeOf(registry), "storeMetaFromEtcd",
				func(r *Registry, functionKey string) (*FunctionMetaInfo, error) {
					return &FunctionMetaInfo{
						FuncMetaData: FuncMetaData{BusinessType: "CAE"},
					}, nil
				}),
			gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return json.Marshal(&workermanager.FuncMetaBaseInfoResp{
					ErrMsg: snerror.ErrText(snerror.ClusterOverloadCode),
				})
			}),
			gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
				return client
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		funcMetaInfo, err := registry.GetFuncMetaInfo("7e1*86a/0@base@testsurgecaeinvokecangjienested-cae/latest/0-0")
		convey.So(funcMetaInfo.BusinessType == "CAE", convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})
}
