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

package instance

import (
	"context"
	"encoding/json"
	"errors"
	"functioncore/pkg/autoscaler/cache"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/tls"
	mockutils "functioncore/test/utils"
)

func Test_ASPersistence_UpdateFuncInsCacheFromRedis(t *testing.T) {
	clusterInsCaches := make([]config.ClusterInsCache, 0)
	clusterInsCaches = append(clusterInsCaches, config.ClusterInsCache{
		Count:     1,
		ClusterID: "cluster",
	})
	instances := make(map[string]*config.InstanceCache)
	instancesCache := &config.InstanceCache{
		TotalCurrentCount: 1,
		ClusterInsCache:   clusterInsCaches,
	}
	instances["mockPodSvc"] = instancesCache
	tests := []struct {
		name string
		asp  *ASPersistence
		want *config.InstanceCache
	}{
		{
			"test update func instance success",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: instances,
			},
			instancesCache,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				cache.GetInstance().FunctionInstanceCache = sync.Map{}
			}()
			tt.asp.updateFuncInsCacheFromRedis()
			cache, ok := cache.GetInstance().FunctionInstanceCache.Load("mockPodSvc")
			got := cache.(*config.InstanceCache)
			if !ok || got != tt.want {
				t.Errorf("GetInstance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ASPersistence_setInstances(t *testing.T) {
	clusterInsCaches := make([]config.ClusterInsCache, 0)
	clusterInsCaches = append(clusterInsCaches, config.ClusterInsCache{
		Count:     1,
		ClusterID: "cluster",
	})
	instances := make(map[string]*config.InstanceCache)
	instancesCache := &config.InstanceCache{
		TotalCurrentCount: 1,
		ClusterInsCache:   clusterInsCaches,
	}
	instances["mockPodSvc"] = instancesCache
	tests := []struct {
		name   string
		asp    *ASPersistence
		want   map[string]*config.InstanceCache
		doFunc func()
	}{
		{
			"test ASPersistence set instances key failed",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: make(map[string]*config.InstanceCache),
			},
			make(map[string]*config.InstanceCache),
			func() {
				cache.GetInstance().FunctionInstanceCache.Store(123, instancesCache)
			},
		},
		{
			"test ASPersistence set instances value failed",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: make(map[string]*config.InstanceCache),
			},
			make(map[string]*config.InstanceCache),
			func() {
				cache.GetInstance().FunctionInstanceCache.Store("mockPodSvc", 123)
			},
		},
		{
			"test ASPersistence set instances success",
			&ASPersistence{
				Version:   "1",
				Timestamp: time.Now().UnixMilli(),
				Instances: make(map[string]*config.InstanceCache),
			},
			instances,
			func() {
				cache.GetInstance().FunctionInstanceCache.Store("mockPodSvc", instancesCache)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.doFunc()
			tt.asp.setInstances()
			defer func() {
				cache.GetInstance().FunctionInstanceCache = sync.Map{}
			}()
			if !reflect.DeepEqual(tt.asp.Instances, tt.want) {
				t.Errorf("GetInstance() = %v, want %v", tt.asp.Instances, tt.want)
			}
		})
	}
}

func TestSaveInsCacheToRedis(t *testing.T) {
	convey.Convey("test save instance cache to redis", t, func() {
		convey.Convey("test OK", func() {
			redisAlarmCount = 1
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{
					HTTPSConfig: &tls.InternalHTTPSConfig{
						HTTPSEnable: false,
						TLSProtocol: "",
						TLSCiphers:  "",
					},
					LocalAuth: &localauth.AuthConfig{},
				}, nil
			})
			patches = gomonkey.ApplyFunc(redisclient.SetWithRetry, func(redisKey string, redisValue []byte, expireTime time.Duration) bool {
				return true
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
			})
			defer patches.Reset()
			SaveInsCacheToRedis()
			convey.So(redisAlarmCount, convey.ShouldEqual, 0)
		})

		convey.Convey("test error", func() {
			redisAlarmCount = 2
			patches := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return nil, errors.New("error")
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc(redisclient.SetWithRetry, func(redisKey string, redisValue []byte, expireTime time.Duration) bool {
				return false
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc((*alarm.Client).ClearAlarm, func(_ *alarm.Client, almType alarm.Type) {
			})
			defer patches.Reset()
			patches = gomonkey.ApplyFunc((*alarm.Client).ReportAlarm, func(_ *alarm.Client, almType alarm.Type, specAddition string) {
				redisAlarmCount++
			})
			defer patches.Reset()
			SaveInsCacheToRedis()
			convey.So(redisAlarmCount, convey.ShouldEqual, 4)
		})
	})
}

func TestRecoverPersistence(t *testing.T) {
	insMap := make(map[string]*config.InstanceCache)
	insMap["func11"] = &config.InstanceCache{TotalCurrentCount: 4}
	asp := &ASPersistence{
		Version:   defaultVersion,
		Timestamp: time.Now().Unix(),
		Instances: insMap,
	}
	aspBytes, _ := json.Marshal(asp)
	convey.Convey("test recover persistence", t, func() {
		convey.Convey("test success", func() {
			patches := mockutils.InitPatchSlice()
			patches.Append(mockutils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
					return &redisclient.Client{}
				}),
				gomonkey.ApplyFunc((*redisclient.Client).Get,
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
						return &redis.StringCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
					"Result", func(_ *redis.StringCmd) (string, error) {
						return string(aspBytes), nil
					}),
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
					a, ok := v.(*ASPersistence)
					if ok {
						a.Version = defaultVersion
						a.Instances = insMap
					}
					return nil
				}),
				gomonkey.ApplyFunc((*clustermgr.ClusterManager).GetAllAvailableCluster,
					func(_ *clustermgr.ClusterManager) map[string]clustermgr.Cluster {
						return map[string]clustermgr.Cluster{"cluster1": {}}
					}),
			})
			defer patches.ResetAll()
			RecoverPersistence()
			_, ok := cache.GetInstance().FunctionInstanceCache.Load("func11")
			convey.So(ok, convey.ShouldBeTrue)
		})
	})
}

func TestUpdateFuncInsCache(t *testing.T) {
	convey.Convey("test update funcInsCache", t, func() {
		updateFlag := false
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&QueryPool{}), "StartWorking", func(_ *QueryPool) {
			updateFlag = true
		})
		defer patch.Reset()
		UpdateFuncInsCache()
		convey.So(updateFlag, convey.ShouldBeTrue)
	})
}
