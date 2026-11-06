/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package counter

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/state-manager/config"
	utils2 "functioncore/pkg/state-manager/utils"
	"functioncore/test/utils"
)

func TestCounter_persistenceCounterMap(t *testing.T) {
	localMap := make(map[string]*tenantCount)
	localMap["test"] = &tenantCount{}

	counterMap := make(map[string]map[string]*tenantCount)

	called := false
	type fields struct {
		config          *config.FConfig
		counterMap      map[string]map[string]*tenantCount
		localMap        map[string]*tenantCount
		counterMapMutex sync.Mutex
		localMapMutex   sync.Mutex
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		// test getCounterMap error
		{
			"case1",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						called = true
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						called = true
						return errors.New("getCounterMap error")
					}),
				})
				return patches
			},
			true,
		},
		// test result error
		{
			"case2",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						called = true
						return nil
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						called = true
						return nil
					}),
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context,
							key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result", func(_ *redis.StatusCmd) (string, error) {
							return "", errors.New("result error")
						}),
				})
				return patches
			},
			true,
		},
		// test get redis client error
		{
			"case3",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						called = true
						return nil
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						called = true
						return nil
					}),
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context,
							key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result", func(_ *redis.StatusCmd) (string, error) {
							return "", errors.New("result error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Counter{
				config: &config.FConfig{
					StateCounterCfg: config.StateCounterCfg{
						ClusterID: "test",
					},
				},
				counterMap:      counterMap,
				localMap:        localMap,
				counterMapMutex: tt.fields.counterMapMutex,
				localMapMutex:   tt.fields.localMapMutex,
			}
			called = false
			patches := tt.patchesFunc()
			c.persistenceCounterMap()
			if called != tt.calledWant {
				t.Errorf("persistenceCounterMap() called = %v, want %v", called, tt.calledWant)
			}
			patches.ResetAll()
		})
	}
}

func TestDecr(t *testing.T) {
	type args struct {
		tenantID string
	}
	tests := []struct {
		name string
		args args
	}{
		// test called
		{
			"case1",
			args{
				"test",
			},
		},
	}
	called := false
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyFunc(getTenantCounter, func(tenantID string) *tenantCount {
			called = true
			return &tenantCount{}
		}),
	})
	defer patches.ResetAll()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Decr(tt.args.tenantID)
			if called != true {
				t.Errorf("TestDecr() error")
			}
		})
	}
}

func Test_tenantCount_decr(t1 *testing.T) {
	called := false
	type fields struct {
		Count int64
		Limit int64
	}
	tests := []struct {
		name       string
		fields     fields
		calledWant bool
	}{
		// test count == 0
		{
			"case1",
			fields{
				Count: 0,
			},
			false,
		},
		// test count > 0
		{
			"case2",
			fields{
				Count: 2,
				Limit: 15,
			},
			true,
		},
	}
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyFunc(getDistributedLock, func() bool {
			return true
		}),
		gomonkey.ApplyFunc(getCounterMap, func() error {
			return errors.New("get counter map error")
		}),
		gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
			return errors.New("get counter map error")
		}),
		gomonkey.ApplyFunc(releaseDistributedLock, func() {
			called = true
			return
		}),
	})
	defer patches.ResetAll()
	for _, tt := range tests {
		called = false
		temp := counter.localMapMutex
		t1.Run(tt.name, func(t1 *testing.T) {
			t := &tenantCount{
				Count: tt.fields.Count,
				Limit: tt.fields.Limit,
			}
			t.decr()
			if called != tt.calledWant {
				t1.Errorf("Test decr error")
			}
		})
		counter.localMapMutex = temp
	}
}

func TestIncr(t *testing.T) {
	type args struct {
		tenantID string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test error
		{
			"case1",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getTenantCounter, func(tenantID string) *tenantCount {
						return &tenantCount{}
					}),
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return errors.New("getCounterMap error")
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := Incr(tt.args.tenantID); (err != nil) != tt.wantErr {
				t.Errorf("Incr() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_tenantCount_incr(t1 *testing.T) {
	type fields struct {
		Count int64
		Limit int64
	}
	type args struct {
		tenantID string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test getCounterMap
		{
			"case1",
			fields{},
			args{
				"test",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return errors.New("getCounterMap error")
					}),
				})
				return patches
			},
			true,
		},
		// test count > limit
		{
			"case2",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return nil
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return nil
					}),
					gomonkey.ApplyFunc(getDistributedLock, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("marshal error")
					}),
					gomonkey.ApplyFunc(releaseDistributedLock, func() {
						return
					}),
					gomonkey.ApplyGlobalVar(&counter, &Counter{config: &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "test"}}}),
				})
				return patches
			},
			false,
		},
		// test limit > count
		{
			"case3",
			fields{
				Count: 0,
				Limit: 10,
			},
			args{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		{
			"case4",
			fields{},
			args{
				"test2",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return nil
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return nil
					}),
					gomonkey.ApplyFunc(getDistributedLock, func() bool {
						return true
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		temp := counter.counterMap

		counterMapItem := make(map[string]*tenantCount)
		counterMapItem["test"] = &tenantCount{
			Limit: 10,
		}
		counterMapItem2 := make(map[string]*tenantCount)
		counterMapItem2["test2"] = &tenantCount{
			Limit: 100,
		}
		counter.counterMap = make(map[string]map[string]*tenantCount)
		counter.counterMap["test"] = counterMapItem
		counter.counterMap["test2"] = counterMapItem2

		t1.Run(tt.name, func(t1 *testing.T) {
			t := &tenantCount{
				Count: tt.fields.Count,
				Limit: tt.fields.Limit,
			}
			patches := tt.patchesFunc()
			if err := t.incr(tt.args.tenantID); (err != nil) != tt.wantErr {
				t1.Errorf("incr() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
		counter.counterMap = temp
	}
}

func Test_getCounterMap(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test redis get error
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", errors.New("redis get error")
						}),
				})
				return patches
			},
			true,
		},
		// test json unMarshal error
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{asdasd\\}", nil
						}),
				})
				return patches
			},
			true,
		},
		// test json unMarshal success
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
			false,
		},
		// test get redis client failed
		{
			"case4",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := getCounterMap(); (err != nil) != tt.wantErr {
				t.Errorf("getCounterMap() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_persistenceCounterMapTask(t *testing.T) {
	called := false
	type args struct {
		stopChan chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
	}{
		// test close stopChan
		{
			"case1",
			args{
				stopChan: make(chan struct{}),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getDistributedLock, func() bool {
						called = true
						return true
					}),
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(releaseDistributedLock, func() {
						return
					}),
				})
				return patches
			},
		},
		// test time ticker run
		{
			"case2",
			args{
				stopChan: make(chan struct{}),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						chanC := make(chan time.Time, 1)
						chanC <- time.Now()
						ticket := &time.Ticker{
							C: chanC,
						}
						return ticket
					}),
					gomonkey.ApplyFunc(getDistributedLock, func() bool {
						called = true
						return true
					}),
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(releaseDistributedLock, func() {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		temp := counter.config
		go func() {
			<-time.After(time.Microsecond)
			close(tt.args.stopChan)
		}()
		// counter.config.StateCounterCfg.PersistenceInterval
		counter.config = &config.FConfig{
			StateCounterCfg: config.StateCounterCfg{
				PersistenceInterval: 1,
			},
		}
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			called = false
			persistenceCounterMapTask(tt.args.stopChan)
			if called != true {
				t.Errorf("persistenceCounterMapTask() called = %v, want %v", called, true)
			}
		})
		patches.ResetAll()
		counter.config = temp
	}
}

func TestNewCounter(t *testing.T) {
	checkFinishChan := make(chan struct{}, 1)
	type args struct {
		c        *config.FConfig
		stopChan chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test getCounterMap error
		{
			"case2",
			args{
				stopChan: make(chan struct{}, 1),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getDistributedLock, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(persistenceCounterMapTask, func(_ <-chan struct{}) {
						close(checkFinishChan)
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := config.FConfigs
			config.FConfigs = &config.FConfig{
				AlarmServerAddress: "127.0.0.1",
			}

			patches := tt.patchesFunc()
			if err := NewCounter(tt.args.c, tt.args.stopChan); (err != nil) != tt.wantErr {
				t.Errorf("NewCounter() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.args.stopChan != nil {
				close(tt.args.stopChan)
			}
			<-checkFinishChan
			patches.ResetAll()
			config.FConfigs = temp
		})
	}
}

func TestCounter_restoreCounterMap(t *testing.T) {
	type fields struct {
		config          *config.FConfig
		counterMap      map[string]map[string]*tenantCount
		localMap        map[string]*tenantCount
		counterMapMutex sync.Mutex
		localMapMutex   sync.Mutex
	}
	tests := []struct {
		name               string
		fields             fields
		patchesFunc        utils.PatchesFunc
		wantErr            bool
		wantLocalMapLength int
	}{
		// test getCounterMap error
		{
			"case1",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return errors.New("getCounterMap error")
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return errors.New("getCounterMap error")
					}),
				})
				return patches
			},
			false,
			0,
		},
		// test success
		{
			"case2",
			fields{counterMap: map[string]map[string]*tenantCount{"testTenant": {"testCluster": &tenantCount{Limit: 10, Count: 5}}}},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getCounterMap, func() error {
						return nil
					}),
					gomonkey.ApplyFunc(getCountMapByNewHashKey, func() error {
						return nil
					}),
				})
				return patches
			},
			false,
			1,
		},
	}
	for _, tt := range tests {
		counterMapItem := make(map[string]*tenantCount)
		counterMapItem["test"] = &tenantCount{}
		counterMap := make(map[string]map[string]*tenantCount)
		counterMap["test"] = counterMapItem
		t.Run(tt.name, func(t *testing.T) {
			c := &Counter{
				config: &config.FConfig{
					StateCounterCfg: config.StateCounterCfg{
						ClusterID: "test",
					},
				},
				counterMap:      counterMap,
				localMap:        tt.fields.localMap,
				counterMapMutex: tt.fields.counterMapMutex,
				localMapMutex:   tt.fields.localMapMutex,
			}
			patches := tt.patchesFunc()
			if err := c.restoreCounterMap(); (err != nil) != tt.wantErr {
				t.Errorf("restoreCounterMap() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_getTenantCounter(t *testing.T) {
	type args struct {
		tenantID string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			"case1",
			args{
				"test",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := counter
			counter = &Counter{
				counterMapMutex: sync.Mutex{},
				localMap:        make(map[string]*tenantCount),
			}
			if got := getTenantCounter(tt.args.tenantID); (got != nil) != tt.want {
				t.Errorf("getTenantCounter() = %v, want %v", got != nil, tt.want)
			}
			counter = temp
		})
	}
}

func Test_getDistributedLock(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test result error
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"SetNX", func(_ *redisclient.Client, ctx context.Context, key string,
							value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result", func(_ *redis.BoolCmd) (bool, error) {
							return true, errors.New("result error")
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
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"SetNX", func(_ *redisclient.Client, ctx context.Context, key string,
							value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result", func(_ *redis.BoolCmd) (bool, error) {
							return true, nil
						}),
				})
				return patches
			},
			true,
		},
		// test result nok
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"SetNX", func(_ *redisclient.Client, ctx context.Context, key string,
							value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result", func(_ *redis.BoolCmd) (bool, error) {
							return false, nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getDistributedLock(); got != tt.want {
				t.Errorf("getDistributedLock() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_releaseDistributedLock(t *testing.T) {
	called := false
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						called = true
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"Del", func(_ *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 0, nil
						}),
				})
				return patches
			},
			true,
		},
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						called = true
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"Del", func(_ *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 0, errors.New("result error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			patches := tt.patchesFunc()
			releaseDistributedLock()
			if called != tt.calledWant {
				t.Errorf("releaseDistributedLock() = %v, want %v", called, tt.calledWant)
			}
			patches.ResetAll()
		})
	}
}

func Test_getCountMapByNewHashKey(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test redis client is nil
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "MGet",
						func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.SliceCmd {
							return &redis.SliceCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.SliceCmd{}),
						"Result", func(_ *redis.SliceCmd) ([]interface{}, error) {
							var emptySlice []interface{}
							return emptySlice, errors.New("redis get error")
						}),
				})
				return patches
			},
			false,
		},
		// test *FConfig is nil
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "MGet",
						func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.SliceCmd {
							return &redis.SliceCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.SliceCmd{}),
						"Result", func(_ *redis.SliceCmd) ([]interface{}, error) {
							var emptySlice []interface{}
							return emptySlice, errors.New("redis get error")
						}),
				})
				return patches
			},
			true,
		},
		// test redis get error
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "MGet",
						func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.SliceCmd {
							return &redis.SliceCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.SliceCmd{}),
						"Result", func(_ *redis.SliceCmd) ([]interface{}, error) {
							var emptySlice []interface{}
							return emptySlice, errors.New("redis get error")
						}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
				})
				return patches
			},
			true,
		},
		// test json unMarshal error
		{
			"case4",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "MGet",
						func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.SliceCmd {
							return &redis.SliceCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.SliceCmd{}),
						"Result", func(_ *redis.SliceCmd) ([]interface{}, error) {
							var testSlice []interface{}
							testSlice = append(testSlice, "{asdasd\\}")
							return testSlice, nil
						}),
					gomonkey.ApplyFunc(refreshStateCacheByStateCountKey, func(_ []string) {
					}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
				})
				return patches
			},
			false,
		},
		// test json unMarshal success
		{
			"case5",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "MGet",
						func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.SliceCmd {
							return &redis.SliceCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.SliceCmd{}),
						"Result", func(_ *redis.SliceCmd) ([]interface{}, error) {
							var testSlice []interface{}
							testSlice = append(testSlice, "{\"testTenant\":{\"count\":10,\"limit\":20}}")
							return testSlice, nil
						}),
					gomonkey.ApplyFunc(refreshStateCacheByStateCountKey, func(_ []string) {
					}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
				})
				return patches
			},
			false,
		},
		// test json unMarshal success, with empty string
		{
			"case6",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "MGet",
						func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.SliceCmd {
							return &redis.SliceCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.SliceCmd{}),
						"Result", func(_ *redis.SliceCmd) ([]interface{}, error) {
							var testSlice []interface{}
							testSlice = append(testSlice, "{\"testTenant\":{\"count\":10,\"limit\":20}}")
							testSlice = append(testSlice, "")
							return testSlice, nil
						}),
					gomonkey.ApplyFunc(refreshStateCacheByStateCountKey, func(_ []string) {
					}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := getCountMapByNewHashKey(); (err != nil) != tt.wantErr {
				t.Errorf("getCounterMap() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_refreshStateCacheByStateCountKey(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		testSlice   []string
	}{
		// testSlice is nil
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
			[]string{},
		},
		// test redis client is nil
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
			[]string{"testCluster1", "testCluster2"},
		},
		// test redis get error
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", errors.New("error")
						}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
				})
				return patches
			},
			[]string{"testCluster1", "testCluster2"},
		},
		// test json unMarshal error
		{
			"case4",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "{asdasd\\}", nil
						}),
					gomonkey.ApplyFunc(refreshStateCacheByStateCountKey, func(_ []string) {
					}),
					gomonkey.ApplyGlobalVar(&counter.counterMap, make(map[string]map[string]*tenantCount)),
				})
				return patches
			},
			[]string{"testCluster1", "testCluster2"},
		},
		// test json unMarshal success
		{
			"case5",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return `{\"tenant1\":{\"cluster001\":{\"count\":11,\"limit\":11},\"cluster002\":{\"count\":0,\"limit\":10},\"cluster003\":{\"count\":0,\"limit\":5}}}`, nil
						}),
					gomonkey.ApplyFunc(refreshStateCacheByStateCountKey, func(_ []string) {
					}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
				})
				return patches
			},
			[]string{"cluster001", "cluster002", "cluster003"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			refreshStateCacheByStateCountKey(tt.testSlice)
			patches.ResetAll()
		})
	}
}

func Test_refreshStateCacheByStateCountKeyWithNilRedisValue(t *testing.T) {
	convey.Convey("cluster001 does not in redis", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Get",
				func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
					return &redis.StringCmd{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
				"Result", func(_ *redis.StringCmd) (string, error) {
					return "{\"tenant1\":{\"cluster001\":{\"count\":11,\"limit\":11},\"cluster002\":{\"count\":0,\"limit\":10},\"cluster003\":{\"count\":0,\"limit\":5}}}", nil
				}),
			gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
		})
		counter.counterMapMutex.Lock()
		if counter.counterMap == nil {
			counter.counterMap = map[string]map[string]*tenantCount{}
		}
		counter.counterMapMutex.Unlock()

		convey.So(counter.counterMap["tenant1"]["testCluster001"], convey.ShouldBeNil)

		refreshStateCacheByStateCountKey([]string{"testCluster001"})
		stateCounter := counter.counterMap["tenant1"]["testCluster001"]
		convey.So(stateCounter, convey.ShouldNotBeNil)
		convey.So(stateCounter.Count, convey.ShouldEqual, 0)
		convey.So(stateCounter.Limit, convey.ShouldEqual, config.TenantStateLocalLimit)

		counter.counterMapMutex.Lock()
		delete(counter.counterMap, "tenant1")
		counter.counterMapMutex.Unlock()

		patches.ResetAll()
	})
}

func Test_ResetRealStateCountByETCD(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		testCount   map[string]int64
	}{
		// FConfig is nil
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{}),
				})
				return patches
			},
			nil,
		},
		// counter.counterMap, counter.localMap are nil, len(clusterStateCount) == 0 return
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(setClusterIDHashTagInRedis, func(_, _ string) {}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
					gomonkey.ApplyGlobalVar(&counter, &Counter{}),
				})
				return patches
			},
			map[string]int64{},
		},
		// delete unExist tenant, json.marshal error
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
						return nil, errors.New("error")
					}),
					gomonkey.ApplyFunc(setClusterIDHashTagInRedis, func(_, _ string) {}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
					gomonkey.ApplyGlobalVar(&counter, &Counter{localMap: map[string]*tenantCount{"tenantX": {Count: 10, Limit: 30}}}),
				})
				return patches
			},
			map[string]int64{"tenant1": 1},
		},
		// test success
		{
			"case4",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(setClusterIDHashTagInRedis, func(_, _ string) {}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
					gomonkey.ApplyGlobalVar(&counter, &Counter{localMap: map[string]*tenantCount{"tenantX": {Count: 10, Limit: 30}}}),
				})
				return patches
			},
			map[string]int64{"tenant1": 1},
		},
		// test counter.counterMap[tenant][curClusterID] is nil
		{
			"case5",
			func() utils.PatchSlice {
				counterMapItem := make(map[string]*tenantCount)
				counterMapItem["testCluster001"] = nil
				counterMap := make(map[string]map[string]*tenantCount)
				counterMap["tenant_case5"] = counterMapItem
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(setClusterIDHashTagInRedis, func(_, _ string) {}),
					gomonkey.ApplyGlobalVar(&config.FConfigs, &config.FConfig{StateCounterCfg: config.StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001", "testCluster002"}}}),
					gomonkey.ApplyGlobalVar(&counter, &Counter{localMap: map[string]*tenantCount{"tenantX": {Count: 10, Limit: 30}}, counterMap: counterMap}),
				})
				return patches
			},
			map[string]int64{"tenant_case5": 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			ResetRealStateCountByETCD(tt.testCount)
			patches.ResetAll()
		})
	}
}

func Test_setClusterIDHashTagInRedis(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
	}{
		// GetRedisCmd is nil
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result", func(_ *redis.StatusCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
		},
		// test redis set error
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result", func(_ *redis.StatusCmd) (string, error) {
							return "", errors.New("error")
						}),
				})
				return patches
			},
		},
		// test success
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(utils2.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							return &redis.StringCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringCmd{}),
						"Result", func(_ *redis.StringCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			setClusterIDHashTagInRedis("clusterKey", "clusterText")
			patches.ResetAll()
		})
	}
}
