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

package monitoring

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/redisclient"
	"functioncore/test/utils"
)

func TestInitVpcMonitor(t *testing.T) {
	tests := []struct {
		name        string
		stopChan    chan struct{}
		patchesFunc utils.PatchesFunc
		want        int
	}{
		{
			"case1",
			nil,
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			0,
		},
		{
			"case2",
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getNicUsage, func() (int, error) {
						return 0, errors.New("get nic usage err")
					}),
				})
				return patches
			},
			0,
		},
		{
			"case3",
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc(getNicUsage, func() (int, error) {
						return 0, errors.New("get nic usage err")
					}),
				})
				return patches
			},
			0,
		},
		{
			"case4",
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc(getNicUsage, func() (int, error) {
						return 99, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&alarm.Client{}),
						"ReportAlarm",
						func(_ *alarm.Client, p alarm.Type, _ string) {
							return
						}),
				})
				return patches
			},
			0,
		},
		{
			"case5",
			make(chan struct{}, 1),
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc(getNicUsage, func() (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&alarm.Client{}),
						"ReportAlarm",
						func(_ *alarm.Client, p alarm.Type, _ string) {
							return
						}),
				})
				return patches
			},
			0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if tt.stopChan != nil {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopChan chan struct{}) {
					<-timer.C
					close(stopChan)
				}(tt.stopChan)
			}
			initVpcMonitor(tt.stopChan)
			patches.ResetAll()
		})
	}
}

func TestGetNicNum(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		want        int
	}{
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
							return []string{
								"{123\"}",
							}, nil
						}),
				})
				return patches
			},
			0,
		},
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
							return []string{
								"{\"pat_pod_id\":\"test\"}",
							}, nil
						}),
				})
				return patches
			},
			0,
		},
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
							return []string{
								"{\"pat_pod_id\":\"test::qwe:22\",\"is_used\":true, \"t_mac_addr\":\"addr\"}",
							}, nil
						}),
				})
				return patches
			},
			1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if res, _ := getNicNum(); res != tt.want {
				t.Errorf("test get nic num %s error: want %v, get %v", tt.name, tt.want, res)
			}
			patches.ResetAll()
		})
	}
}

func TestGetPatPodFromETCD(t *testing.T) {
	tests := []struct {
		name        string
		clientC     *redisclient.Client
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
							return []string{}, errors.New("get etcd values error")
						}),
				})
				return patches
			},
			true,
		},
		{
			"case1",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetValues",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
							return []string{}, nil
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
			if _, err := getPatPodFromETCD(); (err != nil) != tt.want {
				t.Errorf("test get pod from etcd %s error: want %v, get %v", tt.name, tt.want, err != nil)
			}
			patches.ResetAll()
		})
	}
}

func TestGetNodeNum(t *testing.T) {
	tests := []struct {
		name        string
		clientC     *redisclient.Client
		patchesFunc utils.PatchesFunc
		want        int
	}{
		{
			"case1",
			nil,
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			0,
		},
		{
			"case2",
			&redisclient.Client{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"ZCard",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 0, errors.New("redis result err")
						}),
				})
				return patches
			},
			0,
		},
		{
			"case3",
			&redisclient.Client{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"ZCard",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 5, nil
						}),
				})
				return patches
			},
			5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			temp := client.c
			client.c = tt.clientC
			if res, _ := getNodeNum(); res != tt.want {
				t.Errorf("test get node num %s error: want %v, get %v", tt.name, tt.want, res)
			}
			client.c = temp
			patches.ResetAll()
		})
	}
}

func TestNickUsage(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		want        int
	}{
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getNicNum, func() (int, error) {
						return 0, errors.New("get nick num error")
					}),
				})
				return patches
			},
			0,
		},
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getNicNum, func() (int, error) {
						return 0, nil
					}),
					gomonkey.ApplyFunc(getNodeNum, func() (int, error) {
						return 0, errors.New("get node num error")
					}),
					gomonkey.ApplyFunc(saveNicNumToRedis,
						func(nicNum int) {
							return
						}),
				})
				return patches
			},
			0,
		},
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getNicNum, func() (int, error) {
						return 7, nil
					}),
					gomonkey.ApplyFunc(getNodeNum, func() (int, error) {
						return 1, nil
					}),
					gomonkey.ApplyFunc(saveNicNumToRedis,
						func(nicNum int) {
							return
						}),
				})
				return patches
			},
			100,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if res, _ := getNicUsage(); res != tt.want {
				t.Errorf("test nick usage %s error: want %v, get %v", tt.name, tt.want, res)
			}
			patches.ResetAll()
		})
	}
}

func TestSaveNicNumToRedis(t *testing.T) {
	isCalled := false
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
							expiration time.Duration) *redis.StatusCmd {
							isCalled = true

							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}), "Result",
						func(_ *redis.StatusCmd) (string, error) {
							return "", nil
						}),
				})
				client.c = &redisclient.Client{}
				return patches
			},
			true,
		},
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
							expiration time.Duration) *redis.StatusCmd {
							isCalled = true

							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}), "Result",
						func(_ *redis.StatusCmd) (string, error) {
							return "", nil
						}),
				})
				client.c = nil
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			saveNicNumToRedis(5)
			if isCalled != tt.want {
				t.Errorf("test nick usage %s error: want %v, get %v", tt.name, tt.want, isCalled)
			}
			patches.ResetAll()
			isCalled = false
		})
	}
}

func TestCheckVpcAndReport(t *testing.T) {
	called := false
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							called = true
							return
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
			checkVpcAndReport(nil)
			if called != tt.want {
				t.Errorf("test nick usage %s error: want %v, get %v", tt.name, tt.want, called)
			}
			patches.ResetAll()
		})
	}
}
