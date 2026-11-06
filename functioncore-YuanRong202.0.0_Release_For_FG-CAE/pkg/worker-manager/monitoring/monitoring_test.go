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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/client-go/informers/core/v1"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

var m *monitoringClient

func init() {
	m = &monitoringClient{
		nodeLister: mockUtils.InformerFakeFactory.Core().V1().Nodes().Lister(),
		podLister:  mockUtils.InformerFakeFactory.Core().V1().Pods().Lister(),
		c:          redisclient.RedisClient,
	}
}
func TestGetNodeCPUFlagsFromRedis(t *testing.T) {
	convey.Convey("TestDeleteNodeCPUInfoFromRedis", t, func() {
		convey.Convey("test redis client is nil", func() {
			res := getNodeCPUFlagsFromRedis("testIP")
			convey.So(res, convey.ShouldBeNil)
		})

		convey.Convey("test redis client get ok", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*redisclient.Client).Get,
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
						strCmd := &redis.StringCmd{}
						strCmd.SetErr(nil)
						strCmd.SetVal("redisValue")
						return strCmd
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
			})
			defer patches.ResetAll()
			client.c = &redisclient.Client{}

			res := getNodeCPUFlagsFromRedis("testIP")
			convey.So(strings.Join(res, ","), convey.ShouldEqual, strings.Join([]string{"redisValue"}, ","))
		})

		convey.Convey("test redis client get err", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*redisclient.Client).Get,
					func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
						strCmd := &redis.StringCmd{}
						strCmd.SetErr(errors.New("Test error"))
						return strCmd
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
				gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
			})
			defer patches.ResetAll()

			client.c = &redisclient.Client{}

			res := getNodeCPUFlagsFromRedis("testIP")
			convey.So(res, convey.ShouldBeNil)
		})
	})
}

func TestValidateNodeLister(t *testing.T) {
	convey.Convey("TestValidateNodeLister", t, func() {
		convey.Convey("test client.nodeLister is nil", func() {
			client.nodeLister = nil
			nodes, res := validateNodeLister()
			convey.So(nodes, convey.ShouldBeNil)
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test client.nodeLister.List not nil", func() {
			client.nodeLister = mockUtils.InformerFakeFactory.Core().V1().Nodes().Lister()
			nodes, res := validateNodeLister()
			convey.So(nodes, convey.ShouldBeNil)
			convey.So(res, convey.ShouldBeFalse)
		})
	})
}
func TestCreateMonitoringClient(t *testing.T) {
	redisConfig := config.MonitoringRedisServerConfig{}
	recordingConfig := config.PoolRecordingConfig{}
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetPodInformer, func() corev1.PodInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Pods()
					}),
					gomonkey.ApplyFunc(utils.GetNodeInformer, func() corev1.NodeInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Nodes()
					}),
				})
				return patches
			},
			true,
		},
		{
			"case2",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetPodInformer, func() corev1.PodInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Pods()
					}),
					gomonkey.ApplyFunc(utils.GetNodeInformer, func() corev1.NodeInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Nodes()
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return &redisclient.Client{}, nil
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
					gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
						return
					}),
					gomonkey.ApplyFunc(getFunctionProfilingDataFromRedis, func() error {
						return errors.New("get from redis error")
					}),
				})
				return patches
			},
			false,
		},
		{
			"case3",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.GetPodInformer, func() corev1.PodInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Pods()
					}),
					gomonkey.ApplyFunc(utils.GetNodeInformer, func() corev1.NodeInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Nodes()
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return &redisclient.Client{}, nil
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
					gomonkey.ApplyFunc(checkVpcAndReport,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(getAndMonitorFunctionProfiling, func(stopCh <-chan struct{}) {
						return
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
			stopCh := make(chan struct{}, 1)
			err := CreateMonitoringClient(redisConfig, recordingConfig, stopCh)
			if (err != nil) != tt.calledWant {
				t.Error("TestCalRequestCPUAndMemory error")
			}
			patches.ResetAll()
			close(stopCh)
		})
	}
}

func TestInitMetricsCache(t *testing.T) {
	convey.Convey("InitMetricsCache test", t, func() {
		convey.Convey("InitMetricsCache test success", func() {
			instance := v1alpha1.WorkerInstance{
				InstanceID: "111111111111",
				IP:         "1.1.1.1",
				WillDelete: true,
			}
			data, _ := json.Marshal(instance)
			var etcdClient *etcd3.EtcdClient
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(etcdClient), "GetResponse", func(_ *etcd3.EtcdClient,
					ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					kvs := make([]*mvccpb.KeyValue, 0)

					kvs = append(kvs, &mvccpb.KeyValue{
						Key: []byte("/sn/workers/business/yrk/tenant/12345678901234561234567890123456/function/0@defa" +
							"ult@hellonj/version/latest/defaultaz/defaultaz-#-pool22-300-128-fusion-7d8fd76cd9-q64q2"),
						Value: data,
					}, &mvccpb.KeyValue{},
						&mvccpb.KeyValue{
							Key:   []byte("/sn/workers/business/yrk/tenant/12345678901234561234567890123456/function"),
							Value: data,
						},
						&mvccpb.KeyValue{
							Key: []byte("/sn/workers/business/yrk/tenant/0/function/0@default@hellonj/version/latest" +
								"/defaultaz/defaultaz-#-pool22-300-128-fusion-7d8fd76cd9-q64q2"),
							Value: data,
						},
					)
					return &clientv3.GetResponse{
						Kvs: kvs,
					}, nil
				}),
			})
			defer patches.ResetAll()
			convey.So(InitMetricsCache(), convey.ShouldBeNil)
		})
	})
}

func TestInitClient(t *testing.T) {
	monitor := &monitoringClient{}
	tests := []struct {
		name        string
		stopChan    chan struct{}
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1",
			make(chan struct{}),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return nil, errors.New("redis client error")
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
					gomonkey.ApplyFunc(getFunctionProfilingDataFromRedis, func() error {
						return nil
					}),
				})
				return patches
			},
			false,
		},
		{
			"case2",
			make(chan struct{}),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return redisclient.RedisClient, nil
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
					gomonkey.ApplyFunc(checkVpcAndReport,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(getAndMonitorFunctionProfiling,
						func(stopCh <-chan struct{}) {
							return
						}),
				})
				return patches
			},
			false,
		},
		{
			"case3",
			make(chan struct{}),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
						options ...redisclient.Option) (*redisclient.Client, error) {
						return redisclient.RedisClient, nil
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
					gomonkey.ApplyFunc(checkVpcAndReport,
						func(stopCh <-chan struct{}) {
							time.Sleep(100 * time.Millisecond)
							return
						}),
					gomonkey.ApplyFunc(getAndMonitorFunctionProfiling,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						if d != redisReconnectionInternal {
							return &time.Ticker{C: make(chan time.Time, 1)}
						}
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc((*redisclient.Client).Ping, func(_ *redisclient.Client, _ context.Context) *redis.StatusCmd {
						return &redis.StatusCmd{}
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
			err := monitor.initClient(tt.stopChan)
			if (tt.wantErr && err == nil) || (!tt.wantErr && err != nil) {
				t.Errorf("writeNodeInfo() = %v, want %v", err, tt.wantErr)
			}
			close(tt.stopChan)
			patches.ResetAll()
		})
	}

}

func TestCheckSWRAndReport(t *testing.T) {
	var called bool
	tests := []struct {
		name        string
		stopChan    <-chan struct{}
		patchesFunc mockUtils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			make(<-chan struct{}),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							called = true
							return nil, errors.New("get config error")
						}),
				})
				return patches
			},
			true,
		},
		{
			"case2",
			make(<-chan struct{}),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							called = true
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{
									AKey: "test",
									SKey: "test",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
				})
				return patches
			},
			true,
		},
		{
			"case3",
			make(<-chan struct{}),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							called = true
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{
									AKey: "test",
									SKey: "test",
								},
							}, nil
						}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
				})

				return patches
			},
			true,
		},
		{
			"case4",
			make(<-chan struct{}),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							called = true
							return &config.FaasConfig{
								LocalAuth: &localauth.AuthConfig{
									AKey: "test",
									SKey: "test",
								},
							}, nil
						}),
					gomonkey.ApplyFunc(os.Getenv,
						func(key string) string {
							return "test"
						}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
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
			checkSWRAndReport(tt.stopChan)
			patches.ResetAll()
			if called != tt.want {
				t.Errorf("checkSWRAndReport() = %v, want %v", called, tt.want)
			}
		})
	}
}

func TestCalRequestCPUAndMemory(t *testing.T) {
	tests := []struct {
		caseName string
		pods     []*v1.Pod
		nodeName string
	}{
		{
			"case1",
			[]*v1.Pod{
				{
					Spec: v1.PodSpec{
						NodeName: "test1",
					},
				},
			},
			"test1",
		},
	}
	for _, v := range tests {
		t.Run(v.caseName, func(t *testing.T) {
			cpu, memory, usedPod := calRequestCPUAndMemory(v.pods, v.nodeName)
			if cpu < 0 || memory < 0 || usedPod < 0 {
				t.Error("TestCalRequestCPUAndMemory error")
			}
		})
	}
}

func TestWriteNodeInfo(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"Set",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result",
						func(_ *redis.StatusCmd) (string, error) {
							return "", nil
						}),
				})

				return patches
			},
			false,
		},
		{
			"case2",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"Set",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result",
						func(_ *redis.StatusCmd) (string, error) {
							return "", errors.New("err")
						}),
				})
				return patches
			},
			true,
		},
		{
			"case3",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte{}, errors.New("marshal error")
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
			err := m.writeNodeInfo()
			if (tt.wantErr && err == nil) || (!tt.wantErr && err != nil) {
				t.Errorf("writeNodeInfo() = %v, want %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestReportFunctionInstanceCurrentCount(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"Set",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result",
						func(_ *redis.StatusCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
			false,
		},
		{
			"case2",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"Set",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result",
						func(_ *redis.StatusCmd) (string, error) {
							return "", errors.New("err")
						}),
				})
				return patches
			},
			true,
		},
		{
			"case3",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte{}, errors.New("marshal error")
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
			err := m.reportFunctionInstanceCurrentCount()
			if (tt.wantErr && err == nil) || (!tt.wantErr && err != nil) {
				t.Errorf("reportFunctionInstanceCurrentCount() = %v, want %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}

}

func TestClearExpiredDataInternal(t *testing.T) {
	var called bool
	mockMonitoringClient := &monitoringClient{
		c: &redisclient.Client{},
	}
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		stopChan    chan struct{}
		calledStop  bool
		calledWant  bool
	}{
		{
			name: "case1_stopChan_not_nil_stop",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HGetAll",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringStringMapCmd {
							return &redis.StringStringMapCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringStringMapCmd{}),
						"Result",
						func(_ *redis.StringStringMapCmd) (map[string]string, error) {
							return nil, nil
						}),
					gomonkey.ApplyGlobalVar(&client, mockMonitoringClient),
				})
				return patches
			},
			stopChan:   make(chan struct{}, 1),
			calledStop: true,
			calledWant: false,
		},
		{
			name: "case2_stopChan_not_nil_call",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HDel",
						func(_ *redisclient.Client, ctx context.Context, key string, fields ...string) *redis.IntCmd {
							called = true
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HGetAll",
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringStringMapCmd {
							return &redis.StringStringMapCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringStringMapCmd{}),
						"Result",
						func(_ *redis.StringStringMapCmd) (map[string]string, error) {
							resultMap := map[string]string{}
							resultMap["sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base"+
								"@test-java-snapshot-restore-coldstart-001-hello:latest"] = "{\"failedCount\":6,\"time\":1698407137}"
							return resultMap, nil
						}),
					gomonkey.ApplyGlobalVar(&client, mockMonitoringClient),
				})
				return patches
			},
			stopChan:   make(chan struct{}, 1),
			calledStop: false,
			calledWant: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			if tt.calledStop {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopChan chan struct{}) {
					<-timer.C
					close(stopChan)
				}(tt.stopChan)
			}
			patches := tt.patchesFunc()
			clearExpiredDataInternal()
			if !tt.calledStop && called != tt.calledWant {
				t.Error("TestClearExpiredData error")
			}
			if tt.calledStop && tt.calledWant && !called {
				t.Error("TestClearExpiredData error")
			}
			patches.ResetAll()
		})
	}
}

func TestReportPoolInfo(t *testing.T) {
	PoolConfig := []config.PoolConfig{
		{},
		{},
		{},
	}
	tests := []struct {
		name        string
		poolCfg     []config.PoolConfig
		key         string
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{"case1",
			PoolConfig,
			"test",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HMSet",
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return false, errors.New("redis error")
						}),
				})
				return patches
			},
			true,
		},
		{"case2",
			PoolConfig,
			"test",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HMSet",
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"Expire",
						func(_ *redisclient.Client, ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return true, nil
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
			err := m.reportPoolInfo(tt.poolCfg, tt.key)
			if (tt.wantErr && err == nil) || (!tt.wantErr && err != nil) {
				t.Errorf("reportPoolInfo() = %v, want %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPersistData(t *testing.T) {
	var called bool
	tests := []struct {
		name        string
		method      string
		key         string
		value       string
		patchesFunc mockUtils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			"test",
			"key",
			"value",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(redisclient.WriteToDisk, func(redisCache redisclient.RedisCache) error {
						called = true
						return errors.New("error write to disk")
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
			persistData(tt.method, tt.key, tt.value)
			patches.ResetAll()
			if called != tt.want {
				t.Errorf("persistData called = %v, want %v", called, tt.want)
			}
		})
	}
}

func TestRestoreRedis(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		want        bool
	}{
		{"case1",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(redisclient.TakeCacheToRedis, func(c *redisclient.Client) error {
						return errors.New("error tack cache to redis")
					}),
				})
				return patches
			},
			false,
		},
		{"case2",
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(redisclient.TakeCacheToRedis, func(c *redisclient.Client) error {
						return nil
					}),
				})
				return patches
			},
			true,
		},
	}

	redisConnected := client.redisConnected
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client.redisConnected = false
			patches := tt.patchesFunc()
			restoreRedis(&redisclient.Client{})
			patches.ResetAll()
			if client.redisConnected != tt.want {
				t.Errorf("restoreRedis redisConnected = %v, want %v", client.redisConnected, tt.want)
			}
		})
	}
	client.redisConnected = redisConnected
}

func TestAddRatioNum(t *testing.T) {
	type args struct {
		addType string
	}
	var a args
	var a2 args
	a.addType = "TypeRequestNum"
	a2.addType = "TypePodNum"
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
		{"case2", a2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			AddRatioNum(tt.args.addType)
		})
	}
}

func TestGetRatioStatus(t *testing.T) {
	tests := []struct {
		name string
		want bool
	}{
		{"case1", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetRatioStatus(); got != tt.want {
				t.Errorf("GetRatioStatus() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_recordRatio(t *testing.T) {
	type args struct {
		stopChan <-chan struct{}
	}
	var a args
	a.stopChan = nil
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recordRatio(tt.args.stopChan)
		})
	}
}

func Test_setRatioStatus(t *testing.T) {
	tests := []struct {
		name       string
		podAddNum  int32
		requestNum int32
		want       int32
	}{
		{
			"case1",
			0,
			0,
			0,
		},
		{
			"case2",
			1,
			100,
			1,
		},
		{
			"case2",
			1,
			0,
			0,
		},
	}
	beforeRatio := ratio
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ratio = ratioMonitor{
				podAddNum:  tt.podAddNum,
				requestNum: tt.requestNum,
			}
			setRatioStatus()
			if ratio.ratioStatus != tt.want {
				t.Errorf("setRatioStatus ratioStatus error, set %d, want %d", ratio.ratioStatus, tt.want)
			}
		})
	}
	ratio = beforeRatio
}

func TestReportPoolInfoToServer(t *testing.T) {
	var called bool
	tests := []struct {
		name        string
		stopChan    chan struct{}
		patchesFunc mockUtils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			nil,
			func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			false,
		},
		{
			"case2",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			false,
		},
		{
			"case3",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						called = true
						return nil, nil
					}),
				})
				return patches
			},
			true,
		},
		{
			"case4",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						called = true
						return &config.FaasConfig{
							PoolMgrConfig: config.PoolMgrConfig{
								PoolList: config.PoolList{
									Pool: []config.PoolConfig{},
								},
							},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
		{
			"case5",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						called = true
						return &config.FaasConfig{
							PoolMgrConfig: config.PoolMgrConfig{
								PoolList: config.PoolList{
									Pool: []config.PoolConfig{},
								},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}),
						"HMSet",
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return false, errors.New("redis error")
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
			if tt.stopChan != nil {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopChan chan struct{}) {
					<-timer.C
					close(stopChan)
				}(tt.stopChan)
			}
			patches := tt.patchesFunc()
			reportPoolInfoToServer(tt.stopChan)
			if called != tt.calledWant {
				t.Error("TestReportPoolInfoToServer error")
			}
			patches.ResetAll()
		})
	}
}

func TestReportNodeInfoToServer(t *testing.T) {
	var called bool
	tests := []struct {
		name        string
		stopChan    chan struct{}
		patchesFunc mockUtils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			nil,
			func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			false,
		},
		{
			"case2",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			false,
		},
		{
			"case3",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						called = true
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte{}, errors.New("marshal error")
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
			if tt.stopChan != nil {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopChan chan struct{}) {
					<-timer.C
					close(stopChan)
				}(tt.stopChan)
			}

			patches := tt.patchesFunc()
			reportNodeInfoToServer(tt.stopChan)
			if called != tt.calledWant {
				t.Error("TestReportNodeInfoToServer error")
			}
			patches.ResetAll()
		})
	}
}

func TestReportMetricsToServerInternal(t *testing.T) {
	var called bool
	tests := []struct {
		name        string
		stopChan    chan struct{}
		patchesFunc mockUtils.PatchesFunc
		calledWant  bool
	}{
		{
			"case1",
			nil,
			func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			false,
		},
		{
			"case2",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			false,
		},
		{
			"case3",
			make(chan struct{}, 1),
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						called = true
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte{}, errors.New("marshal error")
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
			if tt.stopChan != nil {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopChan chan struct{}) {
					<-timer.C
					close(stopChan)
				}(tt.stopChan)
			}

			patches := tt.patchesFunc()
			reportMetricsToServer(tt.stopChan)
			if called != tt.calledWant {
				t.Error("TestReportMetricsToServerInternal error")
			}
			patches.ResetAll()
		})
	}
}

func TestReportMetricsToServer(t *testing.T) {
	ReportMetricsToServer(nil)
}

func TestReportFailureFee(t *testing.T) {
	type args struct {
		requestInfo *config.RequestInfo
		err         error
	}
	isCalled := false
	tests := []struct {
		name         string
		args         args
		patchesFunc  mockUtils.PatchesFunc
		expectCalled bool
	}{
		{
			name: "test ReportFailureFee",
			args: args{
				err: errors.New("mock error"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			expectCalled: false,
		},
		{
			name: "test with unknown error",
			args: args{
				err: snerror.New(12345, "mock error"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			expectCalled: false,
		},
		{
			name: "test with start up error",
			args: args{
				err: snerror.New(snerror.RuntimeProcessExit, "mock error"),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			expectCalled: false,
		},
		{
			name: "test with start up error and json message",
			args: args{
				err: snerror.New(snerror.RuntimeProcessExit, "{}"),
				requestInfo: &config.RequestInfo{
					FunctionName: "mock-funcName",
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
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
				return patches
			},
			expectCalled: true,
		},
		{
			name: "test with start up error and json message, json unmarshal mock error",
			args: args{
				err: snerror.New(snerror.RuntimeProcessExit, "{}"),
				requestInfo: &config.RequestInfo{
					FunctionName: "mock-funcName",
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("mock error")
					}),
				})
				return patches
			},
			expectCalled: false,
		},
		{
			name: "test with start up error and result return error",
			args: args{
				err: snerror.New(snerror.RuntimeProcessExit, "{}"),
				requestInfo: &config.RequestInfo{
					FunctionName: "mock-funcName",
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
							expiration time.Duration) *redis.StatusCmd {
							isCalled = true

							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}), "Result",
						func(_ *redis.StatusCmd) (string, error) {
							return "", errors.New("mock error")
						}),
				})
				return patches
			},
			expectCalled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client.c = &redisclient.Client{}
			isCalled = false
			patches := tt.patchesFunc()
			ReportFailureFee(tt.args.requestInfo, tt.args.err)
			if isCalled != tt.expectCalled {
				t.Errorf("isCalled is %v want %v", isCalled, tt.expectCalled)
			}
			patches.ResetAll()
			client.c = nil
		})
	}
}

type redisCmd struct {
	m map[string]string
	n map[string]interface{}
	redis.Client
}

func (c *redisCmd) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	c.m[key] = value.(string)
	return &redis.StatusCmd{}
}

func (c *redisCmd) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	c.n[key] = values
	return &redis.IntCmd{}
}

func Test_monitoringClient_reportFunctionReservedInstanceCurrentCount(t *testing.T) {
	convey.Convey("Test_monitoringClient_reportFunctionReservedInstanceCurrentCount", t, func() {
		mc := &monitoringClient{c: &redisclient.Client{}}
		cmd := &redisCmd{m: map[string]string{}}
		mc.c.SetClient(cmd)

		urn, fsID := "urn", "mock_reserved_instance"
		fs := &faasfunction.FunctionService{}
		fs.Spec.Configuration.FunctionVersionURN = urn
		cache.WorkerCache.CreateFunctionCacheByFuncMeta(fsID, fs)
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name",
			Applier: constants.WorkerManagerApplier})
		defer cache.WorkerCache.DeleteFunctionCache(fsID)
		mc.reportFunctionReservedInstanceCurrentCount()
		fmt.Printf("mock redis cache: %v\n", cmd.m)
		convey.So(len(cmd.m), convey.ShouldNotBeZeroValue)
	})
}

func TestReportFunctionInstanceAmount(t *testing.T) {
	convey.Convey("Test Report Function Instance Amount", t, func() {
		mc := &monitoringClient{c: &redisclient.Client{}}
		cmd := &redisCmd{n: map[string]interface{}{}}
		mc.c.SetClient(cmd)
		urn, fsID := "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@default@zcy_web:V1", "mock_reserved_instance"
		fs := &faasfunction.FunctionService{}
		fs.Spec.Configuration.FunctionVersionURN = urn
		cache.WorkerCache.CreateFunctionCacheByFuncMeta(fsID, fs)
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name",
			Applier: ""})
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name1",
			Applier: ""})
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name2",
			Applier: constants.WorkerManagerApplier})
		defer cache.WorkerCache.DeleteFunctionCache(fsID)
		instanceCountMap := cache.WorkerCache.GetWorkerInstanceCount()
		instanceKey := "function_instance_" + mc.redisCFG.ClusterID
		mc.uploadFunctionInstance(instanceKey, instanceCountMap)
		fmt.Printf("mock redis cache: %v\n", cmd.n)
		convey.So(len(cmd.n), convey.ShouldNotBeZeroValue)
	})
	convey.Convey("Test Report Function Instance Amount error", t, func() {
		patch := gomonkey.ApplyFunc((*redis.IntCmd).Result, func(cmd *redis.IntCmd) (int64, error) { return 0, errors.New("err") })
		patch.ApplyFunc(time.Sleep, func(_ time.Duration) {})
		defer patch.Reset()
		mc := &monitoringClient{c: &redisclient.Client{}}
		cmd := &redisCmd{n: map[string]interface{}{}}
		mc.c.SetClient(cmd)
		urn, fsID := "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@default@zcy_web:V1", "mock_reserved_instance"
		fs := &faasfunction.FunctionService{}
		fs.Spec.Configuration.FunctionVersionURN = urn
		cache.WorkerCache.CreateFunctionCacheByFuncMeta(fsID, fs)
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name",
			Applier: ""})
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name1",
			Applier: ""})
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name2",
			Applier: constants.WorkerManagerApplier})
		defer cache.WorkerCache.DeleteFunctionCache(fsID)
		instanceCountMap := cache.WorkerCache.GetWorkerInstanceCount()
		instanceKey := "function_instance_" + mc.redisCFG.ClusterID
		mc.uploadFunctionInstance(instanceKey, instanceCountMap)
		fmt.Printf("mock redis cache: %v\n", cmd.n)
		convey.So(len(cmd.n), convey.ShouldNotBeZeroValue)

	})
}

func TestReportFunctionReservedInstanceAmount(t *testing.T) {
	convey.Convey("Test Report Function Reserved Instance Amount", t, func() {
		mc := &monitoringClient{c: &redisclient.Client{}}
		cmd := &redisCmd{n: map[string]interface{}{}}
		mc.c.SetClient(cmd)
		urn, fsID := "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@default@zcy_web:V1", "mock_reserved_instance"
		fs := &faasfunction.FunctionService{}
		fs.Spec.Configuration.FunctionVersionURN = urn
		cache.WorkerCache.CreateFunctionCacheByFuncMeta(fsID, fs)
		cache.WorkerCache.StoreWorkerInstance(fsID, v1alpha1.WorkerInstance{PodName: "mock_pod_name",
			Applier: constants.WorkerManagerApplier})
		defer cache.WorkerCache.DeleteFunctionCache(fsID)
		reversedCountMap := cache.WorkerCache.GetReservedInstanceMap()
		reversedKey := "function_reserved_instance_" + mc.redisCFG.ClusterID
		mc.uploadFunctionInstance(reversedKey, reversedCountMap)
		fmt.Printf("mock redis cache: %v\n", cmd.n)
		convey.So(len(cmd.n), convey.ShouldNotBeZeroValue)
	})
}

func TestReportEtcdStatusToRedis(t *testing.T) {
	var isCalled = 0
	convey.Convey("test report etcd status to redis", t, func() {
		convey.Convey("report success", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*etcd3.EtcdClient).CheckEtcdNodeStatus,
					func(w *etcd3.EtcdClient) bool {
						return true
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						isCalled++
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}), "Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", nil
					}),

				gomonkey.ApplyFunc(getNicUsage, func() (int, error) {
					return 0, nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					ch := make(chan time.Time, 1)
					ch <- time.Time{}
					return &time.Ticker{C: ch}
				}),
			})
			defer patches.ResetAll()
			client.c = &redisclient.Client{}
			stopCh := make(chan struct{})
			go reportEtcdStatusToRedis(stopCh)
			time.Sleep(100 * time.Millisecond)
			convey.So(isCalled, convey.ShouldEqual, 2)
			close(stopCh)
		})
	})
}

func TestSaveEtcdNodesStatusToRedis(t *testing.T) {
	convey.Convey("test save etcd nodes status to redis", t, func() {
		convey.Convey("redis retry error", func() {
			var isCalled = 0
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						isCalled++
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}), "Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", errors.New("mock error")
					}),
				gomonkey.ApplyFunc(time.Sleep, func(_ time.Duration) {}),
			})
			defer patches.ResetAll()
			client.c = &redisclient.Client{}
			saveEtcdNodesStatusToRedis(typeRouter, false)
			convey.So(isCalled, convey.ShouldBeGreaterThan, 0)
		})
		convey.Convey("redis client empty", func() {
			var isCalled = 0
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						isCalled++
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}), "Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", errors.New("mock error")
					}),
			})
			defer patches.ResetAll()
			client.c = nil
			saveEtcdNodesStatusToRedis(typeRouter, false)
			convey.So(isCalled, convey.ShouldEqual, 0)
		})
	})
}
func TestReportCPUFlagsMatchingRatio(t *testing.T) {
	convey.Convey("TestReportCPUFlagsMatchingRatio", t, func() {
		convey.Convey("test nodeLister is invalid", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(validateNodeLister,
					func() ([]*v1.Node, bool) {
						return nil, false
					}),
			})
			res := reportCPUFlagsMatchingRatio()
			convey.So(res, convey.ShouldEqual, false)
		})

		redisResult := map[string]string{}
		convey.Convey("test core flags is nil", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(validateNodeLister,
					func() ([]*v1.Node, bool) {
						return nil, true
					}),
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return "testcluster000"
					}),
				gomonkey.ApplyFunc(getCoreCPUFlags,
					func() []string {
						return nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						redisResult[key] = value.(string)
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
					"Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", nil
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
			})

			defer patches.ResetAll()
			client.c = &redisclient.Client{}
			res := reportCPUFlagsMatchingRatio()
			convey.So(res, convey.ShouldEqual, false)
			convey.So(redisResult["ClusterState_CpuFeature_testcluster000"], convey.ShouldEqual, "0.00%##all_nodes")
		})

		convey.Convey("test success to calculate ratio and put", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(validateNodeLister,
					func() ([]*v1.Node, bool) {
						return constructNodeList(2), true
					}),
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return "testcluster001"
					}),
				gomonkey.ApplyFunc(getCoreCPUFlags,
					func() []string {
						return []string{"cpu1", "cpu2"}
					}),
				gomonkey.ApplyFunc(calculateNodesCPUMatchedRatio,
					func(nodes []*v1.Node, clusterID string, coreFlags []string, cpuflagsMap map[string][]string, begin time.Time) (string, float32) {
						return "cpu1,cpu2", 99.99
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						redisResult[key] = value.(string)
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
					"Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", nil
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
			})
			defer patches.ResetAll()
			client.c = &redisclient.Client{}
			res := reportCPUFlagsMatchingRatio()
			convey.So(res, convey.ShouldEqual, true)
			convey.So(redisResult["ClusterState_CpuFeature_testcluster001"], convey.ShouldEqual, "99.99%##")
		})

		convey.Convey("test construct unmatched list", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(validateNodeLister,
					func() ([]*v1.Node, bool) {
						return constructNodeList(2), true
					}),
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return "testcluster002"
					}),
				gomonkey.ApplyFunc(getCoreCPUFlags,
					func() []string {
						return []string{"cpu1", "cpu2"}
					}),
				gomonkey.ApplyFunc(utils.GetNodeIPAddress, func(node *v1.Node) string {
					return node.Name
				}),
				gomonkey.ApplyFunc(getNodeCPUFlagsFromRedis,
					func(redisKey string) []string {
						if redisKey == "fss-cpu-feature:testcluster002:node1" {
							return []string{"cpu2", "cpu3"}
						} else {
							return []string{"cpu3"}
						}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						redisResult[key] = value.(string)
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
					"Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", nil
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
			})
			client.c = &redisclient.Client{}
			defer patches.ResetAll()
			res := reportCPUFlagsMatchingRatio()
			convey.So(redisResult["ClusterState_CpuFeature_testcluster002"], convey.ShouldEqual, "50.00%##node2")
			convey.So(res, convey.ShouldEqual, true)

		})

		convey.Convey("test no intersect flags", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(validateNodeLister,
					func() ([]*v1.Node, bool) {
						return constructNodeList(2), true
					}),
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return "testcluster003"
					}),
				gomonkey.ApplyFunc(getCoreCPUFlags,
					func() []string {
						return []string{"cpu1", "cpu2"}
					}),
				gomonkey.ApplyFunc(utils.GetNodeIPAddress, func(node *v1.Node) string {
					return node.Name
				}),
				gomonkey.ApplyFunc(getNodeCPUFlagsFromRedis,
					func(redisKey string) []string {
						return []string{"cpu3"}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						redisResult[key] = value.(string)
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
					"Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", nil
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
			})
			client.c = &redisclient.Client{}
			defer patches.ResetAll()
			res := reportCPUFlagsMatchingRatio()
			convey.So(redisResult["ClusterState_CpuFeature_testcluster003"], convey.ShouldEqual, "0.00%##node1,node2")
			convey.So(res, convey.ShouldEqual, true)
		})
		convey.Convey("test labels error", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(validateNodeLister,
					func() ([]*v1.Node, bool) {
						return constructNodeListWithLabelError(3), true
					}),
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return "testcluster004"
					}),
				gomonkey.ApplyFunc(getCoreCPUFlags,
					func() []string {
						return []string{"cpu1", "cpu2"}
					}),
				gomonkey.ApplyFunc(utils.GetNodeIPAddress, func(node *v1.Node) string {
					return node.Name
				}),
				gomonkey.ApplyFunc(getNodeCPUFlagsFromRedis,
					func(redisKey string) []string {
						return []string{"cpu3"}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Set",
					func(_ *redisclient.Client, ctx context.Context, key string, value interface{},
						expiration time.Duration) *redis.StatusCmd {
						redisResult[key] = value.(string)
						return &redis.StatusCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
					"Result",
					func(_ *redis.StatusCmd) (string, error) {
						return "", nil
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
			})
			client.c = &redisclient.Client{}
			defer patches.ResetAll()
			res := reportCPUFlagsMatchingRatio()
			convey.So(redisResult["ClusterState_CpuFeature_testcluster004"], convey.ShouldEqual, "0.00%##node1,node3")
			convey.So(res, convey.ShouldEqual, true)

		})
	})

}

func constructNodeList(nodeCount int) []*v1.Node {
	var nodeList = []*v1.Node{}
	nodeLabel := map[string]string{"node-role": "data-plane"}
	for i := 1; i <= nodeCount; i++ {
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "node" + fmt.Sprintf("%d", i),
				Namespace: "default",
				Labels:    nodeLabel,
			},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					"cpu":     resource.MustParse("500"),
					"memory":  resource.MustParse("500"),
					"storage": resource.MustParse("500"),
				},
			},
		}
		nodeList = append(nodeList, node)
	}
	return nodeList
}

func constructNodeListWithLabelError(nodeCount int) []*v1.Node {
	var nodeList = []*v1.Node{}
	nodeLabel := map[string]string{"node-role": "data-plane"}
	for i := 1; i <= nodeCount; i++ {
		if i%2 == 0 {
			nodeLabel = map[string]string{"node-role2": "data-plane"}
		} else {
			nodeLabel = map[string]string{"node-role": "data-plane"}
		}
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "node" + fmt.Sprintf("%d", i),
				Namespace: "default",
				Labels:    nodeLabel,
			},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					"cpu":     resource.MustParse("500"),
					"memory":  resource.MustParse("500"),
					"storage": resource.MustParse("500"),
				},
			},
		}
		nodeList = append(nodeList, node)
	}
	return nodeList
}

func TestDeleteNodeCPUInfoFromRedis(t *testing.T) {
	redisResult := map[string]string{}
	redisResult["fss-cpu-feature:testcluster_delete:testIP"] = "nothing"
	redisResult["fss-cpu-feature:testcluster_delete:testIP2"] = "nothing2"
	convey.Convey("TestDeleteNodeCPUInfoFromRedis", t, func() {
		convey.Convey("test redis client is nil", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return "testcluster_delete"
					}),
			})
			old := client.c
			client.c = nil
			defer func() {
				client.c = old
			}()
			DeleteNodeCPUInfoFromRedis("testIP")
			convey.So(redisResult["fss-cpu-feature:testcluster_delete:testIP"], convey.ShouldEqual, "nothing")
		})

		convey.Convey("test redis client delete ok", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						return "testcluster_delete"
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del",
					func(_ *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
						delete(redisResult, keys[0])
						return &redis.IntCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
					"Result",
					func(_ *redis.IntCmd) (int64, error) {
						return 0, nil
					}),
				gomonkey.ApplyFunc(m.checkRedisConnectivity, func(<-chan struct{}) {
					return
				}),
			})
			client.c = &redisclient.Client{}

			DeleteNodeCPUInfoFromRedis("testIP2")
			convey.So(redisResult["fss-cpu-feature:testcluster_delete:testIP2"], convey.ShouldEqual, "")
		})
	})
}

func TestCompareNodeCPUFlags(t *testing.T) {
	convey.Convey("TestCompareNodeCPUFlags", t, func() {
		convey.Convey("test node flag list is empty", func() {
			cpuflagsMap := make(map[string][]string, 10)
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(getNodeCPUFlagsFromRedis,
					func(key string) []string {
						return nil
					}),
			})
			matched, intersectFlags := compareNodeCPUFlags("testIP", []string{"cpu1"}, cpuflagsMap, "key")
			convey.So(matched, convey.ShouldEqual, 0)
			convey.So(intersectFlags, convey.ShouldEqual, "")
		})

		convey.Convey("test node flag list is not empty", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(getNodeCPUFlagsFromRedis,
					func(redisKey string) []string {
						if redisKey == "fss-cpu-feature:testcluster002:node1" {
							return []string{"cpu1", "cpu2", "cpu3"}
						}
						if redisKey == "fss-cpu-feature:testcluster002:node2" {
							return []string{"cpu2", "cpu3", "cpu4"}
						}
						if redisKey == "fss-cpu-feature:testcluster002:node3" {
							return []string{"cpu1", "cpu2"}
						}
						return []string{}
					}),
			})
			cpuflagsMap := make(map[string][]string, 10)
			coreFlags := []string{"cpu2", "cpu3"}

			matched, intersectFlags := compareNodeCPUFlags("testIP1", coreFlags, cpuflagsMap, "fss-cpu-feature:testcluster002:node1")
			convey.So(matched, convey.ShouldEqual, 1)
			convey.So(intersectFlags, convey.ShouldEqual, "cpu2,cpu3")

			matched, intersectFlags = compareNodeCPUFlags("testIP2", coreFlags, cpuflagsMap, "fss-cpu-feature:testcluster002:node2")
			convey.So(matched, convey.ShouldEqual, 2)
			convey.So(intersectFlags, convey.ShouldEqual, "cpu2,cpu3")

			matched, intersectFlags = compareNodeCPUFlags("testIP3", coreFlags, cpuflagsMap, "fss-cpu-feature:testcluster002:node3")
			convey.So(matched, convey.ShouldEqual, 1)
			convey.So(intersectFlags, convey.ShouldEqual, "cpu2")

			matched, intersectFlags = compareNodeCPUFlags("testIP4", coreFlags, cpuflagsMap, "fss-cpu-feature:testcluster002:node4")
			convey.So(matched, convey.ShouldEqual, 1)
			convey.So(intersectFlags, convey.ShouldEqual, "")
		})
	})
}

func TestGetCoreCPUFlags(t *testing.T) {
	convey.Convey("TestGetCoreCPUFlags", t, func() {
		convey.Convey("test get config error", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.FaasConfig, error) {
						return nil, errors.New("Test Error")
					}),
			})
			res := getCoreCPUFlags()
			convey.So(res, convey.ShouldBeNil)
		})

		convey.Convey("test get config nil", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.FaasConfig, error) {
						return nil, nil
					}),
			})
			res := getCoreCPUFlags()
			convey.So(res, convey.ShouldBeNil)
		})

		convey.Convey("test get core cpu info nil", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						PoolMgrConfig: config.PoolMgrConfig{},
					}, nil
				}),
			})
			res := getCoreCPUFlags()
			convey.So(res, convey.ShouldBeNil)
		})

		convey.Convey("test get core cpu info success", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						PoolMgrConfig: config.PoolMgrConfig{
							CoreCPUFeatures: "cpu2 cpu3 cpu1", // need sort
						},
					}, nil
				}),
			})
			res := getCoreCPUFlags()
			convey.So(strings.Join(res, ","), convey.ShouldEqual, strings.Join([]string{"cpu1", "cpu2", "cpu3"}, ","))
		})
	})
}

func testSaveColdStartMetricsToRedis(t *testing.T) {
	convey.Convey("test save cold start metrics to redis", t, func() {
		convey.Convey("save cold start metrics to redis", func() {
			count := 0
			totalMetrics = totalColdStartMetrics{totalDelay: 10, totalCount: 2}
			client = &monitoringClient{}
			client.c = &redisclient.Client{}
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(redisclient.ZADDMetricsToRedis, func(key string, metrics interface{}, limit int64, expireTime time.Duration) error {
					count++
					return errors.New("error")
				}),
			})
			saveColdStartMetricsToRedis()
			convey.ShouldEqual(count, 1)
			patches.ResetAll()
		})
	})
}

func testReportNodeResourcesToRedis(t *testing.T) {
	now := time.Now()
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(src string) ([]byte, error) {
				return []byte("test"), nil
			}),
		gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
			options ...redisclient.Option) (*redisclient.Client, error) {
			return &redisclient.Client{}, nil
		}),
		gomonkey.ApplyFunc(redisclient.ZADDMetricsToRedis,
			func(key string, metrics interface{}, limit int64, expireTime time.Duration) error {
				return nil
			}),
		gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
			c := make(chan time.Time, 1)
			t := &time.Ticker{
				C: c,
			}
			time.Sleep(10 * time.Millisecond)
			c <- time.Now()
			return t
		}),
		gomonkey.ApplyFunc(redisclient.CheckRedisConnectivity, func(clientRedisConfig *redisclient.NewRedisClientParam,
			client *redisclient.Client, stopCh <-chan struct{}) {
		}),
		gomonkey.ApplyFunc(time.Now, func() time.Time {
			return now
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	mockMetrics := ColdStartMetrics{
		Delay: 0.1,
	}
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, totalMetrics.totalCount)

	ch := make(chan struct{})
	go reportWorkerManagerMetricsToRedis(ch)
	time.Sleep(100 * time.Millisecond)

	PublishColdStartMetrics(mockMetrics)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, totalMetrics.totalCount, 1)
	assert.Equal(t, totalMetrics.totalDelay, 0.1)

	close(ch)
	time.Sleep(10 * time.Millisecond)
	reportWorkerManagerMetricsToRedis(nil)
	subscribeColdStartMetrics(nil)
}

func testPublishColdStartMetrics(t *testing.T) {
	convey.Convey("TestPublishColdStartMetrics", t, func() {
		convey.Convey("TestPublishColdStartMetrics success", func() {
			csm := ColdStartMetrics{Delay: 1}
			for i := 0; i < 1000; i++ {
				metricsChan <- csm
			}
			PublishColdStartMetrics(csm)
			PublishColdStartMetrics(csm)
			convey.ShouldEqual(reachMaxChanCount, 2)
		})
	})
}

func TestMetrics(t *testing.T) {
	t.Run("TestReportNodeResourcesToRedis", testReportNodeResourcesToRedis)
	t.Run("TestSaveColdStartMetricsToRedis", testSaveColdStartMetricsToRedis)
	t.Run("TestSaveColdStartMetricsToRedis", testPublishColdStartMetrics)
}

func Test_calRequestResourceOfNode(t *testing.T) {
	tests := []struct {
		name     string
		pods     []*v1.Pod
		nodeName string
		expected *config.NodeInfo
	}{
		{
			name: "case with gpu",
			pods: []*v1.Pod{
				{
					Spec: v1.PodSpec{
						NodeName: "notTarget",
						Containers: []v1.Container{{
							Resources: v1.ResourceRequirements{
								Requests: map[v1.ResourceName]resource.Quantity{
									"cpu":                     resource.MustParse("100m"),
									"memory":                  resource.MustParse("1Gi"),
									utils.NvidiaGPU:           resource.MustParse("1"),
									utils.GpuIsolationCoreKey: resource.MustParse("5"),
									utils.GpuIsolationMemKey:  resource.MustParse("8"),
								},
							}},
						},
					},
				},
				{
					Spec: v1.PodSpec{
						NodeName: "targetNode",
						Containers: []v1.Container{{
							Resources: v1.ResourceRequirements{
								Requests: map[v1.ResourceName]resource.Quantity{
									"cpu":    resource.MustParse("200m"),
									"memory": resource.MustParse("2Gi"),
								},
							}},
						},
					},
				},
				{
					Spec: v1.PodSpec{
						NodeName: "targetNode",
						Containers: []v1.Container{{
							Resources: v1.ResourceRequirements{
								Requests: map[v1.ResourceName]resource.Quantity{
									"cpu":                     resource.MustParse("400m"),
									"memory":                  resource.MustParse("4Gi"),
									utils.NvidiaGPU:           resource.MustParse("1"),
									utils.GpuIsolationCoreKey: resource.MustParse("5"),
									utils.GpuIsolationMemKey:  resource.MustParse("8"),
								},
							}},
						},
					},
				},
			},
			nodeName: "targetNode",
			expected: &config.NodeInfo{
				UsedPods:             2,
				CPUUsed:              600,
				MemoryUsed:           6 * 1024 * 1024 * 1024,
				RequestNvidiaGPU:     1,
				RequestGPUPercentage: 5,
				RequestGPUMem128:     8,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := calRequestResourceOfNode(tt.pods, tt.nodeName)
			assert.Equal(t, tt.expected, info)
		})
	}
}

func Test_reportMigratingMetricsToRedis(t *testing.T) {
	type args struct {
		stopCh chan struct{}
	}
	isMocked := false
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		expectMock  bool
	}{
		{
			name: "test stop channel is nil",
			args: args{
				stopCh: nil,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			expectMock: false,
		},
		{
			name: "test migratingTickerMin is empty",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				return mockUtils.InitPatchSlice()
			},
			expectMock: false,
		},
		{
			name: "test migratingTickerMin is not empty",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				migratingTickerMin = "1"
				return mockUtils.InitPatchSlice()
			},
			expectMock: false,
		},
		{
			name: "test migratingTickerMin is zero",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				migratingTickerMin = "0"
				return mockUtils.InitPatchSlice()
			},
			expectMock: false,
		},
		{
			name: "test migratingTickerMin is not digital",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				migratingTickerMin = "#"
				return mockUtils.InitPatchSlice()
			},
			expectMock: false,
		},
		{
			name: "test with redis client is nil",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(getMigratingReportInterval, func() time.Duration {
						isMocked = true
						return time.Second
					}),
					gomonkey.ApplyFunc(redisclient.ZADDMetricsToRedis, func(key string, metrics interface{}, limit int64, expireTime time.Duration) error {
						return nil
					}),
				})
				return patches
			},
			expectMock: true,
		},
		{
			name: "test with redis client is not nil",
			args: args{
				stopCh: make(chan struct{}),
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(getMigratingReportInterval, func() time.Duration {
						isMocked = true
						client.c = &redisclient.Client{}
						return time.Second
					}),
					gomonkey.ApplyFunc(redisclient.ZADDMetricsToRedis, func(key string, metrics interface{}, limit int64, expireTime time.Duration) error {
						return errors.New("mock zadd to redis error")
					}),
				})
				return patches
			},
			expectMock: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			isMocked = false
			go reportMigratingMetricsToRedis(tt.args.stopCh)
			time.Sleep(50 * time.Millisecond)
			if tt.args.stopCh != nil {
				close(tt.args.stopCh)
			}
			if isMocked != tt.expectMock {
				t.Errorf("expect %v but found %v", tt.expectMock, isMocked)
			}
		})
	}
}

func Test_clearExpiredData(t *testing.T) {
	stopChan := make(chan struct{})
	go func() {
		close(stopChan)
	}()
	clearExpiredData(stopChan)
	recordRatio(stopChan)
	reportFunctionInstances(stopChan)
}

func Test_cleanMigratingMetrics(t *testing.T) {
	temp := migratingMetrics
	migratingMetrics = MigratingMetrics{
		lock: sync.RWMutex{},
	}
	cleanMigratingMetrics()
	migratingMetrics = temp
}
