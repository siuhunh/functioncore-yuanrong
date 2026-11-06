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

// Package workerevaluator -
package workerevaluator

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"
	"github.com/robfig/cron/v3"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/types/workertypes"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/resourceusage"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	urn                     = "sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:Function:1"
	functionKeyWithResource = "TenantID01/function01/latest/300-128"
)

var (
	window = workertypes.TimeWindow{
		StartTime:       1655776292000,
		EndTime:         1655776293000,
		BillingDuration: 1,
		Stage:           "1",
	}
	pushError error
	setError  error
)

func TestCreateMetricsClient(t *testing.T) {
	Convey("createMetricsClient", t, func() {
		Convey("failedToDecrypt", func() {
			patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return nil, errors.New("test err")
			})
			gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
				func(a *alarm.Client, almType alarm.Type, specAddition string) {
					return
				})
			gomonkey.ApplyFunc((*metricsClient).reportCPUFlagsToRedisTicker,
				func(*metricsClient, <-chan struct{}) {
					return
				})
			gomonkey.ApplyFunc(initCronjobClient, func(<-chan struct{}) {
				return
			})
			err := CreateMetricsClient(&common.MetricsServerConfig{}, make(<-chan struct{}))
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("initClientErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam,
					stopCh <-chan struct{}, options ...redisclient.Option) (*redisclient.Client, error) {
					return nil, errors.New("test err")
				}),
				gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
					func(a *alarm.Client, almType alarm.Type, specAddition string) {
						return
					}),
				gomonkey.ApplyFunc((*metricsClient).reportCPUFlagsToRedisTicker,
					func(*metricsClient, <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(initCronjobClient, func(<-chan struct{}) {
					return
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := CreateMetricsClient(&common.MetricsServerConfig{}, make(<-chan struct{}))
			So(err, ShouldBeNil)
		})
		Convey("succeedToInit", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam,
					stopCh <-chan struct{}, options ...redisclient.Option) (*redisclient.Client, error) {
					return &redisclient.Client{}, nil
				}),
				gomonkey.ApplyFunc((*metricsClient).reportCPUFlagsToRedisTicker,
					func(*metricsClient, <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(initCronjobClient, func(<-chan struct{}) {
					return
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := CreateMetricsClient(&common.MetricsServerConfig{}, make(<-chan struct{}))
			So(err, ShouldBeNil)
			So(Client.keepAliveTimeMetrics, ShouldNotBeNil)
		})
	})
}

func mockRPush(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	cmd := &redis.IntCmd{}
	cmd.SetErr(pushError)
	return cmd
}

func mockPing(_ *redisclient.Client, ctx context.Context) *redis.StatusCmd {
	return &redis.StatusCmd{}
}

func mockSet(_ *redisclient.Client, ctx context.Context, key string, value interface{},
	expiration time.Duration) *redis.StatusCmd {
	cmd := &redis.StatusCmd{}
	cmd.SetErr(setError)
	return cmd
}

func clientSetup() {
	client := &redisclient.Client{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "RPush", mockRPush),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Ping", mockPing),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Set", mockSet),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
			return []byte{}, nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam,
			stopCh <-chan struct{}, options ...redisclient.Option) (*redisclient.Client, error) {
			return &redisclient.Client{}, nil
		}),
	})
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(initCronjobClient, func(<-chan struct{}) {
			return
		}),
		gomonkey.ApplyFunc(RetrieveDynamicKeepAliveTimeForWorker, func(*common.Configs) {
			return
		}),
	})
	gomonkey.ApplyFunc((*metricsClient).reportCPUFlagsToRedisTicker,
		func(*metricsClient, <-chan struct{}) {
			return
		})
	CreateMetricsClient(&common.MetricsServerConfig{ServerMode: "single"}, make(<-chan struct{}))
}

func clientTearDown() {
	patches.ResetAll()
	pushError = nil
	setError = nil
	Client = &metricsClient{}
}

func buildMetrics() common.RuntimeMetricsProperty {
	timeWindows := make([]workertypes.TimeWindow, 1)
	timeWindows = append(timeWindows, window)
	res, err := json.Marshal(timeWindows)
	So(err, ShouldBeNil)

	metrics := common.RuntimeMetricsProperty{
		FunctionURN:               urn,
		PodName:                   "pod01",
		MetricsName:               "bill",
		MetricsValue:              string(res),
		IsTriggeredBridgeFunction: false,
		IsBridgeFunction:          false,
	}
	return metrics
}

func TestReportUsedResourceTimeWindowsWithSuccess(t *testing.T) {
	Convey("TestReportUsedResourceTimeWindowsWithSuccess", t, func() {
		pushError = nil
		clientSetup()
		defer clientTearDown()

		metrics := buildMetrics()
		err := Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldBeNil)
	})
}

func TestReportUsedResourceTimeWindowsWithTriggerBridgeSituation(t *testing.T) {
	Convey("TestReportUsedResourceTimeWindowsWithTriggerBridgeSituation", t, func() {
		pushError = nil
		clientSetup()
		defer clientTearDown()

		metrics := buildMetrics()
		metrics.IsBridgeFunction = true
		metrics.IsTriggeredBridgeFunction = true
		err := Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldBeNil)
	})
}

func TestReportUsedResourceTimeWindowsWithFail(t *testing.T) {
	Convey("TestReportUsedResourceTimeWindowsWithFail", t, func() {
		pushError = errors.New("error")
		clientSetup()
		defer clientTearDown()

		metrics := buildMetrics()
		err := Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldBeNil)

		metrics.FunctionURN = "error_urn"
		err = Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldNotBeNil)

		metrics.FunctionURN = urn
		metrics.MetricsName = constants.MetricsUsedResourceTimeWindow
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(redisclient.WriteToDisk, func(redisCache redisclient.RedisCache) error { return errors.New("error") }),
		})
		err = Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldNotBeNil)

		metrics.FunctionURN = urn
		metrics.MetricsName = constants.ReservedInstanceMetricsTimeWindow
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(redisclient.WriteToDisk, func(redisCache redisclient.RedisCache) error { return errors.New("error") }),
		})
		err = Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldNotBeNil)

		pushError = nil
		metrics.MetricsName = constants.MetricsUsedResourceTimeWindow
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(redisclient.TakeCacheToRedis, func(c *redisclient.Client) error { return errors.New("error") }),
		})
		err = Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldNotBeNil)

		metrics.MetricsName = constants.MetricsUsedResourceTimeWindow
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) { return nil, errors.New("failed to marshal") }),
		})
		err = Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldNotBeNil)

		metrics.MetricsValue = "error_metrics"
		metrics.MetricsName = constants.MetricsUsedResourceTimeWindow
		err = Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldNotBeNil)
	})
}

func TestReportUsedResourceTimeWindowsWithReconnected(t *testing.T) {
	Convey("TestReportUsedResourceTimeWindowsWithReconnected", t, func() {
		clientSetup()
		Client.redisConnected = false
		defer clientTearDown()

		metrics := buildMetrics()
		err := Client.ReportUsedResourceTimeWindows(metrics, functionKeyWithResource)
		So(err, ShouldBeNil)
	})
}

func buildAsyncResponse() *asyncResponse {
	return &asyncResponse{
		StatusCode: 0,
	}
}

func TestReportAsyncResponseWithSuccess(t *testing.T) {
	Convey("TestReportAsyncResponse", t, func() {
		clientSetup()
		defer clientTearDown()

		rsp := buildAsyncResponse()
		err := Client.reportAsyncResponse("id0", rsp)
		So(err, ShouldBeNil)
	})
}

func TestReportAsyncResponseWithFail(t *testing.T) {
	Convey("TestReportAsyncResponse", t, func() {
		setError = errors.New("set error")
		setReportAsyncRespRetryInterval(1 * time.Millisecond)
		clientSetup()
		defer clientTearDown()

		rsp := buildAsyncResponse()
		err := Client.reportAsyncResponse("id0", rsp)
		So(err, ShouldNotBeNil)
	})
}

func TestCheckRedisConnectivity(t *testing.T) {
	var isCalled = 0
	Convey("Test check redis connect succeed", t, func() {
		mc := &metricsClient{}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*metricsClient).GetRedisClient, func(*metricsClient) *redisclient.Client {
				return nil
			}),
			gomonkey.ApplyFunc((*metricsClient).initClient,
				func(*metricsClient, <-chan struct{}) error {
					isCalled++
					return nil
				}),
			gomonkey.ApplyFunc((*alarm.Client).ClearAlarm,
				func(a *alarm.Client, almType alarm.Type) {
					return
				}),
			gomonkey.ApplyFunc((*redisclient.Client).Ping,
				func(_ *redisclient.Client, _ context.Context) *redis.StatusCmd {
					return &redis.StatusCmd{}
				}),
			gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				ch := make(chan time.Time, 1)
				ch <- time.Time{}
				return &time.Ticker{C: ch}
			}),
		})
		defer patches.ResetAll()
		stopCh := make(chan struct{}, 0)
		go mc.checkRedisConnectivity(stopCh)
		time.Sleep(100 * time.Millisecond)
		stopCh <- struct{}{}
		So(isCalled, ShouldNotEqual, 0)
	})
	Convey("Test check redis init failed", t, func() {
		mc := &metricsClient{}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*metricsClient).GetRedisClient, func(*metricsClient) *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyFunc((*metricsClient).initClient,
				func(*metricsClient, <-chan struct{}) error {
					isCalled++
					return errors.New("init error")
				}),
			gomonkey.ApplyFunc((*alarm.Client).ClearAlarm,
				func(a *alarm.Client, almType alarm.Type) {
					return
				}),
			gomonkey.ApplyFunc((*redisclient.Client).Ping,
				func(_ *redisclient.Client, _ context.Context) *redis.StatusCmd {
					return &redis.StatusCmd{}
				}),
			gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				ch := make(chan time.Time, 1)
				ch <- time.Time{}
				return &time.Ticker{C: ch}
			}),
		})
		defer patches.ResetAll()
		stopCh := make(chan struct{}, 0)
		go mc.checkRedisConnectivity(stopCh)
		time.Sleep(100 * time.Millisecond)
		stopCh <- struct{}{}
		So(isCalled, ShouldNotEqual, 0)
	})
}

func TestReportCPUFlagsToRedis(t *testing.T) {
	var isFailed = 0
	Convey("Test report to redis succeed", t, func() {
		clientSetup()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*metricsClient).GetRedisClient, func(*metricsClient) *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyFunc((*redis.Client).Ping,
				func(_ *redis.Client, _ context.Context) *redis.StatusCmd {
					return &redis.StatusCmd{}
				}),
			gomonkey.ApplyFunc((*redisclient.Client).Set,
				func(_ *redisclient.Client, ctx context.Context,
					key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
					return &redis.StatusCmd{}
				}),
			gomonkey.ApplyFunc(log.GetLogger().Errorf, func(format string, paras ...interface{}) {
				isFailed++
			}),
		})
		defer patches.ResetAll()
		mc := &metricsClient{}
		reportCPUFlagsToRedis(mc.GetRedisClient())
		So(isFailed, ShouldEqual, 0)
	})

	Convey("Test cpuFlags nil", t, func() {
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(utils.GetCPUFlagList, func() []string {
				return nil
			}),
			gomonkey.ApplyFunc(log.GetLogger().Debugf, func(format string, paras ...interface{}) {
				isFailed++
			}),
		})
		defer patches.ResetAll()
		mc := &metricsClient{}
		reportCPUFlagsToRedis(mc.GetRedisClient())
		So(isFailed, ShouldEqual, 0)
	})
}

func TestReportNodeDiskUsageToRedis(t *testing.T) {
	Convey("Test report to redis succeed", t, func() {
		clientSetup()
		var isFailed = 0
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*metricsClient).GetRedisClient, func(*metricsClient) *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyFunc((*redisclient.Client).Set,
				func(_ *redisclient.Client, ctx context.Context,
					key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
					return &redis.StatusCmd{}
				}),
			gomonkey.ApplyFunc(log.GetLogger().Errorf, func(format string, paras ...interface{}) {
				isFailed++
			}),
		})
		defer patches.ResetAll()
		mc := &metricsClient{}
		reportNodeDiskUsageToRedis(mc.GetRedisClient())
		So(isFailed, ShouldEqual, 0)
	})

	Convey("Test report to redis fail case 1", t, func() {
		var isSet = 0
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*redisclient.Client).Set,
				func(_ *redisclient.Client, ctx context.Context,
					key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
					isSet++
					return &redis.StatusCmd{}
				}),
		})
		defer patches.ResetAll()
		reportNodeDiskUsageToRedis(nil)
		So(isSet, ShouldEqual, 0)
	})

	Convey("Test report to redis fail case 2", t, func() {
		var isSet = 0
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(exec.Command, func(command string, args ...string) *exec.Cmd {
				return nil
			}),
			gomonkey.ApplyFunc((*redisclient.Client).Set,
				func(_ *redisclient.Client, ctx context.Context,
					key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
					isSet++
					return &redis.StatusCmd{}
				}),
		})
		defer patches.ResetAll()
		reportNodeDiskUsageToRedis(nil)
		So(isSet, ShouldEqual, 0)
	})
}

func TestGetDiskUsage(t *testing.T) {
	Convey("Test report to redis succeed", t, func() {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&exec.Cmd{}), "Output", func(_ *exec.Cmd) ([]byte, error) {
			// 返回一个预定义的输出，用于模拟命令的执行结果
			return []byte("Filesystem     1K-blocks    Used Available Use% Mounted on\n/dev/sda1       12345678 1234567   9876543  12% /"), nil
		})
		defer patch.Reset()
		usage, err := getDiskUsage()
		assert.NoError(t, err)
		assert.Equal(t, diskUsage{Filesystem: "/dev/sda1", Size: "12345678", Used: "1234567", Avail: "9876543", Percentage: "12%", MountOn: "/"}, usage)
	})
}

func TestReportResourceUsageToRedis(t *testing.T) {
	bytes, _ := json.Marshal(resourceusage.UploadResourceData{
		UploadTime: time.Now().Unix(),
	})
	type args struct {
		c                  *redisclient.Client
		uploadResourceData []byte
	}
	tests := []struct {
		name      string
		args      args
		patchFunc testutils.PatchesFunc
	}{
		{
			"test client is nil",
			args{
				c: nil,
			},
			func() testutils.PatchSlice {
				return testutils.InitPatchSlice()
			},
		},
		{
			"test ok",
			args{
				c:                  &redisclient.Client{},
				uploadResourceData: bytes,
			},
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HSet",
						func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		ReportResourceUsageToRedis(tt.args.c, tt.args.uploadResourceData)
		patches.ResetAll()
	}
}

func TestReportCaeMetricsToRedis(t *testing.T) {
	bytes, _ := json.Marshal(resourceusage.UploadResourceData{
		UploadTime: time.Now().Unix(),
	})
	type args struct {
		c                  *redisclient.Client
		uploadResourceData []byte
	}
	tests := []struct {
		name      string
		args      args
		patchFunc testutils.PatchesFunc
	}{
		{
			"test client is nil",
			args{
				c: nil,
			},
			func() testutils.PatchSlice {
				return testutils.InitPatchSlice()
			},
		},
		{
			"test ok",
			args{
				c:                  &redisclient.Client{},
				uploadResourceData: bytes,
			},
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HSet",
						func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Err",
						func(_ *redis.IntCmd) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		ReportCaeMetricsToRedis(tt.args.c, tt.args.uploadResourceData)
		patches.ResetAll()
	}
}

func TestDynamicAdjustKeepAliveTimeForWorker(t *testing.T) {
	mockFuncUrn := "mockFuncUrn"
	updateTime := "lastUpdateTime"
	setupTestSuite := func() *keepAliveMetrics {
		return &keepAliveMetrics{
			dynamicKeepAliveTimeMap:     make(map[string]string),
			dynamicKeepAliveTimeMapLock: sync.RWMutex{},
			keepaliveObserver:           nil,
		}
	}
	type args struct {
		config           *common.DynamicKeepAliveConfig
		observer         common.KeepAliveObserver
		c                *redisclient.Client
		keepAliveMetrics *keepAliveMetrics
	}
	tests := []struct {
		name        string
		args        args
		expected    map[string]string
		patchesFunc testutils.PatchesFunc
	}{
		{
			name: "case 1 test when hot load config is false",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: false,
				},
				keepAliveMetrics: setupTestSuite(),
			},
			expected: make(map[string]string),
		}, {
			name: "case 2 test when redis client is nil",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: true,
				},
				keepAliveMetrics: setupTestSuite(),
				c:                nil,
			},
			expected: make(map[string]string),
		}, {
			name: "case 3 test when metrics is nil",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: true,
				},
				keepAliveMetrics: nil,
				c:                nil,
			},
			expected: make(map[string]string),
		}, {
			name: "case 4 setup map successfully after retrieving data from redis",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: true,
				},
				keepAliveMetrics: setupTestSuite(),
				c:                &redisclient.Client{},
			},
			expected: map[string]string{
				mockFuncUrn: "50",
				updateTime:  strconv.FormatInt(time.Now().Unix(), 10),
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*redisclient.Client).HGetAll,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							res := &redis.MapStringStringCmd{}
							res.SetErr(nil)
							res.SetVal(map[string]string{
								mockFuncUrn: "50",
								updateTime:  strconv.FormatInt(time.Now().Unix(), 10),
							})
							return res
						},
					),
				})
				return patches
			},
		}, {
			name: "case 5 test redis client returned error",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: true,
				},
				keepAliveMetrics: setupTestSuite(),
				c:                &redisclient.Client{},
			},
			expected: make(map[string]string),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*redisclient.Client).HGetAll,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							res := &redis.MapStringStringCmd{}
							res.SetErr(errors.New("mock get Redis data error"))
							return res
						},
					),
				})
				return patches
			},
		}, {
			name: "case 6 test redis client returned error",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: true,
				},
				keepAliveMetrics: setupTestSuite(),
				c:                &redisclient.Client{},
			},
			expected: make(map[string]string),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*redisclient.Client).HGetAll,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							res := &redis.MapStringStringCmd{}
							res.SetErr(nil)
							res.SetVal(map[string]string{})
							return res
						},
					),
				})
				return patches
			},
		}, {
			name: "case 7 bad format data from Redis",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: true,
				},
				keepAliveMetrics: setupTestSuite(),
				c:                &redisclient.Client{},
			},
			expected: make(map[string]string),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*redisclient.Client).HGetAll,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							res := &redis.MapStringStringCmd{}
							res.SetErr(nil)
							res.SetVal(map[string]string{
								updateTime: "50.5",
							})
							return res
						},
					),
				})
				return patches
			},
		}, {
			name: "case 8 outdated data will be ignored",
			args: args{
				config: &common.DynamicKeepAliveConfig{
					Enable: true,
				},
				keepAliveMetrics: setupTestSuite(),
				c:                &redisclient.Client{},
			},
			expected: make(map[string]string),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc((*redisclient.Client).HGetAll,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.MapStringStringCmd {
							res := &redis.MapStringStringCmd{}
							res.SetErr(nil)
							res.SetVal(map[string]string{
								mockFuncUrn: "50",
								updateTime:  strconv.FormatInt(time.Now().Unix()-3700, 10),
							})
							return res
						},
					),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &metricsClient{
				keepAliveTimeMetrics: tt.args.keepAliveMetrics,
				c:                    tt.args.c,
			}
			common.FConfig.DynamicKeepAliveConfig = tt.args.config
			var testPatches testutils.PatchSlice
			if tt.patchesFunc != nil {
				testPatches = tt.patchesFunc()
			}
			client.dynamicAdjustKeepAliveTimeForWorker()
			if client.keepAliveTimeMetrics == nil {
				return
			}
			if !reflect.DeepEqual(client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap, tt.expected) {
				t.Errorf("actual %v, want %v", client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap, tt.expected)
			}
			if tt.patchesFunc != nil {
				testPatches.ResetAll()
			}
		})
	}
}

func TestRetrieveDynamicKeepAliveTimeForWorker(t *testing.T) {
	stopChan := make(chan struct{})
	mockCron := &cronJobMgr{
		mgr:      nil,
		entryID:  nil,
		stopChan: stopChan,
	}
	mockKeepAliveMetrics := &keepAliveMetrics{
		dynamicKeepAliveTimeMap:     make(map[string]string),
		dynamicKeepAliveTimeMapLock: sync.RWMutex{},
	}
	type args struct {
		config    *common.Configs
		c         *redisclient.Client
		cron      *cronJobMgr
		kaMetrics *keepAliveMetrics
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "case 1 redis client is nil",
			args: args{
				c:         nil,
				kaMetrics: mockKeepAliveMetrics,
			},
		}, {
			name: "case 2 stop chan is nil",
			args: args{
				c:         &redisclient.Client{},
				cron:      &cronJobMgr{},
				kaMetrics: mockKeepAliveMetrics,
			},
		}, {
			name: "case 3 disable dynamic keep alive policy",
			args: args{
				c:         &redisclient.Client{},
				cron:      mockCron,
				kaMetrics: mockKeepAliveMetrics,
				config: &common.Configs{DynamicKeepAliveConfig: &common.DynamicKeepAliveConfig{
					Enable: false,
				}},
			},
		}, {
			name: "case 4 enable dynamic keep alive policy",
			args: args{
				c:         &redisclient.Client{},
				kaMetrics: mockKeepAliveMetrics,
				cron: &cronJobMgr{
					mgr:      cron.New(),
					stopChan: stopChan,
				},
				config: &common.Configs{DynamicKeepAliveConfig: &common.DynamicKeepAliveConfig{
					Enable:        true,
					StartTimeCron: "10 * * * *",
				}},
			},
		}, {
			name: "case 5 no cron string",
			args: args{
				c:         &redisclient.Client{},
				kaMetrics: mockKeepAliveMetrics,
				cron: &cronJobMgr{
					mgr:      cron.New(),
					stopChan: stopChan,
				},
				config: &common.Configs{DynamicKeepAliveConfig: &common.DynamicKeepAliveConfig{
					Enable: true,
				}},
			},
		}, {
			name: "case 6 keepaliveObserver is nil",
			args: args{
				c:         &redisclient.Client{},
				kaMetrics: nil,
				cron: &cronJobMgr{
					mgr:      cron.New(),
					stopChan: stopChan,
				},
				config: &common.Configs{DynamicKeepAliveConfig: &common.DynamicKeepAliveConfig{
					Enable: true,
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			Client.keepAliveTimeMetrics = tt.args.kaMetrics
			Client.c = tt.args.c
			cronJobClient = tt.args.cron
			RetrieveDynamicKeepAliveTimeForWorker(tt.args.config)
		})
	}
}

func Test_reportCPUFlagsToRedisTicker(t *testing.T) {
	var stopCh chan struct{}
	client := &metricsClient{}
	client.reportCPUFlagsToRedisTicker(stopCh)
}

func Test_initCronjobClient(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc((*cron.Cron).Start, func(_ *cron.Cron) {}),
		gomonkey.ApplyFunc(RetrieveDynamicKeepAliveTimeForWorker, func(config *common.Configs) {}),
		gomonkey.ApplyFunc(cronjobTearDown, func() {}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()
	stopCh := make(chan struct{})
	initCronjobClient(stopCh)
}

func Test_reportMetricsToRedis(t *testing.T) {
	Convey("Test client is nil", t, func() {
		count := 0
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HSet",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					count++
					return &redis.IntCmd{}
				}),
		})
		reportMetricsToRedis(nil, "redisKey", []byte("value"))
		So(count, ShouldEqual, 0)
		patches.ResetAll()
	})

	Convey("Test value is empty", t, func() {
		count := 0
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HSet",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					count++
					return &redis.IntCmd{}
				}),
		})
		reportMetricsToRedis(&redisclient.Client{}, "redisKey", []byte(""))
		So(count, ShouldEqual, 0)
		patches.ResetAll()
	})

	Convey("Test HSet ok", t, func() {
		count := 0
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HSet",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					if key == "redisKey" {
						count++
					}
					return &redis.IntCmd{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					return 0, nil
				}),
		})
		reportMetricsToRedis(&redisclient.Client{}, "redisKey", []byte("value"))
		So(count, ShouldEqual, 1)
		patches.ResetAll()
	})

	Convey("Test HSet retry ok", t, func() {
		count := 0
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HSet",
				func(r *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
					if key == "redisKey" {
						count++
					}
					return &redis.IntCmd{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					if count == 2 {
						return 0, nil
					}
					// for retry
					return 0, errors.New("test error")
				}),
		})
		reportMetricsToRedis(&redisclient.Client{}, "redisKey", []byte("value"))
		So(count, ShouldEqual, 2)
		patches.ResetAll()
	})

}
