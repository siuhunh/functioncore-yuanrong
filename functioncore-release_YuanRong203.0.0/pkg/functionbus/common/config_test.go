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

package common

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"gotest.tools/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/pprof"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/proxy/ratelimit"
	"functioncore/test/utils"
	monitor2 "functioncore/test/utils/monitor"
)

var (
	watcher    *monitor2.MockFileWatcher
	testConfig = &Configs{
		IdleWorkerTimeout: 10,
		MemoryControlConfig: &types.MemoryControlConfig{
			LowerMemoryPercent:  0.5,
			BodyThreshold:       10,
			HighMemoryPercent:   0.7,
			MemDetectIntervalMs: 100,
		},
		EtcdConfig: &etcd3.ClientConfig{
			SslEnable: true,
		},
		RouterETCDConfig: &etcd3.ClientConfig{
			SslEnable: true,
		},
		MemoryEvaluatorConfig: &MemoryEvaluatorConfig{
			RequestMemoryEvaluator:  2,
			ResponseMemoryEvaluator: 1.1,
		},
		AutoScalerConfig: &AutoScalerConfig{
			UserTolerateDelayRatio:      1.0,
			MaxUserTolerateDelayMs:      1200,
			MaxEvaluatedLatencyMs:       2200,
			StableScaleLimitPerFunction: 4,
			BurstScaleLimitPerFunction:  9,
			ScaleEvaluateWindowLengthMs: 900,
			MaxEvaluateScaleLatencyMs:   400,
			ScaleCalculationIntervalMs:  100,
			IgnorePendingRequests:       true,
		},
		AlarmConfig: &AlarmConfig{
			RouterEtcdLeaseTimeout: 10,
		},
		EtcdLeaseConfig: &EtcdLeaseConfig{
			LeaseTTL: 10,
			RenewTTL: 5,
		},
	}
)

func mockReadFile(_ string) ([]byte, error) {
	config := Configs{
		EtcdConfig:       &etcd3.ClientConfig{},
		RouterETCDConfig: &etcd3.ClientConfig{},
	}
	data, err := json.Marshal(config)
	return data, err
}

func TestGetHotloadMetricsConfWithInvalidPath(t *testing.T) {
	convey.Convey("TestGetRealTimeMetricsServerAddrWithInvalidPath", t, func() {
		commonConfigPath = ""
		defer func() {
			commonConfigPath = ""
		}()
		_, _, err := GetHotloadMetricsConf()
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func mockReadAddrFile(_ string) ([]byte, error) {
	config := Configs{
		EtcdConfig:       &etcd3.ClientConfig{},
		RouterETCDConfig: &etcd3.ClientConfig{},
	}
	data, err := json.Marshal(config)
	return data, err
}

func TestGetHotloadMetricsConf(t *testing.T) {
	convey.Convey("TestGetHotloadMetricsConf", t, func() {
		commonConfigPath = ""
		defer func() {
			commonConfigPath = ""
		}()
		_, _, err := GetHotloadMetricsConf()
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("TestGetHotloadMetricsConf", t, func() {
		config := &Configs{AuthenticationEnable: false, MetricsServerConfig: &MetricsServerConfig{ServerAddr: "127.0.0.1"}}
		data, err := json.Marshal(config)
		if err != nil {
			return
		}
		patcher := gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
			return data, nil
		})
		defer patcher.Reset()
		_, _, err = GetHotloadMetricsConf()
		assert.Equal(t, nil, err)
	})
}

// TestInitConfig -
func TestInitConfig(t *testing.T) {
	convey.Convey("test: test init config ok", t, func() {
		convey.Convey("test: str to bool for authentication", func() {
			tasks := []struct {
				val  string
				want bool
			}{
				{
					// normal test
					val:  "true",
					want: true,
				},
				{
					// trim space
					val:  " true ",
					want: true,
				},
				{
					// uppercase and lowercase letters test
					val:  "TRUE",
					want: true,
				},
				{
					val:  "True",
					want: true,
				},
				// normal test
				{
					val:  "False",
					want: false,
				},
				{
					val:  "FALSE",
					want: false,
				},
				{
					// incorrect format
					val:  "tr ue",
					want: false,
				},
			}
			for _, task := range tasks {
				os.Setenv(AuthenticationEnableEnv, task.val)
				InitConfig("")
				convey.So(FConfig.AuthenticationEnable, convey.ShouldEqual, task.want)
			}
			os.Unsetenv(AuthenticationEnableEnv)
		})
		convey.Convey("test: valid value", func() {
			os.Setenv(AuthenticationEnableEnv, "true")
			os.Setenv(LocalAuthConfigEnv, `{"aKey":"testAKey","sKey":"testSKey","duration":10}`)
			os.Setenv(HTTPSConfigEnv, `{"httpsEnable":true,"tlsProtocol":"TLSv1.2","tlsCiphers":"testCiphers"}`)
			os.Setenv(RequestConfigEnv, `{"requestWorkerTimeout":60}`)
			os.Setenv(AlarmServerAddressEnv, "127.0.0.1:28915")
			alarmServerAddress = os.Getenv(AlarmServerAddressEnv)
			envKey := []string{
				AuthenticationEnableEnv, LocalAuthConfigEnv, HTTPSConfigEnv, RequestConfigEnv, AlarmServerAddressEnv,
			}
			defer func() {
				for _, key := range envKey {
					os.Unsetenv(key)
				}
			}()
			InitConfig("")
			convey.So(FConfig.AuthenticationEnable, convey.ShouldBeTrue)
			convey.So(FConfig.LocalAuth.AKey, convey.ShouldEqual, "testAKey")
			convey.So(FConfig.LocalAuth.SKey, convey.ShouldEqual, "testSKey")
			convey.So(FConfig.LocalAuth.Duration, convey.ShouldEqual, 10)
			convey.So(FConfig.HTTPSConfig.HTTPSEnable, convey.ShouldBeTrue)
			convey.So(FConfig.HTTPSConfig.TLSProtocol, convey.ShouldEqual, "TLSv1.2")
			convey.So(FConfig.HTTPSConfig.TLSCiphers, convey.ShouldEqual, "testCiphers")
			convey.So(FConfig.RequestConfig.RequestWorkerTimeout, convey.ShouldEqual, 60)
			convey.So(FConfig.AlarmServerAddress, convey.ShouldEqual, "127.0.0.1:28915")
		})
	})
}

// TestInitConfig2 -
func TestInitConfig2(t *testing.T) {
	convey.Convey("test: preload the configuration needs to be hot load", t, func() {
		patch := gomonkey.ApplyFunc(loadConfig, func(configPath string) (*Configs, error) {
			return &Configs{
				IdleWorkerTimeout: 300,
				MemoryControlConfig: &types.MemoryControlConfig{
					MemDetectIntervalMs: 30,
				},
				MemoryEvaluatorConfig: &MemoryEvaluatorConfig{
					RequestMemoryEvaluator: 2.5,
				},
				AutoScalerConfig: &AutoScalerConfig{
					MaxEvaluatedLatencyMs: 1000,
				},
				AlarmConfig: &AlarmConfig{
					RouterEtcdLeaseDetectInterval: 10,
					RouterEtcdLeaseTimeout:        10,
				},
			}, nil
		})
		defer func() {
			patch.Reset()
		}()
		os.Setenv(AuthenticationEnableEnv, "true")
		os.Setenv(LocalAuthConfigEnv, `{"aKey":"testAKey","sKey":"testSKey"}`)
		defer func() {
			os.Unsetenv(AuthenticationEnableEnv)
			os.Unsetenv(LocalAuthConfigEnv)
		}()
		InitConfig("")
		convey.So(FConfig.IdleWorkerTimeout, convey.ShouldEqual, 300)
		convey.So(FConfig.MemoryControlConfig.MemDetectIntervalMs, convey.ShouldEqual, 30)
		convey.So(FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator, convey.ShouldEqual, 2.5)
		convey.So(FConfig.AutoScalerConfig.MaxEvaluatedLatencyMs, convey.ShouldEqual, 1000)
		convey.So(FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval, convey.ShouldEqual, 10)
		convey.So(FConfig.AlarmConfig.RouterEtcdLeaseTimeout, convey.ShouldEqual, 10)
		convey.So(FConfig.AuthenticationEnable, convey.ShouldBeTrue)
		convey.So(FConfig.LocalAuth.AKey, convey.ShouldEqual, "testAKey")
		convey.So(FConfig.LocalAuth.SKey, convey.ShouldEqual, "testSKey")
	})
}

func mockReadConfigFile(filename string) ([]byte, error) {

	b, _ := json.Marshal(testConfig)
	return b, nil
}

func createMockFileWatcher(stopCh <-chan struct{}) (monitor.FileWatcher, error) {
	watcher = &monitor2.MockFileWatcher{
		Callbacks: map[string]monitor.FileChangedCallback{},
		StopCh:    stopCh,
		EventChan: make(chan string, 1),
	}
	return watcher, nil
}

func TestHotLoadConfig(t *testing.T) {
	convey.Convey("TestHotLoadConfig", t, func() {
		patches := gomonkey.NewPatches()
		patches.ApplyFunc(ioutil.ReadFile, mockReadConfigFile)
		defer func() {
			patches.Reset()
		}()

		InitDefaultConfig()
		monitor.SetCreator(createMockFileWatcher)
		stopCh := make(chan struct{}, 1)

		callbackChan := make(chan int, 1)
		WatchConfig(ConfigPath, stopCh, func() {
			callbackChan <- 1
		})
		watcher.EventChan <- ConfigPath

		<-callbackChan
		convey.So(FConfig.IdleWorkerTimeout, convey.ShouldEqual, testConfig.IdleWorkerTimeout)
		convey.So(FConfig.MemoryControlConfig.LowerMemoryPercent, convey.ShouldEqual,
			testConfig.MemoryControlConfig.LowerMemoryPercent)
		convey.So(FConfig.MemoryControlConfig.BodyThreshold, convey.ShouldEqual,
			testConfig.MemoryControlConfig.BodyThreshold)
		convey.So(FConfig.MemoryControlConfig.HighMemoryPercent, convey.ShouldEqual,
			testConfig.MemoryControlConfig.HighMemoryPercent)
		convey.So(FConfig.MemoryControlConfig.MemDetectIntervalMs, convey.ShouldEqual,
			testConfig.MemoryControlConfig.MemDetectIntervalMs)

		convey.So(FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator, convey.ShouldEqual,
			testConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator)
		convey.So(FConfig.MemoryEvaluatorConfig.ResponseMemoryEvaluator, convey.ShouldEqual,
			testConfig.MemoryEvaluatorConfig.ResponseMemoryEvaluator)

		convey.So(FConfig.AutoScalerConfig.IgnorePendingRequests, convey.ShouldBeTrue)
		convey.So(FConfig.AutoScalerConfig.UserTolerateDelayRatio, convey.ShouldEqual, 1.0)

		convey.So(FConfig.AlarmConfig.RouterEtcdLeaseTimeout,
			convey.ShouldEqual, testConfig.AlarmConfig.RouterEtcdLeaseTimeout)
		close(stopCh)
	})
}

func TestInitDefaultRateLimitConfig(t *testing.T) {
	convey.Convey("TestInitDefaultRateLimitConfig", t, func() {
		FConfig.RateLimitControlConfig = &types.RateLimitControlConfig{
			TraceIDBucketSize: 999,
			TraceIDFrequency:  0.01,
		}
		initDefaultRateLimitConfig()
		convey.So(ratelimit.GetTraceIDBucketSize(), convey.ShouldEqual, FConfig.RateLimitControlConfig.TraceIDBucketSize)
		convey.So(ratelimit.GetTraceIDFrequency(), convey.ShouldEqual, FConfig.RateLimitControlConfig.TraceIDFrequency)
	})
}

func TestHotLoadRateLimitConfig(t *testing.T) {
	convey.Convey("TestHotLoadRateLimitConfig", t, func() {
		hotLoadRateLimitConfig(&Configs{
			RateLimitControlConfig: &types.RateLimitControlConfig{
				TraceIDBucketSize: 998,
				TraceIDFrequency:  0.02,
			},
		})
		convey.So(ratelimit.GetTraceIDBucketSize(), convey.ShouldEqual, 998)
		convey.So(ratelimit.GetTraceIDFrequency(), convey.ShouldEqual, 0.02)
	})
}

func TestStatefulWorker(t *testing.T) {
	WorkerMap.SetVal("worker1")
	if !WorkerMap.CheckVal("worker1") {
		t.Errorf("this should be true")
	}
	if WorkerMap.CheckVal("worker2") {
		t.Errorf("this should be false")
	}
	WorkerMap.DelVal("worker1")
	if WorkerMap.CheckVal("worker1") {
		t.Errorf("this should be false")
	}
}

func TestHotLoadEtcdCacheConfig(t *testing.T) {
	config := &Configs{
		EtcdCacheConfig: &etcd3.EtcdCacheConfig{
			UpdateCacheFilePeriod:  1,
			FlushCacheThreshold:    2,
			EtcdDiffSyncTimeout:    3,
			CacheUpdateBufSize:     4,
			CacheFilePartitionSize: 5,
		},
	}
	hotLoadEtcdCacheConfig(config)
	convey.ShouldEqual(FConfig.EtcdCacheConfig.UpdateCacheFilePeriod, config.EtcdCacheConfig.UpdateCacheFilePeriod)
	convey.ShouldEqual(FConfig.EtcdCacheConfig.FlushCacheThreshold, config.EtcdCacheConfig.FlushCacheThreshold)
	convey.ShouldEqual(FConfig.EtcdCacheConfig.EtcdDiffSyncTimeout, config.EtcdCacheConfig.EtcdDiffSyncTimeout)
	convey.ShouldEqual(FConfig.EtcdCacheConfig.CacheUpdateBufSize, config.EtcdCacheConfig.CacheUpdateBufSize)
	convey.ShouldEqual(FConfig.EtcdCacheConfig.CacheFilePartitionSize, config.EtcdCacheConfig.CacheFilePartitionSize)
}

func TestHotLoadCollectPprofInstanceList(t *testing.T) {
	convey.Convey("TestHotLoadCollectPprofInstanceList_not_change", t, func() {
		FConfig.CollectPprofInstanceList = []string{"test1"}
		config := &Configs{CollectPprofInstanceList: []string{"test1"}}
		enterPprof := false
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(pprof.NotifyStartPprof, func(collectPprofInstanceList []string) {
				enterPprof = true
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		hotLoadCollectPprofInstanceList(config)
		convey.So(enterPprof, convey.ShouldEqual, false)
	})
	convey.Convey("TestHotLoadCollectPprofInstanceList_change", t, func() {
		FConfig.CollectPprofInstanceList = []string{"test1"}
		config := &Configs{CollectPprofInstanceList: []string{"test2"}}
		enterPprof := false
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(pprof.NotifyStartPprof, func(collectPprofInstanceList []string) {
				enterPprof = true
			}),
		})
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		hotLoadCollectPprofInstanceList(config)
		convey.So(enterPprof, convey.ShouldEqual, true)
	})
}

func Test_hotLoadEtcdConfig(t *testing.T) {
	config := &Configs{
		EtcdConfig: &etcd3.ClientConfig{
			ServerList: []string{"100.100.100.100"},
			User:       "",
			Password:   "",
			CaFile:     "",
			CertFile:   "",
			KeyFile:    "",
			SslEnable:  false,
		},
	}
	FConfig = &Configs{
		EtcdConfig: &etcd3.ClientConfig{
			ServerList: []string{"100.100.100.100"},
			User:       "",
			Password:   "",
			CaFile:     "",
			CertFile:   "",
			KeyFile:    "",
			SslEnable:  false,
		},
	}
	hotLoadEtcdConfig(config)
	convey.ShouldEqual(FConfig.EtcdConfig.ServerList, config.EtcdConfig.ServerList)
	convey.ShouldEqual(FConfig.EtcdConfig.User, config.EtcdConfig.User)
	convey.ShouldEqual(FConfig.EtcdConfig.Password, config.EtcdConfig.Password)
	convey.ShouldEqual(FConfig.EtcdConfig.CaFile, config.EtcdConfig.CaFile)
	convey.ShouldEqual(FConfig.EtcdConfig.CertFile, config.EtcdConfig.CertFile)
	convey.ShouldEqual(FConfig.EtcdConfig.KeyFile, config.EtcdConfig.KeyFile)
	convey.ShouldEqual(FConfig.EtcdConfig.SslEnable, config.EtcdConfig.SslEnable)
}

func Test_getReportResourceUsageTickConfig(t *testing.T) {
	tests := []struct {
		name      string
		patchFunc utils.PatchesFunc
		want      int64
	}{
		{
			"test get report resource usage success", func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "100" })})
				return patches
			},
			100,
		},
		{
			"test get report resource error code",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "-1" })})
				return patches
			},
			0,
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		if got := getReportResourceUsageTickConfig(); got != tt.want {
			t.Errorf("getReportResourceUsageTickConfig() = %v, want %v", got, tt.want)
		}
		patches.ResetAll()
	}
}

func Test_getReportResourceUsageDataLenConfig(t *testing.T) {
	tests := []struct {
		name      string
		patchFunc utils.PatchesFunc
		want      int64
	}{
		{
			"test get report resource usage success",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "100" })})
				return patches
			},
			100,
		},
		{
			"test get report resource error code",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv, func(key string) string { return "-1" })})
				return patches
			},
			0,
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		t.Run(tt.name, func(t *testing.T) {
			if got := getReportResourceUsageDataLenConfig(); got != tt.want {
				t.Errorf("getReportResourceUsageDataLenConfig() = %v, want %v", got, tt.want)
			}
		})
		patches.ResetAll()
	}
}

func Test_hotLoadDynamicAdjustKeepAliveTimeConfig(t *testing.T) {
	setup := func() {
		FConfig = &Configs{
			DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        false,
				StartTime:     10,
				StartTimeCron: "10 * * * *",
				Retry:         10,
			},
		}
	}
	tests := []struct {
		name           string
		hotLoadConfig  *Configs
		expectedConfig *Configs
		observer       HotLoadConfigCallback
	}{
		{
			name: "case 1 normal hot load config",
			hotLoadConfig: &Configs{DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        true,
				StartTime:     20,
				StartTimeCron: "",
				Retry:         20,
			}},
			expectedConfig: &Configs{DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        true,
				StartTime:     20,
				StartTimeCron: "20 * * * *",
				Retry:         20,
			}},
		}, {
			name:          "case 2 nil hot load config",
			hotLoadConfig: &Configs{DynamicKeepAliveConfig: nil},
			expectedConfig: &Configs{DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        false,
				StartTime:     10,
				StartTimeCron: "10 * * * *",
				Retry:         10,
			}},
		}, {
			name: "case 3 abnormal retry",
			hotLoadConfig: &Configs{DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        true,
				StartTime:     20,
				StartTimeCron: "",
				Retry:         50,
			}},
			expectedConfig: &Configs{DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        true,
				StartTime:     20,
				StartTimeCron: "20 * * * *",
				Retry:         40,
			}},
		}, {
			name: "case 4 trigger call back",
			hotLoadConfig: &Configs{DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        true,
				StartTime:     20,
				StartTimeCron: "",
				Retry:         20,
			}},
			expectedConfig: &Configs{DynamicKeepAliveConfig: &DynamicKeepAliveConfig{
				Enable:        true,
				StartTime:     20,
				StartTimeCron: "20 * * * *",
				Retry:         20,
			}},
			observer: func(configs *Configs) {},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup()
			adjustKeepaliveDynamicallyCallBack = tt.observer
			hotLoadDynamicAdjustKeepAliveTimeConfig(tt.hotLoadConfig)
			if !reflect.DeepEqual(FConfig.DynamicKeepAliveConfig, tt.expectedConfig.DynamicKeepAliveConfig) {
				t.Errorf("dynamicKeepAliveConfig not configed as epxected, actual: %v, expected: %v",
					FConfig.DynamicKeepAliveConfig, tt.expectedConfig.DynamicKeepAliveConfig)
			}
		})
	}
}

func Test_getVPAConfig(t *testing.T) {
	tests := []struct {
		name        string
		want        *VerticalAutoscalingConfig
		patchesFunc utils.PatchesFunc
	}{
		{
			name: "both env undefined",
			want: &VerticalAutoscalingConfig{
				Enable:                    0,
				ResourceViewAlignInterval: 60,
			},
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				return p
			},
		}, {
			name: "invalid env value",
			want: &VerticalAutoscalingConfig{
				Enable:                    0,
				ResourceViewAlignInterval: 60,
			},
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						if key == EnableVerticalAutoscalingEnv {
							return "wrongType"
						}
						if key == UpdateResourceViewIntervalEnv {
							return "wrongType"
						}
						return ""
					})})
				return p
			},
		}, {
			name: "wrong type and valid env setting",
			want: &VerticalAutoscalingConfig{
				Enable:                    0,
				ResourceViewAlignInterval: 300,
			},
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						if key == EnableVerticalAutoscalingEnv {
							return "wrongType"
						}
						if key == UpdateResourceViewIntervalEnv {
							return "777"
						}
						return ""
					})})
				return p
			},
		}, {
			name: "wrong type and invalid env setting",
			want: &VerticalAutoscalingConfig{
				Enable:                    0,
				ResourceViewAlignInterval: 60,
			},
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						if key == EnableVerticalAutoscalingEnv {
							return "wrongType"
						}
						if key == UpdateResourceViewIntervalEnv {
							return "-9"
						}
						return ""
					})})
				return p
			},
		}, {
			name: "normal case",
			want: &VerticalAutoscalingConfig{
				Enable:                    0,
				ResourceViewAlignInterval: 66,
			},
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc(os.Getenv,
					func(key string) string {
						if key == EnableVerticalAutoscalingEnv {
							return "wrongType"
						}
						if key == UpdateResourceViewIntervalEnv {
							return "66"
						}
						return ""
					})})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			if got := getVPAConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getVPAConfig() = %v, want = %v", got, tt.want)
			}
			p.ResetAll()
		})
	}
}

func TestHotLoadMaxInvokeRespBodySize(t *testing.T) {
	config := &Configs{
		MaxInvokeRespBodySize: 100,
	}
	hotLoadMaxInvokeRespBodySize(config)
	convey.ShouldEqual(FConfig.MaxInvokeRespBodySize, config.MaxInvokeRespBodySize)
	convey.ShouldEqual(FConfig.MaxInvokeRespBodySize, 100)
}

func TestHotLoadReportInnerInvokeIndicatorsSwitch(t *testing.T) {
	config := &Configs{
		ReportInnerInvokeIndicatorEnable: true,
	}
	hotLoadMaxInvokeRespBodySize(config)
	convey.ShouldEqual(FConfig.ReportInnerInvokeIndicatorEnable, config.ReportInnerInvokeIndicatorEnable)
	convey.ShouldEqual(FConfig.ReportInnerInvokeIndicatorEnable, true)

	hotLoadMaxInvokeRespBodySize(config)
	convey.ShouldEqual(FConfig.ReportInnerInvokeIndicatorEnable, true)
}
