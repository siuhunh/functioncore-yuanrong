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

package constant

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/pprof"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	mockUtils "functioncore/test/utils"
	monitor2 "functioncore/test/utils/monitor"
)

const (
	serverPort              = "8888"
	defaultMetaEtcdCafile   = "/home/sn/resource/ca/ca.pem"
	defaultMetaEtcdCertfile = "/home/sn/resource/ca/cert.pem"
	defaultMetaEtcdKeyfile  = "/home/sn/resource/ca/key.pem"

	defaultRouterEtcdCafile   = "/home/sn/resource/routerEtcd/ca.pem"
	defaultRouterEtcdCertfile = "/home/sn/resource/routerEtcd/cert.pem"
	defaultRouterEtcdKeyfile  = "/home/sn/resource/routerEtcd/key.pem"
)

var (
	watcher    *monitor2.MockFileWatcher
	maxTimeout = 100*24*3600 + 1
	testConfig = &Config{
		HTTPConfig: &FrontendHTTP{
			RespTimeOut:               int64(maxTimeout),
			WorkerInstanceReadTimeOut: int64(maxTimeout),
			MaxRequestBodySize:        6,
		},
		Etcd: &etcd3.ClientConfig{
			SslEnable: true,
		},
		RouterETCD: &etcd3.ClientConfig{
			SslEnable: true,
		},
		MemoryControlConfig: &types.MemoryControlConfig{
			LowerMemoryPercent:  0.5,
			BodyThreshold:       10,
			HighMemoryPercent:   0.7,
			MemDetectIntervalMs: 100,
		},
		MemoryEvaluatorConfig: &MemoryEvaluatorConfig{
			RequestMemoryEvaluator: 2,
		},
		LoadBalanceConfig: &LoadBalanceConfig{
			ConcurrencyThreshold:       11,
			MaxInstanceThreshold:       10000,
			DisableBackpressureBalance: 1,
			MaxRequestNumThreshold:     100,
			SyncReleaseTimeThreshold:   200,
		},
		EtcdLeaseConfig: &EtcdLeaseConfig{
			LeaseTTL: 10,
			RenewTTL: 10,
		},
	}
)

var (
	testEnvConfig = &Config{
		Runtime: &RuntimeConfig{
			Port:             serverPort,
			AvailableZoneKey: constants.ZoneKey,
		},
		Etcd: &etcd3.ClientConfig{
			ServerList: []string{"127.0.0.1:32379"},
			User:       "root",
			Password:   "0000",
			CaFile:     defaultMetaEtcdCafile,
			CertFile:   defaultMetaEtcdCertfile,
			KeyFile:    defaultMetaEtcdKeyfile,
			SslEnable:  true,
		},
		RouterETCD: &etcd3.ClientConfig{
			ServerList: []string{"127.0.0.2:32379"},
			User:       "root",
			Password:   "1111",
			CaFile:     defaultRouterEtcdCafile,
			CertFile:   defaultRouterEtcdCertfile,
			KeyFile:    defaultRouterEtcdKeyfile,
			SslEnable:  true,
		},
		RetryConfig: &RetryConfig{
			InstanceExceptionRetry: true,
		},
		HTTPConfig: &FrontendHTTP{
			RespTimeOut:               1260,
			WorkerInstanceReadTimeOut: 1260,
			MaxRequestBodySize:        6,
		},
		TrafficLimitParams: &TrafficLimitParams{
			InstanceLimitRate:  20000,
			InstanceBucketSize: 40000,
			FuncLimitRate:      2000,
			FuncBucketSize:     4000,
		},
		LocalAuth: &localauth.AuthConfig{
			AKey:     "000",
			SKey:     "111",
			Duration: 5,
		},
		HTTPSConfig: &tls.InternalHTTPSConfig{
			HTTPSEnable: true,
			TLSProtocol: "TLSv1.2",
			TLSCiphers:  "TLS_ECDHE_RSA",
		},
		AuthenticationEnable:  true,
		FunctionNameSeparator: "@",
		AlarmServerAddress:    "127.0.0.1:0",
		InvokeMaxRetryTimes:   3,
	}
)

func mockReadFile(_ string) ([]byte, error) {
	data, err := json.Marshal(testConfig)
	return data, err
}

func TestInitConstant(t *testing.T) {
	convey.Convey("TestInitConstant", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(ioutil.ReadFile, mockReadFile),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		setEnv()
		convey.So(InitConstant(), convey.ShouldBeNil)
	})
}

func TestInitDefaultConfig(t *testing.T) {
	convey.Convey("TestInitDefaultConfig", t, func() {
		FConfig = &Config{}
		InitDefaultConfig()
		convey.So(FConfig.MemoryControlConfig.LowerMemoryPercent, convey.ShouldEqual, defaultLowerMemoryPercent)
		convey.So(FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator,
			convey.ShouldEqual, defaultRequestMemoryEvaluator)
		convey.So(FConfig.MemoryControlConfig.StatefulHighMemPercent, convey.ShouldEqual, defaultStatefulHighMemoryPercent)
	})
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
		patches.ApplyFunc(ioutil.ReadFile, mockReadFile)
		defer func() {
			patches.Reset()
		}()

		InitDefaultConfig()
		monitor.SetCreator(createMockFileWatcher)
		stopCh := make(chan struct{}, 1)

		callbackChan := make(chan int, 1)
		WatchConfig(ConfigFilePath, stopCh, func() {
			callbackChan <- 1
		})
		watcher.EventChan <- ConfigFilePath

		<-callbackChan
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
		convey.So(FConfig.LoadBalanceConfig.ConcurrencyThreshold, convey.ShouldEqual,
			testConfig.LoadBalanceConfig.ConcurrencyThreshold)
		convey.So(FConfig.LoadBalanceConfig.MaxInstanceThreshold, convey.ShouldEqual,
			testConfig.LoadBalanceConfig.MaxInstanceThreshold)
		convey.So(FConfig.LoadBalanceConfig.DisableBackpressureBalance, convey.ShouldEqual,
			testConfig.LoadBalanceConfig.DisableBackpressureBalance)
		convey.So(FConfig.LoadBalanceConfig.MaxRequestNumThreshold, convey.ShouldEqual,
			testConfig.LoadBalanceConfig.MaxRequestNumThreshold)
		convey.So(FConfig.LoadBalanceConfig.SyncReleaseTimeThreshold, convey.ShouldEqual,
			testConfig.LoadBalanceConfig.SyncReleaseTimeThreshold)
		close(stopCh)
	})
}

func TestHotLoadCollectPprofInstanceList(t *testing.T) {
	convey.Convey("TestHotLoadCollectPprofInstanceList_not_change", t, func() {
		FConfig.CollectPprofInstanceList = []string{"test1"}
		config := &Config{CollectPprofInstanceList: []string{"test1"}}
		enterPprof := false
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
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
		config := &Config{CollectPprofInstanceList: []string{"test2"}}
		enterPprof := false
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
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

func TestPreprocessConfigWithNoDefaultEnv(t *testing.T) {
	convey.Convey("TestPreprocessConfigWithNoDefaultEnv", t, func() {
		os.Clearenv()
		setEnv()
		config, err := loadConfigFromEnv()
		convey.So(err, convey.ShouldBeNil)

		convey.So(reflect.DeepEqual(config, testEnvConfig), convey.ShouldBeTrue)

	})
}

func TestHotLoadHeartbeatConfig(t *testing.T) {
	convey.Convey("TestHotLoadHeartbeatConfig", t, func() {
		FConfig.HeartbeatConfig = &HeartbeatConfig{
			HeartbeatTimeout:          2,
			HeartbeatInterval:         2,
			HeartbeatTimeoutThreshold: 2,
		}
		convey.So(FConfig.HeartbeatConfig.HeartbeatTimeout, convey.ShouldEqual, 2)
		convey.So(FConfig.HeartbeatConfig.HeartbeatInterval, convey.ShouldEqual, 2)
		convey.So(FConfig.HeartbeatConfig.HeartbeatTimeoutThreshold, convey.ShouldEqual, 2)
		hotLoadHeartbeatConfig(&Config{
			HeartbeatConfig: &HeartbeatConfig{
				HeartbeatTimeout:          1,
				HeartbeatInterval:         1,
				HeartbeatTimeoutThreshold: 1,
			},
		})
		convey.So(FConfig.HeartbeatConfig.HeartbeatTimeout, convey.ShouldEqual, 1)
		convey.So(FConfig.HeartbeatConfig.HeartbeatInterval, convey.ShouldEqual, 1)
		convey.So(FConfig.HeartbeatConfig.HeartbeatTimeoutThreshold, convey.ShouldEqual, 1)
	})
}

func TestHotLoadMultiConcurrencyScheduleConfig(t *testing.T) {
	convey.Convey("TestHotLoadMultiConcurrencyScheduleConfig", t, func() {
		hotLoadMultiConcurrencyScheduleConfig(&Config{
			MultiConcurrencySchedule: MultiConcurrencyScheduleConfig{
				DisableFaaSSchedule: 1,
				FuncWhiteList:       map[string]string{"funcKey": "faas-scheduler-ss-01"},
				TenantWhiteList:     map[string]string{"tenantID": "faas-scheduler-ss-02"},
			},
		})
		convey.So(FConfig.MultiConcurrencySchedule.DisableFaaSSchedule, convey.ShouldEqual, 1)
		convey.So(FConfig.MultiConcurrencySchedule.FuncWhiteList["funcKey"], convey.ShouldEqual, "faas-scheduler-ss-01")
		convey.So(FConfig.MultiConcurrencySchedule.TenantWhiteList["tenantID"], convey.ShouldEqual, "faas-scheduler-ss-02")
	})
}

func setEnv() {
	configJSON := `{
              "etcd": {
                "servers": ["127.0.0.1:32379"],
                "user": "root",
                "password": "0000",
                "cafile": "/home/sn/resource/ca/ca.pem",
                "certfile": "/home/sn/resource/ca/cert.pem",
                "keyfile": "/home/sn/resource/ca/key.pem",
                "sslEnable": true
              },
              "routerETCD": {
                "servers": ["127.0.0.2:32379"],
                "user": "root",
                "password": "1111",
                "cafile": "/home/sn/resource/routerEtcd/ca.pem",
                "certfile": "/home/sn/resource/routerEtcd/cert.pem",
                "keyfile": "/home/sn/resource/routerEtcd/key.pem",
                "sslEnable": true
              },
              "http":{
                "resptimeout": 1260,
                "workerInstanceReadTimeOut": 1260,
                "maxRequestBodySize": "6"
              },
              "authenticationEnable": true,
              "localAuth": {
                "aKey": "000",
                "sKey": "111",
                "duration": 5
              },
              "httpsConfig": {
                "httpsEnable": true,
                "tlsProtocol": "TLSv1.2",
                "tlsCiphers": "TLS_ECDHE_RSA"
              },
              "retry": {
                "instanceExceptionRetry": true
              },
              "runtime": {
                "port": "8888",
                "azkey": "KUBERNETES_IO_AVAILABLEZONE"
              },
              "trafficLimitParams": {
                "instanceLimitRate": 20000,
                "instanceBucketSize": 40000,
                "funcLimitRate": 2000,
                "funcBucketSize": 4000
              },
              "functionNameSeparator": "@",
              "alarmServerAddress": "127.0.0.1:0",
              "invokeMaxRetryTimes": 3
            }`
	os.Setenv(frontendConfigEnvKey, configJSON)
}

func Test_hotLoadEtcdConfig(t *testing.T) {
	type args struct {
		newAllConfig *Config
	}
	etcdConfig := &etcd3.ClientConfig{
		ServerList: []string{"100.100.100.100"},
		User:       "",
		Password:   "",
		CaFile:     "",
		CertFile:   "",
		KeyFile:    "",
		SslEnable:  false,
	}
	FConfig = &Config{Etcd: etcdConfig}
	newConfig := &Config{
		Runtime:               nil,
		Etcd:                  etcdConfig,
		RouterETCD:            nil,
		RetryConfig:           nil,
		HTTPConfig:            nil,
		TrafficLimitParams:    nil,
		LocalAuth:             nil,
		HTTPSConfig:           nil,
		AuthenticationEnable:  false,
		FunctionNameSeparator: "",
		AlarmServerAddress:    "",
		InvokeMaxRetryTimes:   0,
		MemoryControlConfig:   nil,
		MemoryEvaluatorConfig: nil,
		LoadBalanceConfig:     nil,
		EtcdLeaseConfig:       nil,
		HeartbeatConfig:       nil,
	}
	var a args
	a.newAllConfig = newConfig
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
		hotLoadEtcdConfig(a.newAllConfig)
	}
}
