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

package fusion

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/agiledragon/gomonkey/v2"
	"github.com/fsnotify/fsnotify"
	"github.com/smartystreets/goconvey/convey"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/logger/logservice"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/trace"
	"functioncore/pkg/common/types"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/cache"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/rpc"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
	mockUtils "functioncore/test/utils"
)

func TestLogCompressFileFilter(t *testing.T) {
	tests := []struct {
		path     string
		excepted bool
	}{
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log.20220319211712134",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log.20220319211712134.zip",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/python-runtime.log.2022031921171213",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/nodejs-runtime.log.2022031921171213.zip",
			excepted: true,
		},
		{
			path:     "a/home/sn/log/service/runtime/java-runtime.log.20220319211712134",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log.202203192117121345",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime_1.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime_20.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime_2.log.20220319211712134",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime_2.log.20220319211712134.zip",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/php-runtime.log.20220319211712134",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/php-runtime.log.2022031921171213.zip",
			excepted: true,
		},
		{
			path:     "a/home/sn/log/service/runtime/php-runtime.log.20220319211712134",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/php-runtime.log.202203192117121345",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/python2-runtime.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/python2-runtime.log.202203192117121345",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/python2-runtime.log.20220319211712134",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/python2-runtime.log.20220319211712134.zip",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/python2-runtime.log.202203192117121345.zip",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime_1.log.gz",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime_123456.log.gz",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log.gz",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log.202203192117121345.gz",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/gc-2023-12-04_20-24-32.log.0.current",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/gc-2023-12-04_20-24-32.log.2",
			excepted: true,
		},
		{
			path:     "a/home/sn/log/service/runtime/gc-2023-12-04_20-24-32.log.2",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/.gc-2023-12-05_17-15-01.log.0.swx",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/.gc-2023-12-05_17-15-01.log.0.swp",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime.log.202203192117121345",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime.log.20220319211712134",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime.log.20220319211712134.zip",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime.log.202203192117121345.zip",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/cppbin-runtime.20240430101829.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime_0523.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime_0602.log",
			excepted: true,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.excepted, logCompressFileRegex.MatchString(test.path), test.path)
	}
}

func TestCangjielogCompressFileRegex(t *testing.T) {
	tests := []struct {
		path     string
		excepted bool
	}{
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log.20220319211712134",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/java-runtime.log.20220319211712134.zip",
			excepted: false,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime_0523.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime_0523.20250410092658743949.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime_0523.202203192117121345.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime_0602.log",
			excepted: true,
		},
		{
			path:     "/home/sn/log/service/runtime/cangjie-runtime_0602.20250510103132150775.log",
			excepted: true,
		},
	}

	for _, test := range tests {
		assert.Equal(t, test.excepted, cangjieLogCompressFileRegex.MatchString(test.path), test.path)
	}
}

func TestHeartbeatCheck(t *testing.T) {
	p := gomonkey.NewPatches()

	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		return nil
	})

	finish := make(chan struct{})
	p.ApplyGlobalVar(&runtime.ProcessExitSignal, finish)

	var fr *fakeRuntime
	p.ApplyMethodSeq(reflect.TypeOf(fr), "Healthz", []gomonkey.OutputCell{
		{Values: gomonkey.Params{true, nil}},
		{Values: gomonkey.Params{false, nil}},
		{Values: gomonkey.Params{true, snerror.New(snerror.RPCClientEmptyErrorCode, snerror.ErrText(snerror.RPCClientEmptyErrorCode))}},
		{Values: gomonkey.Params{true, errors.New("my error")}},
	})

	c := make(chan time.Time)
	timer := &time.Timer{C: c}
	p.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
		return timer
	})
	p.ApplyMethod(reflect.TypeOf(timer), "Reset", func(_ *time.Timer, _ time.Duration) bool { return true })
	p.ApplyMethod(reflect.TypeOf(timer), "Stop", func(_ *time.Timer) bool { return true })
	defer p.Reset()

	cfg := &config.Configuration{RuntimeConfig: config.RuntimeConfig{
		FuncMountConfig: types.FuncMountConfig{
			FuncMountUser: types.FuncMountUser{
				UserID:  2000,
				GroupID: 2000,
			},
		},
		RMConfig: rmconfig.Config{
			ServerPort: 1234,
			RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
				"a": {Enable: false},
				"b": {Enable: true},
			},
		},
	}}
	r := &fusionRuntime{
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		handlers:     runtime.Handlers{},
		userCodePath: cfg.DeploymentConfig.DeployDir,
		rt:           &fakeRuntime{},
	}

	go func() {
		c <- time.Now()
		c <- time.Now()
		c <- time.Now()
		c <- time.Now()
		close(finish)
	}()

	r.heartbeatCheck(1, 1, types.Heartbeat{}, "java8")
	r.heartbeatCheck(1, 1, types.Heartbeat{}, "http")
}

func TestIsReportThroughM2S(t *testing.T) {
	convey.Convey("isReportThroughM2S return true", t, func() {
		customMetricsReport := types.CustomMetricsReport{
			Handler:     reportCustomMetricsThroughM2SHandlerName,
			Path:        "/actuator/prometheus",
			CollectTime: 15,
			Timeout:     10,
		}
		result := isReportThroughM2S(customMetricsReport)
		convey.ShouldBeTrue(result)
	})
	convey.Convey("isReportThroughM2S return false", t, func() {
		customMetricsReport := types.CustomMetricsReport{
			Handler:     "com.huawei.metrics.reportCustomMetricsHandler.reportCustomMetrics",
			Path:        "",
			CollectTime: 15,
			Timeout:     10,
		}
		result := isReportThroughM2S(customMetricsReport)
		convey.ShouldBeFalse(result)
	})
}

func TestObtainsCustomReportMetrics(t *testing.T) {
	p := gomonkey.NewPatches()

	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp",
		func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
			return nil
		})

	finish := make(chan struct{})
	p.ApplyGlobalVar(&runtime.ProcessExitSignal, finish)

	var fr *fakeRuntime
	p.ApplyMethodSeq(reflect.TypeOf(fr), "ReportCustomMetrics", []gomonkey.OutputCell{
		{Values: gomonkey.Params{[]byte("test"), nil}},
		{Values: gomonkey.Params{nil, nil}},
		{Values: gomonkey.Params{[]byte(""), nil}},
		{Values: gomonkey.Params{nil, errors.New("my error")}},
	})

	c := make(chan time.Time)
	timer := &time.Timer{C: c}
	p.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
		return timer
	})
	p.ApplyMethod(reflect.TypeOf(timer), "Reset", func(_ *time.Timer, _ time.Duration) bool { return true })
	p.ApplyMethod(reflect.TypeOf(timer), "Stop", func(_ *time.Timer) bool { return true })
	defer p.Reset()

	go func() {
		c <- time.Now()
		c <- time.Now()
		c <- time.Now()
		c <- time.Now()
		close(finish)
	}()

	customMetricsReport := types.CustomMetricsReport{
		Handler:     "com.huawei.metrics.reportCustomMetricsHandler.reportCustomMetrics",
		CollectTime: 15,
		Timeout:     10,
	}

	cfg := &config.Configuration{RuntimeConfig: config.RuntimeConfig{
		FuncMountConfig: types.FuncMountConfig{
			FuncMountUser: types.FuncMountUser{
				UserID:  2000,
				GroupID: 2000,
			},
		},
		RMConfig: rmconfig.Config{
			ServerPort: 1234,
			RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
				"a": {Enable: false},
				"b": {Enable: true},
			},
		},
	}}
	r := &fusionRuntime{
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		handlers:     runtime.Handlers{},
		userCodePath: cfg.DeploymentConfig.DeployDir,
		rt:           &fakeRuntime{},
	}

	p.ApplyFunc(isRuntimeProcessStatusRunningOrRecover, func() bool {
		return true
	})

	r.obtainsCustomReportMetrics(customMetricsReport)
}

func TestResetDNSConfig(t *testing.T) {
	convey.Convey("Test_runtime.RuntimeManagerClientNotNil", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
				return []byte{}, nil
			}),
			gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage, func(r *tcpmsg.SocketConn,
				msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{}
			}),
			gomonkey.ApplyFunc(commonUtils.ResetDNSConfig, func(dnsConfig types.DNSConfig) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		DNS := []string{"8.8.8.8", "8.8.8.1", "8.8.8.2"}
		DNSOptions := []string{"timeout:2", "attempts:3"}
		DNSSearch := []string{"example.com", "huawei.example.com"}
		DNSConfig := types.DNSConfig{DNS: DNS, DNSOptions: DNSOptions, DNSSearch: DNSSearch}

		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				DNSConfig: DNSConfig,
			},
		}
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}

		err := resetDNSConfig(conf, false)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test_runtime.RuntimeManagerClientNil", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
				return []byte{}, nil
			}),
			gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage, func(r *tcpmsg.SocketConn,
				msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{}
			}),
			gomonkey.ApplyFunc(commonUtils.ResetDNSConfig, func(dnsConfig types.DNSConfig) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		DNS := []string{"8.8.8.8", "8.8.8.1", "8.8.8.2"}
		DNSOptions := []string{"timeout:2", "attempts:3"}
		DNSSearch := []string{"example.com", "huawei.example.com"}
		DNSConfig := types.DNSConfig{DNS: DNS, DNSOptions: DNSOptions, DNSSearch: DNSSearch}
		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				DNSConfig: DNSConfig,
			},
		}
		runtime.RuntimeManagerClient = nil

		err := resetDNSConfig(conf, true)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test DNS with checkpoint mode failed with SaveDNSConfigBeforeReset", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(commonUtils.SaveDNSConfigBeforeReset, func() ([]byte, error) {
				return nil, errors.New("save failed")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		DNS := []string{"8.8.8.8", "8.8.8.1", "8.8.8.2"}
		DNSOptions := []string{"timeout:2", "attempts:3"}
		DNSSearch := []string{"example.com", "huawei.example.com"}
		DNSConfig := types.DNSConfig{DNS: DNS, DNSOptions: DNSOptions, DNSSearch: DNSSearch}

		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				DNSConfig:   DNSConfig,
				RunningMode: constants.RunningModeCheckpoint,
			},
		}

		err := resetDNSConfig(conf, false)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("Test DNS with checkpoint mode failed with dns is empty", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(commonUtils.SaveDNSConfigBeforeReset, func() ([]byte, error) {
				return nil, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		DNS := []string{"8.8.8.8", "8.8.8.1", "8.8.8.2"}
		DNSOptions := []string{"timeout:2", "attempts:3"}
		DNSSearch := []string{"example.com", "huawei.example.com"}
		DNSConfig := types.DNSConfig{DNS: DNS, DNSOptions: DNSOptions, DNSSearch: DNSSearch}

		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				DNSConfig:   DNSConfig,
				RunningMode: constants.RunningModeCheckpoint,
			},
		}

		err := resetDNSConfig(conf, false)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("Test DNS with checkpoint mode success", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
				return []byte{}, nil
			}),
			gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage, func(r *tcpmsg.SocketConn,
				msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
				return tcpmsg.RuntimeManagerRSP{}
			}),
			gomonkey.ApplyFunc(commonUtils.ResetDNSConfig, func(dnsConfig types.DNSConfig) error {
				return nil
			}),
			gomonkey.ApplyFunc(commonUtils.SaveDNSConfigBeforeReset, func() ([]byte, error) {
				return []byte("nameserver 127.0.0.1"), nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
			utils.PodOriginDNSConf = nil
		}()

		DNS := []string{"8.8.8.8", "8.8.8.1", "8.8.8.2"}
		DNSOptions := []string{"timeout:2", "attempts:3"}
		DNSSearch := []string{"example.com", "huawei.example.com"}
		DNSConfig := types.DNSConfig{DNS: DNS, DNSOptions: DNSOptions, DNSSearch: DNSSearch}

		conf := &config.Configuration{
			RuntimeConfig: config.RuntimeConfig{
				DNSConfig:   DNSConfig,
				RunningMode: constants.RunningModeCheckpoint,
			},
		}
		runtime.RuntimeManagerClient = &tcpmsg.SocketConn{}

		err := resetDNSConfig(conf, false)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestCancelEvent(t *testing.T) {
	r := &fusionRuntime{
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
	}
	p := gomonkey.NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		return nil
	})
	defer p.Reset()

	invokeID := "123"
	resp := r.ProcessCancelEvent(&runtime.CancelRequest{InvokeID: invokeID})
	assert.Equal(t, invokeID, resp.InvokeID)
}

type fakeFile struct{}

func (fakeFile) Name() string       { return "" }
func (fakeFile) Size() int64        { return 0 }
func (fakeFile) Mode() fs.FileMode  { return 0755 }
func (fakeFile) ModTime() time.Time { return time.Time{} }
func (fakeFile) IsDir() bool        { return false }
func (fakeFile) Sys() interface{}   { return nil }

func Test_monitorDiskUsage_001(t *testing.T) {
	var send bool
	r := &fusionRuntime{
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
	}
	p := gomonkey.NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		send = true
		return nil
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	p.ApplyFunc(diskusage.AddProcessHooks, func(hooks ...diskusage.ProcessHook) diskusage.Opt {
		for _, hook := range hooks {
			hook(fsnotify.Event{Name: "a.txt", Op: fsnotify.Create})
		}
		return func(m *diskusage.Monitor) error { return nil }
	})

	monitor := &diskusage.Monitor{}
	p.ApplyMethod(reflect.TypeOf(monitor), "Size", func(_ *diskusage.Monitor) int64 { return 1 << 30 })
	p.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return []fs.FileInfo{fakeFile{}}, nil
	})
	p.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	finish := make(chan struct{})
	p.ApplyGlobalVar(&runtime.ProcessExitSignal, finish)

	defer p.Reset()

	err := r.monitorDiskUsage()
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	close(finish)

	assert.Equal(t, true, send)
}

func Test_monitorDiskUsage_002(t *testing.T) {
	var send bool
	r := &fusionRuntime{
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
	}
	p := gomonkey.NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		send = true
		return nil
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	p.ApplyFunc(diskusage.AddProcessHooks, func(hooks ...diskusage.ProcessHook) diskusage.Opt {
		for _, hook := range hooks {
			hook(fsnotify.Event{Name: "dir", Op: fsnotify.Create})
		}
		return func(m *diskusage.Monitor) error { return nil }
	})

	monitor := &diskusage.Monitor{}
	p.ApplyMethod(reflect.TypeOf(monitor), "FirstInode", func(_ *diskusage.Monitor) int64 { return 1 })
	p.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return []fs.FileInfo{fakeFile{}}, nil
	})
	p.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	finish := make(chan struct{})
	p.ApplyGlobalVar(&runtime.ProcessExitSignal, finish)

	defer p.Reset()

	err := r.monitorDiskUsage()
	assert.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	close(finish)

	assert.Equal(t, true, send)
}

func TestProcessEvent(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	expectedEvent := []byte("test-process-event")
	r := &fusionRuntime{
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		rt: &fakeRuntime{handlers: runtime.Handlers{
			RecvProcessEventHandler: func(ic invokectx.InvokeCtx, resp runtime.InvokeResponse) {
				assert.Equal(t, expectedEvent, resp.Result)
				wg.Done()
			},
		}},
	}

	conf := &config.Configuration{}
	runtime.ExtensionMgr = runtime.GetExtensionManager(conf)
	r.SendProcessEvent(invokectx.InvokeCtx{}, runtime.InvokeRequest{Event: expectedEvent}, conf)
	wg.Wait()
}

func TestNewFusionRuntime(t *testing.T) {
	fn := func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
		return &fakeRuntime{}, nil
	}

	conf := &config.Configuration{
		StartArgs: config.StartArgs{},
		RuntimeConfig: config.RuntimeConfig{
			FunctionType:       "",
			EnableAuthInHeader: false,
			Type:               "",
			URN:                "",
			FuncLogLevel:       "",
			Timeout:            0,
			LoadTime:           0,
			EnvInfo:            "",
			EncryptedUserData:  "",
			EntryFile:          "",
			HandlerName:        "",
			RPCPort:            0,
			ConnectType:        "",
			Language:           "",
			CodeSha256:         "",
			CodeSha512:         "",
			ResourceMetaData:   config.ResourceMetaData{},
			Initializer:        config.Initializer{},
			LogTankService:     logservice.LogTankService{},
			TraceService:       trace.TraceService{},
			RMConfig:           rmconfig.Config{},
			FuncMountConfig:    types.FuncMountConfig{},
			RuntimeLogDir:      "",
			RuntimeContainerID: "",
			MaxStateSize:       "",
			Heartbeat: types.Heartbeat{
				Handler: "handler",
			},
		},
		DeploymentConfig: config.DeploymentConfig{
			DeployDir: "/runtime/tmp",
		},
		HeartbeatConfig: config.HeartbeatConfig{
			FailureThreshold:   0,
			CheckPeriodSeconds: 0,
		},
	}

	_, err := newFusionRuntime(conf, runtime.Handlers{}, fn)
	require.Nil(t, err)
}

func TestNewFusionRuntime2(t *testing.T) {
	fakeRt := &fakeRuntime{}
	fn := func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
		return fakeRt, nil
	}

	conf := &config.Configuration{
		StartArgs: config.StartArgs{},
		RuntimeConfig: config.RuntimeConfig{
			FunctionType:       "",
			EnableAuthInHeader: false,
			Type:               "",
			URN:                "",
			FuncLogLevel:       "",
			Timeout:            0,
			LoadTime:           0,
			EnvInfo:            "",
			EncryptedUserData:  "",
			EntryFile:          "",
			HandlerName:        "",
			RPCPort:            0,
			ConnectType:        "",
			Language:           "",
			CodeSha256:         "",
			CodeSha512:         "",
			ResourceMetaData:   config.ResourceMetaData{},
			Initializer:        config.Initializer{},
			LogTankService:     logservice.LogTankService{},
			TraceService:       trace.TraceService{},
			RMConfig:           rmconfig.Config{},
			FuncMountConfig:    types.FuncMountConfig{},
			RuntimeLogDir:      "",
			RuntimeContainerID: "",
			MaxStateSize:       "",
			Heartbeat: types.Heartbeat{
				Handler: "handler",
			},
		},
		DeploymentConfig: config.DeploymentConfig{
			DeployDir: "",
		},
		HeartbeatConfig: config.HeartbeatConfig{
			FailureThreshold:   0,
			CheckPeriodSeconds: 0,
		},
	}
	p := gomonkey.NewPatches()
	p.ApplyMethod(reflect.TypeOf(fakeRt), "HotLoad", func(_ *fakeRuntime, ctx context.Context, c *config.Configuration) error { return nil })
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		return nil
	})
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	p.ApplyFunc(diskusage.AddProcessHooks, func(hooks ...diskusage.ProcessHook) diskusage.Opt {
		for _, hook := range hooks {
			hook(fsnotify.Event{Name: "a.txt", Op: fsnotify.Create})
		}
		return func(m *diskusage.Monitor) error { return nil }
	})

	monitor := &diskusage.Monitor{}
	p.ApplyMethod(reflect.TypeOf(monitor), "Size", func(_ *diskusage.Monitor) int64 { return 1 << 30 })
	p.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return []fs.FileInfo{fakeFile{}}, nil
	})
	p.ApplyFunc(os.RemoveAll, func(path string) error { return nil })
	finish := make(chan struct{})
	p.ApplyGlobalVar(&runtime.ProcessExitSignal, finish)

	defer p.Reset()

	rt, err2 := newFusionRuntime(conf, runtime.Handlers{}, fn)
	require.Nil(t, err2)

	state := status.Get()
	status.Set(status.Initializing)
	err3 := rt.HotLoad(context.Background(), conf)
	status.Set(state)
	require.Nil(t, err3)

	time.Sleep(50 * time.Millisecond)
	close(finish)
	// wait go routine finished to execute
	time.Sleep(1 * time.Second)
}

func TestReconnect(t *testing.T) {
	p := gomonkey.NewPatches()
	fakeCreator := &fakeCreator{}
	runtime.Registry.Register("", fakeCreator)
	var queue *cache.MessageQueue
	p.ApplyMethod(reflect.TypeOf(queue), "WaitForBacklog", func(*cache.MessageQueue) (uint32, bool) { return 1, true })
	rt := &fakeRuntime{}
	hotLoadErr := snerror.New(snerror.DiskUsageExceed, snerror.ErrText(snerror.DiskUsageExceed))
	p.ApplyMethodSeq(reflect.TypeOf(rt), "HotLoad", []gomonkey.OutputCell{
		{Values: gomonkey.Params{hotLoadErr}},
	})
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		// reconnect should send kill command at the end
		assert.Equal(t, tcpmsg.KillRuntimeProcess, msg.Header.MsgType)
		return nil
	})
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	defer p.Reset()

	r := &fusionRuntime{
		newRuntimeFn: func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			return &fakeRuntime{}, nil
		},
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		cancel:       func() {},
	}

	cache.GetMessageQueue().Block()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := cache.GetMessageQueue().WaitUnblock(1, true)
		assert.Equal(t, hotLoadErr, err)
		wg.Done()
	}()

	time.Sleep(500 * time.Millisecond)
	conf := &config.Configuration{}
	conf.RunningMode = constants.RunningModeRestore
	r.Reconnect(conf)
	assert.Equal(t, conf.RunningMode, constants.RunningModeNormal)
	wg.Wait()
}

func TestReconnect_scaleDown(t *testing.T) {
	p := gomonkey.NewPatches()
	creator := &fakePrestartableCreator{}
	runtime.Registry.Register("", creator)
	count := 0
	p.ApplyMethod(reflect.TypeOf(creator), "Prestart", func(_ *fakePrestartableCreator, c *config.RuntimeConfig) error {
		// will not enter this function
		count = -1
		return errors.New("pre start error")
	})
	var queue *cache.MessageQueue
	p.ApplyMethod(reflect.TypeOf(queue), "UnblockAndFlush", func(_ *cache.MessageQueue, err error) {
		// will enter this function
		count += 1
	})
	p.ApplyFunc(runtime.CloseExitChannel, func() {
		// will enter this function
		count += 1
	})

	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	p.ApplyFunc(status.GetExitState, func() uint32 {
		return 1
	})
	defer p.Reset()

	r := &fusionRuntime{
		newRuntimeFn: func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			return &fakeRuntime{}, nil
		},
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		cancel:       func() {},
	}

	r.Reconnect(&config.Configuration{})
	assert.Equal(t, 2, count)
}

func TestReconnect_HotLoadCrash(t *testing.T) {
	p := gomonkey.NewPatches()
	creator := &fakeCreator{}
	runtime.Registry.Register("", creator)
	var queue *cache.MessageQueue
	first := true
	wait := true
	p.ApplyMethod(reflect.TypeOf(queue), "WaitForBacklog", func(*cache.MessageQueue) (uint32, bool) {
		if wait {
			wait = false
			time.Sleep(1 * time.Second)
		}
		return 1, true
	})
	expectedErr := snerror.New(snerror.DiskUsageExceed, snerror.ErrText(snerror.DiskUsageExceed))
	p.ApplyMethod(reflect.TypeOf(queue), "UnblockAndFlush", func(_ *cache.MessageQueue, err error) {
		if first {
			assert.Equal(t, expectedErr, err)
			first = false // prevent crashing infinitely
			return
		}
		assert.Nil(t, err)
	})
	rt := &fakeRuntime{}
	p.ApplyMethod(reflect.TypeOf(rt), "HotLoad", func(_ *fakeRuntime, ctx context.Context, conf *config.Configuration) error {
		if first {
			<-ctx.Done()
			err := rt.handlers.GetExceptionHandler()
			assert.Equal(t, int32(snerror.DiskUsageExceed), err.Code)
			return expectedErr
		}
		return nil
	})

	restartCh := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		switch msg.Header.MsgType {
		case tcpmsg.GotRuntimeException:
			return tcpmsg.RuntimeManagerRSP{
				Code: expectedErr.Code(),
			}
		case tcpmsg.RestartRuntimeProcess:
			// mock runtime process crashes
			if first {
				close(restartCh)
			}
		default:
			t.Fatal("unexpected msg type", msg.Header.MsgType)
		}
		return tcpmsg.RuntimeManagerRSP{}
	})
	defer p.Reset()

	r := &fusionRuntime{
		newRuntimeFn: func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			assert.NotNil(t, handlers.GetExceptionHandler)
			assert.NotNil(t, handlers.CheckRuntimeStatusHandler)
			rt.handlers = handlers
			return rt, nil
		},
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		cancel:       func() {},
	}
	r.handlers.GetExceptionHandler = r.eh.getException
	r.handlers.CheckRuntimeStatusHandler = r.eh.checkException

	go func() {
		<-restartCh
		r.Reconnect(&config.Configuration{})
		wg.Done()
	}()

	r.Reconnect(&config.Configuration{})
	wg.Wait()
}

func TestReconnect_ProcessEventCrash(t *testing.T) {
	p := gomonkey.NewPatches()
	creator := &fakeCreator{}
	runtime.Registry.Register("", creator)
	var queue *cache.MessageQueue
	p.ApplyMethod(reflect.TypeOf(queue), "WaitForBacklog", func(*cache.MessageQueue) (uint32, bool) { return 1, true })

	p.ApplyMethod(reflect.TypeOf(queue), "UnblockAndFlush", func(_ *cache.MessageQueue, err error) {
		assert.Nil(t, err)
	})

	rt := &fakeRuntime{}
	expectedErrCode := snerror.DiskUsageExceed
	p.ApplyMethod(reflect.TypeOf(rt), "SendProcessEvent", func(_ *fakeRuntime, ic invokectx.InvokeCtx,
		message runtime.InvokeRequest, conf *config.Configuration) {
		go func() {
			time.Sleep(1 * time.Second)
			err := rt.handlers.GetExceptionHandler()
			assert.Equal(t, int32(expectedErrCode), err.Code)
			rt.handlers.RecvProcessEventHandler(ic, runtime.InvokeResponse{})
		}()
	})
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		switch msg.Header.MsgType {
		case tcpmsg.GotRuntimeException:
			return tcpmsg.RuntimeManagerRSP{
				Code: expectedErrCode,
			}
		case tcpmsg.RestartRuntimeProcess:
		default:
			t.Fatal("unexpected msg type", msg.Header.MsgType)
		}
		return tcpmsg.RuntimeManagerRSP{}
	})

	r := &fusionRuntime{
		newRuntimeFn: func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			assert.NotNil(t, handlers.GetExceptionHandler)
			assert.NotNil(t, handlers.CheckRuntimeStatusHandler)
			rt.handlers = handlers
			return rt, nil
		},
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		cancel:       func() {},
	}
	var wg sync.WaitGroup
	wg.Add(2)
	r.handlers.RecvProcessEventHandler = r.decorateRecvProcessEventHandler(func(ic invokectx.InvokeCtx, ir runtime.InvokeResponse) {
		wg.Done()
	}, nil, func(ic invokectx.InvokeCtx, conf *config.Configuration, code int32) bool {
		return false
	})
	r.handlers.GetExceptionHandler = r.eh.getException
	r.handlers.CheckRuntimeStatusHandler = r.eh.checkException
	r.rt, _ = r.newRuntimeFn(&config.Configuration{}, r.handlers)

	conf := &config.Configuration{}
	runtime.ExtensionMgr = runtime.GetExtensionManager(conf)
	r.SendProcessEvent(invokectx.InvokeCtx{}, runtime.InvokeRequest{}, conf)
	r.SendProcessEvent(invokectx.InvokeCtx{}, runtime.InvokeRequest{}, conf)

	time.Sleep(500 * time.Millisecond)
	r.Reconnect(&config.Configuration{})
	wg.Wait()
}

func TestReconnect_HotloadTimeout(t *testing.T) {
	p := gomonkey.NewPatches()
	creator := &fakeCreator{}
	runtime.Registry.Register("", creator)
	runtime.NewRuntimeState(true)
	var queue *cache.MessageQueue
	p.ApplyMethod(reflect.TypeOf(queue), "WaitForBacklog", func(*cache.MessageQueue) (uint32, bool) { return 1, true })

	killCh := make(chan struct{})
	first := true
	expectedErr := snerror.New(snerror.RuntimeInitTimeoutCode, snerror.ErrText(snerror.RuntimeInitTimeoutCode))

	p.ApplyMethod(reflect.TypeOf(queue), "UnblockAndFlush", func(_ *cache.MessageQueue, err error) {
		if first {
			assert.Equal(t, expectedErr, err)
			return
		}
		assert.Nil(t, err)
	})

	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		if first {
			// reconnect should send kill command at the end
			assert.Equal(t, tcpmsg.KillRuntimeProcess, msg.Header.MsgType)
			close(killCh)
			first = false
			return nil
		}
		return nil
	})

	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		switch msg.Header.MsgType {
		case tcpmsg.GotRuntimeException:
			t.Fatal("got runtime exception should not be invoked")
		case tcpmsg.RestartRuntimeProcess:
		default:
			t.Fatal("unexpected msg type", msg.Header.MsgType)
		}
		return tcpmsg.RuntimeManagerRSP{}
	})

	rt := &fakeRuntime{}
	p.ApplyMethod(reflect.TypeOf(rt), "HotLoad", func(_ *fakeRuntime, ctx context.Context, conf *config.Configuration) error {
		if first {
			return expectedErr
		}
		return nil
	})
	r := &fusionRuntime{
		newRuntimeFn: func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			assert.NotNil(t, handlers.GetExceptionHandler)
			assert.NotNil(t, handlers.CheckRuntimeStatusHandler)
			rt.handlers = handlers
			return rt, nil
		},
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		cancel:       func() {},
	}
	r.handlers.GetExceptionHandler = r.eh.getException
	r.handlers.CheckRuntimeStatusHandler = r.eh.checkException
	r.rt, _ = r.newRuntimeFn(&config.Configuration{}, r.handlers)
	r.eh.setException(&runtime.Error{})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		<-killCh
		r.Reconnect(&config.Configuration{})
		wg.Done()
	}()

	r.Reconnect(&config.Configuration{})

	wg.Wait()
}

func TestGetRestoreDead(t *testing.T) {
	assert.Equal(t, false, GetRestoreDead())
}

func TestReconnect_PrestartTimeout(t *testing.T) {
	p := gomonkey.NewPatches()
	creator := &fakePrestartableCreator{}
	runtime.Registry.Register("", creator)
	p.ApplyMethod(reflect.TypeOf(creator), "Prestart", func(_ *fakePrestartableCreator, c *config.RuntimeConfig) error {
		time.Sleep(1 * time.Second)
		return errors.New("pre start error")
	})
	var queue *cache.MessageQueue
	p.ApplyMethod(reflect.TypeOf(queue), "UnblockAndFlush", func(_ *cache.MessageQueue, err error) {})
	p.ApplyFunc(runtime.CloseExitChannel, func() {})

	c := make(chan time.Time)
	timer := &time.Timer{C: c}
	p.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
		return timer
	})
	p.ApplyMethod(reflect.TypeOf(timer), "Reset", func(_ *time.Timer, _ time.Duration) bool { return true })
	p.ApplyMethod(reflect.TypeOf(timer), "Stop", func(_ *time.Timer) bool { return true })
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	defer p.Reset()

	r := &fusionRuntime{
		newRuntimeFn: func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			return &fakeRuntime{}, nil
		},
		crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
		eh:           newExceptionHandler(),
		cancel:       func() {},
	}
	go func() {
		close(c)
	}()
	r.Reconnect(&config.Configuration{})
}

func TestHotLoad(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		return nil
	})
	rt := &fakeRuntime{}
	p.ApplyMethod(reflect.TypeOf(rt), "HotLoad", func(_ *fakeRuntime, ctx context.Context, conf *config.Configuration) error {
		return snerror.New(snerror.DiskUsageExceed, snerror.ErrText(snerror.DiskUsageExceed))
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	defer p.Reset()

	r, err := newFusionRuntime(&config.Configuration{}, runtime.Handlers{},
		func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			return rt, nil
		})
	assert.Nil(t, err)
	err = r.HotLoad(context.Background(), &config.Configuration{})
	assert.Nil(t, err)
	assert.NotNil(t, runtime.ExtensionMgr)
	err = r.HotLoad(context.Background(), &config.Configuration{RuntimeConfig: config.RuntimeConfig{RunningMode: constants.RunningModeCheckpoint}})
	assert.NotNil(t, err)
	assert.NotNil(t, runtime.ExtensionMgr)
}

func TestHotLoadWithSnErr(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		return nil
	})
	rt := &fakeRuntime{}
	var cacheErr error
	mq := cache.GetMessageQueue()
	p.ApplyMethod(reflect.TypeOf(rt), "HotLoad", func(_ *fakeRuntime, ctx context.Context, conf *config.Configuration) error {
		return snerror.New(snerror.DiskUsageExceed, snerror.ErrText(snerror.DiskUsageExceed))
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	p.ApplyMethod(reflect.TypeOf(mq), "Block", func(queue *cache.MessageQueue) {
		return
	})
	p.ApplyMethod(reflect.TypeOf(mq), "Error", func(queue *cache.MessageQueue, err error) {
		cacheErr = err
		return
	})
	defer p.Reset()

	r, err := newFusionRuntime(&config.Configuration{}, runtime.Handlers{},
		func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			return rt, nil
		})
	assert.Nil(t, err)
	err = r.HotLoad(context.Background(), &config.Configuration{})
	assert.Nil(t, err)
	assert.NotNil(t, cacheErr)
	snErr, ok := cacheErr.(snerror.SNError)
	if !ok {
		t.Errorf("this should be ok")
	}
	if snErr.Code() != snerror.DiskUsageExceed || !strings.Contains(snErr.Error(), snerror.ErrText(snerror.DiskUsageExceed)) {
		t.Errorf("thi is error, errCode: %v, errMsg: %s", snErr.Code(), snErr.Error())
	}
	err = r.HotLoad(context.Background(), &config.Configuration{RuntimeConfig: config.RuntimeConfig{RunningMode: constants.RunningModeCheckpoint}})
	assert.NotNil(t, err)
}

func TestLoad_InvalidUser(t *testing.T) {
	rt := &fakeRuntime{}
	r, err := newFusionRuntime(&config.Configuration{}, runtime.Handlers{},
		func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			return rt, nil
		})
	assert.Nil(t, err)
	err = r.load(context.Background(), &config.Configuration{RuntimeConfig: config.RuntimeConfig{
		FuncMountConfig: types.FuncMountConfig{
			FuncMountUser: types.FuncMountUser{
				UserID:  constants.DefaultWorkerUID,
				GroupID: 2000,
			},
		},
	}})
	assert.NotNil(t, err)
	assert.Equal(t, "mountUser uid or gid cannot be system user", err.Error())
}

func TestLoad_initRuntimeProcess(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{Code: 1, Err: "abc"}
	})
	defer p.Reset()

	rt := &fakeRuntime{}
	r, err := newFusionRuntime(&config.Configuration{}, runtime.Handlers{},
		func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
			return rt, nil
		})
	assert.Nil(t, err)
	err = r.load(context.Background(), &config.Configuration{RuntimeConfig: config.RuntimeConfig{
		FuncMountConfig: types.FuncMountConfig{
			FuncMountUser: types.FuncMountUser{
				UserID:  2000,
				GroupID: 2000,
			},
		},
		LogPolicy: types.LogPolicy{
			LogPaths: []string{"/a/xx"},
		},
		UsePoolPod: true,
	}})
	assert.NotNil(t, err)
	_, ok := err.(snerror.SNError)
	assert.True(t, ok)
	val := os.Getenv(constants.EnableLogPath)
	assert.True(t, val == "true")
}

func Test_fusionRuntime_HotLoad(t *testing.T) {
	fakeFn := func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
		return &fusionRuntime{}, nil
	}
	type fields struct {
		eh                   *exceptionHandler
		crashLimiter         *limiter
		newRuntimeFn         newRuntimeFn
		handlers             runtime.Handlers
		userCodePath         string
		rt                   runtime.Runtime
		cancel               context.CancelFunc
		eventCnt             int64
		isRuntimeProcessDead int32
	}
	type args struct {
		ctx  context.Context
		conf *config.Configuration
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 runtime exit quickly, r.cancel not nil", fields{newRuntimeFn: fakeFn},
			args{context.TODO(), &config.Configuration{
				RuntimeConfig: config.RuntimeConfig{FuncMountConfig: types.FuncMountConfig{}}}},
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(diskusage.New,
						func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
							return nil, errors.New("failed to diskusage New")
						}),
					ApplyFunc((*tcpmsg.SocketConn).SendMessage,
						func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
							return tcpmsg.RuntimeManagerRSP{
								Err:      "",
								Code:     0,
								Response: tcpmsg.RuntimeRSP{},
							}
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			r := &fusionRuntime{
				eh:                   tt.fields.eh,
				crashLimiter:         tt.fields.crashLimiter,
				newRuntimeFn:         tt.fields.newRuntimeFn,
				handlers:             tt.fields.handlers,
				userCodePath:         tt.fields.userCodePath,
				rt:                   tt.fields.rt,
				cancel:               tt.fields.cancel,
				eventCnt:             tt.fields.eventCnt,
				isRuntimeProcessDead: tt.fields.isRuntimeProcessDead,
			}
			if err := r.HotLoad(tt.args.ctx, tt.args.conf); (err != nil) != tt.wantErr {
				t.Errorf("HotLoad() error = %v, wantErr %v", err, tt.wantErr)
			}
			if r.cancel == nil || r.rt == nil {
				t.Errorf("fusionRuntime cancel: %v,or rt: %v, is nil", r.cancel, r.rt)
			}
			patches.ResetAll()
		})
	}
}

func TestPrestartRuntime(t *testing.T) {
	p := gomonkey.NewPatches()
	creator := &fakePrestartableCreator{}
	runtime.Registry.Register("", creator)
	p.ApplyMethod(reflect.TypeOf(creator), "Prestart", func(_ *fakePrestartableCreator, c *config.RuntimeConfig) error {
		select {}
	})
	defer p.Reset()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := &fusionRuntime{}
	succeed := r.prestartRuntime(ctx, &config.Configuration{})
	assert.False(t, succeed)
}

func TestProcessCSharpNewRuntimeError(t *testing.T) {
	r := &fusionRuntime{handlers: runtime.Handlers{}}
	r.handlers.GetExceptionHandler = mockGetExceptionHandler
	err := r.processCSharpNewRuntimeError(errors.New("test error"))
	assert.Nil(t, err)
}

func mockGetExceptionHandler() *runtime.Error {
	return &runtime.Error{Code: snerror.RuntimeProcessExit}
}

func TestFusionRuntime_monitorTmpDiskUsage(t *testing.T) {
	Convey("test fusion runtime monitor tmp disk usage", t, func() {
		Convey("case 1: failed to test fusion runtime monitor tmp disk usage", func() {
			patch := ApplyFunc(diskusage.New,
				func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
					return nil, errors.New("failed to diskusage New")
				})
			defer patch.Reset()
			r := &fusionRuntime{}
			err := r.monitorTmpDiskUsage()
			So(err, ShouldNotBeNil)
		})

		Convey("case 2: failed to test fusion runtime monitor tmp disk usage", func() {
			patch := ApplyFunc(diskusage.New,
				func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
					if rootDir == userDataPath {
						return &diskusage.Monitor{}, nil
					}
					return nil, errors.New("test")
				})
			defer patch.Reset()
			r := &fusionRuntime{}
			err := r.monitorTmpDiskUsage()
			So(err, ShouldNotBeNil)
		})
	})
}

func TestSubscribeWorkerErr(t *testing.T) {
	Convey("test subscribe worker error successfully", t, func() {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(diskusage.New,
			func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
				return nil, errors.New("failed to diskusage New")
			})
		patch.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
			return nil
		})
		patch.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
			return tcpmsg.RuntimeManagerRSP{}
		})
		defer patch.Reset()
		rt := &fakeRuntime{}
		r := &fusionRuntime{
			newRuntimeFn: func(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
				assert.NotNil(t, handlers.GetExceptionHandler)
				assert.NotNil(t, handlers.CheckRuntimeStatusHandler)
				rt.handlers = handlers
				return rt, nil
			},
			crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
			eh:           newExceptionHandler(),
			cancel:       func() {},
		}
		SendWorkerErr(runtime.Error{
			Code:    snerror.WorkerPingVpcGatewayError,
			Message: snerror.ErrText(snerror.WorkerPingVpcGatewayError),
		})
		runtime.ProcessExitSignal = make(chan struct{})
		r.subscribeWorkerErr()

		ShouldEqual(runtime.ProcessExitError, snerror.New(snerror.WorkerPingVpcGatewayError,
			snerror.ErrText(snerror.WorkerPingVpcGatewayError)))
	})
}

func TestKillRuntime(t *testing.T) {
	// 创建一个fusionRuntime实例
	r := &fusionRuntime{}
	t.Run("runtimeErr is nil", func(t *testing.T) {
		r.killRuntime(nil)
	})
	t.Run("runtime is dead", func(t *testing.T) {
		atomic.StoreInt32(&r.isRuntimeProcessDead, 1)
		defer func() { atomic.StoreInt32(&r.isRuntimeProcessDead, 0) }()
		r.killRuntime(&runtime.Error{Code: 1})
	})
	t.Run("runtime is dead", func(t *testing.T) {
		block := false
		r.eh = newExceptionHandler()
		patch := gomonkey.ApplyFunc((*cache.MessageQueue).Block,
			func(_ *cache.MessageQueue) {
				block = true
			})
		patch.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp",
			func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
				return nil
			})
		defer patch.Reset()
		r.killRuntime(&runtime.Error{Code: 1})
		assert.True(t, block)
	})
}

func TestAliveHeartbeatCheck(t *testing.T) {
	err := AliveHeartbeatCheck(nil, types.Heartbeat{})
	assert.True(t, strings.Contains(err.Error(), "alive heartbeat check failed for fusionRuntime assert failed"))
}

func TestStartupHeartbeatCheck(t *testing.T) {
	Convey("TestStartupHeartbeatCheck error happened", t, func() {
		runtime.ProcessExitSignal = make(chan struct{})
		bootHealthCheckChan = make(chan bool, 1000)
		fusionRtm := &fusionRuntime{
			rt: &rpc.AbstractRPC{},
		}
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(fusionRtm), "StartupHealthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return false, errors.New("error happened")
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		go func() {
			if ok := WaitStartHealthCheckOK(); !ok {
				close(runtime.ProcessExitSignal)
			}
		}()
		fusionRtm.startupHeartbeatCheck(types.Heartbeat{
			PeriodSeconds: 1,
		})
	})
	Convey("TestStartupHeartbeatCheck failed reach threshold", t, func() {
		runtime.ProcessExitSignal = make(chan struct{})
		bootHealthCheckChan = make(chan bool, 1000)
		fusionRtm := &fusionRuntime{
			rt: &rpc.AbstractRPC{},
		}
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(fusionRtm), "StartupHealthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return false, nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		go func() {
			if ok := WaitStartHealthCheckOK(); !ok {
				close(runtime.ProcessExitSignal)
			}
		}()
		fusionRtm.startupHeartbeatCheck(types.Heartbeat{
			PeriodSeconds:    1,
			SuccessThreshold: 1,
		})
	})
	Convey("TestStartupHeartbeatCheck success reach threshold", t, func() {
		bootHealthCheckChan = make(chan bool, 1000)
		runtime.ProcessExitSignal = make(chan struct{})
		fusionRtm := &fusionRuntime{
			rt: &rpc.AbstractRPC{},
		}
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(fusionRtm), "StartupHealthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return true, nil
				}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		go func() {
			if ok := WaitStartHealthCheckOK(); !ok {
				close(runtime.ProcessExitSignal)
			}
		}()
		fusionRtm.startupHeartbeatCheck(types.Heartbeat{
			PeriodSeconds:    1,
			FailureThreshold: 1,
		})
	})
	runtime.ProcessExitSignal = make(chan struct{})
	bootHealthCheckChan = make(chan bool, 1)
}

func TestAliveHeartbeatCheck1(t *testing.T) {
	Convey("TestAliveHeartbeatCheck1 failed for err happened", t, func() {
		runtime.ProcessExitSignal = make(chan struct{})
		bootHealthCheckChan = make(chan bool, 1000)
		fusionRtm := &fusionRuntime{
			rt: &rpc.AbstractRPC{},
		}
		once := sync.Once{}
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(fusionRtm), "Healthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return false, errors.New("error happened")
				}),
			ApplyFunc(runtime.CloseExitChannel, func() {
				once.Do(func() {
					close(runtime.ProcessExitSignal)
				})
			}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		fusionRtm.aliveHeartbeatCheck(types.Heartbeat{
			PeriodSeconds:    1,
			SuccessThreshold: 1,
		})
	})
	runtime.ProcessExitSignal = make(chan struct{})
	bootHealthCheckChan = make(chan bool, 1)
	Convey("TestAliveHeartbeatCheck1 failed reach threshold", t, func() {
		runtime.ProcessExitSignal = make(chan struct{})
		bootHealthCheckChan = make(chan bool, 1000)
		fusionRtm := &fusionRuntime{
			rt: &rpc.AbstractRPC{},
		}
		once := sync.Once{}
		patches := []*Patches{
			ApplyMethod(reflect.TypeOf(fusionRtm), "Healthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return false, nil
				}),
			ApplyFunc(runtime.CloseExitChannel, func() {
				once.Do(func() {
					close(runtime.ProcessExitSignal)
				})
			}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		fusionRtm.aliveHeartbeatCheck(types.Heartbeat{
			PeriodSeconds:    1,
			SuccessThreshold: 1,
		})
	})
	runtime.ProcessExitSignal = make(chan struct{})
	bootHealthCheckChan = make(chan bool, 1)
}

func TestHeartbeatCheck2(t *testing.T) {
	Convey("test Healthz ok when call heartbeatcheck", t, func() {
		fusionRtm := &fusionRuntime{}
		heartbeat := types.Heartbeat{
			PeriodSeconds: 1,
			HealthEnable:  true,
		}
		mockTimer := time.NewTimer(time.Millisecond * 2)
		runtime.ProcessExitSignal = make(chan struct{})
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(mockTimer), "Reset",
				func(t *time.Timer, d time.Duration) bool {
					close(runtime.ProcessExitSignal)
					return true
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(fusionRtm), "Healthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return true, nil
				}),
			gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return mockTimer
			}),
			gomonkey.ApplyFunc(isRuntimeProcessStatusRunningOrRecover, func() bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fusionRtm.heartbeatCheck(1, 1, heartbeat, "java8")
		fusionRtm.heartbeatCheck(1, 1, heartbeat, "http")
	})
	Convey("test Healthz failed when call heartbeatcheck, err is RPCClientEmptyErrorCode", t, func() {
		fusionRtm := &fusionRuntime{}
		heartbeat := types.Heartbeat{
			PeriodSeconds: 1,
			HealthEnable:  true,
		}
		mockTimer := time.NewTimer(time.Millisecond * 2)
		runtime.ProcessExitSignal = make(chan struct{})
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(mockTimer), "Reset",
				func(t *time.Timer, d time.Duration) bool {
					close(runtime.ProcessExitSignal)
					return true
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(fusionRtm), "Healthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return false, fmt.Errorf(snerror.ErrText(snerror.RPCClientEmptyErrorCode))
				}),
			gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return mockTimer
			}),
			gomonkey.ApplyFunc(isRuntimeProcessStatusRunningOrRecover, func() bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fusionRtm.heartbeatCheck(1, 1, heartbeat, "java8")
	})
	Convey("test Healthz failed when call heartbeatcheck, err isn't RPCClientEmptyErrorCode", t, func() {
		fusionRtm := &fusionRuntime{}
		heartbeat := types.Heartbeat{
			PeriodSeconds: 1,
		}
		mockTimer := time.NewTimer(time.Millisecond * 2)
		runtime.ProcessExitSignal = make(chan struct{})
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(mockTimer), "Reset",
				func(t *time.Timer, d time.Duration) bool {
					close(runtime.ProcessExitSignal)
					return true
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(fusionRtm), "Healthz",
				func(r *fusionRuntime, path string, timeout int) (bool, error) {
					return false, fmt.Errorf(snerror.ErrText(snerror.RuntimeProcessExit))
				}),
			gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
				return mockTimer
			}),
			gomonkey.ApplyFunc(isRuntimeProcessStatusRunningOrRecover, func() bool {
				return true
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		fusionRtm.heartbeatCheck(2, 1, heartbeat, "java8")
	})
}

func mockReportErrMsgHandler(ctx context.Context, URNWithVersion, errMsg, level string, properties map[string]string) error {
	return nil
}

func mockSetExitHandler() {
	return
}

func mockReportErrMsgHandlerWithErr(ctx context.Context, URNWithVersion, errMsg, level string, properties map[string]string) error {
	return errors.New("test")
}

func TestReportEventToBus(t *testing.T) {
	Convey("test ReportErrMsgHandler is nil", t, func() {
		fusionRtm := &fusionRuntime{}
		fusionRtm.reportEventToBus("test", "INFO")
	})
	Convey("failed to exec ReportErrMsgHandler", t, func() {
		fusionRtm := &fusionRuntime{}
		fusionRtm.handlers.ReportErrMsgHandler = mockReportErrMsgHandlerWithErr
		fusionRtm.reportEventToBus("test", "INFO")
	})
	Convey("test ReportErrMsgHandler ok", t, func() {
		fusionRtm := &fusionRuntime{}
		fusionRtm.handlers.ReportErrMsgHandler = mockReportErrMsgHandler
		fusionRtm.reportEventToBus("test", "INFO")
	})
}

func TestSetExit(t *testing.T) {
	Convey("test SetExitHandler is nil", t, func() {
		fusionRtm := &fusionRuntime{}
		fusionRtm.setExit()
	})
	Convey("test SetExitHandler ok", t, func() {
		fusionRtm := &fusionRuntime{}
		fusionRtm.handlers.SetExitHandler = mockSetExitHandler
		fusionRtm.setExit()
	})
}

func Test_fusionRuntime_prestartRuntime(t *testing.T) {
	type fields struct {
		eh                   *exceptionHandler
		crashLimiter         *limiter
		newRuntimeFn         newRuntimeFn
		handlers             runtime.Handlers
		userCodePath         string
		rt                   runtime.Runtime
		mux                  sync.Mutex
		cancel               context.CancelFunc
		eventCnt             int64
		isRuntimeProcessDead int32
		isUserDatasExpansion bool
		subscribeOnce        sync.Once
		urnWithVersion       string
	}
	type args struct {
		ctx  context.Context
		conf *config.Configuration
	}
	mockTimer := time.NewTimer(reconnectRuntimeTimeout / 10)
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		want        bool
	}{
		{
			name:   "test reconnect not timeout",
			fields: fields{},
			args: args{
				ctx:  context.Background(),
				conf: &config.Configuration{},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(time.NewTimer, func(d time.Duration) *time.Timer {
						log.GetLogger().Infof("use mock timer duration is %v", reconnectRuntimeTimeout/10)
						return mockTimer // timeout is 2s
					}),
					gomonkey.ApplyFunc((*runtime.Reg).Prestart,
						func(_ *runtime.Reg, kind string, c *config.RuntimeConfig) error {
							time.Sleep(1500 * time.Millisecond) // 1.5s
							return nil
						}),
				})
				return patches
			},
			want: true,
		}, // expect reconnect not timeout
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &fusionRuntime{
				eh:                   tt.fields.eh,
				crashLimiter:         tt.fields.crashLimiter,
				newRuntimeFn:         tt.fields.newRuntimeFn,
				handlers:             tt.fields.handlers,
				userCodePath:         tt.fields.userCodePath,
				rt:                   tt.fields.rt,
				mux:                  tt.fields.mux,
				cancel:               tt.fields.cancel,
				eventCnt:             tt.fields.eventCnt,
				isRuntimeProcessDead: tt.fields.isRuntimeProcessDead,
				isUserDatasExpansion: tt.fields.isUserDatasExpansion,
				subscribeOnce:        tt.fields.subscribeOnce,
				urnWithVersion:       tt.fields.urnWithVersion,
			}
			patches := tt.patchesFunc()
			assert.Equalf(t, tt.want, r.prestartRuntime(tt.args.ctx, tt.args.conf), "prestartRuntime(%v, %v)", tt.args.ctx, tt.args.conf)
			patches.ResetAll()
		})
	}
}

func TestcleanTmpDiskWithReconnect(t *testing.T) {
	Convey("test cleanTmpDiskWithReconnect", t, func() {
		var fr *fusionRuntime
		runTimeErr := &runtime.Error{
			Code: snerror.INodeUsageExceed,
		}
		var inodeUsageExceedFlag bool
		p := gomonkey.NewPatches()
		p.ApplyFunc(os.RemoveAll, func(path string) error {
			inodeUsageExceedFlag = true
			return nil
		})
		defer p.Reset()

		fr.cleanTmpDiskWithReconnect(runTimeErr)
		So(inodeUsageExceedFlag, ShouldBeTrue)
	})
}

func TestRequestUpdateAgencyWithRuntimeNil(t *testing.T) {
	Convey("test RequestUpdateAgency with runtime is nil", t, func() {
		Convey("case1: runtime is nil", func() {
			var fr *fusionRuntime
			err := fr.RequestUpdateAgency()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "runtime is nil")
		})
	})
}

func TestRuntimeStateRecord(t *testing.T) {
	Convey("TestRuntimeStateRecord", t, func() {
		Convey("case1: set and compare runtime state", func() {
			runtimeState := RuntimeStateRecord{
				Mutex: sync.Mutex{},
			}
			So(runtimeState.SetRuntimeState(4002), ShouldBeFalse)
			So(runtimeState.SetRuntimeState(4002), ShouldBeTrue)
			So(runtimeState.SetRuntimeState(4207), ShouldBeFalse)
			So(runtimeState.SetRuntimeState(4207), ShouldBeTrue)
			So(runtimeState.SetRuntimeState(4225), ShouldBeFalse)
		})
	})
}

func Test_reportCaeRuntimeEvent(t *testing.T) {
	Convey("Test_reportCaeRuntimeEvent", t, func() {
		Convey("case1: runtimeState is nil", func() {
			r := &fusionRuntime{
				crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
				eh:           newExceptionHandler(),
				handlers:     runtime.Handlers{},
				rt:           &fakeRuntime{},
			}
			execSetState := false
			p := gomonkey.NewPatches()
			p.ApplyFunc(utils.GetBusinessType, func() string {
				return constants.BusinessTypeCAE
			})
			p.ApplyMethod(reflect.TypeOf(&RuntimeStateRecord{}), "SetRuntimeState", func(_ *RuntimeStateRecord, newState int32) bool {
				execSetState = true
				return execSetState
			})
			defer p.Reset()
			r.reportCaeRuntimeEvent(&runtime.Error{
				Code:    4202,
				Message: "test",
			})
			So(execSetState, ShouldBeFalse)
		})
		Convey("case2: runtimeError is nil", func() {
			r := &fusionRuntime{
				crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
				eh:           newExceptionHandler(),
				handlers:     runtime.Handlers{},
				rt:           &fakeRuntime{},
				runtimeState: &RuntimeStateRecord{
					Mutex: sync.Mutex{},
				},
			}
			execSetState := false
			p := gomonkey.NewPatches()
			p.ApplyFunc(utils.GetBusinessType, func() string {
				return constants.BusinessTypeCAE
			})
			p.ApplyMethod(reflect.TypeOf(&RuntimeStateRecord{}), "SetRuntimeState", func(_ *RuntimeStateRecord, newState int32) bool {
				execSetState = true
				return execSetState
			})
			defer p.Reset()
			r.reportCaeRuntimeEvent(nil)
			So(execSetState, ShouldBeFalse)
		})
		Convey("case3: errorcode is RuntimeInitTimeoutCode", func() {
			currentMsg := "process initialize time out after 1m30s"
			isMatch := false
			r := &fusionRuntime{
				crashLimiter: newLimiter(limitRestartTimes, limitRestartTimeDuration),
				eh:           newExceptionHandler(),
				handlers: runtime.Handlers{
					ReportErrMsgHandler: func(ctx context.Context, URNWithVersion, errMsg, level string,
						properties map[string]string) error {
						if currentMsg == errMsg {
							isMatch = true
						}
						return nil
					},
				},
				rt: &fakeRuntime{},
				runtimeState: &RuntimeStateRecord{
					Mutex: sync.Mutex{},
				},
			}
			p := gomonkey.NewPatches()
			p.ApplyFunc(utils.GetBusinessType, func() string {
				return constants.BusinessTypeCAE
			})
			defer p.Reset()
			startupError := snerror.FailureFee{
				Log:     "123",
				Summary: snerror.SummaryInfo{},
				Message: "runtime initialization timed out after 1m30s",
			}
			data, _ := json.Marshal(startupError)
			msg := string(data)

			r.reportCaeRuntimeEvent(&runtime.Error{
				Code:    snerror.RuntimeInitTimeoutCode,
				Message: msg,
			})
			So(isMatch, ShouldBeTrue)
		})
	})
}
