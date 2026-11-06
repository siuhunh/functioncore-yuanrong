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
	"reflect"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invokectx"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/status"
	mockUtils "functioncore/test/utils"
)

type fakeRuntime struct {
	handlers runtime.Handlers
}

func (rt *fakeRuntime) RequestUpdateAgency() error {
	return nil
}

func (*fakeRuntime) HotLoad(ctx context.Context, conf *config.Configuration) error { return nil }

func (*fakeRuntime) Healthz(path string, timeout int) (bool, error) { return true, nil }

func (*fakeRuntime) StartupHealthz(path string, timeout int) (bool, error) { return true, nil }

func (*fakeRuntime) ReportCustomMetrics(timeout int) ([]byte, error) { return []byte{}, nil }

func (rt *fakeRuntime) SendProcessEvent(ic invokectx.InvokeCtx, message runtime.InvokeRequest,
	conf *config.Configuration) {
	go rt.handlers.RecvProcessEventHandler(ic, runtime.InvokeResponse{Result: message.Event})
}

func (*fakeRuntime) ProcessCancelEvent(message *runtime.CancelRequest) *runtime.CancelResponse {
	return &runtime.CancelResponse{}
}

func (*fakeRuntime) PreStop() error { return nil }

func (fakeRuntime) GetException(shouldWait bool) *runtime.Error {
	return nil
}

type fakeCreator struct{}

func (*fakeCreator) NewRuntime(configuration *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
	return &fakeRuntime{}, nil
}

type fakePrestartableCreator struct {
	fakeCreator
}

func (*fakePrestartableCreator) Prestart(c *config.RuntimeConfig) error {
	return nil
}

func TestCreator_NewRuntime(t *testing.T) {
	p := NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	var reg *runtime.Reg
	fakeCreator := &fakeCreator{}
	p.ApplyMethod(reflect.TypeOf(reg), "GetCreator", func(_ *runtime.Reg, kind string) runtime.Creator {
		return fakeCreator
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	p.ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
		return functionlog.NewFunctionLogger(nil, "", ""), nil
	})
	defer p.Reset()

	testCases := []struct {
		name                string
		configuration       *config.Configuration
		expectedRuntime     runtime.LegacyRuntime
		expectedErrorString string
	}{
		{
			name: "new",
			configuration: &config.Configuration{RuntimeConfig: config.RuntimeConfig{
				FuncMountConfig: types.FuncMountConfig{
					FuncMountUser: types.FuncMountUser{
						UserID:  2000,
						GroupID: 2000,
					},
				},
			},
			},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			c := creator{}
			_, actualError := c.NewRuntime(tt.configuration, runtime.Handlers{})
			if tt.expectedErrorString == "" {
				require.NoError(t, actualError)
			}
			if tt.expectedErrorString != "" {
				require.Errorf(t, actualError, tt.expectedErrorString)
			}
		})
	}
}

func TestStopRuntimeManagerProcess(t *testing.T) {
	var conn *tcpmsg.SocketConn

	Convey("test stop runtime manager process succeed", t, func() {
		patch := ApplyMethod(reflect.TypeOf(conn), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
			return nil
		})
		patch.ApplyGlobalVar(&preStopped, true)
		// status is not initializing, it will stop runtime process and break for loop
		status.Set(status.Waiting)
		defer patch.Reset()
		stopRuntimeManagerProcess()
	})
}

func TestRegisterRuntime(t *testing.T) {
	Convey("test fusion RegisterRuntime", t, func() {
		creator := runtime.Registry.GetCreator("fusion")
		So(creator, ShouldEqual, nil)
		RegisterRuntime()
		creator = runtime.Registry.GetCreator("fusion")
		So(creator, ShouldNotEqual, nil)
	})
}

func TestRestart(t *testing.T) {
	p := NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	var reg *runtime.Reg
	logger := functionlog.NewFunctionLogger(nil, "", "")
	fakeCreator := &fakeCreator{}
	p.ApplyMethod(reflect.TypeOf(reg), "GetCreator", func(_ *runtime.Reg, kind string) runtime.Creator {
		return fakeCreator
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	p.ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
		return logger, nil
	})
	p.ApplyFunc(tcpmsg.CreateSocketClient, func(address string, handlerFunc func(*tcpmsg.Message) (int, tcpmsg.RuntimeRSP, error)) (*tcpmsg.SocketConn, error) {
		return &tcpmsg.SocketConn{}, nil
	})

	defer p.Reset()

	c := creator{}
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
	rt, err := c.NewRuntime(cfg, runtime.Handlers{})
	require.NoError(t, err)

	err = rt.HotLoad(context.TODO(), cfg)
	require.NoError(t, err)

	err = c.Prestart(&cfg.RuntimeConfig)
	require.NoError(t, err)

	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.ReportLog}})
	require.NoError(t, err)

	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.LogFinished}})
	require.NoError(t, err)

	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.ReportMemory}})
	require.NoError(t, err)

	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.ResetDomainName}})
	require.Error(t, err)

	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.ReportCPU}, Body: make([]byte, 16)})
	require.NoError(t, err)

	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: 255}})
	require.Error(t, err)

	// wait for writeUserFunctionLog to finish
	time.Sleep(1 * time.Second)
}

func TestRestart2(t *testing.T) {
	p := NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessageWithNoResp", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) error {
		return nil
	})
	var reg *runtime.Reg
	logger := functionlog.NewFunctionLogger(nil, "", "")
	fakeCreator := &fakeCreator{}
	p.ApplyMethod(reflect.TypeOf(reg), "GetCreator", func(_ *runtime.Reg, kind string) runtime.Creator {
		return fakeCreator
	})
	p.ApplyFunc(diskusage.New, func(rootDir string, opts ...diskusage.Opt) (*diskusage.Monitor, error) {
		return &diskusage.Monitor{}, nil
	})
	p.ApplyFunc(functionlog.GetFunctionLogger, func(cfg *config.Configuration) (*functionlog.FunctionLogger, error) {
		return logger, nil
	})
	p.ApplyFunc(tcpmsg.CreateSocketClient, func(address string, handlerFunc func(*tcpmsg.Message) (int, tcpmsg.RuntimeRSP, error)) (*tcpmsg.SocketConn, error) {
		return &tcpmsg.SocketConn{}, nil
	})
	defer p.Reset()
	c := creator{}
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
	_, err := c.NewRuntime(cfg, runtime.Handlers{})
	require.NoError(t, err)
	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.ReportRequest}})
	require.NoError(t, err)
	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.RuntimeProcessExit}})
	require.NoError(t, err)
	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.KillRuntimeProcess}})
	require.Error(t, err)
	_, _, err = c.requestHandler(&tcpmsg.Message{Header: tcpmsg.Header{MsgType: tcpmsg.RestoreDead}})
	// wait for writeUserFunctionLog to finish
	time.Sleep(50 * time.Millisecond)
}

func TestStopRuntimeProcess(t *testing.T) {
	p := NewPatches()
	p.ApplyMethod(reflect.TypeOf(runtime.RuntimeManagerClient), "SendMessage", func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
		return tcpmsg.RuntimeManagerRSP{}
	})
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
	resp := StopRuntimeProcess(cfg)
	assert.Equal(t, "", resp.Err)
}

func Test_isDefaultUIDGID(t *testing.T) {
	type args struct {
		uid int
		gid int
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test001",
			args: args{
				uid: 1004,
				gid: 0,
			},
			want: false,
		},
		{
			name: "test001",
			args: args{
				uid: 0,
				gid: 1004,
			},
			want: false,
		},
		{
			name: "test001",
			args: args{
				uid: 0,
				gid: 1003,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, isDefaultUIDGID(tt.args.uid, tt.args.gid), "isDefaultUIDGID(%v, %v)", tt.args.uid, tt.args.gid)
		})
	}
}

func TestSendCheckpointCommand(t *testing.T) {
	p := NewPatches()
	p.ApplyFunc((*tcpmsg.SocketConn).SendMessage,
		func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
			return tcpmsg.RuntimeManagerRSP{
				Err:      "timeout",
				Code:     4010,
				Response: tcpmsg.RuntimeRSP{},
			}
		})
	defer p.Reset()
	err := SendCheckpointCommand()
	assert.NotNil(t, err)
}

func TestBuildUserLogTelemetryEvent(t *testing.T) {
	type args struct {
		msg *tcpmsg.Message
	}
	tests := []struct {
		name        string
		args        args
		want        bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "test001",
			args: args{
				msg: &tcpmsg.Message{
					Body: []byte("[Function]aaaaaaa"),
				},
			},
			want: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
				})
				return patches
			},
		},
		{
			name: "test002",
			args: args{
				msg: &tcpmsg.Message{
					Body: []byte("[Extension]bbbbbbb"),
				},
			},
			want: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
						return true
					}),
				})
				return patches
			},
		},
		{
			name: "test003",
			args: args{
				msg: &tcpmsg.Message{
					Body: []byte("[unknown]aaaaaaa"),
				},
			},
			want: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(slice.ContainsString, func(slice []string, s string, modifier func(s string) string) bool {
						return false
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			buildTelemetryExtensionEvent(tt.args.msg)
			patches.ResetAll()
		})
	}
}
