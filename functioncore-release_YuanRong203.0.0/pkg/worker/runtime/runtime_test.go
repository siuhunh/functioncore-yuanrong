package runtime

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionlog"
	mockutils "functioncore/test/utils"
)

func TestNewAbstractRuntime(t *testing.T) {
	type args struct {
		conf *config.Configuration
	}
	tests := []struct {
		name      string
		args      args
		patchFunc mockutils.PatchesFunc
		want      *AbstractRuntime
		wantErr   bool
	}{
		{
			name: "test01",
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(functionlog.GetFunctionLogger,
						func(*config.Configuration) (*functionlog.FunctionLogger, error) {
							return functionlog.NewFunctionLogger(nil, "", ""), nil
						}),
					gomonkey.ApplyFunc(commonUtils.ValidateFilePath,
						func(string) error {
							return nil
						}),
				})
				return patches
			},
			args:    args{conf: &config.Configuration{}},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			got, err := NewAbstractRuntime(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewAbstractRuntime() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("NewAbstractRuntime() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestCloseExitChannel(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "test01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			CloseExitChannel()
		})
	}
}

func TestProcessLimit_WatchRuntimeRequestCount(t *testing.T) {
	type fields struct {
		Status     *ProcessStatus
		client     *tcpmsg.SocketConn
		enable     bool
		idleTime   time.Duration
		retryTimes int
	}
	type args struct {
		client *tcpmsg.SocketConn
	}
	tests := []struct {
		name      string
		fields    fields
		patchFunc mockutils.PatchesFunc
		args      args
	}{
		{
			name: "test01",
			fields: fields{
				Status:     &ProcessStatus{status: 19},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 0,
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.Configuration, error) {
							return &config.Configuration{
								BackgroundProcessLimit: config.BackgroundProcessLimit{
									LimitEnable: true,
									IdleTime:    1,
									RetryTimes:  1,
								},
							}, nil
						}),
				})
				return patches
			},
			args: args{client: nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ProcessLimit{
				Status:     tt.fields.Status,
				client:     tt.fields.client,
				enable:     tt.fields.enable,
				idleTime:   tt.fields.idleTime,
				retryTimes: tt.fields.retryTimes,
			}
			patches := tt.patchFunc()
			CloseExitChannel()
			p.WatchRuntimeRequestCount(tt.args.client)
			patches.ResetAll()
		})
	}
}

func TestProcessLimit_RecoverRuntime(t *testing.T) {
	type fields struct {
		Status     *ProcessStatus
		client     *tcpmsg.SocketConn
		enable     bool
		idleTime   time.Duration
		retryTimes int
	}
	type args struct {
		workerConfig *config.Configuration
		client       *tcpmsg.SocketConn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test01",
			fields: fields{
				Status:     &ProcessStatus{status: 19},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 0,
			},
			args: args{
				workerConfig: &config.Configuration{
					RuntimeConfig: config.RuntimeConfig{
						Language: "custom-container",
					},
				},
				client: &tcpmsg.SocketConn{},
			},
		},
		{
			name: "test02",
			fields: fields{
				Status:     &ProcessStatus{status: 19},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 0,
			},
			args: args{
				workerConfig: &config.Configuration{
					RuntimeConfig: config.RuntimeConfig{
						Language: "custom-runtime",
					},
				},
				client: &tcpmsg.SocketConn{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ProcessLimit{
				Status:     tt.fields.Status,
				client:     tt.fields.client,
				enable:     tt.fields.enable,
				idleTime:   tt.fields.idleTime,
				retryTimes: tt.fields.retryTimes,
			}
			p.RecoverRuntime(tt.args.workerConfig, tt.args.client)
		})
	}
}

func TestProcessLimit_ProcessRecoverEvent(t *testing.T) {
	type fields struct {
		Status     *ProcessStatus
		client     *tcpmsg.SocketConn
		enable     bool
		idleTime   time.Duration
		retryTimes int
	}
	type args struct {
		client *tcpmsg.SocketConn
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockutils.PatchesFunc
	}{
		{
			name: "test01",
			fields: fields{
				Status:     &ProcessStatus{status: 19},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			},
			args: args{
				client: &tcpmsg.SocketConn{},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ProcessLimit{
				Status:     tt.fields.Status,
				client:     tt.fields.client,
				enable:     tt.fields.enable,
				idleTime:   tt.fields.idleTime,
				retryTimes: tt.fields.retryTimes,
			}
			patches := tt.patchFunc()
			p.ProcessRecoverEvent(tt.args.client)
			patches.ResetAll()
		})
	}
}

func TestCheckAsyncRequest(t *testing.T) {
	tests := []struct {
		name      string
		patchFunc mockutils.PatchesFunc
	}{
		{
			name: "test01",
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*Metrics).GetRequestCount,
						func(_ *Metrics) int {
							return 0
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			CheckAsyncRequest()
			patches.ResetAll()
		})
	}
}

func TestSetIdleMode(t *testing.T) {
	type args struct {
		idleMode bool
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test01",
			args: args{idleMode: false},
		},
		{
			name: "test02",
			args: args{idleMode: true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SetIdleMode(tt.args.idleMode)
		})
	}
}

func TestProcessLimit_PauseRuntimeProcess(t *testing.T) {
	type fields struct {
		Status     *ProcessStatus
		client     *tcpmsg.SocketConn
		enable     bool
		idleTime   time.Duration
		retryTimes int
	}
	type args struct {
		runtimeContainerID string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockutils.PatchesFunc
	}{
		{
			name: "test01",
			fields: fields{
				Status:     &ProcessStatus{status: 19},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			},
			args: args{""},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*Metrics).GetRequestFinishTime,
						func(_ *Metrics) time.Time {
							return time.Unix(275, 0)
						}),
					gomonkey.ApplyFunc(retryingWriteFile,
						func(path string, data []byte, mode os.FileMode) error {
							return nil
						}),
					gomonkey.ApplyFunc(fileState,
						func(path string) (string, error) {
							return "frozen", nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ProcessLimit{
				Status:     tt.fields.Status,
				client:     tt.fields.client,
				enable:     tt.fields.enable,
				idleTime:   tt.fields.idleTime,
				retryTimes: tt.fields.retryTimes,
			}
			patches := tt.patchFunc()
			p.PauseRuntimeProcess(tt.args.runtimeContainerID)
			patches.ResetAll()
		})
	}
}

func TestProcessLimit_ProcessPauseEvent(t *testing.T) {
	type fields struct {
		Status     *ProcessStatus
		client     *tcpmsg.SocketConn
		enable     bool
		idleTime   time.Duration
		retryTimes int
	}
	type args struct {
		client *tcpmsg.SocketConn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "test01",
			fields: fields{
				Status:     &ProcessStatus{status: 19},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			},
			args: args{client: &tcpmsg.SocketConn{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ProcessLimit{
				Status:     tt.fields.Status,
				client:     tt.fields.client,
				enable:     tt.fields.enable,
				idleTime:   tt.fields.idleTime,
				retryTimes: tt.fields.retryTimes,
			}
			p.ProcessPauseEvent(tt.args.client)
		})
	}
}

func TestProcessLimit_processCheck(t *testing.T) {
	type fields struct {
		Status     *ProcessStatus
		client     *tcpmsg.SocketConn
		enable     bool
		idleTime   time.Duration
		retryTimes int
	}
	type args struct {
		workerConfig *config.Configuration
		client       *tcpmsg.SocketConn
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		patchFunc mockutils.PatchesFunc
	}{
		{
			name: "test01",
			fields: fields{
				Status:     &ProcessStatus{status: 19},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			},
			args: args{
				workerConfig: &config.Configuration{RuntimeConfig: config.RuntimeConfig{Language: "custom-container"}},
				client:       &tcpmsg.SocketConn{},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
		},
		{
			name: "test02",
			fields: fields{
				Status:     &ProcessStatus{status: 0},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			},
			args: args{
				workerConfig: &config.Configuration{RuntimeConfig: config.RuntimeConfig{Language: "custom-runtime"}},
				client:       &tcpmsg.SocketConn{},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage,
						func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
							return tcpmsg.RuntimeManagerRSP{
								Err:      "",
								Code:     0,
								Response: tcpmsg.RuntimeRSP{},
							}
						}),
				})
				return patches
			},
		},
		{
			name: "test03",
			fields: fields{
				Status:     &ProcessStatus{status: 1},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			},
			args: args{
				workerConfig: &config.Configuration{RuntimeConfig: config.RuntimeConfig{Language: "custom-runtime"}},
				client:       &tcpmsg.SocketConn{},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc((*tcpmsg.SocketConn).SendMessage,
						func(_ *tcpmsg.SocketConn, msg *tcpmsg.Message) tcpmsg.RuntimeManagerRSP {
							return tcpmsg.RuntimeManagerRSP{
								Err:      "",
								Code:     0,
								Response: tcpmsg.RuntimeRSP{},
							}
						}),
					gomonkey.ApplyFunc((*Metrics).GetRequestCount,
						func(_ *Metrics) int {
							return 1
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ProcessLimit{
				Status:     tt.fields.Status,
				client:     tt.fields.client,
				enable:     tt.fields.enable,
				idleTime:   tt.fields.idleTime,
				retryTimes: tt.fields.retryTimes,
			}
			patches := tt.patchFunc()
			if tt.name == "test01" {
				PodApplier = constants.WorkerManagerApplier
			}
			p.processCheck(tt.args.workerConfig, tt.args.client)
			patches.ResetAll()
		})
	}
}

func Test_checkRequestCount(t *testing.T) {
	type args struct {
		executingReq int
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "test01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.name == "test01" {
				asyncRequestCheckInterval = 2 * time.Second
				asyncRequestMaximumTime = 3 * time.Second
			}
			checkRequestCount(tt.args.executingReq)
		})
	}
	RunMetrics.IncrWsConnCount()
	RunMetrics.IncrWsConnCount()
	RunMetrics.IncreaseRequestCount("")
	RunMetrics.IncreaseRequestCount("")
	checkRequestCount(0)
	assert.Equal(t, int(RunMetrics.wsConnCount), 2)
	RunMetrics.DecreaseRequestCount("")
	RunMetrics.DecrWsConnCount()
	checkRequestCount(0)
	assert.Equal(t, int(RunMetrics.wsConnCount), 1)
}

func Test_fileState(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name      string
		args      args
		want      string
		wantErr   bool
		patchFunc mockutils.PatchesFunc
	}{
		{
			name:    "test01",
			wantErr: true,
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{})
				return patches
			},
		},
		{
			name: "test02",
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile,
						func(string) ([]byte, error) {
							return nil, nil
						}),
				})
				return patches
			},
			wantErr: false,
			want:    "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			got, err := fileState(tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("fileState() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("fileState() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestProcessLimit_setRecoverStatus(t *testing.T) {
	type fields struct {
		Status     *ProcessStatus
		client     *tcpmsg.SocketConn
		enable     bool
		idleTime   time.Duration
		retryTimes int
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			name: "test01",
			fields: fields{
				Status:     &ProcessStatus{status: 0},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &ProcessLimit{
				Status:     tt.fields.Status,
				client:     tt.fields.client,
				enable:     tt.fields.enable,
				idleTime:   tt.fields.idleTime,
				retryTimes: tt.fields.retryTimes,
			}
			p.setRecoverStatus()
		})
	}
}

func TestProcessLimit_setFinalizeStatus(t *testing.T) {
	convey.Convey("Test set finalize status", t, func() {
		convey.Convey("Test current RuntimeProcessStatusReconnect set finalize status", func() {
			p := &ProcessLimit{
				Status:     &ProcessStatus{status: 1},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			}
			p.setFinalizeStatus()
			convey.So(p.Status.Get(), convey.ShouldEqual, tcpmsg.RuntimeProcessStatusReconnect)
		})
		convey.Convey("Test current other status set finalize status", func() {
			p := &ProcessLimit{
				Status:     &ProcessStatus{status: 0},
				client:     &tcpmsg.SocketConn{},
				enable:     true,
				idleTime:   time.Duration(2) * time.Second,
				retryTimes: 1,
			}
			p.setFinalizeStatus()
			convey.So(p.Status.Get(), convey.ShouldEqual, tcpmsg.RuntimeProcessStatusFinalize)
		})
	})
}

func Test_NewRuntimeError(t *testing.T) {
	result := "function invocation exception, error: test"
	err := NewRuntimeError(snerror.FunctionExceptionCode, "test")
	assert.Equal(t, result, err.Message)
}

func Test_NewRuntimeErrorWithoutMessage(t *testing.T) {
	result := "function invocation exception"
	err := NewRuntimeError(snerror.FunctionExceptionCode, "")
	assert.Equal(t, result, err.Message)
}

func Test_changeErrorStructToError(t *testing.T) {
	tmpErr := &Error{4205, "test", nil}
	err := changeErrorStructToError(tmpErr)
	assert.NotNil(t, err)
	err = changeErrorStructToError(nil)
	assert.Nil(t, err)
}

func TestSetBusinessType(t *testing.T) {
	SetBusinessType("test")
	assert.Equal(t, "test", businessType)
}

func TestNewRuntimeProcessStatus(t *testing.T) {
	status := newRuntimeProcessStatus()
	assert.Equal(t, int(status.status), -2)
}
