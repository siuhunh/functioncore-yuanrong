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

// Package nodemgr is manager of worker-manager cluster info
package nodemgr

import (
	"context"
	"io"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	bridgeConfig "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	"functioncore/pkg/state-manager/config"
	"functioncore/test/utils"
)

type MockReadCloser struct{}

func (m MockReadCloser) Close() error {
	return nil
}

func (m MockReadCloser) Read([]byte) (int, error) {
	return 0, io.EOF
}

func TestClusterManager(t *testing.T) {
	convey.Convey("Test cluster manager", t, func() {
		// add node
		Instance().AddNode("node")
		nodeIP := Instance().GetNode()
		convey.So(nodeIP, convey.ShouldEqual, "node")

		// remove node
		Instance().RemoveNode()
		nodeIP = Instance().GetNode()
		convey.So(nodeIP, convey.ShouldEqual, "")
	})
}

func TestGetFunctionInfoByURN(t *testing.T) {
	type args struct {
		urn string
	}
	tests := []struct {
		name string
		args args
		want FunctionInfo
	}{
		{
			"test len is 7 ",
			args{
				urn: "a:b:c:d:e:f:g",
			},
			FunctionInfo{
				Business: "c",
				Tenant:   "d",
				FuncName: "f",
				Version:  "g",
			},
		},
		{
			"test len is 6 ",
			args{
				urn: "a:b:c:d:e:f",
			},
			FunctionInfo{
				Business: "c",
				Tenant:   "d",
				FuncName: "f",
				Version:  "",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetFunctionInfoByURN(tt.args.urn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFunctionInfoByURN() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestManager_GetFuncInstances(t *testing.T) {
	type fields struct {
		lock   sync.RWMutex
		nodeIP string
	}
	type args struct {
		funcSvc string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			"test get config error",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			true,
		},
		{
			"test get config success",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
						return &config.FConfig{
							HTTPSConfig: tls.InternalHTTPSConfig{
								HTTPSEnable: true,
							},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		c := &Manager{
			lock:   tt.fields.lock,
			nodeIP: tt.fields.nodeIP,
		}
		_, err := c.GetFuncInstances(tt.args.funcSvc)
		if (err != nil) != tt.wantErr {
			t.Errorf("GetFuncInstances() error = %v, wantErr %v", err, tt.wantErr)
			return
		}

		patches.ResetAll()
	}
}

func TestManager_ScaleDownInstance(t *testing.T) {
	type fields struct {
		lock   sync.RWMutex
		nodeIP string
	}
	type args struct {
		functionSvcID    string
		workerID         string
		functionMetaInfo *bridgeConfig.FunctionMetaInfo
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			"test get config err",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			true,
		},
		{
			"test get config success",
			fields{},
			args{
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
						return &config.FConfig{
							HTTPSConfig: tls.InternalHTTPSConfig{
								HTTPSEnable: true,
							},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
		{
			"test get config success",
			fields{},
			args{
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
						return &config.FConfig{
							HTTPSConfig: tls.InternalHTTPSConfig{
								HTTPSEnable: true,
							},
						}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client,
						req *http.Request) (*http.Response, error) {
						return &http.Response{
							Body: MockReadCloser{},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		c := &Manager{
			lock:   tt.fields.lock,
			nodeIP: tt.fields.nodeIP,
		}
		if err := c.ScaleDownInstance(tt.args.functionSvcID, tt.args.workerID, tt.args.functionMetaInfo); (err != nil) != tt.wantErr {
			t.Errorf("ScaleDownInstance() error = %v, wantErr %v", err, tt.wantErr)
		}
		patches.ResetAll()
	}
}

func TestManager_ScaleUpInstance(t *testing.T) {
	type fields struct {
		lock   sync.RWMutex
		nodeIP string
	}
	type args struct {
		functionSvcID    string
		functionMetaInfo *bridgeConfig.FunctionMetaInfo
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			"test get config err",
			fields{},
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			true,
		},
		{
			"test get config success",
			fields{},
			args{
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
						return &config.FConfig{
							HTTPSConfig: tls.InternalHTTPSConfig{
								HTTPSEnable: true,
							},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
		{
			"test get config success",
			fields{},
			args{
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
						return &config.FConfig{
							HTTPSConfig: tls.InternalHTTPSConfig{
								HTTPSEnable: true,
							},
						}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client,
						req *http.Request) (*http.Response, error) {
						return &http.Response{
							Body: MockReadCloser{},
						}, nil
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		c := &Manager{
			lock:   tt.fields.lock,
			nodeIP: tt.fields.nodeIP,
		}
		_, err := c.ScaleUpInstance(tt.args.functionSvcID, tt.args.functionMetaInfo)
		if (err != nil) != tt.wantErr {
			t.Errorf("ScaleUpInstance() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
	}
}

func TestManager_makeScaleDownReq(t *testing.T) {
	type fields struct {
		lock   sync.RWMutex
		nodeIP string
	}
	type args struct {
		ctx              context.Context
		cfg              *config.FConfig
		workerID         string
		functionMetaInfo *bridgeConfig.FunctionMetaInfo
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test make scale down req",
			fields{},
			args{
				cfg: &config.FConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: true,
					},
				},
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
			},
			true,
		},
		{
			"test make req",
			fields{},
			args{
				ctx: context.Background(),
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
				cfg: &config.FConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: true,
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Manager{
				lock:   tt.fields.lock,
				nodeIP: tt.fields.nodeIP,
			}
			_, err := c.makeScaleDownReq(tt.args.ctx, tt.args.cfg, tt.args.workerID, tt.args.functionMetaInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("makeScaleDownReq() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestManager_makeScaleUpReq(t *testing.T) {
	type fields struct {
		lock   sync.RWMutex
		nodeIP string
	}
	type args struct {
		ctx              context.Context
		functionMetaInfo *bridgeConfig.FunctionMetaInfo
		traceID          string
		cfg              *config.FConfig
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			"test make req without context",
			fields{},
			args{
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
				cfg: &config.FConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: true,
					},
				},
			},
			true,
		},
		{
			"test make req",
			fields{},
			args{
				ctx: context.Background(),
				functionMetaInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						FunctionVersionURN: "asd",
					},
				},
				cfg: &config.FConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: true,
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Manager{
				lock:   tt.fields.lock,
				nodeIP: tt.fields.nodeIP,
			}
			_, err := c.makeScaleUpReq(tt.args.ctx, tt.args.functionMetaInfo, tt.args.traceID, tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("makeScaleUpReq() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_getScaleTimeOut(t *testing.T) {
	type args struct {
		functionEtcdInfo *bridgeConfig.FunctionMetaInfo
	}
	tests := []struct {
		name string
		args args
		want time.Duration
	}{
		{
			"test config is zero",
			args{
				functionEtcdInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						Timeout: 0,
					},
				},
			},
			defaultScaleTimeout,
		},
		{
			"test config not zero",
			args{
				functionEtcdInfo: &bridgeConfig.FunctionMetaInfo{
					FuncMetaData: bridgeConfig.FuncMetaData{
						Timeout: 10,
					},
					ExtendedMetaData: bridgeConfig.ExtendedMetaData{
						Initializer: types.Initializer{
							Timeout: 10,
						},
					},
				},
			},
			66 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getScaleTimeOut(tt.args.functionEtcdInfo); got != tt.want {
				t.Errorf("getScaleTimeOut() = %v, want %v", got, tt.want)
			}
		})
	}
}
