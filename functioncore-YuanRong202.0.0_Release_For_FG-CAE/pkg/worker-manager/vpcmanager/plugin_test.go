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

package vpcmanager

import (
	"encoding/json"
	"errors"
	"plugin"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	mockUtils "functioncore/test/utils"
)

// TestInitControllerAndCheckResource test initControllerAndCheckResource
func TestPluginVPC_InitControllerAndCheckResource(t *testing.T) {
	convey.Convey("initControllerAndCheckResource", t, func() {
		pluginVPC := PluginVPC{}
		pluginVPC.Plugin = &plugin.Plugin{}
		ch := make(chan string)
		patch := gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
			if s == "InitController" {
				return func() error {
					return nil
				}, nil
			}
			if s == "CheckResource" {
				return func(ch chan string) error {
					return nil
				}, nil
			}
			if s == "CreateResource" {
				return func(request []byte) ([]byte, error) {
					return []byte{}, nil
				}, nil
			}
			if s == "DeleteResource" {
				return func(string string) error {
					return nil
				}, nil
			}
			return func() {}, nil
		})
		defer patch.Reset()
		err := pluginVPC.initController()
		convey.So(err, convey.ShouldEqual, nil)
		err = pluginVPC.checkResource(ch)
		convey.So(err, convey.ShouldEqual, nil)
	})

	type args struct {
		pluginFile *plugin.Plugin
		handler    *VpcHandler
	}
	tests := []struct {
		name                  string
		args                  args
		wantErr               bool
		wantInitControllerErr bool
		wantCheckResourceErr  bool
		patchesFunc           mockUtils.PatchesFunc
	}{
		{
			name:                  "case1 lookup InitController error",
			args:                  args{},
			wantErr:               true,
			wantInitControllerErr: true,
			wantCheckResourceErr:  false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "InitController" {
							return func() error {
								return nil
							}, errors.New("test")
						}
						return func() {}, nil
					}),
				})
				return patches
			}},

		{
			name:                  "case2 initController error",
			args:                  args{},
			wantErr:               true,
			wantInitControllerErr: true,
			wantCheckResourceErr:  false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "InitController" {
							return func() error {
								return errors.New("test")
							}, nil
						}
						return func() {}, nil
					}),
				})
				return patches
			}},

		{
			name:                  "case3 lookup CheckResource error",
			args:                  args{},
			wantErr:               true,
			wantInitControllerErr: false,
			wantCheckResourceErr:  true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "InitController" {
							return func() error {
								return nil
							}, nil
						}
						if s == "CheckResource" {
							return func(ch chan string) error {
								return nil
							}, errors.New("test")
						}
						return func() {}, nil
					}),
				})
				return patches
			}},

		{
			name:                 "case4 checkResource error",
			args:                 args{handler: &VpcHandler{}},
			wantErr:              true,
			wantCheckResourceErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "InitController" {
							return func() error {
								return nil
							}, nil
						}
						if s == "CheckResource" {
							return func(ch chan string) error {
								return errors.New("test")
							}, nil
						}
						return func() {}, nil
					}),
				})
				return patches
			}},

		{
			name:                  "case5 createResource error",
			args:                  args{handler: &VpcHandler{}},
			wantErr:               true,
			wantInitControllerErr: true,
			wantCheckResourceErr:  false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "InitController" {
							return func() error {
								return nil
							}, nil
						}
						if s == "CheckResource" {
							return func(ch chan string) error {
								return nil
							}, nil
						}
						if s == "CreateResource" {
							return func(request []byte) ([]byte, error) {
								return nil, errors.New("test")
							}, errors.New("test")
						}
						return func() {}, nil
					}),
				})
				return patches
			}},

		{
			name:                  "case6 deleteResource error",
			args:                  args{handler: &VpcHandler{}},
			wantErr:               true,
			wantInitControllerErr: true,
			wantCheckResourceErr:  false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*plugin.Plugin).Lookup, func(f *plugin.Plugin, s string) (plugin.Symbol, error) {
						if s == "InitController" {
							return func() error {
								return nil
							}, nil
						}
						if s == "CheckResource" {
							return func(ch chan string) error {
								return nil
							}, nil
						}
						if s == "CreateResource" {
							return func(request []byte) ([]byte, error) {
								return []byte{}, nil
							}, nil
						}
						if s == "DeleteResource" {
							return func(string2 string) error {
								return errors.New("test")
							}, errors.New("test")
						}
						return func() {}, nil
					}),
				})
				return patches
			}},
	}
	pluginVPC := PluginVPC{}
	pluginVPC.Plugin = &plugin.Plugin{}
	ch := make(chan string)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := pluginVPC.initController(); (err != nil) != tt.wantInitControllerErr {
				t.Errorf("initControllerAndCheckResource() error = %v, wantInitControllerErr %v", err, tt.wantInitControllerErr)
			}
			if err := pluginVPC.checkResource(ch); (err != nil) != tt.wantCheckResourceErr {
				t.Errorf("initControllerAndCheckResource() error = %v, wantCheckResourceErr %v", err, tt.wantCheckResourceErr)
			}
			patches.ResetAll()
		})
	}
}

func TestPluginVPC_CreateVpcResource(t *testing.T) {
	type args struct {
		traceID        string
		vpcType        int
		service        *faasfunction.FunctionService
		needInit       bool
		needNewVersion bool
		createResource interface{}
	}
	tests := []struct {
		name        string
		args        args
		want        []types.NATConfigure
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 Marshal error",
			args{
				createResource: func(request []byte) ([]byte, error) {
					return []byte{}, nil
				},
			},
			nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(describeRequest,
						func(traceID string, service *faasfunction.FunctionService, vpcType int, needInit, needNewVersion bool) config.VpcControllerRequester {
							return config.VpcControllerRequester{}
						}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("failed to marshel json")
					}),
				})
				return patches
			}},

		{"case2 createResource error",
			args{
				createResource: func(request []byte) ([]byte, error) {
					return nil, errors.New("test")
				},
			},
			nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(describeRequest,
						func(traceID string, service *faasfunction.FunctionService, vpcType int, needInit, needNewVersion bool) config.VpcControllerRequester {
							return config.VpcControllerRequester{}
						}),
				})
				return patches
			}},

		{"case3 UnMarshal error",
			args{
				createResource: func(request []byte) ([]byte, error) {
					return []byte{}, nil
				},
			},
			nil, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(describeRequest,
						func(traceID string, service *faasfunction.FunctionService, vpcType int, needInit, needNewVersion bool) config.VpcControllerRequester {
							return config.VpcControllerRequester{}
						}),
				})
				return patches
			}},

		{"case4 succeed to createVpcResource",
			args{
				createResource: func(request []byte) ([]byte, error) {
					tmp, _ := json.Marshal([]types.NATConfigure{})
					return tmp, nil
				},
				service: &faasfunction.FunctionService{},
			},
			[]types.NATConfigure{}, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(describeRequest,
						func(traceID string, service *faasfunction.FunctionService, vpcType int, needInit, needNewVersion bool) config.VpcControllerRequester {
							return config.VpcControllerRequester{}
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pluginVPC := PluginVPC{}
			pluginVPC.Plugin = &plugin.Plugin{}
			pluginVPC.createResource = tt.args.createResource
			patches := tt.patchesFunc()
			got, err := pluginVPC.createVpcResource(tt.args.service, tt.args.traceID, tt.args.vpcType, tt.args.needInit, tt.args.needNewVersion)
			if (err != nil) != tt.wantErr {
				t.Errorf("createVpcResource() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createVpcResource() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})

	}
}

func TestPluginVPC_becomeVpcLeader(t *testing.T) {
	type fields struct {
		Plugin         *plugin.Plugin
		becomeLeader   plugin.Symbol
		createResource plugin.Symbol
		deleteResource plugin.Symbol
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{
			name: "test success",
			fields: fields{
				Plugin:       &plugin.Plugin{},
				becomeLeader: func() error { return nil },
			},
			wantErr: false,
		},
		{
			name: "test failed with plugin is nil",
			fields: fields{
				Plugin: nil,
			},
			wantErr: true,
		},
		{
			name: "test failed with become leader assert error",
			fields: fields{
				Plugin:       &plugin.Plugin{},
				becomeLeader: func(test string) error { return nil },
			},
			wantErr: true,
		},
		{
			name: "test failed with become leader return error",
			fields: fields{
				Plugin:       &plugin.Plugin{},
				becomeLeader: func() error { return errors.New("mock become leader error") },
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PluginVPC{
				Plugin:         tt.fields.Plugin,
				becomeLeader:   tt.fields.becomeLeader,
				createResource: tt.fields.createResource,
				deleteResource: tt.fields.deleteResource,
			}
			if err := p.becomeVpcLeader(); (err != nil) != tt.wantErr {
				t.Errorf("becomeVpcLeader() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
