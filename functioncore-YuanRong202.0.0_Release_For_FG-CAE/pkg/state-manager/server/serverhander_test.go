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

package server

import (
	ctls "crypto/tls"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/cluster"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/state-manager/config"
	"functioncore/test/utils"
)

func TestHandler_GetLeader(t *testing.T) {
	discovery := &cluster.ResourceLockCluster{}
	type fields struct {
		Registry       *gin.Engine
		Conf           *config.FConfig
		Discovery      cluster.Discovery
		Name           string
		AuthCheck      bool
		ForwardRequest bool
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
	}{
		// test leader != nil
		{
			"case1",
			fields{},
			args{
				w: utils.MockResponseWriter{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(discovery), "Leader",
						func(_ *cluster.ResourceLockCluster) *cluster.LeaderInfo {
							return &cluster.LeaderInfo{}
						}),
				})
				return patches
			},
		},
		// test leader == nil
		{
			"case2",
			fields{},
			args{
				w: utils.MockResponseWriter{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(discovery), "Leader",
						func(_ *cluster.ResourceLockCluster) *cluster.LeaderInfo {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			a := &Handler{
				Registry:       tt.fields.Registry,
				Conf:           tt.fields.Conf,
				Discovery:      discovery,
				Name:           tt.fields.Name,
				AuthCheck:      tt.fields.AuthCheck,
				ForwardRequest: tt.fields.ForwardRequest,
			}
			patches := tt.patchesFunc()
			a.GetLeader(tt.args.w, tt.args.r)
			patches.ResetAll()
		})
	}
}

func TestHandler_GetRole(t *testing.T) {
	called := true
	type fields struct {
		Registry       *gin.Engine
		Conf           *config.FConfig
		Discovery      cluster.Discovery
		Name           string
		AuthCheck      bool
		ForwardRequest bool
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		callWant    bool
	}{
		// test success
		{
			"case1",
			fields{},
			args{
				w: utils.MockResponseWriter{},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		// test write err
		{
			"case2",
			fields{},
			args{
				w: utils.MockResponseWriter{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&utils.MockResponseWriter{}),
						"Write", func(_ *utils.MockResponseWriter, _ []byte) (int, error) {
							called = true
							return 0, errors.New("write error")
						}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Handler{
				Registry:       tt.fields.Registry,
				Conf:           tt.fields.Conf,
				Discovery:      &cluster.ResourceLockCluster{},
				Name:           tt.fields.Name,
				AuthCheck:      tt.fields.AuthCheck,
				ForwardRequest: tt.fields.ForwardRequest,
			}
			called = false
			patches := tt.patchesFunc()
			a.GetRole(tt.args.w, tt.args.r)
			if called != tt.callWant {
				t.Errorf("TestHandler_GetRole failed")
			}
			patches.ResetAll()
		})
	}
}

func TestHandler_ServeHTTP(t *testing.T) {
	called := false
	type fields struct {
		Registry       *gin.Engine
		Conf           *config.FConfig
		Discovery      cluster.Discovery
		Name           string
		AuthCheck      bool
		ForwardRequest bool
	}
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		PatchesFunc utils.PatchesFunc
		callWant    bool
	}{
		// test run path in rule
		{
			"case1",
			fields{},
			args{
				r: &http.Request{
					URL: &url.URL{
						Path: "/role",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&gin.Engine{}), "ServeHTTP",
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							called = true
							return
						}),
				})
				return patches
			},
			true,
		},
		// test run ForwardRequest
		{
			"case2",
			fields{
				ForwardRequest: true,
			},
			args{
				r: &http.Request{
					URL: &url.URL{
						Path: "/role",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&cluster.ResourceLockCluster{}), "Role",
						func(_ *cluster.ResourceLockCluster) string {
							called = true
							return ""
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&gin.Engine{}), "ServeHTTP",
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							called = true
							return
						}),
				})
				return patches
			},
			true,
		},
		// test run success
		{
			"case3",
			fields{
				ForwardRequest: false,
			},
			args{
				r: &http.Request{
					URL: &url.URL{
						Path: "/role",
					},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&cluster.ResourceLockCluster{}), "Role",
						func(_ *cluster.ResourceLockCluster) string {
							called = true
							return cluster.ClusterRoleLeader
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&gin.Engine{}), "ServeHTTP",
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
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
			a := &Handler{
				Registry:       &gin.Engine{},
				Conf:           tt.fields.Conf,
				Discovery:      &cluster.ResourceLockCluster{},
				Name:           tt.fields.Name,
				AuthCheck:      tt.fields.AuthCheck,
				ForwardRequest: tt.fields.ForwardRequest,
			}
			patches := tt.PatchesFunc()
			a.ServeHTTP(tt.args.w, tt.args.r)
			if called != tt.callWant {
				t.Errorf("TestHandler_ServeHTTP failed")
			}
			patches.ResetAll()
		})
	}
}

func TestNewServerHandler(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		// test called
		{
			"case1",
			false,
		},
	}
	for _, tt := range tests {
		temp := handle
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewServerHandler()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewServerHandler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
		handle = temp
	}
}

func Test_createClusterDiscovery(t *testing.T) {
	type args struct {
		conf   *config.FConfig
		stopCh <-chan struct{}
		wg     *sync.WaitGroup
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// test empty config
		{
			"case1",
			args{conf: nil},
			true,
		},
		// test IsClusterMode = false
		{
			"case2",
			args{
				conf: &config.FConfig{
					Cluster: config.ClusterConfig{
						IsClusterMode: false,
					},
				},
			},
			false,
		},
		// test IsClusterMode && HTTPSEnable
		{
			"case3",
			args{
				conf: &config.FConfig{
					Cluster: config.ClusterConfig{
						IsClusterMode: true,
					},
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: true,
					},
				},
			},
			false,
		},
		// test !HTTPSEnable && IsClusterMode
		{
			"case4",
			args{
				conf: &config.FConfig{
					Cluster: config.ClusterConfig{
						IsClusterMode: true,
					},
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: false,
					},
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(cluster.NewResourceLockCluster,
					func(listenAddress string, stopCh <-chan struct{},
						resourceLockName string, tlsConfig *ctls.Config) (*cluster.ResourceLockCluster, error) {
						return &cluster.ResourceLockCluster{}, nil
					}),
			})
			_, err := createClusterDiscovery(tt.args.conf, tt.args.stopCh, tt.args.wg)
			if (err != nil) != tt.wantErr {
				t.Errorf("createClusterDiscovery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}
