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

package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"

	apiServerOptions "functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/common/cluster"
	"functioncore/pkg/common/localauth"
	mockUtils "functioncore/test/utils"
)

type MockResponseWriter struct{}

func (m MockResponseWriter) Header() http.Header {
	return nil
}

func (m MockResponseWriter) Write([]byte) (int, error) {
	return 0, nil
}

func (m MockResponseWriter) WriteHeader(int) {
	return
}

func newServer() *APIServerHandler {
	handler, _ := newAPIServerHandler()
	clusterDiscovery, _ := createClusterDiscovery(&apiServerOptions.APIServerOptions{}, nil, nil)
	handler.Discovery = clusterDiscovery
	apiRegistry := newAPIRegistry()
	handler.Registry = apiRegistry
	handler.LocalAuth = &localauth.AuthConfig{
		AKey:     "test",
		SKey:     "test",
		Duration: 1,
	}
	return handler
}

// TestNewAPIServerHandler Test New Api Server Handler
func TestNewAPIServerHandler(t *testing.T) {
	handler, _ := newAPIServerHandler()
	tests := []struct {
		name    string
		want    *APIServerHandler
		wantErr bool
	}{
		{"case0", handler, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newAPIServerHandler()
			if (err != nil) != tt.wantErr {
				t.Errorf("newAPIServerHandler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newAPIServerHandler() got value = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAPIServerHandler_GetRole Test Api Server Handler_GetRole
func TestAPIServerHandler_GetRole(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}

	handler := newServer()
	argvApi := args{
		w: MockResponseWriter{},
		r: nil,
	}

	tests := []struct {
		name        string
		a           *APIServerHandler
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1", handler, argvApi, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},

		{"case1", handler, args{
			w: httptest.NewRecorder(),
			r: nil,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.a.getRole(tt.args.w, tt.args.r)
			patches.ResetAll()
		})
	}
}

// TestAPIServerHandler_GetLeader Test Api Server Handler_GetLeader
func TestAPIServerHandler_GetLeader(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		a           *APIServerHandler
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 not found leader", newServer(), args{
			w: MockResponseWriter{},
			r: nil,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},

		{"case2 found leader", newServer(), args{
			w: MockResponseWriter{},
			r: nil,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Leader,
					func(_ *cluster.SingleNodeCluster) *cluster.LeaderInfo {
						return &cluster.LeaderInfo{}
					}),
			})
			return patches
		}},

		{"case3 not found leader, write error", newServer(), args{
			w: httptest.NewRecorder(),
			r: nil,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},

		{"case4 found leader, write error", newServer(), args{
			w: httptest.NewRecorder(),
			r: nil,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Leader,
					func(_ *cluster.SingleNodeCluster) *cluster.LeaderInfo {
						return &cluster.LeaderInfo{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
					func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
						return 0, errors.New("test")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			tt.a.getLeader(tt.args.w, tt.args.r)
			patches.ResetAll()
		})
	}
}

// TestAPIServerHandler_ServeHTTP Test Api Server Handler_ServeHTTP
func TestAPIServerHandler_ServeHTTP(t *testing.T) {
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}

	handler := newServer()
	handler.ForwardRequest = true
	requestURI := "/role"
	req, _ := http.NewRequest("GET", requestURI, nil)
	requestURI2 := "/"
	req2, _ := http.NewRequest("GET", requestURI2, nil)

	argvApi := args{
		w: MockResponseWriter{},
		r: req,
	}
	argvApi2 := args{
		w: MockResponseWriter{},
		r: req2,
	}

	tests := []struct {
		name        string
		a           *APIServerHandler
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to serve http, and path right", handler, argvApi,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*gin.Engine).ServeHTTP,
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							return
						}),
				})
				return patches
			}},
		{"case2 succeed to serve http, and auth check", handler, argvApi2,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*gin.Engine).ServeHTTP,
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							return
						}),
					gomonkey.ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string,
							timestamp string, duration int) error {
							return nil
						}),
				})
				return patches
			}},
		{"case3 failed to serve http, and auth check failed", handler, argvApi2,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*gin.Engine).ServeHTTP,
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							return
						}),
					gomonkey.ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string,
							timestamp string, duration int) error {
							return errors.New("test")
						}),
				})
				return patches
			}},
		{"case4 role not leader", handler, argvApi2,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*gin.Engine).ServeHTTP,
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							return
						}),
					gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Role,
						func(_ *cluster.SingleNodeCluster) string {
							return cluster.ClusterRoleNode
						}),
					gomonkey.ApplyFunc((*cluster.SingleNodeCluster).ForwardRequest,
						func(_ *cluster.SingleNodeCluster, w http.ResponseWriter, r *http.Request) {
							return
						}),
				})
				return patches
			}},
		{"case5 failed to serve http, and errorResponse error", handler, args{
			w: httptest.NewRecorder(),
			r: req2,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*gin.Engine).ServeHTTP,
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							return
						}),
					gomonkey.ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string,
							timestamp string, duration int) error {
							return errors.New("test")
						}),
					gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Role,
						func(_ *cluster.SingleNodeCluster) string {
							return cluster.ClusterRoleLeader
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&httptest.ResponseRecorder{}), "Write",
						func(_ *httptest.ResponseRecorder, buf []byte) (int, error) {
							return 0, errors.New("test")
						}),
				})
				return patches
			}},
		{"case6 failed to serve http, and errorResponse error", handler, args{
			w: httptest.NewRecorder(),
			r: req2,
		},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*gin.Engine).ServeHTTP,
						func(_ *gin.Engine, w http.ResponseWriter, req *http.Request) {
							return
						}),
					gomonkey.ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string,
							timestamp string, duration int) error {
							return errors.New("test")
						}),
					gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Role,
						func(_ *cluster.SingleNodeCluster) string {
							return cluster.ClusterRoleLeader
						}),
					gomonkey.ApplyFunc(json.Marshal,
						func(v interface{}) ([]byte, error) {
							return nil, errors.New("test")
						}),
				})
				return patches
			}},
	}

	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			tt.a.ServeHTTP(tt.args.w, tt.args.r)
		})
		patches.ResetAll()
	}
}
