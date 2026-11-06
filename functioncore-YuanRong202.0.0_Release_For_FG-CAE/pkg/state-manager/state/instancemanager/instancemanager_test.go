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

// Package instance manager implement stateID to nodeID binding, write the relationship to etcd, and ensure consistency
package instancemanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/go-redis/redis/v8"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/state-manager/config"
	"functioncore/pkg/state-manager/resourcemanager"
	"functioncore/pkg/state-manager/scheduler"
	"functioncore/pkg/state-manager/state/counter"
	stateUtil "functioncore/pkg/state-manager/utils"
	"functioncore/test/utils"
)

type snError struct{}

func (snError) Code() int {
	return 400
}

func (snError) Error() string {
	return "error"
}

func TestCreateStateID(t *testing.T) {
	called := false
	test := []struct {
		name        string
		w           http.ResponseWriter
		r           *http.Request
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test parseRequest and errorResponse error
		{
			"case1",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{}, errors.New("parseRequest error")
					}),
					gomonkey.ApplyFunc(stateUtil.ErrorResponse, func(w http.ResponseWriter, code int, message string) error {
						return errors.New("response err")
					}),
				})
				return patches
			},
			true,
		},
		// test parseRequest but errorResponse success
		{
			"case2",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{}, errors.New("parseRequest error")
					}),
					gomonkey.ApplyFunc(stateUtil.ErrorResponse, func(w http.ResponseWriter, code int, message string) error {
						return nil
					}),
				})
				return patches
			},
			true,
		},
		// test parseRequest success but authCheck err
		{
			"case3",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						called = true
						return errors.New("")
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						return &stateUtil.Request{}, nil
					}),
					gomonkey.ApplyFunc(createStateErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
			true,
		},
		// test authCheck success
		{
			"case4",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{}, nil
					}),
					gomonkey.ApplyFunc(createStateErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
				})
				return patches
			},
			true,
		},
		// test func Incr error
		{
			"case5",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{
							FunctionName: "0@1",
						}, nil
					}),
					gomonkey.ApplyFunc(createStateErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
					gomonkey.ApplyFunc(counter.Incr, func(tenantID string) error {
						return errors.New("incr error")
					}),
				})
				return patches
			},
			true,
		},
		// test func Incr error
		{
			"case6",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{
							FunctionName: "0@1",
						}, nil
					}),
					gomonkey.ApplyFunc(createStateErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
					gomonkey.ApplyFunc(counter.Incr, func(tenantID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(bindStateIDToNodeID,
						func(request *stateUtil.Request) (string, string, error) {
							return "", "", nil
						}),
					gomonkey.ApplyFunc(successResponse, func(w http.ResponseWriter, stateID string, nodeID string) error {
						return nil
					}),
				})
				return patches
			},
			true,
		},
		// test successResponse error
		{
			"case7",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{
							FunctionName: "0@1",
						}, nil
					}),
					gomonkey.ApplyFunc(createStateErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
					gomonkey.ApplyFunc(counter.Incr, func(tenantID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(bindStateIDToNodeID,
						func(request *stateUtil.Request) (string, string, error) {
							return "", "", nil
						}),
					gomonkey.ApplyFunc(successResponse, func(w http.ResponseWriter, stateID string, nodeID string) error {
						return errors.New("success response error")
					}),
				})
				return patches
			},
			true,
		},
		// test bindStateIDToNodeID error
		{
			"case8",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{
							FunctionName: "0@1",
						}, nil
					}),
					gomonkey.ApplyFunc(createStateErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
					gomonkey.ApplyFunc(counter.Incr, func(tenantID string) error {
						return nil
					}),
					gomonkey.ApplyFunc(bindStateIDToNodeID,
						func(request *stateUtil.Request) (string, string, error) {
							return "", "", errors.New("bindStateIDToNodeID error")
						}),
					gomonkey.ApplyFunc(successResponse, func(w http.ResponseWriter, stateID string, nodeID string) error {
						return errors.New("success response error")
					}),
					gomonkey.ApplyFunc(counter.Decr, func(tenantID string) {
						return
					}),
				})
				return patches
			},
			true,
		},
		// test incr error
		{
			"case9",
			utils.MockResponseWriter{},
			&http.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{
							FunctionName: "0@1",
						}, nil
					}),
					gomonkey.ApplyFunc(createStateErrorResponse,
						func(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
							return
						}),
					gomonkey.ApplyFunc(counter.Incr, func(tenantID string) error {
						return &snError{}
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range test {
		fmt.Println(tt.name)
		called = false
		patches := tt.patchesFunc()
		createStateIDHandler(tt.w, tt.r)
		if called != tt.wantErr {
			t.Errorf("test create state id  %s error, err %v , wantErr %v", tt.name, called, tt.wantErr)
		}
		patches.ResetAll()
	}
}

func TestDeleteStateID(t *testing.T) {
	StateCacheMap2 := sync.Map{}
	StateCacheMap2.Store("test", "test")

	test := []struct {
		name         string
		req          *stateUtil.Request
		patchesFunc  utils.PatchesFunc
		stateHandler *StateHandler
		wantErr      bool
	}{
		// test stateHandler == nil
		{
			"case1",
			&stateUtil.Request{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			nil,
			true,
		},
		// test stateHandler
		{
			"case2",
			&stateUtil.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetETCDPath,
						func(tenantID, proxyFunction, stateID string) string {
							return "t"
						}),
				})
				return patches
			},
			&StateHandler{
				StateCacheMap: StateCacheMap2,
			},
			false,
		},
		// test date in map
		{
			"case3",
			&stateUtil.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetETCDPath,
						func(tenantID, proxyFunction, stateID string) string {
							return "test"
						}),
					gomonkey.ApplyFunc(deleteStateInEtcdAndCache, func(deleteStateIDKey string) error {
						return errors.New("deleteStateInEtcdAndCache error")
					}),
				})
				return patches
			},
			&StateHandler{
				StateCacheMap: StateCacheMap2,
			},
			true,
		},
		// test date in map
		{
			"case4",
			&stateUtil.Request{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetETCDPath,
						func(tenantID, proxyFunction, stateID string) string {
							return "test"
						}),
					gomonkey.ApplyFunc(deleteStateInEtcdAndCache, func(deleteStateIDKey string) error {
						return nil
					}),
				})
				return patches
			},
			&StateHandler{
				StateCacheMap: StateCacheMap2,
			},
			false,
		},
	}
	for _, tt := range test {
		patches := tt.patchesFunc()
		temp := stateHandler
		stateHandler = tt.stateHandler
		err := deleteStateID(tt.req)
		if (err != nil) != tt.wantErr {
			t.Errorf("process delete event %s error, err %v , wantErr %v", tt.name, err, tt.wantErr)
		}
		stateHandler = temp
		patches.ResetAll()
	}
}

func Test_isStateIDKey(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "test1",
			args: args{key: ""},
			want: false,
		},
		{
			name: "test2",
			args: args{key: "/sn/stateworkers/business/<businessID>/tenant" +
				"/<tenantID>/function/<functiontask>/version2/<version>/<az>/<stateID>"},
			want: false,
		},
		{
			name: "test3",
			args: args{key: "/sn/stateworkers/business/<businessID>/tenant" +
				"/<tenantID>/function/<functiontask>/version/<version>/<az>/<stateID>"},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStateIDKey(tt.args.key); got != tt.want {
				t.Errorf("isStateIDKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateHandler_initStateCacheMap(t *testing.T) {
	type fields struct {
		StateEtcd     *stateUtil.StateEtcd
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test get response error
		{
			"case1",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return nil, errors.New("get response error")
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test json unmarshal error
		{
			"case2",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{
								Kvs: []*mvccpb.KeyValue{
									{
										Key: []byte("/a/stateworkers/a/a/tenant/a/function/a/version/a/a/a"),
									},
									{
										Key: []byte("/a/"),
									},
								},
							}, nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("json unmarshal error")
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
		// test parseFrom error
		{
			"case3",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{
								Kvs: []*mvccpb.KeyValue{
									{
										Key: []byte("/a/stateworkers/a/a/tenant/a/function/a/version/a/a/a"),
									},
									{
										Key: []byte("/a/"),
									},
								},
							}, nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcdkey.StateWorkersKey{}),
						"ParseFrom", func(_ *etcdkey.StateWorkersKey, key string) error {
							return errors.New("parseFrom error")
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case4",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{
								Kvs: []*mvccpb.KeyValue{
									{
										Key: []byte("/a/stateworkers/a/a/tenant/a/function/a/version/a/a/a"),
									},
									{
										Key: []byte("/a/"),
									},
								},
							}, nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
		// test delete error
		{
			"case5",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{
								Kvs: []*mvccpb.KeyValue{
									{
										Key:   []byte("/a/stateworkers/a/a/tenant/a/function/a/version/a/a/a"),
										Value: []byte("{\"a\":\"a\"}"),
									},
									{
										Key: []byte("/a/"),
									},
								},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, opts ...clientv3.OpOption) error {
							return errors.New("delete error")
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test all
		{
			"case6",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{
								Kvs: []*mvccpb.KeyValue{
									{
										Key:   []byte("/a/stateworkers/a/a/tenant/a/function/a/version/a/a/a"),
										Value: []byte("{\"a\":\"a\"}"),
									},
									{
										Key: []byte("/a/"),
									},
								},
							}, nil
						}),
					gomonkey.ApplyFunc(scheduler.IsAvailableNode, func(nodeID string) bool {
						return true
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
		// test get meta client error
		{
			"case7",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return nil, errors.New("get response error")
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
			false,
		},
		// test delete success
		{
			"case8",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{
								Kvs: []*mvccpb.KeyValue{
									{
										Key:   []byte("/a/stateworkers/a/a/tenant/a/function/a/version/a/a/a"),
										Value: []byte("{\"a\":\"a\"}"),
									},
									{
										Key: []byte("/a/"),
									},
								},
							}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Delete", func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateHandler{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
				StateCacheMap: tt.fields.StateCacheMap,
				StateLockMap:  tt.fields.StateLockMap,
			}
			patches := tt.patchesFunc()
			if err := s.initStateCacheMap(); (err != nil) != tt.wantErr {
				t.Errorf("initStateCacheMap() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_filterStateID(t *testing.T) {
	type args struct {
		handler *StateHandler
		request *stateUtil.Request
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test stateId == "0"
		{
			"case1",
			args{
				handler: &StateHandler{
					StateLockMap: sync.Map{},
				},
				request: &stateUtil.Request{StateID: "0"},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test stateId not eq "0"
		{
			"case2",
			args{
				handler: &StateHandler{
					StateLockMap: sync.Map{},
				},
				request: &stateUtil.Request{StateID: "test"},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := filterStateID(tt.args.handler, tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("filterStateID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_deleteStateIDHandler(t *testing.T) {
	called := false
	type args struct {
		w http.ResponseWriter
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		// test parseRequest error
		{
			"case1",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return nil, errors.New("parseRequest error")
					}),
					gomonkey.ApplyFunc(stateUtil.ErrorResponse, func(w http.ResponseWriter, code int, message string) error {
						return errors.New("errorResponse error")
					}),
				})
				return patches
			},
			true,
		},
		// test authCheck error
		{
			"case2",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{}, nil
					}),
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return errors.New("authCheck error")
					}),
					gomonkey.ApplyFunc(stateUtil.ErrorResponse, func(w http.ResponseWriter, code int, message string) error {
						return errors.New("errorResponse error")
					}),
				})
				return patches
			},
			true,
		},
		// test deleteStateID error
		{
			"case3",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{}, nil
					}),
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(deleteStateID, func(req *stateUtil.Request) error {
						return errors.New("deleteStateID error")
					}),
					gomonkey.ApplyFunc(stateUtil.ErrorResponse, func(w http.ResponseWriter, code int, message string) error {
						return errors.New("errorResponse error")
					}),
				})
				return patches
			},
			true,
		},
		// test successResponse error
		{
			"case4",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{}, nil
					}),
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(deleteStateID, func(req *stateUtil.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(counter.Decr, func(tenantID string) {
						return
					}),
					gomonkey.ApplyFunc(successResponse,
						func(w http.ResponseWriter, stateID string, nodeID string) error {
							return errors.New("successResponse error")
						}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case5",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(parseRequest, func(r *http.Request) (*stateUtil.Request, error) {
						called = true
						return &stateUtil.Request{}, nil
					}),
					gomonkey.ApplyFunc(stateUtil.AuthCheck, func(r *http.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(deleteStateID, func(req *stateUtil.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(counter.Decr, func(tenantID string) {
						return
					}),
					gomonkey.ApplyFunc(successResponse,
						func(w http.ResponseWriter, stateID string, nodeID string) error {
							return nil
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
			patches := tt.patchesFunc()
			deleteStateIDHandler(tt.args.w, tt.args.r)
			if called != tt.calledWant {
				t.Errorf("deleteStateIDHandler() called = %v, want %v", called, tt.calledWant)
			}
			patches.ResetAll()
		})
	}
}

func Test_parseRequest(t *testing.T) {
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test empty http request
		{
			"case1",
			args{
				&http.Request{},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test has content-type but not right value
		{
			"case2",
			args{
				&http.Request{
					Header: http.Header{
						"Content-Type": []string{"application"},
					},
				},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test has content-type and right
		{
			"case3",
			args{
				&http.Request{
					Header: http.Header{
						"Content-Type": []string{"application/json"},
					},
					Body: utils.ReadCloserImp{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return nil, errors.New("read all error")
					}),
				})
				return patches
			},
			true,
		},
		// test read all success
		{
			"case4",
			args{
				&http.Request{
					Header: http.Header{
						"Content-Type": []string{"application/json"},
					},
					Body: utils.ReadCloserImp{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte("{\"asd\":}"), nil
					}),
				})
				return patches
			},
			true,
		},
		// test ValidateStruct error
		{
			"case6",
			args{
				&http.Request{
					Header: http.Header{
						"Content-Type": []string{"application/json"},
					},
					Body: utils.ReadCloserImp{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte("{\"functionName\":\"test\"}"), nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, errors.New("ValidateStruct error")
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case7",
			args{
				&http.Request{
					Header: http.Header{
						"Content-Type": []string{"application/json"},
					},
					Body: utils.ReadCloserImp{},
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
						return []byte("{\"functionName\":\"test\"}"), nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := parseRequest(tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func TestStateHandler_RunWatcher(t *testing.T) {
	called := false
	type fields struct {
		StateEtcd     *stateUtil.StateEtcd
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	type args struct {
		watchChan *etcd3.EtcdWatchChan
		stopCh    chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		willDoFunc  func(chan struct{})
		calledWant  bool
	}{
		// test nil stopChan
		{
			"case1",
			fields{},
			args{
				stopCh: nil,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			func(stopCh chan struct{}) {
				go func() {
					if stopCh != nil {
						<-time.After(time.Microsecond)
						close(stopCh)
					}
				}()
			},
			false,
		},
		// test close chan
		{
			"case2",
			fields{},
			args{
				stopCh: make(chan struct{}, 1),
				watchChan: &etcd3.EtcdWatchChan{
					ResultChan: make(chan *etcd3.Event),
				},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			func(stopChan chan struct{}) {
				go func() {
					<-time.After(time.Microsecond)
					close(stopChan)
				}()
			},
			false,
		},
		// test etcd called
		{
			"case3",
			fields{},
			args{
				stopCh: make(chan struct{}, 1),
				watchChan: &etcd3.EtcdWatchChan{
					ResultChan: make(chan *etcd3.Event),
				},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			func(stopChan chan struct{}) {
				go func() {
					<-time.After(time.Microsecond)
					close(stopChan)
				}()
			},
			false,
		},
		// test etcdChan closed
		{
			"case4",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{Client: &clientv3.Client{}},
				},
			},
			args{
				stopCh: make(chan struct{}, 1),
				watchChan: &etcd3.EtcdWatchChan{
					ResultChan: make(chan *etcd3.Event),
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
				})
				return patches
			},
			func(stopChan chan struct{}) {
				go func() {
					<-time.After(time.Microsecond)
					close(stopChan)
				}()
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateHandler{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{Client: &clientv3.Client{}},
				},
				StateCacheMap: tt.fields.StateCacheMap,
				StateLockMap:  tt.fields.StateLockMap,
			}
			called = false
			patches := tt.patchesFunc()
			tt.willDoFunc(tt.args.stopCh)
			s.RunWatcher(tt.args.stopCh)
			if called != tt.calledWant {
				t.Errorf("RunWatcher() called = %v, want %v", called, tt.calledWant)
			}
			patches.ResetAll()
		})
	}
}

func Test_successResponse(t *testing.T) {
	type args struct {
		w       http.ResponseWriter
		stateID string
		nodeID  string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"case1",
			args{
				w: utils.MockResponseWriter{},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := successResponse(tt.args.w, tt.args.stateID, tt.args.nodeID); (err != nil) != tt.wantErr {
				t.Errorf("successResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDeleteStateMappingByNode(t *testing.T) {
	called := false
	type args struct {
		params interface{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		// test not a string
		{
			"case1",
			args{
				params: nil,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		// test deleteStateInEtcdAndCache error
		{
			"case2",
			args{
				params: "params",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(deleteStateInEtcdAndCache, func(deleteStateIDKey string) error {
						called = true
						return errors.New("deleteStateInEtcdAndCache error")
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case3",
			args{
				params: "params",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(deleteStateInEtcdAndCache, func(deleteStateIDKey string) error {
						called = true
						return nil
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := stateHandler
			stateHandler = &StateHandler{
				StateCacheMap: sync.Map{},
			}
			storeMap := make(map[string]string)
			storeMap["params"] = "params"
			stateHandler.StateCacheMap.Store("test1", storeMap)
			called = false
			patches := tt.patchesFunc()
			DeleteStateMappingByNode(tt.args.params)
			if called != tt.calledWant {
				t.Errorf("DeleteStateMappingByNode() called = %v, want %v", called, tt.calledWant)
			}
			patches.ResetAll()
			stateHandler = temp
		})
	}
}

func TestInitialize(t *testing.T) {
	type args struct {
		stopChan <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test GetStateHandler error
		{
			"case1",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						return &StateHandler{}, errors.New("GetStateHandler error")
					}),
				})
				return patches
			},
			true,
		},
		// test initStateCacheMap error
		{
			"case2",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						return &StateHandler{
							StateEtcd: &stateUtil.StateEtcd{
								EtcdClt: &etcd3.EtcdClient{},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(etcd3.CreateEtcdCtxInfoWithTimeout,
						func(ctx context.Context, duration time.Duration) etcd3.EtcdCtxInfo {
							return etcd3.EtcdCtxInfo{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return nil, errors.New("GetResponse error")
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case3",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						return &StateHandler{
							StateEtcd: &stateUtil.StateEtcd{
								EtcdClt: &etcd3.EtcdClient{},
							},
						}, nil
					}),
					gomonkey.ApplyFunc(etcd3.CreateEtcdCtxInfoWithTimeout,
						func(ctx context.Context, duration time.Duration) etcd3.EtcdCtxInfo {
							return etcd3.EtcdCtxInfo{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Watch",
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
					gomonkey.ApplyFunc(resourcemanager.RegisterNodeEvent,
						func(event resourcemanager.NodeEvent, callback func(interface{})) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := Initialize(tt.args.stopChan); (err != nil) != tt.wantErr {
				t.Errorf("Initialize() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestStateHandler_updateCacheAndEtcd(t *testing.T) {
	type fields struct {
		StateEtcd     *stateUtil.StateEtcd
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	type args struct {
		request *stateUtil.Request
		nodeID  string
		key     string
		creator string
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test json marshal error
		{
			"case1",
			fields{},
			args{request: &stateUtil.Request{}},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("json marshal error")
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test put error
		{
			"case2",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			args{request: &stateUtil.Request{}},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
							return errors.New("put error")
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case3",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			args{request: &stateUtil.Request{}},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
		// test get client error
		{
			"case4",
			fields{
				StateEtcd: &stateUtil.StateEtcd{
					EtcdClt: &etcd3.EtcdClient{},
				},
			},
			args{request: &stateUtil.Request{}},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateHandler{
				StateEtcd:     tt.fields.StateEtcd,
				StateCacheMap: tt.fields.StateCacheMap,
				StateLockMap:  tt.fields.StateLockMap,
			}
			patches := tt.patchesFunc()
			if err := s.updateCacheAndEtcd(tt.args.request, tt.args.nodeID, tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("updateCacheAndEtcd() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestStateHandler_handleDeleteEvent(t *testing.T) {
	cacheMap1 := sync.Map{}
	cacheMap1.Store("test", "test")

	cacheMap3 := sync.Map{}
	cacheMap3.Store("test", &sync.Mutex{})

	type fields struct {
		StateEtcd     *stateUtil.StateEtcd
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	type args struct {
		event *etcd3.Event
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		// test not exist
		{
			"case1",
			fields{
				StateLockMap: cacheMap1,
			},
			args{
				&etcd3.Event{Key: "test1"},
			},
		},
		// test in map
		{
			"case2",
			fields{
				StateLockMap: cacheMap1,
			},
			args{
				&etcd3.Event{Key: "test"},
			},
		},
		// test is sync.mutex
		{
			"case3",
			fields{
				StateLockMap: cacheMap3,
			},
			args{
				&etcd3.Event{Key: "test"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateHandler{
				StateEtcd:     tt.fields.StateEtcd,
				StateCacheMap: tt.fields.StateCacheMap,
				StateLockMap:  tt.fields.StateLockMap,
			}
			s.handleDeleteEvent(tt.args.event)
			if stateLock, ok := s.StateLockMap.Load(tt.args.event.Key); ok {
				_, ok = stateLock.(*sync.Mutex)
				if ok {
					t.Errorf("handleDeleteEvent() error")
				}
			}
		})
	}
}

func Test_updateCacheAndEtcd(t *testing.T) {
	type args struct {
		handler           *StateHandler
		request           *stateUtil.Request
		etcdPath          string
		createStateLogger log.FormatLogger
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test GetPriorityNode error
		{
			"case1",
			args{
				request: &stateUtil.Request{
					FunctionName: "test",
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(scheduler.GetPriorityNode, func(funcName string) (string, error) {
						return "", errors.New("GetPriorityNode error")
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test json marshal error
		{
			"case2",
			args{
				request: &stateUtil.Request{
					FunctionName: "test",
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(scheduler.GetPriorityNode, func(funcName string) (string, error) {
						return "", nil
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return nil, errors.New("json marshal error")
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		{
			"case3",
			args{
				request: &stateUtil.Request{
					FunctionName: "test",
				},
				handler: &StateHandler{
					StateEtcd: &stateUtil.StateEtcd{
						EtcdClt: &etcd3.EtcdClient{},
					},
				},
				createStateLogger: log.GetLogger(),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(scheduler.GetPriorityNode, func(funcName string) (string, error) {
						return "", nil
					}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte(""), nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, _, err := updateCacheAndEtcd(tt.args.handler, tt.args.request, tt.args.etcdPath, tt.args.createStateLogger)
			if (err != nil) != tt.wantErr {
				t.Errorf("updateCacheAndEtcd() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_authCheck(t *testing.T) {
	type args struct {
		r *http.Request
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test AuthCheckLocally error
		{
			"case1",
			args{
				&http.Request{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							return errors.New("AuthCheckLocally error")
						}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case2",
			args{
				&http.Request{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(localauth.AuthCheckLocally,
						func(ak string, sk string, requestSign string, timestamp string, duration int) error {
							return nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := config.FConfigs
			config.FConfigs = &config.FConfig{
				LocalAuth: localauth.AuthConfig{
					AKey: "test",
					SKey: "test",
				},
			}
			patches := tt.patchesFunc()
			if err := stateUtil.AuthCheck(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("authCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
			config.FConfigs = temp
		})
	}
}

func Test_errorResponse(t *testing.T) {
	type args struct {
		w       http.ResponseWriter
		code    int
		message string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// test success
		{
			"case1",
			args{
				w:       utils.MockResponseWriter{},
				code:    200,
				message: "message",
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := stateUtil.ErrorResponse(tt.args.w, tt.args.code, tt.args.message); (err != nil) != tt.wantErr {
				t.Errorf("errorResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGetStateHandler(t *testing.T) {
	tests := []struct {
		name    string
		want    *StateHandler
		wantErr bool
	}{
		{
			"case1",
			&StateHandler{},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := GetStateHandler()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetStateHandler() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_createStateErrorResponse(t *testing.T) {
	called := false
	type args struct {
		w                 http.ResponseWriter
		code              int
		message           string
		createStateLogger log.FormatLogger
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		calledWant  bool
	}{
		// test errorResponse error
		{
			"case1",
			args{
				createStateLogger: log.GetLogger(),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.ErrorResponse, func(w http.ResponseWriter, code int, message string) error {
						called = true
						return errors.New("errorResponse error")
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case2",
			args{
				createStateLogger: log.GetLogger(),
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.ErrorResponse, func(w http.ResponseWriter, code int, message string) error {
						called = true
						return nil
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
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
			patches := tt.patchesFunc()
			createStateErrorResponse(tt.args.w, tt.args.code, tt.args.message, tt.args.createStateLogger)
			if called != tt.calledWant {
				t.Errorf("createStateErrorResponse() called = %v, want %v", called, tt.calledWant)
			}
			patches.ResetAll()
		})
	}
}

func TestGetStateCacheMapNum(t *testing.T) {
	stateCacheMap1 := sync.Map{}
	stateCacheMap1.Store("test", "test")
	stateCacheMap1.Store("test2", "test2")
	tests := []struct {
		name         string
		stateHandler *StateHandler
		want         int
	}{
		// test stateHandler == nil
		{
			"case1",
			nil,
			-1,
		},
		// test !empty StateCacheMap
		{
			"case2",
			&StateHandler{StateCacheMap: stateCacheMap1},
			2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := stateHandler

			stateHandler = tt.stateHandler
			if got := GetStateCacheMapNum(); got != tt.want {
				t.Errorf("GetStateCacheMapNum() = %v, want %v", got, tt.want)
			}
			stateHandler = temp
		})
	}
}

func TestStateHandler_handlePutEvent(t *testing.T) {
	called := false
	type fields struct {
		StateEtcd     *stateUtil.StateEtcd
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	type args struct {
		event *etcd3.Event
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc utils.PatchesFunc
		args        args
		calledWant  bool
	}{
		// test error isStateIDKey false
		{
			"case1",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(isStateIDKey, func(key string) bool {
						called = true
						return false
					}),
				})
				return patches
			},
			args{
				event: &etcd3.Event{},
			},
			true,
		},
		// test json unmarshal error
		{
			"case2",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(isStateIDKey, func(key string) bool {
						called = true
						return true
					}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("json unmarshal error")
					}),
				})
				return patches
			},
			args{
				event: &etcd3.Event{},
			},
			true,
		},
		// test parse from error
		{
			"case3",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(isStateIDKey, func(key string) bool {
						called = true
						return true
					}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcdkey.StateWorkersKey{}), "ParseFrom",
						func(_ *etcdkey.StateWorkersKey, key string) error {
							return errors.New("parse from error")
						}),
				})
				return patches
			},
			args{
				event: &etcd3.Event{},
			},
			true,
		},
		// test empty map
		{
			"case4",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(isStateIDKey, func(key string) bool {
						called = true
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcdkey.StateWorkersKey{}), "ParseFrom",
						func(workerKey *etcdkey.StateWorkersKey, key string) error {
							workerKey.StateID = "test"
							return nil
						}),
				})
				return patches
			},
			args{
				event: &etcd3.Event{
					Value: []byte("{\"a\":\"test\"}"),
				},
			},
			true,
		},
		// test map not empty
		{
			"case5",
			fields{
				StateLockMap: sync.Map{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(isStateIDKey, func(key string) bool {
						called = true
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcdkey.StateWorkersKey{}), "ParseFrom",
						func(workerKey *etcdkey.StateWorkersKey, key string) error {
							workerKey.StateID = "test"
							return nil
						}),
				})
				return patches
			},
			args{
				event: &etcd3.Event{
					Value: []byte("{\"test\":\"test\"}"),
				},
			},
			true,
		},
		// test cluster is empty and cluster status is running, need to CompeteStateAndTryToUpdate
		{
			"case6",
			fields{
				StateLockMap: sync.Map{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(isStateIDKey, func(key string) bool {
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcdkey.StateWorkersKey{}), "ParseFrom",
						func(workerKey *etcdkey.StateWorkersKey, key string) error {
							workerKey.StateID = "test"
							return nil
						}),
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusRunning, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(CompeteStateAndTryToUpdate, func(stateID string, oldNodeID string) error {
						called = true
						return nil
					}),
				})
				return patches
			},
			args{
				event: &etcd3.Event{
					Value: []byte("{\"test\":\"test\"}"),
				},
			},
			true,
		},
		// test cluster is empty and cluster status is not running, no need to CompeteStateAndTryToUpdate
		{
			"case7",
			fields{
				StateLockMap: sync.Map{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(isStateIDKey, func(key string) bool {
						return true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcdkey.StateWorkersKey{}), "ParseFrom",
						func(workerKey *etcdkey.StateWorkersKey, key string) error {
							workerKey.StateID = "test"
							return nil
						}),
					gomonkey.ApplyFunc(clusterstatusutil.IsCurrentClustersStatusRunning, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(CompeteStateAndTryToUpdate, func(stateID string, oldNodeID string) error {
						called = true
						return nil
					}),
				})
				return patches
			},
			args{
				event: &etcd3.Event{
					Value: []byte("{\"test\":\"test\"}"),
				},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateHandler{
				StateEtcd:     tt.fields.StateEtcd,
				StateCacheMap: tt.fields.StateCacheMap,
				StateLockMap:  tt.fields.StateLockMap,
			}
			called = false
			patches := tt.patchesFunc()
			s.handlePutEvent(tt.args.event)
			if called != tt.calledWant {
				t.Errorf("handlePutEvent() = %v, want %v", called, tt.calledWant)
			}
			patches.ResetAll()
		})
	}
}

func Test_deleteStateInEtcdAndCache(t *testing.T) {
	type args struct {
		deleteStateIDKey string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test deleteStateIDKey = ""
		{
			"case1",
			args{
				deleteStateIDKey: "",
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test can't find state id
		{
			"case2",
			args{
				deleteStateIDKey: "test",
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		// test can find state id but type error
		{
			"case3",
			args{
				deleteStateIDKey: "test",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						syncMap := sync.Map{}
						syncMap.Store("test", "")
						return &StateHandler{
							StateLockMap: syncMap,
						}, nil
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test can find state id and type right
		{
			"case4",
			args{
				deleteStateIDKey: "test",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						syncMap := sync.Map{}
						syncMap.Store("test", &sync.Mutex{})
						return &StateHandler{
							StateLockMap: syncMap,
							StateEtcd: &stateUtil.StateEtcd{
								EtcdClt: &etcd3.EtcdClient{},
							},
						}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, opts ...clientv3.OpOption) error {
							return errors.New("etcd delete error")
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case5",
			args{
				deleteStateIDKey: "test",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						syncMap := sync.Map{}
						syncMap.Store("test", &sync.Mutex{})
						return &StateHandler{
							StateLockMap: syncMap,
							StateEtcd: &stateUtil.StateEtcd{
								EtcdClt: &etcd3.EtcdClient{},
							},
						}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			false,
		},
		// test get client error
		{
			"case5",
			args{
				deleteStateIDKey: "test",
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						syncMap := sync.Map{}
						syncMap.Store("test", &sync.Mutex{})
						return &StateHandler{
							StateLockMap: syncMap,
							StateEtcd: &stateUtil.StateEtcd{
								EtcdClt: &etcd3.EtcdClient{},
							},
						}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Delete",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, opts ...clientv3.OpOption) error {
							return nil
						}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := deleteStateInEtcdAndCache(tt.args.deleteStateIDKey); (err != nil) != tt.wantErr {
				t.Errorf("deleteStateInEtcdAndCache() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestDeleteStateIDHandlerByManager(t *testing.T) {
	type args struct {
		req *stateUtil.Request
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			args: args{
				&stateUtil.Request{},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(deleteStateID, func(req *stateUtil.Request) error {
						return errors.New("deleteStateID error")
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case2",
			args: args{
				&stateUtil.Request{},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(deleteStateID, func(req *stateUtil.Request) error {
						return nil
					}),
					gomonkey.ApplyFunc(counter.Decr, func(tenantID string) {
						return
					}),
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := DeleteStateIDHandlerByManager(tt.args.req); (err != nil) != tt.wantErr {
				t.Errorf("DeleteStateIDHandlerByManager() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestStateHandler_parseUserFunctionEvent(t *testing.T) {
	type fields struct {
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	type args struct {
		event *etcd3.Event
	}
	event1 := &etcd3.Event{
		Type:      etcd3.PUT,
		Key:       "",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	b := &StateHandler{
		StateEtcd:     nil,
		StateCacheMap: sync.Map{},
		StateLockMap:  sync.Map{},
	}
	b.parseStateWorkersEvent(event1)
	event2 := &etcd3.Event{
		Type:      etcd3.DELETE,
		Key:       "",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	b.parseStateWorkersEvent(event2)
	event3 := &etcd3.Event{
		Type:      etcd3.SYNCED,
		Key:       "",
		Value:     nil,
		PrevValue: nil,
		Rev:       0,
	}
	b.parseStateWorkersEvent(event3)
}

func Test_bindStateIDToNodeID(t *testing.T) {
	type args struct {
		request *stateUtil.Request
	}
	tests := []struct {
		name        string
		args        args
		want        string
		want1       string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			args: args{
				&stateUtil.Request{},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						return nil, errors.New("failed to get StateHandler")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case2",
			args: args{
				&stateUtil.Request{},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(GetStateHandler, func() (*StateHandler, error) {
						return nil, errors.New("failed to get StateHandler")
					}),
					gomonkey.ApplyFunc(filterStateID, func(handler *StateHandler,
						request *stateUtil.Request) (string, string, error) {
						return "", "", errors.New("filter StateID error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := bindStateIDToNodeID(tt.args.request)
			if (err != nil) != tt.wantErr {
				t.Errorf("bindStateIDToNodeID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_getStateIDFromETCDKey(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty stateID key",
			args: args{key: ""},
			want: "",
		},
		{
			name: "invalid stateID key(version not valid)",
			args: args{key: "/sn/stateworkers/business/<businessID>/tenant" +
				"/<tenantID>/function/<functiontask>/version2/<version>/<az>/<stateID>"},
			want: "",
		},
		{
			name: "valid stateID key",
			args: args{key: "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/6ea8202c-2b96-5a94-8324-664fd732b17b"},
			want: "6ea8202c-2b96-5a94-8324-664fd732b17b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getStateIDFromETCDKey(tt.args.key); got != tt.want {
				t.Errorf("getStateIDFromETCDKey() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_GetStateIDListOfCluster(t *testing.T) {
	stateCacheMapInvalid := sync.Map{}
	stateCacheMapInvalid.Store("test", "test")
	stateCacheMapInvalid.Store("/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/6ea8202c-2b96-5a94-8324-664fd732b17b", "test")

	stateCacheMapValid := sync.Map{}
	storeMap1 := make(map[string]string)
	storeMap1[constants.ClusterKey] = "cluster002"

	storeMap2 := make(map[string]string)
	storeMap2[constants.ClusterKey] = "cluster001"
	stateCacheMapValid.Store("/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/6ea8202c-2b96-5a94-8324-664fd732b17b", storeMap1)
	stateCacheMapValid.Store("/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/801cbf50-3c25-557a-882c-81a4ee47ca12", storeMap2)
	stateCacheMapValid.Store("/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/9b753fa0-0300-55e8-b3cc-05cac2ff8142", storeMap2)

	var expectedEmptyList []string
	type fields struct {
		StateEtcd     *stateUtil.StateEtcd
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc utils.PatchesFunc
		wanted      []string
	}{
		{
			"StateCacheMap is empty",
			fields{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster001"
					}),
				})
				return patches
			},
			expectedEmptyList,
		},
		{
			"StateCacheMap key or value is invalid",
			fields{
				StateCacheMap: stateCacheMapInvalid,
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster001"
					}),
				})
				return patches
			},
			expectedEmptyList,
		},
		{
			"Only cluster001 should be recorded",
			fields{StateCacheMap: stateCacheMapValid},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster001"
					}),
				})
				return patches
			},
			[]string{"801cbf50-3c25-557a-882c-81a4ee47ca12", "9b753fa0-0300-55e8-b3cc-05cac2ff8142"},
		},
		{
			"Only cluster002 should be recorded",
			fields{StateCacheMap: stateCacheMapValid},

			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster002"
					}),
				})
				return patches
			},
			[]string{"6ea8202c-2b96-5a94-8324-664fd732b17b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateHandler{
				StateEtcd:     tt.fields.StateEtcd,
				StateCacheMap: tt.fields.StateCacheMap,
				StateLockMap:  tt.fields.StateLockMap,
			}
			stateHandler = s
			patches := tt.patchesFunc()
			stateIDList := GetStateIDListOfCluster()
			sort.Strings(stateIDList)
			sort.Strings(tt.wanted)
			if !reflect.DeepEqual(tt.wanted, stateIDList) {
				t.Errorf("Expected %v but got %v", tt.wanted, stateIDList)
			}
			patches.ResetAll()
		})
	}
}

func Test_MigrateStateIDToOtherCluster(t *testing.T) {
	stateCacheMapValid := sync.Map{}

	storeMap := make(map[string]string)
	storeMap[constants.ClusterKey] = "cluster001"
	stateCacheMapValid.Store("/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/6ea8202c-2b96-5a94-8324-664fd732b17b", storeMap)
	stateCacheMapValid.Store("/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/801cbf50-3c25-557a-882c-81a4ee47ca12", storeMap)
	stateCacheMapValid.Store("/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/9b753fa0-0300-55e8-b3cc-05cac2ff8142", storeMap)

	type fields struct {
		StateEtcd     *stateUtil.StateEtcd
		StateCacheMap sync.Map
		StateLockMap  sync.Map
	}
	tests := []struct {
		name        string
		fields      fields
		args        string
		patchesFunc utils.PatchesFunc
		wanted      bool
	}{
		{
			"StateCacheMap is empty",
			fields{},
			"6ea8202c-2b96-5a94-8324-664fd732b17b",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster001"
					}),
				})
				return patches
			},
			false,
		},
		{
			"state id is not exist",
			fields{
				StateCacheMap: stateCacheMapValid,
			},
			"teststateID",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster001"
					}),
				})
				return patches
			},
			false,
		},
		{
			"Put etcd failed",
			fields{StateCacheMap: stateCacheMapValid},
			"6ea8202c-2b96-5a94-8324-664fd732b17b",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster001"
					}),
				})
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.PutStateWorkersToETCDWithRetry, func(key string, value []byte) error {
						return errors.New("Test error")
					}),
				})
				return patches
			},
			false,
		},
		{
			"Put etcd succeed",
			fields{StateCacheMap: stateCacheMapValid},
			"6ea8202c-2b96-5a94-8324-664fd732b17b",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetClusterID, func() string {
						return "cluster001"
					}),
				})
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.PutStateWorkersToETCDWithRetry, func(key string, value []byte) error {
						return nil
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateHandler{
				StateEtcd:     tt.fields.StateEtcd,
				StateCacheMap: tt.fields.StateCacheMap,
				StateLockMap:  tt.fields.StateLockMap,
			}
			stateHandler = s
			patches := tt.patchesFunc()
			if got := MigrateStateIDToOtherCluster(tt.args); got != tt.wanted {
				t.Errorf("MigrateStateIDToOtherCluster() = %v, want %v", got, tt.wanted)
			}
			patches.ResetAll()
		})
	}
}

func TestUpdateEtcdWithStateAndNode(t *testing.T) {
	stateRouteData := map[string]string{}
	stateRouteData["test"] = "node"
	value, _ := json.Marshal(stateRouteData)
	kvs := []*mvccpb.KeyValue{}
	kv := &mvccpb.KeyValue{
		Value: value,
	}
	kvs = append(kvs, kv)
	tests := []struct {
		name      string
		newNode   string
		state     string
		patchFunc utils.PatchesFunc
		wantErr   bool
	}{
		// test get error
		{
			"case1",
			"node",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return nil, errors.New("get error")
						}),
				})
				return patches
			},
			false,
		},
		// test marshal error
		{
			"case2",
			"node",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{Kvs: kvs}, nil
						}),
					gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
						return []byte("test"), errors.New("marshal error")
					}),
				})
				return patches
			},
			true,
		},
		// test put error
		{
			"case3",
			"node",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{Kvs: kvs}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, value string, opts ...clientv3.OpOption) error {
							return errors.New("put error")
						}),
				})
				return patches
			},
			false,
		},
		// test success
		{
			"case4",
			"node",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{Kvs: kvs}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			},
			false,
		},
		// test stateClient nil
		{
			"case5",
			"node",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{}
					}),
				})
				return patches
			},
			true,
		},
		// test success with empty response
		{
			"case6",
			"node",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"GetResponse",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
							opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return &clientv3.GetResponse{}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}),
						"Put",
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
							key string, value string, opts ...clientv3.OpOption) error {
							return nil
						}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			if err := UpdateEtcdWithStateAndNode(tt.newNode, tt.state); (err != nil) != tt.wantErr {
				t.Errorf("updateEtcdWithStateAndNode() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestCompeteStateAndTryToUpdate(t *testing.T) {
	mockCount := 0
	tests := []struct {
		name          string
		stateID       string
		oldNodeID     string
		patchesFunc   utils.PatchesFunc
		wantErr       bool
		wantMockCount int
	}{
		{
			"case1",
			"test",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return true, nil
						}),
					gomonkey.ApplyFunc(scheduler.GetPriorityNodeWithoutFuncName, func() (string, error) {
						return "test", nil
					}),
					gomonkey.ApplyFunc(UpdateEtcdWithStateAndNode, func(newNode string, state string) error {
						return errors.New("updateEtcdWithStateAndNode error")
					}),
					gomonkey.ApplyFunc(clearAlarmWhenUpdateStateIDNodeSucceed, func(stateID string, oldKey string) {
					}),
				})
				return patches
			},
			true,
			0,
		},
		{
			"Cluster can not find available node, need to migrate",
			"test",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del",
						func(_ *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return true, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 1, nil
						}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
						// delete OK, no need to sleep
						mockCount = 1
					}),
					gomonkey.ApplyFunc(scheduler.GetPriorityNodeWithoutFuncName, func() (string, error) {
						return "", errors.New("GetPriorityNodeWithoutFuncName error")
					}),
					gomonkey.ApplyFunc(MigrateStateIDToOtherCluster, func(stateID string) bool {
						return true
					}),
					gomonkey.ApplyFunc(clearAlarmWhenUpdateStateIDNodeSucceed, func(stateID string, oldKey string) {
					}),
				})
				return patches
			},
			false,
			0,
		},
		{
			"Cluster can not find available node, need to migrate and release redis lock failed",
			"test",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del",
						func(_ *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return true, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 1, errors.New("test error")
						}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
						// delete NOK, need to sleep
						mockCount = 100
					}),
					gomonkey.ApplyFunc(scheduler.GetPriorityNodeWithoutFuncName, func() (string, error) {
						return "", errors.New("GetPriorityNodeWithoutFuncName error")
					}),
					gomonkey.ApplyFunc(MigrateStateIDToOtherCluster, func(stateID string) bool {
						return true
					}),
				})
				return patches
			},
			false,
			100,
		},
		{
			"Cluster can not find available node when sendAlarmWhenFailedSeveralTimes return true",
			"test",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del",
						func(_ *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return true, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 1, nil
						}),
					gomonkey.ApplyFunc(scheduler.GetPriorityNodeWithoutFuncName, func() (string, error) {
						return "", errors.New("GetPriorityNodeWithoutFuncName error")
					}),
					gomonkey.ApplyFunc(sendAlarmWhenFailedSeveralTimes, func(stateID string, oldKey string) bool {
						return true
					}),
					gomonkey.ApplyFunc(MigrateStateIDToOtherCluster, func(stateID string) bool {
						mockCount = 1
						return true
					}),
				})
				return patches
			},
			false,
			0,
		},
		{
			"Cluster can not find available node when sendAlarmWhenFailedSeveralTimes return true",
			"test",
			"test",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(stateUtil.GetRedisCmd, func() *redisclient.Client {
						return &redisclient.Client{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX",
						func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
							return &redis.BoolCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del",
						func(_ *redisclient.Client, ctx context.Context, keys ...string) *redis.IntCmd {
							return &redis.IntCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
						"Result",
						func(_ *redis.BoolCmd) (bool, error) {
							return true, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}),
						"Result",
						func(_ *redis.IntCmd) (int64, error) {
							return 1, nil
						}),
					gomonkey.ApplyFunc(scheduler.GetPriorityNodeWithoutFuncName, func() (string, error) {
						return "", errors.New("GetPriorityNodeWithoutFuncName error")
					}),
					gomonkey.ApplyFunc(sendAlarmWhenFailedSeveralTimes, func(stateID string, oldKey string) bool {
						return false
					}),
					gomonkey.ApplyFunc(MigrateStateIDToOtherCluster, func(stateID string) bool {
						mockCount = 1
						return true
					}),
				})
				return patches
			},
			false,
			1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			mockCount = 0
			if err := CompeteStateAndTryToUpdate(tt.stateID, tt.oldNodeID); (err != nil) != tt.wantErr {
				t.Errorf("competeStateAndTryToUpdate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantMockCount != mockCount {
				t.Errorf("competeStateAndTryToUpdate() error")
			}
			patches.ResetAll()
		})
	}
}

func Test_ConstructTimerForCompetitionOfStateID(t *testing.T) {
	convey.Convey("Test_ConstructTimerForCompetitionOfStateID", t, func() {
		convey.Convey("Test timer timeout", func() {
			stateCompetitionTimeout = time.Second
			count := 0
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(CompeteStateAndTryToUpdate, func(stateID string, oldNodeID string) error {
					count += 1
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			ConstructTimerForCompetitionOfStateID("stateID", "nodeID")
			convey.So(count, convey.ShouldEqual, 1)
			convey.So(len(stateIDChannels), convey.ShouldEqual, 0)
		})
		convey.Convey("Test channel already exist", func() {
			// LLT will fail due to timeout
			stateCompetitionTimeout = time.Hour
			count := 0
			stateID := "stateID_exist"
			stateIDChannels[stateID] = make(chan bool)
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(CompeteStateAndTryToUpdate, func(stateID string, oldNodeID string) error {
					count += 1
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			ConstructTimerForCompetitionOfStateID(stateID, "nodeID")
			convey.So(count, convey.ShouldEqual, 0)
			convey.So(len(stateIDChannels), convey.ShouldEqual, 1)
			delete(stateIDChannels, stateID)
		})
	})
}
func Test_SendAlarmWhenFailedSeveralTimes(t *testing.T) {
	t.Run("failedCount is 0", func(t *testing.T) {
		sendAlarm := sendAlarmWhenFailedSeveralTimes("stateID_failCount_0", "oldKey")
		if sendAlarm {
			t.Errorf("sendAlarmWhenFailedSeveralTimes error")
		}
		stateIDWithoutNewNodeMap.Delete("stateID_failCount_0")
	})
	t.Run("failedCount is 3", func(t *testing.T) {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(stateUtil.GetRedisCmd, func() *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "SetNX",
				func(_ *redisclient.Client, ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
					return &redis.BoolCmd{}
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redis.BoolCmd{}),
				"Result",
				func(_ *redis.BoolCmd) (bool, error) {
					return true, nil
				}),
			gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
				func(a *alarm.Client, almType alarm.Type, specAddition string) {}),
		})
		sendAlarmWhenFailedSeveralTimes("stateID_failCount_1", "oldKey")
		sendAlarmWhenFailedSeveralTimes("stateID_failCount_1", "oldKey")
		sendAlarm := sendAlarmWhenFailedSeveralTimes("stateID_failCount_1", "oldKey")
		if !sendAlarm {
			t.Errorf("sendAlarmWhenFailedSeveralTimes error")
		}
		stateIDWithoutNewNodeMap.Delete("stateID_failCount_1")
	})
}
