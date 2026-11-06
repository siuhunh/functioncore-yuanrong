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

// Package utils implement tools of state-manager module
package utils

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/state-manager/config"
	"functioncore/test/utils"
)

func TestIsStateful(t *testing.T) {
	assert.Equal(t, true, IsStateful("0@123-test"))
	assert.Equal(t, false, IsStateful("caller"))
}

func TestGetServiceID(t *testing.T) {
	assert.Equal(t, "", GetServiceID("caller"))
	assert.Equal(t, "", GetServiceID("0@-caller"))
	assert.Equal(t, "snid", GetServiceID("0@snid@caller"))
}

func TestGetETCDPath(t *testing.T) {
	type args struct {
		tenantID      string
		proxyFunction string
		stateID       string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "test001",
			args: args{
				tenantID:      "0",
				proxyFunction: "function-task",
				stateID:       "stateID",
			},
			want: "/sn/stateworkers/business/yrk/tenant/0" +
				"/function/function-task/version/$latest/defaultaz/stateID",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetETCDPath(tt.args.tenantID, tt.args.proxyFunction, tt.args.stateID); got != tt.want {
				t.Errorf("GetETCDPath() = %v, want %v", got, tt.want)
			}
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
			if err := ErrorResponse(tt.args.w, tt.args.code, tt.args.message); (err != nil) != tt.wantErr {
				t.Errorf("errorResponse() error = %v, wantErr %v", err, tt.wantErr)
			}
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
			if err := AuthCheck(tt.args.r); (err != nil) != tt.wantErr {
				t.Errorf("authCheck() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
			config.FConfigs = temp
		})
	}
}

func Test_GetSNStateWorkerPath(t *testing.T) {
	tests := []struct {
		name    string
		stateID string
		want    string
	}{
		{
			"emptyStateID",
			"",
			"",
		},
		{
			"valid stateID",
			"6ea8202c-2b96-5a94-8324-664fd732b17b",
			"/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/6ea8202c-2b96-5a94-8324-664fd732b17b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if res := GetSNStateWorkerPath(tt.stateID); res != tt.want {
				t.Errorf("res %s != tt.want %s", res, tt.want)
			}
		})
	}
}

func Test_PutValueToETCDWithRetry(t *testing.T) {
	convey.Convey("TestGetValueFromMetaEtcd", t, func() {
		convey.Convey("client is nil", func() {
			patch := gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			err := PutStateWorkersToETCDWithRetry("testkey", []byte{})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(strings.Contains(err.Error(), "failed to init state ETCD client"), convey.ShouldBeTrue)
			patch.Reset()
		})
		convey.Convey("put etcd timeout", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
						return context.DeadlineExceeded
					}),
			})
			defer patches.ResetAll()
			err := PutStateWorkersToETCDWithRetry("testkey", []byte{})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(strings.Contains(err.Error(), "context deadline exceeded"), convey.ShouldBeTrue)
		})
		convey.Convey("put etcd error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
						return errors.New("Test error")
					}),
			})
			defer patches.ResetAll()
			err := PutStateWorkersToETCDWithRetry("testkey", []byte{})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(strings.Contains(err.Error(), "Test error"), convey.ShouldBeTrue)
		})
		convey.Convey("put etcd succeed", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).Put,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
						return nil
					}),
			})
			defer patches.ResetAll()
			err := PutStateWorkersToETCDWithRetry("testkey", []byte{})
			convey.So(err, convey.ShouldBeNil)
		})
	})

}

func Test_GetAllValueOfSNWorkers(t *testing.T) {
	convey.Convey("TestGetValueFromMetaEtcd", t, func() {
		convey.Convey("get response error", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return nil, errors.New("get response error")
				}),
			})
			defer patches.ResetAll()
			kvMap, err := GetAllValueOfSNWorkers(etcd3.GetStateEtcdClient())
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(strings.Contains(err.Error(), "get response error"), convey.ShouldBeTrue)
			convey.So(kvMap, convey.ShouldBeNil)
		})
		convey.Convey("get response empty", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{}}, nil
				}),
			})
			defer patches.ResetAll()
			kvMap, err := GetAllValueOfSNWorkers(etcd3.GetStateEtcdClient())
			convey.So(err, convey.ShouldBeNil)
			convey.So(kvMap, convey.ShouldNotBeNil)
			convey.So(len(kvMap), convey.ShouldEqual, 0)
		})
		convey.Convey("get response ok", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string,
					opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
					return &clientv3.GetResponse{Kvs: []*mvccpb.KeyValue{{Key: []byte("Key"), Value: []byte("value")}}}, nil
				}),
			})
			defer patches.ResetAll()
			kvMap, err := GetAllValueOfSNWorkers(etcd3.GetStateEtcdClient())
			convey.So(err, convey.ShouldBeNil)
			convey.So(kvMap, convey.ShouldNotBeNil)
			convey.So(len(kvMap), convey.ShouldEqual, 1)
		})
	})
}

func TestGetClusterNumInCurrentCell(t *testing.T) {
	convey.Convey("TestGetClusterNumInCurrentCell", t, func() {
		convey.Convey("get config error", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return nil, errors.New("GetConfig error")
			})
			defer patch.Reset()

			convey.So(GetClusterNumInCurrentCell(), convey.ShouldEqual, 0)
		})
		convey.Convey("get config ok", func() {
			patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
				return &config.FConfig{
					StateCounterCfg: config.StateCounterCfg{SiteClusters: []string{"cluster001", "cluster002", "cluster003"}},
				}, nil
			})
			defer patch.Reset()
			convey.So(GetClusterNumInCurrentCell(), convey.ShouldEqual, 3)
		})
	})
}

func TestIsClusterInCurrentCell_get_config_fail(t *testing.T) {
	patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
		return nil, errors.New("GetConfig error")
	})
	defer patch.Reset()
	assert.Equal(t, false, IsClusterInCurrentCell("cluster001"))
}

func TestIsClusterInCurrentCell(t *testing.T) {
	patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FConfig, error) {
		return &config.FConfig{
			StateCounterCfg: config.StateCounterCfg{SiteClusters: []string{"cluster001", "cluster002", "cluster003"}},
		}, nil
	})
	defer patch.Reset()

	tests := []struct {
		clusterID string
		expected  bool
	}{
		{
			clusterID: "cluster001",
			expected:  true,
		},
		{
			clusterID: "cluster003",
			expected:  true,
		},
		{
			clusterID: "cluster004",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.clusterID, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsClusterInCurrentCell(tt.clusterID))
		})
	}
}
