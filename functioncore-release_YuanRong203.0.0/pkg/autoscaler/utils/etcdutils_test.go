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

// Package utils -
package utils

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	mockutils "functioncore/test/utils"
)

func TestGetFunctionListFromEtcd(t *testing.T) {
	resp := &clientv3.GetResponse{}
	tests := []struct {
		name      string
		want      *clientv3.GetResponse
		wantErr   bool
		patchFunc mockutils.PatchesFunc
	}{
		{
			"case succeed to get function list from etcd",
			resp,
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetCAEMetaEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{
							Client: &clientv3.Client{},
						}
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse, func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo,
						key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						return resp, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			got, err := GetFunctionListFromEtcd()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetFunctionListFromEtcd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetFunctionListFromEtcd() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestInitETCDClient(t *testing.T) {
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			"case test succeed to init etcd client",
			args{
				stopCh: make(<-chan struct{}),
			},
			false,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
						return &config.Config{
							RouteEtcd:   &etcd3.ClientConfig{},
							CAEMetaEtcd: &etcd3.ClientConfig{},
						}, nil
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
						func(a *alarm.Client, almType alarm.Type) {
							return
						}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(clientv3.New, func(cfg clientv3.Config) (*clientv3.Client, error) {
						return &clientv3.Client{}, nil
					}),
				})
				return patches
			},
		},
		{
			"case test failed to init etcd client",
			args{
				stopCh: make(<-chan struct{}),
			},
			true,
			func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := InitETCDClient(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("InitETCDClient() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
