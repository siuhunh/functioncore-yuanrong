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

package alarm

import (
	"context"
	"crypto/tls"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	mockUtils "functioncore/test/utils"
	"functioncore/tools/autoscaleralarm/config"
)

func Test_InitAlarmInfo(t *testing.T) {
	type fields struct {
		httpClient  *http.Client
		baseURLPath string
	}
	type args struct {
		timeout     time.Duration
		tls         *tls.Config
		baseURLPath string
	}

	tests := []struct {
		name   string
		fields fields
		args   args

		wantErr bool
	}{
		{
			"case1: create http client ok",
			fields{&http.Client{}, ""},
			args{10 * time.Second, nil, "test1"},
			false,
		},
		{
			"case1: create https client ok",
			fields{&http.Client{}, ""},
			args{10 * time.Second, &tls.Config{}, "test1"},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alarm := &Alarm{
				httpClient:  tt.fields.httpClient,
				baseURLPath: tt.fields.baseURLPath,
			}
			err := alarm.initClient(tt.args.timeout, tt.args.tls, tt.args.baseURLPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("doRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_GenAlarmRequest(t *testing.T) {
	type fields struct {
		baseURLPath string
		AlarmInfo   *Info
	}
	type args struct {
		ak string
		sk string
	}

	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1: GenAlarmRequest ok",
			fields{"test1", &Info{}},
			args{"111", "2222"},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.SignOMSVC, func(ak, sk, url string, data []byte) (string, string) {
						return "", ""
					}),
				},
				)
				return patches
			},
			false,
		},
		{
			"case2: GenAlarmRequest error",
			fields{"test1", &Info{}},
			args{"111", "2222"},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(http.NewRequestWithContext, func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
						return &http.Request{}, fmt.Errorf("error")
					}),
				},
				)
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			a := &Alarm{
				AlarmInfo:   tt.fields.AlarmInfo,
				baseURLPath: tt.fields.baseURLPath,
			}
			_, err := a.GenAlarmRequest(tt.args.ak, tt.args.sk)
			if (err != nil) != tt.wantErr {
				t.Errorf("doRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_sendAlarmMessage(t *testing.T) {
	type fields struct {
	}
	type args struct {
		cfg *config.Config
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1: get etcd key error",
			fields{},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
						return nil, fmt.Errorf("get AutoScalerMasterLockKey error")
					}),
				},
				)
				return patches
			},
			true,
		},
		{
			"case2: no alarm, status ok",
			fields{},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(getAutoScalerMasterLockKey, func() (*clientv3.GetResponse, error) {
						return &clientv3.GetResponse{
							Kvs: make([]*mvccpb.KeyValue, 1),
						}, nil
					}),
				},
				)
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			alarm := &Alarm{
				AlarmInfo: &Info{},
			}
			patches := tt.patchesFunc()
			err := alarm.sendAlarmMessage(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("doRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}

}

func Test_getAutoScalerMasterLockKey(t *testing.T) {
	type fields struct {
	}
	type args struct {
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			"case1: getAutoScalerMasterLockKey error",
			fields{},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
						return nil
					}),
				},
				)
				return patches
			},
			true,
		},
		{
			"case2: getAutoScalerMasterLockKey error",
			fields{},
			args{},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{
							Client: nil,
						}
					}),
				},
				)
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := getAutoScalerMasterLockKey()
			if (err != nil) != tt.wantErr {
				t.Errorf("doRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}
