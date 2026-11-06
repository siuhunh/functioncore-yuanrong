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

package utils

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/state-manager/config"
)

func TestGetRedisCmd(t *testing.T) {
	tests := []struct {
		name string
		want *redisclient.Client
	}{
		{"case1", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetRedisCmd(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetRedisCmd() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInitRedisCmd(t *testing.T) {
	patches := gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
		options ...redisclient.Option) (*redisclient.Client, error) {
		return &redisclient.Client{}, nil
	})
	defer patches.Reset()

	patchesNew := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
		return nil, nil
	})
	defer patchesNew.Reset()

	type args struct {
		conf *config.FConfig
	}

	var a args
	a.conf = &config.FConfig{
		StateCounterCfg: config.StateCounterCfg{
			PersistenceInterval: 10,
			ReleaseInterval:     10,
			ClusterID:           "01",
			ServerAddr:          "127.0.0.1",
			ServerMode:          "single",
			Password:            "aaa",
			EnableTLS:           false,
		},
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := InitRedisCmd(nil, tt.args.conf); (err != nil) != tt.wantErr {
				t.Errorf("InitRedisCmd() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
