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
package stateclient

import (
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/efs/kvstorage"
)

func TestStateStore_SetTTL(t *testing.T) {
	type fields struct {
		storage kvstorage.Storage
	}
	type args struct {
		key string
		ttl int
	}
	var f fields
	var a args
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateStore{
				storage: tt.fields.storage,
			}
			if got := s.SetTTL(tt.args.key, tt.args.ttl); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetTTL() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateStore_SetStateAndResult(t *testing.T) {
	type fields struct {
		storage kvstorage.Storage
	}
	type args struct {
		key    *metadata.Metadata
		state  string
		TTL    int
		req    *common.Request
		result *common.Response
	}
	var f fields
	var a args
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateStore{
				storage: tt.fields.storage,
			}
			if got := s.SetStateAndResult(tt.args.key, tt.args.state, tt.args.TTL, tt.args.req, tt.args.result); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetStateAndResult() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStateStore_IsStateIDInHash(t *testing.T) {
	type fields struct {
		storage kvstorage.Storage
	}
	type args struct {
		stateHashKey string
		stateID      string
	}
	var f fields
	var a args
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
		want1  common.TaskError
	}{
		{"case1", f, a, true, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateStore{
				storage: tt.fields.storage,
			}
			got, got1 := s.IsStateIDInHash(tt.args.stateHashKey, tt.args.stateID)
			if got != tt.want {
				t.Errorf("IsStateIDInHash() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("IsStateIDInHash() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestStateStore_DeleteState(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyGlobalVar(&config.Conf.PersistentState, true),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type fields struct {
		storage kvstorage.Storage
	}
	type args struct {
		key      string
		funcName string
	}
	var f fields
	storage := kvstorage.NewStorage()
	f.storage = storage
	var a args
	tests := []struct {
		name   string
		fields fields
		args   args
		want   common.TaskError
	}{
		{"case1", f, a, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateStore{
				storage: tt.fields.storage,
			}
			if got := s.DeleteState(tt.args.key, tt.args.funcName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DeleteState() = %v, want %v", got, tt.want)
			}
		})
	}
}
