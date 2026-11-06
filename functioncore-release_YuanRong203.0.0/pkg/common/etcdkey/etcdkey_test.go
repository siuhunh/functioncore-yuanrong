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

// Package etcdkey contains etcd key definition and tools
package etcdkey

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStateWorkersKey_ParseFrom(t *testing.T) {
	type fields struct {
		KeyType    string
		BusinessID string
		TenantID   string
		Function   string
		Version    string
		Zone       string
		StateID    string
	}
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "test001",
			fields: fields{
				KeyType:    "stateworkers",
				BusinessID: "yrk",
				TenantID:   "tenantID",
				Function:   "function",
				Version:    "$latest",
				Zone:       "defaultaz",
				StateID:    "stateID",
			},
			args: args{
				key: "/sn/stateworkers/business/yrk/tenant/tenantID" +
					"/function/function/version/$latest/defaultaz/stateID",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateWorkersKey{
				TypeKey:    tt.fields.KeyType,
				BusinessID: tt.fields.BusinessID,
				TenantID:   tt.fields.TenantID,
				Function:   tt.fields.Function,
				Version:    tt.fields.Version,
				Zone:       tt.fields.Zone,
				StateID:    tt.fields.StateID,
			}
			if err := s.ParseFrom(tt.args.key); (err != nil) != tt.wantErr {
				t.Errorf("ParseFrom() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestStateWorkersKey_String(t *testing.T) {
	type fields struct {
		KeyType    string
		BusinessID string
		TenantID   string
		Function   string
		Version    string
		Zone       string
		StateID    string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "test002",
			fields: fields{
				KeyType:    "stateworkers",
				BusinessID: "yrk",
				TenantID:   "tenantID",
				Function:   "function",
				Version:    "$latest",
				Zone:       "defaultaz",
				StateID:    "stateID",
			},
			want: "/sn/stateworkers/business/yrk/tenant/tenantID" +
				"/function/function/version/$latest/defaultaz/stateID",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &StateWorkersKey{
				TypeKey:    tt.fields.KeyType,
				BusinessID: tt.fields.BusinessID,
				TenantID:   tt.fields.TenantID,
				Function:   tt.fields.Function,
				Version:    tt.fields.Version,
				Zone:       tt.fields.Zone,
				StateID:    tt.fields.StateID,
			}
			if got := s.String(); got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWorkerInstanceKey_ParseFrom(t *testing.T) {
	tests := []struct {
		arg  string
		want WorkerInstanceKey
		err  bool
	}{
		{
			arg: "/sn/workers/business/busid/tenant/abcd/function/" +
				"0-counter-addone/version/$latest/defaultaz/defaultaz-#-pool7-500-500-python3.7-58f588848d-smss8",
			want: WorkerInstanceKey{
				TypeKey:    "workers",
				BusinessID: "busid",
				TenantID:   "abcd",
				Function:   "0-counter-addone",
				Version:    "$latest",
				Zone:       "defaultaz",
				Instance:   "defaultaz-#-pool7-500-500-python3.7-58f588848d-smss8",
			},
			err: false,
		},
		{
			arg: "/sn/workers/business/busid/tenant/abcd/function/" +
				"0-counter-addone/version/$latest/defaultaz",
			want: WorkerInstanceKey{},
			err:  true,
		},
	}
	for _, tt := range tests {
		worker := WorkerInstanceKey{}
		err := worker.ParseFrom(tt.arg)
		if tt.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, tt.want, worker)
		}
	}
}

func TestWorkerInstanceKey_String(t *testing.T) {
	tests := []struct {
		arg  WorkerInstanceKey
		want string
	}{
		{
			want: "/sn/workers/business/busid/tenant/abcd/function/" +
				"0-counter-addone/version/$latest/defaultaz/defaultaz-#-pool7-500-500-python3.7-58f588848d-smss8",
			arg: WorkerInstanceKey{
				TypeKey:    "workers",
				BusinessID: "busid",
				TenantID:   "abcd",
				Function:   "0-counter-addone",
				Version:    "$latest",
				Zone:       "defaultaz",
				Instance:   "defaultaz-#-pool7-500-500-python3.7-58f588848d-smss8",
			},
		},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, tt.arg.String())
	}
}

func TestAnonymizeTenantCommonEtcdKey(t *testing.T) {
	keyA := "/sn/functions/business/yrk"
	anonymizeKeyA := AnonymizeTenantCommonEtcdKey(keyA)
	assert.Equal(t, keyA, anonymizeKeyA)

	keyB := "/sn/functions/business/yrk/tenant/8e08d5cc0ad34032bba8d636040a278c/function/0-test1-addone/version/$latest"
	anonymizeKeyB := AnonymizeTenantCommonEtcdKey(keyB)
	assert.NotEqual(t, keyB, anonymizeKeyB)
}

func TestAnonymizeTenantFunctionMetadataEtcdKey(t *testing.T) {
	keyA := "/repo/FunctionVersion/business"
	anonymizeKeyA := AnonymizeTenantFunctionMetadataEtcdKey(keyA)
	assert.Equal(t, keyA, anonymizeKeyA)

	keyB := "/repo/FunctionVersion/business/tenant/funcName/version/"
	anonymizeKeyB := AnonymizeTenantFunctionMetadataEtcdKey(keyB)
	assert.NotEqual(t, keyB, anonymizeKeyB)
}

// TestGetStateETCDPath -
func TestGetStateETCDPath(t *testing.T) {
	type args struct {
		stateID string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"case1",
			args{
				stateID: "test",
			},
			"/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetStateETCDPath(tt.args.stateID); got != tt.want {
				t.Errorf("GetStateETCDPath() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestGetTaskETCDPath -
func TestGetTaskETCDPath(t *testing.T) {
	type args struct {
		instanceID string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"case1",
			args{
				instanceID: "test",
			},
			"/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/test",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetTaskETCDPath(tt.args.instanceID); got != tt.want {
				t.Errorf("GetStateETCDPath() = %v, want %v", got, tt.want)
			}
		})
	}
}
