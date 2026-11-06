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
	"reflect"
	"testing"

	"functioncore/pkg/worker-manager/api"
)

// Test New Api Registry
func TestNewAPIRegistry(t *testing.T) {
	handler := newAPIRegistry()
	tests := []struct {
		name string
		want *APIRegistry
	}{
		{"NilRegistry", handler},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newAPIRegistry(); reflect.TypeOf(tt.want) != reflect.TypeOf(got) {
				t.Errorf("newAPIRegistry() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestAPIRegistry_RegisterAPI Test Api Registry_RegisterAPI
func TestAPIRegistry_RegisterAPI(t *testing.T) {
	type args struct {
		apiVersion api.Version
	}
	tests := []struct {
		name    string
		r       *APIRegistry
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.r.registerAPI(tt.args.apiVersion); (err != nil) != tt.wantErr {
				t.Errorf("APIRegistry.registerAPI() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
