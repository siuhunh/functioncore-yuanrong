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

package constants

import (
	"os"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestParseDefaultSyncRequestTimeout(t *testing.T) {
	convey.Convey("TestParseDefaultSyncRequestTimeout return default val", t, func() {
		timeout := parseDefaultSyncRequestTimeout()
		convey.So(timeout, convey.ShouldEqual, defaultSyncRequestTimeout)
	})
	convey.Convey("TestParseDefaultSyncRequestTimeout err return default val", t, func() {
		_ = os.Setenv("SYNC_REQUEST_TIMEOUT", "200x")
		timeout := parseDefaultSyncRequestTimeout()
		convey.So(timeout, convey.ShouldEqual, defaultSyncRequestTimeout)
	})
	convey.Convey("TestParseDefaultSyncRequestTimeout return env val", t, func() {
		_ = os.Setenv("SYNC_REQUEST_TIMEOUT", "200")
		timeout := parseDefaultSyncRequestTimeout()
		convey.So(timeout, convey.ShouldEqual, 200)
	})
}

func TestParseNetworkModel(t *testing.T) {
	convey.Convey("TestParseDefaultSyncRequestTimeout return default val", t, func() {
		model := parseNetworkModel()
		convey.So(model, convey.ShouldEqual, "tunnel")
	})
	convey.Convey("TestParseDefaultSyncRequestTimeout return env val", t, func() {
		_ = os.Setenv("NETWORK_MODEL", "tunnel1")
		model := parseNetworkModel()
		convey.So(model, convey.ShouldEqual, "tunnel1")
	})
}

func TestGetValidNodeRoleValues(t *testing.T) {
	values := GetValidNodeRoleValues()
	convey.Convey("TestGetValidNodeRoleValues", t, func() {
		convey.So(values[0], convey.ShouldEqual, "data-plane")
		convey.So(values[1], convey.ShouldEqual, "data-plane-scaled")
	})
}

func TestGetValidNodeRoleValuesWithGPU(t *testing.T) {
	values := GetValidNodeRoleValuesWithGPU()
	convey.Convey("TestGetValidNodeRoleValuesWithGPU", t, func() {
		convey.So(values[0], convey.ShouldEqual, "data-plane")
		convey.So(values[1], convey.ShouldEqual, "data-plane-gpu")
		convey.So(values[2], convey.ShouldEqual, "data-plane-scaled")

	})
}

func TestIsStateID(t *testing.T) {

	tests := []struct {
		key        string
		wantBool   bool
	}{
		{
			key: "11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: true,
		},
		{
			key: "applier",
			wantBool: false,
		},
		{
			key: "businessID11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
		{
			key: "cluster",
			wantBool: false,
		},
		{
			key: "cpu11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
		{
			key: "creator11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
		{
			key: "funcName11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
		{
			key: "memory11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
		{
			key: "serviceID11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
		{
			key: "tenantID11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
		{
			key: "version11adfb18-91e5-5f1b-8f1b-4da286e880bd",
			wantBool: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			res := IsStateID(tt.key)
			if res != tt.wantBool {
				t.Errorf("res %t is not equl tt.wantBool %t", res, tt.wantBool)
			}
		})
	}
}