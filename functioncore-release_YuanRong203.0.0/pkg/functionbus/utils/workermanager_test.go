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

// Package utils worker manager client
package utils

import (
	"net/http"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/functionbus/common"
)

func TestGetWorkerManagerClient(t *testing.T) {
	bakHttpsConfig := common.FConfig.HTTPSConfig
	common.FConfig.HTTPSConfig = &commontls.InternalHTTPSConfig{
		HTTPSEnable: true,
		TLSProtocol: "tls",
		TLSCiphers:  "ciphers",
	}
	common.FConfig.RequestConfig = &common.RequestConfig{RequestWorkerTimeout: 10}
	client := GetWorkerManagerClient()
	if client == nil {
		t.Errorf("failed to get worker manager client")
	}
	common.FConfig.HTTPSConfig = bakHttpsConfig
}

// TestFillInWorkerManagerRequestHeaders -
func TestFillInWorkerManagerRequestHeaders(t *testing.T) {
	convey.Convey("test: fill in workermanager request header", t, func() {
		patch := gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "authorization", "timestamp"
		})
		common.FConfig = &common.Configs{
			LocalAuth: &localauth.AuthConfig{},
		}
		defer func() {
			patch.Reset()
			common.FConfig = &common.Configs{}
		}()
		req := http.Request{
			Header: map[string][]string{},
		}
		FillInWorkerManagerRequestHeaders(&req)
		convey.So(req.Header.Get(constants.HeaderAuthTimestamp), convey.ShouldEqual, "timestamp")
		convey.So(req.Header.Get(constants.Authorization), convey.ShouldEqual, "authorization")
		convey.So(req.Header.Get(constants.HeaderEventSourceID), convey.ShouldEqual, appID)
		convey.So(req.Header.Get(constants.HeaderCallType), convey.ShouldEqual, "active")
	})
}

// TestGetWorkerManagerBaseURL -
func TestGetWorkerManagerBaseURL(t *testing.T) {
	_ = os.Setenv(constants.PodNamespaceEnvKey, "default")
	convey.Convey("test: get worker manager base url", t, func() {
		tasks := []struct {
			caseName    string
			httpsEnable bool
			patch       func()
			recover     func()
			expect      string
		}{
			{
				caseName:    "http",
				httpsEnable: false,
				patch: func() {
					common.FConfig = &common.Configs{HTTPSConfig: &commontls.InternalHTTPSConfig{HTTPSEnable: false}}
				},
				recover: func() {
					common.FConfig = &common.Configs{}
				},
				expect: "http://worker-manager.default.svc.cluster.local:58866",
			},
			{
				caseName:    "https",
				httpsEnable: true,
				patch: func() {
					common.FConfig = &common.Configs{HTTPSConfig: &commontls.InternalHTTPSConfig{HTTPSEnable: true}}
				},
				recover: func() {
					common.FConfig = &common.Configs{}
				},
				expect: "https://worker-manager.default.svc.cluster.local:58866",
			},
		}
		// test
		for _, task := range tasks {
			task.patch()
			actual := GetWorkerManagerBaseURL()
			convey.So(actual, convey.ShouldEqual, task.expect)
			task.recover()
		}
	})
}
