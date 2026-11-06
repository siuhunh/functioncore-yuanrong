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

package config

import (
	"io/ioutil"
	"net/http"
	"strings"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_GetConfig(t *testing.T) {
	Convey("test init module", t, func() {
		mockCfgInfo := "{\n\"filepath\": \"/home/sn/mock\",\n\"level\": \"INFO\",\n\"maxsize\": " +
			"500,\n\"maxbackups\": 1,\n\"maxage\": 1,\n\"compress\": true\n}"
		patches := [...]*Patches{
			ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
				return []byte(mockCfgInfo), nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		if jsonConfig := GetConfig(); jsonConfig != nil {
			t.Fatalf("failed to create an EtcdCli")
		}
	})
}

func Test_parseReqBody(t *testing.T) {
	Convey("test init module", t, func() {
		Convey("body of http request is nil", func() {
			http_req := &http.Request{}
			if byte := parseReqBody(http_req); len(byte) != 0 {
				t.Fatalf("failed to test parseReqBody")
			}
		})
		Convey("body of http request is not nil", func() {
			request := &http.Request{
				Body: ioutil.NopCloser(strings.NewReader("hello world")),
			}
			if byte := parseReqBody(request); len(byte) != 11 {
				t.Fatalf("failed to test parseReqBody")
			}
		})
	})
}
