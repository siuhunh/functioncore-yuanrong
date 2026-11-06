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

package router

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
)

type MockResponseWriter struct{}

func (m MockResponseWriter) Header() http.Header {
	return nil
}

func (m MockResponseWriter) Write([]byte) (int, error) {
	return 0, nil
}

func (m MockResponseWriter) WriteHeader(int) {
	return
}

func TestInit(t *testing.T) {
	Convey("TestInit", t, func() {
		Init()
		paths := []struct {
			path       string
			statusCode int
		}{
			{"/api/futureStore/detail", 404},
		}
		ts := httptest.NewServer(http.DefaultServeMux)
		defer ts.Close()
		for _, path := range paths {
			fmt.Println(path.path)
			resp, _ := http.Get(ts.URL + path.path)
			So(resp.StatusCode, ShouldEqual, path.statusCode)
		}
	})
}

func Test_writeHeaders(t *testing.T) {
	type args struct {
		w      http.ResponseWriter
		status int
		msg    string
	}
	var a args
	a.msg = "msg"
	a.status = 200
	var w MockResponseWriter
	a.w = w
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"success write headers", a, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := writeHeaders(tt.args.w, tt.args.status, tt.args.msg); (err != nil) != tt.wantErr {
				t.Errorf("writeHeaders() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_handleHealthCheck(t *testing.T) {
	type args struct {
		w       http.ResponseWriter
		request *http.Request
	}
	var a args
	var w MockResponseWriter
	a.w = w
	tests := []struct {
		name string
		args args
	}{
		{"check health", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handleHealthCheck(tt.args.w, tt.args.request)
		})
	}
}

func Test_handleHealthCheck_2(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(writeHeaders, func(w http.ResponseWriter, status int, msg string) error {
			return errors.New("aaa")
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		w       http.ResponseWriter
		request *http.Request
	}
	var a args
	var w MockResponseWriter
	a.w = w
	tests := []struct {
		name string
		args args
	}{
		{"check health", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handleHealthCheck(tt.args.w, tt.args.request)
		})
	}
}
