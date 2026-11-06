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
package healthcheck

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterHandlers(t *testing.T) {
	type args struct {
		r *gin.Engine
	}
	var a args
	router := gin.Default()
	a.r = router
	tests := []struct {
		name string
		args args
	}{
		{"test1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			RegisterHandlers(a.r)
		})
	}
}

func Test_healthCheck(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	RegisterHandlers(router)
	type args struct {
		c *gin.Context
	}
	var a args
	c := &gin.Context{
		Request:  nil,
		Writer:   nil,
		Params:   nil,
		Keys:     nil,
		Errors:   nil,
		Accepted: nil,
	}
	a.c = c
	tests := []struct {
		name string
		args args
	}{
		{"test1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rsp := PerformRequest(router, "GET", "/healthcheck", []byte{})
			fmt.Println(rsp.Code)
		})
	}
}

func PerformRequest(r http.Handler, method, path string, input []byte) *httptest.ResponseRecorder {
	req := &http.Request{}
	req = nil
	if input == nil {
		req = httptest.NewRequest(method, path, nil)
	} else {
		buffer := bytes.NewReader(input)
		req = httptest.NewRequest(method, path, buffer)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}
