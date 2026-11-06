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

// Package healthcheck implement health check
package healthcheck

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/state-manager/server"
	"functioncore/test/utils"
)

func TestRegisterParam(t *testing.T) {
	name := "hello"
	hello := func() error {
		fmt.Println("It is healthy")
		return nil
	}
	RegisterParam(name, hello)
	assert.Equal(t, 1, len(checkMap.m))
	delete(checkMap.m, name)
}

func TestInitHealthCheckNoRouter(t *testing.T) {
	called := false
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyFunc(server.RegisterRouter, func(param *server.HandleParam, name string) {
			called = true
			return
		}),
	})

	defer patches.ResetAll()
	InitHealthCheck()
	if called != true {
		t.Error("initHealthCheckNoRouter test error")
	}
}

func TestCheckHandler(t *testing.T) {
	called := false
	healthCheckMap2 := &healthCheckMap{m: make(map[string]*checkFunc, initHealthRouter)}
	healthCheckMap2.m["test"] = &checkFunc{
		CallBack: func() error {
			called = true
			return nil
		},
	}
	healthCheckMap3 := &healthCheckMap{m: make(map[string]*checkFunc, initHealthRouter)}
	healthCheckMap3.m["test"] = &checkFunc{
		CallBack: func() error {
			called = true
			return errors.New("call back err")
		},
	}
	test := []struct {
		name       string
		checkMap   *healthCheckMap
		w          http.ResponseWriter
		r          *http.Request
		calledWant bool
	}{
		// test map empty
		{
			"case1",
			&healthCheckMap{m: make(map[string]*checkFunc, initHealthRouter)},
			utils.MockResponseWriter{},
			&http.Request{},
			false,
		},
		// test callback success
		{
			"case2",
			healthCheckMap2,
			utils.MockResponseWriter{},
			&http.Request{},
			true,
		},
		// test callback error
		{
			"case3",
			healthCheckMap3,
			utils.MockResponseWriter{},
			&http.Request{},
			true,
		},
	}

	for _, tt := range test {
		called = false
		temp := checkMap
		checkMap = tt.checkMap
		checkHandler(tt.w, tt.r)
		if called != tt.calledWant {
			t.Error("checkHandler test error")
		}
		checkMap = temp
	}
}
