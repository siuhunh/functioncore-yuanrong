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

// Package server is http server of debug agent
package server

import (
	"errors"
	"net/http"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/debug-agent/config"
	"functioncore/test/utils"
)

func TestCreateHTTPServer(t *testing.T) {
	server := &http.Server{
		Addr:           "127.0.0.1",
		Handler:        buildHandler(),
		MaxHeaderBytes: defaultMaxHeaderBytes,
		ReadTimeout:    defaultReadTimeout,
		WriteTimeout:   defaultWriteTimeout,
	}

	tests := []struct {
		name        string
		conf        config.DebugAgentConfig
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1: success",
			conf: config.DebugAgentConfig{
				ServerConfig: config.ServerConfig{
					Port:           8082,
					ReadTimeout:    333,
					WriteTimeout:   333,
					MaxHeaderBytes: 1048576,
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "127.0.0.1", true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(server), "ListenAndServe", func(_ *http.Server) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case2: success, serverConfig user default value",
			conf: config.DebugAgentConfig{
				ServerConfig: config.ServerConfig{
					Port: 8082,
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "127.0.0.1", true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(server), "ListenAndServe", func(_ *http.Server) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "case3: podIP does not exist",
			conf: config.DebugAgentConfig{
				ServerConfig: config.ServerConfig{
					Port: 8082,
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "", false
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case4: failed to start http server",
			conf: config.DebugAgentConfig{
				ServerConfig: config.ServerConfig{
					Port: 8082,
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "127.0.0.1", true
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(server), "ListenAndServe", func(_ *http.Server) error {
						return errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		err := CreateHTTPServer(&tt.conf)
		if (err != nil) != tt.wantErr {
			t.Errorf("CreateHTTPServer error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		patches.ResetAll()
	}
}
