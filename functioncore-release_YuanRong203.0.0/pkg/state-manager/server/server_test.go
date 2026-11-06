/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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
	"context"
	"errors"
	"net"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/cluster"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/state-manager/config"
	"functioncore/test/utils"
)

func TestRegisterRouter(t *testing.T) {
	type args struct {
		param *HandleParam
		name  string
	}
	tests := []struct {
		name    string
		args    args
		handleM map[string]*HandleParam
	}{
		// test register success
		{
			"case1",
			args{
				param: &HandleParam{},
				name:  "test",
			},
			make(map[string]*HandleParam),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := handle.m
			handle.m = tt.handleM
			RegisterRouter(tt.args.param, tt.args.name)
			if _, ok := handle.m[tt.args.name]; !ok {
				t.Errorf("TestRegisterRouter error")
			}
			handle.m = temp
		})
	}
}

func TestStartHTTPServer(t *testing.T) {
	handleM := make(map[string]*HandleParam)
	handleM["test"] = &HandleParam{Method: "GET", HandleFunc: func(c *gin.Context) { return }}
	handleM2 := make(map[string]*HandleParam)
	handleM2["test2"] = &HandleParam{Method: "GET", HandleFunc: func(c *gin.Context) { return }, Path: "/test"}
	handleM3 := make(map[string]*HandleParam)
	handleM3["test3"] = &HandleParam{Method: "GET", HandleFunc: func(c *gin.Context) { return }, Path: "/test2"}
	handleM4 := make(map[string]*HandleParam)
	handleM4["test4"] = &HandleParam{Method: "GET", HandleFunc: func(c *gin.Context) { return }, Path: "/test3"}
	type args struct {
		conf       *config.FConfig
		stopCh     <-chan struct{}
		startWatch func(stopper <-chan struct{}) error
	}
	tests := []struct {
		name        string
		args        args
		handleM     map[string]*HandleParam
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test handle.m length = 0
		{
			"case1",
			args{
				conf:   &config.FConfig{},
				stopCh: make(<-chan struct{}),
				startWatch: func(stopper <-chan struct{}) error {
					return nil
				},
			},
			make(map[string]*HandleParam),
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			true,
		},
		// test get handler error
		{
			"case2",
			args{
				conf:   &config.FConfig{},
				stopCh: make(<-chan struct{}),
				startWatch: func(stopper <-chan struct{}) error {
					return nil
				},
			},
			handleM,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getHandler, func(conf *config.FConfig, stopCh <-chan struct{},
						startWatch func(stopper <-chan struct{}) error) (*Handler, *sync.WaitGroup, error) {
						return &Handler{}, &sync.WaitGroup{}, errors.New("get handler error")
					}),
				})
				return patches
			},
			true,
		},
		// test can't get net ip
		{
			"case3",
			args{
				conf:   &config.FConfig{},
				stopCh: make(<-chan struct{}),
				startWatch: func(stopper <-chan struct{}) error {
					return nil
				},
			},
			handleM2,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getHandler, func(conf *config.FConfig, stopCh <-chan struct{},
						startWatch func(stopper <-chan struct{}) error) (*Handler, *sync.WaitGroup, error) {
						return &Handler{}, &sync.WaitGroup{}, nil
					}),
					gomonkey.ApplyFunc(net.ParseIP, func(s string) net.IP {
						return nil
					}),
				})
				return patches
			},
			true,
		},
		// test HTTPSEnable = false
		{
			"case4",
			args{
				conf: &config.FConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: false,
					},
				},
				stopCh: make(<-chan struct{}),
				startWatch: func(stopper <-chan struct{}) error {
					return nil
				},
			},
			handleM3,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getHandler, func(conf *config.FConfig, stopCh <-chan struct{},
						startWatch func(stopper <-chan struct{}) error) (*Handler, *sync.WaitGroup, error) {
						return &Handler{}, &sync.WaitGroup{}, nil
					}),
					gomonkey.ApplyFunc(net.ParseIP, func(s string) net.IP {
						return []byte("127.0.0.1")
					}),
				})
				return patches
			},
			true,
		},
		{
			"case5",
			args{
				conf: &config.FConfig{
					HTTPSConfig: tls.InternalHTTPSConfig{
						HTTPSEnable: true,
					},
				},
				stopCh: make(<-chan struct{}),
				startWatch: func(stopper <-chan struct{}) error {
					return nil
				},
			},
			handleM4,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(getHandler, func(conf *config.FConfig, stopCh <-chan struct{},
						startWatch func(stopper <-chan struct{}) error) (*Handler, *sync.WaitGroup, error) {
						return &Handler{}, &sync.WaitGroup{}, nil
					}),
					gomonkey.ApplyFunc(net.ParseIP, func(s string) net.IP {
						return []byte("127.0.0.1")
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			temp := handle.m
			handle.m = tt.handleM
			patches := tt.patchesFunc()
			if err := StartHTTPServer(tt.args.conf, tt.args.stopCh, tt.args.startWatch); (err != nil) != tt.wantErr {
				t.Errorf("StartHTTPServer() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
			handle.m = temp
		})
	}
}

func Test_getHandler(t *testing.T) {
	type args struct {
		conf       *config.FConfig
		stopCh     <-chan struct{}
		startWatch func(stopper <-chan struct{}) error
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		// test createClusterDiscovery failed
		{
			"case1",
			args{
				conf:   &config.FConfig{},
				stopCh: make(<-chan struct{}),
				startWatch: func(stopper <-chan struct{}) error {
					return nil
				},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(createClusterDiscovery, func(conf *config.FConfig, stopCh <-chan struct{},
						wg *sync.WaitGroup) (cluster.Discovery, error) {
						return nil, errors.New("createClusterDiscovery error")
					}),
				})
				return patches
			},
			true,
		},
		{
			"case2",
			args{
				conf:   &config.FConfig{},
				stopCh: make(<-chan struct{}),
				startWatch: func(stopper <-chan struct{}) error {
					return nil
				},
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, _, err := getHandler(tt.args.conf, tt.args.stopCh, tt.args.startWatch)
			if (err != nil) != tt.want {
				t.Errorf("getHandler() error = %v, wantErr %v", err, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_waitUtilInterrupt(t *testing.T) {
	called := false
	type args struct {
		stopCh chan struct{}
		wg     *sync.WaitGroup
		server *http.Server
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		want        bool
	}{
		{
			"case1",
			args{
				nil,
				nil,
				nil,
			},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
		{
			"case1",
			args{
				make(chan struct{}),
				&sync.WaitGroup{},
				&http.Server{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "Shutdown", func(_ *http.Server, ctx context.Context) error {
						called = true
						return errors.New("shut down error")
					}),
				})
				return patches
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.stopCh != nil {
				timer := time.NewTicker(time.Microsecond * 1)
				go func(stopChan chan struct{}) {
					<-timer.C
					close(stopChan)
				}(tt.args.stopCh)
			}
			called = false
			patches := tt.patchesFunc()
			waitUtilInterrupt(tt.args.stopCh, tt.args.wg, tt.args.server)
			if called != tt.want {
				t.Errorf("waitUtilInterrupt error")
			}
			patches.ResetAll()
		})
	}
}
