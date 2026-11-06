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
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	informercorev1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	typecorev1 "k8s.io/client-go/kubernetes/typed/core/v1"

	apiServerOptions "functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/cluster"
	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snapshotmanager"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/tokenrotation"
	v1 "functioncore/pkg/worker-manager/api/v1"
	"functioncore/pkg/worker-manager/api/v1/faasfunction"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/checkpoint"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	interfacelog "functioncore/pkg/worker-manager/log"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/reactor"
	"functioncore/pkg/worker-manager/state"
	"functioncore/pkg/worker-manager/tenantcommand"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpa"
	"functioncore/pkg/worker-manager/vpcmanager"
	mockUtils "functioncore/test/utils"
)

type mockKV struct {
	sync.RWMutex
	kv map[string]string
}

var FakeEtcd mockKV

func TestRun(t *testing.T) {
	type args struct {
		options *apiServerOptions.APIServerOptions
		stopCh  <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{"case1 succeed to run server",
			args{
				options: apiServerOptions.NewAPIServerOptions(),
				stopCh:  make(chan struct{}, 1),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{
								AKey: "test",
								SKey: "test"},
						}, nil
					}),
					gomonkey.ApplyFunc(getHandler,
						func(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}, c *config.FaasConfig) (
							*APIServerHandler, *sync.WaitGroup, error) {
							return &APIServerHandler{}, &sync.WaitGroup{}, nil
						}),
					gomonkey.ApplyFunc(startHTTPServer,
						func(options *apiServerOptions.APIServerOptions, handler http.Handler, wg *sync.WaitGroup,
							stopCh <-chan struct{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(state.InitHTTPClient, func() error {
						return nil
					}),
				})
				return patches
			},
			false},
		{"case2 ValidateStruct error",
			args{
				options: &apiServerOptions.APIServerOptions{
					ClusterType: "111111111111111111111111111111111111111111111111111111111" +
						"1111111111111111111111111111111111111111111111111111"},
				stopCh: make(chan struct{}, 1),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{})
				return patches
			},
			true},
		{"case2 CreateInterLogger error",
			args{
				options: &apiServerOptions.APIServerOptions{},
				stopCh:  make(chan struct{}, 1),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(interfacelog.CreateInterLogger,
						func(logPath string) error {
							return errors.New("test")
						}),
					gomonkey.ApplyFunc(state.InitHTTPClient, func() error {
						return nil
					}),
				})
				return patches
			},
			true},
		{"case3 get config error",
			args{
				options: &apiServerOptions.APIServerOptions{},
				stopCh:  make(chan struct{}, 1),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(interfacelog.CreateInterLogger,
						func(logPath string) error {
							return nil
						}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{
								AKey: "test",
								SKey: "test"},
							HTTPSConfig: &commontls.InternalHTTPSConfig{
								HTTPSEnable: true},
						}, errors.New("get config error")
					}),
					gomonkey.ApplyFunc(state.InitHTTPClient, func() error {
						return nil
					}),
				})
				return patches
			},
			true},
		{"case4 getHandler error",
			args{
				options: apiServerOptions.NewAPIServerOptions(),
				stopCh:  make(chan struct{}, 1),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{
								AKey: "test",
								SKey: "test"},
						}, nil
					}),
					gomonkey.ApplyFunc(localauth.DecryptKeys,
						func(inputAKey string, inputSKey string) ([]byte, []byte, error) {
							return []byte{}, []byte{}, nil
						}),
					gomonkey.ApplyFunc(getHandler,
						func(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}, c *config.FaasConfig) (
							*APIServerHandler, *sync.WaitGroup, error) {
							return nil, nil, errors.New("test")
						}),
					gomonkey.ApplyFunc(state.InitHTTPClient, func() error {
						return nil
					}),
				})
				return patches
			},
			true},
		{"case5 startHTTPServer error",
			args{
				options: apiServerOptions.NewAPIServerOptions(),
				stopCh:  make(chan struct{}, 1),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{
								AKey: "test",
								SKey: "test"},
						}, nil
					}),
					gomonkey.ApplyFunc(getHandler,
						func(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}, c *config.FaasConfig) (
							*APIServerHandler, *sync.WaitGroup, error) {
							return &APIServerHandler{}, &sync.WaitGroup{}, nil
						}),
					gomonkey.ApplyFunc(startHTTPServer,
						func(options *apiServerOptions.APIServerOptions, handler http.Handler, wg *sync.WaitGroup,
							stopCh <-chan struct{}) error {
							return errors.New("test")
						}),
					gomonkey.ApplyFunc(state.InitHTTPClient, func() error {
						return nil
					}),
				})
				return patches
			},
			true},

		{"case6 startHTTP client error",
			args{
				options: apiServerOptions.NewAPIServerOptions(),
				stopCh:  make(chan struct{}, 1),
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{
							LocalAuth: &localauth.AuthConfig{
								AKey: "test",
								SKey: "test"},
						}, nil
					}),
					gomonkey.ApplyFunc(getHandler,
						func(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}, c *config.FaasConfig) (
							*APIServerHandler, *sync.WaitGroup, error) {
							return &APIServerHandler{}, &sync.WaitGroup{}, nil
						}),
					gomonkey.ApplyFunc(startHTTPServer,
						func(options *apiServerOptions.APIServerOptions, handler http.Handler, wg *sync.WaitGroup,
							stopCh <-chan struct{}) error {
							return errors.New("test")
						}),
					gomonkey.ApplyFunc(state.InitHTTPClient, func() error {
						return errors.New("error")
					}),
				})
				return patches
			},
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := Run(tt.args.options, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("Run() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_newWorkerClient(t *testing.T) {
	patch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
		f()
	})
	defer patch.Reset()
	tests := []struct {
		name        string
		want        bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to newWorkerClient enableHTTPS",
			false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
						return &tls.Config{}
					}),
				})
				return patches
			}},
		{"case2 succeed to newWorkerClient not enableHTTPS",
			true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
						return &tls.Config{}
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := newWorkerClient(); (got.TLSConfig == nil) != tt.want {
				t.Errorf("newWorkerClient() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_startHTTPServer(t *testing.T) {
	type args struct {
		options *apiServerOptions.APIServerOptions
		handler http.Handler
		wg      *sync.WaitGroup
		stopCh  <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 start http server normal", args{
			options: apiServerOptions.NewAPIServerOptions(),
			handler: &APIServerHandler{},
			wg:      &sync.WaitGroup{},
			stopCh:  make(chan struct{}),
		}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "127.0.0.1"
					}),
					gomonkey.ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
						return nil
					}),
					gomonkey.ApplyFunc(getMaxServerConnection, func() int {
						return 2000
					}),
					gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
						return &tls.Config{}
					}),
					gomonkey.ApplyFunc((*http.Server).Serve, func(_ *http.Server, l net.Listener) error {
						return nil
					}),
					gomonkey.ApplyFunc(waitUtilInterrupt,
						func(stopCh <-chan struct{}, wg *sync.WaitGroup, server *http.Server) {}),
				})
				return patches
			}},
		{"case2 net listen error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			handler: &APIServerHandler{},
			wg:      &sync.WaitGroup{},
			stopCh:  make(chan struct{}),
		}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "127.0.0.1"
					}),
					gomonkey.ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
						return nil
					}),
					gomonkey.ApplyFunc(getMaxServerConnection, func() int {
						return 2000
					}),
					gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
						return &tls.Config{}
					}),
					gomonkey.ApplyFunc((*http.Server).Serve, func(_ *http.Server, l net.Listener) error {
						return nil
					}),
					gomonkey.ApplyFunc(waitUtilInterrupt,
						func(stopCh <-chan struct{}, wg *sync.WaitGroup, server *http.Server) {}),
					gomonkey.ApplyFunc(net.Listen,
						func(network, address string) (net.Listener, error) {
							return nil, errors.New("test")
						}),
				})
				return patches
			}},
		{"case3 Serve error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			handler: &APIServerHandler{},
			wg:      &sync.WaitGroup{},
			stopCh:  make(chan struct{}),
		}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "127.0.0.1"
					}),
					gomonkey.ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
						return nil
					}),
					gomonkey.ApplyFunc(getMaxServerConnection, func() int {
						return 2000
					}),
					gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
						return &tls.Config{}
					}),
					gomonkey.ApplyFunc((*http.Server).Serve, func(_ *http.Server, l net.Listener) error {
						return errors.New("test")
					}),
					gomonkey.ApplyFunc(waitUtilInterrupt,
						func(stopCh <-chan struct{}, wg *sync.WaitGroup, server *http.Server) {}),
				})
				return patches
			}},
		{"case4 start https server normal", args{
			options: apiServerOptions.NewAPIServerOptions(),
			handler: &APIServerHandler{},
			wg:      &sync.WaitGroup{},
			stopCh:  make(chan struct{}),
		}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "127.0.0.1"
					}),
					gomonkey.ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
						return nil
					}),
					gomonkey.ApplyFunc(getMaxServerConnection, func() int {
						return 2000
					}),
					gomonkey.ApplyFunc((*http.Server).ServeTLS, func(_ *http.Server, l net.Listener, certFile, keyFile string) error {
						return nil
					}),
					gomonkey.ApplyFunc(waitUtilInterrupt,
						func(stopCh <-chan struct{}, wg *sync.WaitGroup, server *http.Server) {}),
				})
				return patches
			}},
		{"case5 ServeTLS error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			handler: &APIServerHandler{},
			wg:      &sync.WaitGroup{},
			stopCh:  make(chan struct{}),
		}, true,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
						return true
					}),
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "127.0.0.1"
					}),
					gomonkey.ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
						return nil
					}),
					gomonkey.ApplyFunc(getMaxServerConnection, func() int {
						return 2000
					}),
					gomonkey.ApplyFunc((*http.Server).ServeTLS, func(_ *http.Server, l net.Listener, certFile, keyFile string) error {
						return errors.New("test")
					}),
					gomonkey.ApplyFunc(waitUtilInterrupt,
						func(stopCh <-chan struct{}, wg *sync.WaitGroup, server *http.Server) {}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := startHTTPServer(tt.args.options, tt.args.handler, tt.args.wg, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("startHTTPServer() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_getMaxServerConnection(t *testing.T) {
	tests := []struct {
		name        string
		want        int
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case1 getMaxServerConnection normal", 2000, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "2000"
					}),
				})
				return patches
			}},
		{
			"case2 getMaxServerConnection os get env returns nil", defaultConnectionLimit, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			}},
		{
			"case3 getMaxServerConnection str to int error", defaultConnectionLimit, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "abc"
					}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := getMaxServerConnection(); got != tt.want {
				t.Errorf("getMaxServerConnection() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_checkHeartbeats(t *testing.T) {
	type args struct {
		clusterDiscovery cluster.Discovery
		waitTime         time.Duration
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 the node switched to leader,with no need to check.", args{
			clusterDiscovery: &cluster.SingleNodeCluster{},
			waitTime:         time.Duration(1) * time.Second,
		}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Role, func(_ *cluster.SingleNodeCluster) string {
					return cluster.ClusterRoleLeader
				}),
				gomonkey.ApplyFunc(os.Exit, func(code int) {
					panic("checkHeartbeats shouldn't call os.Exit")
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			checkHeartbeats(tt.args.clusterDiscovery, tt.args.waitTime)
			patches.ResetAll()
		})
	}
}

func Test_listenBackNode(t *testing.T) {
	type args struct {
		clusterDiscovery cluster.Discovery
		waitTime         time.Duration
	}
	tests := []struct {
		name        string
		args        args
		want        bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 succeed to listenBackNode ", args{
			clusterDiscovery: &cluster.SingleNodeCluster{},
			waitTime:         time.Duration(1) * time.Second,
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Leader,
					func(_ *cluster.SingleNodeCluster) *cluster.LeaderInfo {
						return &cluster.LeaderInfo{Host: "127.0.0.1"}
					}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{
							AKey: "test",
							SKey: "test"},
						HTTPSConfig: &commontls.InternalHTTPSConfig{
							HTTPSEnable: true},
					}, nil
				}),
				gomonkey.ApplyFunc(localauth.CreateAuthorization,
					func(ak, sk, url, appID string, data []byte) (string, string) {
						return "test", "timestamp"
					}),
				gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
					return true
				}),
				gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
					return &tls.Config{}
				}),
				gomonkey.ApplyFunc((*fasthttp.Client).DoTimeout,
					func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, timeout time.Duration) error {
						resp.Header.SetStatusCode(http.StatusOK)
						return nil
					}),
			})
			return patches
		}},
		{"case2 failed to listenBackNode status code not 200", args{
			clusterDiscovery: &cluster.SingleNodeCluster{},
			waitTime:         time.Duration(1) * time.Second,
		}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Leader,
					func(_ *cluster.SingleNodeCluster) *cluster.LeaderInfo {
						return &cluster.LeaderInfo{Host: "127.0.0.1"}
					}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
					return &config.FaasConfig{
						LocalAuth: &localauth.AuthConfig{
							AKey: "test",
							SKey: "test"},
						HTTPSConfig: &commontls.InternalHTTPSConfig{
							HTTPSEnable: true},
					}, nil
				}),
				gomonkey.ApplyFunc(localauth.CreateAuthorization,
					func(ak, sk, url, appID string, data []byte) (string, string) {
						return "test", "timestamp"
					}),
				gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
					return true
				}),
				gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
					return &tls.Config{}
				}),
				gomonkey.ApplyFunc((*fasthttp.Client).DoTimeout,
					func(_ *fasthttp.Client, req *fasthttp.Request,
						resp *fasthttp.Response, timeout time.Duration) error {
						resp.Header.SetStatusCode(http.StatusInternalServerError)
						return nil
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if got := listenBackNode(tt.args.clusterDiscovery, tt.args.waitTime); got != tt.want {
				t.Errorf("listenBackNode() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getHandler(t *testing.T) {
	stopCh := make(chan struct{})
	alarm.NewClient("", "", stopCh)
	close(stopCh)
	type args struct {
		options *apiServerOptions.APIServerOptions
		stopCh  <-chan struct{}
		c       *config.FaasConfig
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case0 succeed to get handler, clusterDiscovery singleNode", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Discovery, func(_ *cluster.SingleNodeCluster) error {
					return nil
				}),
				gomonkey.ApplyFunc(cluster.GetResourceLockConfig, func() (*cluster.ResourceLockConfig, error) {
					return &cluster.ResourceLockConfig{}, nil
				}),
				gomonkey.ApplyFunc(v1.NewAPIV1,
					func(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}) (*v1.APIV1, error) {
						return &v1.APIV1{FunctionManager: &faasfunction.FunctionAPI{}}, nil
					}),
				gomonkey.ApplyFunc(checkHeartbeats,
					func(clusterDiscovery cluster.Discovery, waitTime time.Duration) {
						return
					}),
			})
			return patches
		}},
		{"case1 failed to get handler, alarm.NewClient error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return errors.New("test")
					}),
			})
			return patches
		}},
		{"case2 failed to get handler, CreatePoolMgr error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case3 failed to get handler, init etcd client error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case4 failed to get handler, CreatePodFetcher error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case5 failed to get handler, CreateMonitoringClient error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case6 failed to get handler, createClusterDiscovery error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(createClusterDiscovery,
					func(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{},
						wg *sync.WaitGroup) (cluster.Discovery, error) {
						return nil, errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case6 failed to get handler, Discovery error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Discovery, func(_ *cluster.SingleNodeCluster) error {
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case7 failed to get handler, GetResourceLockConfig error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Discovery, func(_ *cluster.SingleNodeCluster) error {
					return nil
				}),
				gomonkey.ApplyFunc(cluster.GetResourceLockConfig, func() (*cluster.ResourceLockConfig, error) {
					return nil, errors.New("test")
				}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case8 failed to get handler, setupAPIs error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Discovery, func(_ *cluster.SingleNodeCluster) error {
					return nil
				}),
				gomonkey.ApplyFunc(cluster.GetResourceLockConfig, func() (*cluster.ResourceLockConfig, error) {
					return &cluster.ResourceLockConfig{}, nil
				}),
				gomonkey.ApplyFunc(checkHeartbeats, func(clusterDiscovery cluster.Discovery, waitTime time.Duration) {
					return
				}),
				gomonkey.ApplyFunc(setupAPIs,
					func(options *apiServerOptions.APIServerOptions,
						apiRegistry *APIRegistry, stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case9 failed to get handler, newAPIServerHandler error", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc((*cluster.SingleNodeCluster).Discovery, func(_ *cluster.SingleNodeCluster) error {
					return nil
				}),
				gomonkey.ApplyFunc(cluster.GetResourceLockConfig, func() (*cluster.ResourceLockConfig, error) {
					return &cluster.ResourceLockConfig{}, nil
				}),
				gomonkey.ApplyFunc(checkHeartbeats, func(clusterDiscovery cluster.Discovery, waitTime time.Duration) {
					return
				}),
				gomonkey.ApplyFunc(setupAPIs,
					func(options *apiServerOptions.APIServerOptions,
						apiRegistry *APIRegistry, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(newAPIServerHandler,
					func() (*APIServerHandler, error) {
						return nil, errors.New("")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case failed to InitEnvs", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return errors.New("test")
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, _, err := getHandler(tt.args.options, tt.args.stopCh, tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("getHandler() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestPrintInputLog(t *testing.T) {
	tests := []struct {
		name string
		mock mockUtils.PatchesFunc
	} {
		{
			name: "pool manager is not nil",
			mock: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{gomonkey.ApplyFunc(poolmanager.GetPoolManager,
					func() *poolmanager.PoolMgr {
						return &poolmanager.PoolMgr{}
					})})
				return patches
			},
		},
		{
			name: "pool manager is nil",
			mock: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{gomonkey.ApplyFunc(poolmanager.GetPoolManager,
					func() *poolmanager.PoolMgr {
						return nil
					})})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patch := tt.mock()
			defer patch.ResetAll()
			PrintInputLog()
		})
	}
}

func Test_setupAPIs(t *testing.T) {
	type args struct {
		options     *apiServerOptions.APIServerOptions
		apiRegistry *APIRegistry
		stopCh      <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 failed to NewAPIV1", args{
			options:     apiServerOptions.NewAPIServerOptions(),
			apiRegistry: newAPIRegistry(),
			stopCh:      make(chan struct{}),
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(v1.NewAPIV1,
					func(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}) (*v1.APIV1, error) {
						return nil, errors.New("test")
					}),
				gomonkey.ApplyFunc(clusterstatusutil.InitClusterStatusCache, func() {
					return
				}),
			})
			return patches
		}},

		{"case2 failed to registerAPI", args{
			options:     apiServerOptions.NewAPIServerOptions(),
			apiRegistry: newAPIRegistry(),
			stopCh:      make(chan struct{}),
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
					return &podfetcher.PodFetcher{}
				}),
				gomonkey.ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
					return &vpcmanager.VpcHandler{}, nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
					func() *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				gomonkey.ApplyFunc(utils.WatcherUserAgency, func(stopCh <-chan struct{}) error {
					return nil
				}),
				gomonkey.ApplyFunc((*v1.APIV1).RegisterAPI, func(_ *v1.APIV1, router *gin.Engine) error {
					return errors.New("test")
				}),
				gomonkey.ApplyFunc(clusterstatusutil.InitClusterStatusCache, func() {
					return
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := setupAPIs(tt.args.options, tt.args.apiRegistry, tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("setupAPIs() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_waitUtilInterrupt(t *testing.T) {
	type args struct {
		stopCh chan struct{}
		wg     *sync.WaitGroup
		server *http.Server
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"case1 stopCh nil", args{
				stopCh: nil,
				wg:     &sync.WaitGroup{},
				server: &http.Server{},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(vpcmanager.NotifyVpcControllerExit, func() {
						return
					}),
				})
				return patches
			},
		},
		{
			"case2 stopCh get signal", args{
				stopCh: make(chan struct{}, 1),
				wg:     &sync.WaitGroup{},
				server: &http.Server{},
			},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(vpcmanager.NotifyVpcControllerExit, func() {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if tt.args.stopCh != nil {
				tt.args.stopCh <- struct{}{}
			}
			waitUtilInterrupt(tt.args.stopCh, tt.args.wg, tt.args.server)
			patches.ResetAll()
		})
	}
}

func Test_startWatch(t *testing.T) {
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
			return &poolmanager.PoolMgr{}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&poolmanager.PoolMgr{}), "InitTerminatingLongRun", func(*poolmanager.PoolMgr, []corev1.Pod) {
			return
		}),
		gomonkey.ApplyFunc(cache.RunImageManager, func(_ <-chan struct{}) error {
			return nil
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 monitoring InitMetricsCache error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return errors.New("test")
						}),
				})
				return patches
			}},
		{"case2 GetKubernetesClient nil",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return nil
					}),
				})
				return patches
			}},
		{"case3 InitWorkerManagerDeploy error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy, func() error {
						return errors.New("test")
					}),
				})
				return patches
			}},
		{"case4 InitWorkerCache error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy, func() error {
						return nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc(cache.InitWorkerCache, func(instancePods []corev1.Pod) error {
						return errors.New("test")
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							tmpValue := " {\"cpu\":1000, timestap:xxxx}"
							tmpKey := "/hcs/license/resource/quota/cluster/fgs-yuanrong-az1-01"
							FakeEtcd.kv = make(map[string]string)
							FakeEtcd.kv[tmpKey] = tmpValue
							var kvs []*mvccpb.KeyValue
							for k, v := range FakeEtcd.kv {
								kvs = append(kvs, &mvccpb.KeyValue{
									Key:   []byte(k),
									Value: []byte(v),
								})
							}
							resp := &clientv3.GetResponse{
								Kvs: kvs,
							}
							return resp, nil
						}),
				})
				return patches
			}},
		{"case5 NewEventDrive error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy, func() error {
						return nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc(cache.InitWorkerCache, func(instancePods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(vpcmanager.DeleteRemainedVpcPullTrigger, func() {
						return
					}),
					gomonkey.ApplyFunc(cache.UpdateAllTenantCache,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(cache.UpdateAllFuncDNSDomain,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(reactor.NewEventDrive,
						func(name string, kubeClient kubernetes.Interface, stopCh <-chan struct{}) (*reactor.Reactor, *reactor.Reactor, error) {
							return nil, nil, errors.New("test")
						}),
					gomonkey.ApplyFunc(cache.InitNodeCache, func(pods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							tmpValue := " {\"cpu\":1000, timestap:xxxx}"
							tmpKey := "/hcs/license/resource/quota/cluster/fgs-yuanrong-az1-01"
							FakeEtcd.kv = make(map[string]string)
							FakeEtcd.kv[tmpKey] = tmpValue
							var kvs []*mvccpb.KeyValue
							for k, v := range FakeEtcd.kv {
								kvs = append(kvs, &mvccpb.KeyValue{
									Key:   []byte(k),
									Value: []byte(v),
								})
							}
							resp := &clientv3.GetResponse{
								Kvs: kvs,
							}
							return resp, nil
						}),
				})
				return patches
			}},
		{"case6 checkpoint InitManager error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(checkpoint.InitManager,
						func(_ *fasthttp.Client, stopCh <-chan struct{}) error {
							return errors.New("test")
						}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							tmpValue := " {\"cpu\":1000, timestap:xxxx}"
							tmpKey := "/hcs/license/resource/quota/cluster/fgs-yuanrong-az1-01"
							FakeEtcd.kv = make(map[string]string)
							FakeEtcd.kv[tmpKey] = tmpValue
							var kvs []*mvccpb.KeyValue
							for k, v := range FakeEtcd.kv {
								kvs = append(kvs, &mvccpb.KeyValue{
									Key:   []byte(k),
									Value: []byte(v),
								})
							}
							resp := &clientv3.GetResponse{
								Kvs: kvs,
							}
							return resp, nil
						}),
				})
				return patches
			}},
		{"case7 succeed to startWatch",
			args{stopCh: make(chan struct{}, 1)}, false, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}}, nil
						}),
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(utils.GetPodInformer, func() informercorev1.PodInformer {
						return mockUtils.InformerFakeFactory.Core().V1().Pods()
					}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy, func() error {
						return nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType, func(_ *alarm.Client, almType alarm.Type) {}),
					gomonkey.ApplyFunc(cache.InitWorkerCache, func(instancePods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(vpcmanager.DeleteRemainedVpcPullTrigger, func() {
						return
					}),
					gomonkey.ApplyFunc(vpcmanager.GetVPCController, func() (*vpcmanager.VpcHandler, error) {
						return &vpcmanager.VpcHandler{}, nil
					}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).RecoverPatPodsCache,
						func(_ *vpcmanager.VpcHandler, pods []corev1.Pod) {}),
					gomonkey.ApplyFunc((*vpcmanager.VpcHandler).BecomeLeader,
						func(_ *vpcmanager.VpcHandler) error {
							return nil
						}),
					gomonkey.ApplyFunc(cache.UpdateAllTenantCache,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(cache.UpdateAllFuncDNSDomain,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(reactor.NewEventDrive,
						func(name string, kubeClient kubernetes.Interface, stopCh <-chan struct{}) (*reactor.Reactor, *reactor.Reactor, error) {
							return &reactor.Reactor{}, &reactor.Reactor{}, nil
						}),
					gomonkey.ApplyFunc(checkpoint.InitManager,
						func(_ *fasthttp.Client, stopCh <-chan struct{}) error {
							return nil
						}),
					gomonkey.ApplyFunc(monitoring.ReportMetricsToServer,
						func(stopChan <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(poolmanager.Run,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc((*reactor.Reactor).Run,
						func(_ *reactor.Reactor, stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(cache.InitNodeCache, func(pods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(cache.CheckMinInsAndReport,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(etcdhandler.RegisterToCAEMetaETCD,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(initStateCache, func() {}),
					gomonkey.ApplyFunc(initAllCache, func(_ []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							tmpValue := " {\"cpu\":1000, timestap:xxxx}"
							tmpKey := "/hcs/license/resource/quota/cluster/fgs-yuanrong-az1-01"
							FakeEtcd.kv = make(map[string]string)
							FakeEtcd.kv[tmpKey] = tmpValue
							var kvs []*mvccpb.KeyValue
							for k, v := range FakeEtcd.kv {
								kvs = append(kvs, &mvccpb.KeyValue{
									Key:   []byte(k),
									Value: []byte(v),
								})
							}
							resp := &clientv3.GetResponse{
								Kvs: kvs,
							}
							return resp, nil
						}),
				})
				return patches
			}},
		{"case8 start token rotation error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tokenrotation.StartTokenRotation,
						func(stopCh <-chan struct{}) error {
							return fmt.Errorf("error")
						}),
					gomonkey.ApplyFunc(cache.InitNodeCache, func(pods []corev1.Pod) error {
						return nil
					}),
				})
				return patches
			}},
		{"case9 tenantcommand InitManager error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(tenantcommand.InitManager,
						func(_ *fasthttp.Client, stopCh <-chan struct{}) error {
							return errors.New("test")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := startWatch(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("startWatch() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(100 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_createClusterDiscovery(t *testing.T) {
	type args struct {
		options *apiServerOptions.APIServerOptions
		stopCh  <-chan struct{}
		wg      *sync.WaitGroup
	}
	tests := []struct {
		name        string
		args        args
		want        cluster.Discovery
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 options nil", args{
			options: nil,
			stopCh:  make(chan struct{}, 1),
			wg:      &sync.WaitGroup{},
		}, nil, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
		{"case2 NewResourceLockCluster", args{
			options: &apiServerOptions.APIServerOptions{IsClusterMode: true},
			stopCh:  make(chan struct{}, 1),
			wg:      &sync.WaitGroup{},
		}, &cluster.ResourceLockCluster{}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cluster.NewResourceLockCluster,
					func(listenAddress string, stopCh <-chan struct{},
						resourceLockName string, tlsConfig *tls.Config) (*cluster.ResourceLockCluster, error) {
						return &cluster.ResourceLockCluster{}, nil
					}),
			})
			return patches
		}},
		{"case3 NewResourceLockCluster, enable Https", args{
			options: &apiServerOptions.APIServerOptions{IsClusterMode: true},
			stopCh:  make(chan struct{}, 1),
			wg:      &sync.WaitGroup{},
		}, &cluster.ResourceLockCluster{}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cluster.NewResourceLockCluster,
					func(listenAddress string, stopCh <-chan struct{},
						resourceLockName string, tlsConfig *tls.Config) (*cluster.ResourceLockCluster, error) {
						return &cluster.ResourceLockCluster{}, nil
					}),
				gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
					return true
				}),
				gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
					return &tls.Config{}
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			got, err := createClusterDiscovery(tt.args.options, tt.args.stopCh, tt.args.wg)
			if (err != nil) != tt.wantErr {
				t.Errorf("createClusterDiscovery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createClusterDiscovery() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_startHealthCheckServer(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{"name", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc((*http.Server).ListenAndServe, func(_ *http.Server) error {
					return errors.New("test")
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			startHealthCheckServer()
			patches.ResetAll()
		})
	}
}

func Test_syncConfig(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case_01 get config failed",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, fmt.Errorf("error")
					}),
				})
				return patches
			},
		}, {
			name: "case_02 new config is nil",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, nil
					}),
				})
				return patches
			},
		},
		{
			name: "case_03 poolMgr is nil, not panic",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
					gomonkey.ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
						return nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			syncConfig()
			patches.ResetAll()
		})
	}
}

func Test_processConfigUpdate(t *testing.T) {
	utils.SetSmartWarmerEnable(true)
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "case_01 normal scenario",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(poolmanager.GetPoolManager, func() *poolmanager.PoolMgr {
						return &poolmanager.PoolMgr{}
					}),
					gomonkey.ApplyFunc((*poolmanager.PoolMgr).ProcessPoolSizeUpdate, func(_ *poolmanager.PoolMgr) {
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			processConfigUpdate()
			patches.ResetAll()
		})
	}
}

func Test_initExecution(t *testing.T) {
	stopCh := make(chan struct{})
	alarm.NewClient("", "", stopCh)
	close(stopCh)
	type args struct {
		options *apiServerOptions.APIServerOptions
		stopCh  <-chan struct{}
		c       *config.FaasConfig
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 initExecution GetKubernetesClient nil", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
					return nil
				}),
			})
			return patches
		}},
		{"case2 initExecution initAllModules failed", args{
			options: apiServerOptions.NewAPIServerOptions(),
			stopCh:  make(chan struct{}),
			c:       &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
					return &kubernetes.Clientset{}
				}),
				gomonkey.ApplyFunc(initAllModules, func() error {
					return errors.New("initAllModules error")
				}),
			})
			return patches
		}},
		{"case5 initExecution NewEventDrive snapshot error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy, func() error {
						return nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc(cache.InitWorkerCache, func(instancePods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(vpcmanager.DeleteRemainedVpcPullTrigger, func() {
						return
					}),
					gomonkey.ApplyFunc(cache.UpdateAllTenantCache,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(cache.UpdateAllFuncDNSDomain,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(reactor.NewEventDrive,
						func(name string, kubeClient kubernetes.Interface, stopCh <-chan struct{}) (*reactor.Reactor, *reactor.Reactor, error) {
							return nil, nil, errors.New("test")
						}),
					gomonkey.ApplyFunc(cache.InitNodeCache, func(pods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(initAllModules, func() error {
						return nil
					}),
				})
				return patches
			}},
		{"case6 initExecution checkpoint InitManager error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy, func() error {
						return nil
					}),
					gomonkey.ApplyFunc(cache.InitWorkerCache, func(instancePods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
						return &typecorev1.CoreV1Client{}
					}),
					gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
						return mockUtils.NewK8sPodImp(namespace)
					}),
					gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{Items: []corev1.Pod{{}}}, nil
					}),
					gomonkey.ApplyFunc(vpcmanager.DeleteRemainedVpcPullTrigger, func() {
						return
					}),
					gomonkey.ApplyFunc(cache.UpdateAllTenantCache,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(cache.UpdateAllFuncDNSDomain,
						func(stopChan <-chan struct{}, interval time.Duration) {
							return
						}),
					gomonkey.ApplyFunc(reactor.NewEventDrive,
						func(name string, kubeClient kubernetes.Interface, stopCh <-chan struct{}) (*reactor.Reactor, *reactor.Reactor, error) {
							return &reactor.Reactor{}, &reactor.Reactor{}, nil
						}),
					gomonkey.ApplyFunc(cache.InitNodeCache, func(pods []corev1.Pod) error {
						return nil
					}),
					gomonkey.ApplyFunc(initAllModules, func() error {
						return nil
					}),
					gomonkey.ApplyFunc(poolmanager.Run,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc((*reactor.Reactor).Run,
						func(_ *reactor.Reactor, stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(checkpoint.InitManager,
						func(_ *fasthttp.Client, stopCh <-chan struct{}) error {
							return errors.New("test")
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := initExecution(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("initExecution() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_executeSnapshotModeProcess(t *testing.T) {
	stopCh := make(chan struct{})
	alarm.NewClient("", "", stopCh)
	close(stopCh)
	type args struct {
		stopCh <-chan struct{}
		c      *config.FaasConfig
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 InitEnvs error",
			args{stopCh: make(chan struct{}, 1)}, true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.InitInformer,
						func(stopCh <-chan struct{}) {
							return
						}),
					gomonkey.ApplyFunc(config.InitEnvs, func() error {
						return errors.New("test")
					}),
				})
				return patches
			}},
		{"case2 failed to executeSnapshotModeProcess, alarm.NewClient error", args{
			stopCh: make(chan struct{}),
			c:      &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
				gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
					return false
				}),
				gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
					return &tls.Config{}
				}),
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return errors.New("test")
					}),
			})
			return patches
		}},
		{"case3 failed to executeSnapshotModeProcess, CreatePoolMgr error", args{
			stopCh: make(chan struct{}),
			c:      &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
					return false
				}),
				gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
					return &tls.Config{}
				}),
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case4 failed to executeSnapshotModeProcess, init etcd client error", args{
			stopCh: make(chan struct{}),
			c:      &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
					return false
				}),
				gomonkey.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
					return &tls.Config{}
				}),
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						tmpValue := " {\"cpu\":1000, timestap:xxxx}"
						tmpKey := "/hcs/license/resource/quota/cluster/fgs-yuanrong-az1-01"
						FakeEtcd.kv = make(map[string]string)
						FakeEtcd.kv[tmpKey] = tmpValue
						var kvs []*mvccpb.KeyValue
						for k, v := range FakeEtcd.kv {
							kvs = append(kvs, &mvccpb.KeyValue{
								Key:   []byte(k),
								Value: []byte(v),
							})
						}
						resp := &clientv3.GetResponse{
							Kvs: kvs,
						}
						return resp, nil
					}),
			})
			return patches
		}},
		{"case5 failed to executeSnapshotModeProcess, CreatePodFetcher error", args{
			stopCh: make(chan struct{}),
			c:      &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case6 failed to executeSnapshotModeProcess , CreateMonitoringClient error", args{
			stopCh: make(chan struct{}),
			c:      &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
			})
			return patches
		}},
		{"case6 failed to executeSnapshotModeProcess , initExecution error", args{
			stopCh: make(chan struct{}),
			c:      &config.FaasConfig{LocalAuth: &localauth.AuthConfig{}},
		}, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitInformer,
					func(stopCh <-chan struct{}) {
						return
					}),
				gomonkey.ApplyFunc(config.InitEnvs, func() error {
					return nil
				}),
				gomonkey.ApplyFunc(poolmanager.CreatePoolMgr,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(utils.InitETCDClient,
					func(stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(podfetcher.CreatePodFetcher,
					func(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(monitoring.CreateMonitoringClient,
					func(redisConfig config.MonitoringRedisServerConfig,
						recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return nil
					}),
				gomonkey.ApplyFunc(initExecution, func(stopCh <-chan struct{}) error {
					return errors.New("test")
				}),
			})
			return patches
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := executeSnapshotModeProcess(tt.args.stopCh, tt.args.c); (err != nil) != tt.wantErr {
				t.Errorf("executeSnapshotModeProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_cleanRemainingOnDemandPod(t *testing.T) {
	deleteCount := 0
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
			return &kubernetes.Clientset{}
		}),
		gomonkey.ApplyFunc((*kubernetes.Clientset).CoreV1, func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
			return &typecorev1.CoreV1Client{}
		}),
		gomonkey.ApplyFunc((*typecorev1.CoreV1Client).Pods, func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
			return mockUtils.NewK8sPodImp(namespace)
		}),
		gomonkey.ApplyFunc((*mockUtils.K8sPodImp).List, func(_ *mockUtils.K8sPodImp, ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
			return &corev1.PodList{Items: []corev1.Pod{
				{
					// custom pod
					ObjectMeta: metav1.ObjectMeta{Name: "custom.runtime.gpu-xxx"},
				},
				{
					// pool pod
					ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-xxx"},
				},
			}}, nil
		}),
		gomonkey.ApplyFunc((*mockUtils.K8sPodImp).Delete, func(_ *mockUtils.K8sPodImp, ctx context.Context, name string, opts metav1.DeleteOptions) error {
			deleteCount++
			return nil
		}),
	}

	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	cleanRemainingOnDemandPod()

	assert.Equal(t, 1, deleteCount)
}

func Test_initAllModules(t *testing.T) {
	type test func(ctx context.Context, path, value, operate string) error
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test InitMetricsCache error", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient { return &etcd3.EtcdClient{} }),
					gomonkey.ApplyFunc(registerAlarmTypes, func() {}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return errors.New("error")
						}),
				})
				return patches
			},
		},
		{
			"test InitWorkerManagerDeploy error", true, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient { return &etcd3.EtcdClient{} }),
					gomonkey.ApplyFunc(registerAlarmTypes, func() {}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy,
						func() error {
							return errors.New("error")
						}),
				})
				return patches
			},
		},
		{
			name: "test AddVpaPodFull error", wantErr: true, patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient { return &etcd3.EtcdClient{} }),
					gomonkey.ApplyFunc(registerAlarmTypes, func() {}),
					gomonkey.ApplyFunc(monitoring.InitMetricsCache,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(utils.InitWorkerManagerDeploy,
						func() error {
							return nil
						}),
					gomonkey.ApplyFunc(cleanRemainingOnDemandPod, func() {
						return
					}),
					gomonkey.ApplyFunc(vpa.NewVpaPausePod, func(opts ...vpa.Option) vpa.VerticalPod {
						return &vpa.VerticalPausePod{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&vpa.VerticalPausePod{}), "AddVpaPodFull",
						func(_ *vpa.VerticalPausePod, ctx context.Context) error {
							return errors.New("error")
						}),
					gomonkey.ApplyFunc(utils.GetKubernetesClient, func() kubernetes.Interface {
						return &kubernetes.Clientset{}
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&kubernetes.Clientset{}), "CoreV1",
						func(_ *kubernetes.Clientset) typecorev1.CoreV1Interface {
							return &typecorev1.CoreV1Client{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&typecorev1.CoreV1Client{}), "Pods",
						func(_ *typecorev1.CoreV1Client, namespace string) typecorev1.PodInterface {
							return mockUtils.NewK8sPodImp(namespace)
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&mockUtils.K8sPodImp{}), "List", func(_ *mockUtils.K8sPodImp,
						ctx context.Context, opts metav1.ListOptions) (*corev1.PodList, error) {
						return &corev1.PodList{
							Items: []corev1.Pod{{}},
						}, errors.New("get isPoolPod=false error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := initAllModules(); (err != nil) != tt.wantErr {
				t.Errorf("executeSnapshotModeProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_initAllCache(t *testing.T) {
	tests := []struct {
		name        string
		wantErr     bool
		pods        []corev1.Pod
		patchesFunc mockUtils.PatchesFunc
	}{
		{"test InitMetricsCache error", true, []corev1.Pod{}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.InitStateWorkersFromETCD,
					func() error {
						return errors.New("error")
					}),
			})
			return patches
		},
		},
		{"test cache.InitWorkerCache error", true, []corev1.Pod{}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.InitStateWorkersFromETCD,
					func() error {
						return nil
					}),
				gomonkey.ApplyFunc(cache.InitWorkerCache,
					func(instancePods []corev1.Pod) error {
						return errors.New("error")
					}),
			})
			return patches
		},
		},
		{"test cache.InitWorkerCache error", true, []corev1.Pod{}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.InitStateWorkersFromETCD,
					func() error {
						return nil
					}),
				gomonkey.ApplyFunc(cache.InitWorkerCache,
					func(instancePods []corev1.Pod) error {
						return nil
					}),
				gomonkey.ApplyFunc(cache.InitNodeCache,
					func(instancePods []corev1.Pod) error {
						return errors.New("error")
					}),
			})
			return patches
		},
		},
		{"test all clear", false, []corev1.Pod{}, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(cache.InitStateWorkersFromETCD,
					func() error {
						return nil
					}),
				gomonkey.ApplyFunc(cache.InitWorkerCache,
					func(instancePods []corev1.Pod) error {
						return nil
					}),
				gomonkey.ApplyFunc(cache.InitNodeCache,
					func(instancePods []corev1.Pod) error {
						return nil
					}),
			})
			return patches
		},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := initAllCache(tt.pods); (err != nil) != tt.wantErr {
				t.Errorf("executeSnapshotModeProcess() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_initStateCache(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test InitStatefulCache is 0", func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(cache.InitStatefulCache, func() []cache.PodAction {
						return []cache.PodAction{}
					}),
				})
				return patches
			},
		},
		{
			"test InitStatefulCache is not 0, podfetcher.GetPodFetcher is nil", func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(cache.InitStatefulCache, func() []cache.PodAction {
						return []cache.PodAction{{FuncSvc: "123"}}
					}),
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return nil
					}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).DoActions, func(_ *podfetcher.PodFetcher, funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					}),
				})
				return patches
			},
		},
		{
			"test InitStatefulCache is not 0, podfetcher.GetPodFetcher is not nil", func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(cache.InitStatefulCache, func() []cache.PodAction {
						return []cache.PodAction{{FuncSvc: "123"}}
					}),
					gomonkey.ApplyFunc(podfetcher.GetPodFetcher, func() *podfetcher.PodFetcher {
						return &podfetcher.PodFetcher{}
					}),
					gomonkey.ApplyFunc((*podfetcher.PodFetcher).DoActions, func(_ *podfetcher.PodFetcher, funcSvcID string, actions []cache.Action, metadata cache.Metadata) {
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			initStateCache()
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestOnStoppedLeadingCallback(t *testing.T) {
	convey.Convey("TestOnStoppedLeadingCallback", t, func() {
		onStoppedLeadingCallback()
		convey.So(cache.HasSynced, convey.ShouldBeFalse)
	})
}

func Test_loadConfigFromEtcd(t *testing.T) {
	convey.Convey("Test_loadConfigFromEtcd ok", t, func() {
		caeConfig := &config.CaeConfig{
			CaeBaseImage: "CaeBaseImage",
		}
		bytes, _ := json.Marshal(caeConfig)
		cfg := &config.FaasConfig{
			CAEMetaEtcd: &etcd3.ClientConfig{},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetValuesFromSelectETCD, func(selection etcd3.EtcdType,
				functionKey string, valueType int) ([]string, error) {
				return []string{string(bytes)}, nil
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return cfg, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		loadConfigFromEtcd()
		convey.So(cfg.PoolMgrConfig.PoolContainerTags.CaeBaseImageTag, convey.ShouldEqual, "CaeBaseImage")
	})
	convey.Convey("Test_loadConfigFromEtcd GetValuesFromSelectETCD error", t, func() {
		caeConfig := &config.CaeConfig{
			CaeBaseImage: "CaeBaseImage",
		}
		bytes, _ := json.Marshal(caeConfig)
		cfg := &config.FaasConfig{
			CAEMetaEtcd: &etcd3.ClientConfig{},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetValuesFromSelectETCD, func(selection etcd3.EtcdType,
				functionKey string, valueType int) ([]string, error) {
				return []string{string(bytes)}, errors.New("GetValuesFromSelectETCD error")
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return cfg, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		loadConfigFromEtcd()
		convey.So(cfg.PoolMgrConfig.PoolContainerTags.CaeBaseImageTag, convey.ShouldEqual, "")
	})
	convey.Convey("Test_loadConfigFromEtcd config from etcd empty", t, func() {
		cfg := &config.FaasConfig{
			CAEMetaEtcd: &etcd3.ClientConfig{},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetValuesFromSelectETCD, func(selection etcd3.EtcdType,
				functionKey string, valueType int) ([]string, error) {
				return []string{}, nil
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
				return cfg, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		loadConfigFromEtcd()
		convey.So(cfg.PoolMgrConfig.PoolContainerTags.CaeBaseImageTag, convey.ShouldEqual, "")
	})
	convey.Convey("Test_loadConfigFromEtcd config is nil", t, func() {
		caeConfig := &config.CaeConfig{
			CaeBaseImage: "CaeBaseImage",
		}
		bytes, _ := json.Marshal(caeConfig)
		cfg := &config.FaasConfig{
			CAEMetaEtcd: &etcd3.ClientConfig{},
		}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.GetValuesFromSelectETCD, func(selection etcd3.EtcdType,
				functionKey string, valueType int) ([]string, error) {
				return []string{string(bytes)}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		loadConfigFromEtcd()
		convey.So(cfg.PoolMgrConfig.PoolContainerTags.CaeBaseImageTag, convey.ShouldEqual, "")
	})
}

func Test_registerAlarmTypes(t *testing.T) {
	convey.Convey("Test_registerAlarmTypes ok", t, func() {
		snapshotmanager.SetSnapshotManagerMode(false)
		_ = alarm.NewClient("", constants.InstanceManager, make(<-chan struct{}))
		registerAlarmTypes()
		convey.So(alarm.GetClient().CheckAlarmType(alarm.EtcdOperationFailedContinuously), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.InsufficientEphemeralStorage), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.CheckpointAbnormalContinuously), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.CheckpointConcurrentNumExceedLimit), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.CustomImageSchedulerFailed), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.CheckpointQueueLongTime), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.CheckpointRestoreFailed), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.CreatePodRespFormatErr), convey.ShouldBeTrue)
		convey.So(alarm.GetClient().CheckAlarmType(alarm.MigrateStateIDFailed), convey.ShouldBeFalse)
	})
}

func Test_calculateCurCPUUsage(t *testing.T) {
	realCallAddCPUResourceCount := 0
	type args struct {
		instancePodList *corev1.PodList
	}
	tests := []struct {
		name                    string
		args                    args
		callAddCPUResourceCount int
		patchFunc               mockUtils.PatchesFunc
	}{
		{
			"case01 successfully calculate calculate current CPU usage", args{
				instancePodList: &corev1.PodList{
					Items: []corev1.Pod{
						{Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: utils.RuntimeContainerName,
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%dm", int64(4)))},
									},
								},
							},
						}},
					},
				},
			},
			1,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "fgs-yuanrong-az1-01"
					}),
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							tmpValue := " {\"cpu\":1000, timestap:xxxx}"
							tmpKey := "/hcs/license/resource/quota/cluster/fgs-yuanrong-az1-01"
							FakeEtcd.kv = make(map[string]string)
							FakeEtcd.kv[tmpKey] = tmpValue
							var kvs []*mvccpb.KeyValue
							for k, v := range FakeEtcd.kv {
								kvs = append(kvs, &mvccpb.KeyValue{
									Key:   []byte(k),
									Value: []byte(v),
								})
							}
							resp := &clientv3.GetResponse{
								Kvs: kvs,
							}
							return resp, nil
						}),
					gomonkey.ApplyFunc(utils.AddCPUResource, func(_ int64) {
						realCallAddCPUResourceCount = 1
					}),
				})
				return patches
			},
		},
		{
			"case02 license key is not exist", args{
				instancePodList: &corev1.PodList{
					Items: []corev1.Pod{
						{Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: utils.RuntimeContainerName,
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%dm", int64(4)))},
									},
								},
							},
						}},
					},
				},
			},
			0,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "fgs-yuanrong-az1-01"
					}),
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							var kvs []*mvccpb.KeyValue
							resp := &clientv3.GetResponse{
								Kvs: kvs,
							}
							return resp, nil
						}),
					gomonkey.ApplyFunc(utils.AddCPUResource, func(_ int64) {
						realCallAddCPUResourceCount = 1
					}),
				})
				return patches
			},
		},
		{
			"case03 failed to get quota license from etcd", args{
				instancePodList: &corev1.PodList{
					Items: []corev1.Pod{
						{Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name: utils.RuntimeContainerName,
									Resources: corev1.ResourceRequirements{
										Limits: map[corev1.ResourceName]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%dm", int64(4)))},
									},
								},
							},
						}},
					},
				},
			},
			0,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "fgs-yuanrong-az1-01"
					}),
					gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
						func() *etcd3.EtcdClient {
							return &etcd3.EtcdClient{}
						}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).GetResponse,
						func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
							return nil, errors.New("error")
						}),
					gomonkey.ApplyFunc(utils.AddCPUResource, func(_ int64) {
						realCallAddCPUResourceCount = 1
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		t.Run(tt.name, func(t *testing.T) {
			calculateCurCPUUsage(tt.args.instancePodList)
			if tt.callAddCPUResourceCount != realCallAddCPUResourceCount {
				t.Errorf("calculateCurCPUUsage callAddCPUResourceCount = %d, want %d",
					realCallAddCPUResourceCount, tt.callAddCPUResourceCount)
			}
		})
		patches.ResetAll()
		realCallAddCPUResourceCount = 0
	}
}

func Test_processDecoyConfigUpdate(t *testing.T) {
	realCallRestorePoolCount := 0
	tests := []struct {
		name                 string
		callRestorePoolCount int
		patchFunc            mockUtils.PatchesFunc
	}{
		{
			"case01 success",
			1,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.IsVpaFeatureEnabled, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return &config.FaasConfig{}, nil
					}),
					gomonkey.ApplyFunc((*poolmanager.PoolMgr).RestorePool,
						func(_ *poolmanager.PoolMgr, _ *config.FaasConfig) {
							realCallRestorePoolCount = 1
						}),
				})
				return patches
			},
		},
		{
			"case02 vpa feature enabled",
			0,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.IsVpaFeatureEnabled, func() bool {
						return true
					}),
					gomonkey.ApplyFunc((*poolmanager.PoolMgr).RestorePool,
						func(_ *poolmanager.PoolMgr, _ *config.FaasConfig) {
							realCallRestorePoolCount = 1
						}),
				})
				return patches
			},
		},
		{
			"case03 failed to get new config",
			0,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utils.IsVpaFeatureEnabled, func() bool {
						return false
					}),
					gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("error")
					}),
					gomonkey.ApplyFunc((*poolmanager.PoolMgr).RestorePool,
						func(_ *poolmanager.PoolMgr, _ *config.FaasConfig) {
							realCallRestorePoolCount = 1
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchFunc()
		t.Run(tt.name, func(t *testing.T) {
			processDecoyConfigUpdate()
			if tt.callRestorePoolCount != realCallRestorePoolCount {
				t.Errorf("processDecoyConfigUpdate callRestorePoolCount = %d, want %d",
					realCallRestorePoolCount, tt.callRestorePoolCount)
			}
		})
		patches.ResetAll()
		realCallRestorePoolCount = 0
	}
}
