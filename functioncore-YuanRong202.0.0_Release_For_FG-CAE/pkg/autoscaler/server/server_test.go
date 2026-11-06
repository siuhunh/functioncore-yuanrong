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

// Package options define server
package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/instance"
	"functioncore/pkg/autoscaler/metrics"
	"functioncore/pkg/autoscaler/options"
	"functioncore/pkg/autoscaler/reactor"
	"functioncore/pkg/autoscaler/resourceusage"
	"functioncore/pkg/autoscaler/scaler"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	mockUtils "functioncore/test/utils"
)

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
		{"test http enable", func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "127.0.0.1"
				}),
				gomonkey.ApplyFunc(config.EnableHTTPS, func() bool {
					return true
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServeTLS",
					func(_ *http.Server, certFile, keyFile string) error {
						return errors.New("failed to start tls server")
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			startServer()
			patches.ResetAll()
		})
	}
}

func TestRun(t *testing.T) {
	t.Run("success running!", func(t *testing.T) {
		// Mock the necessary functions and objects
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(govalidator.ValidateStruct, func(interface{}) (bool, error) {
				return true, nil
			}),
			gomonkey.ApplyFunc(config.InitConfigPath, func(configFile string) {
			}),
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{LocalAuth: &localauth.AuthConfig{
					AKey: "test1",
					SKey: "test2",
				}}, nil
			}),
			gomonkey.ApplyFunc(config.WatchConfig,
				func(stopCh <-chan struct{}, callback config.ChangedCallback) error {
					return nil
				}),
			gomonkey.ApplyFunc(alarm.NewClient,
				func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
					return nil
				}),
			gomonkey.ApplyFunc(metrics.CreateMonitoringClient,
				func(redisConfig config.MetricServerConfig, stopCh <-chan struct{}) error {
					return nil
				}),
			gomonkey.ApplyFunc(utils.InitETCDClient,
				func(stopCh <-chan struct{}) error {
					return nil
				}),
			gomonkey.ApplyFunc(initAllModules, func() error {
				return nil
			}),
			gomonkey.ApplyFunc(startServer, func() {
				return
			}),
			gomonkey.ApplyFunc(startSelecterMasterNode, func(stopCh <-chan struct{}) {
				return
			}),
			gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
				func(a *alarm.Client, almType alarm.Type) {
					return
				}),
		})

		// Create a stop channel for the test
		stopCh := make(chan struct{})

		// Run the Run function with mocked dependencies
		err := Run(&options.AutoScalerOptions{}, stopCh)
		if err != nil {
			t.Errorf("Expected nil error, got %v", err)
		}

		// Close the stop channel to avoid goroutine leaks
		patches.ResetAll()
		t.Cleanup(func() {
			close(stopCh)
		})
	})
}

func TestStartMasterNodeJob(t *testing.T) {
	t.Run("success running!", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
				func(a *alarm.Client, almType alarm.Type) {
					return
				}),
			gomonkey.ApplyFunc(scaler.CreateScaleMgr, func(stopCh <-chan struct{}) error {
				return nil
			}),
			gomonkey.ApplyFunc(resourceusage.GetHandler().HandlerResourceUsage, func() {
				return
			}),
			gomonkey.ApplyFunc((*reactor.Reactor).Run, func(_ *reactor.Reactor, stopCh <-chan struct{}) {
				return
			}),
			gomonkey.ApplyFunc(reactor.NewEventDrive, func() (*reactor.Reactor, error) {
				return &reactor.Reactor{}, nil
			}),
			gomonkey.ApplyFunc(cache.RecoverPersistence, func() {
				return
			}),
			gomonkey.ApplyFunc(startInsCacheTicker, func(stopCh <-chan struct{}) {
				return
			}),
			gomonkey.ApplyFunc(instance.CheckSpecialReBalanceKey, func(stopCh <-chan struct{}) {
				return
			}),
		})
		stopCh := make(chan struct{})
		conf := &config.Config{
			Clusters:   []string{"cluster1"},
			TickerTime: 10,
			SyncPeriod: 40,
		}
		err := startMasterNodeJob(stopCh, conf)
		assert.NoError(t, err, "startMasterNodeJob should not return an error")
		patches.ResetAll()
		t.Cleanup(func() {
			close(stopCh)
		})
	})

	t.Run("NewEventDrive fail", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(reactor.NewEventDrive, func() (*reactor.Reactor, error) {
				return nil, errors.New("Test error")
			}),
			gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
				func(a *alarm.Client, almType alarm.Type) {
					return
				}),
			gomonkey.ApplyFunc(startInsCacheTicker, func(stopCh <-chan struct{}) {
				return
			}),
		})
		stopCh := make(chan struct{})
		conf := &config.Config{
			Clusters:   []string{"cluster1"},
			TickerTime: 10,
			SyncPeriod: 40,
		}
		err := startMasterNodeJob(stopCh, conf)
		assert.Error(t, err, "startMasterNodeJob should have an error")
		t.Cleanup(func() {
			close(stopCh)
		})
		patches.ResetAll()
	})

	t.Run("CreateScaleMgr fail", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
				func(a *alarm.Client, almType alarm.Type) {
					return
				}),
			gomonkey.ApplyFunc(scaler.CreateScaleMgr, func(stopCh <-chan struct{}) error {
				return errors.New("Test error")
			}),
			gomonkey.ApplyFunc(resourceusage.GetHandler().HandlerResourceUsage, func() {
				return
			}),
			gomonkey.ApplyFunc((*reactor.Reactor).Run, func(_ *reactor.Reactor, stopCh <-chan struct{}) {
				return
			}),
			gomonkey.ApplyFunc(reactor.NewEventDrive, func() (*reactor.Reactor, error) {
				return &reactor.Reactor{}, nil
			}),
		})
		stopCh := make(chan struct{})
		conf := &config.Config{
			Clusters:   []string{"cluster1"},
			TickerTime: 10,
			SyncPeriod: 40,
		}
		err := startMasterNodeJob(stopCh, conf)
		assert.Error(t, err, "startMasterNodeJob should have an error")
		t.Cleanup(func() {
			close(stopCh)
		})
		patches.ResetAll()
	})
}

func TestStartInsCacheTicker(t *testing.T) {
	t.Run("success run", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{SyncPeriod: 10}, nil
			}),
			gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
				func(_ *alarm.Client, almType alarm.Type) {
					return
				}),
		})
		stopCh := make(chan struct{})

		go func() {
			time.Sleep(5 * time.Second)
			close(stopCh)
		}()

		startInsCacheTicker(stopCh)

		patches.ResetAll()
	})

	t.Run("get config error", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
				return &config.Config{SyncPeriod: 10}, errors.New("test error")
			}),
			gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
				func(_ *alarm.Client, almType alarm.Type) {
					return
				}),
		})
		stopCh := make(chan struct{})

		go func() {
			time.Sleep(5 * time.Second)
			close(stopCh)
		}()

		startInsCacheTicker(stopCh)

		patches.ResetAll()
	})
}

func Test_initAllModules(t *testing.T) {
	tests := []struct {
		name        string
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			"test initAllModules",
			false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(cache.InitAutoScalerCache, func() error {
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
			if err := initAllModules(); (err != nil) != tt.wantErr {
				t.Errorf("initAllModules() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_switchFlow(t *testing.T) {
	type args struct {
		c *gin.Context
	}
	context, _ := gin.CreateTestContext(httptest.NewRecorder())
	jsonBytes, _ := json.Marshal(SwitchFlowRequest{})
	context.Request, _ = http.NewRequest("POST", "/switchflow", bytes.NewReader(jsonBytes))
	var a args
	a.c = context
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"request auth check success", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(initTraceID, func(r *http.Request) string {
					return uuid.New().String()
				}),
				gomonkey.ApplyFunc(requestAuthCheck, func(r *http.Request, w http.ResponseWriter, traceID string) bool {
					return false
				}),
			})
			return patches
		}},
		{"request auth check fail", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(initTraceID, func(r *http.Request) string {
					return uuid.New().String()
				}),
				gomonkey.ApplyFunc(requestAuthCheck, func(r *http.Request, w http.ResponseWriter, traceID string) bool {
					return true
				}),
			})
			return patches
		}},
		{"read body fail", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(initTraceID, func(r *http.Request) string {
					return uuid.New().String()
				}),
				gomonkey.ApplyFunc(requestAuthCheck, func(r *http.Request, w http.ResponseWriter, traceID string) bool {
					return false
				}),
				gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
					return nil, errors.New("aaa")
				}),
			})
			return patches
		}},
		{"response marshal fail", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(initTraceID, func(r *http.Request) string {
					return uuid.New().String()
				}),
				gomonkey.ApplyFunc(requestAuthCheck, func(r *http.Request, w http.ResponseWriter, traceID string) bool {
					return false
				}),
				gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("aaa")
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			switchFlow(tt.args.c)
		})
		patches.ResetAll()
	}
}

func Test_initTraceID(t *testing.T) {
	type args struct {
		r *http.Request
	}
	var a args
	request := &http.Request{Header: map[string][]string{}}
	a.r = request

	var a2 args
	request2 := &http.Request{Header: map[string][]string{}}
	request2.Header.Set(constants.HeaderTraceID, "aaa")
	a2.r = request2
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
		{"case2", a2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			initTraceID(tt.args.r)
		})
	}
}

func Test_requestAuthCheck(t *testing.T) {
	type args struct {
		r       *http.Request
		w       http.ResponseWriter
		traceID string
	}
	var a args
	tests := []struct {
		name        string
		args        args
		want        bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"auth check fail", a, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(authCheck, func(r *http.Request) error {
					return errors.New("aaa")
				}),
				gomonkey.ApplyFunc(sendResponseAndWriteLog, func(code, iCode int, message, traceID string,
					w http.ResponseWriter) {
					return
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, requestAuthCheck(tt.args.r, tt.args.w, tt.args.traceID), "requestAuthCheck(%v, %v, %v)", tt.args.r, tt.args.w, tt.args.traceID)
		})
		patches.ResetAll()
	}
}

func Test_authCheck(t *testing.T) {
	type args struct {
		r *http.Request
	}
	var a args
	request := &http.Request{}
	a.r = request
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"AuthenticationEnable fail", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
					return nil, errors.New("aaa")
				}),
			})
			return patches
		}},
		{"AuthenticationEnable false", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
					return &config.Config{AuthenticationEnable: false}, nil
				}),
			})
			return patches
		}},
		{"AuthCheckLocally fail", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
					return &config.Config{AuthenticationEnable: true, LocalAuth: &localauth.AuthConfig{
						AKey:     "",
						SKey:     "",
						Duration: 0,
					}}, nil
				}),
				gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string, timestamp string, duration int) error {
					return errors.New("aaa")
				}),
			})
			return patches
		}},
		{"AuthCheckLocally success", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Config, error) {
					return &config.Config{AuthenticationEnable: true, LocalAuth: &localauth.AuthConfig{
						AKey:     "",
						SKey:     "",
						Duration: 0,
					}}, nil
				}),
				gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string, timestamp string, duration int) error {
					return nil
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			authCheck(tt.args.r)
		})
		patches.ResetAll()
	}
}

func Test_sendResponseAndWriteLog(t *testing.T) {
	type args struct {
		code    int
		iCode   int
		message string
		traceID string
		w       http.ResponseWriter
	}
	var a args
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"marshal response error", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("aaa")
				}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			sendResponseAndWriteLog(tt.args.code, tt.args.iCode, tt.args.message, tt.args.traceID, tt.args.w)
		})
		patches.ResetAll()
	}
}

func Test_handleFlowOutRequest(t *testing.T) {
	type args struct {
		switchFlowRequest SwitchFlowRequest
		traceID           string
	}
	var request SwitchFlowRequest
	clusterManager := &clustermgr.ClusterManager{}
	request.SwitchType = "FLOW_OUT"
	request.ClusterID = "cluster002"
	urnList := map[string]map[string]int{}
	list := map[string]int{}
	list["cluster001"] = 1
	list["cluster002"] = 2
	list["cluster003"] = 0
	list["cluster004"] = -1
	list2 := map[string]int{}
	list2["cluster001"] = 1
	list2["cluster002"] = 2
	list2["cluster003"] = 0
	list2["cluster004"] = -1
	urnList["sn:cn:yrk:6d5b16f6ef0e4b7d938d5035356aa378:function:0@default@app1:latest"] = list
	urnList["sn:cn:yrk:6d5b16f6ef0e4b7d938d5035356aa378:function:0@default@app2:latest"] = list2
	request.UrnList = urnList
	var a args
	a.switchFlowRequest = request
	a.traceID = uuid.New().String()
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"handle flow out request", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(clustermgr.GetClusterMgr, func() *clustermgr.ClusterManager {
					return clusterManager
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(clusterManager), "GetClusterInfo",
					func(_ *clustermgr.ClusterManager, clusterID string) clustermgr.Cluster {
						return clustermgr.Cluster{
							ClusterID: "cluster001",
							ActiveNodes: clustermgr.NodeStatus{
								IP: "10.2.3.1",
							},
						}
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			handleFlowOutRequest(tt.args.switchFlowRequest, tt.args.traceID)
		})
		patches.ResetAll()
	}
}

func Test_handleFlowInRequest(t *testing.T) {
	type args struct {
		switchFlowRequest SwitchFlowRequest
		traceID           string
	}
	var request SwitchFlowRequest
	clusterManager := &clustermgr.ClusterManager{}
	request.SwitchType = "FLOW_IN"
	request.ClusterID = "cluster002"
	urnList := map[string]map[string]int{}
	list := map[string]int{}
	list["cluster001"] = 1
	list["cluster002"] = 2
	list["cluster003"] = 0
	list["cluster004"] = -1
	list2 := map[string]int{}
	list2["cluster001"] = 1
	list2["cluster002"] = 2
	list2["cluster003"] = 0
	list2["cluster004"] = -1
	urnList["sn:cn:yrk:6d5b16f6ef0e4b7d938d5035356aa378:function:0@default@app1:latest"] = list
	urnList["sn:cn:yrk:6d5b16f6ef0e4b7d938d5035356aa378:function:0@default@app2:latest"] = list2
	request.UrnList = urnList
	var a args
	a.switchFlowRequest = request
	a.traceID = uuid.New().String()
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"handle flow in request", a, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(clustermgr.GetClusterMgr, func() *clustermgr.ClusterManager {
					return clusterManager
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(clusterManager), "GetClusterInfo",
					func(_ *clustermgr.ClusterManager, clusterID string) clustermgr.Cluster {
						return clustermgr.Cluster{
							ClusterID: "cluster001",
							ActiveNodes: clustermgr.NodeStatus{
								IP: "10.2.3.1",
							},
						}
					}),
			})
			return patches
		}},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			handleFlowInRequest(tt.args.switchFlowRequest, tt.args.traceID)
		})
		patches.ResetAll()
	}
}
