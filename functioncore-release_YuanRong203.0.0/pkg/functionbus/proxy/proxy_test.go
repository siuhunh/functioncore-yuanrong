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

// Package proxy is used for routing messages between nodes in the cluster
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"functioncore/pkg/functionbus/invokemetric"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/api/workermanager"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/busserver/tcpbusserver"
	"functioncore/pkg/functionbus/proxy/busserver/tlsbusserver"
	"functioncore/pkg/functionbus/proxy/channelbusconn"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/requestdispatcher"
	"functioncore/pkg/functionbus/proxy/statedispatcher/statedispatcher"
	"functioncore/pkg/functionbus/proxy/taskdispatcher"
	"functioncore/pkg/functionbus/proxy/vpa"
	"functioncore/pkg/functionbus/proxy/wmhealth"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	traceID  = "1234"
	tenantID = "TenantID01"
	// stateId         = "state01"
	workerID      = "worker01"
	functionKey   = "TenantID01/function01/latest"
	functionName  = "function01"
	localHostName = "127.0.0.1"
	// remoteHostName  = "127.0.0.2"
	functionVersion = "latest"
)

var (
	patches           testutils.PatchSlice
	unRegisterSuccess bool
)

func TestCreateChannelBus(t *testing.T) {
	convey.Convey("TestCreateChannelBus", t, func() {
		convey.Convey("create channel bus success", func() {
			patches.ResetAll()
			patches = testutils.InitPatchSlice()
			defer patches.ResetAll()
			patches.Append(testutils.PatchSlice{})
			p := &Proxy{}
			err := createChannelBus(p, func(_ bool) {})
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("create channel bus conn failed", func() {
			patches.ResetAll()
			patches = testutils.InitPatchSlice()
			defer patches.ResetAll()
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyFunc(channelbusconn.CreateChannelBusConn,
					func(opt *channelbusconn.ConnOption) (*channelbusconn.ChannelBusConn, error) {
						return nil, errors.New("create channel bus conn failed")
					}),
			})
			p := &Proxy{}
			err := createChannelBus(p, func(_ bool) {})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "create channel bus conn failed")
		})
		convey.Convey("start channel bus conn failed", func() {
			patches.ResetAll()
			patches = testutils.InitPatchSlice()
			defer patches.ResetAll()
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyFunc((*channelbusconn.ChannelBusConn).StartChannelBusConn,
					func(c *channelbusconn.ChannelBusConn) error {
						return errors.New("start channel bus conn failed")
					}),
			})
			p := &Proxy{}
			err := createChannelBus(p, func(_ bool) {})
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "start channel bus conn failed")
		})
	})
}

func TestHandReqWithUpgrading(t *testing.T) {
	busProxy = &Proxy{
		workerDispatcher: nil,
		taskDispatcher:   nil,
	}
	busProxy.isUpgrading = true
	response := busProxy.HandReq("", nil, nil)
	expectResponse := &commontypes.TCPResponse{
		ErrorCode:    proxyerrors.IsUpgradingCode,
		ErrorMessage: proxyerrors.IsUpgradingErr,
	}
	assert.Equal(t, expectResponse, response)
}

// TestRegistry_RegistryCallBack -
func TestRegistry_RegistryCallBack(t *testing.T) {
	convey.Convey("TestRegistryCallBack", t, func() {
		convey.Convey("TestStateIDDel", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&statedispatcher.StateDispatcher{}), "DeleteInstanceByStateID",
					func(_ *statedispatcher.StateDispatcher, stateID string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = &Proxy{
				stateDispatcher: statedispatcher.CreateStateDispatcher(registry),
			}
			registryCallBack(common.DelStateIDEvent, "testKey", false)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("TestDelCAEFunc", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&requestdispatcher.WorkerDispatcher{}), "DeleteFunctionDispatcher",
					func(_ *requestdispatcher.WorkerDispatcher, _ string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = &Proxy{
				workerDispatcher: requestdispatcher.CreateWorkerDispatcher(registry),
			}
			registryCallBack(common.DelFunctionEvent, "testKey", false)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("Test eventType invalid", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&statedispatcher.StateDispatcher{}), "DeleteInstanceByStateID",
					func(_ *statedispatcher.StateDispatcher, stateID string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = &Proxy{
				stateDispatcher: statedispatcher.CreateStateDispatcher(registry),
			}
			registryCallBack("test", "testKey", false)
			convey.So(flag, convey.ShouldBeFalse)
		})

		convey.Convey("Test eventInfo invalid", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&statedispatcher.StateDispatcher{}), "DeleteInstanceByStateID",
					func(_ *statedispatcher.StateDispatcher, stateID string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = &Proxy{
				stateDispatcher: statedispatcher.CreateStateDispatcher(registry),
			}
			registryCallBack(common.DelStateIDEvent, 1, false)
			convey.So(flag, convey.ShouldBeFalse)
		})

		convey.Convey("Test busProxy nil", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&statedispatcher.StateDispatcher{}), "DeleteInstanceByStateID",
					func(_ *statedispatcher.StateDispatcher, stateID string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = nil
			registryCallBack(common.DelStateIDEvent, "testKey", false)
			convey.So(flag, convey.ShouldBeFalse)
		})

		convey.Convey("Test DeleteFunctionDispatcher", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&statedispatcher.StateDispatcher{}), "DeleteStateReqDispatcher",
					func(_ *statedispatcher.StateDispatcher, funcKey string) {
						flag = true
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&requestdispatcher.WorkerDispatcher{}), "DeleteFunctionDispatcher",
					func(_ *requestdispatcher.WorkerDispatcher, functionKey string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = &Proxy{
				stateDispatcher:  statedispatcher.CreateStateDispatcher(registry),
				workerDispatcher: requestdispatcher.CreateWorkerDispatcher(registry),
			}
			registryCallBack(common.DelFunctionEvent, "testKey", false)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("Test UpdateFunctionDispatcher", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&requestdispatcher.WorkerDispatcher{}), "UpdateFunctionDispatcher",
					func(_ *requestdispatcher.WorkerDispatcher, functionKey string) {
						flag = true
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&statedispatcher.StateDispatcher{}), "UpdateFunctionDispatcher",
					func(_ *statedispatcher.StateDispatcher, functionKey string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = &Proxy{
				stateDispatcher:  statedispatcher.CreateStateDispatcher(registry),
				workerDispatcher: requestdispatcher.CreateWorkerDispatcher(registry),
			}
			registryCallBack(common.UpdFunctionEvent, "testKey", false)
			convey.So(flag, convey.ShouldBeTrue)
		})
		convey.Convey("Test DeleteStateQueueByStateID", func() {
			flag := false
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(statedispatcher.DeleteStateQueueByStateID,
					func(stateRouter map[string]string) {
						flag = true
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			registry := servicediscovery.GetRegistry()
			registry.RegisterCallBack(registryCallBack)
			busProxy = &Proxy{
				stateDispatcher:  statedispatcher.CreateStateDispatcher(registry),
				workerDispatcher: requestdispatcher.CreateWorkerDispatcher(registry),
			}
			stateRouters := make(map[string]string, 5)
			stateRouters["a"] = "bbb"
			registryCallBack(common.DelStateIDEventStateQueue, stateRouters, false)
			convey.So(flag, convey.ShouldBeTrue)
		})
	})
}

func buildRequest() *commontypes.TCPRequest {
	return &commontypes.TCPRequest{
		CPU:    "300",
		Memory: "128",
		UserData: map[string]string{
			common.RemoteNotify: "1",
			common.RemoteEnable: "{}",
		},
	}
}

func buildSpecificInstanceIDRequest() *commontypes.TCPRequest {
	tcpReq := buildRequest()
	tcpReq.InstanceID = "evajohnson"
	return tcpReq
}

func buildTestFunctionProperty() *common.FunctionProperty {
	return &common.FunctionProperty{
		InstanceID:      localHostName,
		TenantID:        tenantID,
		FunctionVersion: functionVersion,
		FunctionName:    functionName,
	}
}

func mockRequest(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
	rsp := &commontypes.TCPResponse{
		ErrorCode: proxyerrors.ForwardCode,
	}
	req.Response <- rsp
	return nil
}

func mockSendRequest(_ *channelbusconn.ChannelBusConn, _ string, _ *commontypes.TCPRequest,
	_ *common.FunctionProperty) *commontypes.TCPResponse {
	return &commontypes.TCPResponse{
		ErrorCode: proxyerrors.StateOKCode,
	}
}

func setup() {
	stopCh := make(chan struct{}, 1)
	workerevaluator.CreateInstance(stopCh)

	patches = testutils.InitPatchSlice()

	dispatcher := &requestdispatcher.WorkerDispatcher{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", mockRequest),
		gomonkey.ApplyMethod(reflect.TypeOf(&servicediscovery.Registry{}), "GetFunctionMetadata", func(*servicediscovery.Registry, string) (*servicediscovery.FunctionMetaInfo, error) {
			return nil, errors.New("func meta not found")
		}),
	})

	busConn := &channelbusconn.ChannelBusConn{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(busConn), "SendRequest", mockSendRequest),
	})
}

func tearDown() {
	patches.ResetAll()
}

func TestHandReqWithForwardRequest(t *testing.T) {
	convey.Convey("TestHandReqWithForwardRequest", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		busProxy = &Proxy{
			workerDispatcher: nil,
			taskDispatcher:   nil,
		}
		req := buildRequest()
		property := buildTestFunctionProperty()
		rsp := busProxy.HandReq(traceID, req, property)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
	})
}

func TestHandReqWithRemoteNotifyEnable(t *testing.T) {
	convey.Convey("TestHandReqWithRemoteNotifyEnable", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		busProxy = &Proxy{
			workerDispatcher: nil,
			taskDispatcher:   nil,
		}
		req := buildRequest()
		property := buildTestFunctionProperty()
		property.TenantID = common.BusProxyTenantID
		rsp := busProxy.HandReq(traceID, req, property)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.WorkerBrokerCreateErrCode)
	})
}

func TestHandReqWithRemoteNotifyDisable(t *testing.T) {
	convey.Convey("TestHandReqWithRemoteNotifyDisable", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		busProxy = &Proxy{
			workerDispatcher: nil,
			taskDispatcher:   nil,
		}
		req := buildRequest()
		req.UserData = map[string]string{
			common.RemoteNotify:  "1",
			common.RemoteDisable: "{}",
		}
		property := buildTestFunctionProperty()
		property.TenantID = common.BusProxyTenantID
		rsp := busProxy.HandReq(traceID, req, property)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
	})
}

type mockRegistry struct{}

func (r *mockRegistry) Register() error {
	return nil
}

func (r *mockRegistry) Unregister() error {
	unRegisterSuccess = true
	return nil
}

func initSetup() {
	patches = testutils.InitPatchSlice()

	common.InitDefaultConfig()

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(internal.GetEnvs, func() error {
			return nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(wmhealth.StartWorkerManagerHealthCheck, func(stopCh <-chan struct{}) error {
			return nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(common.InitConfig, func(_ string) error {
			return nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(localauth.DecryptKeys, func(_, _ string) ([]byte, []byte, error) {
			return nil, nil, nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(workerevaluator.CreateMetricsClient, func(_ *common.MetricsServerConfig, _ <-chan struct{}) error {
			return nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.InitEtcdClient, func(_ <-chan struct{}) error {
			return nil
		}),
	})

	bus := &tcpbusserver.TCPBusServer{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "StartServer", func(_ *tcpbusserver.TCPBusServer) error {
			return nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(servicediscovery.StartServiceDiscovery, func(_ <-chan struct{}) error {
			return nil
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(channelbusconn.NewRegistry, func(_ channelbusconn.RegisterInfo, _ <-chan struct{},
			_ common.ETCDConnectHandler) channelbusconn.Interface {

			return &mockRegistry{}
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(waitForShutdown, func(stopCh <-chan struct{}, info channelbusconn.RegisterInfo) {
		}),
	})

	conn := &channelbusconn.ChannelBusConn{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(conn), "StartChannelBusConn",
			func(_ *channelbusconn.ChannelBusConn) error {
				return errors.New("StartChannelBusConn error")
			}),
	})
}

func initTearDown() {
	patches.ResetAll()
}

func TestInitBusProxy(t *testing.T) {
	convey.Convey("TestHandReqWithRemoteNotifyDisable", t, func() {
		initSetup()
		defer func() {
			initTearDown()
		}()

		patch := gomonkey.ApplyFunc(os.Exit, func(code int) {})
		defer func() {
			patch.Reset()
		}()

		err := createAndStartBusServer(nil)
		convey.So(err, convey.ShouldNotBeNil)

		config := BusProxyConfig{}
		stopCh := make(chan struct{}, 1)
		regInfo := channelbusconn.RegisterInfo{}
		checker := &Verification{}

		common.FConfig.LocalAuth = &localauth.AuthConfig{}
		common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
		common.FConfig.IdleWorkerTimeout = 1
		common.FConfig.RequestQueueTimeout = 1
		common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
			Protocol:  "tcp",
			EnableTLS: true,
		}

		err = InitBusProxy(config, stopCh, regInfo, checker)
		convey.So(err, convey.ShouldNotBeNil)

		addObservers()
		convey.So(servicediscovery.EtcdObserverList, convey.ShouldNotBeEmpty)
	})
}

func TestGracefulShutdown(t *testing.T) {
	convey.Convey("TestGracefulShutdown", t, func() {
		initSetup()
		defer func() {
			initTearDown()
		}()

		busProxy = nil
		gracefulShutdown(channelbusconn.RegisterInfo{})
		convey.So(unRegisterSuccess, convey.ShouldBeFalse)

		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(channelbusconn.NewRegistry, func(_ channelbusconn.RegisterInfo, _ <-chan struct{},
				_ common.ETCDConnectHandler) channelbusconn.Interface {
				return &mockRegistry{}
			}),
		})
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&requestdispatcher.WorkerDispatcher{}), "StopAllDispatchers", func(*requestdispatcher.WorkerDispatcher) {
				return
			}),
		})
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(workerbroker.GetInstance()), "DeleteAllWorkerBrokerWithGracefulExit", func(*workerbroker.Manager) {
				return
			}),
		})
		busProxy = &Proxy{}
		gracefulShutdown(channelbusconn.RegisterInfo{})
		time.Sleep(1 * time.Second)

		convey.So(unRegisterSuccess, convey.ShouldBeTrue)
	})
}

func mockStateRequest(p *statedispatcher.StateDispatcher, req *commontypes.DispatcherRequest) error {
	rsp := &commontypes.TCPResponse{
		ErrorCode: proxyerrors.ForwardCode,
	}
	req.Response <- rsp
	return errors.New("error response")
}

func mockTaskRequest(p *taskdispatcher.TaskDispatcher, req *commontypes.DispatcherRequest, dst *common.FunctionProperty) error {
	close(req.Response)
	return nil
}

func TestErrorDispatcher(t *testing.T) {
	convey.Convey("TestErrorDispatcher", t, func() {
		setup()
		defer func() {
			tearDown()
		}()

		stateDispatcher := &statedispatcher.StateDispatcher{}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(stateDispatcher), "Request", mockStateRequest),
		})

		taskdispatcher := &taskdispatcher.TaskDispatcher{}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(taskdispatcher), "Request", mockTaskRequest),
		})

		busProxy = &Proxy{
			workerDispatcher: nil,
			taskDispatcher:   taskdispatcher,
			stateDispatcher:  stateDispatcher,
		}
		req := buildRequest()
		req.CPU = ""
		property := buildTestFunctionProperty()
		functionKey := utils.GetFuncKey(property.TenantID, property.FunctionName, property.FunctionVersion)
		metaInfo := &servicediscovery.FunctionMetaInfo{
			ResourceMetaData: servicediscovery.ResourceMetaData{
				CPU:    500,
				Memory: 128,
			},
		}
		registry := servicediscovery.GetRegistry()
		registry.FuncMeta[functionKey] = metaInfo

		property.StateID = "1"
		rsp := busProxy.HandReq(traceID, req, property)
		convey.So(rsp, convey.ShouldNotBeNil)

		property.StateID = ""
		property.TenantID = common.TaskTenantID
		rsp = busProxy.HandReq(traceID, req, property)
		convey.So(rsp, convey.ShouldNotBeNil)

		busProxy.requestCount = internal.RequestsLimit
		rsp = busProxy.HandReq(traceID, req, property)
		convey.So(rsp, convey.ShouldNotBeNil)
	})
}

func mockSeverSendRequest(server *tlsbusserver.TLSBusServer, nodeID, traceID string, callReq *commontypes.TCPRequest,
	dst *common.FunctionProperty) *commontypes.TCPResponse {
	return nil
}

func TestForwardRequest(t *testing.T) {
	convey.Convey("TestForwardRequest", t, func() {
		req := &commontypes.DispatcherRequest{
			Request: buildRequest(),
			Position: commontypes.Position{
				InstanceID: "",
				NodeID:     localHostName,
			},
			Canceled: true,
		}
		property := buildTestFunctionProperty()

		busserver := &tlsbusserver.TLSBusServer{}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(busserver), "SendRequest", mockSeverSendRequest),
		})

		busProxy = &Proxy{
			busServer: busserver,
		}
		rsp := busProxy.forwardRequest(req, "", property)
		convey.So(rsp, convey.ShouldNotBeNil)
		req.Canceled = false
		rsp = busProxy.forwardRequest(req, "", property)
		convey.So(rsp, convey.ShouldBeNil)
	})
}

func TestErrorInitBusProxy(t *testing.T) {
	convey.Convey("TestErrorInitBusProxy", t, func() {
		config := BusProxyConfig{}
		stopCh := make(chan struct{}, 1)
		regInfo := channelbusconn.RegisterInfo{}
		checker := &Verification{}
		err := InitBusProxy(config, stopCh, regInfo, checker)
		convey.So(err, convey.ShouldBeError)

		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()

		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(internal.GetEnvs, func() error {
				return nil
			}),
			gomonkey.ApplyFunc(common.InitConfig, func(configPath string) error {
				return errors.New("test err")
			}),
		})
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(os.Exit, func(code int) {}),
		})
		err = InitBusProxy(config, stopCh, regInfo, checker)
		convey.So(err, convey.ShouldBeError)
	})
}

func TestWaitForShutdown(t *testing.T) {
	convey.Convey("TestWaitForShutdown", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(channelbusconn.NewRegistry, func(_ channelbusconn.RegisterInfo, _ <-chan struct{},
				_ common.ETCDConnectHandler) channelbusconn.Interface {
				return &mockRegistry{}
			}),
		})
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(os.Exit, func(code int) {}),
		})
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&requestdispatcher.WorkerDispatcher{}), "StopAllDispatchers", func(*requestdispatcher.WorkerDispatcher) {
				return
			}),
		})
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(workerbroker.GetInstance()), "DeleteAllWorkerBrokerWithGracefulExit", func(*workerbroker.Manager) {
				return
			}),
		})
		unRegisterSuccess = false
		waitForShutdown(nil, channelbusconn.RegisterInfo{})
		convey.So(unRegisterSuccess, convey.ShouldBeFalse)
		stopCh := make(chan struct{}, 1)
		busProxy = &Proxy{}
		go waitForShutdown(stopCh, channelbusconn.RegisterInfo{})
		time.Sleep(100 * time.Millisecond)
		close(stopCh)
		time.Sleep(1 * time.Second)
		convey.So(unRegisterSuccess, convey.ShouldBeTrue)
	})
}

func TestStartProxy(t *testing.T) {
	stopCh := make(chan struct{}, 1)
	convey.Convey("TestStartProxy", t, func() {
		convey.Convey("new alarm client error", func() {
			patches.ResetAll()
			patches = testutils.InitPatchSlice()
			defer patches.ResetAll()
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return errors.New("test")
					}),
			})
			err := startProxy(stopCh, func(_ bool) {})
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("init etcd client error", func() {
			patches.ResetAll()
			patches = testutils.InitPatchSlice()
			defer patches.ResetAll()
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyFunc(utils.InitEtcdClient,
					func(stopCh <-chan struct{}) error {
						return errors.New("test")
					}),
			})
			err := startProxy(stopCh, func(_ bool) {})
			convey.So(err, convey.ShouldBeError)
		})
		convey.Convey("init tls config error", func() {
			oldHTTPSCfg := common.FConfig.HTTPSConfig
			common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{HTTPSEnable: true}
			patches.ResetAll()
			patches = testutils.InitPatchSlice()
			defer patches.ResetAll()
			patches.Append(testutils.PatchSlice{
				gomonkey.ApplyFunc(tls.InitTLSConfig,
					func(tlsProtocols string, tlsCiphers string) error {
						return errors.New("test")
					}),
			})
			err := startProxy(stopCh, func(_ bool) {})
			convey.So(err, convey.ShouldBeError)
			common.FConfig.HTTPSConfig = oldHTTPSCfg
		})
	})
}

func Test_initWorkerRuntimeServer(t *testing.T) {
	oldCfg := common.FConfig
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(tcpbus.StartServer, func(opt tcpbus.ServerOption) error {
			return nil
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
		common.FConfig = oldCfg
	}()

	tests := []struct {
		name             string
		bus              *Proxy
		workerServerCfg  *common.BusServerConfig
		runtimeServerCfg *common.BusServerConfig
		expectErr        bool
	}{
		{
			name:             "bus is nil",
			workerServerCfg:  &common.BusServerConfig{},
			runtimeServerCfg: &common.BusServerConfig{},
			expectErr:        true,
		},
		{
			name:             "init worker server failed",
			bus:              &Proxy{},
			runtimeServerCfg: &common.BusServerConfig{},
			expectErr:        true,
		},
		{
			name:            "init runtime server failed",
			bus:             &Proxy{},
			workerServerCfg: &common.BusServerConfig{},
			expectErr:       true,
		},
		{
			name:             "init success",
			bus:              &Proxy{},
			workerServerCfg:  &common.BusServerConfig{},
			runtimeServerCfg: &common.BusServerConfig{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			common.FConfig.BusRuntimeServerConfig = tt.runtimeServerCfg
			common.FConfig.BusWorkerServerConfig = tt.workerServerCfg

			err := initWorkerRuntimeServer(tt.bus)

			if tt.expectErr {
				assert.NotNil(t, err, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
			}
		})
	}
}

func TestUpdateLeaseInfo(t *testing.T) {
	convey.Convey("TestUpdateLeaseInfo", t, func() {
		convey.Convey("succeed to update lease info", func() {
			etcdLeaseCfg := &common.EtcdLeaseConfig{
				LeaseTTL: 50,
				RenewTTL: 50,
			}
			common.FConfig.EtcdLeaseConfig = etcdLeaseCfg
			updateLeaseInfo()
		})
	})
}

func mockActionClientDo(_ *http.Client, _ *http.Request) (*http.Response, error) {
	type testPara struct {
		statusCode int
		body       []byte
		doRsp      error
	}
	var testParaInstance testPara
	body := testParaInstance.body
	return &http.Response{
		StatusCode: testParaInstance.statusCode,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, testParaInstance.doRsp
}

func Test_reportPendingRequestMetrics(t *testing.T) {
	convey.Convey("marshal error", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return []byte{}, errors.New("")
			}),
		})
		pendingRequestView := workermanager.PendingRequestView{
			TenantID:        tenantID,
			FuncName:        "funcName",
			FuncVersion:     "funcVersion",
			BusinessID:      common.InternalScaleBusinessID,
			ReportTimestamp: time.Now().UTC().Unix(),
			PendingRequests: 10,
		}
		view := workermanager.View{
			Type:   workermanager.PendingRequestMetric,
			Object: pendingRequestView,
		}
		reportPendingRequestMetrics(&view)
	})

	convey.Convey("NewRequestWithContext error", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(http.NewRequestWithContext, func(ctx context.Context, method, url string,
				body io.Reader) (*http.Request, error) {
				return nil, errors.New("")
			}),
		})
		pendingRequestView := workermanager.PendingRequestView{
			TenantID:        tenantID,
			FuncName:        "funcName",
			FuncVersion:     "funcVersion",
			BusinessID:      common.InternalScaleBusinessID,
			ReportTimestamp: time.Now().UTC().Unix(),
			PendingRequests: 10,
		}
		view := workermanager.View{
			Type:   workermanager.PendingRequestMetric,
			Object: pendingRequestView,
		}
		reportPendingRequestMetrics(&view)
	})

	convey.Convey("do request err", t, func() {
		client := &http.Client{}
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", func(_ *http.Client,
				_ *http.Request) (*http.Response, error) {
				return nil, errors.New("")
			}),
		})
		pendingRequestView := workermanager.PendingRequestView{
			TenantID:        tenantID,
			FuncName:        "funcName",
			FuncVersion:     "funcVersion",
			BusinessID:      common.InternalScaleBusinessID,
			ReportTimestamp: time.Now().UTC().Unix(),
			PendingRequests: 10,
		}
		view := workermanager.View{
			Type:   workermanager.PendingRequestMetric,
			Object: pendingRequestView,
		}
		reportPendingRequestMetrics(&view)
	})

	convey.Convey("do request no err", t, func() {
		client := &http.Client{}
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo),
		})
		pendingRequestView := workermanager.PendingRequestView{
			TenantID:        tenantID,
			FuncName:        "funcName",
			FuncVersion:     "funcVersion",
			BusinessID:      common.InternalScaleBusinessID,
			ReportTimestamp: time.Now().UTC().Unix(),
			PendingRequests: 10,
		}
		view := workermanager.View{
			Type:   workermanager.PendingRequestMetric,
			Object: pendingRequestView,
		}
		reportPendingRequestMetrics(&view)
	})
}

func Test_periodCollectPendingRequestMetrics(t *testing.T) {

	convey.Convey("collect pending request metric close stopChan", t, func() {
		stopCh := make(chan struct{}, 1)
		common.FConfig.WorkerMetricsTimeWindow = &common.WorkerMetricsTimeWindow{
			TimeWindowLength: 10,
			NumberThreshold:  10,
		}
		go periodCollectPendingRequestMetrics(stopCh)
		time.Sleep(2 * time.Second)
		close(stopCh)
	})
}

func Test_collectPendingRequestMetrics(t *testing.T) {

	convey.Convey("busProxy is nil", t, func() {
		collectPendingRequestMetrics()
		busProxy = &Proxy{
			workerDispatcher: nil,
			taskDispatcher:   nil,
		}
	})

	convey.Convey("workerDispatcher is nil", t, func() {
		busProxy = &Proxy{
			workerDispatcher: nil,
		}
		collectPendingRequestMetrics()
	})

	convey.Convey("pendingRequestMetrics length is zero", t, func() {
		registry := servicediscovery.GetRegistry()
		registry.RegisterCallBack(registryCallBack)
		busProxy = &Proxy{
			workerDispatcher: requestdispatcher.CreateWorkerDispatcher(registry),
		}
		collectPendingRequestMetrics()
	})

	convey.Convey("pendingRequestMetrics length is not zero", t, func() {
		registry := servicediscovery.GetRegistry()
		registry.RegisterCallBack(registryCallBack)
		busProxy = &Proxy{
			workerDispatcher: requestdispatcher.CreateWorkerDispatcher(registry),
		}
		client := &http.Client{}
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*requestdispatcher.WorkerDispatcher).GetPendingRequestsMetrics,
				func(_ *requestdispatcher.WorkerDispatcher) []workermanager.PendingRequestView {
					return []workermanager.PendingRequestView{{FuncName: "aaaaaa"}}
				}),
		})

		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo),
		})
		collectPendingRequestMetrics()
	})
}

func Test_getCPUAndMemoryStrFromCache(t *testing.T) {

	convey.Convey("there is an error to get  resourceMeta", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetResourceValue,
				func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.ResourceMetaData, error) {
					return nil, errors.New("")
				}),
		})
		cpu, memory := getCPUAndMemoryStrFromCache("not-exist")
		convey.ShouldEqual(cpu, "")
		convey.ShouldEqual(memory, "")
	})

	convey.Convey("success to get CPUAndMemoryStrFromCache", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetResourceValue,
				func(_ *servicediscovery.Registry, functionKey string) (*servicediscovery.ResourceMetaData, error) {
					return &servicediscovery.ResourceMetaData{CPU: 10, Memory: 10}, nil
				}),
		})
		cpu, memory := getCPUAndMemoryStrFromCache("not-exist")
		convey.ShouldEqual(cpu, 10)
		convey.ShouldEqual(memory, 10)
	})
}

func Test_genDispatcherRequest(t *testing.T) {
	convey.Convey("Atoi memory err", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		traceID := "1234"
		busCallReq := commontypes.TCPRequest{}
		dst := common.FunctionProperty{
			TenantID:        tenantID,
			FunctionName:    functionName,
			FunctionVersion: functionVersion,
			FutureID:        "1111",
		}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(getCPUAndMemoryStrFromCache,
				func(functionKey string) (string, string) {
					return "0", "10Gi"
				}),
		})
		request := genDispatcherRequest(traceID, &busCallReq, &dst)
		convey.ShouldNotBeNil(request)
	})

	convey.Convey("CPU is not zero", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		traceID := "1234"
		busCallReq := commontypes.TCPRequest{}
		dst := common.FunctionProperty{
			TenantID:        tenantID,
			FunctionName:    functionName,
			FunctionVersion: functionVersion,
			FutureID:        "1111",
		}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(getCPUAndMemoryStrFromCache,
				func(functionKey string) (string, string) {
					return "10", "10"
				}),
		})
		request := genDispatcherRequest(traceID, &busCallReq, &dst)
		convey.ShouldNotBeNil(request)
	})

	convey.Convey("Atoi memory success", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		traceID := "1234"
		busCallReq := commontypes.TCPRequest{}
		dst := common.FunctionProperty{
			TenantID:        tenantID,
			FunctionName:    functionName,
			FunctionVersion: functionVersion,
			FutureID:        "1111",
		}
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(getCPUAndMemoryStrFromCache,
				func(functionKey string) (string, string) {
					return "0", "10"
				}),
		})
		request := genDispatcherRequest(traceID, &busCallReq, &dst)
		convey.ShouldNotBeNil(request)
	})
}

func Test_distributeRequest(t *testing.T) {
	req := commontypes.DispatcherRequest{
		Position: commontypes.Position{},
	}
	dst := common.FunctionProperty{
		TenantID:        tenantID,
		FunctionName:    functionName,
		FunctionVersion: functionVersion,
		FutureID:        "1111",
		StateID:         "stateID",
		InstanceID:      "instanceID",
	}
	convey.Convey("Test stateDispatcher.Request with user error ", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()

		gomonkey.ApplyFunc((*statedispatcher.StateDispatcher).Request,
			func(p *statedispatcher.StateDispatcher, req *commontypes.DispatcherRequest) error {
				return snerror.New(4001, "function entry exception")
			})
		registry := servicediscovery.GetRegistry()
		p := &Proxy{
			stateDispatcher: statedispatcher.CreateStateDispatcher(registry),
		}
		print(p)
		resp := p.distributeRequest(&req, &dst)
		convey.ShouldEqual(resp.ErrorCode, 4001)
		convey.ShouldEqual(resp.ErrorMessage, "function entry exception")
	})

	convey.Convey("Test stateDispatcher.Request with system error ", t, func() {
		patches.ResetAll()
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()

		gomonkey.ApplyFunc((*statedispatcher.StateDispatcher).Request,
			func(p *statedispatcher.StateDispatcher, req *commontypes.DispatcherRequest) error {
				return snerror.New(snerror.InternalError, snerror.InternalErrorMsg)
			})
		registry := servicediscovery.GetRegistry()
		p := &Proxy{
			stateDispatcher: statedispatcher.CreateStateDispatcher(registry),
		}
		resp := p.distributeRequest(&req, &dst)
		convey.ShouldEqual(resp.ErrorCode, proxyerrors.SendReqErrCode)
		convey.ShouldEqual(resp.ErrorMessage, proxyerrors.SendReqErr)
	})
}

func Test_getRetryTimeout(t *testing.T) {
	old := servicediscovery.GetRegistry().FuncMeta
	servicediscovery.GetRegistry().FuncMeta = map[string]*servicediscovery.FunctionMetaInfo{
		"CAE_normal":     {FuncMetaData: servicediscovery.FuncMetaData{Timeout: 20, BusinessType: "CAE"}, ExtendedMetaData: servicediscovery.ExtendedMetaData{Initializer: servicediscovery.Initializer{InitializerTimeout: 3}}},
		"CAE_container":  {FuncMetaData: servicediscovery.FuncMetaData{Timeout: 20, BusinessType: "CAE", IsCustomContainer: true}, ExtendedMetaData: servicediscovery.ExtendedMetaData{Initializer: servicediscovery.Initializer{InitializerTimeout: 3}}},
		"FaaS_container": {FuncMetaData: servicediscovery.FuncMetaData{Timeout: 20, IsCustomContainer: true}, ExtendedMetaData: servicediscovery.ExtendedMetaData{Initializer: servicediscovery.Initializer{InitializerTimeout: 3}}},
		"FaaS_normal":    {FuncMetaData: servicediscovery.FuncMetaData{Timeout: 20}, ExtendedMetaData: servicediscovery.ExtendedMetaData{Initializer: servicediscovery.Initializer{InitializerTimeout: 3}}},
		"FaaS_longrun":   {FuncMetaData: servicediscovery.FuncMetaData{Timeout: 910}, ExtendedMetaData: servicediscovery.ExtendedMetaData{Initializer: servicediscovery.Initializer{InitializerTimeout: 3}}},
	}

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(utils.GetValueFromEtcd, func(key string, etcdSelection etcd3.EtcdType) ([]byte, error) {
			return nil, errors.New("failed to get from etcd")
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
		servicediscovery.GetRegistry().FuncMeta = old
	}()

	tests := []struct {
		name     string
		expected time.Duration
	}{
		{
			name:     "no func",
			expected: defaultRetryTimeout,
		},
		{
			name:     "0/function-task/latest",
			expected: defaultRetryTimeout,
		},
		{
			name:     "CAE_normal",
			expected: (23 + e2eCAEReservedTimeout) * time.Second,
		},
		{
			name:     "CAE_container",
			expected: (23 + e2eCAEReservedTimeout) * time.Second,
		},
		{
			name:     "FaaS_container",
			expected: defaultRetryTimeout,
		},
		{
			name:     "FaaS_normal",
			expected: (23 + e2eFaaSReservedTimeout) * time.Second,
		},
		{
			name:     "FaaS_longrun",
			expected: (903 + e2eFaaSReservedTimeout) * time.Second,
		},
	}

	p := &Proxy{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, p.getRetryTimeout(tt.name))
		})
	}
}

func Test_doRequestWithRetry(t *testing.T) {
	oldRetryTime := atomic.LoadInt32(&common.FConfig.MaxRetryTime)
	oldHostName := internal.HostName
	atomic.StoreInt32(&common.FConfig.MaxRetryTime, 3)
	internal.HostName = "some_host_ip"

	stopCh := make(chan struct{}, 1)
	workerevaluator.CreateInstance(stopCh)

	patches := testutils.InitPatchSlice()

	dispatcher := &requestdispatcher.WorkerDispatcher{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&servicediscovery.Registry{}), "GetFunctionMetadata", func(*servicediscovery.Registry, string) (*servicediscovery.FunctionMetaInfo, error) {
			return nil, errors.New("func meta not found")
		}),
	})

	busConn := &channelbusconn.ChannelBusConn{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(busConn), "SendRequest", mockSendRequest),
	})

	defer func() {
		close(stopCh)
		patches.ResetAll()
		atomic.StoreInt32(&common.FConfig.MaxRetryTime, oldRetryTime)
		internal.HostName = oldHostName
	}()

	busProxy = &Proxy{
		workerDispatcher: nil,
		taskDispatcher:   nil,
	}

	tryCount := 0

	tests := []struct {
		name          string
		callReq       *commontypes.TCPRequest
		dst           *common.FunctionProperty
		patchesFunc   func() testutils.PatchSlice
		expectedCode  uint32
		expectedCount int
	}{
		{
			name:    "user error not retry",
			callReq: buildRequest(),
			dst:     buildTestFunctionProperty(),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", func(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
						tryCount++
						req.Response <- &commontypes.TCPResponse{
							ErrorCode: 4010,
						}
						return nil
					}),
				})
				return patches
			},
			expectedCode:  4010,
			expectedCount: 1,
		},
		{
			name:    "internal error retry 3 times",
			callReq: buildRequest(),
			dst:     buildTestFunctionProperty(),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", func(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
						tryCount++
						req.Response <- &commontypes.TCPResponse{
							ErrorCode: 211424,
						}
						return nil
					}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
				})
				return patches
			},
			expectedCode:  211424,
			expectedCount: 3,
		},
		{
			name:    "retry success",
			callReq: buildRequest(),
			dst:     buildTestFunctionProperty(),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", func(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
						tryCount++
						if tryCount < 3 {
							req.Response <- &commontypes.TCPResponse{
								ErrorCode: 211424,
							}
							return nil
						}
						req.Response <- &commontypes.TCPResponse{
							ErrorCode: 0,
						}
						return nil
					}),
				})
				return patches
			},
			expectedCode:  0,
			expectedCount: 3,
		},
		{
			name:    "retry success with upgrading functiontask",
			callReq: buildRequest(),
			dst:     buildTestFunctionProperty(),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", func(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
						tryCount++
						if tryCount < 3 {
							req.Response <- &commontypes.TCPResponse{
								ErrorCode: 211410,
							}
							return nil
						}
						req.Response <- &commontypes.TCPResponse{
							ErrorCode: 0,
						}
						return nil
					}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
				})
				return patches
			},
			expectedCode:  0,
			expectedCount: 3,
		},
		{
			name:    "stateful reloading error not retry",
			callReq: buildRequest(),
			dst:     buildTestFunctionProperty(),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", func(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
						tryCount++
						req.Response <- &commontypes.TCPResponse{
							ErrorCode: 4223,
						}
						return nil
					}),
				})
				return patches
			},
			expectedCode:  4223,
			expectedCount: 1,
		},
		{
			name:    "context timeout, use last error code",
			callReq: buildRequest(),
			dst:     buildTestFunctionProperty(),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(context.WithTimeout, func(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
						ctx, cancel := context.WithCancel(parent)
						go func() {
							time.Sleep(50 * time.Millisecond)
							cancel()
						}()
						return ctx, cancel
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", func(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
						tryCount++
						if tryCount < 2 {
							req.Response <- &commontypes.TCPResponse{
								ErrorCode: 211424,
							}
							return nil
						}
						req.Response <- &commontypes.TCPResponse{
							ErrorCode: 0,
						}
						return nil
					}),
				})
				return patches
			},
			expectedCode:  211424,
			expectedCount: 1,
		},
		{
			name:    "specific instanceID schedule failed",
			callReq: buildSpecificInstanceIDRequest(),
			dst:     buildTestFunctionProperty(),
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(dispatcher), "Request", func(p *requestdispatcher.WorkerDispatcher, req *commontypes.DispatcherRequest) error {
						tryCount++
						req.Response <- &commontypes.TCPResponse{
							ErrorCode: 4026,
						}
						return nil
					}),
					gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {}),
				})
				return patches
			},
			expectedCode:  4026,
			expectedCount: 3,
		},
	}

	for _, tt := range tests {
		patches := tt.patchesFunc()
		tryCount = 0
		resp := busProxy.doRequestWithRetry(tt.name, tt.callReq, tt.dst)
		assert.Equal(t, tt.expectedCode, resp.ErrorCode, tt.name)
		assert.Equal(t, tt.expectedCount, tryCount, tt.name)
		patches.ResetAll()
	}
}

func TestRespBackPressureNodeIDSet(t *testing.T) {
	req := &types.DispatcherRequest{
		Request:     &types.TCPRequest{},
		Response:    make(chan *commontypes.TCPResponse, 1),
		FunctionKey: functionKey,
		TraceID:     traceID,
	}
	go func(req *types.DispatcherRequest) {
		req.Response <- &types.TCPResponse{
			ErrorCode: constants.BackpressureCode,
			NodeID:    "node01",
		}
	}(req)
	go func(req *types.DispatcherRequest) {
		resp, _ := <-req.Response
		if resp.ErrorCode == constants.BackpressureCode {
			req.Position.InstanceID = common.ProxyBackPressurePrefix + internal.SelfNodeIP
		}
		if resp.ErrorCode == constants.BackpressureCode && resp.NodeID != "" {
			req.Position.NodeID = resp.NodeID
		}
	}(req)
	time.Sleep(100 * time.Millisecond)
	convey.ShouldEqual(t, req.Position.NodeID, "node01")
}

func Test_refreshDynamicResourceFromStateCache(t *testing.T) {
	busCallReq := &types.TCPRequest{}
	dst := &common.FunctionProperty{}
	convey.Convey("stateID is empty", t, func() {
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetDynamicCPUByStateID,
				func(_ *servicediscovery.Registry, stateID string) string {
					return "100"
				}),
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetDynamicMemoryByStateID,
				func(_ *servicediscovery.Registry, stateID string) string {
					return "100"
				}),
		})
		refreshDynamicResourceFromStateCache("traceID", busCallReq, dst)
		convey.ShouldEqual(busCallReq.CPU, "")
		convey.ShouldEqual(busCallReq.Memory, "")
	})
	convey.Convey("function is function-task", t, func() {
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetDynamicCPUByStateID,
				func(_ *servicediscovery.Registry, stateID string) string {
					return "100"
				}),
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetDynamicMemoryByStateID,
				func(_ *servicediscovery.Registry, stateID string) string {
					return "100"
				}),
		})
		dst.FunctionName = "function-task"
		dst.StateID = "StateID"
		refreshDynamicResourceFromStateCache("traceID", busCallReq, dst)
		convey.ShouldEqual(busCallReq.CPU, "")
		convey.ShouldEqual(busCallReq.Memory, "")
	})
	convey.Convey("refresh cpu and memory", t, func() {
		patches = testutils.InitPatchSlice()
		defer patches.ResetAll()
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetDynamicCPUByStateID,
				func(_ *servicediscovery.Registry, stateID string) string {
					return "100"
				}),
			gomonkey.ApplyFunc((*servicediscovery.Registry).GetDynamicMemoryByStateID,
				func(_ *servicediscovery.Registry, stateID string) string {
					return "100"
				}),
		})
		dst.StateID = "1"
		refreshDynamicResourceFromStateCache("traceID", busCallReq, dst)
		convey.ShouldEqual(busCallReq.CPU, "100")
		convey.ShouldEqual(busCallReq.CPU, "100")
	})
}

func TestInitDispatcher(t *testing.T) {
	convey.Convey("provider is nil", t, func() {
		InitDispatcher("", "", nil, true)
	})
}

func Test_periodReportMirroredDiskSpace(t *testing.T) {
	convey.Convey("Test period Report Mirrored Disk Space fail stop channel is nil", t, func() {
		patches := testutils.InitPatchSlice()
		defer patches.ResetAll()
		periodReportMirroredDiskSpace(nil)
	})

	convey.Convey("Test period Report Mirrored Disk Space normal interval", t, func() {
		stopChan := make(chan struct{})
		_ = os.Setenv(reportDiskMetricsInterval, "20")
		patches := testutils.InitPatchSlice()
		defer patches.ResetAll()
		go periodReportMirroredDiskSpace(stopChan)
		time.Sleep(time.Second)
		close(stopChan)
	})

	convey.Convey("Test period Report Mirrored Disk Space invalid interval", t, func() {
		stopChan := make(chan struct{})
		_ = os.Setenv(reportDiskMetricsInterval, "100")
		patches := testutils.InitPatchSlice()
		defer patches.ResetAll()
		go periodReportMirroredDiskSpace(stopChan)
		time.Sleep(time.Second)
		close(stopChan)
	})
}

func Test_collectMirroredDiskSpace(t *testing.T) {
	convey.Convey("Test collect Mirrored Disk Space get failed", t, func() {
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{gomonkey.ApplyFunc(utils.GetMirrorDiskSpace, func() (string, error) {
			return "", errors.New("fail")
		})})
		defer patches.ResetAll()
		collectMirroredDiskSpace()
	})

	convey.Convey("Test collect Mirrored Disk Space get success and marshal fail", t, func() {
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{gomonkey.ApplyFunc(utils.GetMirrorDiskSpace, func() (string, error) {
			return "mock", nil
		})})
		patches.Append(testutils.PatchSlice{gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
			return nil, errors.New("fail")
		})})
		defer patches.ResetAll()
		collectMirroredDiskSpace()
	})

	convey.Convey("Test collect Mirrored Disk Space get success and marshal success", t, func() {
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{gomonkey.ApplyFunc(utils.GetMirrorDiskSpace, func() (string, error) {
			return "10G", nil
		})})
		patches.Append(testutils.PatchSlice{gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(request *http.Request) {
			return
		})})
		patches.Append(testutils.PatchSlice{gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return &http.Client{Timeout: 5 * time.Second}
		})})
		defer patches.ResetAll()
		collectMirroredDiskSpace()
	})
}

func Test_startUploadResourceViewFromSLOAgent(t *testing.T) {
	tests := []struct {
		name            string
		stopCh          chan struct{}
		vpaConfig       *common.VerticalAutoscalingConfig
		stopChannelFunc func(c chan struct{})
		patchesFunc     testutils.PatchesFunc
	}{
		{
			name:      "close vpa feature in config",
			stopCh:    make(chan struct{}),
			vpaConfig: &common.VerticalAutoscalingConfig{},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name:   "close stop channel before syncing resource view",
			stopCh: make(chan struct{}),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable:                    1,
				ResourceViewAlignInterval: 10,
			},
			stopChannelFunc: func(c chan struct{}) {
				close(c)
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				return p
			},
		}, {
			name:   "retrieve resource view from SLO agent error",
			stopCh: make(chan struct{}),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable:                    1,
				ResourceViewAlignInterval: 2,
			},
			stopChannelFunc: func(c chan struct{}) {
				time.Sleep(time.Millisecond * 2000)
				close(c)
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(vpa.GetCurrentResourceView, func(traceID string) ([]byte, snerror.SNError) {
						return nil, snerror.New(proxyerrors.VPASyncResourceViewErrCode, "mock sync resource view error")
					}),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						c <- time.Now()
						return &time.Ticker{C: c}
					}),
				})
				return p
			},
		}, {
			name:   "sync resource view to worker manager error",
			stopCh: make(chan struct{}),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable:                    1,
				ResourceViewAlignInterval: 2,
			},
			stopChannelFunc: func(c chan struct{}) {
				time.Sleep(time.Millisecond * 300)
				close(c)
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(vpa.GetCurrentResourceView, func(traceID string) ([]byte, snerror.SNError) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(vpa.SyncResourceView, func(payload []byte, traceID string) snerror.SNError {
						return snerror.New(proxyerrors.VPASyncResourceViewErrCode, "sync resource view to worker manager error")
					}),
					gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						c <- time.Now()
						return &time.Ticker{C: c}
					}),
				})
				return p
			},
		}, {
			name:   "succeed to sync resource view to worker manager",
			stopCh: make(chan struct{}),
			vpaConfig: &common.VerticalAutoscalingConfig{
				Enable:                    1,
				ResourceViewAlignInterval: 2,
			},
			stopChannelFunc: func(c chan struct{}) {
				time.Sleep(time.Millisecond * 300)
				close(c)
			},
			patchesFunc: func() testutils.PatchSlice {
				p := testutils.InitPatchSlice()
				p.Append(testutils.PatchSlice{
					gomonkey.ApplyFunc(vpa.GetCurrentResourceView, func(traceID string) ([]byte, snerror.SNError) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(vpa.SyncResourceView, func(payload []byte, traceID string) snerror.SNError {
						return nil
					}),
					gomonkey.ApplyFunc(time.NewTicker, func() *time.Ticker {
						c := make(chan time.Time, 1)
						go func() {
							c <- time.Time{}
						}()
						return &time.Ticker{C: c}
					}),
				})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := tt.patchesFunc()
			common.FConfig.VPAConfig = tt.vpaConfig
			go startUploadResourceViewFromSLOAgent(tt.stopCh)
			if tt.stopChannelFunc != nil {
				tt.stopChannelFunc(tt.stopCh)
			}
			p.ResetAll()
		})
	}
}

func Test_hasLoadLatestRev(t *testing.T) {
	convey.Convey("Test hasNotSynced", t, func() {
		ok := hasSynced(etcd3.CAERouter, "/sn/workers")
		convey.So(ok, convey.ShouldBeTrue)
	})
	convey.Convey("Test hasSynced", t, func() {
		servicediscovery.EtcdObserverList = append(servicediscovery.EtcdObserverList, &servicediscovery.EtcdObserver{
			EtcdSelection: etcd3.CAERouter,
			WatchPath:     "/sn/workers",
			HasSynced:     true,
		})
		ok := hasSynced(etcd3.CAERouter, "/sn/workers")
		convey.So(ok, convey.ShouldBeTrue)
	})
}

func Test_recordIndicators(t *testing.T) {
	convey.Convey("Test recordIndicators", t, func() {
		var errorCode uint32
		patches := testutils.InitPatchSlice()
		patches.Append(testutils.PatchSlice{gomonkey.ApplyFunc(invokemetric.RecordIndicators, func(_ string,
			_ *commontypes.TCPRequest, _ *common.FunctionProperty, rsp *commontypes.TCPResponse) {
			errorCode = rsp.ErrorCode
		})})
		defer patches.ResetAll()
		recordIndicators("traceID", nil, nil, nil)
		convey.So(errorCode, convey.ShouldEqual, proxyerrors.InternalErrCode)
	})
}

func TestProxy_alarm(t *testing.T) {
	convey.Convey("Test Proxy_alarm", t, func() {
		stopCh := make(chan struct{})
		defer close(stopCh)
		alarm.NewClient(common.FConfig.AlarmServerAddress, constants.FunctionTask, stopCh)
		defer gomonkey.ApplyMethod(alarm.GetClient(), "ReportAlarm",
			func(c *alarm.Client, almType alarm.Type, specAddition string) {
				fmt.Printf("alarm %d %s\n", almType, specAddition)
				return
			}).Reset()
		defer gomonkey.ApplyMethod(alarm.GetClient(), "ClearAlarm",
			func(c *alarm.Client, almType alarm.Type) {
				fmt.Printf("clear alarm %d \n", almType)
			}).Reset()
		convey.Convey("alarmed", func() {
			p := Proxy{}
			p.alarm(true, "123456")
			_, ok := p.alarmedTenant.Load("123456")
			convey.So(ok, convey.ShouldBeTrue)

			p.alarm(false, "123456")
			_, ok = p.alarmedTenant.Load("123456")
			convey.So(ok, convey.ShouldBeFalse)
		})
	})
}
