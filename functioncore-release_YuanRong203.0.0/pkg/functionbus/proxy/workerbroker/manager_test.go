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

package workerbroker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/test/utils"
)

type ManagerSetupConfig struct {
	callRspCode uint32
	err         error
}

const (
	funcKey             = "7e186a/0@base@testresourcepython36768/latest"
	funcKeyWithResource = "7e186a/0@base@testresourcepython36768/latest/800-768"
	workerID            = "defaultaz-#-pool24-600-512-fusion-575dd88b5b-gw422"
	workerAddr          = "127.0.0.1:22668"
	podUID              = "5e8c9436-d487-4eb4-9657-7603a49b22e2"
)

var (
	patches utils.PatchSlice
	config  = &BrokerConfig{
		WorkerID:            workerID,
		FuncKeyWithResource: funcKeyWithResource,
		WorkerAddr:          workerAddr,
		CreateEventChan:     false,
	}
	defaultConfig = &ManagerSetupConfig{
		callRspCode: proxyerrors.StateOKCode,
		err:         nil,
	}
	containerIDs = []string{
		"docker://a8e548dbaa8b98a9cb3240d29854de42bdb652f7a26a2dd0729ba8315fdcf475",
		"docker://cb589b243a570f9fbf2479285efed31fd36abb2d06db0f9dccb5eaafae5d123b",
	}
)

func setup(config *ManagerSetupConfig) {
	workerevaluator.CreateInstance(nil)

	patches = utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyFunc(tcpbus.BuildClientConnection,
			func(_ tcpbus.ClientOption) (*tcpbus.Connection, error) {
				return &tcpbus.Connection{}, nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&servicediscovery.Registry{}), "GetFunctionMetadata", func(*servicediscovery.Registry, string) (*servicediscovery.FunctionMetaInfo, error) {
			return nil, errors.New("func meta not found")
		}),
	})

	bus := &tcpbus.TCPBus{}
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "Call",
			func(_ *tcpbus.TCPBus, _ context.Context, _ *common.FunctionProperty, req *types.TCPRequest,
				_ bool) (*types.TCPResponse, error) {
				if config.err == nil {
					return &types.TCPResponse{
						RequestID:    req.RequestID,
						ErrorCode:    config.callRspCode,
						ErrorMessage: "",
					}, config.err
				} else {
					return nil, config.err
				}
			}),
	})

	patches.Append(utils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(bus), "Cancel",
			func(_ *tcpbus.TCPBus, _ context.Context, _ *common.CancelProperty,
				req *types.TCPRequest) (*types.TCPResponse, error) {
				return &types.TCPResponse{
					RequestID:    req.RequestID,
					ErrorCode:    proxyerrors.StateOKCode,
					ErrorMessage: "",
				}, nil
			}),
	})
	m := Manager{}
	patches.Append(utils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(&m), "Recover", mockRecover)})

}

func tearDown() {
	patches.ResetAll()
}

// 1.first add a local worker broker
// 2.check get worker broker success
func TestCreateWorkerBrokerWithLocal(t *testing.T) {
	convey.Convey("TestCreateWorkerBrokerWithValidConfig", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)
		workerBroker := m.getWorkerBroker(config.WorkerID)
		convey.So(workerBroker, convey.ShouldNotBeNil)
		m.DeleteWorkerBroker(funcKeyWithResource, workerID, workerAddr, false)
	})
}

func TestCreateWorkerBrokerWithDuplicatedWorker(t *testing.T) {
	convey.Convey("TestCreateWorkerBrokerWithDuplicatedWorker", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)
		workerBroker := m.getWorkerBroker(config.WorkerID)
		convey.So(workerBroker, convey.ShouldNotBeNil)
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)
	})
}

func TestCreateWorkerBrokerWithDifferrentAddr(t *testing.T) {
	convey.Convey("TestCreateWorkerBrokerWithDifferrentAddr", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)
		newConfig := &BrokerConfig{
			WorkerID:            workerID,
			FuncKeyWithResource: funcKeyWithResource,
			WorkerAddr:          "127.0.0.1:22669",
		}
		convey.So(m.CreateWorkerBroker(newConfig), convey.ShouldBeNil)
	})
}

// 1.first add a remote worker broker
// 2.delete remote worker
// 3.check remote worker not exist
func TestCreateRemoteBrokerWithValidConfig(t *testing.T) {
	convey.Convey("TestCreateRemoteBrokerWithValidConfig", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		err := m.CreateRemoteBroker(funcKeyWithResource, workerID, "127.0.0.2", "")
		convey.So(err, convey.ShouldBeNil)
		m.DeleteRemoteBroker(funcKeyWithResource, workerID)
		_, ok := m.workerBrokers.Load(workerID)
		convey.So(ok, convey.ShouldEqual, false)
	})
}

func TestProcessWithNotExistWorker(t *testing.T) {
	convey.Convey("TestProcessWithNotExistWorker", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		m.Process(workerID, req)
		rsp := <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.SendReqErrCode)
	})
}

func TestProcessWithLocalBrokers(t *testing.T) {
	convey.Convey("TestProcessWithLocalBrokers", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)

		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		m.Process(workerID, req)
		rsp := <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
	})
}

func TestProcessWithLocalBrokersExitError(t *testing.T) {
	convey.Convey("TestProcessWithLocalBrokers", t, func() {
		setupConfig := &ManagerSetupConfig{
			callRspCode: proxyerrors.ClientExitErrCode,
		}
		setup(setupConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)

		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		m.Process(workerID, req)
		rsp := <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func TestProcessWithLocalBrokersReturnStateAccepted(t *testing.T) {
	convey.Convey("TestProcessWithLocalBrokersReturnStateAccepted", t, func() {
		setupConfig := &ManagerSetupConfig{
			callRspCode: proxyerrors.StateAcceptedCode,
		}
		setup(setupConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)

		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		m.Process(workerID, req)
		rsp := <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func TestProcessWithLocalBrokersCallError(t *testing.T) {
	convey.Convey("TestProcessWithLocalBrokers", t, func() {
		setupConfig := &ManagerSetupConfig{
			err: errors.New("call error"),
		}
		setup(setupConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)

		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		m.Process(workerID, nil)
		m.Process(workerID, req)
		rsp := <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func TestGetWorkerBrokerCPUOverloaded(t *testing.T) {
	convey.Convey("TestGetWorkerBrokerCPUOverloaded", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)
		workerBroker := m.getWorkerBroker(config.WorkerID)
		convey.So(workerBroker, convey.ShouldNotBeNil)
		workerBroker.storeResourceUsage(common.ResourceUsage{
			CPUUsage: 0.9})

		isCPUOverloaded := workerBroker.getCPUOverloaded()
		convey.So(isCPUOverloaded, convey.ShouldBeFalse)
		isOverloadedFlag := m.GetWorkerBrokerCPUOverloaded(config.WorkerID)
		convey.So(isOverloadedFlag, convey.ShouldBeFalse)
	})
}

func remoteCancel(_, _ string, _ *types.TCPRequest, _ *common.CancelProperty) *types.TCPResponse {
	return &types.TCPResponse{
		RequestID:    1,
		ErrorCode:    proxyerrors.StateOKCode,
		ErrorMessage: "",
	}
}

func TestProcessWithRemoteBrokers(t *testing.T) {
	convey.Convey("TestProcessWithRemoteBrokers", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		backupCall := common.SendCallReq
		actualDtsInstanceID := ""
		common.SendCallReq = func(_, _ string, _ *types.TCPRequest, dts *common.FunctionProperty) *types.TCPResponse {
			actualDtsInstanceID = dts.InstanceID
			return &types.TCPResponse{
				RequestID:    1,
				ErrorCode:    proxyerrors.StateOKCode,
				ErrorMessage: "",
			}
		}
		defer func() {
			common.SendCallReq = backupCall
		}()

		m := Manager{}
		err := m.CreateRemoteBroker(funcKeyWithResource, workerID, "127.0.0.2", "")
		convey.So(err, convey.ShouldBeNil)

		req := &types.DispatcherRequest{
			Request:  &types.TCPRequest{},
			Response: make(chan *types.TCPResponse, 1),
		}
		m.Process(workerID, req)
		rsp := <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)

		m.ProcessRemote("", workerID, req)
		rsp = <-req.Response
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
		convey.So(actualDtsInstanceID, convey.ShouldEqual, common.ProxyReqPrefix+workerID)
	})
}

// TestManager_loadOrStoreWorkerBroker1 -
func TestManager_loadOrStoreWorkerBroker1(t *testing.T) {
	convey.Convey("test: broker exists in cache and is valid", t, func() {
		m := &Manager{}
		m.workerBrokers.Store(workerID, &workerBroker{
			workerID:   workerID,
			workerAddr: workerAddr,
		})
		newBroker := &workerBroker{
			workerID:   workerID,
			workerAddr: "127.0.0.1:22669",
		}
		broker, loaded := m.loadOrStoreWorkerBroker(newBroker)
		convey.So(loaded, convey.ShouldBeTrue)
		convey.So(broker.workerAddr, convey.ShouldEqual, workerAddr)
	})
}

// TestManager_loadOrStoreWorkerBroker2 -
func TestManager_loadOrStoreWorkerBroker2(t *testing.T) {
	convey.Convey("test: broker exists in cache but is invalid", t, func() {
		m := &Manager{}
		m.workerBrokers.Store(workerID, "")
		newBroker := &workerBroker{
			workerID:   workerID,
			workerAddr: "127.0.0.1:22669",
		}
		broker, loaded := m.loadOrStoreWorkerBroker(newBroker)
		convey.So(loaded, convey.ShouldBeFalse)
		convey.So(broker.workerAddr, convey.ShouldEqual, newBroker.workerAddr)
	})
}

// TestManager_loadOrStoreWorkerBroker3 -
func TestManager_loadOrStoreWorkerBroker3(t *testing.T) {
	convey.Convey("test: broker does not exist", t, func() {
		m := &Manager{}
		newBroker := &workerBroker{
			workerID:   workerID,
			workerAddr: "127.0.0.1:22669",
		}
		broker, loaded := m.loadOrStoreWorkerBroker(newBroker)
		convey.So(loaded, convey.ShouldBeFalse)
		convey.So(broker.workerAddr, convey.ShouldEqual, newBroker.workerAddr)
	})
}

func TestCancelWithLocalWorker(t *testing.T) {
	convey.Convey("TestCancelWithLocalWorker", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()
		m := Manager{}
		convey.So(m.CreateWorkerBroker(config), convey.ShouldBeNil)
		dst := &common.CancelProperty{}

		rsp := m.Cancel("nonExistWorker", dst, 0, "")
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.SendReqErrCode)

		m.workerBrokers.Store("errorWorker", nil)
		rsp = m.Cancel("errorWorker", dst, 0, "")
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.InternalErrCode)

		rsp = m.Cancel(workerID, dst, 0, "")
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
	})
}

func TestCancelWithRemoteWorker(t *testing.T) {
	convey.Convey("TestCancelWithRemoteWorker", t, func() {
		setup(defaultConfig)
		defer func() {
			tearDown()
		}()

		backupCancel := common.SendCancelReq
		common.SendCancelReq = remoteCancel
		defer func() {
			common.SendCancelReq = backupCancel
		}()

		m := Manager{}
		err := m.CreateRemoteBroker(funcKeyWithResource, workerID, "127.0.0.2", "")
		convey.So(err, convey.ShouldBeNil)

		dst := &common.CancelProperty{}
		rsp := m.Cancel(workerID, dst, 0, "")
		convey.So(rsp.ErrorCode, convey.ShouldEqual, proxyerrors.StateOKCode)
	})
}

func TestRecoverWithNotExistWorker(t *testing.T) {
	convey.Convey("TestRecoverWithNotExistWorker", t, func() {
		m := Manager{}
		rsp := m.Recover(workerID, 1, "", false)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func mockRecover(m *Manager, workerID string, requestID uint64, traceID string, flag bool) *types.TCPResponse {
	records := []string{""}
	recoverProperty := &common.RecoverProperty{
		Requests: map[string]*common.RecoverTraceRecord{
			workerID: {
				Records: records,
			},
		},
	}
	data, _ := json.Marshal(recoverProperty)
	return &types.TCPResponse{
		ErrorCode: proxyerrors.StateOKCode,
		RawData:   data,
	}

}

func mockRecoverRawDataInvalid(m *Manager, workerID string, requestID uint64, traceID string, flag bool) *types.TCPResponse {
	return &types.TCPResponse{
		ErrorCode: proxyerrors.StateOKCode,
		RawData:   []byte{1, 2, 3},
	}

}

func TestRecoverWithExistInvalidWorker(t *testing.T) {
	convey.Convey("TestRecoverWithExistInvalidWorker", t, func() {
		m := Manager{}

		m.workerBrokers.LoadOrStore(workerID, struct{}{})
		rsp := m.Recover(workerID, 1, "", false)
		convey.So(rsp, convey.ShouldNotBeNil)
		convey.So(rsp.ErrorCode, convey.ShouldNotEqual, proxyerrors.StateOKCode)
	})
}

func TestRecoverAndGetCurrentRequestNum(t *testing.T) {
	convey.Convey("TestRecoverAndGetCurrentRequestNum", t, func() {
		m := Manager{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(&m), "Recover", mockRecover),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		n := m.recoverAndGetCurrentRequestNum(workerID, false)
		convey.So(n, convey.ShouldEqual, 1)
	})
}

func TestRecoverAndGetCurrentRequestNumWithMarshalFailed(t *testing.T) {
	convey.Convey("TestRecoverAndGetCurrentRequestNumWithMarshalFailed", t, func() {
		m := Manager{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyMethod(reflect.TypeOf(&m), "Recover", mockRecoverRawDataInvalid),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		n := m.recoverAndGetCurrentRequestNum(workerID, false)
		convey.So(n, convey.ShouldEqual, 0)
	})
}

func TestGetWorkerBrokerWithInvalidWorker(t *testing.T) {
	convey.Convey("TestGetWorkerBrokerWithInvalidWorker", t, func() {
		m := Manager{}
		m.workerBrokers.LoadOrStore(workerID, struct{}{})
		broker := m.getWorkerBroker(workerID)
		convey.So(broker, convey.ShouldBeNil)
	})
}

func TestProcessWithInvalidWorker(t *testing.T) {
	convey.Convey("TestGetWorkerBrokerWithInvalidWorker", t, func() {
		m := Manager{}
		m.workerBrokers.Store(workerID, struct{}{})
		m.Process(workerID, &types.DispatcherRequest{})
		_, ok := m.workerBrokers.Load(workerID)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestDeleteWorkerBroker(t *testing.T) {
	convey.Convey("TestGetWorkerBrokerWithInvalidWorker", t, func() {
		m := Manager{}
		m.DeleteWorkerBroker(funcKeyWithResource, workerID, workerAddr, false)
		m.workerBrokers.LoadOrStore(workerID, &remoteBroker{})
		m.DeleteWorkerBroker(funcKeyWithResource, workerID, workerAddr, false)
		_, ok := m.workerBrokers.Load(workerID)
		convey.So(ok, convey.ShouldBeFalse)
	})
}

func TestCreateRemoteBrokerWithInvalidConfig(t *testing.T) {
	convey.Convey("TestCreateRemoteBroker", t, func() {
		m := Manager{}
		err := m.CreateRemoteBroker("", "", "", "")
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestManager_WaitEventSignal(t *testing.T) {
	convey.Convey("TestManager_WaitEventSignal", t, func() {
		result := 100
		m := Manager{}
		ch := make(chan struct{}, 1)
		if _, loaded := m.eventSignal.LoadOrStore(config.WorkerID, ch); !loaded {
			go func(int) {
				m.WaitEventSignal(workerID)
				result = 10
			}(result)
			time.Sleep(100 * time.Millisecond)
			convey.So(result, convey.ShouldEqual, 100)
			result = 200
			convey.So(result, convey.ShouldEqual, 200)
			go func() {
				ch <- struct{}{}
				m.eventSignal.Delete(config.WorkerID)
			}()
		}
		time.Sleep(10 * time.Millisecond)
		convey.So(result, convey.ShouldEqual, 10)
	})
}

func TestCreateWorkerBrokerWithDirectFunc(t *testing.T) {
	tests := []struct {
		name       string
		managerCfg *ManagerSetupConfig
		brokerCfg  *BrokerConfig
		patchFunc  utils.PatchesFunc

		expectedErr bool
	}{
		{
			name:       "failed to recover runtime",
			managerCfg: defaultConfig,
			brokerCfg: &BrokerConfig{
				FuncKeyWithResource: funcKeyWithResource,
				WorkerID:            workerID,
				WorkerAddr:          workerAddr,
				IsDirectFunc:        true,
			},
			patchFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
						return nil, errors.New("failed to recover runtime")
					}),
				})
				return patches
			},
			expectedErr: true,
		},
		{
			name:       "wait runtime conn timeout",
			managerCfg: defaultConfig,
			brokerCfg: &BrokerConfig{
				FuncKeyWithResource: funcKeyWithResource,
				WorkerID:            workerID,
				WorkerAddr:          workerAddr,
				IsDirectFunc:        true,
			},
			patchFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(context.WithTimeout, func(_ context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
						ctx, cancel := context.WithCancel(context.Background())
						cancel()
						return ctx, cancel
					}),
					gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
						return &types.TCPResponse{}, nil
					}),
				})
				return patches
			},
			expectedErr: true,
		},
		{
			name:       "wait runtime conn is nil",
			managerCfg: defaultConfig,
			brokerCfg: &BrokerConfig{
				FuncKeyWithResource: funcKeyWithResource,
				WorkerID:            workerID,
				WorkerAddr:          workerAddr,
				IsDirectFunc:        true,
			},
			patchFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc((*tcpbus.ConnCache).GetOrCreateConnWaiter, func(_ *tcpbus.ConnCache, _ string) <-chan tcpbus.RuntimeReport {
						ch := make(chan tcpbus.RuntimeReport, 1)
						close(ch)
						return ch
					}),
					gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
						return &types.TCPResponse{}, nil
					}),
				})
				return patches
			},
			expectedErr: true,
		},
		{
			name:       "create broker success",
			managerCfg: defaultConfig,
			brokerCfg: &BrokerConfig{
				FuncKeyWithResource: funcKeyWithResource,
				WorkerID:            workerID,
				WorkerAddr:          workerAddr,
				IsDirectFunc:        true,
			},
			patchFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc((*tcpbus.ConnCache).GetOrCreateConnWaiter, func(_ *tcpbus.ConnCache, _ string) <-chan tcpbus.RuntimeReport {
						ch := make(chan tcpbus.RuntimeReport, 1)
						ch <- tcpbus.RuntimeReport{Conn: &tcpbus.Connection{}, Stage: "load"}
						return ch
					}),
					gomonkey.ApplyFunc((*tcpbus.TCPBus).RecoverRuntime, func(_ *tcpbus.TCPBus, _ context.Context, _ bool) (*types.TCPResponse, error) {
						return &types.TCPResponse{}, nil
					}),
					gomonkey.ApplyFunc(GetSummaryFunctionData, func(funcKeyWithResource string) (int64, bool, string, error) {
						return 1, true, "test", nil
					}),
				})
				return patches
			},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(tt.managerCfg)
			defer tearDown()
			patch := tt.patchFunc()
			defer patch.ResetAll()

			m := Manager{}
			err := m.CreateWorkerBroker(tt.brokerCfg)
			broker := m.getWorkerBroker(tt.brokerCfg.WorkerID)
			if tt.expectedErr {
				assert.NotNil(t, err, tt.name)
				assert.Nil(t, broker, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
				assert.NotNil(t, broker, tt.name)
				close(broker.stopCh)
			}
		})
	}
}

func TestWatchDisable(t *testing.T) {
	convey.Convey("TestWatchDisable", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc((*tcpbus.TCPBus).WaitBusExit, func(_ *tcpbus.TCPBus) {
				return
			}),
		}
		defer func() {
			for i := range patches {
				patches[i].Reset()
			}
		}()
		common.NotifyRemote = func(_ string, _ *common.FunctionProperty, _ *common.NotifyEvent) error {
			return nil
		}
		m := Manager{}
		b := &workerBroker{
			workerID: "worker1",
			ownerIP:  "otherOwnerIP",
		}
		m.watchDisable(&tcpbus.TCPBus{}, b)
		_, exist := m.workerBrokers.Load("worker1")
		convey.So(exist, convey.ShouldBeFalse)
	})
}

func TestManager_Process(t *testing.T) {
	tests := []struct {
		name              string
		businessType      string
		expectedErrorCode uint32
		needAddBroker     bool
	}{
		{
			name:              "return 211406 when no normal worker broker",
			expectedErrorCode: proxyerrors.SendReqErrCode,
			needAddBroker:     true,
		}, {
			name:              "waiting broker to create when business type is websocket",
			businessType:      constants.BusinessTypeWebSocket,
			expectedErrorCode: proxyerrors.StateOKCode,
			needAddBroker:     true,
		}, {
			name:              "failed to wait broker to create when business type is websocket",
			businessType:      constants.BusinessTypeWebSocket,
			expectedErrorCode: proxyerrors.SendReqErrCode,
			needAddBroker:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup(defaultConfig)
			m := &Manager{}
			req := &types.DispatcherRequest{
				Request: &types.TCPRequest{
					BusinessType: tt.businessType,
				},
				Response: make(chan *types.TCPResponse, 1),
			}
			go func() {
				if !tt.needAddBroker {
					return
				}
				time.Sleep(time.Millisecond * 300)
				err := m.CreateWorkerBroker(config)
				if err != nil {
					t.Errorf("failed to add worker broker")
				}
			}()
			m.Process(workerID, req)
			rsp := <-req.Response
			if rsp.ErrorCode != tt.expectedErrorCode {
				t.Errorf("Process(%s) = %d; expected %d", t.Name(), rsp.ErrorCode, tt.expectedErrorCode)
			}
			time.Sleep(time.Millisecond * 600)
			tearDown()
		})
	}
}

func Test_generateRuntimeMetrics(t *testing.T) {
	convey.Convey("Test_generateRuntimeMetrics", t, func() {
		b := &workerBroker{
			workerID: "defaultaz-#-pool22-300-128-fusion-595d598b8c-c2btg",
			ownerIP:  "otherOwnerIP",
		}
		data := generateRuntimeMetrics(b, 0, "test", true)
		convey.So(data.EphemeralStorage, convey.ShouldEqual, 10240)
		convey.So(data.PodName, convey.ShouldEqual, "pool22-300-128-fusion-595d598b8c-c2btg")
	})
}

func TestManager_SetVPAInfo(t *testing.T) {
	mockWorkerBroker := &workerBroker{}
	expectedWorkerBroker := &workerBroker{}
	expectedWorkerBroker.verticalAutoscalingInfo.Store(&verticalAutoscalingInfo{
		workerContainerID:        containerIDs[0],
		runtimeContainerID:       containerIDs[1],
		podUID:                   "5e8c9436-d487-4eb4-9657-7603a49b22e2",
		runtimeCPULimit:          1000,
		runtimeCPURequest:        500,
		runtimeMemoryLimit:       1024,
		runtimeMemoryRequest:     1024,
		workerCPULimit:           1000,
		workerCPURequest:         500,
		workerMemoryLimit:        1024,
		workerMemoryRequest:      1024,
		runtimePoolCPULimit:      600,
		runtimePoolCPURequest:    150,
		runtimePoolMemoryLimit:   512,
		runtimePoolMemoryRequest: 512,
		workerPoolCPULimit:       1000,
		workerPoolCPURequest:     100,
		workerPoolMemoryLimit:    200,
		workerPoolMemoryRequest:  100,
	})
	nilPatchFunc := func() utils.PatchSlice {
		p := utils.InitPatchSlice()
		return p
	}
	type args struct {
		workerID       string
		req            *vpa.FuncInstanceInfo
		needCheckField bool
	}
	tests := []struct {
		name        string
		args        args
		errCode     int
		errMessage  string
		patchesFunc utils.PatchesFunc
	}{
		{
			name: "test if no local function instance can be found",
			args: args{
				workerID: workerID,
				req:      nil,
			},
			errCode:     214504,
			errMessage:  "no function instance in bus local cache",
			patchesFunc: nilPatchFunc,
		}, {
			name: "test if req body is nil",
			args: args{
				workerID: workerID,
				req:      nil,
			},
			errCode:    214504,
			errMessage: "invalid container ID or resource config in request body",
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc((*Manager).getWorkerBroker,
					func(_ *Manager, id string) *workerBroker {
						if id == workerID {
							return mockWorkerBroker
						}
						return nil
					})})
				return p
			},
		}, {
			name: "test if req body's container ID is nil",
			args: args{
				workerID: workerID,
				req:      &vpa.FuncInstanceInfo{},
			},
			errCode:    214504,
			errMessage: "invalid container ID or resource config in request body",
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc((*Manager).getWorkerBroker,
					func(_ *Manager, id string) *workerBroker {
						if id == workerID {
							return mockWorkerBroker
						}
						return nil
					})})
				return p
			},
		}, {
			name: "nil worker resource config",
			args: args{
				workerID: workerID,
				req: &vpa.FuncInstanceInfo{
					ContainerIDs: &vpa.ContainerInfo{
						Worker:  containerIDs[0],
						Runtime: containerIDs[1],
					},
					RuntimeInitResource: &vpa.ResourceConfig{
						CPULimit:      1000,
						CPURequest:    500,
						MemoryLimit:   1024,
						MemoryRequest: 1024,
					},
				},
			},
			errCode:    214504,
			errMessage: "invalid container ID or resource config in request body",
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc((*Manager).getWorkerBroker,
					func(_ *Manager, id string) *workerBroker {
						if id == workerID {
							return mockWorkerBroker
						}
						return nil
					})})
				return p
			},
		}, {
			name: "normal case",
			args: args{
				needCheckField: true,
				workerID:       workerID,
				req: &vpa.FuncInstanceInfo{
					Action:   "create",
					FuncName: funcKey,
					PodName:  workerID,
					PodUID:   podUID,
					ContainerIDs: &vpa.ContainerInfo{
						Worker:  containerIDs[0],
						Runtime: containerIDs[1],
					},
					WorkerInitResource: &vpa.ResourceConfig{
						CPULimit:      1000,
						CPURequest:    500,
						MemoryLimit:   1024,
						MemoryRequest: 1024,
					},
					RuntimeInitResource: &vpa.ResourceConfig{
						CPULimit:      1000,
						CPURequest:    500,
						MemoryLimit:   1024,
						MemoryRequest: 1024,
					},
					WorkerPoolResource: &vpa.ResourceConfig{
						CPULimit:      1000,
						CPURequest:    100,
						MemoryLimit:   200,
						MemoryRequest: 100,
					},
					RuntimePoolResource: &vpa.ResourceConfig{
						CPULimit:      600,
						CPURequest:    150,
						MemoryLimit:   512,
						MemoryRequest: 512,
					},
				},
			},
			errCode:    0,
			errMessage: "",
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc((*Manager).getWorkerBroker,
					func(_ *Manager, id string) *workerBroker {
						if id == workerID {
							return mockWorkerBroker
						}
						return nil
					})})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manager{}
			p := tt.patchesFunc()
			errCode, errMessage := m.SetVPAInfo(tt.args.workerID, tt.args.req)
			assert.Equalf(t, tt.errCode, errCode, "SetVPAInfo(%v, %v) want: %d, actual: %d", tt.args.workerID, tt.args.req, tt.errCode, errCode)
			assert.Equalf(t, tt.errMessage, errMessage, "SetVPAInfo(%v, %v) want: %s, actual: %s", tt.args.workerID, tt.args.req, tt.errMessage, errMessage)
			if tt.args.needCheckField && !reflect.DeepEqual(expectedWorkerBroker.verticalAutoscalingInfo.Load(), mockWorkerBroker.verticalAutoscalingInfo.Load()) {
				t.Errorf("SetVPAInfo error: actual = %v, want = %v", mockWorkerBroker.verticalAutoscalingInfo.Load(), expectedWorkerBroker.verticalAutoscalingInfo.Load())
			}
			p.ResetAll()
		})
	}
}

func TestManager_GetAllInstanceVPAInfo(t *testing.T) {
	mockWorkerBroker := []interface{}{
		&workerBroker{
			funcKeyWithResource: funcKeyWithResource,
		},
		&workerBroker{},
		&verticalAutoscalingInfo{
			workerContainerID:  containerIDs[0],
			runtimeContainerID: containerIDs[1],
			podUID:             podUID,
		},
	}
	mockWorkerBroker[0].(*workerBroker).verticalAutoscalingInfo.Store(&verticalAutoscalingInfo{
		workerContainerID:    containerIDs[0],
		runtimeContainerID:   containerIDs[1],
		podUID:               podUID,
		runtimeCPULimit:      0,
		runtimeCPURequest:    0,
		runtimeMemoryLimit:   0,
		runtimeMemoryRequest: 0,
	})
	tests := []struct {
		name        string
		inputLen    int
		expectedLen int
	}{
		{
			name:        "no local function instance",
			expectedLen: 0,
			inputLen:    0,
		}, {
			name:        "test only valid function instance info",
			expectedLen: 1,
			inputLen:    1,
		}, {
			name:        "test function instance info without vpa info",
			expectedLen: 1,
			inputLen:    2,
		}, {
			name:        "test wrong type in sync map",
			expectedLen: 1,
			inputLen:    3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manager{}
			for i, v := range mockWorkerBroker {
				if i+1 > tt.inputLen {
					break
				}
				m.workerBrokers.Store(strconv.Itoa(i), v)
			}
			res := m.GetAllInstanceVPAInfo("mockTraceID")
			if len(res.FuncInstanceInfo) != tt.expectedLen {
				t.Errorf("GetAllInstanceVPAInfo error: actual = %d, want = %d", len(res.FuncInstanceInfo), tt.expectedLen)
			}
		})
	}
}

func TestManager_deleteWorkerMonitorInSLOAgent(t *testing.T) {
	tests := []struct {
		name                   string
		wb                     *workerBroker
		registerDeleteCallBack bool
		patchesFunc            utils.PatchesFunc
	}{
		{
			name: "no vpa info in worker broker",
			wb:   nil,
		}, {
			name: "test with valid vpa info",
			wb:   &workerBroker{},
		}, {
			name:                   "test with valid vpa info",
			wb:                     &workerBroker{},
			registerDeleteCallBack: true,
		}, {
			name: "test with json marshal error",
			wb:   &workerBroker{},
			patchesFunc: func() utils.PatchSlice {
				p := utils.InitPatchSlice()
				p.Append(utils.PatchSlice{gomonkey.ApplyFunc(json.Marshal,
					func(v any) ([]byte, error) {
						return nil, errors.New("mock json marshal error")
					})})
				return p
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &Manager{}
			var p utils.PatchSlice
			if tt.registerDeleteCallBack {
				GetInstance().RegisterDeleteFuncInsHandler(func(payload []byte, workerID string) {})
			}
			if tt.patchesFunc != nil {
				p = tt.patchesFunc()
			}
			m.deleteWorkerMonitorInSLOAgent(tt.wb, "mock function instance ID")
			if tt.patchesFunc != nil {
				p.ResetAll()
			}
		})
	}
}

func Test_registerReadyInsToSLO(t *testing.T) {
	patch := gomonkey.NewPatches()
	worker := &workerBroker{
		funcKeyWithResource: funcKeyWithResource,
	}
	common.FConfig = &common.Configs{
		VPAConfig: &common.VerticalAutoscalingConfig{
			Enable:                    1,
			ResourceViewAlignInterval: 1,
		},
	}

	convey.Convey("test registerReadyInsToSLO", t, func() {
		convey.Convey("test get vpa info is nil", func() {
			flag := false
			SetSendInstanceInfoToSLOFunc(mockSendInstanceInfoToSLOAgent)
			patch.ApplyPrivateMethod(reflect.TypeOf(&workerBroker{}), "getVerticalAutoscalingInfo",
				func() *verticalAutoscalingInfo {
					flag = true
					return nil
				})

			registerReadyInsToSLO(worker)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test SendInstanceInfoToSLOFunc error", func() {
			flag := false
			mockSendInstanceInfoToSLOFunc := func(info vpa.FuncInstanceInfo) error {
				flag = true
				return errors.New("mock error")
			}
			SetSendInstanceInfoToSLOFunc(mockSendInstanceInfoToSLOFunc)
			mockGetVerticalAutoscalingInfo(patch, &verticalAutoscalingInfo{})

			registerReadyInsToSLO(worker)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test SendInstanceInfoToSLOFunc success", func() {
			flag := false
			mockSendInstanceInfoToSLOFunc := func(info vpa.FuncInstanceInfo) error {
				flag = true
				return nil
			}
			SetSendInstanceInfoToSLOFunc(mockSendInstanceInfoToSLOFunc)
			mockGetVerticalAutoscalingInfo(patch, &verticalAutoscalingInfo{})

			registerReadyInsToSLO(worker)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func mockGetVerticalAutoscalingInfo(patch *gomonkey.Patches, vpaInfo *verticalAutoscalingInfo) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&workerBroker{}), "getVerticalAutoscalingInfo",
		func() *verticalAutoscalingInfo {
			return vpaInfo
		})
}

func mockSendInstanceInfoToSLOAgent(info vpa.FuncInstanceInfo) error {
	return nil
}
