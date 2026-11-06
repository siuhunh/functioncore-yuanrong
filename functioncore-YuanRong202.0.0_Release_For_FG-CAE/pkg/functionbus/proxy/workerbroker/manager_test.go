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
	"functioncore/pkg/common/constants"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/types"
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

func remoteCall(_, _ string, _ *types.TCPRequest, _ *common.FunctionProperty) *types.TCPResponse {
	return &types.TCPResponse{
		RequestID:    1,
		ErrorCode:    proxyerrors.StateOKCode,
		ErrorMessage: "",
	}
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
		common.SendCallReq = remoteCall
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
