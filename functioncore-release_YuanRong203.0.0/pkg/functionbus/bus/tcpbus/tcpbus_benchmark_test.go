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

package tcpbus

import (
	"context"
	"log"
	"sync"
	"testing"

	"functioncore/pkg/common/constants"
	commonTypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/functionbus/bus/types"
	buscommon "functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/utils"
)

const addr = "127.0.0.1:44444"

var mockReportCache sync.Map

type MockTCPBusServer struct {
	busObj *TCPBus
}

func (t *MockTCPBusServer) Adaptor(ctx context.Context, dst *buscommon.FunctionProperty,
	req *commonTypes.TCPRequest) error {
	t.busObj.SetResult(&commonTypes.TCPResponse{
		RequestID: req.RequestID,
		ErrorCode: 0,
	})
	return nil
}

func (t *MockTCPBusServer) Cancel(ctx context.Context, dst *buscommon.CancelProperty,
	req *commonTypes.TCPRequest) error {
	t.busObj.SetCancelResult(&commonTypes.TCPResponse{
		RequestID: req.RequestID,
		ErrorCode: 0,
	})
	return nil
}

func (t *MockTCPBusServer) Recover(req *commonTypes.TCPRequest, bus interface{}) error {
	t.busObj.SetRecoverResult(&commonTypes.TCPResponse{
		RequestID: req.RequestID,
		ErrorCode: 0,
	})
	return nil
}

func (t *MockTCPBusServer) reportRuntimeMetrics(runtimeMetrics buscommon.RuntimeMetricsProperty) error {
	return nil
}

func (t *MockTCPBusServer) HandleRequest(payload []byte, clientIP string, client *Connection) types.Result {
	return types.Result{}
}

func (t *MockTCPBusServer) RegisterService(client *Connection) error {
	log.Printf("receive connection from %s", client.PeerIP)
	tcpBusOpt := &Option{
		CreateClient: false,
		Client:       client,
		Remote:       true,
		SingleMode:   true,
	}
	tcpBus, err := NewTCPBus(buscommon.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		log.Printf("failed to create tcp bus: %s", err.Error())
		return err
	}
	tcpBus.reportCache = &mockReportCache
	tcpBus.RegisterCallback(t.Adaptor)
	tcpBus.RegisterCancel(t.Cancel)
	tcpBus.RegisterRecover(t.Recover)
	tcpBus.RegisterReportRuntimeMetrics(t.reportRuntimeMetrics)
	tcpBus.RegisterPauseRuntime(func() error {
		return nil
	})
	tcpBus.RegisterRecoverRuntime(func(restartRuntime bool) error {
		return nil
	})
	tcpBus.RegisterCheckRuntime(func(traceID string, shouldWait bool) ([]byte, error) {
		return []byte{}, nil
	})
	t.busObj = tcpBus
	return nil
}

func createBusServer() {
	StartServer(ServerOption{
		Receiver:   &MockTCPBusServer{},
		ListenAddr: addr,
	})
}

func createBusClient() (*TCPBus, error) {
	tcpBusOpt := &Option{
		CreateClient: true,
		Remote:       true,
		SingleMode:   true,
		ServerAddr:   addr,
	}
	return NewTCPBus(buscommon.BusOption{NewBusOpt: tcpBusOpt})
}

func BenchmarkTCPBus_Call(b *testing.B) {
	createBusServer()
	busObj, err := createBusClient()
	if err != nil {
		b.Errorf("failed to create bus client %s", err)
	}

	for i := 0; i < b.N; i++ {
		ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, uuid.New().String())
		busObj.Call(ctx, &buscommon.FunctionProperty{}, &commonTypes.TCPRequest{}, false)
	}
}
