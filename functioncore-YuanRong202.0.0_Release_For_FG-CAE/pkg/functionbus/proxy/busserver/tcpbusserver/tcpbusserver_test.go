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

package tcpbusserver

import (
	"context"
	"fmt"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/busserver"
	"functioncore/pkg/functionbus/proxyerrors"
)

func TestTCPBusServer_CreateTCPBusServer(t *testing.T) {
	Convey("test CreateTCPBusServer", t, func() {
		server := getMockTCPBusServer()
		So(server.listenAddr, ShouldEqual, "127.0.0.1:32568")
	})
}

func TestStartServer(t *testing.T) {
	Convey("test StartServer", t, func() {
		server := getMockTCPBusServer()
		err := server.StartServer()
		So(err, ShouldEqual, nil)
	})
}

func TestTCPBusServer_NotifyRemote(t *testing.T) {
	server := getMockTCPBusServer()
	dst := &common.FunctionProperty{
		InstanceID:   "instance1",
		FunctionName: "test",
	}
	event := &common.NotifyEvent{
		Enable: true,
	}
	Convey("Test TCPBusServer NotifyRemote", t, func() {
		Convey("failed to NotifyRemote ", func() {
			err := server.NotifyRemote("127.0.0.2", dst, event)
			assert.NotNil(t, err)
		})
		Convey("success to NotifyRemote ", func() {
			patches := [...]*Patches{
				ApplyFunc(tcpbus.BuildClientConnection,
					func(opt tcpbus.ClientOption) (*tcpbus.Connection, error) {
						return &tcpbus.Connection{}, nil
					}),
				ApplyFunc(tcpbus.NewTCPBus,
					func(opt common.BusOption) (*tcpbus.TCPBus, error) {
						return &tcpbus.TCPBus{}, nil
					}),
				ApplyFunc((*tcpbus.TCPBus).Call, func(t *tcpbus.TCPBus, ctx context.Context,
					dst *common.FunctionProperty, req *commontypes.TCPRequest,
					isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := server.NotifyRemote("127.0.0.2", dst, event)
			assert.Nil(t, err)
		})
	})
}

func TestTCPBusServer_SendRequest(t *testing.T) {
	server := getMockTCPBusServer()
	dst := &common.FunctionProperty{
		InstanceID:   "instance1",
		FunctionName: "test",
	}
	req := &commontypes.TCPRequest{}
	Convey("Test TCPBusServer SendRequest", t, func() {
		Convey("failed to SendRequest ", func() {
			response := server.SendRequest("node001", "traceID", req, dst)
			assert.Equal(t, proxyerrors.SendReqErrCode, int(response.ErrorCode))
		})
		Convey("succeed to SendRequest ", func() {
			patches := [...]*Patches{
				ApplyFunc(tcpbus.BuildClientConnection,
					func(opt tcpbus.ClientOption) (*tcpbus.Connection, error) {
						return &tcpbus.Connection{}, nil
					}),
				ApplyFunc(tcpbus.NewTCPBus,
					func(opt common.BusOption) (*tcpbus.TCPBus, error) {
						return &tcpbus.TCPBus{}, nil
					}),
				ApplyFunc((*tcpbus.TCPBus).Call, func(t *tcpbus.TCPBus, ctx context.Context,
					dst *common.FunctionProperty, req *commontypes.TCPRequest,
					isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			response := server.SendRequest("node001", "traceID", req, dst)
			assert.Equal(t, 0, int(response.ErrorCode))
		})
	})
}

func TestTCPBusServer_HandleRequest(t *testing.T) {
	server := getMockTCPBusServer()
	dst := &pb.FunctionProperty{
		InstanceID:   "instance1",
		FunctionName: "test",
	}
	data, err := proto.Marshal(&pb.Message{
		Type:        pb.Message_CALL_REQUEST,
		CallMessage: &pb.Message_CallMessage{Dst: dst, Req: &pb.CallRequest{}},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	result := server.HandleRequest(data, "", nil)
	assert.Equal(t, 0, int(result.ErrorCode))

	data, err = proto.Marshal(&pb.Message{
		Type:          pb.Message_CANCEL_REQUEST,
		CancelMessage: &pb.Message_CancelMessage{Force: false},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	result = server.HandleRequest(data, "", nil)
	assert.Equal(t, 0, int(result.ErrorCode))

	data, err = proto.Marshal(&pb.Message{
		Type: 99,
	})
	result = server.HandleRequest(data, "", nil)
	assert.Equal(t, proxyerrors.InternalErrCode, int(result.ErrorCode))
}

func getMockTCPBusServer() *TCPBusServer {
	serverOpt := &busserver.Option{
		ListenAddr:     "127.0.0.1" + ":" + constants.TCPort,
		RequestHandler: requestHandler,
	}
	server, _ := CreateTCPBusServer(serverOpt)
	return server
}

func requestHandler(traceID string,
	request *commontypes.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	fmt.Println("requestHandler")
	return &commontypes.TCPResponse{}
}
