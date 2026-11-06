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

package tlsbusserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/constants"
	commontls "functioncore/pkg/common/tls"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/busserver"
	"functioncore/pkg/functionbus/proxyerrors"
)

func TestTLSBusServer_CreateTLSBusServer(t *testing.T) {
	Convey("test CreateTLSBusServer", t, func() {
		patches := [...]*Patches{
			ApplyFunc(commontls.InitTLSConfig, func(tlsProtocols string,
				tlsCiphers string) (err error) {
				return nil
			}),
			ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
				return &tls.Config{}
			}),
			ApplyFunc(tls.Listen, func(network, laddr string, config *tls.Config) (net.Listener, error) {
				return &fakeNetListener{}, nil
			}),
		}
		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		common.FConfig.HTTPSConfig = &commontls.InternalHTTPSConfig{
			HTTPSEnable: true,
			TLSProtocol: "tls",
			TLSCiphers:  "ciphers",
		}
		serverOpt := &busserver.Option{
			ListenAddr:     "127.0.0.1" + ":" + constants.TCPort,
			RequestHandler: handReq,
		}
		server, _ := CreateTLSBusServer(serverOpt)
		So(server.listenAddr, ShouldEqual, "127.0.0.1:32568")
		err := server.StartServer()
		assert.Nil(t, err)
	})
}

func TestTLSBusServer_NotifyRemote(t *testing.T) {
	server := getMockTLSBusServer()
	dst := &common.FunctionProperty{
		InstanceID:   "instance1",
		FunctionName: "test",
	}
	event := &common.NotifyEvent{
		Enable: true,
	}
	Convey("Test TLSBusServer NotifyRemote", t, func() {
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

func TestTLSBusServer_SendRequest(t *testing.T) {
	server := getMockTLSBusServer()
	dst := &common.FunctionProperty{
		InstanceID:   "instance1",
		FunctionName: "test",
	}
	req := &commontypes.TCPRequest{}
	Convey("Test TLSBusServer SendRequest", t, func() {
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

func TestTLSBusServer_SendCancelRequest(t *testing.T) {
	server := getMockTLSBusServer()
	dst := &common.CancelProperty{}
	req := &commontypes.TCPRequest{}
	Convey("Test TLSBusServer SendCancelRequest", t, func() {
		Convey("failed to SendCancelRequest ", func() {
			response := server.SendCancelRequest("node001", "traceID", req, dst)
			assert.Equal(t, proxyerrors.SendReqErrCode, int(response.ErrorCode))
		})
		Convey("succeed to SendCancelRequest ", func() {
			patches := [...]*Patches{
				ApplyFunc(tcpbus.BuildClientConnection,
					func(opt tcpbus.ClientOption) (*tcpbus.Connection, error) {
						return &tcpbus.Connection{}, nil
					}),
				ApplyFunc(tcpbus.NewTCPBus,
					func(opt common.BusOption) (*tcpbus.TCPBus, error) {
						return &tcpbus.TCPBus{}, nil
					}),
				ApplyFunc((*tcpbus.TCPBus).Cancel, func(t *tcpbus.TCPBus,
					ctx context.Context, dst *common.CancelProperty,
					req *commontypes.TCPRequest) (*commontypes.TCPResponse, error) {
					return &commontypes.TCPResponse{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			response := server.SendCancelRequest("node001", "traceID", req, dst)
			assert.Equal(t, 0, int(response.ErrorCode))
		})
	})
}

func TestTLSBusServer_HandleRequest(t *testing.T) {
	server := getMockTLSBusServer()
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
		Type: 99,
	})
	result = server.HandleRequest(data, "", nil)
	assert.Equal(t, proxyerrors.InternalErrCode, int(result.ErrorCode))
}

func getMockTLSBusServer() *TLSBusServer {
	busClients := &busserver.ConnectionClient{
		ProxyClients: make(map[string]bus.CommunicationBus, mapInitSize),
		Mux:          sync.RWMutex{},
	}
	return &TLSBusServer{
		baseBusServer: &busserver.BaseBusServer{
			BusClients: busClients,
		},
		requestHandler: handReq,
		listenAddr:     "127.0.0.1:8081",
	}
}

func handReq(traceID string,
	request *commontypes.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
	fmt.Println(traceID, request, dst)
	return &commontypes.TCPResponse{}
}

type fakeNetListener struct {
}

func (ln *fakeNetListener) Accept() (net.Conn, error) {
	return &fakeConn{}, errors.New("failed to accept")
}

func (ln *fakeNetListener) Close() error {
	return nil
}

func (ln *fakeNetListener) Addr() net.Addr {
	return nil
}

type fakeConn struct {
	err error // The error to return when io is performed over the connection.
}

func (f *fakeConn) Read([]byte) (int, error)        { return 0, f.err }
func (f *fakeConn) Write([]byte) (int, error)       { return 0, f.err }
func (f *fakeConn) Close() error                    { return nil }
func (fakeConn) LocalAddr() net.Addr                { return &fakeAddr{} }
func (fakeConn) RemoteAddr() net.Addr               { return &fakeAddr{} }
func (fakeConn) SetDeadline(t time.Time) error      { return nil }
func (fakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (fakeConn) SetWriteDeadline(t time.Time) error { return nil }

type fakeAddr struct{}

func (a fakeAddr) Network() string {
	return "net"
}

func (a fakeAddr) String() string {
	return "127.0.0.1:8080"
}
