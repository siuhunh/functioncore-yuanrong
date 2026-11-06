/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package busworkerserver is used for routing messages between nodes in the cluster
package busworkerserver

import (
	"errors"
	"fmt"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/pb"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
)

func Test_InitServer(t *testing.T) {
	Convey("Test_InitServer", t, func() {
		Convey("init server success", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.StartServer, func(opt tcpbus.ServerOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
				Protocol:  "",
				EnableTLS: false,
			}
			_, err := InitServer(requestHandler)
			So(err, ShouldEqual, nil)
		})
		Convey("init server failed", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.StartServer, func(opt tcpbus.ServerOption) error {
					return errors.New("start server failed")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
				Protocol:  "",
				EnableTLS: false,
			}
			_, err := InitServer(requestHandler)
			So(err, ShouldNotEqual, nil)
			So(err.Error(), ShouldEqual, "start server failed")
		})
		Convey("init tls server success", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.StartServerWithTLS, func(opt tcpbus.ServerOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
				Protocol:  "",
				EnableTLS: true,
			}
			_, err := InitServer(requestHandler)
			So(err, ShouldEqual, nil)
		})
		Convey("unsupported protocol", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.StartServerWithTLS, func(opt tcpbus.ServerOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
				Protocol:  "unix",
				EnableTLS: true,
			}
			_, err := InitServer(requestHandler)
			So(err, ShouldNotEqual, nil)
			So(err.Error(), ShouldEqual, "unsupported protocol: to be implemented")
		})
		Convey("unknown message type", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.StartServer, func(opt tcpbus.ServerOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
				Protocol:  "",
				EnableTLS: false,
			}
			s, err := InitServer(requestHandler)
			So(err, ShouldEqual, nil)
			var b []byte
			request := s.HandleRequest(b, "127.0.0.1", nil)
			So(request.ErrorCode, ShouldEqual, proxyerrors.InternalErrCode)
			So(request.ErrorMsg, ShouldEqual, "unknown message type")
		})
		Convey("failed to unmarshal message", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.StartServer, func(opt tcpbus.ServerOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
				Protocol:  "",
				EnableTLS: false,
			}
			s, err := InitServer(requestHandler)
			So(err, ShouldEqual, nil)
			b := []byte{'e', 'r', 'r'}
			request := s.HandleRequest(b, "127.0.0.1", nil)
			So(request.ErrorCode, ShouldEqual, proxyerrors.InternalErrCode)
			So(request.ErrorMsg, ShouldEqual, proxyerrors.InternalErr)
		})
		Convey("handle request", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(tcpbus.StartServer, func(opt tcpbus.ServerOption) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			common.FConfig.BusWorkerServerConfig = &common.BusServerConfig{
				Protocol:  "",
				EnableTLS: false,
			}
			s, err := InitServer(requestHandler)
			So(err, ShouldEqual, nil)
			msg := &pb.Message{
				Type: pb.Message_CALL_REQUEST,
				CallMessage: &pb.Message_CallMessage{
					Dst: &pb.FunctionProperty{},
					Req: &pb.CallRequest{},
				},
			}
			b, _ := proto.Marshal(msg)
			request := s.HandleRequest(b, "127.0.0.1", nil)
			So(request.RspData, ShouldNotEqual, nil)
		})
	})
}

func requestHandler(traceID string, request *commontypes.TCPRequest,
	dst *common.FunctionProperty) *commontypes.TCPResponse {
	fmt.Println("requestHandler")
	return &commontypes.TCPResponse{}
}

func TestInitServerWithRuntime(t *testing.T) {
	oldCfg := common.FConfig.BusRuntimeServerConfig

	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(tcpbus.StartServer, func(opt tcpbus.ServerOption) error {
			return nil
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
		common.FConfig.BusWorkerServerConfig = oldCfg
	}()

	tests := []struct {
		name        string
		cfg         common.BusServerConfig
		expectedErr bool
	}{
		{
			name:        "create server failed",
			cfg:         common.BusServerConfig{Protocol: "unix"},
			expectedErr: true,
		},
		{
			name:        "create server success",
			cfg:         common.BusServerConfig{},
			expectedErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			common.FConfig.BusRuntimeServerConfig = &tt.cfg
			server, err := InitServerWithRuntime(requestHandler)
			if tt.expectedErr {
				assert.Error(t, err, tt.name)
				assert.Nil(t, server, tt.name)
			} else {
				assert.Nil(t, err, tt.name)
				assert.NotNil(t, server, tt.name)
			}
		})
	}

}
