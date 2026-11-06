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

// Package busserver is used for routing messages between nodes in the cluster
package busserver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/proto"

	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/bus/tcpbus/protocol"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
)

func TestNotifyRemoteProxy(t *testing.T) {
	tcpBusObj := &tcpbus.TCPBus{}
	// notify event error
	{
		err := NotifyRemoteProxy(tcpBusObj, &common.FunctionProperty{}, &common.NotifyEvent{
			NotifyType: -1,
		})
		if err == nil {
			t.Errorf("failed to test invalid notify data")
		}
	}
}

func TestNotifyRemoteProxyTimeout(t *testing.T) {
	convey.Convey("TestNotifyRemoteProxyTimeout", t, func() {
		chanMsg := make(chan *protocol.Message, 1)
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(getNotifyData, func(event *common.NotifyEvent) (map[string]string, error) {
				return make(map[string]string), nil
			}),
			gomonkey.ApplyFunc((*tcpbus.TCPBus).Call, func(c *tcpbus.TCPBus, ctx context.Context,
				dst *common.FunctionProperty, req *commontypes.TCPRequest,
				isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
				select {
				case <-ctx.Done():
					return &commontypes.TCPResponse{ErrorCode: proxyerrors.TimeoutErrCode,
						ErrorMessage: proxyerrors.TimeoutErr}, nil
				case <-chanMsg:
					return &commontypes.TCPResponse{ErrorCode: proxyerrors.StateOKCode}, nil
				}
				return &commontypes.TCPResponse{ErrorCode: proxyerrors.InternalErrCode,
					ErrorMessage: proxyerrors.InternalErr}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		tcpBusObj := &tcpbus.TCPBus{}
		// notify event error
		{
			go func() {
				time.Sleep(6 * time.Second)
				chanMsg <- &protocol.Message{}
			}()
			err := NotifyRemoteProxy(tcpBusObj, &common.FunctionProperty{}, &common.NotifyEvent{
				NotifyType: common.ChangeState,
			})
			if err == nil {
				t.Errorf("failed to test notify remote proxy")
			}
			convey.So(err.Error(), convey.ShouldContainSubstring, proxyerrors.TimeoutErr)
		}
	})
}

func TestBaseBusServer_CreateProxyClient(t *testing.T) {
	server := &BaseBusServer{BusClients: &ConnectionClient{
		ProxyClients: make(map[string]bus.CommunicationBus, 0),
		Mux:          sync.RWMutex{},
	}}
	tcpConn := &tcpbus.Connection{DisabledSignal: make(chan struct{})}
	// nodeIP is empty
	{
		_, err := server.CreateProxyClient("", tcpbus.ClientOption{})
		if err == nil {
			t.Errorf("test no nodeIP failed")
		}
	}
	// new tcp bus error
	{
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyFunc(tcpbus.BuildClientConnection, func(_ tcpbus.ClientOption) (*tcpbus.Connection, error) {
			return tcpConn, nil
		})

		patches.ApplyFunc(tcpbus.NewTCPBus, func(_ common.BusOption) (*tcpbus.TCPBus, error) {
			return nil, fmt.Errorf("error")
		})
		_, err := server.CreateProxyClient("not empty", tcpbus.ClientOption{})
		if err == nil {
			t.Errorf("failed to test new tcp bus error")
		}
	}
	// client disabled
	{
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyFunc(tcpbus.BuildClientConnection, func(_ tcpbus.ClientOption) (*tcpbus.Connection, error) {
			return tcpConn, nil
		})

		patches.ApplyFunc(tcpbus.NewTCPBus, func(_ common.BusOption) (*tcpbus.TCPBus, error) {
			return &tcpbus.TCPBus{}, nil
		})
		_, err := server.CreateProxyClient("not empty", tcpbus.ClientOption{})
		tcpConn.DisabledSignal <- struct{}{}
		if err != nil {
			t.Errorf("failed to test create proxy client, error: %s", err.Error())
		}
	}
}

func TestBaseBusServer_SendRecoverRequest(t *testing.T) {
	server := &BaseBusServer{}
	tcpBusObj := &tcpbus.TCPBus{}
	// recover error
	{
		patches := gomonkey.NewPatches()
		defer patches.Reset()
		patches.ApplyFunc(proto.Marshal, func(m proto.Message) ([]byte, error) {
			return nil, fmt.Errorf("error")
		})
		resp := server.SendRecoverRequest(tcpBusObj, "nodeID", "traceID",
			&commontypes.TCPRequest{})
		if resp.ErrorCode != proxyerrors.SendReqErrCode {
			t.Errorf("failed to test recover error")
		}
	}
}
