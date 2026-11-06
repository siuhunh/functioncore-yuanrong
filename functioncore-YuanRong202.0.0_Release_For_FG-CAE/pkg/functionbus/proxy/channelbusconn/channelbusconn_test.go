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

// Package channelbusconn is used for routing messages between functiontask with bus proxy
package channelbusconn

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/channelbus"
	"functioncore/pkg/functionbus/common"
	testutils "functioncore/test/utils"
)

type fakeInterface struct{}

func (fakeInterface) Register() error   { return nil }
func (fakeInterface) Unregister() error { return nil }

func TestChannelBusConn(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(NewRegistry, func(regInfo RegisterInfo, stopCh <-chan struct{}, handler common.ETCDConnectHandler) Interface {
		return fakeInterface{}
	})
	defer patches.Reset()

	_, err := CreateChannelBusConn(nil)
	assert.NotNil(t, err)

	conn, err := CreateChannelBusConn(&ConnOption{
		RequestHandler: func(traceID string, request *commontypes.TCPRequest, dst *common.FunctionProperty) *commontypes.TCPResponse {
			return &commontypes.TCPResponse{}
		},
		EtcdHandler: func(b bool) {},
	})
	require.NoError(t, err)

	conn.StartChannelBusConn()
	conn.HandleRequest(&common.ChannelBusMessage{
		Property: &common.FunctionProperty{TenantID: "0"},
		MsgType:  common.RegMsg,
	})
	conn.HandleRequest(&common.ChannelBusMessage{
		Property: &common.FunctionProperty{FunctionVersion: "latest"},
		MsgType:  common.ReqMsg,
		Request:  &commontypes.TCPRequest{},
	})
	conn.HandleRequest(&common.ChannelBusMessage{
		Property: &common.FunctionProperty{},
		MsgType:  common.ReqMsg,
		Request:  &commontypes.TCPRequest{},
	})
	conn.HandleRequest(&common.ChannelBusMessage{
		Property: &common.FunctionProperty{FunctionVersion: "latest"},
		MsgType:  3,
		Request:  &commontypes.TCPRequest{},
	})
	conn.HandleRequest(&common.ChannelBusMessage{
		Property: &common.FunctionProperty{FunctionVersion: "latest", TenantID: "3"},
		MsgType:  common.RegMsg,
		Request:  &commontypes.TCPRequest{},
	})
	conn.AwaitReady()
	time.Sleep(1 * time.Second)
}

func mockCall(bus *channelbus.ChannelBus, ctx context.Context, dst *common.FunctionProperty, req *commontypes.TCPRequest,
	isSendingRemoteProxy bool) (*commontypes.TCPResponse, error) {
	return nil, errors.New("failed to call")
}

func TestSendRequest(t *testing.T) {
	patches := testutils.InitPatchSlice()
	defer func() {
		patches.ResetAll()
	}()
	channelBus := &channelbus.ChannelBus{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(channelBus), "Call", mockCall),
	})
	c := &ChannelBusConn{
		channelBus: channelBus,
	}
	requestID := uint64(1234)
	rsp := c.SendRequest("traceID", &commontypes.TCPRequest{RequestID: requestID}, &common.FunctionProperty{InstanceID: "ID"})
	assert.Equal(t, rsp.RequestID, requestID)
}
