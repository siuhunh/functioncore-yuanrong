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

package channelbus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/calls"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxyerrors"
)

func TestNewChannelBus(t *testing.T) {
	convey.Convey("NewChannelBus", t, func() {
		convey.Convey("nil NewBusOpt ", func() {
			busOption := common.BusOption{}
			_, err := NewChannelBus(busOption)
			assert.Error(t, err, "NewBusOpt is nil")
			busOption.NewBusOpt = 11
			_, err = NewChannelBus(busOption)
			assert.Error(t, err, "failed to assert type")
		})
		convey.Convey("newChannelBusForProxy", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true, Receiver: &MockChannelBusConn{}}
			client, err := NewChannelBus(busOption)
			assert.Nil(t, err)
			assert.NotNil(t, client)
		})
		convey.Convey("newChannelBusForProxy and client is nil", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true}
			client, err := NewChannelBus(busOption)
			assert.NotNil(t, err)
			assert.Nil(t, client)
		})
		convey.Convey("newChannelBusForTask", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &common.ChannelBusOption{}
			client, err := NewChannelBus(busOption)
			assert.Nil(t, err)
			assert.NotNil(t, client)
		})
	})
}

func TestRegisterBus(t *testing.T) {
	busOption := common.BusOption{}
	busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true, Receiver: &MockChannelBusConn{}}
	proxyBus, _ := NewChannelBus(busOption)

	proxyBus.RegisterCallback(nil)
	proxyBus.RegisterCancel(nil)
	proxyBus.RegisterRecover(nil)

	err := proxyBus.RegisterBus(common.FunctionProperty{})
	assert.NotNil(t, err)
}

func TestCall(t *testing.T) {
	busOption := common.BusOption{}
	busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true, Receiver: &MockChannelBusConn{}}
	proxyBus, _ := NewChannelBus(busOption)
	dst := &common.FunctionProperty{FunctionName: "test"}
	ctx, cancel := context.WithTimeout(context.Background(), common.DefaultRegisterTimeout)
	defer cancel()
	response, err := proxyBus.Call(ctx, dst, &commontypes.TCPRequest{TraceCtx: ctx}, false)
	assert.Equal(t, proxyerrors.TimeoutErrCode, int(response.ErrorCode))
	assert.Nil(t, err)
}

func TestResponseError(t *testing.T) {
	convey.Convey("TestResponseError", t, func() {
		convey.Convey("response is nil", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true, Receiver: &MockChannelBusConn{}}
			proxyBus, _ := NewChannelBus(busOption)
			dst := &common.FunctionProperty{FunctionName: "test"}
			ctx, cancel := context.WithTimeout(context.Background(), common.DefaultRegisterTimeout)
			defer cancel()
			go func() {
				time.Sleep(100 * time.Millisecond)
				proxyBus.pending.Range(func(key, value interface{}) bool {
					req := value.(*pendingReq)
					req.done <- &common.ChannelBusMessage{}
					return true
				})
			}()
			response, err := proxyBus.Call(ctx, dst, &commontypes.TCPRequest{TraceCtx: ctx}, false)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(response, convey.ShouldBeNil)
		})
		convey.Convey("response chan is closed ", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true, Receiver: &MockChannelBusConn{}}
			proxyBus, _ := NewChannelBus(busOption)
			dst := &common.FunctionProperty{FunctionName: "test"}
			ctx, cancel := context.WithTimeout(context.Background(), common.DefaultRegisterTimeout)
			defer cancel()
			go func() {
				time.Sleep(100 * time.Millisecond)
				proxyBus.pending.Range(func(key, value interface{}) bool {
					req := value.(*pendingReq)
					req.done <- &common.ChannelBusMessage{
						ErrorCode: 500,
					}
					return true
				})
			}()
			response, err := proxyBus.Call(ctx, dst, &commontypes.TCPRequest{TraceCtx: ctx}, false)
			convey.So(response.ErrorCode, convey.ShouldEqual, 500)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("response status is no ok ", func() {
			busOption := common.BusOption{}
			busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true, Receiver: &MockChannelBusConn{}}
			proxyBus, _ := NewChannelBus(busOption)
			dst := &common.FunctionProperty{FunctionName: "test"}
			ctx, cancel := context.WithTimeout(context.Background(), common.DefaultRegisterTimeout)
			defer cancel()
			go func() {
				time.Sleep(100 * time.Millisecond)
				proxyBus.pending.Range(func(key, value interface{}) bool {
					req := value.(*pendingReq)
					close(req.done)
					return true
				})
			}()
			response, err := proxyBus.Call(ctx, dst, &commontypes.TCPRequest{TraceCtx: ctx}, false)
			convey.So(response.ErrorCode, convey.ShouldEqual, proxyerrors.InternalErrCode)
			convey.So(err, convey.ShouldBeNil)
		})
	})

}

func TestChannelBus_HandleRequest(t *testing.T) {
	busOption := common.BusOption{}
	busOption.NewBusOpt = &common.ChannelBusOption{
		IsProxy: true, Receiver: &MockChannelBusConn{},
	}
	proxyBus, _ := NewChannelBus(busOption)
	msg := &common.ChannelBusMessage{
		MsgType: 99,
	}
	proxyBus.RegisterCallback(func(ctx context.Context, dst *common.FunctionProperty, req *commontypes.TCPRequest) error {
		resp := &commontypes.TCPResponse{
			RequestID: req.RequestID,
		}
		return proxyBus.SetResult(resp)
	})
	response := proxyBus.HandleRequest(msg)
	assert.Equal(t, proxyerrors.InternalErrCode, int(response.ErrorCode))

	msg = &common.ChannelBusMessage{
		MsgType:  common.ReqMsg,
		PacketID: "1dddb175-2fc5-4ee7-8df4-928b089f7b69",
		Request:  &commontypes.TCPRequest{RequestID: uint64(1)},
	}
	response = proxyBus.HandleRequest(msg)
	assert.Equal(t, proxyerrors.StateOKCode, int(response.ErrorCode))
}

func TestStartRecvMessage(t *testing.T) {
	busOption := common.BusOption{}
	busOption.NewBusOpt = &common.ChannelBusOption{IsProxy: true, Receiver: &MockChannelBusConn{}}
	proxyBus, _ := NewChannelBus(busOption)
	proxyBus.StartRecvMessage()

	packageID := "1"
	msg := &common.ChannelBusMessage{MsgType: common.ReqMsg, PacketID: packageID}
	proxyBus.chanConn.receiveChan <- msg

	taskBusOption := common.BusOption{}
	taskBusOption.NewBusOpt = &common.ChannelBusOption{}
	taskBus, _ := NewChannelBus(taskBusOption)
	taskBus.pending.Store(packageID, &pendingReq{done: make(chan *common.ChannelBusMessage, 1)})
	taskBus.StartRecvMessage()
	time.Sleep(1 * time.Second)
	_, ok := taskBus.pending.Load(packageID)
	assert.Equal(t, false, ok)
	time.Sleep(1 * time.Second)
}

func TestChannelBusFunc(t *testing.T) {
	taskBusOption := common.BusOption{}
	taskBusOption.NewBusOpt = &common.ChannelBusOption{}
	taskBus, _ := NewChannelBus(taskBusOption)

	err := taskBus.SetResult(&commontypes.TCPResponse{})
	assert.Nil(t, err)

	err = taskBus.WaitRequestsFinished(true, nil)
	assert.Nil(t, err)

	assert.Equal(t, true, taskBus.CheckHealth())
	assert.Equal(t, true, taskBus.CheckAuth("", ""))

	_, err = taskBus.Cancel(context.Background(), nil, nil)
	assert.Error(t, err, "not implement")

	_, err = taskBus.ReportRuntimeMetrics(context.Background(), nil)
	assert.Error(t, err, "not implement")

	_, err = taskBus.Recover(context.Background(), nil)
	assert.Error(t, err, "not implement")

	err = taskBus.SetCancelResult(nil)
	assert.Error(t, err, "not implement")

	err = taskBus.SetRecoverResult(nil)
	assert.Error(t, err, "not implement")

	taskBus.Stop()
	taskBus.RegisterReportRuntimeMetrics(nil)
}

// ChannelBusConn channel bus connection struct
type MockChannelBusConn struct {
}

// HandleRequest handle users' request
func (c *MockChannelBusConn) HandleRequest(msg *common.ChannelBusMessage) common.ReqResult {
	fmt.Println("MockChannelBusConn handle request")
	return common.ReqResult{Response: &commontypes.TCPResponse{RawData: []byte("SUCCESS")},
		ErrorCode: proxyerrors.StateOKCode, ErrorMsg: ""}
}

func Test_CheckRuntimeConnect(t *testing.T) {
	cb := &ChannelBus{}
	cb.CheckRuntimeConnect()
}

func Test_Init(t *testing.T) {
	cb := &ChannelBus{}
	ctx := context.TODO()
	cb.Init(ctx, 1)
	cb.CheckRuntime(ctx, "traceID", false)
}

func Test_asyncRequest(t *testing.T) {
	convey.Convey("Test asyncRequest", t, func() {
		convey.Convey("cb.collector.AddRequest() err != nil", func() {
			cb := &ChannelBus{collector: &calls.CallCollector{}}
			ctx := context.TODO()
			dst := &common.FunctionProperty{}
			req := &commontypes.TCPRequest{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*calls.CallCollector).AddRequest, func(_ *calls.CallCollector) (uint64, chan *commontypes.TCPResponse, error) {
					return 0, nil, errors.New("error")
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			if _, err := cb.asyncRequest(ctx, dst, req); err == nil {
				t.Errorf("this is error")
			}
		})

		convey.Convey("cb.collector.AddRequest() ch = nil", func() {
			cb := &ChannelBus{collector: &calls.CallCollector{}}
			ctx := context.TODO()
			dst := &common.FunctionProperty{}
			req := &commontypes.TCPRequest{}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*calls.CallCollector).AddRequest, func(_ *calls.CallCollector) (uint64, chan *commontypes.TCPResponse, error) {
					return 0, nil, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			if _, err := cb.asyncRequest(ctx, dst, req); err == nil {
				t.Errorf("this is error")
			}
		})

		convey.Convey("cb.collector.AddRequest() callback error", func() {

			ctx := context.TODO()
			dst := &common.FunctionProperty{}
			req := &commontypes.TCPRequest{}
			ch := make(chan *commontypes.TCPResponse, 1)
			cb := &ChannelBus{collector: &calls.CallCollector{}, callback: func(ctx context.Context, dst *common.FunctionProperty, req *commontypes.TCPRequest) error {
				close(ch)
				return errors.New("error")
			}}
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*calls.CallCollector).AddRequest, func(_ *calls.CallCollector) (uint64, chan *commontypes.TCPResponse, error) {
					return 0, ch, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			if _, err := cb.asyncRequest(ctx, dst, req); err == nil {
				t.Errorf("this is error")
			}
		})

	})
}
