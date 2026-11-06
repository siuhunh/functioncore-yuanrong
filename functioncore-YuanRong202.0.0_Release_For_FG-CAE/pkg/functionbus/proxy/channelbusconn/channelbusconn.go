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
	"fmt"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/functionbus/bus"
	"functioncore/pkg/functionbus/bus/channelbus"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/utils"
)

const (
	defaultRetryInterval    = 5 * time.Second // write task info into etcd failed retryInterval
	registerRetryTimes      = 2
	putRouterEtcdRetryTimes = 3
)

// ConnOption option of creating channel bus connection
type ConnOption struct {
	RequestHandler internal.HandleReq
	EtcdHandler    common.ETCDConnectHandler
}

// ChannelBusConn channel bus connection struct
type ChannelBusConn struct {
	channelBus     bus.CommunicationBus
	readySign      chan struct{}
	requestHandler internal.HandleReq
	etcdHandler    common.ETCDConnectHandler
}

// CreateChannelBusConn create a channel bus connection
func CreateChannelBusConn(opt *ConnOption) (*ChannelBusConn, error) {
	if opt == nil || opt.RequestHandler == nil {
		return nil, fmt.Errorf("channel bus request handle is nil")
	}
	channelBusConn := &ChannelBusConn{
		readySign:      make(chan struct{}, 1),
		requestHandler: opt.RequestHandler,
		etcdHandler:    opt.EtcdHandler,
	}

	return channelBusConn, nil
}

// AwaitReady await ChannelBus of task registers to proxy
func (c *ChannelBusConn) AwaitReady() {
	<-c.readySign
}

// StartChannelBusConn start channel bus connection
func (c *ChannelBusConn) StartChannelBusConn() error {
	channelBusOpt := &common.ChannelBusOption{Receiver: c, IsProxy: true}
	channelBus, err := channelbus.NewChannelBus(common.BusOption{NewBusOpt: channelBusOpt})
	if err != nil {
		log.GetLogger().Errorf("failed to create a new channel bus")
		return err
	}
	channelBus.StartRecvMessage()

	c.channelBus = channelBus

	return nil
}

// HandleRequest handle users' request
func (c *ChannelBusConn) HandleRequest(msg *common.ChannelBusMessage) common.ReqResult {
	switch msg.MsgType {
	case common.RegMsg: // task registry to proxy
		if err := c.handleRegMsg(msg.Property); err != nil {
			log.GetLogger().Errorf("failed to handle registry message: %s", err)
			return common.ReqResult{Response: nil, ErrorCode: proxyerrors.ProcessRegErr, ErrorMsg: "register error"}
		}
		return common.ReqResult{Response: &commontypes.TCPResponse{RawData: []byte("register successfully")},
			ErrorCode: proxyerrors.StateOKCode, ErrorMsg: ""}
	case common.ReqMsg:
		return c.handleReqMsg(msg)
	default:
		log.GetLogger().Errorf("unknown message type: %d", msg.MsgType)
		return common.ReqResult{Response: nil, ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg: "unknown message type"}
	}
}

// handleRegMsg proxy handle register message from task
func (c *ChannelBusConn) handleRegMsg(msg *common.FunctionProperty) error {
	log.GetLogger().Infof("start to handle register msg, tenantID: %s funcName: %s insID: %s",
		urnutils.Anonymize(msg.TenantID), msg.FunctionName, msg.InstanceID)

	// message not from task
	if msg.TenantID != common.TaskTenantID {
		log.GetLogger().Errorf("registry message not from task, invalid tenantID %f",
			urnutils.Anonymize(msg.TenantID))
		return fmt.Errorf("registry message not from task")
	}

	if err := c.sendRegMsg(msg); err != nil {
		return err
	}

	c.readySign <- struct{}{}

	return nil
}

// handleReqMsg proxy or task process request message
func (c *ChannelBusConn) handleReqMsg(msg *common.ChannelBusMessage) common.ReqResult {
	log.GetLogger().Debugf("start to handle request message, tenantID %s funcName %s insID %s, stateID %s,"+
		" traceID %s, memory: %s, cpu: %s", urnutils.Anonymize(msg.Property.TenantID), msg.Property.FunctionName,
		msg.Property.InstanceID, msg.Property.StateID, msg.TraceID, msg.Request.Memory, msg.Request.CPU)

	var err error
	if msg.Property.FunctionVersion == "" {
		msg.Property.FunctionName, msg.Property.FunctionVersion, err = internal.ConvertFuncName(msg.Property.TenantID,
			msg.Property.FunctionName, msg.Property.StateID)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to handle functionInfo: %s, traceID: %s", err, msg.TraceID)
		return common.ReqResult{Response: nil, ErrorCode: proxyerrors.InternalErrCode,
			ErrorMsg: proxyerrors.InternalErr}
	}
	busCallReq := &commontypes.TCPRequest{
		StateID:         msg.Property.StateID,
		LogType:         msg.Request.LogType,
		RawData:         msg.Request.RawData,
		RequestID:       msg.Request.RequestID,
		UserData:        msg.Request.UserData,
		Priority:        msg.Request.Priority,
		PersistResponse: msg.Request.PersistResponse,
		Extra:           msg.Request.Extra,
		WorkerID:        msg.Request.WorkerID,
		CPU:             msg.Request.CPU,
		Memory:          msg.Request.Memory,
		IsStateFul:      msg.Request.IsStateFul,
	}
	dst := &common.FunctionProperty{
		TenantID:        msg.Property.TenantID,
		FunctionName:    msg.Property.FunctionName,
		FunctionVersion: msg.Property.FunctionVersion,
		InstanceID:      msg.Property.InstanceID,
		StateID:         msg.Property.StateID,
		FutureID:        msg.Property.FutureID,
	}
	resp := c.requestHandler(msg.TraceID, busCallReq, dst)
	log.GetLogger().Debugf("request got errCode %d traceID %s", resp.ErrorCode, msg.TraceID)
	return common.ReqResult{Response: resp, ErrorCode: proxyerrors.StateOKCode, ErrorMsg: ""}
}

// sendRegMsg local task registry info, should write this info in etcd
func (c *ChannelBusConn) sendRegMsg(msg *common.FunctionProperty) error {
	var err error
	var regInfo = RegisterInfo{
		FuncName:        msg.FunctionName,
		NodeID:          msg.InstanceID,
		FunctionVersion: msg.FunctionVersion,
		Tenant:          msg.TenantID,
		WorkerInstance: WorkerInstance{
			IP:       "",
			Port:     "8080",
			Status:   "ready",
			P2pPort:  constants.TCPort,
			NodeIP:   internal.SelfNodeIP,
			NodePort: constants.HTTPort,
		},
	}
	register := NewRegistry(regInfo, signals.WaitForSignal(), c.etcdHandler)
	for i := 0; i < registerRetryTimes; i++ {
		if err = register.Register(); err == nil {
			return nil
		}
		log.GetLogger().Errorf("failed to send register message: %s, retry times %d", err, i)
		time.Sleep(defaultRetryInterval)
	}
	return err
}

// SendRequest send requests to tasks
func (c *ChannelBusConn) SendRequest(traceID string, callReq *commontypes.TCPRequest,
	dst *common.FunctionProperty) *commontypes.TCPResponse {

	ctx := utils.ContextWithValueString(context.Background(), constants.HeaderTraceID, traceID)
	resp, err := c.channelBus.Call(ctx, dst, callReq, false)

	if err != nil {
		log.GetLogger().Errorf("failed to call %s: %s, traceID %s", dst.InstanceID, err, traceID)
		return &commontypes.TCPResponse{
			RequestID:    callReq.RequestID,
			ErrorCode:    proxyerrors.SendReqErrCode,
			ErrorMessage: proxyerrors.SendReqErr,
		}
	}
	return resp
}
