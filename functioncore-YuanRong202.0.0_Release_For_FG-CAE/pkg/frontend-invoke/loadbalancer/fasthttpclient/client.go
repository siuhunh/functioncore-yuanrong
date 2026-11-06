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

// Package fasthttpclient -
package fasthttpclient

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/util"
)

const (
	maxConn             = 10000
	readTimeout         = 5 * time.Second
	writeTimeout        = 5 * time.Second
	readBufSize         = 1 * 1024
	maxIdleConnDuration = 5 * time.Second
	dialTimeout         = 250 * time.Millisecond

	websocketReadTimeout = 25 * time.Minute
)

var (
	instance = struct {
		c *fasthttp.Client
		sync.Once
	}{}
	websocketInstance = struct {
		c *fasthttp.Client
		sync.Once
	}{}
	heartbeatInterval  = 2 * time.Second
	heartbeatTimeout   = 2 * time.Second
	notEnableThreshold = 10
	alarmThreshold     = 150
)

// CreateClient -
func CreateClient(config common.RPCClientConfig) (common.RPCClient, error) {
	return &clientAdapter{
		conn:          getClientInstance(),
		websocketConn: getWebsocketClientInstance(),
		config:        config,
		stopCh:        make(chan struct{}, 1),
		serverAddr:    fmt.Sprintf("%s:%s", config.IP, config.Port),
	}, nil
}

func getClientInstance() *fasthttp.Client {
	instance.Do(func() {
		instance.c = newFastHTTPClient(readTimeout)
	})
	return instance.c
}

func getWebsocketClientInstance() *fasthttp.Client {
	websocketInstance.Do(func() {
		websocketInstance.c = newFastHTTPClient(websocketReadTimeout)
	})
	return websocketInstance.c
}

func newFastHTTPClient(readTimeout time.Duration) *fasthttp.Client {
	tcpDialer := fasthttp.TCPDialer{Concurrency: maxConn}
	return &fasthttp.Client{
		MaxIdleConnDuration: maxIdleConnDuration,
		MaxConnsPerHost:     maxConn,
		ReadTimeout:         readTimeout,
		WriteTimeout:        writeTimeout,
		ReadBufferSize:      readBufSize,
		Dial: func(addr string) (net.Conn, error) {
			return tcpDialer.DialTimeout(addr, dialTimeout)
		},
	}
}

type clientAdapter struct {
	conn          *fasthttp.Client
	websocketConn *fasthttp.Client
	config        common.RPCClientConfig
	stopCh        chan struct{}
	enable        bool
	serverAddr    string
}

// Start -
func (c *clientAdapter) Start() {
	c.enable = true
	go c.circleHeartbeat()
}

// IsEnable -
func (c *clientAdapter) IsEnable() bool {
	return c.enable
}

// Stop -
func (c *clientAdapter) Stop() {
	close(c.stopCh)
	c.enable = false
}

// ServerKey get client used scheduler server key
func (c *clientAdapter) ServerKey() string {
	return c.config.ServerKey
}

// AcquireInstance -
func (c *clientAdapter) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	httpReq := fasthttp.AcquireRequest()
	httpRsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(httpReq)
	defer fasthttp.ReleaseResponse(httpRsp)

	c.setCommonHeader(httpReq)
	err := c.encodeAcquireInstanceReq(req, httpReq)
	if err != nil {
		return nil, err
	}

	httpReq.SetRequestURI(schedulerapi.AcquireInstanceURI)
	if req.ReqType > schedulerapi.NormalRequest {
		requestTimeout, err := time.ParseDuration(req.Timeout)
		if err != nil {
			return nil, fmt.Errorf("failed to parse duration, err: %s", err.Error())
		}
		log.GetLogger().Debugf("%s | funcKeyWithRes: %s | timeout: %s | start to send request to faas-scheduler",
			req.TraceID, req.FuncKeyWithRes, req.Timeout)
		err = c.websocketConn.DoTimeout(httpReq, httpRsp, requestTimeout)
	} else {
		err = c.conn.DoTimeout(httpReq, httpRsp, c.config.RequestTimeout)
	}
	if err != nil {
		return nil, err
	}
	log.GetLogger().Debugf("%s | funcKeyWithRes: %s | httpRsp: %s | succeed to receive response from "+
		"faas-scheduler", req.TraceID, req.FuncKeyWithRes, httpRsp.Body())
	if httpRsp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("failed to AcquireInstance, http code %d", httpRsp.StatusCode())
	}

	rsp, err := c.decodeAcquireInstanceRsp(httpRsp)
	log.GetLogger().Debugf("%s | funcKeyWithRes: %s | instanceKey: %s | processorID: %d | succeed to "+
		"decode response body", req.TraceID, req.FuncKeyWithRes, rsp.InstanceKey, rsp.ProcessorID)
	if err != nil {
		return nil, err
	}
	if len(rsp.InstanceKey) == 0 {
		return nil, errors.New("no available instance returned")
	}
	return rsp, nil
}

// ReleaseInstance -
func (c *clientAdapter) ReleaseInstance(
	req *schedulerapi.ReleaseInstanceReq) error {
	httpReq := fasthttp.AcquireRequest()
	httpRsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(httpReq)
	defer fasthttp.ReleaseResponse(httpRsp)

	c.setCommonHeader(httpReq)
	err := c.encodeReleaseInstanceReq(req, httpReq)
	if err != nil {
		return err
	}

	httpReq.SetRequestURI(schedulerapi.ReleaseInstanceURI)
	err = c.conn.DoTimeout(httpReq, httpRsp, c.config.RequestTimeout)
	if err != nil {
		return err
	}

	if httpRsp.StatusCode() != http.StatusOK {
		return fmt.Errorf("failed to release instance, err: %s", httpRsp.Body())
	}

	return nil
}

// RetainInstance -
func (c *clientAdapter) RetainInstance(
	req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	httpReq := fasthttp.AcquireRequest()
	httpRsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(httpReq)
	defer fasthttp.ReleaseResponse(httpRsp)

	c.setCommonHeader(httpReq)
	err := c.encodeRetainInstanceReq(req, httpReq)
	if err != nil {
		return nil, err
	}

	httpReq.SetRequestURI(schedulerapi.RetainInstanceURI)
	err = c.conn.DoTimeout(httpReq, httpRsp, c.config.RequestTimeout)
	if err != nil {
		return nil, err
	}

	if httpRsp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("failed to RetainInstance, http code %d", httpRsp.StatusCode())
	}

	rsp, err := c.decodeRetainInstanceRsp(httpRsp)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (c *clientAdapter) setCommonHeader(req *fasthttp.Request) {
	req.Header.SetMethod(http.MethodPost)
	req.Header.ResetConnectionClose()
	req.Header.SetHost(c.serverAddr)
	req.URI().SetScheme(tls.GetURLScheme(constant.FConfig.HTTPSConfig.HTTPSEnable))

	// authorization
	var authorization string
	var timestamp string
	authorization, timestamp = localauth.SignLocally(constant.FConfig.LocalAuth.AKey,
		constant.FConfig.LocalAuth.SKey, util.AppID, constant.FConfig.LocalAuth.Duration)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.HeaderAuthorization, authorization)
}

func (c *clientAdapter) encodeAcquireInstanceReq(
	req *schedulerapi.AcquireInstanceReq, ctx *fasthttp.Request) error {
	protoReq := &pb.AcquireInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		ReqType:        req.ReqType,
		WebSocketInsAcquireConfig: &pb.WebSocketInsAcquireConfig{
			TargetIP:   req.WebSocketInsAcquireConfig.TargetIP,
			TargetPort: req.WebSocketInsAcquireConfig.TargetPort,
			Path:       req.WebSocketInsAcquireConfig.Path,
		},
		Timeout: req.Timeout,
	}
	body, err := proto.Marshal(protoReq)
	if err != nil {
		return err
	}
	ctx.SetBody(body)
	return nil
}

func (c *clientAdapter) decodeAcquireInstanceRsp(
	ctx *fasthttp.Response) (*schedulerapi.AcquireInstanceRsp, error) {
	body := ctx.Body()
	rsp := &pb.AcquireInstanceRsp{}
	err := proto.Unmarshal(body, rsp)
	if err != nil {
		return nil, err
	}
	if rsp.Error == nil {
		rsp.Error = &pb.Error{
			ErrorCode:    0,
			ErrorMessage: "",
		}
	}
	return &schedulerapi.AcquireInstanceRsp{
		InstanceKey:  rsp.InstanceKey,
		ProcessorID:  rsp.ProcessorID,
		ThreadNum:    rsp.ThreadNum,
		LeaseVersion: rsp.LeaseVersion,
		InstanceIP:   rsp.InstanceIP,
		InstancePort: rsp.InstancePort,
		NodeIP:       rsp.NodeIP,
		NodePort:     rsp.NodePort,
		Error: schedulerapi.Error{
			ErrorCode:    rsp.Error.ErrorCode,
			ErrorMessage: rsp.Error.ErrorMessage,
		},
	}, nil
}

func (c *clientAdapter) encodeReleaseInstanceReq(
	req *schedulerapi.ReleaseInstanceReq, ctx *fasthttp.Request) error {
	protoReq := &pb.ReleaseInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
		IsWebSocketReq: req.IsWebSocketReq,
	}
	body, err := proto.Marshal(protoReq)
	if err != nil {
		return err
	}
	ctx.SetBody(body)
	return nil
}

func (c *clientAdapter) encodeRetainInstanceReq(
	req *schedulerapi.RetainInstanceReq, ctx *fasthttp.Request) error {
	protoReq := &pb.RetainInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		ProcessorID:    req.ProcessorID,
		InstanceKey:    req.InstanceKey,
		LeaseVersion:   req.LeaseVersion,
	}
	body, err := proto.Marshal(protoReq)
	if err != nil {
		return err
	}
	ctx.SetBody(body)
	return nil
}

func (c *clientAdapter) decodeRetainInstanceRsp(
	ctx *fasthttp.Response) (*schedulerapi.RetainInstanceRsp, error) {
	body := ctx.Body()
	rsp := &pb.RetainInstanceRsp{}
	err := proto.Unmarshal(body, rsp)
	if err != nil {
		return nil, err
	}

	return &schedulerapi.RetainInstanceRsp{
		Error: schedulerapi.Error{
			ErrorCode: rsp.Error.ErrorCode,
		},
	}, nil
}

func (c *clientAdapter) circleHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	failedCnt := 0
	for {
		select {
		case <-ticker.C:
			err := c.heartbeat()
			if err == nil {
				alarm.GetClient().ClearAlarm(alarm.FaaSSchedulerConnection)
				failedCnt = 0
				c.enable = true
				break
			}

			failedCnt++
			log.GetLogger().Warnf("heartbeat to %s failed cnt %d err %s",
				c.config.ServerKey, failedCnt, err)

			if failedCnt >= notEnableThreshold {
				c.enable = false
			}

			if failedCnt >= alarmThreshold && failedCnt%alarmThreshold == 0 {
				msg := fmt.Sprintf("heartbeat to %s failed over %d times",
					c.config.ServerKey, notEnableThreshold)
				alarm.GetClient().ReportAlarm(alarm.FaaSSchedulerConnection, msg)
			}
		case <-c.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (c *clientAdapter) heartbeat() error {
	httpReq := fasthttp.AcquireRequest()
	httpRsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(httpReq)
	defer fasthttp.ReleaseResponse(httpRsp)

	c.setCommonHeader(httpReq)
	httpReq.SetRequestURI(schedulerapi.HeartbeatURI)
	err := c.conn.DoTimeout(httpReq, httpRsp, c.config.RequestTimeout)
	if err != nil {
		return err
	}

	if httpRsp.StatusCode() != http.StatusOK {
		return fmt.Errorf("failed to DoTimeout, http code %d", httpRsp.StatusCode())
	}

	log.GetLogger().Debugf("success to send heartbeat to scheduler %s", c.config.ServerKey)
	return nil
}
