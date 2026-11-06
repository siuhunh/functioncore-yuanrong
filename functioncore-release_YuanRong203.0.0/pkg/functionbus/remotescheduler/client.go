/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package remotescheduler -
package remotescheduler

import (
	"encoding/json"
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
	"functioncore/pkg/functionbus/common"
)

const (
	notEnableThreshold            = 3
	alarmThreshold                = 30
	heartbeatInterval             = 10 * time.Second
	heartbeatTimeout              = 2 * time.Second
	maxConn                       = 10000
	writeTimeout                  = 5 * time.Second
	readTimeout                   = 5 * time.Second
	readBufSize                   = 1 * 1024
	maxIdleConnDuration           = 5 * time.Second
	dialTimeout                   = 250 * time.Millisecond
	defaultAcquireInstanceTimeout = 250 * time.Millisecond
)

var (
	instance = struct {
		c *fasthttp.Client
		sync.Once
	}{}
)

type clientAdapter struct {
	conn       *fasthttp.Client
	instanceID string
	serverAddr string
	stopCh     chan struct{}
	enable     bool
	config     requestConfig
	sync.Mutex
}

type requestConfig struct {
	acquireInstanceTimeout time.Duration
}

// AcquireInstance acquire instance
func (c *clientAdapter) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	httpReq := fasthttp.AcquireRequest()
	httpRsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(httpReq)
	defer fasthttp.ReleaseResponse(httpRsp)

	c.setCommonHeader(httpReq)
	err := c.encodeAcquireReq(req, httpReq)
	if err != nil {
		return nil, err
	}

	httpReq.SetRequestURI(schedulerapi.AcquireInstanceURI)
	err = c.conn.DoTimeout(httpReq, httpRsp, c.config.acquireInstanceTimeout)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Debugf("%s | funcKeyWithRes: %s | httpRsp: %s | succeed to receive response from "+
		"faas-scheduler", req.TraceID, req.FuncKeyWithRes, httpRsp.Body())
	if httpRsp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("failed to AcquireInstance, http code %d", httpRsp.StatusCode())
	}

	rsp, err := c.decodeAcquireRsp(httpRsp)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Debugf("%s | funcKeyWithRes: %s | instanceKey: %s | processorID: %d | succeed to "+
		"decode response body", req.TraceID, req.FuncKeyWithRes, rsp.InstanceKey, rsp.ProcessorID)
	if len(rsp.InstanceKey) == 0 {
		return nil, errors.New("no available instance")
	}
	return rsp, nil
}

func (c *clientAdapter) encodeAcquireReq(
	req *schedulerapi.AcquireInstanceReq, ctx *fasthttp.Request) error {
	protoReq := &pb.AcquireInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
	}
	body, err := proto.Marshal(protoReq)
	if err != nil {
		return err
	}
	ctx.SetBody(body)
	return nil
}

func (c *clientAdapter) decodeAcquireRsp(
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
		InstanceIP:   rsp.InstanceIP,
		InstancePort: rsp.InstancePort,
		NodeIP:       rsp.NodeIP,
		NodePort:     rsp.NodePort,
		ProcessorID:  rsp.ProcessorID,
		ThreadNum:    rsp.ThreadNum,
		RestThread:   rsp.RestThread,
		LeaseVersion: rsp.LeaseVersion,
		Error: schedulerapi.Error{
			ErrorCode:    rsp.Error.ErrorCode,
			ErrorMessage: rsp.Error.ErrorMessage,
		},
	}, nil
}

// ReleaseInstance release instance
func (c *clientAdapter) ReleaseInstance(
	req *schedulerapi.ReleaseInstanceReq) error {
	httpReq := fasthttp.AcquireRequest()
	httpRsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(httpReq)
	defer fasthttp.ReleaseResponse(httpRsp)

	c.setCommonHeader(httpReq)
	err := c.encodeReleaseReq(req, httpReq)
	if err != nil {
		return err
	}

	httpReq.SetRequestURI(schedulerapi.ReleaseInstanceURI)
	err = c.conn.DoTimeout(httpReq, httpRsp, c.config.acquireInstanceTimeout)
	if err != nil {
		return err
	}
	if httpRsp.StatusCode() != http.StatusOK {
		return fmt.Errorf("failed to release instance, err: %s", httpRsp.Body())
	}
	return nil
}

func (c *clientAdapter) encodeReleaseReq(
	req *schedulerapi.ReleaseInstanceReq, ctx *fasthttp.Request) error {
	request := &pb.ReleaseInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
	}
	body, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	ctx.SetBody(body)
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
	err := c.encodeRetainReq(req, httpReq)
	if err != nil {
		return nil, err
	}
	httpReq.SetRequestURI(schedulerapi.RetainInstanceURI)
	err = c.conn.DoTimeout(httpReq, httpRsp, c.config.acquireInstanceTimeout)
	if err != nil {
		return nil, err
	}
	if httpRsp.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("failed to retain instance, http code %d", httpRsp.StatusCode())
	}
	rsp, err := c.decodeRetainRsp(httpRsp)
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (c *clientAdapter) encodeRetainReq(
	req *schedulerapi.RetainInstanceReq, ctx *fasthttp.Request) error {
	protoReq := &pb.RetainInstanceReq{
		FuncKeyWithRes: req.FuncKeyWithRes,
		TraceID:        req.TraceID,
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

func (c *clientAdapter) decodeRetainRsp(
	ctx *fasthttp.Response) (*schedulerapi.RetainInstanceRsp, error) {
	body := ctx.Body()
	rsp := &pb.RetainInstanceRsp{}
	err := proto.Unmarshal(body, rsp)
	if err != nil {
		return nil, err
	}

	if rsp.Error == nil {
		return &schedulerapi.RetainInstanceRsp{}, nil
	}

	return &schedulerapi.RetainInstanceRsp{
		Error: schedulerapi.Error{
			ErrorCode: rsp.Error.ErrorCode,
		},
	}, nil
}

// CheckInstance check instance
func (c *clientAdapter) CheckInstance(req *schedulerapi.CheckInstanceReq) error {
	httpReq := fasthttp.AcquireRequest()
	httpRsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(httpReq)
	defer fasthttp.ReleaseResponse(httpRsp)

	c.setCommonHeader(httpReq)
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	httpReq.SetBody(body)

	httpReq.SetRequestURI(schedulerapi.CheckInstanceURI)
	return c.conn.DoTimeout(httpReq, httpRsp, c.config.acquireInstanceTimeout)
}

func createClient(instanceID, ip, port string) *clientAdapter {
	config := requestConfig{acquireInstanceTimeout: defaultAcquireInstanceTimeout}
	if common.FConfig.RemoteSchedulerConfig != nil && common.FConfig.RemoteSchedulerConfig.AcquireInstanceTimeout > 0 {
		config.acquireInstanceTimeout =
			time.Duration(common.FConfig.RemoteSchedulerConfig.AcquireInstanceTimeout) * time.Millisecond
	}
	return &clientAdapter{
		instanceID: instanceID,
		conn:       getClientConn(),
		stopCh:     make(chan struct{}, 1),
		serverAddr: fmt.Sprintf("%s:%s", ip, port),
		config:     config,
	}
}

func getClientConn() *fasthttp.Client {
	instance.Do(func() {
		instance.c = newFastHTTPClient(readTimeout)
	})
	return instance.c
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

func (c *clientAdapter) start() {
	c.Lock()
	c.enable = true
	c.Unlock()
	go c.circleHeartbeat()
}

func (c *clientAdapter) circleHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	failedCount := 0
	for {
		select {
		case <-ticker.C:
			err := c.heartbeat()
			if err == nil {
				alarm.GetClient().ClearAlarm(alarm.FaaSSchedulerConnection)
				failedCount = 0
				c.Lock()
				c.enable = true
				c.Unlock()
				break
			}

			failedCount++
			log.GetLogger().Warnf("heartbeat to %s failed cnt %d err %s", c.instanceID, failedCount, err)

			if failedCount >= notEnableThreshold {
				c.Lock()
				c.enable = false
				c.Unlock()
			}

			if failedCount >= alarmThreshold && failedCount%alarmThreshold == 0 {
				msg := fmt.Sprintf("heartbeat to %s failed over %d times", c.instanceID, notEnableThreshold)
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
	err := c.conn.DoTimeout(httpReq, httpRsp, heartbeatTimeout)
	if err != nil {
		return err
	}

	if httpRsp.StatusCode() != http.StatusOK {
		return fmt.Errorf("failed to DoTimeout, http code %d", httpRsp.StatusCode())
	}

	log.GetLogger().Debugf("success to send heartbeat to scheduler %s", c.instanceID)
	return nil
}

func (c *clientAdapter) setCommonHeader(req *fasthttp.Request) {
	req.Header.SetMethod(http.MethodPost)
	req.Header.ResetConnectionClose()
	req.Header.SetHost(c.serverAddr)
	req.URI().SetScheme(tls.GetURLScheme(common.FConfig.HTTPSConfig.HTTPSEnable))
	var authorization string
	var timestamp string
	authorization, timestamp = localauth.SignLocally(common.FConfig.LocalAuth.AKey,
		common.FConfig.LocalAuth.SKey, constants.FunctionTask, common.FConfig.LocalAuth.Duration)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.HeaderAuthorization, authorization)
}

func (c *clientAdapter) isEnable() bool {
	return c.enable
}

func (c *clientAdapter) stop() {
	close(c.stopCh)
	c.Lock()
	c.enable = false
	c.Unlock()
}
