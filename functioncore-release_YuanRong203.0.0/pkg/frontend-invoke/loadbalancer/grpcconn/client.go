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

// Package grpcconn contains grpc client logic
package grpcconn

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
)

const (
	dialTimeout   = 5 * time.Second
	disableStatus = 0
	enableStatus  = 1
)

var (
	heartbeatInterval        = 2 * time.Second
	heartbeatTimeout         = 2 * time.Second
	heartbeatFailedThreshold = 10
)

type clientAdapter struct {
	conn   *grpc.ClientConn
	client pb.SchedulerRPCClient
	config common.RPCClientConfig
	enable uint32
	stopCh chan struct{}
}

// CreateGrpcClient -
func CreateGrpcClient(config common.RPCClientConfig) (common.RPCClient, error) {
	return &clientAdapter{
		config: config,
		enable: 0,
		stopCh: make(chan struct{}, 1),
	}, nil
}

// CreateConnection -
func (c *clientAdapter) CreateConnection() (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
	defer cancel()
	dst := fmt.Sprintf("%s:%s", c.config.IP, c.config.Port)
	credential := c.getCredentials()
	conn, err := grpc.DialContext(ctx, dst, grpc.WithTransportCredentials(credential))
	if err != nil {
		log.GetLogger().Errorf("failed to DialContext, err %s", err.Error())
		return conn, err
	}
	return conn, nil

}

// Start -
func (c *clientAdapter) Start() {
	conn, err := c.CreateConnection()
	if err != nil {
		log.GetLogger().Errorf("failed to CreateConnection, err %s", err.Error())
		return
	}
	c.conn = conn
	c.client = pb.NewSchedulerRPCClient(conn)
	atomic.StoreUint32(&c.enable, enableStatus)
	go c.circleHeartbeat()
}

// IsEnable -
func (c *clientAdapter) IsEnable() bool {
	status := atomic.LoadUint32(&c.enable)
	return status == enableStatus
}

// Stop -
func (c *clientAdapter) Stop() {
	close(c.stopCh)
	if c.conn != nil {
		c.conn.Close()
	}
	log.GetLogger().Infof("clientAdapter stopped!, c.config is %+v", c.config)
}

// ServerKey get client used scheduler server key
func (c *clientAdapter) ServerKey() string {
	return c.config.ServerKey
}

// AcquireInstance -
func (c *clientAdapter) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	grpcReq := &pb.AcquireInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.config.RequestTimeout)
	defer cancel()
	grpcRsp, err := c.client.AcquireInstance(ctx, grpcReq)
	if err != nil {
		return nil, err
	}
	return &schedulerapi.AcquireInstanceRsp{
		InstanceKey:  grpcRsp.InstanceKey,
		ProcessorID:  grpcRsp.ProcessorID,
		ThreadNum:    grpcRsp.ThreadNum,
		RestThread:   grpcRsp.RestThread,
		LeaseVersion: grpcRsp.LeaseVersion,
		Error: schedulerapi.Error{
			ErrorCode: grpcRsp.Error.ErrorCode,
		},
	}, nil
}

// ReleaseInstance -
func (c *clientAdapter) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	grpcReq := &pb.ReleaseInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.RequestTimeout)
	defer cancel()
	_, err := c.client.ReleaseInstance(ctx, grpcReq)
	if err != nil {
		return err
	}
	return nil
}

func (c *clientAdapter) CheckInstance(req *schedulerapi.CheckInstanceReq) error {
	return nil
}

// RetainInstance -
func (c *clientAdapter) RetainInstance(req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	grpcReq := &pb.RetainInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.config.RequestTimeout)
	defer cancel()
	grpcRsp, err := c.client.RetainInstance(ctx, grpcReq)
	if err != nil {
		return nil, err
	}
	return &schedulerapi.RetainInstanceRsp{
		Error: schedulerapi.Error{
			ErrorCode: grpcRsp.Error.ErrorCode,
		},
	}, nil
}

func (c *clientAdapter) circleHeartbeat() {
	ticker := time.NewTicker(heartbeatInterval)
	failedCnt := 0
	for {
		select {
		case <-ticker.C:
			healthy := c.heartbeat()
			if !healthy {
				failedCnt++
			} else {
				alarm.GetClient().ClearAlarm(alarm.FaaSSchedulerConnection)
				failedCnt = 0
				atomic.StoreUint32(&c.enable, enableStatus)
			}

			if failedCnt >= heartbeatFailedThreshold && failedCnt%heartbeatFailedThreshold == 0 {
				msg := fmt.Sprintf("heartbeat to %s failed over %d times",
					c.config.ServerKey, heartbeatFailedThreshold)
				alarm.GetClient().ReportAlarm(alarm.FaaSSchedulerConnection, msg)
				log.GetLogger().Errorf("heartbeat to %s failed cnt %d",
					c.config.ServerKey, failedCnt)
				atomic.StoreUint32(&c.enable, disableStatus)
			}
		case <-c.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (c *clientAdapter) heartbeat() bool {
	req := &pb.HeartbeatReq{}
	ctx, cancel := context.WithTimeout(context.Background(), heartbeatTimeout)
	defer cancel()
	_, err := c.client.Heartbeat(ctx, req)
	if err != nil {
		log.GetLogger().Errorf("failed to send heartbeat to scheduler %s err %s",
			c.config.ServerKey, err.Error())
		return false
	}
	log.GetLogger().Debugf("success to send heartbeat to scheduler %s", c.config.ServerKey)
	return true
}

func (c *clientAdapter) getCredentials() credentials.TransportCredentials {
	if constant.FConfig.HTTPSConfig.HTTPSEnable {
		return credentials.NewTLS(tls.GetClientTLSConfig())
	}

	return insecure.NewCredentials()
}
