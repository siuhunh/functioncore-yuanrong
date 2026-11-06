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

// Package grpcserver contains grpc server logic
package grpcserver

import (
	"context"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/grpc/pb"
)

type serverAdapter struct {
	pb.UnimplementedSchedulerRPCServer
	service schedulerapi.Scheduler
}

// AcquireInstance -
func (s *serverAdapter) AcquireInstance(
	c context.Context, req *pb.AcquireInstanceReq) (*pb.AcquireInstanceRsp, error) {
	innerReq := &schedulerapi.AcquireInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
	}
	innerRsp, err := s.service.AcquireInstance(innerReq)
	if err != nil {
		return nil, err
	}
	return &pb.AcquireInstanceRsp{
		InstanceKey:  innerRsp.InstanceKey,
		ProcessorID:  innerRsp.ProcessorID,
		ThreadNum:    innerRsp.ThreadNum,
		LeaseVersion: innerRsp.LeaseVersion,
		Error: &pb.Error{
			ErrorCode:    innerRsp.Error.ErrorCode,
			ErrorMessage: innerRsp.Error.ErrorMessage,
		},
	}, nil
}

// RetainInstance -
func (s *serverAdapter) RetainInstance(
	ctx context.Context, req *pb.RetainInstanceReq) (*pb.RetainInstanceRsp, error) {
	innerReq := &schedulerapi.RetainInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
	}
	innerRsp, err := s.service.RetainInstance(innerReq)
	if err != nil {
		return nil, err
	}
	return &pb.RetainInstanceRsp{
		Error: &pb.Error{
			ErrorCode:    innerRsp.Error.ErrorCode,
			ErrorMessage: innerRsp.Error.ErrorMessage,
		},
	}, nil
}

// ReleaseInstance -
func (s *serverAdapter) ReleaseInstance(
	c context.Context, req *pb.ReleaseInstanceReq) (*pb.ReleaseInstanceRsp, error) {
	innerReq := &schedulerapi.ReleaseInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
	}
	s.service.ReleaseInstance(innerReq)
	return &pb.ReleaseInstanceRsp{}, nil
}

// Heartbeat -
func (s *serverAdapter) Heartbeat(context.Context, *pb.HeartbeatReq) (*pb.HeartbeatRsp, error) {
	return &pb.HeartbeatRsp{}, nil
}
