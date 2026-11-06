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

// Package fasthttpserver contains fast http server logic
package fasthttpserver

import (
	"net/http"

	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/faas-scheduler/config"
)

type serverAdapter struct {
	service schedulerapi.Scheduler
}

func createServerAdapter(service schedulerapi.Scheduler) *serverAdapter {
	return &serverAdapter{
		service: service,
	}
}

func (adapter *serverAdapter) route(ctx *fasthttp.RequestCtx) {
	err := adapter.auth(ctx)
	if err != nil {
		ctx.SetStatusCode(http.StatusUnauthorized)
		log.GetLogger().Errorf("failed to check auth, error: %s", err.Error())
		return
	}

	path := string(ctx.Path())
	switch path {
	case schedulerapi.AcquireInstanceURI:
		adapter.acquireInstance(ctx)
	case schedulerapi.ReleaseInstanceURI:
		adapter.releaseInstance(ctx)
	case schedulerapi.RetainInstanceURI:
		adapter.retainInstance(ctx)
	case schedulerapi.HeartbeatURI:
		adapter.heartbeat(ctx)
	default:
		ctx.SetStatusCode(http.StatusInternalServerError)
		log.GetLogger().Errorf("unsupported http request path %s", path)
	}
	return
}

func (adapter *serverAdapter) auth(ctx *fasthttp.RequestCtx) error {
	if config.FConfig.AuthenticationEnable == false {
		return nil
	}

	sign := string(ctx.Request.Header.Peek(constants.HeaderAuthorization))
	timestamp := string(ctx.Request.Header.Peek(constants.HeaderAuthTimestamp))
	err := localauth.AuthCheckLocally(config.FConfig.LocalAuth.AKey, config.FConfig.LocalAuth.SKey, sign,
		timestamp, config.FConfig.LocalAuth.Duration)
	if err != nil {
		return err
	}
	return nil
}

func (adapter *serverAdapter) acquireInstance(ctx *fasthttp.RequestCtx) {
	err := adapter.acquireInstanceInner(ctx)
	if err != nil {
		ctx.SetStatusCode(http.StatusInternalServerError)
		log.GetLogger().Errorf("failed to acquire instance, err %s", err.Error())
		return
	}
	ctx.SetStatusCode(http.StatusOK)
}

func (adapter *serverAdapter) releaseInstance(ctx *fasthttp.RequestCtx) {
	err := adapter.releaseInstanceInner(ctx)
	if err != nil {
		ctx.SetStatusCode(http.StatusInternalServerError)
		ctx.SetBodyString(err.Error())
		log.GetLogger().Warnf("failed to release instance, err %s", err.Error())
		return
	}
	ctx.SetStatusCode(http.StatusOK)
}

func (adapter *serverAdapter) retainInstance(ctx *fasthttp.RequestCtx) {
	err := adapter.retainInstanceInner(ctx)
	if err != nil {
		ctx.SetStatusCode(http.StatusInternalServerError)
		log.GetLogger().Errorf("failed to retain instance, err %s", err.Error())
		return
	}
	ctx.SetStatusCode(http.StatusOK)
}

func (adapter *serverAdapter) heartbeat(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(http.StatusOK)
}

func (adapter *serverAdapter) retainInstanceInner(ctx *fasthttp.RequestCtx) error {
	req, err := adapter.decodeRetainInstanceReq(ctx)
	if err != nil {
		return err
	}

	rsp, err := adapter.service.RetainInstance(req)
	if err != nil {
		return err
	}

	err = adapter.encodeRetainInstanceRsp(rsp, ctx)
	if err != nil {
		return err
	}
	return nil
}

func (adapter *serverAdapter) releaseInstanceInner(ctx *fasthttp.RequestCtx) error {
	req, err := adapter.decodeReleaseInstanceReq(ctx)
	if err != nil {
		return err
	}

	err = adapter.service.ReleaseInstance(req)
	if err != nil {
		return err
	}

	return nil
}

func (adapter *serverAdapter) acquireInstanceInner(ctx *fasthttp.RequestCtx) error {
	req, err := adapter.decodeAcquireInstanceReq(ctx)
	if err != nil {
		return err
	}

	rsp, err := adapter.service.AcquireInstance(req)
	if err != nil {
		return err
	}

	err = adapter.encodeAcquireInstanceRsp(rsp, ctx)
	if err != nil {
		return err
	}

	return nil
}

func (adapter *serverAdapter) decodeAcquireInstanceReq(
	ctx *fasthttp.RequestCtx) (*schedulerapi.AcquireInstanceReq, error) {
	body := ctx.Request.Body()
	req := &pb.AcquireInstanceReq{}
	err := proto.Unmarshal(body, req)
	if err != nil {
		return nil, err
	}
	if req.ReqType > schedulerapi.NormalRequest {
		return &schedulerapi.AcquireInstanceReq{
			TraceID:        req.TraceID,
			FuncKeyWithRes: req.FuncKeyWithRes,
			ReqType:        req.ReqType,
			WebSocketInsAcquireConfig: schedulerapi.WebSocketInsAcquireConfig{
				TargetIP:    req.WebSocketInsAcquireConfig.TargetIP,
				TargetPort:  req.WebSocketInsAcquireConfig.TargetPort,
				Path:        req.WebSocketInsAcquireConfig.Path,
				Concurrency: req.WebSocketInsAcquireConfig.Concurrency,
			},
			Timeout: req.Timeout,
		}, nil
	}
	return &schedulerapi.AcquireInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
	}, nil
}

func (adapter *serverAdapter) encodeAcquireInstanceRsp(
	rsp *schedulerapi.AcquireInstanceRsp, ctx *fasthttp.RequestCtx) error {
	protoRsp := &pb.AcquireInstanceRsp{
		InstanceKey:  rsp.InstanceKey,
		ProcessorID:  rsp.ProcessorID,
		ThreadNum:    rsp.ThreadNum,
		LeaseVersion: rsp.LeaseVersion,
		InstanceIP:   rsp.InstanceIP,
		InstancePort: rsp.InstancePort,
		NodeIP:       rsp.NodeIP,
		NodePort:     rsp.NodePort,
		Error: &pb.Error{
			ErrorCode:    rsp.Error.ErrorCode,
			ErrorMessage: rsp.Error.ErrorMessage,
		},
	}
	body, err := proto.Marshal(protoRsp)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal AcquireInstanceRsp")
		return err
	}
	ctx.Response.SetBody(body)
	return nil
}

func (adapter *serverAdapter) decodeReleaseInstanceReq(
	ctx *fasthttp.RequestCtx) (*schedulerapi.ReleaseInstanceReq, error) {
	body := ctx.Request.Body()
	req := &pb.ReleaseInstanceReq{}
	err := proto.Unmarshal(body, req)
	if err != nil {
		return nil, err
	}

	return &schedulerapi.ReleaseInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
		IsWebSocketReq: req.IsWebSocketReq,
	}, nil
}

func (adapter *serverAdapter) decodeRetainInstanceReq(
	ctx *fasthttp.RequestCtx) (*schedulerapi.RetainInstanceReq, error) {
	body := ctx.Request.Body()
	req := &pb.RetainInstanceReq{}
	err := proto.Unmarshal(body, req)
	if err != nil {
		return nil, err
	}

	return &schedulerapi.RetainInstanceReq{
		TraceID:        req.TraceID,
		FuncKeyWithRes: req.FuncKeyWithRes,
		InstanceKey:    req.InstanceKey,
		ProcessorID:    req.ProcessorID,
		LeaseVersion:   req.LeaseVersion,
	}, nil
}

func (adapter *serverAdapter) encodeRetainInstanceRsp(
	rsp *schedulerapi.RetainInstanceRsp, ctx *fasthttp.RequestCtx) error {
	protoRsp := &pb.RetainInstanceRsp{
		Error: &pb.Error{
			ErrorCode:    rsp.Error.ErrorCode,
			ErrorMessage: rsp.Error.ErrorMessage,
		},
	}

	body, err := proto.Marshal(protoRsp)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal RetainInstanceRsp")
		return err
	}
	ctx.Response.SetBody(body)
	return nil
}
