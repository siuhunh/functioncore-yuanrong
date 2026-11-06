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

package grpcconn

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
)

const (
	testFuncKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	testInstanceKey    = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	traceID            = "1234"
	ip                 = "127.0.0.1"
	port               = "8889"
)

var (
	server *grpc.Server
	config = common.RPCClientConfig{
		IP:             "127.0.0.1",
		Port:           "8889",
		RequestTimeout: 500 * time.Millisecond,
		ServerKey:      "serverKey",
	}
	testAcquireInstanceReq = &schedulerapi.AcquireInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: testFuncKeyWithRes,
	}
	testReleaseInstanceReq = &schedulerapi.ReleaseInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: testFuncKeyWithRes,
		InstanceKey:    testInstanceKey,
		ProcessorID:    0,
		LeaseVersion:   1,
	}
	testRetainInstanceReq = &schedulerapi.RetainInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: testFuncKeyWithRes,
		InstanceKey:    testInstanceKey,
		ProcessorID:    0,
		LeaseVersion:   1,
	}
)

type mockServer struct {
	pb.UnimplementedSchedulerRPCServer
	service schedulerapi.Scheduler
}

func (s *mockServer) AcquireInstance(
	c context.Context, req *pb.AcquireInstanceReq) (*pb.AcquireInstanceRsp, error) {
	return &pb.AcquireInstanceRsp{
		InstanceKey:  testInstanceKey,
		ProcessorID:  0,
		ThreadNum:    1,
		LeaseVersion: 1,
		Error: &pb.Error{
			ErrorCode: 0,
		},
	}, nil
}

func (s *mockServer) RetainInstance(
	ctx context.Context, req *pb.RetainInstanceReq) (*pb.RetainInstanceRsp, error) {
	return &pb.RetainInstanceRsp{
		Error: &pb.Error{
			ErrorCode: 0,
		},
	}, nil
}

func (s *mockServer) ReleaseInstance(
	c context.Context, req *pb.ReleaseInstanceReq) (*pb.ReleaseInstanceRsp, error) {
	return &pb.ReleaseInstanceRsp{}, nil
}

func (s *mockServer) Heartbeat(context.Context, *pb.HeartbeatReq) (*pb.HeartbeatRsp, error) {
	return &pb.HeartbeatRsp{}, nil
}

func startServer() error {
	server = grpc.NewServer()
	pb.RegisterSchedulerRPCServer(server, &mockServer{})

	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%s", ip, port))
	if err != nil {
		log.GetLogger().Errorf("failed to listen tcp addr:%s:%s, err:%s", ip, port, err.Error())
		return err
	}
	if err = server.Serve(lis); err != nil {
		log.GetLogger().Errorf("failed to start grpc Serve addr:%s:%s, err:%s",
			ip, port, err.Error())
		return err
	}
	return nil
}

func TestMain(m *testing.M) {
	constant.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
		HTTPSEnable: false,
	}
	go startServer()
	time.Sleep(1000 * time.Millisecond)

	m.Run()
	server.Stop()
}

func TestAcquireInstance(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		client, err := CreateGrpcClient(config)
		convey.So(err, convey.ShouldBeNil)
		client.Start()
		convey.So(client.IsEnable(), convey.ShouldEqual, true)

		rsp, err := client.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, testInstanceKey)
		client.Stop()

		rsp, err = client.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestReleaseInstance(t *testing.T) {
	convey.Convey("TestReleaseInstance", t, func() {
		client, err := CreateGrpcClient(config)
		convey.So(err, convey.ShouldBeNil)
		client.Start()
		defer client.Stop()
		err = client.ReleaseInstance(testReleaseInstanceReq)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestRetainInstance(t *testing.T) {
	convey.Convey("TestRetainInstance", t, func() {
		client, err := CreateGrpcClient(config)
		convey.So(err, convey.ShouldBeNil)
		client.Start()
		defer client.Stop()
		rsp, err := client.RetainInstance(testRetainInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, 0)
	})
}

func TestHeartBeat(t *testing.T) {
	convey.Convey("TestHeartBeat", t, func() {
		heartbeatInterval = 50 * time.Millisecond
		heartbeatTimeout = 50 * time.Millisecond
		client, err := CreateGrpcClient(config)
		convey.So(err, convey.ShouldBeNil)
		client.Start()
		defer client.Stop()

		time.Sleep(500 * time.Millisecond)
		convey.So(client.IsEnable(), convey.ShouldEqual, true)
	})
}

func TestServerKey(t *testing.T) {
	convey.Convey("TestServerKey", t, func() {
		client, _ := CreateGrpcClient(config)
		serverKey := client.ServerKey()
		convey.So(serverKey, convey.ShouldEqual, "serverKey")
	})
}
