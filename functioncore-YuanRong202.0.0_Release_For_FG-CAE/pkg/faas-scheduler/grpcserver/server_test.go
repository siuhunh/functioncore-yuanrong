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

package grpcserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

const (
	funcKey        = "7e186a/0@base@testresourcepython36768/latest"
	funcKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	instanceID     = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	traceID        = "1234"
)

var (
	testConfig = Config{
		IP:      "127.0.0.1",
		Port:    "8889",
		Service: &mockService{},
	}
	server *Server
	client pb.SchedulerRPCClient
)

type mockService struct {
}

func (service *mockService) AcquireInstance(req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	return &schedulerapi.AcquireInstanceRsp{
		InstanceKey: instanceID,
		ProcessorID: 0,
	}, nil
}

func (service *mockService) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	return nil
}

func (service *mockService) RetainInstance(req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	return &schedulerapi.RetainInstanceRsp{}, nil
}

func startServer() {
	config.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
		HTTPSEnable: false,
	}
	server, _ = NewServer(testConfig)
	go server.Start()
}

func startClient() {
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", testConfig.IP, testConfig.Port), grpc.WithInsecure())
	if err != nil {
		log.GetLogger().Errorf("grpc Dial failed, err %s", err)
		return
	}
	client = pb.NewSchedulerRPCClient(conn)
}

func TestMain(m *testing.M) {
	startServer()
	startClient()
	time.Sleep(3000 * time.Millisecond)

	m.Run()
	server.Stop()
}

func TestServerReleaseInstance(t *testing.T) {
	convey.Convey("TestServerReleaseInstance", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		req := &pb.ReleaseInstanceReq{
			TraceID:        traceID,
			FuncKeyWithRes: funcKeyWithRes,
			InstanceKey:    instanceID,
			ProcessorID:    0,
		}
		rsp, err := client.ReleaseInstance(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp, convey.ShouldNotBeNil)
	})
}

func TestServerAcquireInstance(t *testing.T) {
	convey.Convey("TestServerAcquireInstance", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		req := &pb.AcquireInstanceReq{
			TraceID:        traceID,
			FuncKeyWithRes: funcKeyWithRes,
		}
		rsp, err := client.AcquireInstance(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.InstanceKey, convey.ShouldEqual, instanceID)
	})
}

func TestServerRetainInstance(t *testing.T) {
	convey.Convey("TestServerRetainInstance", t, func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		req := &pb.RetainInstanceReq{
			TraceID:        traceID,
			FuncKeyWithRes: funcKeyWithRes,
		}
		rsp, err := client.RetainInstance(ctx, req)
		convey.So(err, convey.ShouldBeNil)
		convey.So(rsp.Error.ErrorCode, convey.ShouldEqual, 0)
	})
}
