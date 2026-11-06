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
	"errors"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
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

type mockListener struct{}

func (m *mockListener) Accept() (net.Conn, error) {
	return nil, fmt.Errorf("failed to accept")
}

func (m *mockListener) Close() error {
	return nil
}
func (m *mockListener) Addr() net.Addr {
	return nil
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

func (service *mockService) CheckInstance(req *schedulerapi.CheckInstanceReq) error {
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

func Test_serverStart(t *testing.T) {
	convey.Convey("Test_serverStart ok", t, func() {
		grpcServer := grpc.NewServer(grpc.Creds(getCredentials()))
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(net.Listen, func(network, address string) (net.Listener, error) {
				return &mockListener{}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(grpcServer), "Serve", func(s *grpc.Server, lis net.Listener) error {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(grpcServer), "GracefulStop", func(s *grpc.Server) {
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		service := &serverAdapter{service: testConfig.Service}
		pb.RegisterSchedulerRPCServer(grpcServer, service)
		svr := &Server{grpcServer: grpcServer, config: testConfig}
		err := svr.Start()
		convey.So(err, convey.ShouldBeNil)
		err = svr.Stop()
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test_serverStart listen err", t, func() {
		grpcServer := grpc.NewServer(grpc.Creds(getCredentials()))
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(net.Listen, func(network, address string) (net.Listener, error) {
				return nil, errors.New("listen err")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		service := &serverAdapter{service: testConfig.Service}
		pb.RegisterSchedulerRPCServer(grpcServer, service)
		svr := &Server{grpcServer: grpcServer, config: testConfig}
		err := svr.Start()
		convey.So(err.Error(), convey.ShouldEqual, "listen err")
	})
	convey.Convey("Test_serverStart server err", t, func() {
		grpcServer := grpc.NewServer(grpc.Creds(getCredentials()))
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(net.Listen, func(network, address string) (net.Listener, error) {
				return &mockListener{}, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(grpcServer), "Serve", func(s *grpc.Server, lis net.Listener) error {
				return errors.New("server err")
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		service := &serverAdapter{service: testConfig.Service}
		pb.RegisterSchedulerRPCServer(grpcServer, service)
		svr := &Server{grpcServer: grpcServer, config: testConfig}
		err := svr.Start()
		convey.So(err.Error(), convey.ShouldEqual, "server err")
	})
}
