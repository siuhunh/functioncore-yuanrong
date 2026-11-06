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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

const (
	funcKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	instanceID     = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	traceID        = "1234"

	timeout = 2 * time.Second
)

var (
	tcpDialer = fasthttp.TCPDialer{Concurrency: 100}
	client    = newClient()
	server    *Server

	testConfig = Config{
		IP:      "127.0.0.1",
		Port:    "8890",
		Service: &mockService{},
	}

	testAcquireInstanceReq = &schedulerapi.AcquireInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: funcKeyWithRes,
	}

	testAcquireInstanceRsp = &schedulerapi.AcquireInstanceRsp{
		InstanceKey:  instanceID,
		ProcessorID:  0,
		LeaseVersion: 1,
		ThreadNum:    1,
	}

	testReleaseInstanceReq = &schedulerapi.ReleaseInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: funcKeyWithRes,
		InstanceKey:    instanceID,
		ProcessorID:    0,
		LeaseVersion:   1,
	}

	testRetainInstanceReq = &schedulerapi.RetainInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: funcKeyWithRes,
		InstanceKey:    instanceID,
		ProcessorID:    1,
		LeaseVersion:   2,
	}
)

type mockService struct {
}

func (service *mockService) AcquireInstance(req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
	equal := reflect.DeepEqual(req, testAcquireInstanceReq)
	if !equal {
		return nil, errors.New("not valid req")
	}
	return testAcquireInstanceRsp, nil
}

func (service *mockService) ReleaseInstance(req *schedulerapi.ReleaseInstanceReq) error {
	equal := reflect.DeepEqual(req, testReleaseInstanceReq)
	if !equal {
		return errors.New("not valid req")
	}
	return nil
}

// CheckInstance -
func (service *mockService) CheckInstance(req *schedulerapi.CheckInstanceReq) error {
	return nil
}

func (service *mockService) RetainInstance(req *schedulerapi.RetainInstanceReq) (*schedulerapi.RetainInstanceRsp, error) {
	equal := reflect.DeepEqual(req, testRetainInstanceReq)
	if !equal {
		return nil, errors.New("not valid req")
	}
	return &schedulerapi.RetainInstanceRsp{}, nil
}

func newClient() *fasthttp.Client {
	return &fasthttp.Client{
		MaxIdleConnDuration: 10 * time.Second,
		MaxConnsPerHost:     100,
		ReadTimeout:         10 * time.Second,
		WriteTimeout:        10 * time.Second,
		ReadBufferSize:      6 * 1024,
		Dial: func(addr string) (net.Conn, error) {
			return tcpDialer.DialTimeout(addr, 10*time.Second)
		},
	}
}

func TestMain(m *testing.M) {
	config.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
		HTTPSEnable: false,
	}

	config.FConfig.LocalAuth = &localauth.AuthConfig{
		AKey:     "",
		SKey:     "",
		Duration: 0,
	}

	errorChan := make(chan error, 1)
	server, _ = NewServer(testConfig)
	server.Start(errorChan)
	time.Sleep(1 * time.Second)

	m.Run()
	server.Stop()
}

func setCommonHeader(req *fasthttp.Request) {
	req.Header.SetMethod(http.MethodPost)
	req.Header.ResetConnectionClose()
	req.Header.SetHost(fmt.Sprintf("%s:%s", testConfig.IP, testConfig.Port))
	req.URI().SetScheme("http")
}

func TestAcquireInstance(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.AcquireInstanceURI)
		protoReq := &pb.AcquireInstanceReq{
			TraceID:        testAcquireInstanceReq.TraceID,
			FuncKeyWithRes: testAcquireInstanceReq.FuncKeyWithRes,
		}
		body, _ := proto.Marshal(protoReq)
		req.SetBody(body)

		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusOK)
	})
}

func TestAcquireInstanceWithInvalidReq(t *testing.T) {
	convey.Convey("TestAcquireInstanceWithInvalidReq", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.AcquireInstanceURI)
		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusInternalServerError)
	})
}

func TestAcquireInstanceWithInvalidPath(t *testing.T) {
	convey.Convey("TestAcquireInstanceWithInvalidPath", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI("/test")
		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusInternalServerError)
	})
}

func TestCheckInstance(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.CheckInstanceURI)
		checkInstanceReq := &schedulerapi.CheckInstanceReq{
			FuncKeyWithRes: testAcquireInstanceReq.FuncKeyWithRes,
		}
		body, _ := json.Marshal(checkInstanceReq)
		req.SetBody(body)

		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusOK)
	})
}

func TestCheckInstanceWithInvalidReq(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.CheckInstanceURI)
		body, _ := json.Marshal("hello world")
		req.SetBody(body)

		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusInternalServerError)
	})
}

func TestReleaseInstance(t *testing.T) {
	convey.Convey("TestReleaseInstance", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.ReleaseInstanceURI)
		protoReq := &pb.ReleaseInstanceReq{
			TraceID:        testReleaseInstanceReq.TraceID,
			FuncKeyWithRes: testAcquireInstanceReq.FuncKeyWithRes,
			InstanceKey:    testReleaseInstanceReq.InstanceKey,
			ProcessorID:    testReleaseInstanceReq.ProcessorID,
			LeaseVersion:   testReleaseInstanceReq.LeaseVersion,
		}
		body, _ := proto.Marshal(protoReq)
		req.SetBody(body)

		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusOK)
	})
}

func TestReleaseInstanceWithInvalidReq(t *testing.T) {
	convey.Convey("TestReleaseInstanceWithInvalidReq", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.ReleaseInstanceURI)
		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusInternalServerError)
	})
}

func TestRetainInstance(t *testing.T) {
	convey.Convey("TestRetainInstance", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.RetainInstanceURI)
		protoReq := &pb.RetainInstanceReq{
			TraceID:        testRetainInstanceReq.TraceID,
			FuncKeyWithRes: testRetainInstanceReq.FuncKeyWithRes,
			InstanceKey:    testRetainInstanceReq.InstanceKey,
			ProcessorID:    testRetainInstanceReq.ProcessorID,
			LeaseVersion:   testRetainInstanceReq.LeaseVersion,
		}
		body, _ := proto.Marshal(protoReq)
		req.SetBody(body)

		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusOK)
	})
}

func TestRetainInstanceWithInvalidReq(t *testing.T) {
	convey.Convey("TestRetainInstanceWithInvalidReq", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.RetainInstanceURI)
		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusInternalServerError)
	})
}

func TestHeartbeat(t *testing.T) {
	convey.Convey("TestHeartbeat", t, func() {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		req.SetRequestURI(schedulerapi.HeartbeatURI)
		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusOK)
	})
}

func TestServerStart(t *testing.T) {
	convey.Convey("TestServerStart", t, func() {
		config.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
		}
		cfg := Config{
			IP:      "1.0.0.2",
			Port:    "8891",
			Service: &mockService{},
		}
		s, _ := NewServer(cfg)

		err := s.Start(nil)
		convey.So(err, convey.ShouldNotBeNil)
		errorChan := make(chan error, 1)
		s.Start(errorChan)
		err = <-errorChan
		convey.So(err, convey.ShouldNotBeNil)
		s.Stop()
	})
}

func TestAuth(t *testing.T) {
	convey.Convey("TestAuth", t, func() {
		config.FConfig.AuthenticationEnable = true

		patched := []*gomonkey.Patches{
			gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string,
				requestSign string, timestamp string, duration int) error {
				if timestamp == "" {
					return errors.New("no auth check info")
				}
				return nil
			}),
		}
		defer func() {
			config.FConfig.AuthenticationEnable = false
			for i := range patched {
				patched[i].Reset()
			}
		}()

		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)

		setCommonHeader(req)
		err := client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusUnauthorized)

		req.Header.Set(constants.HeaderAuthTimestamp, "123")
		err = client.DoTimeout(req, resp, timeout)
		convey.So(err, convey.ShouldBeNil)
		convey.So(resp.StatusCode(), convey.ShouldEqual, http.StatusInternalServerError)
	})
}
