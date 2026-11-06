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

package remotescheduler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/proto"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/grpc/pb"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/functionbus/common"
	testutils "functioncore/test/utils"
)

const (
	funcKeyWithRes = "7e186a/0@base@testresourcepython36768/latest/300-128"
	instanceID     = "defaultaz-#-pool22-300-128-fusion-6465c665dc-x2xr9"
	traceID        = "1234"
)

// Server -
type Server struct {
	fastServer *fasthttp.Server
	config     Config
}

// Config network server config
type Config struct {
	Service schedulerapi.Scheduler
	IP      string
	Port    string
}

type serverAdapter struct {
	service schedulerapi.Scheduler
}

var (
	testServerConfig = Config{
		IP:      "127.0.0.1",
		Port:    "8888",
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
		ProcessorID:    1,
		LeaseVersion:   2,
	}

	testRetainInstanceReq = &schedulerapi.RetainInstanceReq{
		TraceID:        traceID,
		FuncKeyWithRes: funcKeyWithRes,
		InstanceKey:    instanceID,
		ProcessorID:    1,
		LeaseVersion:   2,
	}

	testRetainInstanceRsp = &schedulerapi.RetainInstanceRsp{
		Error: schedulerapi.Error{
			ErrorCode: 1,
		},
	}
)

func TestMain(m *testing.M) {
	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "", ""
		}),
	}
	defer func() {
		for i := range patched {
			patched[i].Reset()
		}
	}()

	common.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
		HTTPSEnable: true,
		TLSProtocol: "tls",
		TLSCiphers:  "ciphers",
	}
	common.FConfig.LocalAuth = &localauth.AuthConfig{}
	errorChan := make(chan error, 1)
	adapter := &serverAdapter{
		service: testServerConfig.Service,
	}
	fastServer := &fasthttp.Server{
		Handler:            adapter.route,
		ReadBufferSize:     1024,
		ReadTimeout:        5 * time.Second,
		WriteTimeout:       5 * time.Second,
		MaxRequestBodySize: 1024 * 1024,
	}
	server := &Server{
		fastServer: fastServer,
		config:     testServerConfig,
	}
	server.Start(errorChan)
	time.Sleep(1 * time.Second)

	m.Run()
	server.Stop()
}

func (adapter *serverAdapter) route(ctx *fasthttp.RequestCtx) {
	path := string(ctx.Path())
	switch path {
	case schedulerapi.AcquireInstanceURI:
		body := ctx.Request.Body()
		req := &pb.AcquireInstanceReq{}
		err := proto.Unmarshal(body, req)
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		rsp, err := adapter.service.AcquireInstance(&schedulerapi.AcquireInstanceReq{
			TraceID:        req.TraceID,
			FuncKeyWithRes: req.FuncKeyWithRes,
		})
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		protoRsp := &pb.AcquireInstanceRsp{
			InstanceKey:  rsp.InstanceKey,
			ProcessorID:  rsp.ProcessorID,
			LeaseVersion: rsp.LeaseVersion,
			ThreadNum:    rsp.ThreadNum,
			Error: &pb.Error{
				ErrorCode:    rsp.Error.ErrorCode,
				ErrorMessage: rsp.Error.ErrorMessage,
			},
		}
		result, err := proto.Marshal(protoRsp)
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		ctx.Response.SetBody(result)
		ctx.SetStatusCode(http.StatusOK)
	case schedulerapi.ReleaseInstanceURI:
		body := ctx.Request.Body()
		req := &pb.ReleaseInstanceReq{}
		err := proto.Unmarshal(body, req)
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		err = adapter.service.ReleaseInstance(&schedulerapi.ReleaseInstanceReq{
			TraceID:        req.TraceID,
			FuncKeyWithRes: req.FuncKeyWithRes,
			InstanceKey:    req.InstanceKey,
			ProcessorID:    req.ProcessorID,
			LeaseVersion:   req.LeaseVersion,
			IsWebSocketReq: req.IsWebSocketReq,
		})
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		ctx.SetStatusCode(http.StatusOK)
	case schedulerapi.RetainInstanceURI:
		body := ctx.Request.Body()
		req := &pb.RetainInstanceReq{}
		err := proto.Unmarshal(body, req)
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		rsp, err := adapter.service.RetainInstance(&schedulerapi.RetainInstanceReq{
			TraceID:        req.TraceID,
			FuncKeyWithRes: req.FuncKeyWithRes,
			InstanceKey:    req.InstanceKey,
			ProcessorID:    req.ProcessorID,
			LeaseVersion:   req.LeaseVersion,
		})
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		protoRsp := &pb.RetainInstanceRsp{
			Error: &pb.Error{
				ErrorCode:    rsp.Error.ErrorCode,
				ErrorMessage: rsp.Error.ErrorMessage,
			},
		}
		result, err := proto.Marshal(protoRsp)
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		ctx.Response.SetBody(result)
		ctx.SetStatusCode(http.StatusOK)
	case schedulerapi.CheckInstanceURI:
		body := ctx.Request.Body()
		req := &schedulerapi.CheckInstanceReq{}
		err := json.Unmarshal(body, req)
		if err != nil {
			ctx.SetStatusCode(http.StatusBadRequest)
			return
		}
		adapter.service.CheckInstance(req)
		ctx.SetStatusCode(http.StatusOK)
	case schedulerapi.HeartbeatURI:
		ctx.SetStatusCode(http.StatusOK)
	default:
		ctx.SetStatusCode(http.StatusInternalServerError)
	}
	return
}

type mockService struct {
}

func (service *mockService) AcquireInstance(
	req *schedulerapi.AcquireInstanceReq) (*schedulerapi.AcquireInstanceRsp, error) {
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
	return testRetainInstanceRsp, nil
}

func TestAcquireInstance(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		client := createClient("faas-scheduler-001", "127.0.0.1", "8888")
		client.start()
		convey.So(client.isEnable(), convey.ShouldEqual, true)

		rsp, err := client.AcquireInstance(testAcquireInstanceReq)
		convey.So(err, convey.ShouldBeNil)
		client.CheckInstance(&schedulerapi.CheckInstanceReq{})
		convey.So(reflect.DeepEqual(rsp, testAcquireInstanceRsp), convey.ShouldBeTrue)
	})
}

func TestReleaseInstance(t *testing.T) {
	convey.Convey("TestReleaseInstance", t, func() {
		client := createClient("faas-scheduler-001", "127.0.0.1", "8888")
		client.start()
		convey.So(client.isEnable(), convey.ShouldEqual, true)

		err := client.ReleaseInstance(testReleaseInstanceReq)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestRetainInstance(t *testing.T) {
	convey.Convey("TestAcquireInstance", t, func() {
		client := createClient("faas-scheduler-001", "127.0.0.1", "8888")
		client.start()
		convey.So(client.isEnable(), convey.ShouldEqual, true)

		rsp, err := client.RetainInstance(testRetainInstanceReq)
		convey.So(err, convey.ShouldBeNil)

		convey.So(reflect.DeepEqual(rsp, testRetainInstanceRsp), convey.ShouldBeTrue)
	})
}

func TestHeartBeat(t *testing.T) {
	convey.Convey("TestHeartBeat", t, func() {
		client := createClient("faas-scheduler-001", "127.0.0.1", "8888")
		client.start()
		defer client.stop()

		time.Sleep(500 * time.Millisecond)
		convey.So(client.isEnable(), convey.ShouldEqual, true)
	})
}

func Test_circleHeartbeat_reportAlarm(t *testing.T) {
	ch := make(chan time.Time)
	alarmCount := int64(0)
	patches := []*gomonkey.Patches{
		gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
			return &time.Ticker{C: ch}
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&fasthttp.Client{}), "DoTimeout", func(c *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
			return errors.New("do request failed")
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(alarm.GetClient()), "ReportAlarm", func(_ *alarm.Client, _ alarm.Type, _ string) {
			atomic.AddInt64(&alarmCount, 1)
		}),
	}
	defer func() {
		for i := range patches {
			patches[i].Reset()
		}
	}()

	client := &clientAdapter{
		conn:       nil,
		config:     requestConfig{},
		stopCh:     make(chan struct{}),
		enable:     true,
		serverAddr: "",
	}
	defer func() {
		client.stopCh <- struct{}{}
	}()
	go client.circleHeartbeat()

	for i := 0; i < alarmThreshold-1; i++ {
		ch <- time.Time{}
		assert.Equal(t, int64(0), atomic.LoadInt64(&alarmCount), i)
	}
	ch <- time.Time{}
	ch <- time.Time{}
	assert.Equal(t, int64(1), atomic.LoadInt64(&alarmCount))
}

func Test_clientAdapter_decodeAcquireInstanceRsp(t *testing.T) {
	tests := []struct {
		name        string
		want        *schedulerapi.AcquireInstanceRsp
		arg         *pb.AcquireInstanceRsp
		wantErr     error
		patchesFunc testutils.PatchesFunc
	}{
		{
			name:    "case 1 normal case",
			wantErr: nil,
			want: &schedulerapi.AcquireInstanceRsp{
				InstanceKey: "mockKey",
				Error: schedulerapi.Error{
					ErrorCode:    200,
					ErrorMessage: "200",
				},
			},
			arg: &pb.AcquireInstanceRsp{
				InstanceKey: "mockKey",
				Error: &pb.Error{
					ErrorCode:    200,
					ErrorMessage: "200",
				},
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				return patches
			},
		}, {
			name:    "case 2 protobuf has nil content",
			wantErr: nil,
			want: &schedulerapi.AcquireInstanceRsp{
				InstanceKey: "mockKey",
			},
			arg: &pb.AcquireInstanceRsp{
				InstanceKey: "mockKey",
				Error:       nil,
			},
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				return patches
			},
		}, {
			name:    "case 3 proto returned error",
			wantErr: errors.New("mock proto error"),
			want:    nil,
			arg:     nil,
			patchesFunc: func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				gomonkey.ApplyFunc(proto.Unmarshal, func(b []byte, m proto.Message) error {
					return errors.New("mock proto error")
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c := &clientAdapter{}
			tmp, _ := proto.Marshal(tt.arg)
			ctx := &fasthttp.Response{}
			ctx.SetBody(tmp)
			got, err := c.decodeAcquireRsp(ctx)
			if !errors.Is(err, tt.wantErr) && tt.wantErr.Error() != err.Error() {
				t.Errorf("decodeAcquireInstanceRsp() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("decodeAcquireInstanceRsp() actual = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

// Start -
func (s *Server) Start(errChan chan<- error) error {
	if errChan == nil {
		return errors.New("errChan is nil")
	}
	go func() {
		err := s.start()
		if err != nil {
			log.GetLogger().Errorf("failed to start server, err %s", err.Error())
		}
		errChan <- err
	}()
	return nil
}

// Stop -
func (s *Server) Stop() {
	err := s.fastServer.Shutdown()
	if err != nil {
		log.GetLogger().Errorf("failed to shutdown http server")
		return
	}
	log.GetLogger().Infof("success to shutdown http server")
}

func (s *Server) start() error {
	addr := fmt.Sprintf("%s:%s", s.config.IP, s.config.Port)
	log.GetLogger().Infof("start to listen the http request on addr: %s", addr)
	err := s.fastServer.ListenAndServe(addr)
	if err != nil {
		log.GetLogger().Errorf("failed to listen the http request on addr %s: %s", addr, err.Error())
		return err
	}
	return nil
}
