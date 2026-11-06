/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
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

package frontend

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/signals"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/stream"
	"functioncore/pkg/frontend-invoke/util"
	testutils "functioncore/test/utils"
)

func startHttpServerPatch() *gomonkey.Patches {
	patch := gomonkey.ApplyFunc(startWatch, func(stopCh <-chan struct{}) {
	})
	gomonkey.ApplyFunc(alarm.NewClient, func(serverAddress string, serviceName string,
		stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
		return nil
	})
	patch.ApplyFunc(registerInstance, func(stopCh <-chan struct{}) {})
	patch.ApplyFunc(util.InitETCDClient, func(stopCh <-chan struct{}) error {
		return nil
	})
	patch.ApplyFunc(stream.InitDiscovery, func(stopCh <-chan struct{}) error {
		return nil
	})

	patch.ApplyMethod(reflect.TypeOf(&alarm.Client{}), "RegisterAlarmType",
		func(client *alarm.Client, almType alarm.Type) {
		})

	return patch
}

func TestServerSupportH2(t *testing.T) {
	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{
		MaxRequestBodySize: 100,
	}
	constant.FConfig.HTTPSConfig = &commontls.InternalHTTPSConfig{
		HTTPSEnable: true,
	}
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	urnutils.Separator = "@"
	constant.NativeAz = constants.DefaultAZ
	constant.FConfig.InvokeMaxRetryTimes = 3
	constant.FConfig.Runtime = &constant.RuntimeConfig{Port: "8892"}

	os.Setenv("POD_IP", "127.0.0.1")
	stopCh := make(chan struct{})
	defer func() {
		close(stopCh)
	}()

	suits := make([]uint16, 0)
	for _, v := range commontls.TLS_CIPHER_SUITE_MAP {
		suits = append(suits, v)
	}

	certs := testutils.GenerateTestCert()

	patch := startHttpServerPatch()
	patch.ApplyFunc(alarm.WithTLSEnable, func(_ *tls.Config) alarm.ClientOption {
		return nil
	})
	patch.ApplyFunc(commontls.GetClientTLSConfig, func() *tls.Config {
		return &tls.Config{
			Certificates:       certs,
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			MaxVersion:         tls.VersionTLS12,
		}
	})
	patch.ApplyFunc(alarm.WithAuth, func(_, _ string) alarm.ClientOption {
		return nil
	})
	defer patch.Reset()

	go func() {
		err := StartHTTPServer(stopCh)
		fmt.Println(err)
	}()

	time.Sleep(100 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://127.0.0.1:8892/healthz", nil)
	cli := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
			ForceAttemptHTTP2: true,
		},
	}
	resp, err := cli.Do(req)
	assert.Nil(t, err)
	assert.Equal(t, "HTTP/2.0", resp.Proto)
}

func TestStartHttpServer(t *testing.T) {
	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{
		MaxRequestBodySize: 100,
	}
	constant.FConfig.HTTPSConfig = &commontls.InternalHTTPSConfig{}
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	urnutils.Separator = "@"
	constant.NativeAz = constants.DefaultAZ
	constant.FConfig.InvokeMaxRetryTimes = 3
	constant.FConfig.Runtime = &constant.RuntimeConfig{Port: "8888"}

	stopCh := signals.WaitForSignal()

	os.Setenv("POD_IP", "127.0.0.1")
	convey.Convey("Test StartHTTPServer", t, func() {
		convey.Convey("succeed to start http server", func() {
			patch := startHttpServerPatch()
			patch.ApplyFunc(alarm.NewClient,
				func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
					return nil
				})
			patch.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServe",
				func(_ *http.Server) error {
					return nil
				})
			defer patch.Reset()
			err := StartHTTPServer(stopCh)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("failed to start http server", func() {
			patch := startHttpServerPatch()
			patch.ApplyFunc(alarm.NewClient,
				func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
					return errors.New("test")
				})
			patch.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServe",
				func(_ *http.Server) error {
					return errors.New("failed to start http server")
				})
			defer patch.Reset()
			err := StartHTTPServer(stopCh)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("succeed to start tls server", func() {
			patch := startHttpServerPatch()
			patch.ApplyFunc(alarm.NewClient,
				func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
					return nil
				})
			constant.FConfig.HTTPSConfig.HTTPSEnable = true
			patch.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServeTLS",
				func(_ *http.Server, certFile, keyFile string) error {
					return nil
				})
			defer patch.Reset()
			err := StartHTTPServer(stopCh)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("failed to start tls server", func() {
			patch := startHttpServerPatch()
			patch.ApplyFunc(alarm.NewClient,
				func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
					return nil
				})
			constant.FConfig.HTTPSConfig.HTTPSEnable = true
			patch.ApplyMethod(reflect.TypeOf(&http.Server{}), "ListenAndServeTLS",
				func(_ *http.Server, certFile, keyFile string) error {
					return errors.New("failed to start tls server")
				})
			defer patch.Reset()
			err := StartHTTPServer(stopCh)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("stop channel nil", func() {
			patch := startHttpServerPatch()
			defer patch.Reset()
			err := StartHTTPServer(nil)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("failed to get pod ip", func() {
			os.Setenv("POD_IP", "11223344")
			patch := startHttpServerPatch()
			patch.ApplyFunc(alarm.NewClient,
				func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
					return nil
				})
			defer patch.Reset()
			err := StartHTTPServer(stopCh)
			convey.So(err.Error(), convey.ShouldEqual, "failed to get pod ip")
		})
		convey.Convey("failed to init alarm client", func() {
			patch := startHttpServerPatch()
			patch.ApplyFunc(alarm.NewClient,
				func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
					return errors.New("test")
				})
			defer patch.Reset()
			err := StartHTTPServer(stopCh)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func Test_healthz(t *testing.T) {
	registerOnce = sync.Once{}
	count := int64(0)
	defer gomonkey.ApplyFunc(registerInstance, func(stopCh <-chan struct{}) {
		atomic.AddInt64(&count, 1)
	}).Reset()
	router := gin.New()
	router.GET("/healthz", func(context *gin.Context) {
		healthz(context.Writer, context.Request)
	})
	assert.Equal(t, int64(0), atomic.LoadInt64(&count))
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest(http.MethodGet, "/healthz", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		// check response status code
		assert.Equal(t, http.StatusOK, w.Result().StatusCode, "healthz response 200")
		assert.Equal(t, int64(1), atomic.LoadInt64(&count))
	}
}

func Test_waitShutdown(t *testing.T) {
	convey.Convey("Test waitShutdown", t, func() {
		convey.Convey("stop chan is nil", func() {
			err := waitShutdown(&http.Server{}, nil, make(chan error))
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("error chan is nil", func() {
			err := waitShutdown(&http.Server{}, make(chan struct{}), nil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("stop chan is closed", func() {
			ch := make(chan struct{})
			errCh := make(chan error)
			close(ch)
			err := waitShutdown(&http.Server{}, ch, errCh)
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("errChan has error", func() {
			ch := make(chan struct{})
			errCh := make(chan error, 1)
			startErr := errors.New("failed to start server")
			errCh <- startErr
			err := waitShutdown(&http.Server{}, ch, errCh)
			convey.So(err, convey.ShouldEqual, startErr)
		})
		convey.Convey("errChan has no error", func() {
			ch := make(chan struct{})
			errCh := make(chan error, 1)
			errCh <- nil
			err := waitShutdown(&http.Server{}, ch, errCh)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}
