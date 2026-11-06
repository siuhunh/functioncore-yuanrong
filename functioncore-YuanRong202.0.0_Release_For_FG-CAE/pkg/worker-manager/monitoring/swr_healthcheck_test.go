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

package monitoring

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/utils"
)

func TestSWRAlarm(t *testing.T) {
	cli := http.DefaultClient
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(localauth.SignOMSVC, func(ak, sk, url string, data []byte) (string, string) {
		return "", ""
	})
	patches.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		if req.URL.Host == "aaaa" {
			return &http.Response{StatusCode: http.StatusOK, Body: ioutil.NopCloser(bytes.NewBuffer([]byte(`{"RawToken": "token"}`)))}, nil
		} else {
			return &http.Response{StatusCode: http.StatusOK, Body: ioutil.NopCloser(bytes.NewBuffer([]byte{}))}, nil
		}
	})
	err := checkSWRHealth(cli, "aaaa", "ak", "sk", "swr.cn-north-7.myhuaweicloud.com")
	assert.NoError(t, err)
}

func (ReadCloserImp) Read(s []byte) (n int, err error) {
	return len(s), io.EOF
}

func (ReadCloserImp) Close() error {
	return nil
}

type ReadCloserImp struct{}

func Test_sendSWRRequest(t *testing.T) {
	c := &http.Client{}
	convey.Convey("sendSWRRequest", t, func() {
		convey.Convey("illegal url", func() {
			retry, err := sendSWRRequest(c, "://localhost", "token string")
			convey.So(retry, convey.ShouldEqual, true)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("request failed", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(c), "Do",
				func(*http.Client, *http.Request) (*http.Response, error) {
					return nil, errors.New("request failed")
				}).Reset()
			retry, err := sendSWRRequest(c, "http://testurl-test", "token string")
			convey.So(retry, convey.ShouldEqual, true)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("too many requests", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(c), "Do",
				func(*http.Client, *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusTooManyRequests, Body: ReadCloserImp{}}, nil
				}).Reset()
			retry, err := sendSWRRequest(c, "http://testurl-test", "token string")
			convey.So(retry, convey.ShouldEqual, false)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("not 200", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(c), "Do",
				func(*http.Client, *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusNotFound, Body: ReadCloserImp{}}, nil
				}).Reset()
			retry, err := sendSWRRequest(c, "http://testurl-test", "token string")
			convey.So(retry, convey.ShouldEqual, true)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestAlarm_swrHealthCheckAndReport(t *testing.T) {
	stopCh := make(chan struct{})

	reportChan := make(chan int, 1)

	convey.Convey("swrHealthCheckAndReport", t, func() {
		patches := gomonkey.NewPatches()
		defer func() {
			patches.Reset()
		}()
		patches.ApplyFunc((*alarm.Client).RegisterAlarmType,
			func(a *alarm.Client, almType alarm.Type) {
				return
			})
		patches.ApplyFunc((*alarm.Client).ReportAlarm,
			func(a *alarm.Client, almType alarm.Type, specAddition string) {
				reportChan <- 1
			})
		patches.ApplyMethod(reflect.TypeOf(http.DefaultClient), "Do",
			func(*http.Client, *http.Request) (*http.Response, error) {
				return nil, errors.New("")
			})
		patches.ApplyFunc(time.Sleep, func(time.Duration) {
			close(stopCh)
		})
		swrHealthCheckAndReport("127.0.0.1", "ak", "sk", "test", nil)
		swrHealthCheckAndReport("127.0.0.1", "ak", "sk", "test", stopCh)
		res := <-reportChan
		assert.Equal(t, 1, res)
	})
}

func TestAlarm_checkSWRHealth(t *testing.T) {
	cli := http.DefaultClient
	convey.Convey("checkSWRHealth", t, func() {
		convey.Convey("success", func() {
			patches := gomonkey.NewPatches()
			defer patches.Reset()
			patches.ApplyFunc(getTokenByAddresses, func(baseURLPath, ak, sk string) (string, error) {
				return "test", nil
			})
			patches.ApplyFunc(sendSWRRequest, func(cli *http.Client, url, token string) (bool, error) {
				return true, errors.New("test")
			})
			patches.ApplyFunc(utils.ClearStringMemory, func(s string) {
				return
			})
			err := checkSWRHealth(cli, "127.0.0.1", "ak", "sk", "swr.cn-north-7.myhuaweicloud.com")
			assert.Error(t, err)
		})
		convey.Convey("failed", func() {
			patches := gomonkey.NewPatches()
			defer patches.Reset()
			patches.ApplyFunc(getTokenByAddresses, func(baseURLPath, ak, sk string) (string, error) {
				return "", errors.New("failed")
			})
			patches.ApplyFunc(sendSWRRequest, func(cli *http.Client, url, token string) (bool, error) {
				return true, errors.New("test")
			})
			patches.ApplyFunc(utils.ClearStringMemory, func(s string) {
				return
			})
			err := checkSWRHealth(cli, "127.0.0.1", "ak", "sk", "swr.cn-north-7.myhuaweicloud.com")
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestAlarm_getTokenByAddresses(t *testing.T) {
	convey.Convey("getTokenByAddresses", t, func() {
		convey.Convey("two address failed", func() {
			patches := gomonkey.NewPatches()
			defer patches.Reset()
			patches.ApplyFunc(getToken, func(baseURLPath, ak, sk string) (string, error) {
				return "", errors.New("failed")
			})
			token, err := getTokenByAddresses("127.0.0.1,127.0.0.2", "ak", "sk")
			convey.So(token, convey.ShouldEqual, "")
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("two address success", func() {
			patches := gomonkey.NewPatches()
			defer patches.Reset()
			patches.ApplyFunc(getToken, func(baseURLPath, ak, sk string) (string, error) {
				return "token", nil
			})
			token, err := getTokenByAddresses("127.0.0.1,127.0.0.2", "ak", "sk")
			convey.So(token, convey.ShouldEqual, "token")
			convey.So(err, convey.ShouldBeNil)
		})
	})
}
