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

package utils

import (
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	fhttp "github.com/valyala/fasthttp"
)

func TestGetFastHTTPClient(t *testing.T) {
	fastHTTPClient := &fhttp.Client{}
	fastHTTPClient.MaxIdemponentCallAttempts = MaxIdemponentCallAttempts
	fastHTTPClient.MaxIdleConnDuration = MaxIdleConnDuration * time.Second
	fastHTTPClient.MaxConnsPerHost = MaxConnsPerHost
	fastHTTPClient.ReadTimeout = MaxReadTimeout * time.Second
	convey.Convey("TestGetFastHTTPClient", t, func() {
		convey.Convey("success", func() {
			patches := ApplyFunc(fhttp.DialTimeout, func(string, time.Duration) (net.Conn, error) {
				return nil, errors.New("err")
			})
			defer patches.Reset()
			res := GetFastHTTPClient().Name
			actual := fastHTTPClient.Name
			convey.So(res, convey.ShouldEqual, actual)
		})
	})
}

func TestDailFunc(t *testing.T) {
	convey.Convey("TestDailFunc", t, func() {
		convey.Convey("test001", func() {
			patches := ApplyFunc(fhttp.DialTimeout, func(string, time.Duration) (net.Conn, error) {
				return nil, errors.New("mock dial timeout err")
			})
			conn, err := fastHTTPDailFunc("test address")
			convey.So(conn, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			patches.Reset()
		})
		convey.Convey("test002", func() {
			patches := ApplyFunc(fhttp.DialTimeout, func(string, time.Duration) (net.Conn, error) {
				return &net.TCPConn{}, nil
			})
			conn, err := fastHTTPDailFunc("test address")
			convey.So(conn, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			patches.Reset()
		})
		tcpConn := &net.TCPConn{}
		convey.Convey("test003", func() {
			patches := [...]*Patches{
				ApplyFunc(fhttp.DialTimeout, func(string, time.Duration) (net.Conn, error) {
					return &net.TCPConn{}, nil
				}),
				ApplyMethod(reflect.TypeOf(tcpConn), "SetKeepAlive",
					func(_ *net.TCPConn, keepalive bool) error {
						return nil
					}),
			}
			conn, err := fastHTTPDailFunc("test address")
			convey.So(conn, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			for _, p := range patches {
				p.Reset()
			}
		})
		convey.Convey("test004", func() {
			patches := [...]*Patches{
				ApplyFunc(fhttp.DialTimeout, func(string, time.Duration) (net.Conn, error) {
					return &net.TCPConn{}, nil
				}),
				ApplyMethod(reflect.TypeOf(tcpConn), "SetKeepAlive",
					func(_ *net.TCPConn, keepalive bool) error {
						return nil
					}),
				ApplyMethod(reflect.TypeOf(tcpConn), "SetKeepAlivePeriod",
					func(_ *net.TCPConn, d time.Duration) error {
						return nil
					}),
			}
			conn, err := fastHTTPDailFunc("test address")
			convey.So(conn, convey.ShouldNotBeNil)
			convey.So(err, convey.ShouldBeNil)
			for _, p := range patches {
				p.Reset()
			}
		})
	})
}
