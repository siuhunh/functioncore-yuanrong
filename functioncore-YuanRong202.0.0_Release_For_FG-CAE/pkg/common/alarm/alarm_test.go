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

package alarm

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/snapshotmanager"
)

func Test_exponentialWithJitterBackoff(t *testing.T) {
	convey.Convey("exponentialWithJitterBackoff", t, func() {
		convey.Convey("case 1", func() {
			res := exponentialWithJitterBackoff(4)
			convey.So(res, convey.ShouldBeBetween, time.Duration(0)*time.Second, time.Duration(16)*time.Second)
		})

		convey.Convey("case 2", func() {
			res := exponentialWithJitterBackoff(7)
			convey.So(res, convey.ShouldBeBetween, time.Duration(0)*time.Second, time.Duration(128)*time.Second)
		})

		convey.Convey("case 3", func() {
			res := exponentialWithJitterBackoff(1000000)
			convey.So(res, convey.ShouldBeBetween, time.Duration(0)*time.Second, time.Duration(128)*time.Second)
		})
	})
}

func Test_generateAbnormalAlarmWithSuppression(t *testing.T) {
	meta := &alarmMeta{lock: &sync.Mutex{}, info: &AlarmInfo{Severity: RouterEtcdRegistry.Severity()}}
	convey.Convey("generateAbnormalAlarmWithSuppression", t, func() {
		convey.Convey("duplicated abnormal", func() {
			meta.isNormal = false
			req := meta.generateAbnormalAlarmWithSuppression("")
			convey.So(meta.num, convey.ShouldEqual, 0)
			convey.So(req, convey.ShouldBeNil)
		})
		convey.Convey("first abnormal alarm", func() {
			meta.isNormal = true
			req := meta.generateAbnormalAlarmWithSuppression("")
			convey.So(meta.num, convey.ShouldEqual, 1)
			convey.So(req, convey.ShouldNotBeNil)
		})
		convey.Convey("suppressed abnormal alarm", func() {
			meta.isNormal = true
			req := meta.generateAbnormalAlarmWithSuppression("")
			convey.So(meta.num, convey.ShouldEqual, 2)
			convey.So(req, convey.ShouldBeNil)
		})
		convey.Convey("alarm after suppressed abnormal alarm", func() {
			meta.isNormal = true
			meta.num = alarmSuppressNum
			req := meta.generateAbnormalAlarmWithSuppression("")
			convey.So(req, convey.ShouldNotBeNil)
		})
	})
}

func TestAlarm_notifyAlarm(t *testing.T) {
	c := Client{httpClient: &http.Client{}, sendCh: make(chan alarmReq)}
	convey.Convey("stop alarm", t, func() {
		ctx, cancel := context.WithCancel(context.TODO())
		req := alarmReq{
			ctx:    ctx,
			cancel: cancel,
			data:   nil,
		}
		cancel()
		// finish by cancelled
		c.notifyAlarm(req)
	})
	convey.Convey("cancel alarm1", t, func() {
		ctx, _ := context.WithCancel(context.TODO())
		req := alarmReq{
			ctx:  ctx,
			data: nil,
		}
		defer gomonkey.ApplyMethod(reflect.TypeOf(c.httpClient), "Do",
			func(*http.Client, *http.Request) (*http.Response, error) {
				return nil, context.Canceled
			}).Reset()
		// finish by cancelled
		c.notifyAlarm(req)
	})
	convey.Convey("send alarm", t, func() {
		ctx, _ := context.WithCancel(context.TODO())
		req := alarmReq{
			ctx:  ctx,
			data: nil,
		}
		times := 0
		defer gomonkey.ApplyMethod(reflect.TypeOf(c.httpClient), "Do",
			func(*http.Client, *http.Request) (*http.Response, error) {
				if times == 0 {
					times++
					return &http.Response{StatusCode: http.StatusNotFound}, nil
				}
				if times == 1 {
					times++
					return &http.Response{StatusCode: http.StatusNotFound,
						Body: ioutil.NopCloser(bytes.NewReader([]byte("abc")))}, nil
				}
				return &http.Response{StatusCode: http.StatusOK}, nil
			}).Reset()
		defer gomonkey.ApplyFunc(time.Sleep, func(duration time.Duration) {}).Reset()
		// finished by alarm success
		c.notifyAlarm(req)
	})
}

func TestAlarm_reportAlarmInfo(t *testing.T) {
	c := Client{httpClient: &http.Client{}, alarmMetas: make(map[Type]*alarmMeta, 0), sendCh: make(chan alarmReq, typeSize)}
	convey.Convey("reportAlarmInfo", t, func() {
		convey.Convey("nil channel", func() {
			c.reportAlarmInfo(nil)
		})
		convey.Convey("stop sending alarm", func() {
			stopCh := make(chan struct{})
			sendCh := make(chan alarmReq, typeSize)
			close(c.sendCh)
			c.reportAlarmInfo(stopCh)
			c.sendCh = sendCh
		})
	})
}

func Test_newHTTPRequest(t *testing.T) {
	c := Client{}
	convey.Convey("", t, func() {
		convey.Convey("succeed to new HTTP request", func() {
			url := "http://localhost:1234"
			req := c.newHTTPRequest(alarmReq{
				ctx: context.TODO(),
			}, url)
			convey.So(req, convey.ShouldNotBeNil)
		})
		convey.Convey("failed to new HTTP request", func() {
			url := "://localhost"
			req := c.newHTTPRequest(alarmReq{
				ctx: context.TODO(),
			}, url)
			convey.So(req, convey.ShouldBeNil)
		})
	})
}

func assertRegistered(t *testing.T, p Type) {
	_, ok := client.alarmMetas[p]
	if !ok {
		t.Errorf("assert type registered failed: %d", p)
	}
}

func assertNotRegistered(t *testing.T, p Type) {
	_, ok := client.alarmMetas[p]
	if ok {
		t.Errorf("assert type not registered failed: %d", p)
	}
}

func TestClient_ReportAlarm(t *testing.T) {
	convey.Convey("TestInitLeaseAlarm", t, func() {
		stopCh := make(chan struct{})
		err := NewClient("testurl-test", constants.FunctionTask, stopCh, WithAuth("", ""),
			WithTLSEnable(nil))
		convey.So(err, convey.ShouldBeNil)

		// report alarm without registration
		GetClient().ReportAlarm(RouterEtcdRegistry, "")
		GetClient().ClearAlarm(RouterEtcdRegistry)
		assertNotRegistered(t, RouterEtcdRegistry)

		// report alarm after registration
		GetClient().RegisterAlarmType(RouterEtcdRegistry)
		assertRegistered(t, RouterEtcdRegistry)
		GetClient().ReportAlarm(RouterEtcdRegistry, fmt.Sprintf("error msg=etcd lease failed for %v,", time.Duration(1)*time.Second))
		convey.So(client.alarmMetas[RouterEtcdRegistry].isNormal, convey.ShouldBeFalse)
		convey.So(client.alarmMetas[RouterEtcdRegistry].num, convey.ShouldEqual, 1)
		convey.So(strings.Contains(client.alarmMetas[RouterEtcdRegistry].info.Additional, "1s"), convey.ShouldBeTrue)
		convey.So(client.alarmMetas[RouterEtcdRegistry].info.AlarmID, convey.ShouldEqual, routerEtcdRegistryAlarmID)
		GetClient().ClearAlarm(RouterEtcdRegistry)
		convey.So(client.alarmMetas[RouterEtcdRegistry].isNormal, convey.ShouldBeTrue)
		GetClient().ReportAlarm(RouterEtcdRegistry, fmt.Sprintf("error msg=etcd lease failed for %v,", time.Duration(1)*time.Second))
		convey.So(strings.Count(client.alarmMetas[RouterEtcdRegistry].info.Additional, "1s"), convey.ShouldEqual, 1)

		// report alarm without suppressed
		GetClient().RegisterAlarmType(SWRConnection)
		GetClient().ReportAlarm(SWRConnection, fmt.Sprintf("error msg=etcd lease failed for %v,", time.Duration(1)*time.Second))
		convey.So(client.alarmMetas[SWRConnection].isNormal, convey.ShouldBeFalse)
		convey.So(client.alarmMetas[SWRConnection].num, convey.ShouldEqual, 0)
		convey.So(client.alarmMetas[SWRConnection].info.AlarmID, convey.ShouldEqual, swrConnectionAlarmID)

		GetClient().RegisterAlarmType(typeSize - 1)
		GetClient().ReportAlarm(typeSize-1, fmt.Sprintf("error msg=test error"))
		convey.So(client.alarmMetas[typeSize-1].info.AlarmID, convey.ShouldEqual, (typeSize - 1).AlarmID())
		convey.So(client.alarmMetas[typeSize-1].info.AlarmName, convey.ShouldEqual, (typeSize - 1).AlarmName())

		close(GetClient().sendCh)
		close(stopCh)
	})
	convey.Convey("report alarm with snapshotManagerMode true", t, func() {
		stopCh := make(chan struct{})
		err := NewClient("test-url", constants.FunctionTask, stopCh, WithAuth("", ""),
			WithTLSEnable(nil))
		defer func() {
			close(stopCh)
			close(GetClient().sendCh)
		}()
		convey.So(err, convey.ShouldBeNil)

		GetClient().RegisterAlarmType(RouterEtcdRegistry)
		assertRegistered(t, RouterEtcdRegistry)
		snapshotmanager.SetSnapshotManagerMode(true)
		defer snapshotmanager.SetSnapshotManagerMode(false)
		GetClient().ReportAlarm(RouterEtcdRegistry, fmt.Sprintf("error msg=etcd lease failed for %v,", time.Duration(1)*time.Second))
		convey.So(client.alarmMetas[RouterEtcdRegistry].isNormal, convey.ShouldBeTrue)
		convey.So(client.alarmMetas[RouterEtcdRegistry].num, convey.ShouldEqual, 0)
	})
}

func TestAlarm_doAlarmRequestByUrls(t *testing.T) {
	testCases := []struct {
		name        string
		address     string
		expectUrls  []string
		expectError bool
	}{
		{
			name:        "request two address failed",
			address:     "127.0.0.1:1234,127.0.0.2:1234",
			expectUrls:  []string{"http://127.0.0.1:1234/v2/fgs/om/alarm", "http://127.0.0.2:1234/v2/fgs/om/alarm"},
			expectError: true,
		},
		{
			name:        "request two address success",
			address:     "127.0.0.1:1234,127.0.0.2:1234",
			expectUrls:  []string{"http://127.0.0.1:1234/v2/fgs/om/alarm", "http://127.0.0.2:1234/v2/fgs/om/alarm"},
			expectError: false,
		},
		{
			name:        "request one address failed",
			address:     "127.0.0.1:1234",
			expectUrls:  []string{"http://127.0.0.1:1234/v2/fgs/om/alarm"},
			expectError: true,
		},
		{
			name:        "request two address success",
			address:     "127.0.0.1:1234",
			expectUrls:  []string{"http://127.0.0.1:1234/v2/fgs/om/alarm"},
			expectError: false,
		},
	}
	for _, tt := range testCases {
		convey.Convey(tt.name, t, func() {
			stopCh := make(chan struct{})
			err := NewClient(tt.address, constants.FunctionTask, stopCh, WithAuth("", ""),
				WithTLSEnable(nil))
			defer func() {
				close(stopCh)
				close(GetClient().sendCh)
			}()
			convey.So(err, convey.ShouldBeNil)
			convey.So(reflect.DeepEqual(client.urls, tt.expectUrls), convey.ShouldBeTrue)
			defer gomonkey.ApplyMethod(reflect.TypeOf(client.httpClient), "Do",
				func(*http.Client, *http.Request) (*http.Response, error) {
					if tt.expectError {
						return nil, errors.New("failed")
					}
					return nil, nil
				}).Reset()
			err = client.doAlarmRequestByUrls(alarmReq{ctx: context.TODO()})
			if tt.expectError {
				convey.So(err, convey.ShouldNotBeNil)
			} else {
				convey.So(err, convey.ShouldBeNil)
			}
		})
	}
}
