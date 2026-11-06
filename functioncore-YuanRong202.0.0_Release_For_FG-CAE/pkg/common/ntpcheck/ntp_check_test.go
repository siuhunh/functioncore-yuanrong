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

package ntpcheck

import (
	sslTls "crypto/tls"
	"errors"
	"net/http"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
)

func TestGetNtpConfig(t *testing.T) {
	convey.Convey("GetNtpConfig", t, func() {
		convey.Convey("case 1: the file does not exist.", func() {
			err := InitConfig("../cmd/ntp-check/ntp_check_config.json")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("case 2: the parameter is not compliant.", func() {
			err := InitConfig("../../../cmd/ntp-check/ntp_check_config.json")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("case 2:", func() {
			defer gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
				return false, nil
			}).Reset()
			err := InitConfig("../../../cmd/ntp-check/ntp_check_config.json")
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestGetEnvValues(t *testing.T) {
	convey.Convey("GetEnvValues", t, func() {
		gomonkey.ApplyFunc(os.Getenv, func(a string) string {
			if a == "HOSTNAME" {
				return "hostName_test"
			}
			if a == "SSH_CONNECTION" {
				return "127.0.0.1 123 121.0.0.1 123"
			}
			return ""
		})
		GetEnvValues()
		convey.So(hostName, convey.ShouldEqual, "hostName_test")
		convey.So(localIP, convey.ShouldEqual, "121.0.0.1")
	})
}

func TestSetEnvValues(t *testing.T) {
	convey.Convey("SetEnvValues", t, func() {
		gomonkey.ApplyFunc(os.Setenv, func(a, b string) error {
			return nil
		})
		err := SetEnvValues(ntpCheckTLSConfig{SslPath: "123", CipherPath: "123"})
		convey.So(err, convey.ShouldEqual, nil)
	})
}

func TestNtpClient(t *testing.T) {
	convey.Convey("NtpClient", t, func() {
		ntpCfg = &Config{}
		convey.Convey("InitClientOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(a string) string {
					if a == "HOSTNAME" {
						return "hostName_test"
					}
					if a == "SSH_CONNECTION" {
						return "127.0.0.1 123 121.0.0.1 123"
					}
					return ""
				}),
				gomonkey.ApplyFunc(os.Setenv, func(a, b string) error {
					return nil
				}),
				gomonkey.ApplyFunc(tls.InitTLSConfig, func(a, b string) error {
					return nil
				}),
				gomonkey.ApplyFunc(tls.GetClientTLSConfig, func() *sslTls.Config {
					return nil
				}),
				gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
					func(a *alarm.Client, almType alarm.Type) {
						return
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitAlarmClient()
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("InitAlarmClientNOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(a string) string {
					if a == "HOSTNAME" {
						return "hostName_test"
					}
					if a == "SSH_CONNECTION" {
						return "127.0.0.1 123 121.0.0.1 123"
					}
					return ""
				}),
				gomonkey.ApplyFunc(os.Setenv, func(a, b string) error {
					return nil
				}),
				gomonkey.ApplyFunc(tls.InitTLSConfig, func(a, b string) error {
					return nil
				}),
				gomonkey.ApplyFunc(tls.GetClientTLSConfig, func() *sslTls.Config {
					return nil
				}),
				gomonkey.ApplyFunc(alarm.NewClient,
					func(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...alarm.ClientOption) error {
						return errors.New("test")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitAlarmClient()
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestRegularNtpCheck(t *testing.T) {
	convey.Convey("RegularNtpCheck", t, func() {
		ntpCfg = &Config{}
		ntpCfg.NtpServeraddress = map[string]string{"ntp0": "ntp01-in.huawei.com:123"}
		ntpCfg.CheckParam = checkParam{
			Interval:  1,
			AlarmCnt:  1,
			MaxOffset: 1,
		}

		patche1 := gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do",
			func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK}, nil
			})
		defer patche1.Reset()
		patche2 := gomonkey.ApplyFunc(localauth.SignOMSVC,
			func(ak, sk, url string, data []byte) (string, string) {
				return "666", "666"
			})
		defer patche2.Reset()

		convey.Convey("case 1 test report time offset alarm", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(time.Time{}), "Sub",
				func(_ time.Time, u time.Time) time.Duration {
					return time.Duration(1<<63 - 1)
				}).Reset()
			alarmCh := make(chan int, 1)
			alarmPatch := gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
				func(a *alarm.Client, almType alarm.Type, specAddition string) {
					close(alarmCh)
				})
			defer alarmPatch.Reset()
			go RegularNtpCheck()
			// assert report alarm called
			<-alarmCh
			ntpStopCh <- struct{}{}
		})
		convey.Convey("case 2 clear time offset alarm", func() {
			alarmFlag = true
			alarmCh := make(chan int, 1)
			alarmPatch := gomonkey.ApplyFunc(SendRemoveAlarmToOmsvc,
				func() {
					close(alarmCh)
				})
			defer alarmPatch.Reset()
			go RegularNtpCheck()
			// assert report alarm called
			<-alarmCh
			ntpStopCh <- struct{}{}
		})
	})
}

func TestNtpConnAlarm(t *testing.T) {
	convey.Convey("NtpClient disconnected test 1", t, func() {
		ntpCfg = nil
		RegularNtpCheck()
	})
	convey.Convey("NtpClient disconnected test 2,report alarm", t, func() {
		ntpCfg = &Config{}
		ntpCfg.NtpServeraddress = map[string]string{"ntp0": "ntp01-in.huawei.com:123"}
		ntpCfg.CheckParam = checkParam{
			Interval:  1,
			AlarmCnt:  1,
			MaxOffset: 1,
		}
		patches := gomonkey.ApplyFunc(GetNtpServerTimeEx, func(ntpServeraddress map[string]string) (time.Time, error) {
			return time.Now(), errors.New("a")
		})
		patches.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
			ch := make(chan time.Time, failedConnTimes+1)
			for i := 0; i < failedConnTimes+1; i++ {
				ch <- time.Time{}
			}
			return &time.Ticker{C: ch}
		})
		defer patches.Reset()
		alarmCh := make(chan int, 1)
		alarmPatch := gomonkey.ApplyFunc((*alarm.Client).ReportAlarm,
			func(a *alarm.Client, almType alarm.Type, specAddition string) {
				close(alarmCh)
			})
		defer alarmPatch.Reset()
		go RegularNtpCheck()
		<-alarmCh
		ntpStopCh <- struct{}{}
	})
	convey.Convey("NtpClient disconnected test 3,clear alarm", t, func() {
		alarmCh := make(chan int, 1)
		alarmPatch := gomonkey.ApplyFunc((*alarm.Client).ClearAlarm,
			func(a *alarm.Client, almType alarm.Type) {
				close(alarmCh)
			})
		defer alarmPatch.Reset()
		tickerCh := make(chan time.Time, failedConnTimes+1)
		patches := gomonkey.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
			for i := 0; i < failedConnTimes+1; i++ {
				tickerCh <- time.Time{}
			}
			return &time.Ticker{C: tickerCh}
		})
		defer patches.Reset()
		ntpCfg.NtpServeraddress = map[string]string{"ntp0": "aa"}
		go RegularNtpCheck()
		time.Sleep(100 * time.Millisecond)
		ntpCfg.NtpServeraddress = map[string]string{"ntp0": "ntp01-in.huawei.com:123"}
		tickerCh <- time.Time{}
		<-alarmCh
		ntpStopCh <- struct{}{}
	})
}
