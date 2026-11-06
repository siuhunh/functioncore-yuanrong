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

package coreconfig

import (
	json2 "encoding/json"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	loggerConfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/functiontask/task/config"
)

// TestInit -
func TestInit(t *testing.T) {
	Convey("test: init err and not", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(initTaskLog, func() error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		Convey("test: json unmarshal err", func() {
			Convey("unmarshal string to int err", func() {
				os.Setenv(TaskConfigEnv, `{"redis_max_idle":"16"}`)
				defer func() {
					os.Unsetenv(TaskConfigEnv)
				}()
				_, err := Init()
				So(err, ShouldNotBeNil)
			})
			Convey("unmarshal string to bool err", func() {
				os.Setenv(TaskConfigEnv, `{"state_server_enable_tls":"true"}`)
				defer func() {
					os.Unsetenv(TaskConfigEnv)
				}()
				_, err := Init()
				So(err, ShouldNotBeNil)
			})
		})
		Convey("test: json unmarshal ok", func() {
			os.Setenv(TaskConfigEnv, `{"state_server_addr":"127.0.0.1","redis_max_idle":10,
"state_server_enable_tls":true,"future_ttl":30}`)
			defer func() {
				os.Unsetenv(TaskConfigEnv)
			}()
			Convey("test: no host ip", func() {
				_, err := Init()
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "get HostIP error")
			})
			Convey("test: no pod ip", func() {
				os.Setenv("HOST_IP", "127.0.0.1")
				defer func() {
					os.Unsetenv("HOST_IP")
				}()
				_, err := Init()
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "get podIP error")
			})
			Convey("test: init ok", func() {
				patch := gomonkey.ApplyFunc(
					GetHotloadStateServerConf, func() (string, redisclient.TimeoutConf, error) {
						return "127.0.0.1", redisclient.GetDefaultTimeoutConf(), nil
					})
				os.Setenv("HOST_IP", "127.0.0.1")
				os.Setenv("POD_IP", "127.0.0.1")
				defer func() {
					patch.Reset()
					os.Unsetenv("HOST_IP")
					os.Unsetenv("POD_IP")
				}()
				cfg, err := Init()
				So(err, ShouldBeNil)
				So(cfg.StateServerAddr, ShouldEqual, "127.0.0.1")
				So(cfg.RedisMaxIdle, ShouldEqual, 10)
				So(cfg.StateServerEnableTLS, ShouldBeTrue)
				So(cfg.FutureTTL, ShouldEqual, 30*time.Second)
			})
		})
	})
}

// TestInit_confCheck -
func TestInit_confCheck(t *testing.T) {
	Convey("test: conf check", t, func() {
		Convey("test: default value", func() {
			conf := new(Config)
			confCheck(conf)
			So(conf.CallTimeout, ShouldEqual, DefaultTimeout*time.Second)
			So(conf.FutureTTL, ShouldEqual, config.DefaultFutureTTL)
			So(conf.ExternalFutureTTL, ShouldEqual, config.DefaultExternalFutureTTL)
			So(conf.MaxSingleWaitFutureCnt, ShouldEqual, config.DefaultMaxSingleWaitFutureCnt)
		})
		Convey("test: invalid call timeout", func() {
			conf := new(Config)
			conf.CallTimeout = 1
			confCheck(conf)
			So(conf.CallTimeout, ShouldEqual, config.DefaultCallTimeout)
		})
		Convey("test: valid value", func() {
			conf := new(Config)
			conf.FutureTTL = 100
			conf.ExternalFutureTTL = 100
			confCheck(conf)
			So(conf.FutureTTL, ShouldEqual, 100*time.Second)
			So(conf.ExternalFutureTTL, ShouldEqual, 100)
		})
	})
}

func Test_initTaskLog(t *testing.T) {
	logConfigs := loggerConfig.CoreInfo{
		FilePath: "/home/sn/log",
		Level:    "INFO",
	}
	Convey("Test_initTaskLog", t, func() {
		patches := gomonkey.ApplyFunc(loggerConfig.GetCoreInfoFromEnv, func() (loggerConfig.CoreInfo, error) {
			return logConfigs, nil
		})
		defer patches.Reset()
		Convey("test1", func() {
			So(initTaskLog(), ShouldBeNil)
		})
	})
}

// TestGetHotloadStateServerConf tests open file failed condition
func TestGetHotloadStateServerConf(t *testing.T) {
	tests := []struct {
		name    string
		want    string
		want2   redisclient.TimeoutConf
		wantErr bool
	}{
		{"failed open file", "", redisclient.GetDefaultTimeoutConf(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got2, err := GetHotloadStateServerConf()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetHotloadStateServerConf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetHotloadStateServerConf() got = %v, want %v", got, tt.want)
			}
			if got2 != tt.want2 {
				t.Errorf("GetHotloadStateServerConf() got = %v, want %v", got2, tt.want2)
			}
		})
	}
}

// TestGetHotloadStateServerConf_2  tests open file success condition
func TestGetHotloadStateServerConf_2(t *testing.T) {
	patch := gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
		return &os.File{}, nil
	})
	patchs := gomonkey.ApplyMethod(reflect.TypeOf(&json2.Decoder{}), "Decode",
		func(_ *json2.Decoder, v interface{}) error {
			switch v.(type) {
			case *Config:
				v = &Config{
					Config: config.Config{
						CallTimeout:            10,
						FutureTTL:              10,
						ExternalFutureTTL:      10,
						InitStateClient:        false,
						MaxSingleWaitFutureCnt: 10,
					},
				}
			}
			return nil
		})
	defer patch.Reset()
	defer patchs.Reset()
	tests := []struct {
		name    string
		want    string
		want2   redisclient.TimeoutConf
		wantErr bool
	}{
		{"success open file", "", redisclient.GetDefaultTimeoutConf(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got2, err := GetHotloadStateServerConf()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetHotloadStateServerConf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("GetHotloadStateServerConf() got = %v, want %v", got, tt.want)
			}
			if got2 != tt.want2 {
				t.Errorf("GetHotloadStateServerConf() got = %v, want %v", got2, tt.want2)
			}
		})
	}
}
