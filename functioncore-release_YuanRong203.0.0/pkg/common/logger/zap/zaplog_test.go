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
package zap

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	uberZap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/logger/config"
)

// TestNewDvelopmentLog Test New Dvelopment Log
func TestNewDvelopmentLog(t *testing.T) {
	if _, err := NewDevelopmentLog(); err != nil {
		t.Errorf("NewDevelopmentLog() = %q, wants *logger", err)
	}
}

func TestNewConsoleLog(t *testing.T) {
	tests := []struct {
		name    string
		want    *uberZap.Logger
		wantErr bool
	}{
		{"case1", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewConsoleLog()
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConsoleLog() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestNewWithLevel(t *testing.T) {
	type args struct {
		coreInfo config.CoreInfo
		isAsync  bool
	}
	var a args
	tests := []struct {
		name    string
		args    args
		want    *uberZap.Logger
		wantErr bool
	}{
		{"case1", a, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewWithLevel(tt.args.coreInfo, tt.args.isAsync)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewWithLevel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewWithLevel() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoggerWithFormat_Infof(t *testing.T) {
	type fields struct {
		Logger *uberZap.Logger
	}
	type args struct {
		format string
		paras  []interface{}
	}
	coreInfo := config.CoreInfo{
		FilePath:   "tmp",
		Level:      "DEBUG",
		Tick:       0,
		First:      0,
		Thereafter: 0,
		Tracing:    false,
		Disable:    false,
	}
	logger, err := NewWithLevel(coreInfo, true)
	if err != nil {
		fmt.Println(err)
	}
	var f fields
	f.Logger = logger
	var a args
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"case1", f, a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := &LoggerWithFormat{
				Logger: tt.fields.Logger,
			}
			z.Infof(tt.args.format, tt.args.paras...)
		})
	}
}

func TestNewCoreWithDebugLevel(t *testing.T) {
	convey.Convey("TestNewCoreWithInfoLevel", t, func() {
		coreInfo := config.CoreInfo{
			FilePath:   "tmp",
			Level:      "DEBUG",
			Tick:       0,
			First:      0,
			Thereafter: 0,
			Tracing:    false,
			Disable:    false,
		}
		logger, err := NewWithLevel(coreInfo, true)
		convey.So(logger, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		type fields struct {
			Logger *uberZap.Logger
		}
		z := &LoggerWithFormat{
			Logger: logger,
		}
		cnt := 0
		gomonkey.ApplyMethod(reflect.TypeOf(logger), "Debug",
			func(log *uberZap.Logger, msg string, fields ...uberZap.Field) {
				cnt += 1
			})
		z.Debugf("should print")
		convey.So(cnt, convey.ShouldEqual, 1)
	})
}

func TestNewCoreWithInfoLevel(t *testing.T) {
	convey.Convey("TestNewCoreWithInfoLevel", t, func() {
		coreInfo := config.CoreInfo{
			FilePath:   "tmp",
			Level:      "INFO",
			Tick:       0,
			First:      0,
			Thereafter: 0,
			Tracing:    false,
			Disable:    false,
		}
		logger, err := NewWithLevel(coreInfo, true)
		convey.So(logger, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
		type fields struct {
			Logger *uberZap.Logger
		}
		z := &LoggerWithFormat{
			Logger: logger,
		}
		cnt := 0
		gomonkey.ApplyMethod(reflect.TypeOf(logger), "Debug",
			func(log *uberZap.Logger, msg string, fields ...uberZap.Field) {
				cnt += 1
			})
		z.Debugf("should not print")
		convey.So(cnt, convey.ShouldEqual, 0)
	})
}

func TestLevelToShort(t *testing.T) {
	convey.Convey("test levelToShort", t, func() {
		convey.Convey("case1: debug", func() {
			level := levelToShort(zapcore.Level(-1))
			convey.So(level, convey.ShouldEqual, "D")
		})
		convey.Convey("case2: info", func() {
			level := levelToShort(zapcore.Level(0))
			convey.So(level, convey.ShouldEqual, "I")
		})
		convey.Convey("case3: warn", func() {
			level := levelToShort(zapcore.Level(1))
			convey.So(level, convey.ShouldEqual, "W")
		})
		convey.Convey("case4: error", func() {
			level := levelToShort(zapcore.Level(2))
			convey.So(level, convey.ShouldEqual, "E")
		})
		convey.Convey("case5: dpanic", func() {
			level := levelToShort(zapcore.Level(3))
			convey.So(level, convey.ShouldEqual, "DP")
		})
		convey.Convey("case6: panic", func() {
			level := levelToShort(zapcore.Level(4))
			convey.So(level, convey.ShouldEqual, "P")
		})
		convey.Convey("case7: fatal", func() {
			level := levelToShort(zapcore.Level(5))
			convey.So(level, convey.ShouldEqual, "F")
		})
		convey.Convey("case7: other", func() {
			level := levelToShort(zapcore.Level(6))
			convey.So(level, convey.ShouldEqual, "")
		})
	})
}
