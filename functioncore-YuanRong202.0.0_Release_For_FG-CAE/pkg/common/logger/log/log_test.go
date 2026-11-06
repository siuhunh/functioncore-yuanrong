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

package log

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	uberZap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/zap"
	mockutils "functioncore/test/utils"
)

func TestInitRunLog(t *testing.T) {
	logConfigs := config.CoreInfo{
		FilePath: "/home/sn/log",
		Level:    "INFO",
	}
	type args struct {
		fileName string
	}
	var tests = []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "test01",
			args: args{fileName: "logTest"},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetCoreInfoFromEnv,
						func() (config.CoreInfo, error) {
							return logConfigs, nil
						}),
				})
				return patches
			},
		},
		{
			// GetCoreInfoFromEnv error
			name: "test02",
			args: args{fileName: "logTest"},
			wantErr: true,
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(config.GetCoreInfoFromEnv,
						func() (config.CoreInfo, error) {
							return logConfigs, errors.New("Test error")
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := InitRunLog(tt.args.fileName, true); (err != nil) != tt.wantErr {
				t.Errorf("InitRunLog() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestInitRunLogWithConfig(t *testing.T) {
	type args struct {
		fileName string
		isAsync  bool
		coreInfo config.CoreInfo
	}
	tests := []struct {
		name      string
		args      args
		want      FormatLogger
		wantErr   bool
		patchFunc mockutils.PatchesFunc
	}{
		{
			name: "test01",
			args: args{
				fileName: "",
				coreInfo: config.CoreInfo{},
			},
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			got, err := InitRunLogWithConfig(tt.args.fileName, tt.args.isAsync, tt.args.coreInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("InitRunLogWithConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got == nil {
				t.Errorf("InitRunLogWithConfig() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestNewConsoleLogger(t *testing.T) {
	tests := []struct {
		name      string
		want      FormatLogger
		patchFunc mockutils.PatchesFunc
	}{
		{
			name: "test01",
			patchFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(zap.NewConsoleLog, func() (*uberZap.Logger, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchFunc()
			if got := NewConsoleLogger(); got == nil {
				t.Errorf("NewConsoleLogger() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestGetLogger(t *testing.T) {
	tests := []struct {
		name string
		want FormatLogger
	}{
		{
			name: "test01",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetLogger(); got == nil {
				t.Errorf("GetLogger() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_zapLoggerWithFormat_With(t *testing.T) {
	type fields struct {
		Logger  *uberZap.Logger
		SLogger *uberZap.SugaredLogger
	}
	type args struct {
		fields []zapcore.Field
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   FormatLogger
	}{
		{
			name: "test01",
			fields: fields{
				Logger:  &uberZap.Logger{},
				SLogger: &uberZap.SugaredLogger{},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := &zapLoggerWithFormat{
				Logger:  tt.fields.Logger,
				SLogger: tt.fields.SLogger,
			}
			if got := z.With(tt.args.fields...); got == nil {
				t.Errorf("With() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewFormatLogger(t *testing.T) {
	convey.Convey("Test New Format Logger", t, func() {
		fileName := constants.MonitorFileName
		coreInfo := config.CoreInfo{}
		formatLogger, err := newFormatLogger(fileName, true, coreInfo)
		assert.NotEmpty(t, formatLogger)
		assert.Empty(t, err)
	})
	convey.Convey("Test New Format Logger 2", t, func() {
		patch := gomonkey.ApplyFunc(zap.NewWithLevel,
			func(coreInfo config.CoreInfo, isAsync bool) (*uberZap.Logger, error) {
				return nil, errors.New("1")
			})
		defer patch.Reset()
		fileName := constants.MonitorFileName
		coreInfo := config.CoreInfo{}
		formatLogger, err := newFormatLogger(fileName, false, coreInfo)
		assert.Empty(t, formatLogger)
		assert.NotEmpty(t, err)
	})
}

func TestGetLogger1(t *testing.T) {
	convey.Convey("TestGetLogger1", t, func() {
		logger := GetLogger()
		convey.So(logger, convey.ShouldNotBeNil)
		logger.Debug("test debug")
		logger.Info("test info")
		logger.Warn("test warn")
		logger.Error("test error")
		logger.Sync()
	})
}
