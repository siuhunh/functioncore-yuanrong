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

package functionlog

import (
	"io"
	"testing"

	"functioncore/pkg/common/logger"
	loggerConfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/worker/config"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
)

func TestNewBillingLogger(t *testing.T) {
	convey.Convey("Test NewBillingLogger", t, func() {
		logConfigs := loggerConfig.CoreInfo{
			FilePath: "/home/sn/log/service",
			Level:    "INFO",
		}
		p := gomonkey.NewPatches()
		p.ApplyFunc(logger.CreateSink, func(coreInfo loggerConfig.CoreInfo) (io.Writer, error) {
			return io.Discard, nil
		})
		p.ApplyFunc(loggerConfig.ExtractCoreInfoFromEnv, func(env string) (loggerConfig.CoreInfo, error) {
			return logConfigs, nil
		})
		defer p.Reset()

		cfg := config.Configuration{
			StartArgs:               config.StartArgs{},
			RuntimeConfig:           config.RuntimeConfig{},
			DeploymentConfig:        config.DeploymentConfig{},
			ServerConfig:            config.ServerConfig{},
			VPCConfig:               config.VPCConfig{},
			StorageConfig:           config.StorageConfig{},
			InvokeServer:            config.InvokeServer{},
			BackgroundProcessLimit:  config.BackgroundProcessLimit{},
			HeartbeatConfig:         config.HeartbeatConfig{},
			UserLogTag:              "",
			FunctionNameSeparator:   "",
			ReportTimeWindowsPeriod: 0,
			SyncRequestTimeout:      0,
			ReconnectTimes:          0,
		}

		f, _ := NewBillingLogger(&cfg)
		f.WriteLog("test", "test")
	})

	convey.Convey("Test NewBillingLogger with nil writer", t, func() {
		f := &BillingLogger{}
		f.WriteLog("test", "test")
	})
}
