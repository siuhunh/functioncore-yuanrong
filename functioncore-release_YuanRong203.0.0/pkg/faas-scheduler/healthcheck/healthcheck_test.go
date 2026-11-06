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

package healthcheck

import (
	"os"
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

func TestStartHealthCheck(t *testing.T) {
	convey.Convey("TestStartHealthCheck", t, func() {
		os.Setenv("POD_IP", "127.0.0.1")
		config.InitDefaultConfig()
		config.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
		}
		errChan := make(chan error, 2)
		err := StartHealthCheck(errChan)
		convey.So(err, convey.ShouldBeNil)
		time.Sleep(1 * time.Second)
	})
}

func TestStartHealthCheckFail(t *testing.T) {
	convey.Convey("TestStartHealthCheckFail", t, func() {
		config.InitDefaultConfig()
		config.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{
			HTTPSEnable: false,
		}

		err := StartHealthCheck(nil)
		convey.So(err, convey.ShouldNotBeNil)

		os.Clearenv()
		errChan := make(chan error, 1)
		err = StartHealthCheck(errChan)
		convey.So(err, convey.ShouldNotBeNil)

		os.Setenv("POD_IP", "123")
		err = StartHealthCheck(errChan)
		convey.So(err, convey.ShouldNotBeNil)

		os.Setenv("POD_IP", "1.1.1.1")
		err = StartHealthCheck(errChan)
		convey.So(err, convey.ShouldBeNil)
		err = <-errChan
		convey.So(err, convey.ShouldNotBeNil)

		config.FConfig.HTTPSConfig.HTTPSEnable = true
		err = StartHealthCheck(errChan)
		convey.So(err, convey.ShouldBeNil)
		err = <-errChan
		convey.So(err, convey.ShouldNotBeNil)
	})
}
