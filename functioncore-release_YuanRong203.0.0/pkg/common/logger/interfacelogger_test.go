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

package logger

import (
	"errors"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/logger/config"
)

func TestInterfaceLogger(t *testing.T) {
	cfg := InterfaceEncoderConfig{ModuleName: "WorkerManager"}
	interfaceLog, err := NewInterfaceLogger("", "worker-manager-interface", cfg)
	interfaceLog.Write("123")
	assert.Empty(t, err)
	assert.NotEmpty(t, interfaceLog)
}

func TestCreateSink(t *testing.T) {
	convey.Convey("Test Create Sink Error", t, func() {
		coreInfo := config.CoreInfo{}
		w, err := CreateSink(coreInfo)
		convey.So(w, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test Create Sink Error 2", t, func() {
		patch := gomonkey.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return errors.New("err")
		})
		defer patch.Reset()
		coreInfo := config.CoreInfo{}
		w, err := CreateSink(coreInfo)
		convey.So(w, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})

}
