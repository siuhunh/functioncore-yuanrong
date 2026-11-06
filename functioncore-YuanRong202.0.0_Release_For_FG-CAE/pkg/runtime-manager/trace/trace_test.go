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

package trace

import (
	"bufio"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
)

func TestEditTraceConfig(t *testing.T) {
	tmpTraceConfig := "/tmp/trace.config"
	Content := "log.level=info" + "\n"
	Content += "master.address=" + "http://" + "\n"

	convey.Convey("test editTraceAPMConfig", t, func() {
		convey.Convey("failed to open apmConfig file to read", func() {
			patch := gomonkey.ApplyFunc(os.Open,
				func(name string) (*os.File, error) {
					return nil, errors.New("failed to open")
				})
			defer patch.Reset()
			err := EditTraceConfig("127.0.0.1", tmpTraceConfig)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to open")
		})

		convey.Convey("failed to open apmConfig file to write", func() {
			patch := gomonkey.ApplyFunc(os.OpenFile,
				func(name string, flag int, perm os.FileMode) (*os.File, error) {
					return nil, errors.New("failed to openFile")
				})
			defer patch.Reset()
			err := EditTraceConfig("127.0.0.1", tmpTraceConfig)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to openFile")
		})

		convey.Convey("failed to WriteString to apmConfig", func() {
			os.WriteFile(tmpTraceConfig, []byte(Content), 0666)
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&bufio.Writer{}), "WriteString",
				func(_ *bufio.Writer, s string) (int, error) {
					return 0, errors.New("failed to WriteString")
				})
			defer patch.Reset()
			err := EditTraceConfig("127.0.0.1", tmpTraceConfig)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to WriteString")
		})

		convey.Convey("failed to Flush", func() {
			os.WriteFile(tmpTraceConfig, []byte(Content), 0666)
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&bufio.Writer{}), "Flush",
				func(_ *bufio.Writer) error {
					return errors.New("failed to Flush")
				})
			defer patch.Reset()
			err := EditTraceConfig("127.0.0.1", tmpTraceConfig)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to Flush")
		})

		convey.Convey("succeeded to editTraceAPMConfig", func() {
			os.WriteFile(tmpTraceConfig, []byte(Content), 0666)
			err := EditTraceConfig("127.0.0.1", tmpTraceConfig)
			convey.So(err, convey.ShouldBeNil)
			expectResult := "log.path=/tmp" + "\n"
			expectResult += "log.level=info" + "\n"
			expectResult += "master.address=" + "127.0.0.1" + "\n"
			data, _ := ioutil.ReadFile(tmpTraceConfig)
			actualString := string(data)
			convey.So(actualString, convey.ShouldEqual, expectResult)
		})
		convey.Convey("exist log.path editTraceAPMConfig", func() {
			content := "log.level=info" + "\n"
			content += "log.path=/tmp" + "\n"
			content += "#master.address={{master_address}}" + "\n"
			os.WriteFile(tmpTraceConfig, []byte(content), 0666)
			err := EditTraceConfig("127.0.0.1", tmpTraceConfig)
			convey.So(err, convey.ShouldBeNil)
			expectResult := "log.level=info" + "\n"
			expectResult += "log.path=/tmp" + "\n"
			expectResult += "master.address=" + "127.0.0.1" + "\n"
			data, _ := ioutil.ReadFile(tmpTraceConfig)
			actualString := string(data)
			convey.So(actualString, convey.ShouldEqual, expectResult)
		})
	})
}
