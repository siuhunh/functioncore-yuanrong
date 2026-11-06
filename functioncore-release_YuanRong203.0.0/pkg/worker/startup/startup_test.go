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

package startup

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	convey "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/runtime"
)

func TestGenerateErrorString(t *testing.T) {
	convey.Convey("TestGenerateErrorString", t, func() {
		convey.Convey("test false", func() {
			logs := "testLog"
			summary := runtime.Summary{}
			runtimeError := &runtime.Error{
				Code:    snerror.RuntimeBootstrapNotFound,
				Message: "test error kind false",
			}
			actualErrorString := GenerateErrorString(logs, summary.SummaryInfo, runtimeError)
			convey.So(actualErrorString, convey.ShouldEqual,
				`test error kind false`)
		})
		convey.Convey("test", func() {
			logs := "testLog"
			summary := runtime.Summary{}
			runtimeError := &runtime.Error{
				Code:    snerror.RuntimeInitTimeoutCode,
				Message: "runtime initialization timed out after 3s",
			}
			actualErrorString := GenerateErrorString(logs, summary.SummaryInfo, runtimeError)
			convey.So(actualErrorString, convey.ShouldEqual,
				`{"log":"testLog","summary":{"funcDigest":"","duration":0,"billingDuration":0,"memorySize":0,"memoryUsed":0,"podName":"","gpuMemorySize":0,"nodeIP":"","clusterID":"","storage_used":0},"errorMessage":"runtime initialization timed out after 3s"}`)
		})
	})

	convey.Convey("TestGenerateErrorString", t, func() {
		convey.Convey("test marshel false", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
					return nil, errors.New("failed to marshel json")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			logs := "testLog"
			summary := runtime.Summary{}
			runtimeError := &runtime.Error{
				Code:    snerror.RuntimeInitTimeoutCode,
				Message: "runtime initialization timed out after 3s",
			}
			actualErrorString := GenerateErrorString(logs, summary.SummaryInfo, runtimeError)
			convey.So(actualErrorString, convey.ShouldEqual,
				``)
		})
	})
}

func TestDecodeErrorString(t *testing.T) {
	convey.Convey("TestDecodeErrorString", t, func() {
		convey.Convey("test false", func() {
			s := `{"log":"testLog","summary":{"funcDigest":"","duration":0,"billingDuration":0,"memorySize":0,"memoryUsed":0,"podName":""},"errorMessage":"runtime initialization timed out after 3s"}`
			actualError := DecodeErrorString(s)
			convey.So(actualError.Message, convey.ShouldEqual, "runtime initialization timed out after 3s")
		})
	})
	convey.Convey("TestDecodeErrorString", t, func() {
		convey.Convey("test unmarshel false", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
					return errors.New("failed to unmarshel json")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			s := `{"log":"testLog","summary":{"funcDigest":"","duration":0,"billingDuration":0,"memorySize":0,"memoryUsed":0,"podName":""},"errorMessage":"runtime initialization timed out after 3s"}`
			actualError := DecodeErrorString(s)
			convey.So(actualError.Message, convey.ShouldEqual, s)
		})
	})
}
