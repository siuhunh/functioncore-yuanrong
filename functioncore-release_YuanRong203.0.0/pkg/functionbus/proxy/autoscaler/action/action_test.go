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

package action

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/logger/log"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

type scaleTestPara struct {
	statusCode int
	body       []byte
	doRsp      error
}

var (
	scaleTestParaInstance scaleTestPara
	patches               testutils.PatchSlice
	stopCh                <-chan struct{}
	tempCh                chan struct{}
)

func buildValidScaleUpBody() []byte {
	rsp := GetWorkerSuccessResponse{
		Code: http.StatusOK,
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		log.GetLogger().Errorf("Marshal failed")
		return nil
	}
	return body
}

func buildValidScaleDownBody() []byte {
	rsp := GetWorkerSuccessResponse{
		Code: http.StatusInternalServerError,
	}
	body, err := json.Marshal(rsp)
	if err != nil {
		log.GetLogger().Errorf("Marshal failed")
		return nil
	}
	return body
}

func mockActionClientDo(_ *http.Client, _ *http.Request) (*http.Response, error) {
	body := scaleTestParaInstance.body
	return &http.Response{
		StatusCode: scaleTestParaInstance.statusCode,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, scaleTestParaInstance.doRsp
}

func actionSetup() {
	stopCh = utils.GetGlobalStopCh()
	tempCh = make(chan struct{})
	utils.SetGlobalStopCh(tempCh)

	patches = testutils.InitPatchSlice()

	client := &http.Client{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return client
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", mockActionClientDo),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerBaseURL, func() string {
			return ""
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.FillInWorkerManagerRequestHeaders, func(_ *http.Request) {
		}),
	})
}

func actionTeardown() {
	patches.ResetAll()
	scaleTestParaInstance = scaleTestPara{}
	close(tempCh)
	utils.SetGlobalStopCh(stopCh)
}

func buildScaleUpParam() common.ScaleUpParam {
	return common.ScaleUpParam{
		InsResource: &types.InstanceResource{},
	}
}

func TestActionScaleUpWithInternalServerError(t *testing.T) {
	common.FConfig.HTTPSConfig = &commontls.InternalHTTPSConfig{}
	convey.Convey("TestActionScaleUpWithInternalServerError", t, func() {
		actionSetup()
		defer actionTeardown()

		scaleTestParaInstance = scaleTestPara{
			statusCode: http.StatusInternalServerError,
			body:       []byte{1, 2, 3},
		}

		scaleUpParam := buildScaleUpParam()
		_, err, _ := ScaleUp(scaleUpParam)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestActionScaleUpWithStatusOK(t *testing.T) {
	convey.Convey("TestActionScaleUpWithStatusOK", t, func() {
		actionSetup()
		defer actionTeardown()

		scaleTestParaInstance = scaleTestPara{
			statusCode: http.StatusOK,
			body:       []byte{1, 2, 3},
		}

		scaleUpParam := buildScaleUpParam()
		_, err, _ := ScaleUp(scaleUpParam)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestActionScaleUpWithValidBody(t *testing.T) {
	convey.Convey("TestActionScaleUpWithValidBody", t, func() {
		actionSetup()
		defer actionTeardown()

		scaleTestParaInstance = scaleTestPara{
			statusCode: http.StatusOK,
			body:       buildValidScaleUpBody(),
		}

		scaleUpParam := buildScaleUpParam()
		_, err, _ := ScaleUp(scaleUpParam)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestActionScaleDownWithDoError(t *testing.T) {
	convey.Convey("TestActionScaleDownWithDoError", t, func() {
		actionSetup()
		defer actionTeardown()

		scaleTestParaInstance = scaleTestPara{
			doRsp: errors.New("do error"),
		}
		ok, err, _ := ScaleDown(ScaleDownParam{WorkerID: "1"}, "2", false)
		convey.So(ok, convey.ShouldEqual, false)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestActionScaleDownWithServerError", t, func() {
		actionSetup()
		defer actionTeardown()

		scaleTestParaInstance = scaleTestPara{
			statusCode: http.StatusInternalServerError,
			body:       []byte{1, 2, 3},
		}
		ok, err, _ := ScaleDown(ScaleDownParam{WorkerID: "1"}, "2", false)
		convey.So(ok, convey.ShouldEqual, true)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestActionScaleDownWithRespError", t, func() {
		actionSetup()
		defer actionTeardown()

		scaleTestParaInstance = scaleTestPara{
			statusCode: http.StatusOK,
			body:       buildValidScaleDownBody(),
		}
		ok, err, _ := ScaleDown(ScaleDownParam{WorkerID: "1"}, "2", false)
		convey.So(ok, convey.ShouldEqual, true)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("TestActionScaleDownWithRetry", t, func() {
		actionSetup()
		defer actionTeardown()

		checkExist := func() (exist bool) {
			exist = false
			instances.Range(func(k, v interface{}) bool {
				exist = true
				return false
			})
			return
		}
		c := make(chan time.Time)
		patches.Append(testutils.PatchSlice{
			gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
				return &time.Ticker{C: c}
			})})
		once = sync.Once{}
		addToRetryList("1", "2", false)
		convey.So(checkExist(), convey.ShouldEqual, true)
		scaleTestParaInstance = scaleTestPara{
			statusCode: http.StatusOK,
			body:       buildValidScaleDownBody(),
		}
		c <- time.Now()
		time.Sleep(50 * time.Millisecond)
		convey.So(checkExist(), convey.ShouldEqual, false)
	})
}

func TestWrapScaleUpAdaptor(t *testing.T) {
	convey.Convey("TestWrapScaleUpAdaptor", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(tracer.EnableCommonTracer, func() bool {
				return true
			}),
			gomonkey.ApplyFunc(ScaleUp, func(_ common.ScaleUpParam) (*types.Instance, uint32, string) {
				return nil, 0, ""
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		param := common.ScaleUpParam{
			InsResource: &types.InstanceResource{},
		}
		_, num, msg := WrapScaleUpAdaptor(param)
		convey.So(0, convey.ShouldEqual, num)
		convey.So("", convey.ShouldEqual, msg)
	})
}
