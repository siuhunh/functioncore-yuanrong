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

package autoscaler

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
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/autoscaler/action"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxy/workerbroker"
	"functioncore/pkg/functionbus/proxyerrors"
	"functioncore/pkg/functionbus/servicediscovery"
	"functioncore/pkg/functionbus/utils"
	testutils "functioncore/test/utils"
)

const (
	workerID            = "defaultaz-#-pool24-600-512-fusion-575dd88b5b-gw422"
	funcKeyWithResource = "7e186a/0@base@testresourcepython36768/latest/800-768"
)

var (
	patches testutils.PatchSlice
	rspCode int
)

func mockClientDo(_ *http.Client, _ *http.Request) (*http.Response, error) {
	rsp := action.DeleteWorkerResponse{
		Code: http.StatusOK,
	}

	body, err := json.Marshal(rsp)
	if err != nil {
		log.GetLogger().Errorf("Marshal failed")
		return nil, err
	}
	return &http.Response{
		StatusCode: rspCode,
		Body:       ioutil.NopCloser(bytes.NewReader(body)),
	}, nil
}

func scaleDownSetup() {
	patches = testutils.InitPatchSlice()

	client := &http.Client{}
	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyFunc(utils.GetWorkerManagerClient, func() *http.Client {
			return client
		}),
	})

	patches.Append(testutils.PatchSlice{
		gomonkey.ApplyMethod(reflect.TypeOf(client), "Do", mockClientDo),
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

func scaleDownTearDown() {
	patches.ResetAll()
}

func TestStateScaleDownWithOkRsp(t *testing.T) {
	convey.Convey("TestStateScaleDownWithOkRsp", t, func() {
		rspCode = http.StatusOK
		scaleDownSetup()
		defer scaleDownTearDown()

		scaler := NewStateAutoScaler(funcKeyWithResource, servicediscovery.FunctionMetaInfo{})
		convey.So(scaler.ScaleDown(workerID, "test"), convey.ShouldEqual, true)
	})
}

func TestStateScaleDownWithNotOkRsp(t *testing.T) {
	convey.Convey("TestStateScaleDownWithNotOkRsp", t, func() {
		rspCode = http.StatusInternalServerError
		scaleDownSetup()
		defer scaleDownTearDown()

		scaler := NewStateAutoScaler(funcKeyWithResource, servicediscovery.FunctionMetaInfo{})
		convey.So(scaler.ScaleDown(workerID, "test"), convey.ShouldEqual, false)
	})
}

func TestScaleUpFail(t *testing.T) {
	request := &types.DispatcherRequest{
		TraceID:     "TraceID",
		NodeLabel:   "NodeLabel",
		FunctionKey: "FunctionKey",
		Request:     &types.TCPRequest{StateID: "StateID"},
		Response:    make(chan *types.TCPResponse),
	}
	convey.Convey("TestScaleUpFail", t, func() {
		scaler := NewStateAutoScaler(funcKeyWithResource, servicediscovery.FunctionMetaInfo{})
		patch := gomonkey.ApplyFunc(utils.GetResourceFromFuncKeyWithResource, func(funcKeyWithResource string) *types.InstanceResource {
			return &types.InstanceResource{CPU: "300", Memory: "128"}
		})
		patch.ApplyFunc(action.ScaleUp, func(scaleUpParam common.ScaleUpParam) (*types.Instance, uint32, string) {
			return nil, proxyerrors.ScaleUpRequestErrCode, "errorMsg"
		})
		patch.ApplyFunc((*stateAutoScaler).responseError, func(_ *stateAutoScaler, req *types.DispatcherRequest, response *types.TCPResponse) {
			return
		})
		defer patch.Reset()
		_, err := scaler.ScaleUp(request)
		convey.ShouldNotBeNil(err)
		convey.ShouldEqual(err.Error(), "errorMsg")
	})

	convey.Convey("TestScaleUpFailWithUserError", t, func() {
		scaler := NewStateAutoScaler(funcKeyWithResource, servicediscovery.FunctionMetaInfo{})
		patch := gomonkey.ApplyFunc(utils.GetResourceFromFuncKeyWithResource, func(funcKeyWithResource string) *types.InstanceResource {
			return &types.InstanceResource{CPU: "300", Memory: "128"}
		})
		patch.ApplyFunc(action.ScaleUp, func(scaleUpParam common.ScaleUpParam) (*types.Instance, uint32, string) {
			return nil, 4001, "function entry exception"
		})
		patch.ApplyFunc((*stateAutoScaler).responseError, func(_ *stateAutoScaler, req *types.DispatcherRequest, response *types.TCPResponse) {
			return
		})
		defer patch.Reset()
		_, err := scaler.ScaleUp(request)
		userErr, ok := err.(snerror.SNError)

		convey.ShouldBeTrue(ok)
		convey.ShouldNotBeNil(userErr)
		convey.ShouldEqual(userErr.Code(), 4001)
		convey.ShouldEqual(err.Error(), "function entry exception")
	})
}

func Test_stateAutoScaler_workerScaleUp(t *testing.T) {
	type fields struct {
		functionKeyWithResource string
		scaleTimeout            time.Duration
		Mutex                   sync.Mutex
	}
	type args struct {
		instance *types.Instance
		stateID  string
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		internalIp   string
		NotifyRemote func(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error
		want         string
		wantErr      bool
		patchesFunc  testutils.PatchesFunc
	}{
		{
			"test",
			fields{},
			args{
				instance: &types.Instance{
					DeployedIP: "111",
				},
				stateID: "stateID",
			},
			"111",
			func(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error {
				return nil
			},
			"",
			false,
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&workerbroker.Manager{}), "CreateWorkerBroker",
						func(_ *workerbroker.Manager, config *workerbroker.BrokerConfig) error {
							return nil
						}),
				})
				return patches
			},
		},
		{
			"test",
			fields{},
			args{
				instance: &types.Instance{
					DeployedIP: "111",
				},
				stateID: "stateID",
			},
			"111",
			func(remoteIP string, dst *common.FunctionProperty, event *common.NotifyEvent) error {
				return nil
			},
			"",
			true,
			func() testutils.PatchSlice {
				patches := testutils.InitPatchSlice()
				patches.Append(testutils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&workerbroker.Manager{}), "CreateWorkerBroker",
						func(_ *workerbroker.Manager, config *workerbroker.BrokerConfig) error {
							return errors.New("error")
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()

		tempIP := internal.SelfNodeIP
		tempNotifyRemote := common.NotifyRemote

		internal.SelfNodeIP = tt.internalIp
		common.NotifyRemote = tt.NotifyRemote

		sas := &stateAutoScaler{
			functionKeyWithResource: tt.fields.functionKeyWithResource,
			scaleTimeout:            tt.fields.scaleTimeout,
			Mutex:                   tt.fields.Mutex,
		}
		_, err := sas.workerScaleUp(tt.args.instance, tt.args.stateID)
		if (err != nil) != tt.wantErr {
			t.Errorf("worker scale up error")
		}

		common.NotifyRemote = tempNotifyRemote
		internal.SelfNodeIP = tempIP
		patches.ResetAll()
	}
}
