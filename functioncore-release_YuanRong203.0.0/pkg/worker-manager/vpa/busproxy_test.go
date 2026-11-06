/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package vpa

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/vpa"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

func TestSendFuncInfoToBus(t *testing.T) {
	patch := gomonkey.NewPatches()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
			UID:  "test-uid",
		},
		Status: corev1.PodStatus{
			HostIP: "127.0.0.1",
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name:        "worker",
					ContainerID: "docker://worker-id",
				},
				{
					Name:        "runtime",
					ContainerID: "docker://runtime-id",
				},
			},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{
			{
				Name: "worker",
				Resources: corev1.ResourceRequirements{
					Limits: map[corev1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse(fmt.Sprintf("%dm", 400)),
						"memory": resource.MustParse(fmt.Sprintf("%dMi", 256)),
					},
					Requests: map[corev1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse(fmt.Sprintf("%dm", 60)),
						"memory": resource.MustParse(fmt.Sprintf("%dMi", 100)),
					},
				},
			},
			{
				Name: "runtime",
				Resources: corev1.ResourceRequirements{
					Limits: map[corev1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse(fmt.Sprintf("%dm", 1000)),
						"memory": resource.MustParse(fmt.Sprintf("%dMi", 1024)),
					},
					Requests: map[corev1.ResourceName]resource.Quantity{
						"cpu":    resource.MustParse(fmt.Sprintf("%dm", 60)),
						"memory": resource.MustParse(fmt.Sprintf("%dMi", 100)),
					},
				},
			},
		}},
	}
	reqInfo := &config.RequestInfo{
		URN:             "test-urn",
		TraceID:         "test-trace-id",
		FunctionName:    "test-func",
		FunctionVersion: "v1",
	}
	service := &faasfunction.FunctionService{
		Spec: faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{
				FunctionName:    "test-func",
				FunctionVersion: "v1",
				FunctionURN:     "test-urn",
				Resource: faasfunction.ResourceSpec{
					CPU: 200,     // 0.2 cpu
					Mem: 2 << 28, // 256 Mi
				},
				BusinessType: "CAE",
			},
		},
	}

	poolResource := utils.PoolResource{
		Worker: utils.WorkerResource{
			RequestCPU: 100,
			RequestMem: 256,
			LimitCPU:   1000,
			LimitMem:   512,
		},
		Runtime: utils.RuntimeResource{
			RequestCPU: 100,
			RequestMem: 256,
			LimitCPU:   1000,
			LimitMem:   512,
		},
	}

	convey.Convey("test SendFuncInfoToBus", t, func() {
		convey.Convey("test vpa switch is disabled", func() {
			mockIsVpaFeatureEnabled(patch, false)
			snErr := SendFuncVpaReqToBus(constants.VPACreate, pod, reqInfo, service)
			convey.So(snErr, convey.ShouldBeNil)
		})

		convey.Convey("test get pool resource error", func() {
			service.Spec.Configuration.BusinessType = ""
			mockIsVpaFeatureEnabled(patch, true)
			mockGetPoolResource(patch, utils.PoolResource{}, snerror.New(snerror.VPAWorkerManagerError, "inner error"))
			snErr := SendFuncVpaReqToBus(constants.VPACreate, pod, reqInfo, service)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "failed to get pool resource")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPAWorkerManagerError)
		})

		convey.Convey("test do func vpa request error", func() {
			service.Spec.Configuration.BusinessType = "CAE"
			mockIsVpaFeatureEnabled(patch, true)
			mockGetPoolResource(patch, utils.PoolResource{}, nil)
			mockDoFuncVpaReq(patch, snerror.New(snerror.VPACommunicationError, "do func vpa request error"))
			snErr := SendFuncVpaReqToBus(constants.VPACreate, pod, reqInfo, service)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "failed deal to communicate with busproxy")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test do func vpa request success", func() {
			service.Spec.Configuration.BusinessType = ""
			mockIsVpaFeatureEnabled(patch, true)
			mockGetPoolResource(patch, poolResource, nil)
			mockDoFuncVpaReq(patch, nil)
			snErr := SendFuncVpaReqToBus(constants.VPACreate, pod, reqInfo, service)
			convey.So(snErr, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestClient_dealFuncVpaReq(t *testing.T) {
	patch := gomonkey.NewPatches()
	req := newHTTPRequest(WithRequestTimeout(2 * time.Second))
	requestInfo := &vpa.FuncInstanceInfo{
		Action: constants.VPACreate,
		Resource: &vpa.ResourceConfig{
			CPULimit:      0,
			CPURequest:    0,
			MemoryLimit:   0,
			MemoryRequest: 0,
		},
	}
	traceId := "test-123456"
	service := &faasfunction.FunctionService{
		Spec: faasfunction.FunctionServiceSpec{
			Configuration: faasfunction.ConfigurationSpec{
				FunctionName:    "test-func",
				FunctionVersion: "v1",
				FunctionURN:     "test-urn",
				Resource: faasfunction.ResourceSpec{
					CPU: 200,     // 0.2 cpu
					Mem: 2 << 28, // 256 Mi
				},
				BusinessType: "CAE",
			},
		},
	}

	convey.Convey("test doFuncVpaReq", t, func() {
		convey.Convey("test do func vpa request error in create phase", func() {
			mockDoFuncVpaReq(patch, snerror.New(snerror.VPACommunicationError, "do func vpa request error"))
			snErr := req.dealFuncVpaReq(requestInfo, service, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "failed deal to communicate with busproxy")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test do func vpa success in create phase", func() {
			mockDoFuncVpaReq(patch, nil)
			snErr := req.dealFuncVpaReq(requestInfo, service, traceId)
			convey.So(snErr, convey.ShouldBeNil)
		})

		convey.Convey("test do func vpa success in ready phase", func() {
			requestInfo.Action = constants.VPAReady
			mockDoFuncVpaReq(patch, nil)
			snErr := req.dealFuncVpaReq(requestInfo, service, traceId)
			convey.So(snErr, convey.ShouldBeNil)
		})

		convey.Convey("test do func vpa success in ready phase with no retry", func() {
			requestInfo.Action = constants.VPAReady
			flag := false
			patch.ApplyPrivateMethod(reflect.TypeOf(&Client{}), "doFuncVpaReq",
				func(requestInfo *vpa.FuncInstanceInfo, traceId string) snerror.SNError {
					flag = true
					return nil
				})

			req.retryInformerBusProxyUntilSuccess(requestInfo, service, traceId)
			convey.So(flag, convey.ShouldBeTrue)
		})

		convey.Convey("test do func vpa success in ready phase after several times retry", func() {
			requestInfo.Action = constants.VPAReady
			executeTimes := 0
			patch.ApplyPrivateMethod(reflect.TypeOf(&Client{}), "doFuncVpaReq",
				func(requestInfo *vpa.FuncInstanceInfo, traceId string) snerror.SNError {
					executeTimes++
					return snerror.New(snerror.VPACommunicationError, "do func vpa request error")
				})

			mockTimeSleep(patch)
			req.retryInformerBusProxyUntilSuccess(requestInfo, service, traceId)
			convey.So(executeTimes, convey.ShouldBeGreaterThanOrEqualTo, 3)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func TestClient_doRequest(t *testing.T) {
	patch := gomonkey.NewPatches()
	req := newHTTPRequest(WithRequestTimeout(5 * time.Second))
	requestInfo := &vpa.FuncInstanceInfo{}
	traceId := "test-123456"

	convey.Convey("test doFuncVpaReq", t, func() {
		convey.Convey("test do func vpa request error", func() {
			mockJsonMarshal(patch, nil, errors.New("do func vpa request error"))
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "do func vpa request error")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPAWorkerManagerError)
		})

		convey.Convey("test new request with context error", func() {
			mockNewRequestWithContext(patch, nil, errors.New("action failed when make vpa req"))
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "action failed when make vpa req")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPAWorkerManagerError)
		})

		convey.Convey("test send client do error", func() {
			mockAuthSignLocally(patch, "", "")
			mockClientDo(patch, &http.Response{}, errors.New("do func vpa request error"))
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "do func vpa request error")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test send client do error with auth config is nil", func() {
			mockAuthConfig(patch, nil)
			mockClientDo(patch, &http.Response{}, errors.New("do func vpa request error"))
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "do func vpa request error")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test send client do error with auth config is not nil", func() {
			mockAuthConfig(patch, &localauth.AuthConfig{})
			mockAuthSignLocally(patch, "", "")
			mockClientDo(patch, &http.Response{Body: http.NoBody}, errors.New("do func vpa request error"))
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "do func vpa request error")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test read resp body error", func() {
			mockAuthSignLocally(patch, "", "")
			mockClientDo(patch, &http.Response{Body: http.NoBody}, nil)
			mockIOReadAll(patch, nil, errors.New("read resp body error"))
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "read resp body error")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test resp return internal error and unmarshal resp body failed", func() {
			mockAuthConfig(patch, &localauth.AuthConfig{})
			mockAuthSignLocally(patch, "", "")
			mockClientDo(patch, &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body:       io.NopCloser(bytes.NewReader([]byte("{\"slo-agent client error\"}"))),
			}, nil)
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test resp return internal error and unmarshal resp body success", func() {
			mockAuthConfig(patch, &localauth.AuthConfig{})
			mockAuthSignLocally(patch, "", "")
			mockClientDo(patch, &http.Response{
				StatusCode: http.StatusInternalServerError,
				Body: io.NopCloser(bytes.NewReader(
					[]byte("{\"code\":150602,\"message\":\"slo-agent client error\"}"))),
			}, nil)

			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "slo-agent client error")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test resp return 40X error", func() {
			mockAuthConfig(patch, &localauth.AuthConfig{})
			mockAuthSignLocally(patch, "", "")
			mockClientDo(patch, &http.Response{
				StatusCode: http.StatusNotFound,
				Body:       io.NopCloser(bytes.NewReader([]byte("{\"message\":\"the slo-agent req not found\"}"))),
			}, nil)

			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldNotBeNil)
			convey.So(snErr.Error(), convey.ShouldEqual, "failed to receive bus proxy response")
			convey.So(snErr.Code(), convey.ShouldEqual, snerror.VPACommunicationError)
		})

		convey.Convey("test client do success", func() {
			mockAuthConfig(patch, &localauth.AuthConfig{})
			mockAuthSignLocally(patch, "", "")
			mockClientDo(patch, &http.Response{
				StatusCode: http.StatusOK,
				Body:       http.NoBody,
			}, nil)
			snErr := req.doFuncVpaReq(requestInfo, traceId)
			convey.So(snErr, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_isFuncNameContainFuncVersion(t *testing.T) {
	convey.Convey("test isFuncNameContainFuncVersion", t, func() {
		convey.Convey("test func name without version", func() {
			res := isFuncNameContainFuncVersion("0@default@test-vpa")
			convey.So(res, convey.ShouldBeFalse)
		})

		convey.Convey("test func name with version", func() {
			res := isFuncNameContainFuncVersion("0@default@test-vpa:latest")
			convey.So(res, convey.ShouldBeTrue)
		})
	})
}

func mockDoFuncVpaReq(patch *gomonkey.Patches, err snerror.SNError) {
	patch.ApplyPrivateMethod(reflect.TypeOf(&Client{}), "doFuncVpaReq",
		func(requestInfo *vpa.FuncInstanceInfo, traceId string) snerror.SNError {
			return err
		})
}

func mockJsonMarshal(patch *gomonkey.Patches, b []byte, err error) {
	patch.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
		return b, err
	})
}

func mockNewRequestWithContext(patch *gomonkey.Patches, req *http.Request, err error) {
	patch.ApplyFunc(http.NewRequestWithContext, func(ctx context.Context, method, url string, body io.Reader) (
		*http.Request, error) {
		return req, err
	})
}

func mockAuthSignLocally(patch *gomonkey.Patches, auth, timestamp string) {
	patch.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
		return auth, timestamp
	})
}

func mockClientDo(patch *gomonkey.Patches, resp *http.Response, err error) {
	patch.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (
		*http.Response, error) {
		return resp, err
	})
}

func mockIOReadAll(patch *gomonkey.Patches, body []byte, err error) {
	patch.ApplyFunc(io.ReadAll, func(r io.Reader) ([]byte, error) {
		return body, err
	})
}

func mockAuthConfig(patch *gomonkey.Patches, authConfig *localauth.AuthConfig) {
	patch.ApplyFunc(config.GetLocalAuth, func() *localauth.AuthConfig {
		return authConfig
	})
}

func mockTimeSleep(patch *gomonkey.Patches) {
	patch.ApplyFunc(time.Sleep, func(_ time.Duration) {})
}

func mockGetPoolResource(patch *gomonkey.Patches, poolResource utils.PoolResource, err error) {
	patch.ApplyFunc(utils.GetPoolResource, func(limitCPU int64, limitMemory int64,
		businessType string) (utils.PoolResource, error) {
		return poolResource, err
	})
}
