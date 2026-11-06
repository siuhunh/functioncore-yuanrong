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

package communication

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"functioncore/pkg/common/tls"
	"golang.org/x/net/context"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/core-sdk/common"
	configs "functioncore/pkg/core-sdk/config"
	"functioncore/pkg/core-sdk/types"
)

func TestCreateStateByHttp(t *testing.T) {
	convey.Convey("TestCreateStateByHttp", t, func() {
		convey.Convey("fail: no such host", func() {
			_, _, err := CreateStateByHTTP("func", "0",
				types.CreateStateConfig{TenantID: "tenantid", ProxyFunction: "functiontask"})
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("succeed", func() {
			go startServer1("8080")
			time.Sleep(3 * time.Second)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8080"),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			_, _, err := CreateStateByHTTP("func", "0",
				types.CreateStateConfig{TenantID: "tenantid", ProxyFunction: "functiontask"})
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("fail", func() {
			go startServer2("8083")
			time.Sleep(3 * time.Second)

			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8083"),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			_, _, err := CreateStateByHTTP("func", "0",
				types.CreateStateConfig{TenantID: "tenantid", ProxyFunction: "functiontask"})
			convey.So(err.Error(), convey.ShouldEqual, "http response status is not ok")
		})
		convey.Convey("make post request failed", func() {
			configs.FConfigs = &configs.Configs{
				HTTPSConfig: &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
				},
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(http.NewRequestWithContext, func(ctx context.Context, method, url string, body io.Reader) (*http.Request, error) {
					return &http.Request{}, fmt.Errorf("Test error")
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			_, _, err := CreateStateByHTTP("func", "0",
				types.CreateStateConfig{TenantID: "tenantid", ProxyFunction: "functiontask"})
			convey.So(err.Error(), convey.ShouldEqual, "Test error")
		})
	})
}

func TestDeleteStateByHttp(t *testing.T) {
	convey.Convey("TestDeleteStateByHttp", t, func() {
		convey.Convey("fail: no such host", func() {
			err := DeleteStateByHTTP("stateid", "tenantid", "functiontask")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("succeed", func() {
			go startServer1("8081")
			time.Sleep(3 * time.Second)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8081"),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			err := DeleteStateByHTTP("stateid", "tenantid", "functiontask")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("fail", func() {
			go startServer2("8084")
			time.Sleep(3 * time.Second)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8084"),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			err := DeleteStateByHTTP("stateid", "tenantid", "functiontask")
			convey.So(err.Error(), convey.ShouldEqual, "http response status is not ok")
		})
	})
}

func startServer1(port string) {
	router := gin.New()
	router.GET("/createstate", func(context *gin.Context) {
		createResponseSucceed(context.Writer, context.Request)
	})
	router.GET("/deletestate", func(context *gin.Context) {
		deleteResponseSucceed(context.Writer, context.Request)
	})

	fmt.Println("ListenAndServe1")
	httpServer := &http.Server{
		Handler: router,
		Addr:    fmt.Sprintf("127.0.0.1:%s", port),
	}
	err := httpServer.ListenAndServe()
	if err != nil {
		fmt.Println("start server err: ", err.Error())
		return
	}
}

func startServer2(port string) {
	router := gin.New()
	router.GET("/createstate", func(context *gin.Context) {
		createResponseFail(context.Writer, context.Request)
	})
	router.GET("/deletestate", func(context *gin.Context) {
		deleteResponseFail(context.Writer, context.Request)
	})

	fmt.Println("ListenAndServe2")
	httpServer := &http.Server{
		Handler: router,
		Addr:    fmt.Sprintf("127.0.0.1:%s", port),
	}
	err := httpServer.ListenAndServe()
	if err != nil {
		fmt.Println("start server err: ", err.Error())
		return
	}
}

func startServer3(port string) {
	router := gin.New()
	router.POST("/applyStateKey", func(context *gin.Context) {
		getBridgeFuncFail(context.Writer, context.Request)
	})
	router.POST("/release", func(context *gin.Context) {
		releaseBridgeFail(context.Writer, context.Request)
	})

	fmt.Println("ListenAndServe3")
	httpServer := &http.Server{
		Handler: router,
		Addr:    fmt.Sprintf("127.0.0.1:%s", port),
	}
	err := httpServer.ListenAndServe()
	if err != nil {
		fmt.Println("start server err: ", err.Error())
		return
	}
}

func startServer4(port string) {
	router := gin.New()
	router.POST("/applyStateKey", func(context *gin.Context) {
		getBridgeFuncSucceed(context.Writer, context.Request)
	})
	router.POST("/release", func(context *gin.Context) {
		releaseBridgeSucceed(context.Writer, context.Request)
	})

	fmt.Println("ListenAndServe4")
	httpServer := &http.Server{
		Handler: router,
		Addr:    fmt.Sprintf("127.0.0.1:%s", port),
	}
	err := httpServer.ListenAndServe()
	if err != nil {
		fmt.Println("start server err: ", err.Error())
		return
	}
}

func createResponseSucceed(w http.ResponseWriter, r *http.Request) {
	fmt.Println("createStateResponseSucceed")
	body := CreateStateResponse{
		Code:    200,
		StateID: "stateid",
		NodeID:  "nodeid",
		Err:     "",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func deleteResponseSucceed(w http.ResponseWriter, r *http.Request) {
	fmt.Println("deleteStateResponseSucceed")
	body := DeleteStateResponse{
		Code: 200,
		Err:  "",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func deleteResponseFail(w http.ResponseWriter, r *http.Request) {
	fmt.Println("createStateResponseFail")
	body := snerror.BadResponse{
		Code:    500,
		Message: "error",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write(data)
}

func createResponseFail(w http.ResponseWriter, r *http.Request) {
	fmt.Println("createStateResponseFail")
	body := snerror.BadResponse{
		Code:    500,
		Message: "error",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write(data)
}

func releaseBridgeFail(w http.ResponseWriter, r *http.Request) {
	fmt.Println("releaseBridgeFail")
	body := snerror.BadResponse{
		Code:    500,
		Message: "error",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write(data)
}

func releaseBridgeSucceed(w http.ResponseWriter, r *http.Request) {
	fmt.Println("releaseBridgeSucceed")
	body := DeleteStateResponse{
		Code: 200,
		Err:  "",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func getBridgeFuncSucceed(w http.ResponseWriter, r *http.Request) {
	fmt.Println("releaseBridgeSucceed")
	body := DeleteStateResponse{
		Code: 200,
		Err:  "",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func getBridgeFuncFail(w http.ResponseWriter, r *http.Request) {
	fmt.Println("getBridgeFuncFail")
	body := snerror.BadResponse{
		Code:    500,
		Message: "error",
	}
	data, _ := json.Marshal(body)
	w.WriteHeader(http.StatusInternalServerError)
	w.Write(data)
}

func TestGetBridgeFuncByHTTP(t *testing.T) {
	convey.Convey("TestGetBridgeFuncByHTTP", t, func() {
		convey.Convey("fail: no such host", func() {
			_, err := ApplyStateKeyByHTTP("func", bridgetypes.ClientInstance{}, "12345678987654321")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("succeed", func() {
			go startServer4("8080")
			time.Sleep(3 * time.Second)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8080"),
				gomonkey.ApplyGlobalVar(configs.FConfigs.HTTPSConfig.HTTPSEnable, false),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			_, err := ApplyStateKeyByHTTP("func", bridgetypes.ClientInstance{PodName: "111", FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID: "111", ServiceID: "0@base", BridgeFuncName: "test123"}}, "12345678987654321")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("fail", func() {
			go startServer3("8083")
			time.Sleep(3 * time.Second)

			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8083"),
				gomonkey.ApplyGlobalVar(configs.FConfigs.HTTPSConfig.HTTPSEnable, false),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			_, err := ApplyStateKeyByHTTP("func", bridgetypes.ClientInstance{PodName: "111", FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID: "111", ServiceID: "0@base", BridgeFuncName: "test123"}}, "12345678987654321")
			convey.So(err.Error(), convey.ShouldEqual, "http response status is not ok")
		})
	})
}

func TestReleaseBridgeByHTTP(t *testing.T) {
	convey.Convey("TestGetBridgeFuncByHTTP", t, func() {
		convey.Convey("fail: no such host", func() {
			_, err := ReleaseByHTTP("func", bridgetypes.ClientInstance{PodName: "111", FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID: "111", ServiceID: "0@base", BridgeFuncName: "test123"}}, "12345678987654321")
			gomonkey.ApplyGlobalVar(configs.FConfigs.HTTPSConfig.HTTPSEnable, false)
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		convey.Convey("succeed", func() {
			go startServer1("8080")
			time.Sleep(3 * time.Second)
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8080"),
				gomonkey.ApplyGlobalVar(configs.FConfigs.HTTPSConfig.HTTPSEnable, false),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			_, err := ReleaseByHTTP("func", bridgetypes.ClientInstance{PodName: "111", FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID: "111", ServiceID: "0@base", BridgeFuncName: "test123"}}, "12345678987654321")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("fail", func() {
			go startServer2("8083")
			time.Sleep(3 * time.Second)

			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyGlobalVar(&addr, "127.0.0.1:8083"),
				gomonkey.ApplyGlobalVar(configs.FConfigs.HTTPSConfig.HTTPSEnable, false),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			_, err := ReleaseByHTTP("func", bridgetypes.ClientInstance{PodName: "111", FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID: "111", ServiceID: "0@base", BridgeFuncName: "test123"}}, "12345678987654321")
			convey.So(err.Error(), convey.ShouldEqual, "http response status is not ok")
		})
	})
}

func TestGetErrorResponse(t *testing.T) {
	convey.Convey("TestGetErrorResponse", t, func() {
		resp := &http.Response{}
		convey.Convey("resp.body is nil", func() {
			_, err := getErrorResponse(resp)
			convey.So(err.Error(), convey.ShouldEqual, "http response status is not ok")
		})
		convey.Convey("succeed get resp", func() {
			resp := &http.Response{}
			body := snerror.BadResponse{Code: common.InternalErrCode}
			testBytes, err := json.Marshal(body)
			if err != nil {
				return
			}
			resp.Body = ioutil.NopCloser(bytes.NewReader(testBytes))
			result, err := getErrorResponse(resp)
			convey.So(err, convey.ShouldBeNil)
			convey.So(result.Code, convey.ShouldEqual, common.InternalErrCode)
		})

		convey.Convey("ioutil.ReadAll error", func() {
			resp := &http.Response{}
			resp.Body = nil

			patch := gomonkey.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return nil, errors.New("test")
			})
			defer patch.Reset()

			_, err := getErrorResponse(resp)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("Unmarshal error", func() {
			resp := &http.Response{}
			body := snerror.BadResponse{Code: common.InternalErrCode}
			testBytes, err := json.Marshal(body)
			if err != nil {
				return
			}
			resp.Body = ioutil.NopCloser(bytes.NewReader(testBytes))

			patch := gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
				return errors.New("test")
			})
			defer patch.Reset()

			_, err = getErrorResponse(resp)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestRetryDoHttpRequest(t *testing.T) {
	convey.Convey("TestRetryDoHttpRequest", t, func() {
		request := requestParam{
			Ctx:  context.Background(),
			Body: []byte("test body"),
			Path: "path",
		}
		convey.Convey("http client is nil", func() {
			resp, err := retryDoHTTPRequest(request, "traceID")
			convey.So(resp, convey.ShouldEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, common.HTTPClientErrMsg)
		})
		convey.Convey("http request with retry error(EOF)", func() {
			retryCount := 0
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "Do",
					func(*http.Client, *http.Request) (*http.Response, error) {
						if retryCount == 4 {
							return &http.Response{}, nil
						}
						retryCount++
						return nil, errors.New("error: EOF")
					}),
				gomonkey.ApplyFunc(makePostRequest, func(reqParam requestParam) (*http.Request, error) {
					if reqParam.Path == "path" {
						return &http.Request{}, nil
					}
					return &http.Request{}, errors.New("error")
				}),
				gomonkey.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			resp, err := retryDoHTTPRequest(request, "traceID")
			convey.So(resp, convey.ShouldNotEqual, nil)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("http request with retry error(address error)", func() {
			retryCount := 0
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "Do",
					func(*http.Client, *http.Request) (*http.Response, error) {
						if retryCount == 4 {
							return &http.Response{}, nil
						}
						retryCount++
						return nil, errors.New("error: cannot assign requested address")
					}),
				gomonkey.ApplyFunc(makePostRequest, func(reqParam requestParam) (*http.Request, error) {
					if reqParam.Path == "path" {
						return &http.Request{}, nil
					}
					return &http.Request{}, errors.New("error")
				}),
				gomonkey.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			resp, err := retryDoHTTPRequest(request, "traceID")
			convey.So(resp, convey.ShouldNotEqual, nil)
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("http request with no retry error", func() {
			retryCount := 0
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(httpClient), "Do",
					func(*http.Client, *http.Request) (*http.Response, error) {
						if retryCount == 0 {
							retryCount++
							return nil, errors.New("error: cannot assign requested address")
						}
						return nil, errors.New("error: no retry")
					}),
				gomonkey.ApplyFunc(makePostRequest, func(reqParam requestParam) (*http.Request, error) {
					if reqParam.Path == "path" {
						return &http.Request{}, nil
					}
					return &http.Request{}, errors.New("error")
				}),
				gomonkey.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			resp, err := retryDoHTTPRequest(request, "traceID")
			convey.So(resp, convey.ShouldEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, "error: no retry")
			convey.So(retryCount, convey.ShouldEqual, 1)
		})
	})
}
