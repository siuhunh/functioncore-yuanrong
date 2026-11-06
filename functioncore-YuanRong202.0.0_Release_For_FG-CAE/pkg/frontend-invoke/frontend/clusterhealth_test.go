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

package frontend

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/util"
)

func TestClusterHealth(t *testing.T) {
	convey.Convey("InitEtcdClient", t, func() {
		convey.Convey("InitClientOK", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte{}, nil
				}),
				gomonkey.ApplyFunc(clientv3.New, func(cfg clientv3.Config) (*clientv3.Client, error) {
					return &clientv3.Client{}, nil
				}),
				gomonkey.ApplyFunc(concurrency.NewSession, func(client *clientv3.Client,
					opts ...concurrency.SessionOption) (*concurrency.Session, error) {
					return &concurrency.Session{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			stopCh := make(<-chan struct{})
			_ = etcd3.InitParam().
				WithStopCh(stopCh).
				WithMetaEtcdConfig(&etcd3.ClientConfig{}).
				WithRouterEtcdConfig(&etcd3.ClientConfig{}).
				InitClient()
		})
	})
	constant.FConfig.AuthenticationEnable = true
	constant.FConfig.HTTPConfig = &constant.FrontendHTTP{
		MaxRequestBodySize: 100,
	}
	constant.FConfig.HTTPSConfig = &tls.InternalHTTPSConfig{}
	constant.FConfig.LocalAuth = &localauth.AuthConfig{}
	urnutils.Separator = "@"
	constant.NativeAz = constants.DefaultAZ
	constant.FConfig.InvokeMaxRetryTimes = 3

	patched := []*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.AuthCheckLocally, func(ak string, sk string, requestSign string, timestamp string, duration int) error {
			if timestamp == "" {
				return errors.New("no auth check info")
			}
			return nil
		}),
	}
	defer func() {
		constant.FConfig.AuthenticationEnable = false
		for i := range patched {
			patched[i].Reset()
		}
	}()

	router := gin.New()
	router.GET("/serverless/v1/componentshealth", func(context *gin.Context) {
		ClusterHealth(context.Writer, context.Request)
	})

	client := util.GetGlobalClient()

	tests := []struct {
		name            string
		proxyStatusMap  map[string]int
		headerMap       map[string]string
		timestamp       string
		exceptedCode    int
		expectResultMap map[string]string
	}{
		{
			name:            "no proxy",
			proxyStatusMap:  map[string]int{},
			timestamp:       strconv.Itoa(int(time.Now().Unix())),
			exceptedCode:    http.StatusInternalServerError,
			expectResultMap: map[string]string{task: unhealthy, instanceManager: unknown, functionAccessor: healthy},
		},
		{
			name:            "frontend lost router etcd contact",
			proxyStatusMap:  map[string]int{},
			timestamp:       strconv.Itoa(int(time.Now().Unix())),
			headerMap:       map[string]string{functionAccessor: "false"},
			exceptedCode:    http.StatusInternalServerError,
			expectResultMap: map[string]string{task: unknown, instanceManager: unknown, functionAccessor: subhealthy},
		},
		{
			name:           "proxy all ok",
			timestamp:      strconv.Itoa(int(time.Now().Unix())),
			proxyStatusMap: map[string]int{"127.0.0.1": http.StatusOK, "127.0.0.2": http.StatusOK},
			exceptedCode:   http.StatusOK,
		},
		{
			name:            "instance manager is not ok",
			timestamp:       strconv.Itoa(int(time.Now().Unix())),
			proxyStatusMap:  map[string]int{"127.0.0.1": http.StatusInternalServerError, "127.0.0.2": http.StatusInternalServerError},
			exceptedCode:    http.StatusInternalServerError,
			expectResultMap: map[string]string{task: healthy, instanceManager: unhealthy, functionAccessor: healthy},
		},
		{
			name:            "instance manager lost router etcd contact",
			timestamp:       strconv.Itoa(int(time.Now().Unix())),
			proxyStatusMap:  map[string]int{"127.0.0.1": http.StatusInternalServerError, "127.0.0.2": http.StatusInternalServerError},
			headerMap:       map[string]string{functionAccessor: "true", instanceManager: "false"},
			exceptedCode:    http.StatusInternalServerError,
			expectResultMap: map[string]string{task: healthy, instanceManager: subhealthy, functionAccessor: healthy},
		},
		{
			name:            "proxy all not ok",
			timestamp:       strconv.Itoa(int(time.Now().Unix())),
			proxyStatusMap:  map[string]int{"127.0.0.1": http.StatusBadRequest, "127.0.0.2": http.StatusBadRequest},
			exceptedCode:    http.StatusInternalServerError,
			expectResultMap: map[string]string{task: unhealthy, instanceManager: unknown, functionAccessor: healthy},
		},
		{
			name:           "auth check failed",
			proxyStatusMap: map[string]int{"127.0.0.1": http.StatusOK, "127.0.0.2": http.StatusOK},
			exceptedCode:   http.StatusUnauthorized,
		},
	}
	for _, test := range tests {
		for nodeIP, _ := range test.proxyStatusMap {
			node := &functionlb.Node{NodeIP: nodeIP}
			nodeKey := "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/" + nodeIP
			functionlb.NodeRoute.AddNode(node, nodeKey)
		}
		patch := gomonkey.ApplyMethod(reflect.TypeOf(client), "DoTimeout",
			func(_ *fasthttp.Client, req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
				nodeIP := strings.Split(string(req.Host()), ":")[0]
				if test.headerMap != nil && test.headerMap[instanceManager] == "false" {
					resp.Header.Set(constants.HeaderRouterEtcdState, "false")
				}
				resp.SetStatusCode(test.proxyStatusMap[nodeIP])
				return nil
			})
		patch.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
			return &etcd3.EtcdClient{}
		})
		patch.ApplyFunc((*etcd3.EtcdClient).GetEtcdStatusLostContact, func(_ *etcd3.EtcdClient) bool {
			if test.headerMap != nil && test.headerMap[functionAccessor] == "false" {
				return false
			}
			return true
		})
		defer func() {
			// clean functionlb.NodeRoute
			for nodeIP, _ := range test.proxyStatusMap {
				nodeKey := "/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/" + nodeIP
				functionlb.NodeRoute.RemoveNode(nodeKey)
			}
			patch.Reset()
		}()
		req, _ := http.NewRequest(http.MethodGet, "/serverless/v1/componentshealth", nil)
		req.Header.Set(constants.HeaderAuthTimestamp, test.timestamp)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		// check response status code
		assert.Equal(t, test.exceptedCode, w.Result().StatusCode, test.name)
		// cluster is healthy only response http.StatusOK without responseBody
		if test.exceptedCode == http.StatusInternalServerError {
			expectResponseBody, _ := json.Marshal(test.expectResultMap)
			actualResponseBody, _ := ioutil.ReadAll(w.Result().Body)
			assert.Equal(t, string(expectResponseBody), string(actualResponseBody), test.name)
		}
	}
}
