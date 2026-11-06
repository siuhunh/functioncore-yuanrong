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

// iam -
package iam

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
)

func Test_InitHTTPClient(t *testing.T) {
	convey.Convey("Test InitHTTPClient, get config error", t, func() {
		patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return nil, errors.New("error")
		})
		defer patch.Reset()
		if err := InitHTTPClient(); err == nil {
			t.Errorf("this should error")
		}
	})

	convey.Convey("Test InitHTTPClient, HTTPSEnable", t, func() {
		patch := gomonkey.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		defer patch.Reset()
		var err error
		if err = InitHTTPClient(); err != nil {
			t.Errorf("this should not error")
		}
	})
}

func Test_makeRequest(t *testing.T) {
	t.Run("NewRequest err", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(http.NewRequest, func(method, url string, body io.Reader) (*http.Request, error) {
			return nil, errors.New("test error")
		})
		defer patch.Reset()
		_, err := makeRequest("tenantID")
		assert.Equal(t, "test error", err.Error())
	})
	t.Run("get config error", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return nil, errors.New("test config error")
		})
		defer patch.Reset()
		_, err := makeRequest("tenantID")
		assert.Equal(t, "test config error", err.Error())
	})
	t.Run("makeRequest success", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		patch.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", time.Now().String()
		})
		defer patch.Reset()
		request, err := makeRequest("tenantID")
		assert.Equal(t, nil, err)
		assert.Equal(t, "tenantID", request.Header.Get(headerTenantIDKey))
		assert.Equal(t, "active", request.Header.Get(constants.HeaderCallType))
	})
}

func TestGetIAMTokenByHttp(t *testing.T) {
	t.Run("http client is nil", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(InitHTTPClient, func() error {
			httpClient = nil
			return nil
		})
		defer patch.Reset()
		InitHTTPClient()
		_, err := GetIAMTokenByHTTP("tenantID")
		assert.Equal(t, "iam client is nil", err.Error())
	})
	t.Run("makeRequest error", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(makeRequest, func(tenantID string) (*http.Request, error) {
			return nil, errors.New("test makeRequest error")
		})
		patch.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout})
		defer patch.Reset()
		_, err := GetIAMTokenByHTTP("tenantID")
		assert.Equal(t, "test makeRequest error", err.Error())
	})
	t.Run("send http failed", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		patch.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", time.Now().String()
		})
		patch.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout})
		patch.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
			error) {
			return nil, errors.New("send http failed")
		})
		defer patch.Reset()
		_, err := GetIAMTokenByHTTP("tenantID")
		assert.Equal(t, "send http failed", err.Error())
	})
	t.Run("http status code is nok", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		patch.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", time.Now().String()
		})
		patch.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout})
		patch.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
			error) {
			return &http.Response{StatusCode: http.StatusBadRequest}, nil
		})
		defer patch.Reset()
		_, err := GetIAMTokenByHTTP("tenantID")
		assert.Equal(t, "http response status is not ok", err.Error())
	})
	t.Run("get token from iam is empty", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		patch.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", time.Now().String()
		})
		patch.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout})
		patch.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
			error) {
			response := &http.Response{Header: make(http.Header), StatusCode: http.StatusOK}
			response.Header.Add(headerTokenKey, "")
			return response, nil
		})
		defer patch.Reset()
		_, err := GetIAMTokenByHTTP("tenantID")
		assert.Equal(t, "token is empty", err.Error())
	})
	t.Run("get token from iam is not empty, encrypt error", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		patch.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", time.Now().String()
		})
		patch.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout})
		patch.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
			error) {
			response := &http.Response{Header: make(http.Header), StatusCode: http.StatusOK}
			response.Header.Add(headerTokenKey, "testToken")
			return response, nil
		})
		patch.ApplyFunc(localauth.Encrypt, func(src string) (string, error) {
			return "", errors.New("encrypt error")
		})
		patch.ApplyFunc(utils.ClearStringMemory, func(s string) {
			return
		})
		defer patch.Reset()
		_, err := GetIAMTokenByHTTP("tenantID")
		assert.Equal(t, "encrypt error", err.Error())
	})
	t.Run("get token from iam success", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
			return &config.FaasConfig{HTTPSConfig: &tls.InternalHTTPSConfig{HTTPSEnable: true}, LocalAuth: &localauth.AuthConfig{}}, nil
		})
		patch.ApplyFunc(localauth.SignLocally, func(ak, sk, appID string, duration int) (string, string) {
			return "asdfsdffsdafasdf", time.Now().String()
		})
		patch.ApplyGlobalVar(&httpClient, &http.Client{Timeout: reqTimeout})
		patch.ApplyFunc((*http.Client).Do, func(client *http.Client, req *http.Request) (*http.Response,
			error) {
			response := &http.Response{Header: make(http.Header), StatusCode: http.StatusOK}
			response.Header.Add(headerTokenKey, "testToken")
			return response, nil
		})
		patch.ApplyFunc(localauth.Encrypt, func(src string) (string, error) {
			return "encryptedToken", nil
		})
		patch.ApplyFunc(utils.ClearStringMemory, func(s string) {
			return
		})
		defer patch.Reset()
		token, err := GetIAMTokenByHTTP("tenantID")
		assert.Equal(t, nil, err)
		assert.Equal(t, "encryptedToken", token)
	})
}

func Test_SetIAMTokenRequestHeader(t *testing.T) {
	t.Run("tenant token exist in token map", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(cache.GetFromExpiringIAMTokenMap, func(tenantID string) *cache.TokenSalt {
			return &cache.TokenSalt{
				Token: "testToken",
			}
		})
		defer patch.Reset()
		req := fhttp.AcquireRequest()
		SetIAMTokenRequestHeader(req, "tenantID", "urn")
		assert.Equal(t, "testToken", string(req.Header.Peek(constants.HeaderIAMToken)))
	})
	t.Run("tenant token not in token map, get by http failed", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(cache.GetFromExpiringIAMTokenMap, func(tenantID string) *cache.TokenSalt {
			return nil
		})
		patch.ApplyFunc(GetIAMTokenByHTTP, func(tenantID string) (string, error) {
			return "", errors.New("test error")
		})
		defer patch.Reset()
		req := fhttp.AcquireRequest()
		SetIAMTokenRequestHeader(req, "tenantID_falied", "urn")
		assert.Equal(t, "", string(req.Header.Peek(constants.HeaderIAMToken)))
	})
	t.Run("tenant token not in token map, get by http success", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(cache.GetFromExpiringIAMTokenMap, func(tenantID string) *cache.TokenSalt {
			return nil
		})
		patch.ApplyFunc(GetIAMTokenByHTTP, func(tenantID string) (string, error) {
			return "testTokenByIAM", nil
		})
		defer patch.Reset()
		req := fhttp.AcquireRequest()
		SetIAMTokenRequestHeader(req, "tenantID_success", "urn")
		assert.Equal(t, "testTokenByIAM", string(req.Header.Peek(constants.HeaderIAMToken)))
	})
	t.Run("Test setIAMTokenRequestHeader concurrently, only query iam once for the same tenantID", func(t *testing.T) {
		queryCount := 0
		var mu sync.Mutex
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(cache.GetFromExpiringIAMTokenMap, func(tenantID string) *cache.TokenSalt {
			mu.Lock()
			defer mu.Unlock()
			if queryCount >= 1 {
				return &cache.TokenSalt{
					Token: "testTokenByIAM",
				}
			}
			return nil
		})
		patch.ApplyFunc(GetIAMTokenByHTTP, func(tenantID string) (string, error) {
			mu.Lock()
			defer mu.Unlock()
			queryCount += 1
			return "testTokenByIAM", nil
		})
		defer patch.Reset()
		req := fhttp.AcquireRequest()

		times := 10
		wg := sync.WaitGroup{}
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func() {
				SetIAMTokenRequestHeader(req, "tenantID_query", "urn")
				wg.Done()
			}()
		}
		wg.Wait()
		assert.Equal(t, "testTokenByIAM", string(req.Header.Peek(constants.HeaderIAMToken)))
		assert.Equal(t, 1, queryCount)
	})
	t.Run("Test setIAMTokenRequestHeader concurrently, multiple queries for different tenantIDs", func(t *testing.T) {
		patch := gomonkey.NewPatches()
		patch.ApplyFunc(cache.GetFromExpiringIAMTokenMap, func(tenantID string) *cache.TokenSalt {
			return nil
		})
		patch.ApplyFunc(GetIAMTokenByHTTP, func(tenantID string) (string, error) {
			return "testTokenByIAM" + tenantID, nil
		})
		defer patch.Reset()
		var records [10]string
		times := 10
		wg := sync.WaitGroup{}
		wg.Add(times)
		for i := 0; i < times; i++ {
			go func(i int) {
				req := fhttp.AcquireRequest()
				SetIAMTokenRequestHeader(req, fmt.Sprintf("tenantID_%d", i), "urn")
				records[i] = string(req.Header.Peek(constants.HeaderIAMToken))
				wg.Done()
			}(i)
		}
		wg.Wait()
		for i := 0; i < times; i++ {
			println("token is " + records[i])
			assert.Equal(t, fmt.Sprintf("testTokenByIAMtenantID_%d", i), records[i])
		}
	})
}
