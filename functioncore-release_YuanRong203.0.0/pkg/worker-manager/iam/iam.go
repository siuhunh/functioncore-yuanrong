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

// Package iam -
package iam

import (
	"errors"
	"net/http"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tls"
	tools "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

const (
	workerManagerAppID  = "worker-manager"
	reqTimeout          = 6 * time.Second
	yrIAMReqURL         = "iam-server:31218/iam-server/v1/token/require"
	schemaHTTP          = "http://"
	schemaHTTPS         = "https://"
	clientErrMsg        = "iam client is nil"
	headerTenantIDKey   = "X-Tenant-ID"
	headerTokenKey      = "X-Auth"
	headerTenantSaltKey = "X-Salt"
	callTypeActive      = "active"
	singleFlightTTL     = 100 * time.Millisecond
)

var (
	httpClient *http.Client
	schema     = schemaHTTP
	sf         = singleflight.NewSingleFlight()
)

// GetIAMTokenByHTTP send a http request to YR IAM
func GetIAMTokenByHTTP(tenantID string) (string, error) {
	if httpClient == nil {
		return "", snerror.New(utils.StatusInternalServerError, clientErrMsg)
	}
	req, err := makeRequest(tenantID)
	if err != nil {
		return "", snerror.New(utils.StatusInternalServerError, err.Error())
	}
	resp, err := httpClient.Do(req)

	if err != nil {
		return "", snerror.New(utils.StatusInternalServerError, err.Error())
	}
	if resp == nil {
		return "", snerror.New(utils.StatusInternalServerError, "response is nil")
	}
	if resp.Body != nil {
		defer resp.Body.Close()
	}

	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("http response status is not ok, status code: %d", resp.StatusCode)
		return "", snerror.New(utils.StatusInternalServerError, "http response status is not ok")
	}
	token := resp.Header.Get(headerTokenKey)
	salt := resp.Header.Get(headerTenantSaltKey)

	if token == "" {
		return "", snerror.New(utils.StatusBadRequest, "token is empty")
	}

	defer tools.ClearStringMemory(token)
	encryptedToken, err := localauth.Encrypt(token)
	if err != nil {
		return "", snerror.New(utils.StatusInternalServerError, err.Error())
	}
	tokenSalt := &cache.TokenSalt{
		Token: encryptedToken,
		Salt:  salt,
	}
	err = cache.PutExpiringIAMTokenMap(tenantID, tokenSalt)
	if err != nil {
		log.GetLogger().Warnf("PutExpiringIAMTokenMap for %s failed, err: %s", tenantID, err.Error())
	} else {
		log.GetLogger().Debugf("success put token into map for tenantID: %s", tenantID)
	}
	log.GetLogger().Infof("success get iam token for tenantID: %s", tenantID)
	return encryptedToken, nil
}

func makeRequest(tenantID string) (*http.Request, error) {
	path := schema + yrIAMReqURL
	request, err := http.NewRequest("GET", path, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to make iam request for %s, err %s", tenantID, err.Error())
		return nil, err
	}
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to makeRequest while getting config, err: %s", err.Error())
		return nil, snerror.New(utils.StatusInternalServerError, err.Error())
	}

	request.Header.Set(headerTenantIDKey, tenantID)

	request.Header.Set(constants.HeaderEventSourceID, workerManagerAppID)
	request.Header.Set(constants.HeaderCallType, callTypeActive)
	request.Header.Set(constants.ContentType, constants.ApplicationJSON)
	authorization, timestamp := localauth.SignLocally(conf.LocalAuth.AKey,
		conf.LocalAuth.SKey, utils.AppID, conf.LocalAuth.Duration)
	request.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	request.Header.Set(constants.Authorization, authorization)

	return request, nil
}

// InitHTTPClient init http client
func InitHTTPClient() error {
	httpClient = &http.Client{Timeout: reqTimeout}
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to initialize worker-manager http client while getting config, err: %s", err.Error())
		return err
	}

	if conf.HTTPSConfig.HTTPSEnable {
		transport, transportOk := http.DefaultTransport.(*http.Transport)
		if !transportOk {
			log.GetLogger().Errorf("failed to assert https default transport")
			return errors.New("failed to assert https default transport")
		}
		transport.TLSClientConfig = tls.GetClientTLSConfig()
		httpClient.Transport = transport
		schema = schemaHTTPS
	}
	return nil
}

// SetIAMTokenRequestHeader set iam token request header
func SetIAMTokenRequestHeader(req *fasthttp.Request, tenantID string, urn string) {
	tokenSalt := cache.GetFromExpiringIAMTokenMap(tenantID)
	if tokenSalt != nil {
		req.Header.Set(constants.HeaderIAMToken, tokenSalt.Token)
	} else {
		token, err := sf.Do(tenantID, func() (interface{}, error) {
			token, err := GetIAMTokenByHTTP(tenantID)
			time.AfterFunc(singleFlightTTL, func() {
				sf.Remove(tenantID)
			})
			return token, err
		})
		if err != nil {
			log.GetLogger().Errorf("GetIAMTokenByHTTP error, tenantID: %s, urn: %s, err: %s",
				tenantID, urn, err.Error())
			return
		}
		req.Header.Set(constants.HeaderIAMToken, token.(string))
	}
	log.GetLogger().Debugf("success set iam token, tenant: %s, urn: %s", tenantID, urn)
}
