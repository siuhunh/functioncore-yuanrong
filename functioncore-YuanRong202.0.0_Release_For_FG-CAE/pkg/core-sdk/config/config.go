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

// Package config implements get config and generate auth info
package config

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
)

const (
	// ConfigPath config.json file path
	ConfigPath = "/home/sn/config/conf.json"
)

const (
	// LocalAuthConfigEnv -
	LocalAuthConfigEnv = "LOCAL_AUTH_CONFIG"
	// HTTPSConfigEnv -
	HTTPSConfigEnv     = "HTTPS_CONFIG"
)

// FConfigs define config struct
var FConfigs *Configs

// Configs config map json
type Configs struct {
	LocalAuth   *localauth.AuthConfig    `json:"localAuth" valid:"optional"`
	HTTPSConfig *tls.InternalHTTPSConfig `json:"httpsConfig" valid:"optional"`
}

// GetConfig get config from config map json file
func GetConfig() error {
	FConfigs = &Configs{
		LocalAuth:   getLocalAuthConfig(),
		HTTPSConfig: getHTTPSConfig(),
	}
	_, err := govalidator.ValidateStruct(FConfigs)
	if err != nil {
		return err
	}
	if FConfigs.HTTPSConfig.HTTPSEnable {
		log.GetLogger().Debugf("core-sdk HTTPS Enable")
		err = tls.InitTLSConfig(FConfigs.HTTPSConfig.TLSProtocol, FConfigs.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return err
		}
	}
	return nil
}

func getHTTPSConfig() *tls.InternalHTTPSConfig {
	datas := os.Getenv(HTTPSConfigEnv)
	config := new(tls.InternalHTTPSConfig)
	if err := json.Unmarshal([]byte(datas), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal https config, err: %s", err.Error())
		return nil
	}
	return config
}

func getLocalAuthConfig() *localauth.AuthConfig {
	datas := os.Getenv(LocalAuthConfigEnv)
	config := new(localauth.AuthConfig)
	err := json.Unmarshal([]byte(datas), config)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal localAuth config, err: %s", err.Error())
		return nil
	}
	return config
}

func parseReqBody(r *http.Request) []byte {
	var err error
	body := r.Body
	var bodyByte = []byte("")
	if body != nil {
		bodyByte, err = ioutil.ReadAll(body)
		if err != nil {
			return nil
		}
		defer r.Body.Close()
	}
	// must preset body
	r.Body = ioutil.NopCloser(bytes.NewBuffer(bodyByte))
	return bodyByte
}
