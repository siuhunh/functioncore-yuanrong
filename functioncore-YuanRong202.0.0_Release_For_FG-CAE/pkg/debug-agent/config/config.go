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

// Package config contains configuration of debug agent
package config

import (
	"encoding/json"
	"errors"
	"os"
	"sync"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
)

// DebugAgentConfig is total config of debug agent
type DebugAgentConfig struct {
	ServerConfig         `json:"serverConfig" valid:"required"`
	localauth.AuthConfig `json:"localAuth" valid:"optional"`
}

// ServerConfig is config of http server
type ServerConfig struct {
	HTTPSConfig    tls.InternalHTTPSConfig `json:"httpsConfig" valid:"optional"`
	Port           int32                   `json:"port" valid:"required"`
	ReadTimeout    int64                   `json:"readTimeout" valid:"optional"`
	WriteTimeout   int64                   `json:"writeTimeout" valid:"optional"`
	MaxHeaderBytes int                     `json:"maxHeaderBytes" valid:"optional"`
}

var (
	configSingleton = struct {
		sync.Once
		config *DebugAgentConfig
	}{}
)

const (
	configEnvKey = "DEBUG_AGENT_CONFIG"
)

// GetConfig gets config of debug agent
func GetConfig() (*DebugAgentConfig, error) {
	configSingleton.Do(func() {
		serializedConfig, ok := os.LookupEnv(configEnvKey)
		if !ok {
			log.GetLogger().Errorf("config key %s does not exist", configEnvKey)
			configSingleton.config = nil
			return
		}

		configSingleton.config = &DebugAgentConfig{}
		if err := json.Unmarshal([]byte(serializedConfig), configSingleton.config); err != nil {
			log.GetLogger().Errorf("failed to unmarshal config, err: %s", err.Error())
			configSingleton.config = nil
			return
		}

		_, err := govalidator.ValidateStruct(configSingleton.config)
		if err != nil {
			log.GetLogger().Errorf("config is invalid, err: %s", err.Error())
			configSingleton.config = nil
			return
		}
	})

	if configSingleton.config == nil {
		return nil, errors.New("failed to get debug agent config")
	}
	return configSingleton.config, nil
}
