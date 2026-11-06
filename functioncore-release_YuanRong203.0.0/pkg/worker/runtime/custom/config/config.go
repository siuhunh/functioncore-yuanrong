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

package config

import (
	"errors"
	"io/ioutil"
	"os"
	"sync"

	"github.com/asaskevich/govalidator"
	"gopkg.in/yaml.v3"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
)

const (
	// CustomRuntimeConfigPath
	CustomRuntimeConfigPath = "/home/sn/config/custom-runtime-config.yaml"
)

var (
	configSingleton = struct {
		sync.Once
		config *ServerConfiguration
	}{}
	runtimeConfigSingleton = struct {
		sync.Once
		config *RuntimeConfigSingleton
	}{}
)

// RuntimeConfigSingleton is custom runtime config from config map
type RuntimeConfigSingleton struct {
	MaxRequestBodySize string `json:"maxRequestBodySize" valid:"required"`
}

// ServerConfiguration represents total config for app
type ServerConfiguration struct {
	ServerUrl     `yaml:"serverUrl" valid:"required"`
	ServerHeaders `yaml:"serverHeaders" valid:"required"`
	ServerEnvKey  `yaml:"serverEnvKey" valid:"optional"`
}

type ServerUrl struct {
	ReadyPath    string `yaml:"readyPath" valid:"optional"`
	NextPath     string `yaml:"nextPath" valid:"required"`
	ResponsePath string `yaml:"responsePath" valid:"required"`
	ErrorPath    string `yaml:"errorPath" valid:"required"`
}

type ServerHeaders struct {
	HeaderRequestIDKey      string `yaml:"headerRequestIDKey" valid:"required"`
	HeaderAccessKey         string `yaml:"headerAccessKey" valid:"optional"`
	HeaderSecretKey         string `yaml:"headerSecretKey" valid:"optional"`
	HeaderSecurityAccessKey string `yaml:"headerSecurityAccessKey" valid:"optional"`
	HeaderSecuritySecretKey string `yaml:"headerSecuritySecretKey" valid:"optional"`
	HeaderWorkflowID        string `yaml:"headerWorkflowID" valid:"optional"`
	HeaderOriginVersionTag  string `yaml:"HeaderOriginVersionTag" valid:"optional"`
	HeaderWorkflowRunID     string `yaml:"headerWorkflowRunID" valid:"optional"`
	HeaderWorkflowStateID   string `yaml:"headerWorkflowStateID" valid:"optional"`
	HeaderAuthToken         string `yaml:"headerAuthToken" valid:"optional"`
	HeaderSecurityToken     string `yaml:"headerSecurityToken" valid:"optional"`
}

type ServerEnvKey struct {
	RuntimeProject     string `yaml:"runtimeProject" valid:"optional"`
	RuntimeFuncName    string `yaml:"runtimeFuncName" valid:"optional"`
	RuntimeFuncVersion string `yaml:"runtimeFuncVersion" valid:"optional"`
	RuntimePackage     string `yaml:"runtimePackage" valid:"optional"`
	RuntimeHandler     string `yaml:"runtimeHandler" valid:"optional"`
	RuntimeTime        string `yaml:"runtimeTime" valid:"optional"`
	RuntimeUserData    string `yaml:"runtimeUserData" valid:"optional"`
	RuntimeCPU         string `yaml:"runtimeCPU" valid:"optional"`
	RuntimeMemory      string `yaml:"runtimeMemory" valid:"optional"`
	RuntimeCodeRoot    string `yaml:"runtimeCodeRoot" valid:"optional"`
	RuntimeApiAddr     string `yaml:"runtimeApiAddr" valid:"optional"`
}

// GetServerConfig get runtime Server Config
func GetServerConfig() (*ServerConfiguration, error) {
	configSingleton.Do(func() {
		serializedConfig, err := ioutil.ReadFile(CustomRuntimeConfigPath)
		if err != nil {
			log.GetLogger().Errorf("custom-runtime-config file reading error: %s", err)
			return
		}
		configSingleton.config = &ServerConfiguration{}
		if err = yaml.Unmarshal(serializedConfig, configSingleton.config); err != nil {
			log.GetLogger().Errorf("failed to unmarshal the custom-runtime-config file, error: %s", err)
			configSingleton.config = nil
			return
		}
		_, err = govalidator.ValidateStruct(configSingleton.config)
		if err != nil {
			log.GetLogger().Errorf("serverConfiguration checking error: %s", err)
			configSingleton.config = nil
			return
		}
	})
	if configSingleton.config == nil {
		return nil, errors.New("failed to get custom-runtime-config config")
	}
	return configSingleton.config, nil
}

// GetRuntimeConfig get runtime Config
func GetRuntimeConfig() (*RuntimeConfigSingleton, error) {
	runtimeConfigSingleton.Do(func() {
		maxRequestBodySize, ok := os.LookupEnv(constants.MaxRequestBodySize)
		if !ok {
			log.GetLogger().Errorf("config key %s does not exist", constants.MaxRequestBodySize)
			return
		}

		runtimeConfigSingleton.config = &RuntimeConfigSingleton{
			MaxRequestBodySize: maxRequestBodySize,
		}
	})
	if runtimeConfigSingleton.config == nil {
		return nil, errors.New("failed to get custom-runtime-config config")
	}
	return runtimeConfigSingleton.config, nil
}
