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

// Package config contains configuration of runtime manager
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/asaskevich/govalidator"
)

const configEnvKey = "RUNTIME_MANAGER_CONFIG"

// RuntimeConfig runtime config
type RuntimeConfig struct {
	Port                  int32             `json:"port" valid:",optional"`
	Enable                bool              `json:"enable" valid:",optional"`
	DisableTrace          []int64           `json:"disableTrace" valid:",optional"`
	ExcludeSpecifications []int64           `json:"excludeSpecifications" valid:",optional"`
	CustomArgs            []string          `json:"customArgs" valid:",optional"`
	Addition              map[string]string `json:"addition" valid:",optional"`
	TraceMasterAddress    string            `json:"traceMasterAddress" valid:",optional"`
	WorkerDir             string            `json:"workerDir" valid:",optional"`
	SocketType            string            `json:"socketType" valid:",optional"` // tcp unix
}

// Config runtime manager config
type Config struct {
	ServerPort         int32                    `json:"serverPort" valid:",optional"`
	RuntimeConfigs     map[string]RuntimeConfig `json:"runtimeConfigs" valid:",optional"`
	EnableCPUWatchLoop bool                     `json:"enableCPUWatchLoop" valid:",optional"`
	SnuserDirSizeLimit int32                    `json:"snuserDirSizeLimit" valid:",optional"`
}

// GetConfig get runtime manager config
func GetConfig() (*Config, error) {
	serializedConfig, ok := os.LookupEnv(configEnvKey)
	if !ok {
		return nil, fmt.Errorf("config key %s does not exist", configEnvKey)
	}
	runtimeManagerConf := &Config{}
	var err error
	if err = json.Unmarshal([]byte(serializedConfig), runtimeManagerConf); err != nil {
		return nil, errors.New("failed to unmarshal runtime manager config with error: " + err.Error())
	}
	_, err = govalidator.ValidateStruct(runtimeManagerConf)
	if err != nil {
		return nil, errors.New("runtime manager config file checking error: " + err.Error())
	}
	return runtimeManagerConf, nil
}
