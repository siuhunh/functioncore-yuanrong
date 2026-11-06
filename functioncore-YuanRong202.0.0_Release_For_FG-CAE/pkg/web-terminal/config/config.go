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

// Package config define config of web-terminal
package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"sync"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/web-terminal/constant"
)

const (
	configEnvKey = "WEBTERMINAL_CONFIG"
)

var (
	configOnce            sync.Once
	configInstance        *Config
	configChangedCallback ChangedCallback
	configWatcher         monitor.FileWatcher
)

// ChangedCallback define hotconfig function
type ChangedCallback func()

// Config define all config of web-terminal
type Config struct {
	HTTPSConfig          *tls.InternalHTTPSConfig  `json:"httpsConfig" valid:"optional"`
	LocalAuth            *localauth.AuthConfig     `json:"localAuth" valid:"optional"`
	AuthenticationEnable bool                      `json:"authenticationEnable" valid:"optional"`
	MaxSessions          int                       `json:"maxSessions" valid:"optional"`
	ClusterConfig        map[string]*ClusterConfig `json:"clusterConfig" valid:"optional"`
	PingPongConfig       *PingPongConfig           `json:"pingPongConfig" valid:"optional"`
}

// ClusterConfig define yuanrong k8s clusterConfig
type ClusterConfig struct {
	ClusterID       string `json:"clusterID" valid:"required"`
	Address         string `json:"address" valid:"required"`
	CertificatePath string `json:"certPath" valid:"optional"`
}

// PingPongConfig define websocket ping pong configuration
type PingPongConfig struct {
	PingPongEnable   bool `json:"pingPongEnable" valid:"optional"`
	PingPongInterval int  `json:"pingPongInterval" valid:"optional"`
}

// GetConfig  -
func GetConfig() (*Config, error) {
	var err error
	configOnce.Do(func() {
		configFromFile, err := loadConfig(constant.ConfigFilePath)
		if err != nil {
			log.GetLogger().Errorf("config files error: %s", err.Error())
			return
		}
		configFromEnv, err := loadConfigFromEnv()
		if err != nil {
			log.GetLogger().Errorf("loadConfigFromEnv failed err %s", err)
			return
		}
		configInstance = configFromEnv
		configInstance.ClusterConfig = configFromFile.ClusterConfig
		_, err = govalidator.ValidateStruct(configInstance)
		if err != nil {
			log.GetLogger().Errorf("param check error, err is: %s", err.Error())
			configInstance = nil
			return
		}
	})
	if err != nil {
		return nil, err
	}
	if configInstance == nil {
		return nil, errors.New("empty configInstance")
	}
	return configInstance, nil
}

// EnableHTTPS checks if https is enabled and lazily initialize the tls library
func EnableHTTPS() bool {
	cfg, err := GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get configuration, %s", err.Error())
		return false
	}
	if cfg.HTTPSConfig == nil {
		return false // HTTPSConfig is not provided, default to turn off
	}

	enabled := cfg.HTTPSConfig.HTTPSEnable
	if enabled {
		// It is OK to call InitTLSConfig multiple times because it uses sync.Once internally
		err = tls.InitTLSConfig(cfg.HTTPSConfig.TLSProtocol, cfg.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return false
		}
	}
	return enabled
}

// GetPingPongConfig check if enable ping pong check and initialize
func GetPingPongConfig() (bool, int) {
	cfg, err := GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get configuration, %s", err.Error())
		return false, constant.DefaultPingPongInterval
	}
	if cfg.PingPongConfig == nil {
		return false, constant.DefaultPingPongInterval
	}

	enabled := cfg.PingPongConfig.PingPongEnable
	if enabled {
		return enabled, cfg.PingPongConfig.PingPongInterval
	}
	return false, constant.DefaultPingPongInterval
}

func loadConfigFromEnv() (*Config, error) {
	configJSON := os.Getenv(configEnvKey)
	config := &Config{}
	err := json.Unmarshal([]byte(configJSON), config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

func loadConfig(configPath string) (*Config, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// WatchConfig describe config watch api
func WatchConfig(stopCh <-chan struct{}, callback ChangedCallback) error {
	watcher, err := monitor.CreateFileWatcher(stopCh)
	if err != nil {
		return err
	}

	configWatcher = watcher
	configChangedCallback = callback
	configWatcher.RegisterCallback(constant.ConfigFilePath, hotLoadConfig)
	return nil
}

func hotLoadConfig(filename string, opType monitor.OpType) {
	log.GetLogger().Infof("file %s hot load start", filename)
	newConfig, err := loadConfig(filename)
	if err != nil {
		log.GetLogger().Errorf("hotLoadConfig failed file: %s, opType: %d, err: %s",
			filename, opType, err.Error())
		return
	}

	hotLoadClusterConfig(newConfig)
	hotLoadSessionsConfig(newConfig)

	if configChangedCallback != nil {
		configChangedCallback()
	}
}

func hotLoadClusterConfig(newAllConfig *Config) {
	hotloadClusterConfig := newAllConfig.ClusterConfig
	if hotloadClusterConfig != nil && len(hotloadClusterConfig) > 0 {
		log.GetLogger().Infof("update cluster config new: %v", hotloadClusterConfig)
		configInstance.ClusterConfig = hotloadClusterConfig
	}
}

func hotLoadSessionsConfig(newAllConfig *Config) {
	hotloadMaxSessions := newAllConfig.MaxSessions
	if hotloadMaxSessions > 0 {
		log.GetLogger().Infof("update maxSessions config new: %d", hotloadMaxSessions)
		configInstance.MaxSessions = hotloadMaxSessions
	}
}
