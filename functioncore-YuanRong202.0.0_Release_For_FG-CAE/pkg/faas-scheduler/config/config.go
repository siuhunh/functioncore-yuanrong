/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package config contains scheduler config
package config

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/tls"
)

var (
	// FConfig scheduler config
	FConfig = &Config{}
	// ConfigFilePath define Config File Path
	filePath               = "/home/sn/config/config.json"
	configEnvKey           = "SCHEDULER_CONFIG"
	configWatcher          monitor.FileWatcher
	configChangedCallbacks = make([]ChangedCallback, 0)
)

// ChangedCallback config changed callback func
type ChangedCallback func()

// InstanceLeaseConfig -
type InstanceLeaseConfig struct {
	LeaseCheckInterval int `json:"leaseCheckInterval" valid:"optional"`
	LeaseSpan          int `json:"leaseSpan" valid:"optional"`
}

// Config scheduler config
type Config struct {
	Runtime              *RuntimeConfig           `json:"runtime" valid:"optional"`
	Etcd                 *etcd3.ClientConfig      `json:"etcd" valid:"required"`
	CAEMetaETCD          *etcd3.ClientConfig      `json:"caeMetaEtcd" valid:"optional"`
	RouterETCD           *etcd3.ClientConfig      `json:"routerETCD" valid:"required"`
	MetaEtcdProxy        *etcd3.ClientConfig      `json:"metaEtcdProxy" valid:"optional"`
	CAEMetaEtcdProxy     *etcd3.ClientConfig      `json:"caeMetaEtcdProxy" valid:"optional"`
	EtcdLeaseConfig      *EtcdLeaseConfig         `json:"etcdLeaseConfig" valid:"optional"`
	LocalAuth            *localauth.AuthConfig    `json:"localAuth" valid:"optional"`
	InstanceLeaseConfig  *InstanceLeaseConfig     `json:"processorLeaseConfig" valid:"optional"`
	HTTPSConfig          *tls.InternalHTTPSConfig `json:"httpsConfig" valid:"optional"`
	AlarmServerAddress   string                   `json:"alarmServerAddress" valid:"optional"`
	AuthenticationEnable bool                     `json:"authenticationEnable" valid:"optional"`
}

// HotLoadConfig defines config in hot load conf file
type HotLoadConfig struct {
	EtcdLeaseConfig *EtcdLeaseConfig    `json:"etcdLeaseConfig" valid:"optional"`
	Etcd            *etcd3.ClientConfig `json:"etcd" valid:"required"`
}

// EtcdLeaseConfig etcd lease config
type EtcdLeaseConfig struct {
	LeaseTTL int64 `yaml:"leaseTTL" valid:"optional"`
	RenewTTL int64 `yaml:"renewTTL" valid:"optional"`
}

// RuntimeConfig config info
type RuntimeConfig struct {
	ServicePort     string `json:"servicePort" valid:",optional"`
	HealthCheckPort string `json:"healthCheckPort" valid:",optional"`
}

// InitConfig -
func InitConfig() error {
	config, err := loadConfigFromEnv()
	if err != nil {
		return err
	}
	FConfig = config

	InitDefaultConfig()

	hotLoadCfg, err := readHotLoadConfig(filePath)
	if err != nil {
		return err
	}
	// merge hot load config
	setHotLoadConfig(config, hotLoadCfg)
	log.GetLogger().Infof("succeed to merge hot load config")
	return nil
}

// WatchConfig start config watcher and register callback
func WatchConfig(stopCh <-chan struct{}, callback ChangedCallback) error {
	watcher, err := monitor.CreateFileWatcher(stopCh)
	if err != nil {
		return err
	}

	configWatcher = watcher
	configChangedCallbacks = append(configChangedCallbacks, callback)
	configWatcher.RegisterCallback(filePath, hotLoadConfig)
	return nil
}

// setHotLoadConfig
func setHotLoadConfig(config *Config, hotLoadConfig *HotLoadConfig) {
	hotLoadLeaseConfig(config, hotLoadConfig)
	hotLoadETCDConfig(config, hotLoadConfig)
}

func hotLoadLeaseConfig(config *Config, newConfig *HotLoadConfig) {
	if newConfig.EtcdLeaseConfig == nil {
		return
	}
	if newConfig.EtcdLeaseConfig.LeaseTTL > 0 {
		log.GetLogger().Infof("EtcdLeaseConfig leaseTTL updated, new: %d", newConfig.EtcdLeaseConfig.LeaseTTL)
		config.EtcdLeaseConfig.LeaseTTL = newConfig.EtcdLeaseConfig.LeaseTTL
	}
	if newConfig.EtcdLeaseConfig.RenewTTL > 0 {
		log.GetLogger().Infof("EtcdLeaseConfig renewTTL updated, new: %d", newConfig.EtcdLeaseConfig.RenewTTL)
		config.EtcdLeaseConfig.RenewTTL = newConfig.EtcdLeaseConfig.RenewTTL
	}
}

func hotLoadETCDConfig(config *Config, newConfig *HotLoadConfig) {
	oldConfig := config.Etcd
	hotLoadEtcdConfig := newConfig.Etcd
	if hotLoadEtcdConfig != nil && len(hotLoadEtcdConfig.ServerList) > 0 {
		log.GetLogger().Infof("update EtcdConfig new: %d", hotLoadEtcdConfig.ServerList)
		oldConfig.ServerList = hotLoadEtcdConfig.ServerList
	}
}

func readHotLoadConfig(configPath string) (*HotLoadConfig, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	config := &HotLoadConfig{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// hotLoadConfig hot load config and call funcs to process config changes
func hotLoadConfig(filename string, opType monitor.OpType) {
	log.GetLogger().Infof("start to hot load config file: %s", filename)
	newHotLoadConfig, err := readHotLoadConfig(filename)
	if err != nil {
		log.GetLogger().Errorf("failed to get new config file: %s, opType: %d, err: %s", filename, opType, err.Error())
		return
	}
	_, err = govalidator.ValidateStruct(newHotLoadConfig)
	if err != nil {
		log.GetLogger().Errorf("new config file param check error: %s, err is: %s", filename, err.Error())
		return
	}

	// only hot load the params we care about
	setHotLoadConfig(FConfig, newHotLoadConfig)
	log.GetLogger().Infof("succeed to refresh hot load params in configInstance")

	// take turns to invoke callback func to process changed config
	if configChangedCallbacks != nil {
		for _, configChangedCallback := range configChangedCallbacks {
			configChangedCallback()
		}
	}
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

// InitDefaultConfig -
func InitDefaultConfig() {
	if FConfig.Runtime == nil {
		FConfig.Runtime = &RuntimeConfig{}
	}

	if FConfig.Runtime.ServicePort == "" {
		FConfig.Runtime.ServicePort = defaultServicePort
	}

	if FConfig.Runtime.HealthCheckPort == "" {
		FConfig.Runtime.HealthCheckPort = defaultHealthCheckPort
	}

	if FConfig.InstanceLeaseConfig == nil {
		FConfig.InstanceLeaseConfig = &InstanceLeaseConfig{}
	}

	if FConfig.InstanceLeaseConfig.LeaseCheckInterval == 0 {
		FConfig.InstanceLeaseConfig.LeaseCheckInterval = defaultLeaseCheckInterval
	}

	if FConfig.InstanceLeaseConfig.LeaseSpan == 0 {
		FConfig.InstanceLeaseConfig.LeaseSpan = defaultLeaseSpan
	}

	if FConfig.EtcdLeaseConfig == nil {
		FConfig.EtcdLeaseConfig = &EtcdLeaseConfig{}
	}

	log.GetLogger().Infof("LeaseCheckInterval %d, LeaseSpan %d",
		FConfig.InstanceLeaseConfig.LeaseCheckInterval, FConfig.InstanceLeaseConfig.LeaseSpan)
}
