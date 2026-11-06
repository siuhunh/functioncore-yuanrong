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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/tls"
)

const (
	// DefaultLimitCodeSize default limit of the cached code size
	DefaultLimitCodeSize     = 100 * 1024
	configPath               = "/home/sn/config/cache-manager.conf"
	metaEtcdKey              = "META_ETCD_CONFIG"
	metaEtcdProxyKey         = "META_ETCD_PROXY_CONFIG"
	s3ConfigKey              = "S3_CONFIG"
	localAuthKey             = "LOCAL_AUTH"
	httpsConfigKey           = "HTTPS_CONFIG"
	persistenceKey           = "PERSISTENCE_CONFIG"
	alarmAddrKey             = "ALARM_SERVER_ADDR"
	limitCodeSizeKey         = "LIMIT_CODE_SIZE"
	defaultSmallCodePoolSize = 10
	defaultLargeCodePoolSize = 1
)

var configOnce sync.Once
var configInstance *CacheManagerConfig
var configWatcher monitor.FileWatcher
var configChangedCallbacks = make([]ChangedCallback, 0)

// ChangedCallback config changed callback func
type ChangedCallback func()

// GetConfig : singleton pattern thread safe
func GetConfig() *CacheManagerConfig {
	configOnce.Do(func() {
		configInstance = newCacheConfig(configPath)
		if configInstance == nil {
			fmt.Println("failed to create config")
		}
	})

	// get hotload config from file refreshed by configmap
	hotLoadConfig, err := readHotLoadConfig(configPath)
	if err != nil {
		return nil
	}

	// merge hotload config
	setHotLoadConfig(configInstance, hotLoadConfig)
	monitor.SetMemoryControlConfig(configInstance.MemoryControlConfig)
	return configInstance
}

func newCacheConfig(path string) *CacheManagerConfig {
	path, err := filepath.Abs(path)
	if err != nil {
		log.GetLogger().Errorf("failed to abs file.path: %s, error: %s", path, err.Error())
		return nil
	}
	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("failed to open file.path: %s, error: %s\n", path, err.Error())
		return nil
	}
	// close file when fail or succeed
	defer file.Close()
	config, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("failed to read file")
		return nil
	}
	conf := &CacheManagerConfig{}
	err = json.Unmarshal(config, conf)
	if err != nil {
		return nil
	}
	conf.HTTPSConfig = loadHTTPSConfig()
	conf.LocalAuth = loadLocalAuthConfig()
	conf.EtcdConfig = loadMetaEtcdConfig()
	conf.MetaEtcdProxy = loadMetaEtcdProxyConfig()
	conf.S3Config = loadS3Config()
	conf.Persistence = loadPersistenceConfig()
	conf.LimitCodeSize = loadLimitCodeSize()
	conf.AlarmServerAddress = loadAlarmServerAddr()
	conf.SmallCodePoolSize = loadSmallCodePoolSize()
	conf.LargeCodePoolSize = loadLargeCodePoolSize()

	if _, err := govalidator.ValidateStruct(conf); err != nil {
		fmt.Printf("failed to validate cache-manager config, err: %s\n", err.Error())
		return nil
	}
	log.GetLogger().Infof("cache-manager mem config %v", conf.MemoryControlConfig)
	return conf
}

func loadHTTPSConfig() *tls.InternalHTTPSConfig {
	config := os.Getenv(httpsConfigKey)
	httpsConfig := &tls.InternalHTTPSConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), httpsConfig)
		if err != nil {
			log.GetLogger().Errorf("environment %s is not json string: %s", httpsConfigKey, err.Error())
			return &tls.InternalHTTPSConfig{}
		}
	} else {
		log.GetLogger().Errorf("failed to get environment %s", httpsConfigKey)
		return &tls.InternalHTTPSConfig{}
	}
	return httpsConfig
}

func loadLocalAuthConfig() *localauth.AuthConfig {
	config := os.Getenv(localAuthKey)
	authConfig := &localauth.AuthConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), authConfig)
		if err != nil {
			log.GetLogger().Errorf("environment %s is not json string: %s", localAuthKey, err.Error())
			return &localauth.AuthConfig{}
		}
	} else {
		log.GetLogger().Errorf("failed to get environment %s", localAuthKey)
		return &localauth.AuthConfig{}
	}
	return authConfig
}

func loadMetaEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv(metaEtcdKey)
	etcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), etcdConfig)
		if err != nil {
			log.GetLogger().Errorf("environment %s is not json string: %s", metaEtcdKey, err.Error())
			return nil
		}
	} else {
		log.GetLogger().Errorf("failed to get environment %s", metaEtcdKey)
		return nil
	}
	return etcdConfig
}

func loadMetaEtcdProxyConfig() *etcd3.ClientConfig {
	config := os.Getenv(metaEtcdProxyKey)
	etcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), etcdConfig)
		if err != nil {
			log.GetLogger().Errorf("environment %s is not json string: %s", metaEtcdProxyKey, err.Error())
			return nil
		}
	} else {
		log.GetLogger().Warnf("failed to get environment %s", metaEtcdProxyKey)
		return nil
	}
	if len(etcdConfig.ServerList) == 0 {
		log.GetLogger().Warnf("empty meta etcd proxy server list, skip connect")
		return nil
	}
	return etcdConfig
}

func loadS3Config() S3Config {
	config := os.Getenv(s3ConfigKey)
	s3Config := S3Config{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &s3Config)
		if err != nil {
			log.GetLogger().Errorf("environment %s is not json string: %s", s3ConfigKey, err.Error())
			return S3Config{}
		}
		if _, err = govalidator.ValidateStruct(s3Config); err != nil {
			log.GetLogger().Errorf("failed to validate s3Config: %s", err.Error())
			return S3Config{}
		}
	} else {
		log.GetLogger().Errorf("failed to get environment %s", s3ConfigKey)
		return S3Config{}
	}
	return s3Config
}

func loadPersistenceConfig() PersistenceConfig {
	config := os.Getenv(persistenceKey)
	persistenceConfig := PersistenceConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &persistenceConfig)
		if err != nil {
			log.GetLogger().Errorf("environment %s is not json string: %s", persistenceKey, err.Error())
			return PersistenceConfig{}
		}
	} else {
		log.GetLogger().Errorf("failed to get environment %s", persistenceKey)
		return PersistenceConfig{}
	}
	return persistenceConfig
}

func parseEnvInt(key string, defaultValue int) int {
	env := os.Getenv(key)
	if env == "" {
		log.GetLogger().Warnf("failed to get environment %s", key)
		return defaultValue
	}
	val, err := strconv.Atoi(env)
	if err != nil || val <= 0 {
		log.GetLogger().Warnf("failed to transform environment %s", key)
		return defaultValue
	}
	return val
}

func loadLimitCodeSize() int {
	return parseEnvInt(limitCodeSizeKey, DefaultLimitCodeSize)
}

func loadSmallCodePoolSize() int {
	return parseEnvInt("SMALL_CODE_POOL_SIZE", defaultSmallCodePoolSize)
}

func loadLargeCodePoolSize() int {
	return parseEnvInt("LARGE_CODE_POOL_SIZE", defaultLargeCodePoolSize)
}

func loadAlarmServerAddr() string {
	config := os.Getenv(alarmAddrKey)
	if config == "" {
		log.GetLogger().Warnf("failed to get environment %s", alarmAddrKey)
	}
	return config
}

// WatchConfig start config watcher and register callback
func WatchConfig(stopCh <-chan struct{}, callback ChangedCallback) error {
	watcher, err := monitor.CreateFileWatcher(stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to create file watcher, %s", err.Error())
		return err
	}

	configWatcher = watcher
	configChangedCallbacks = append(configChangedCallbacks, callback)
	configWatcher.RegisterCallback(configPath, hotLoadConfig)
	return nil
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
	setHotLoadConfig(configInstance, newHotLoadConfig)
	log.GetLogger().Infof("succeed to refresh hotload params in configInstance")

	// take turns to invoke callback func to process changed config
	if configChangedCallbacks != nil {
		for _, configChangedCallback := range configChangedCallbacks {
			configChangedCallback()
		}
	}
}

func readHotLoadConfig(p string) (*HotLoadConfig, error) {
	r, err := os.Open(p)
	if err != nil {
		return nil, err
	}

	// close file when fail or succeed
	defer func() {
		err := r.Close()
		if err != nil {
			log.GetLogger().Errorf("failed to close file %s %s", p, err.Error())
		}
	}()

	configContent, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var hotLoadConfig HotLoadConfig
	err = json.Unmarshal(configContent, &hotLoadConfig)
	if err != nil {
		return nil, err
	}
	return &hotLoadConfig, nil
}

func setHotLoadConfig(config *CacheManagerConfig, hotLoadConfig *HotLoadConfig) {
	hotLoadEtcdAndMemConfig(config, hotLoadConfig)
}

func hotLoadEtcdAndMemConfig(config *CacheManagerConfig, hotLoadConfig *HotLoadConfig) {
	oldConfig := config.EtcdConfig
	hotLoadEtcdConfig := hotLoadConfig.EtcdConfig
	if hotLoadEtcdConfig != nil && len(hotLoadEtcdConfig.ServerList) > 0 {
		log.GetLogger().Infof("update EtcdConfig new: %d", hotLoadEtcdConfig.ServerList)
		oldConfig.ServerList = hotLoadEtcdConfig.ServerList
	}
	hotLoadMemoryConfig := hotLoadConfig.MemoryControlConfig
	if hotLoadMemoryConfig != nil {
		log.GetLogger().Infof("update MemoryConfig, LowerMemoryPercent %f, HighMemoryPercent %f, "+
			"MemDetectIntervalMs %d", hotLoadMemoryConfig.LowerMemoryPercent, hotLoadMemoryConfig.HighMemoryPercent,
			hotLoadMemoryConfig.MemDetectIntervalMs)
		config.MemoryControlConfig = hotLoadMemoryConfig
	}
}
