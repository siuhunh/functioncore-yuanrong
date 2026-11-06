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

// Package config define the way to get config info
package config

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/tls"
)

var (
	// Logger defines is a global logger used by function task.
	Logger                       log.FormatLogger
	configOnce                   sync.Once
	configInstance               *Config
	configPath                   = "/home/sn/config/autoscaler.conf"
	configChangedCallbacks       = make([]ChangedCallback, 0)
	configWatcher                monitor.FileWatcher
	funcScalePolicyWhiteListLock = sync.RWMutex{}
)

// ChangedCallback define hotconfig function
type ChangedCallback func()

// ClusterConfig define standby cluster election leader
type ClusterConfig struct {
	IsClusterMode  bool   `json:"clusterMode" valid:"optional"`
	ClusterType    string `json:"clusterType" valid:"required"`
	ForwardRequest bool   `json:"forwardRequest" valid:"required"`
}

// MetricConfig defines autoscaler metric config
type MetricConfig struct {
	TargetCPUUtilPercentage int64   `json:"targetCpuUtilPercentage"`
	TargetMemUtilPercentage int64   `json:"targetMemUtilPercentage"`
	SyncPeriod              int64   `json:"syncPeriod"`
	DownscaleStabilization  int64   `json:"downscaleStabilization,omitempty" valid:",optional"`
	ScaleUpTolerance        float64 `json:"scaleUpTolerance"`
	ScaleDownTolerance      float64 `json:"scaleDownTolerance"`
	PredictEnable           bool    `json:"predictEnable"`
	ScaleActionFunc         func()
	ScoreFunc               func() float64
}

// MetricServerConfig define monitoring server config
type MetricServerConfig struct {
	ServerAddr  string                  `json:"serverAddr,omitempty" valid:",optional"`
	ServerMode  string                  `json:"serverMode,omitempty" valid:",optional"`
	Password    string                  `json:"password,omitempty" valid:",optional"`
	EnableTLS   bool                    `json:"enableTLS,omitempty" valid:",optional"`
	TimeoutConf redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
}

// Config defines configurations for autoscaler.
type Config struct {
	SyncPeriod                  int64                              `json:"syncPeriod"`
	RedisPersistenceVersion     string                             `json:"redisPersistenceVersion" valid:"optional"`
	DownscaleStabilization      int64                              `json:"downscaleStabilization,omitempty" valid:",optional"`
	ScaleUpTolerance            float64                            `json:"scaleUpTolerance"`
	ScaleDownTolerance          float64                            `json:"scaleDownTolerance"`
	Clusters                    []string                           `json:"clusters"`
	MetricServerConfig          MetricServerConfig                 `json:"metricServerConfig,omitempty" valid:",optional"`
	CAEMetaEtcd                 *etcd3.ClientConfig                `json:"caeMetaEtcd" valid:"optional"`
	TickerTime                  int64                              `json:"tickerTime,omitempty" valid:",optional"`
	RouteEtcd                   *etcd3.ClientConfig                `json:"caeRouteEtcd" valid:"optional"`
	CAEMetaEtcdProxy            *etcd3.ClientConfig                `json:"caeMetaEtcdProxy" valid:"optional"`
	HTTPSConfig                 *tls.InternalHTTPSConfig           `json:"httpsConfig" valid:"optional"`
	LocalAuth                   *localauth.AuthConfig              `json:"localAuth" valid:"optional"`
	AuthenticationEnable        bool                               `json:"authenticationEnable" valid:"optional"`
	AlarmServerAddress          string                             `json:"alarmServerAddress" valid:"optional"`
	SelectMasterRefreshInterval int64                              `json:"selectMasterRefreshInterval"`
	SelectMasterLeaseTTL        int64                              `json:"selectMasterLeaseTTL"`
	RemoveOfflineClusterPeriod  int64                              `json:"removeOfflineClusterPeriod"`
	EnableInstanceKeyHandle     bool                               `json:"enableInstanceKeyHandle" valid:"optional"`
	PredictConfig               PredictionConfig                   `json:"predictConfig"`
	FuncScalePolicyWhiteList    map[string]SpecificFuncScalePolicy `json:"funcScalePolicyWhiteList" valid:"optional"`
	InstanceQueryPoolLen        int64                              `json:"instanceQueryPoolLen" valid:"optional"`
}

// SpecificFuncScalePolicy -
type SpecificFuncScalePolicy struct {
	ScaleUpStepPercent   int64 `json:"scaleUpStepPercent" valid:"optional"`
	ScaleDownStepPercent int64 `json:"scaleDownStepPercent" valid:"optional"`
}

// PredictionConfig -
type PredictionConfig struct {
	StoreTime             int64    `json:"storeTime"` // 单位 秒
	PredictDownloadTicker int64    `json:"predictDownloadTicker"`
	PredictUploadTicker   int64    `json:"predictUploadTicker"`
	PredictFuncWhiteList  []string `json:"predictFuncWhiteList"` // 预测白名单，白名单中存在的函数使用预测结果，白名单无值时，都使用预测结果
	PredictEnable         bool     `json:"predictEnable"`        // 开启预测功能
	PredictUseResult      bool     `json:"predictUseResult"`     //  是否使用预测的结果
}

// HotLoadConfig defines config in hot load conf file
type HotLoadConfig struct {
	FuncScalePolicyWhiteList map[string]SpecificFuncScalePolicy `json:"funcScalePolicyWhiteList" valid:"optional"`
	PredictConfig            PredictionConfig                   `json:"predictConfig" valid:"optional"`
	InstanceQueryPoolLen     int64                              `json:"instanceQueryPoolLen" valid:"optional"`
}

func getHTTPSConfig() *tls.InternalHTTPSConfig {
	config := os.Getenv("HTTPS_CONFIG")
	httpsConfig := &tls.InternalHTTPSConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &httpsConfig)
		if err != nil {
			log.GetLogger().Error("environment HTTPS_CONFIG is not json string")
			return &tls.InternalHTTPSConfig{}
		}
		if _, err := govalidator.ValidateStruct(httpsConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the https config: %s", err.Error())
			return &tls.InternalHTTPSConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment HTTPS_CONFIG")
		return &tls.InternalHTTPSConfig{}
	}
	return httpsConfig
}

func getAlarmServerAddress() string {
	config := os.Getenv("ALARM_SERVER_ADDRESS")
	return config
}

func getLocalAuthConfig() *localauth.AuthConfig {
	config := os.Getenv("LOCAL_AUTH")
	localAuthConfig := &localauth.AuthConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &localAuthConfig)
		if err != nil {
			log.GetLogger().Error("environment LOCAL_AUTH is not json string")
			return &localauth.AuthConfig{}
		}
		if _, err := govalidator.ValidateStruct(localAuthConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the local auth config: %s", err.Error())
			return &localauth.AuthConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment LOCAL_AUTH")
		return &localauth.AuthConfig{}
	}
	return localAuthConfig
}

func getAuthenticationEnable() bool {
	config := os.Getenv("AUTHENTICATION_ENABLE")
	return strings.EqualFold(strings.TrimSpace(config), "true")
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

// GetLocalAuth get local auth
func GetLocalAuth() *localauth.AuthConfig {
	cfg, err := GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get configuration, %s", err.Error())
		return &localauth.AuthConfig{}
	}
	return cfg.LocalAuth
}

func loadConfig(path string) (*Config, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// close file when fail or succeed
	defer func() {
		err := r.Close()
		if err != nil {
			log.GetLogger().Errorf("failed to close file %s %s", path, err.Error())
		}
	}()

	configContent, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	var config Config
	err = json.Unmarshal(configContent, &config)
	if err != nil {
		return nil, err
	}
	if len(config.CAEMetaEtcdProxy.ServerList) == 0 {
		log.GetLogger().Warnf("empty cae meta etcd proxy server list, skip connect")
		config.CAEMetaEtcdProxy = nil
	}
	return &config, nil
}

// InitConfigPath Set Config Path
func InitConfigPath(path string) {
	configPath = path
}

// GetConfig  -
func GetConfig() (*Config, error) {
	var err error
	configOnce.Do(func() {
		configInstance, err = loadConfig(configPath)
		if err != nil {
			log.GetLogger().Errorf("config files error: %s", err.Error())
			return
		}
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
		errMsg := "auto-scaler config is null"
		log.GetLogger().Error(errMsg)
		return nil, errors.New(errMsg)
	}

	loadConfigFromEnv()

	return configInstance, nil
}

// GetScaleUpPercent -
func GetScaleUpPercent(functionSvcID string) int64 {
	cfg, err := GetConfig()
	if err != nil {
		log.GetLogger().Warnf("failed to get config file of %s for scale step, err %s",
			functionSvcID, err.Error())
		return 0
	}
	funcScalePolicyWhiteListLock.RLock()
	defer funcScalePolicyWhiteListLock.RUnlock()
	scaleCfg := cfg.FuncScalePolicyWhiteList[functionSvcID]
	return scaleCfg.ScaleUpStepPercent
}

// GetScaleDownPercent -
func GetScaleDownPercent(functionSvcID string) int64 {
	cfg, err := GetConfig()
	if err != nil {
		log.GetLogger().Warnf("failed to get config file of %s for scale step, err %s",
			functionSvcID, err.Error())
		return 0
	}
	funcScalePolicyWhiteListLock.RLock()
	defer funcScalePolicyWhiteListLock.RUnlock()
	scaleCfg := cfg.FuncScalePolicyWhiteList[functionSvcID]
	return scaleCfg.ScaleDownStepPercent
}

func loadConfigFromEnv() {
	configInstance.HTTPSConfig = getHTTPSConfig()
	configInstance.AlarmServerAddress = getAlarmServerAddress()
	configInstance.LocalAuth = getLocalAuthConfig()
	configInstance.AuthenticationEnable = getAuthenticationEnable()
}

// WatchConfig start config watcher and register callback
func WatchConfig(stopCh <-chan struct{}, callback ChangedCallback) error {
	watcher, err := monitor.CreateFileWatcher(stopCh)
	if err != nil {
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

func setHotLoadConfig(config *Config, hotLoadConfig *HotLoadConfig) {
	hotFuncScalePolicyWhiteList(config, hotLoadConfig)
	hotLoadInstanceQueryPoolLen(config, hotLoadConfig)
}

func hotFuncScalePolicyWhiteList(config *Config, newConfig *HotLoadConfig) {
	if config == nil || newConfig == nil {
		return
	}
	funcScalePolicyWhiteListLock.Lock()
	config.PredictConfig.StoreTime = newConfig.PredictConfig.StoreTime
	log.GetLogger().Infof("succeed to update funcScalePolicyWhiteList: %+v, oldConfig: %+v",
		newConfig.FuncScalePolicyWhiteList, config.FuncScalePolicyWhiteList)
	log.GetLogger().Infof("succeed ot update predict config:%+v, oldConfig:%+v",
		newConfig.PredictConfig, config.PredictConfig)
	config.FuncScalePolicyWhiteList = newConfig.FuncScalePolicyWhiteList
	funcScalePolicyWhiteListLock.Unlock()
}

func hotLoadInstanceQueryPoolLen(config *Config, newConfig *HotLoadConfig) {
	if config == nil || newConfig == nil {
		return
	}

	oldInstanceQueryPoolLen := atomic.LoadInt64(&config.InstanceQueryPoolLen)
	newInstanceQueryPoolLen := newConfig.InstanceQueryPoolLen
	if oldInstanceQueryPoolLen != newInstanceQueryPoolLen {
		log.GetLogger().Infof("update instance query pool len from %d to %d",
			oldInstanceQueryPoolLen, newInstanceQueryPoolLen)

		atomic.StoreInt64(&config.InstanceQueryPoolLen, newInstanceQueryPoolLen)
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
