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

// Package config implements config for state manager module
package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"strconv"
	"sync/atomic"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/pprof"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/state-manager/constant"
)

// FConfigs -
var FConfigs *FConfig

var (
	// TenantStateLimit -
	TenantStateLimit int64 = 100
	// TenantStateLocalLimit -
	TenantStateLocalLimit  int64 = 10
	address                      = os.Getenv(constant.Address)
	kubeCfgTimeout               = os.Getenv(constant.KubeCfgTimeout)
	alarmServerAddress           = os.Getenv(constant.AlarmServerAddress)
	configWatcher          monitor.FileWatcher
	configChangedCallbacks = make([]ChangedCallback, 0)
	clusterKeyByHashTag    = make([]string, 0)
)

const (
	configPath = "/home/sn/config/config.json"
	// HashTag -
	HashTag = "{hashTag}"
	// DefaultPullInstanceTimeSpan -
	DefaultPullInstanceTimeSpan = 30
)

// ChangedCallback config changed callback func
type ChangedCallback func()

// LogConfig Configuration information for logs
type LogConfig struct {
	LogDir      string `json:"log_dir"`       // /home/sn/log
	LogFilename string `json:"log_file_name"` // state-manager-run
}

// ClusterConfig define standby cluster election leader
type ClusterConfig struct {
	IsClusterMode  bool   `json:"clusterMode" valid:"optional"`
	ClusterType    string `json:"clusterType" valid:"required"`
	ForwardRequest bool   `json:"forwardRequest" valid:"required"`
}

// SchedulerConfig define config of scheduler algorithm
type SchedulerConfig struct {
	NodeAffinity NodeAffinity `json:"nodeAffinity" valid:"optional"`
	Filter       Filter       `json:"filter" valid:"required"`
	Scorer       Scorer       `json:"scorer" valid:"required"`
}

// NodeAffinity defines node where the pool is placed
type NodeAffinity struct {
	NodeAffinityKey   string `json:"nodeAffinityKey" valid:"required"`
	NodeAffinityValue string `json:"nodeAffinityValue" valid:"required"`
}

// Filter define config of filter algorithm
type Filter struct {
	AlgorithmName string `json:"algorithm_name" valid:"required"`
}

// Scorer define config of scorer algorithm
type Scorer struct {
	AlgorithmName    string  `json:"algorithm_name" valid:"required"`
	CPUScoreFactor   float32 `json:"cpu_score_factor"`
	MemScoreFactor   float32 `json:"mem_score_factor"`
	StateScoreFactor float32 `json:"state_score_factor" valid:"required"`
	AffinityFactor   float32 `json:"affinity_factor" valid:"required"`
}

// StateCounterCfg redis counter config
type StateCounterCfg struct {
	PersistenceInterval int                     `json:"persistenceInterval,omitempty" valid:",optional"`
	ReleaseInterval     int                     `json:"releaseInterval,omitempty" valid:",optional"`
	ClusterID           string                  `json:"clusterID,omitempty" valid:",optional"`
	ServerAddr          string                  `json:"serverAddr,omitempty" valid:",optional"`
	ServerMode          string                  `json:"serverMode,omitempty" valid:",optional"`
	Password            string                  `json:"password,omitempty" valid:",optional"`
	EnableTLS           bool                    `json:"enableTLS,omitempty" valid:",optional"`
	TimeoutConf         redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
	SiteClusters        []string                `json:"siteClusters,omitempty" valid:",optional"`
}

// ScaleData -
type ScaleData struct {
	SyncPeriod             int64                       `json:"syncPeriod" valid:"optional"` // scale period
	ScaleUpTolerance       float64                     `json:"scaleUpTolerance" valid:"optional"`
	ScaleDownTolerance     float64                     `json:"scaleDownTolerance" valid:"optional"`
	TickerTime             int64                       `json:"tickerTime" valid:"optional"`             // ticker of get data from redis
	DownscaleStabilization int64                       `json:"downscaleStabilization" valid:"optional"` // scale down coldTime
	ScaleRules             map[string]map[string]int64 `json:"scaleRules" valid:"optional"`             // key is type, example: cache, mysql....
}

// TenantConfig is used for tenant limit
type TenantConfig struct {
	TenantStateLimit int64 `json:"tenantStateLimit" valid:"optional"`
}

// FConfig is used to store all configurations
type FConfig struct {
	EtcdCfg                  *etcd3.ClientConfig             `json:"etcd" valid:"required"`
	StateEtcdCfg             *etcd3.ClientConfig             `json:"stateEtcd" valid:"required"`
	RouterETCD               etcd3.ClientConfig              `json:"routerETCD" valid:"required"`
	CAERouterEtcd            *etcd3.ClientConfig             `json:"caeRouterEtcd" valid:"optional"`
	CAEMetaEtcd              *etcd3.ClientConfig             `json:"caeMetaEtcd" valid:"optional"`
	MetaEtcdProxy            *etcd3.ClientConfig             `json:"MetaEtcdProxy" valid:"optional"`
	LogCfg                   LogConfig                       `json:"log" valid:"required"`
	LocalAuth                localauth.AuthConfig            `json:"localAuth" valid:"optional"`
	HTTPSConfig              tls.InternalHTTPSConfig         `json:"httpsConfig" valid:"optional"`
	Address                  string                          `json:"address" valid:"required"`
	Cluster                  ClusterConfig                   `json:"cluster" valid:"required"`
	ScheCfg                  SchedulerConfig                 `json:"scheduler" valid:"required"`
	KubeCfgTimeout           int64                           `json:"kubeCfgTimeout,omitempty" valid:"optional"`
	StateCounterCfg          StateCounterCfg                 `json:"stateCounterCfg" valid:"required"`
	AlarmServerAddress       string                          `json:"alarmServerAddress" valid:"required"`
	ScaleData                ScaleData                       `json:"scaleData" valid:"required"`
	BridgeManagerConfig      bridgetypes.BridgeManagerConfig `json:"bridgeManagerConfig,omitempty" valid:"optional"`
	CollectPprofInstanceList []string                        `json:"collectPprofInstanceList" valid:",optional"`
}

// HotLoadConfig defines config in hot load conf file
type HotLoadConfig struct {
	Etcd                     *etcd3.ClientConfig             `json:"etcd" valid:"optional"`
	StateCounterCfg          StateCounterCfg                 `json:"stateCounterCfg" valid:"optional"`
	ScaleData                ScaleData                       `json:"scaleData" valid:"required"`
	BridgeManagerConfig      bridgetypes.BridgeManagerConfig `json:"bridgeManagerConfig,omitempty" valid:"optional"`
	TenantConfig             TenantConfig                    `json:"tenantConfig" valid:"optional"`
	CollectPprofInstanceList []string                        `json:"collectPprofInstanceList" valid:",optional"`
}

// GetConfig is used to get all configurations
func GetConfig() (*FConfig, error) {
	if FConfigs != nil {
		return FConfigs, nil
	}
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	FConfigs = &FConfig{}
	err = json.Unmarshal(data, FConfigs)
	if err != nil {
		log.GetLogger().Errorf("failed to Unmarshal FConfigs: %s", err.Error())
		return nil, err
	}
	kubeConfigTimeout, err := strconv.ParseInt(kubeCfgTimeout, 10, 64)
	if err != nil {
		return nil, err
	}
	FConfigs.Address = address
	FConfigs.KubeCfgTimeout = kubeConfigTimeout
	FConfigs.AlarmServerAddress = alarmServerAddress
	FConfigs.EtcdCfg = getEtcdConfig()
	FConfigs.StateEtcdCfg = getStateEtcdConfig()
	FConfigs.RouterETCD = getRouteEtcdConfig()
	FConfigs.CAERouterEtcd = getCAERouterEtcdConfig()
	FConfigs.CAEMetaEtcd = getCAEMetaEtcdConfig()
	FConfigs.MetaEtcdProxy = getMetaEtcdProxyConfig()
	FConfigs.HTTPSConfig = getHTTPSConfig()
	FConfigs.LocalAuth = getLocalAuthConfig()
	FConfigs.LogCfg = getLogConfig()
	FConfigs.Cluster = getClusterConfig()
	FConfigs.ScheCfg = getSchedulerConfig()
	FConfigs = setStateCounterCfg(FConfigs)
	if _, err = govalidator.ValidateStruct(FConfigs); err != nil {
		log.GetLogger().Errorf("failed to ValidateStruct FConfigs: %s", err.Error())
		return nil, err
	}
	hotLoadCfg, err := readHotLoadConfig(configPath)
	if err != nil {
		return nil, err
	}
	// merge hot load config
	setHotLoadConfig(FConfigs, hotLoadCfg)
	return FConfigs, nil
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

// setHotLoadConfig
func setHotLoadConfig(config *FConfig, hotLoadConfig *HotLoadConfig) {
	hotLoadETCDConfig(config, hotLoadConfig)
	hotBridgeManagerConfig(config, hotLoadConfig)
	hotScaleDataConfig(config, hotLoadConfig)
	hotLoadTenantConfig(hotLoadConfig)
	hotLoadCollectPprofInstanceList(config, hotLoadConfig)
}

func hotLoadCollectPprofInstanceList(config *FConfig, newConfig *HotLoadConfig) {
	oldPprofInstanceList := config.CollectPprofInstanceList
	newPprofInstanceList := newConfig.CollectPprofInstanceList
	oldPprofInstanceListStr, err := json.Marshal(oldPprofInstanceList)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal oldPprofInstanceList: %s",
			err.Error())
		return
	}
	newPprofInstanceListStr, err := json.Marshal(newPprofInstanceList)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal newPprofInstanceList: %s",
			err.Error())
		return
	}
	if string(oldPprofInstanceListStr) == string(newPprofInstanceListStr) {
		log.GetLogger().Warnf("PprofInstanceList not changed, ignore it")
		return
	}
	log.GetLogger().Infof("PprofInstanceList changed, oldWhiteList: %s, newWhiteList: %s",
		oldPprofInstanceListStr, newPprofInstanceListStr)
	config.CollectPprofInstanceList = newPprofInstanceList
	pprof.NotifyStartPprof(config.CollectPprofInstanceList)
}

func hotLoadETCDConfig(config *FConfig, newConfig *HotLoadConfig) {
	oldConfig := config.EtcdCfg
	hotLoadEtcdConfig := newConfig.Etcd
	if hotLoadEtcdConfig != nil && len(hotLoadEtcdConfig.ServerList) > 0 {
		log.GetLogger().Infof("update EtcdConfig new: %d", hotLoadEtcdConfig.ServerList)
		oldConfig.ServerList = hotLoadEtcdConfig.ServerList
	}
}

func hotScaleDataConfig(config *FConfig, newConfig *HotLoadConfig) {
	if config == nil || newConfig == nil {
		return
	}

	log.GetLogger().Infof("succeed to update scaleData Config: %+v, oldConfig: %+v",
		newConfig.ScaleData, config.ScaleData)
	config.ScaleData = newConfig.ScaleData
}

func hotBridgeManagerConfig(config *FConfig, newConfig *HotLoadConfig) {
	if config == nil || newConfig == nil {
		return
	}
	oldBridgeConfig := config.BridgeManagerConfig
	if newConfig.BridgeManagerConfig.PullInstanceTimeSpan < 0 {
		log.GetLogger().Warnf("pullInstanceTimeSpan illegal, use default:%d", DefaultPullInstanceTimeSpan)
		newConfig.BridgeManagerConfig.PullInstanceTimeSpan = DefaultPullInstanceTimeSpan
	}
	config.BridgeManagerConfig.PullInstanceTimeSpan = newConfig.BridgeManagerConfig.PullInstanceTimeSpan
	config.BridgeManagerConfig.BridgeFuncWhiteList = newConfig.BridgeManagerConfig.BridgeFuncWhiteList
	log.GetLogger().Infof("hotBridgeManagerConfig, old config %+v, new config %+v", oldBridgeConfig, config.BridgeManagerConfig)
}

func hotLoadTenantConfig(newConfig *HotLoadConfig) {
	if newConfig == nil {
		return
	}

	if newConfig.TenantConfig.TenantStateLimit == 0 {
		log.GetLogger().Warnf("TenantStateLimit shouldn't be 0 or nil")
		return
	}

	log.GetLogger().Infof("succeed to update tenant Config: %+v, oldConfig: %x",
		newConfig.TenantConfig, TenantStateLimit)
	atomic.StoreInt64(&TenantStateLimit, newConfig.TenantConfig.TenantStateLimit)
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
	setHotLoadConfig(FConfigs, newHotLoadConfig)
	log.GetLogger().Infof("succeed to refresh hot load params in configInstance")

	// take turns to invoke callback func to process changed config
	if configChangedCallbacks != nil {
		for _, configChangedCallback := range configChangedCallbacks {
			configChangedCallback()
		}
	}
}

// GetHotloadMetricsConf  get real-time config info from config.json
func GetHotloadMetricsConf() (string, redisclient.TimeoutConf, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return "", redisclient.GetDefaultTimeoutConf(), err
	}
	tmpConfig := &FConfig{}
	err = json.Unmarshal(data, tmpConfig)
	if err != nil {
		return "", redisclient.GetDefaultTimeoutConf(), err
	}
	log.GetLogger().Infof("get serverAddr: %s, timeoutConf: %v", tmpConfig.StateCounterCfg.ServerAddr,
		tmpConfig.StateCounterCfg.TimeoutConf)
	return tmpConfig.StateCounterCfg.ServerAddr, tmpConfig.StateCounterCfg.TimeoutConf, nil
}

func getEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv(constant.Etcd)
	etcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &etcdConfig)
		if err != nil {
			log.GetLogger().Error("environment ETCD is not json string")
			return nil
		}
		if _, err := govalidator.ValidateStruct(etcdConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the etcd config: %s", err.Error())
			return nil
		}
	} else {
		log.GetLogger().Error("failed to get environment ETCD config")
		return nil
	}
	return etcdConfig
}

func getMetaEtcdProxyConfig() *etcd3.ClientConfig {
	config := os.Getenv(constant.MetaEtcdProxy)
	etcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &etcdConfig)
		if err != nil {
			log.GetLogger().Errorf("environment META_ETCD_PROXY is not json string: %s", err.Error())
			return nil
		}
		if _, err := govalidator.ValidateStruct(etcdConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the etcd config: %s", err.Error())
			return nil
		}
	} else {
		log.GetLogger().Warnf("failed to get environment META_ETCD_PROXY config")
		return nil
	}
	if len(etcdConfig.ServerList) == 0 {
		log.GetLogger().Warnf("empty meta etcd proxy server list, skip connect")
		return nil
	}
	return etcdConfig
}

func getStateEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv(constant.StateEtcd)
	stateEtcdConfig := &etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &stateEtcdConfig)
		if err != nil {
			log.GetLogger().Error("environment STATE_ETCD is not json string")
			return nil
		}
		if _, err := govalidator.ValidateStruct(stateEtcdConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the state etcd config: %s", err.Error())
			return nil
		}
	} else {
		log.GetLogger().Error("failed to get environment STATE_ETCD config")
		return nil
	}
	return stateEtcdConfig
}

func getRouteEtcdConfig() etcd3.ClientConfig {
	config := os.Getenv(constant.RouteEtcd)
	routeEtcdConfig := etcd3.ClientConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &routeEtcdConfig)
		if err != nil {
			log.GetLogger().Error("environment ETCD is not json string")
			return etcd3.ClientConfig{}
		}
		if _, err := govalidator.ValidateStruct(routeEtcdConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the etcd config: %s", err.Error())
			return etcd3.ClientConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment ETCD config")
		return etcd3.ClientConfig{}
	}
	return routeEtcdConfig
}

func getCAERouterEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv(constant.CAERouterEtcd)
	if config == "" {
		log.GetLogger().Errorf("failed to get environment %s config", constant.CAERouterEtcd)
		return nil
	}
	caeRouterEtcdConfig := &etcd3.ClientConfig{}
	err := json.Unmarshal([]byte(config), &caeRouterEtcdConfig)
	if err != nil {
		log.GetLogger().Errorf("environment %s is not json string", constant.CAERouterEtcd)
		return nil
	}
	if _, err := govalidator.ValidateStruct(caeRouterEtcdConfig); err != nil {
		log.GetLogger().Errorf("failed to validate the cae router etcd config: %s", err.Error())
		return nil
	}
	if len(caeRouterEtcdConfig.ServerList) == 0 {
		log.GetLogger().Warnf("empty cae router etcd server list, skip connect")
		return nil
	}
	return caeRouterEtcdConfig
}

func getCAEMetaEtcdConfig() *etcd3.ClientConfig {
	config := os.Getenv(constant.CAEMetaEtcd)
	if config == "" {
		log.GetLogger().Errorf("failed to get environment %s config", constant.CAEMetaEtcd)
		return nil
	}
	caeMetaEtcdConfig := &etcd3.ClientConfig{}
	err := json.Unmarshal([]byte(config), &caeMetaEtcdConfig)
	if err != nil {
		log.GetLogger().Errorf("environment %s is not json string", constant.CAEMetaEtcd)
		return nil
	}
	if _, err := govalidator.ValidateStruct(caeMetaEtcdConfig); err != nil {
		log.GetLogger().Errorf("failed to validate the cae meta etcd config: %s", err.Error())
		return nil
	}
	if len(caeMetaEtcdConfig.ServerList) == 0 {
		log.GetLogger().Warnf("empty cae meta etcd server list, skip connect")
		return nil
	}
	return caeMetaEtcdConfig
}

func getHTTPSConfig() tls.InternalHTTPSConfig {
	config := os.Getenv(constant.HTTPSConfig)
	httpsConfig := tls.InternalHTTPSConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &httpsConfig)
		if err != nil {
			log.GetLogger().Error("environment HTTPS_CONFIG is not json string")
			return tls.InternalHTTPSConfig{}
		}
		if _, err := govalidator.ValidateStruct(httpsConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the https config: %s", err.Error())
			return tls.InternalHTTPSConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment HTTPS_CONFIG config")
		return tls.InternalHTTPSConfig{}
	}
	return httpsConfig
}

func getLocalAuthConfig() localauth.AuthConfig {
	config := os.Getenv(constant.LocalAuth)
	localAuthConfig := localauth.AuthConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &localAuthConfig)
		if err != nil {
			log.GetLogger().Error("environment LOCAL_AUTH is not json string")
			return localauth.AuthConfig{}
		}
		if _, err := govalidator.ValidateStruct(localAuthConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the local auth config: %s", err.Error())
			return localauth.AuthConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment LOCAL_AUTH config")
		return localauth.AuthConfig{}
	}
	return localAuthConfig
}

func getLogConfig() LogConfig {
	config := os.Getenv(constant.Log)
	logConfig := LogConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &logConfig)
		if err != nil {
			log.GetLogger().Error("environment LOG is not json string")
			return LogConfig{}
		}
		if _, err := govalidator.ValidateStruct(logConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the log config: %s", err.Error())
			return LogConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment LOG config")
		return LogConfig{}
	}
	return logConfig
}

func getClusterConfig() ClusterConfig {
	config := os.Getenv(constant.Cluster)
	clusterConfig := ClusterConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &clusterConfig)
		if err != nil {
			log.GetLogger().Error("environment CLUSTER is not json string")
			return ClusterConfig{}
		}
		if _, err := govalidator.ValidateStruct(clusterConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the cluster config: %s", err.Error())
			return ClusterConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment CLUSTER config")
		return ClusterConfig{}
	}
	return clusterConfig
}

func getScaleDataConfig() ScaleData {
	config := os.Getenv(constant.ScaleData)
	scaleDataConfig := ScaleData{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &scaleDataConfig)
		if err != nil {
			log.GetLogger().Error("environment CLUSTER is not json string")
			return ScaleData{}
		}
		if _, err := govalidator.ValidateStruct(scaleDataConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the cluster config: %s", err.Error())
			return ScaleData{}
		}
	} else {
		log.GetLogger().Error("failed to get environment CLUSTER config")
		return ScaleData{}
	}
	return scaleDataConfig
}

func getSchedulerConfig() SchedulerConfig {
	config := os.Getenv(constant.Scheduler)
	schedulerConfig := SchedulerConfig{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &schedulerConfig)
		if err != nil {
			log.GetLogger().Error("environment SCHEDULER is not json string")
			return SchedulerConfig{}
		}
		if _, err := govalidator.ValidateStruct(schedulerConfig); err != nil {
			log.GetLogger().Errorf("failed to validate the scheduler config: %s", err.Error())
			return SchedulerConfig{}
		}
	} else {
		log.GetLogger().Error("failed to get environment SCHEDULER config")
		return SchedulerConfig{}
	}
	return schedulerConfig
}

func getStateCounterCfg() StateCounterCfg {
	config := os.Getenv(constant.StateCounterCfg)
	stateCounterCfg := StateCounterCfg{}
	if config != "" {
		err := json.Unmarshal([]byte(config), &stateCounterCfg)
		if err != nil {
			log.GetLogger().Error("environment STATE_COUNTER_CFG is not json string")
			return StateCounterCfg{}
		}
		if _, err := govalidator.ValidateStruct(stateCounterCfg); err != nil {
			log.GetLogger().Errorf("failed to validate the stateCounter config: %s", err.Error())
			return StateCounterCfg{}
		}
	} else {
		log.GetLogger().Error("failed to get environment STATE_COUNTER_CFG config")
		return StateCounterCfg{}
	}
	if len(stateCounterCfg.SiteClusters) == 0 {
		stateCounterCfg.SiteClusters = []string{}
		stateCounterCfg.SiteClusters = append(stateCounterCfg.SiteClusters, stateCounterCfg.ClusterID)
	}

	log.GetLogger().Infof("init state-manager conf siteCluster succeed, value: %v",
		stateCounterCfg.SiteClusters)
	return stateCounterCfg
}

func setStateCounterCfg(fConfig *FConfig) *FConfig {
	stateCounterCfg := getStateCounterCfg()
	fConfig.StateCounterCfg.Password = stateCounterCfg.Password
	fConfig.StateCounterCfg.ClusterID = stateCounterCfg.ClusterID
	fConfig.StateCounterCfg.EnableTLS = stateCounterCfg.EnableTLS
	fConfig.StateCounterCfg.PersistenceInterval = stateCounterCfg.PersistenceInterval
	fConfig.StateCounterCfg.ReleaseInterval = stateCounterCfg.ReleaseInterval
	fConfig.StateCounterCfg.ServerMode = stateCounterCfg.ServerMode
	fConfig.StateCounterCfg.SiteClusters = stateCounterCfg.SiteClusters
	return fConfig
}

// TransToClusterTenantHashTagKey -
func TransToClusterTenantHashTagKey(tenantCluster []string) []string {
	if len(clusterKeyByHashTag) == 0 {
		for _, cluster := range tenantCluster {
			clusterKeyByHashTag = append(clusterKeyByHashTag, cluster+HashTag)
		}
	}
	return clusterKeyByHashTag
}
