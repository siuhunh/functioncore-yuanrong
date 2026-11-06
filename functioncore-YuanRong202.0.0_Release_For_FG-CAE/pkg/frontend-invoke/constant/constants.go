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

// Package constant define constant of frontend-invoke
package constant

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/configutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/utils"
)

// constant string of true and false
const (
	TrueStr  = "true"
	FalseStr = "false"
)

const (
	// ConfigFilePath define Config File Path
	ConfigFilePath = "/home/sn/config/config.json"

	// MaxTraceIDLength is the max length of traceID
	MaxTraceIDLength = 128

	// DefaultLogFlag is the default flag for log
	DefaultLogFlag = "None"
	// DefaultResponseTimeOut 1200s
	DefaultResponseTimeOut = 1200 * time.Second

	// DefaultServerReadTimeOut 1300s
	DefaultServerReadTimeOut = 1300 * time.Second
	// DefaultServerWriteTimeOut 1300s
	DefaultServerWriteTimeOut = 1300 * time.Second
	// DefaultGraphReadBufferSize is default readBuffer size is 6k (4k for log)
	DefaultGraphReadBufferSize = 6 * 1024

	// RateLimitResetTime is the same as scale-in time of workers
	RateLimitResetTime = 900 * time.Second
	// DefaultInstanceLimitRate is default instance limit rate for traffic limitation
	DefaultInstanceLimitRate = 20000
	// DefaultInstanceBucketSize is default instance bucket size for traffic limitation
	DefaultInstanceBucketSize = 40000
	// DefaultFuncLimitRate is default function limit rate for traffic limitation
	DefaultFuncLimitRate = 2000
	// DefaultFuncBucketSize is default function bucket size for traffic limitation
	DefaultFuncBucketSize = 4000
	// DefaultTimeout define a defaultTimeout
	DefaultTimeout = 1200
	// DefaultTenantLimitRate  default tenant limit rate for traffic limitation
	DefaultTenantLimitRate = 1800
	// TrafficRedundantRate  limit redundancy rate for traffic limitation
	TrafficRedundantRate = 1.1
	// FunctionAccessorInitCopies Initial number of copies
	FunctionAccessorInitCopies = 3

	defaultLowerMemoryPercent        = 0.6
	defaultHighMemoryPercent         = 0.8
	defaultStatefulHighMemoryPercent = 0.85
	defaultMemoryRefreshInterval     = 20
	defaultBodyThreshold             = 40000

	defaultRequestMemoryEvaluator = 2

	defaultAcquireInstanceTimeout   = 250
	defaultConcurrencyThreshold     = 1
	defaultReleaseChanSize          = 2000
	defaultLeaseSpan                = 6000
	defaultLeaseCheckInterval       = 2000
	defaultSchedulerDelay           = 250
	defaultThreadPoolSize           = 10000
	defaultMaxInstanceThreshold     = 100
	defaultRequestNumThreshold      = 500
	defaultSyncReleaseTimeThreshold = 100
	defaultQPSThreshold             = 1000
	// heartbeat default config
	defaultHeartbeatTimeout          = 2
	defaultHeartbeatInterval         = 3
	defaultHeartbeatTimeoutThreshold = 3

	defaultE2EMaxDelay = 50
)

const (
	// AuthCheckEnable -
	AuthCheckEnable = "true"
	// WorkerInstancePrefix worker instance in etcd prefix
	WorkerInstancePrefix = "/sn/workers"
	// KeySeparator is the separator for key path
	KeySeparator = "/"

	frontendConfigEnvKey = "FRONTEND_CONFIG"
)

var (
	// FConfig define config struct
	FConfig = &Config{}

	// NativeAz -
	NativeAz   = ""
	loadAzOnce sync.Once

	configChangedCallback ConfigChangedCallback
	configWatcher         monitor.FileWatcher
)

// ConfigChangedCallback config change callback func
type ConfigChangedCallback func()

func initDefaultMemoryControlConfig() {
	if FConfig.MemoryControlConfig == nil {
		FConfig.MemoryControlConfig = &types.MemoryControlConfig{}
	}

	if FConfig.MemoryControlConfig.LowerMemoryPercent <= 0 {
		FConfig.MemoryControlConfig.LowerMemoryPercent = defaultLowerMemoryPercent
	}
	if FConfig.MemoryControlConfig.HighMemoryPercent <= 0 {
		FConfig.MemoryControlConfig.HighMemoryPercent = defaultHighMemoryPercent
	}
	if FConfig.MemoryControlConfig.StatefulHighMemPercent <= 0 {
		FConfig.MemoryControlConfig.StatefulHighMemPercent = defaultStatefulHighMemoryPercent
	}
	if FConfig.MemoryControlConfig.MemDetectIntervalMs <= 0 {
		FConfig.MemoryControlConfig.MemDetectIntervalMs = defaultMemoryRefreshInterval
	}
	if FConfig.MemoryControlConfig.BodyThreshold <= 0 {
		FConfig.MemoryControlConfig.BodyThreshold = defaultBodyThreshold
	}
}

func initDefaultMemoryEvaluatorConfig() {
	if FConfig.MemoryEvaluatorConfig == nil {
		FConfig.MemoryEvaluatorConfig = &MemoryEvaluatorConfig{}
	}
	if FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator <= 0 {
		FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator = defaultRequestMemoryEvaluator
	}
	log.GetLogger().Infof("RequestMemoryEvaluator %f", FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator)
}

func initDefaultLoadBalancerConfig() {
	if FConfig.LoadBalanceConfig == nil {
		FConfig.LoadBalanceConfig = &LoadBalanceConfig{}
	}
	if FConfig.LoadBalanceConfig.AcquireInstanceTimeout <= 0 {
		FConfig.LoadBalanceConfig.AcquireInstanceTimeout = defaultAcquireInstanceTimeout
	}

	if FConfig.LoadBalanceConfig.ConcurrencyThreshold == 0 {
		FConfig.LoadBalanceConfig.ConcurrencyThreshold = defaultConcurrencyThreshold
	}

	if FConfig.LoadBalanceConfig.ReleaseChanSize <= 0 {
		FConfig.LoadBalanceConfig.ReleaseChanSize = defaultReleaseChanSize
	}

	if FConfig.LoadBalanceConfig.LeaseSpan <= 0 {
		FConfig.LoadBalanceConfig.LeaseSpan = defaultLeaseSpan
	}

	if FConfig.LoadBalanceConfig.LeaseCheckInterval <= 0 {
		FConfig.LoadBalanceConfig.LeaseCheckInterval = defaultLeaseCheckInterval
	}

	if FConfig.LoadBalanceConfig.SchedulerDelay <= 0 {
		FConfig.LoadBalanceConfig.SchedulerDelay = defaultSchedulerDelay
	}

	if FConfig.LoadBalanceConfig.ThreadPoolSize <= 0 {
		FConfig.LoadBalanceConfig.ThreadPoolSize = defaultThreadPoolSize
	}

	if FConfig.LoadBalanceConfig.MaxInstanceThreshold <= 0 {
		FConfig.LoadBalanceConfig.MaxInstanceThreshold = defaultMaxInstanceThreshold
	}

	if atomic.LoadInt64(&FConfig.LoadBalanceConfig.MaxRequestNumThreshold) == 0 {
		atomic.StoreInt64(&FConfig.LoadBalanceConfig.MaxRequestNumThreshold, defaultRequestNumThreshold)
	}
	if atomic.LoadInt64(&FConfig.LoadBalanceConfig.SyncReleaseTimeThreshold) <= 0 {
		atomic.StoreInt64(&FConfig.LoadBalanceConfig.SyncReleaseTimeThreshold, defaultSyncReleaseTimeThreshold)
	}
	if atomic.LoadInt64(&FConfig.LoadBalanceConfig.QPSThreshold) <= 0 {
		atomic.StoreInt64(&FConfig.LoadBalanceConfig.QPSThreshold, defaultQPSThreshold)
	}
}

func initDefaultHeartbeatConfig() {
	if FConfig.HeartbeatConfig == nil {
		FConfig.HeartbeatConfig = &HeartbeatConfig{}
	}
	if FConfig.HeartbeatConfig.HeartbeatTimeout <= 0 {
		FConfig.HeartbeatConfig.HeartbeatTimeout = defaultHeartbeatTimeout
	}
	if FConfig.HeartbeatConfig.HeartbeatInterval <= 0 {
		FConfig.HeartbeatConfig.HeartbeatInterval = defaultHeartbeatInterval
	}
	if FConfig.HeartbeatConfig.HeartbeatTimeoutThreshold <= 0 {
		FConfig.HeartbeatConfig.HeartbeatTimeoutThreshold = defaultHeartbeatTimeoutThreshold
	}
}

// InitDefaultConfig init default config
func InitDefaultConfig() {
	initDefaultMemoryControlConfig()
	initDefaultMemoryEvaluatorConfig()
	initDefaultLoadBalancerConfig()
	initDefaultHeartbeatConfig()

	if FConfig.E2EMaxDelayTime <= 0 {
		FConfig.E2EMaxDelayTime = defaultE2EMaxDelay
	}
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

// InitConstant init constant
func InitConstant() error {
	configFromFile, err := loadConfig(ConfigFilePath)
	if err != nil {
		return err
	}

	configFromEnv, err := loadConfigFromEnv()
	if err != nil {
		log.GetLogger().Errorf("loadConfigFromEnv failed err %s", err)
		return err
	}

	FConfig = configFromEnv
	FConfig.MemoryEvaluatorConfig = configFromFile.MemoryEvaluatorConfig
	FConfig.MemoryControlConfig = configFromFile.MemoryControlConfig
	FConfig.LoadBalanceConfig = configFromFile.LoadBalanceConfig
	FConfig.MultiConcurrencySchedule = configFromFile.MultiConcurrencySchedule
	FConfig.EtcdLeaseConfig = &EtcdLeaseConfig{}

	log.GetLogger().Infof("init config success, authenticationEnable: %v", FConfig.AuthenticationEnable)

	utils.ValidateTimeout(&FConfig.HTTPConfig.RespTimeOut, DefaultTimeout)
	utils.ValidateTimeout(&FConfig.HTTPConfig.WorkerInstanceReadTimeOut, DefaultTimeout)
	if _, err = govalidator.ValidateStruct(FConfig); err != nil {
		log.GetLogger().Errorf("initConfigData error: %s", err.Error())
		return err
	}

	InitDefaultConfig()

	loadAzOnce.Do(func() {
		var exist bool
		NativeAz, exist = os.LookupEnv(FConfig.Runtime.AvailableZoneKey)
		if !exist || NativeAz == "" {
			NativeAz = constants.DefaultAZ
		}
		if len(NativeAz) > constants.ZoneNameLen {
			NativeAz = NativeAz[0 : constants.ZoneNameLen-1]
		}
	})
	return nil
}

func loadConfigFromEnv() (*Config, error) {
	configJSON := os.Getenv(frontendConfigEnvKey)
	config := &Config{}
	err := json.Unmarshal([]byte(configJSON), config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// WatchConfig describe config watch api
func WatchConfig(configPath string, stopCh <-chan struct{}, callback ConfigChangedCallback) error {
	watcher, err := monitor.CreateFileWatcher(stopCh)
	if err != nil {
		return err
	}

	configWatcher = watcher
	configChangedCallback = callback
	configWatcher.RegisterCallback(configPath, hotLoadConfig)
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

	hotLoadMemoryControlConfig(newConfig)
	hotLoadMemoryEvaluatorConfig(newConfig)
	hotLoadLoadBalancerConfig(newConfig)
	hotLoadEtcdLeaseConfig(newConfig)
	hotLoadEtcdConfig(newConfig)
	hotLoadHeartbeatConfig(newConfig)
	hotLoadE2EMaxDelayTime(newConfig)
	hotLoadMultiConcurrencyScheduleConfig(newConfig)

	if configChangedCallback != nil {
		configChangedCallback()
	}
}

func hotLoadMultiConcurrencyScheduleConfig(newAllConfig *Config) {
	FConfig.MultiConcurrencySchedule.FuncWhiteList = newAllConfig.MultiConcurrencySchedule.FuncWhiteList
	FConfig.MultiConcurrencySchedule.TenantWhiteList = newAllConfig.MultiConcurrencySchedule.TenantWhiteList

	oldCfg := atomic.LoadUint32(&FConfig.MultiConcurrencySchedule.DisableFaaSSchedule)
	newCfg := newAllConfig.MultiConcurrencySchedule.DisableFaaSSchedule
	if oldCfg != newCfg {
		atomic.StoreUint32(&FConfig.MultiConcurrencySchedule.DisableFaaSSchedule, newCfg)
		log.GetLogger().Infof("MultiConcurrencySchedule.DisableFaaSSchedule update from %d to %d", oldCfg, newCfg)
	}
}

func hotLoadE2EMaxDelayTime(newAllConfig *Config) {
	if newAllConfig.E2EMaxDelayTime > 0 && FConfig.E2EMaxDelayTime != newAllConfig.E2EMaxDelayTime {
		log.GetLogger().Infof("E2EMaxDelayTime update from %d to %d",
			FConfig.E2EMaxDelayTime, newAllConfig.E2EMaxDelayTime)
		FConfig.E2EMaxDelayTime = newAllConfig.E2EMaxDelayTime
	}
}

func hotLoadHeartbeatConfig(newAllConfig *Config) {
	if newAllConfig.HeartbeatConfig == nil {
		return
	}
	if FConfig.HeartbeatConfig == nil {
		FConfig.HeartbeatConfig = &HeartbeatConfig{}
	}
	newConfig := newAllConfig.HeartbeatConfig
	oldConfig := FConfig.HeartbeatConfig
	if newConfig.HeartbeatTimeout > 0 {
		oldConfig.HeartbeatTimeout = newConfig.HeartbeatTimeout
		log.GetLogger().Infof("heartbeat timeout updated, new: %d", newConfig.HeartbeatTimeout)
	}
	if newConfig.HeartbeatInterval > 0 {
		oldConfig.HeartbeatInterval = newConfig.HeartbeatInterval
		log.GetLogger().Infof("heartbeat interval updated, new: %d", newConfig.HeartbeatInterval)
	}
	if newConfig.HeartbeatTimeoutThreshold > 0 {
		oldConfig.HeartbeatTimeoutThreshold = newConfig.HeartbeatTimeoutThreshold
		log.GetLogger().Infof("heartbeat timeout threshold updated, new: %d", newConfig.HeartbeatTimeoutThreshold)
	}
}

func hotLoadEtcdLeaseConfig(newAllConfig *Config) {
	if newAllConfig.EtcdLeaseConfig == nil {
		return
	}
	if FConfig.EtcdLeaseConfig == nil {
		FConfig.EtcdLeaseConfig = &EtcdLeaseConfig{}
	}
	newConfig := newAllConfig.EtcdLeaseConfig
	oldConfig := FConfig.EtcdLeaseConfig
	if newConfig.LeaseTTL > 0 {
		oldConfig.LeaseTTL = newConfig.LeaseTTL
		log.GetLogger().Infof("LeaseTTL update, new: %d", newConfig.LeaseTTL)

	}
	if newConfig.RenewTTL > 0 {
		oldConfig.RenewTTL = newConfig.RenewTTL
		log.GetLogger().Infof("RenewTTL update, new: %d", newConfig.RenewTTL)
	}
}

func hotLoadEtcdConfig(newAllConfig *Config) {
	if newAllConfig.Etcd == nil {
		return
	}
	newConfig := newAllConfig.Etcd
	oldConfig := FConfig.Etcd
	if len(newConfig.ServerList) > 0 {
		oldConfig.ServerList = newConfig.ServerList
		log.GetLogger().Infof("etcd serverList update, new: %v", newConfig.ServerList)
	}
}

func hotLoadMemoryControlConfig(newAllConfig *Config) {
	if newAllConfig.MemoryControlConfig == nil {
		return
	}
	configutil.UpdateMemoryControlConfig(newAllConfig.MemoryControlConfig, FConfig.MemoryControlConfig)
}

func hotLoadMemoryEvaluatorConfig(newAllConfig *Config) {
	if newAllConfig.MemoryEvaluatorConfig == nil {
		return
	}
	oldConfig := FConfig.MemoryEvaluatorConfig
	newConfig := newAllConfig.MemoryEvaluatorConfig
	if newConfig.RequestMemoryEvaluator > 0 {
		log.GetLogger().Infof("RequestMemoryEvaluator update old: %f, new: %f",
			oldConfig.RequestMemoryEvaluator, newConfig.RequestMemoryEvaluator)
		oldConfig.RequestMemoryEvaluator = newConfig.RequestMemoryEvaluator
	}
}

func hotLoadLoadBalancerConfig(newAllConfig *Config) {
	if newAllConfig.LoadBalanceConfig == nil {
		return
	}
	oldConfig := FConfig.LoadBalanceConfig
	newConfig := newAllConfig.LoadBalanceConfig
	if newConfig.ConcurrencyThreshold != 0 {
		log.GetLogger().Infof("ConcurrencyThreshold update old: %d, new: %d",
			oldConfig.ConcurrencyThreshold, newConfig.ConcurrencyThreshold)
		oldConfig.ConcurrencyThreshold = newConfig.ConcurrencyThreshold
	}
	if newConfig.MaxInstanceThreshold != 0 {
		log.GetLogger().Infof("MaxInstanceThreshold update old: %d, new: %d",
			oldConfig.MaxInstanceThreshold, newConfig.MaxInstanceThreshold)
		oldConfig.MaxInstanceThreshold = newConfig.MaxInstanceThreshold
	}
	if newConfig.DisableBackpressureBalance != oldConfig.DisableBackpressureBalance {
		log.GetLogger().Infof("DisableBackpressureBalance update old: %d, new: %d",
			oldConfig.DisableBackpressureBalance, newConfig.DisableBackpressureBalance)
		oldConfig.DisableBackpressureBalance = newConfig.DisableBackpressureBalance
	}

	if newConfig.MaxRequestNumThreshold != 0 {
		log.GetLogger().Infof("MaxRequestNumThreshold update old: %d, new: %d",
			atomic.LoadInt64(&oldConfig.MaxRequestNumThreshold), newConfig.MaxRequestNumThreshold)
		atomic.StoreInt64(&oldConfig.MaxRequestNumThreshold, newConfig.MaxRequestNumThreshold)
	}

	if newConfig.SyncReleaseTimeThreshold != 0 {
		log.GetLogger().Infof("SyncReleaseTimeThreshold update old: %d, new: %d",
			atomic.LoadInt64(&oldConfig.SyncReleaseTimeThreshold), newConfig.SyncReleaseTimeThreshold)
		atomic.StoreInt64(&oldConfig.SyncReleaseTimeThreshold, newConfig.SyncReleaseTimeThreshold)
	}
	if newConfig.QPSThreshold != 0 {
		log.GetLogger().Infof("QPSThreshold update old: %d, new: %d",
			atomic.LoadInt64(&oldConfig.QPSThreshold), newConfig.QPSThreshold)
		atomic.StoreInt64(&oldConfig.QPSThreshold, newConfig.QPSThreshold)
	}
}

// RuntimeConfig config info
type RuntimeConfig struct {
	Port             string `json:"port" valid:",optional"`
	AvailableZoneKey string `json:"azkey,omitempty" valid:",optional"`
}

// RetryConfig define retry config
type RetryConfig struct {
	InstanceExceptionRetry bool `json:"instanceExceptionRetry" valid:"optional"`
}

// FrontendHTTP   Used to configure the ResponseTimeout
type FrontendHTTP struct {
	RespTimeOut               int64 `json:"resptimeout"  valid:",optional"`
	WorkerInstanceReadTimeOut int64 `json:"workerInstanceReadTimeOut"  valid:",optional"`
	// MaxRequestBodySize unit is M
	MaxRequestBodySize int `json:"maxRequestBodySize,string" valid:"required"`
}

// TrafficLimitParams parameters of traffic limitation
type TrafficLimitParams struct {
	InstanceLimitRate  float64 `json:"instanceLimitRate" valid:",optional"`
	InstanceBucketSize int     `json:"instanceBucketSize" valid:",optional"`
	FuncLimitRate      float64 `json:"funcLimitRate" valid:",optional"`
	FuncBucketSize     int     `json:"funcBucketSize" valid:",optional"`
}

// Config of frontend
type Config struct {
	Runtime               *RuntimeConfig           `json:"runtime" valid:"optional"`
	Etcd                  *etcd3.ClientConfig      `json:"etcd" valid:"optional"`
	RouterETCD            *etcd3.ClientConfig      `json:"routerETCD" valid:"optional"`
	CAEMetaETCD           *etcd3.ClientConfig      `json:"caeMetaEtcd" valid:"optional"`
	MetaEtcdProxy         *etcd3.ClientConfig      `json:"metaEtcdProxy" valid:"optional"`
	CAEMetaEtcdProxy      *etcd3.ClientConfig      `json:"caeMetaEtcdProxy" valid:"optional"`
	StateETCD             *etcd3.ClientConfig      `json:"stateEtcd" valid:"optional"`
	RedisConfig           RedisConfig              `json:"redisConfig" valid:"optional"`
	RetryConfig           *RetryConfig             `json:"retry" valid:"optional"`
	HTTPConfig            *FrontendHTTP            `json:"http" valid:"optional"`
	TrafficLimitParams    *TrafficLimitParams      `json:"trafficLimitParams" valid:"optional"`
	LocalAuth             *localauth.AuthConfig    `json:"localAuth" valid:"optional"`
	HTTPSConfig           *tls.InternalHTTPSConfig `json:"httpsConfig" valid:"optional"`
	AuthenticationEnable  bool                     `json:"authenticationEnable" valid:"optional"`
	FunctionNameSeparator string                   `json:"functionNameSeparator" valid:"optional"`
	// Configure the IP address and port number for reporting alarms
	AlarmServerAddress       string                         `json:"alarmServerAddress" valid:"optional"`
	InvokeMaxRetryTimes      int                            `json:"invokeMaxRetryTimes" valid:"optional"`
	MemoryControlConfig      *types.MemoryControlConfig     `json:"memoryControlConfig" valid:"optional"`
	MemoryEvaluatorConfig    *MemoryEvaluatorConfig         `json:"memoryEvaluatorConfig" valid:"optional"`
	LoadBalanceConfig        *LoadBalanceConfig             `json:"loadBalanceConfig" valid:"optional"`
	EtcdLeaseConfig          *EtcdLeaseConfig               `json:"etcdLeaseConfig" valid:"optional"`
	HeartbeatConfig          *HeartbeatConfig               `json:"heartbeatConfig" valid:"optional"`
	E2EMaxDelayTime          int64                          `json:"e2eMaxDelayTime" valid:"optional"`
	StreamConfig             StreamConfig                   `json:"streamConfig" valid:"optional"`
	MultiConcurrencySchedule MultiConcurrencyScheduleConfig `json:"multiConcurrencySchedule" valid:"optional"`
}

// MultiConcurrencyScheduleConfig -
type MultiConcurrencyScheduleConfig struct {
	DisableFaaSSchedule uint32            `json:"disableFaaSSchedule" valid:"optional"`
	TenantWhiteList     map[string]string `json:"tenantWhiteList" valid:"optional"`
	FuncWhiteList       map[string]string `json:"funcWhiteList" valid:"optional"`
}

// RedisConfig redis config
type RedisConfig struct {
	ClusterID   string                  `json:"clusterID,omitempty" valid:",optional"`
	ServerAddr  string                  `json:"serverAddr,omitempty" valid:",optional"`
	ServerMode  string                  `json:"serverMode,omitempty" valid:",optional"`
	Password    string                  `json:"password,omitempty" valid:",optional"`
	EnableTLS   bool                    `json:"enableTLS,omitempty" valid:",optional"`
	TimeoutConf redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
}

// EtcdLeaseConfig etcd lease config
type EtcdLeaseConfig struct {
	LeaseTTL int64 `yaml:"leaseTTL" valid:"optional"`
	RenewTTL int64 `yaml:"renewTTL" valid:"optional"`
}

// MemoryEvaluatorConfig memory evaluator config
type MemoryEvaluatorConfig struct {
	RequestMemoryEvaluator float64 `json:"requestMemoryEvaluator" valid:",optional"`
}

// LoadBalanceConfig -
type LoadBalanceConfig struct {
	AcquireInstanceTimeout     int64 `json:"acquireInstanceTimeout"  valid:",optional"`
	ConcurrencyThreshold       int   `json:"concurrencyThreshold" valid:"optional"`
	ReleaseChanSize            int   `json:"releaseChanSize" valid:"optional"`
	LeaseCheckInterval         int   `json:"leaseCheckInterval" valid:"optional"`
	LeaseSpan                  int   `json:"leaseSpan" valid:"optional"`
	SchedulerDelay             int   `json:"schedulerDelay" valid:"optional"`
	ThreadPoolSize             int   `json:"threadPoolSize" valid:"optional"`
	MaxInstanceThreshold       int   `json:"maxInstanceThreshold" valid:"optional"`
	DisableBackpressureBalance int   `json:"disableBackpressureBalance" valid:"optional"`
	MaxRequestNumThreshold     int64 `json:"maxRequestNumThreshold" valid:"optional"`
	SyncReleaseTimeThreshold   int64 `json:"syncReleaseTimeThreshold" valid:"optional"`
	QPSThreshold               int64 `json:"qpsThreshold" valid:"optional"`
}

// HeartbeatConfig heartbeat config
type HeartbeatConfig struct {
	HeartbeatTimeout          int `json:"heartbeatTimeout"  valid:",optional"`
	HeartbeatInterval         int `json:"heartbeatInterval" valid:"optional"`
	HeartbeatTimeoutThreshold int `json:"heartbeatTimeoutThreshold" valid:"optional"`
}

// StreamConfig stream handler config
type StreamConfig struct {
	HealthCheckPort string `json:"healthCheckPort"  valid:"optional"`
	StreamTransport string `json:"streamTransport"  valid:"optional"`
	ThreadPoolSize  int    `json:"threadPoolSize" valid:"optional"`
}
