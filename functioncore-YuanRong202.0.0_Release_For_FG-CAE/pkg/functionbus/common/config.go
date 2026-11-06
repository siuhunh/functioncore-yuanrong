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

// Package common config is used for etcd
package common

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/asaskevich/govalidator"

	utilities "functioncore/pkg/common/configutil"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
)

// FConfig used for etcd
var (
	FConfig = &Configs{}

	commonConfigPath = ""

	configChangedCallback ConfigChangedCallback
	configWatcher         monitor.FileWatcher
)

const (
	// AuthenticationEnableEnv -
	AuthenticationEnableEnv = "AUTHENTICATION_ENABLE"
	// LocalAuthConfigEnv -
	LocalAuthConfigEnv = "LOCAL_AUTH_CONFIG"
	// HTTPSConfigEnv -
	HTTPSConfigEnv = "HTTPS_CONFIG"
	// RequestConfigEnv -
	RequestConfigEnv = "REQUEST_CONFIG"
	// MetaETCDConfigEnv -
	MetaETCDConfigEnv = "META_ETCD_CONFIG"
	// RouterETCDConfigEnv -
	RouterETCDConfigEnv = "ROUTER_ETCD_CONFIG"
	// AlarmServerAddressEnv -
	AlarmServerAddressEnv = "ALARM_SERVER_ADDRESS"
	// MetricsServerEnv -
	MetricsServerEnv = "METRICS_SERVER_ENV"
	// BusWorkerServerEnv -
	BusWorkerServerEnv = "BUS_WORKER_SERVER_ENV"
	// BusRuntimeServerEnv env of bus server for runtime to connect
	BusRuntimeServerEnv = "BUS_RUNTIME_SERVER_ENV"
	// EnableDynamicKeepAlivePolicy -
	EnableDynamicKeepAlivePolicy = "ENABLE_DYNAMIC_KEEP_ALIVE_POLICY_ENV"
	// ReportResourceUsageTick -
	ReportResourceUsageTick = "REPORT_RESOURCE_USAGE_TICK"
	// ReportResourceUsageDataLen -
	ReportResourceUsageDataLen = "REPORT_RESOURCE_DATA_LEN"
	// LeastConnections -
	LeastConnections = "LC"
	// RoundRobin -
	RoundRobin = "RR"
	base       = 10
	bitSize    = 64
	// DynamicKeepAliveStartTime -
	DynamicKeepAliveStartTime = "DYNAMIC_KEEP_ALIVE_START_TIME_ENV"
	// DynamicKeepAliveRetry -
	DynamicKeepAliveRetry = "ENABLE_DYNAMIC_KEEP_ALIVE_RETRY_ENV"
)

var (
	alarmServerAddress = os.Getenv(AlarmServerAddressEnv)
)

// ConfigChangedCallback config change callback func
type ConfigChangedCallback func()

// HotLoadConfigCallback callback func of hotload config
type HotLoadConfigCallback func(*Configs)

var (
	workerManagerHealthCheckCallBack   HotLoadConfigCallback
	adjustKeepaliveDynamicallyCallBack HotLoadConfigCallback
)

// RegisterWorkerManagerHealthCheckCallBack -
func RegisterWorkerManagerHealthCheckCallBack(f HotLoadConfigCallback) {
	workerManagerHealthCheckCallBack = f
}

// TraceInfo trace info
type TraceInfo struct {
	TraceID     string
	TraceParent string
}

// RegisterAdjustKeepaliveDynamicallyCallBack -
func RegisterAdjustKeepaliveDynamicallyCallBack(f HotLoadConfigCallback) {
	adjustKeepaliveDynamicallyCallBack = f
}

// RequestConfig defines configs relevant to routing requests
type RequestConfig struct {
	RequestWorkerTimeout  int `json:"requestWorkerTimeout" valid:"optional"`
	InvokerRequestTimeout int `json:"invokerRequestTimeout" valid:"optional"`
}

// AutoScalerConfig -
type AutoScalerConfig struct {
	UserTolerateDelayRatio      float32 `json:"userTolerateDelayRatio" valid:"optional"`
	MaxUserTolerateDelayMs      int     `json:"maxUserTolerateDelayMs" valid:"optional"`
	MaxEvaluatedLatencyMs       int     `json:"maxEvaluatedLatencyMs" valid:"optional"`
	StableScaleLimitPerFunction int     `json:"stableScaleLimitPerFunction" valid:"optional"`
	BurstScaleLimitPerFunction  int     `json:"burstScaleLimitPerFunction" valid:"optional"`
	ScaleEvaluateWindowLengthMs int     `json:"scaleEvaluateWindowLengthMs" valid:"optional"`
	MaxEvaluateScaleLatencyMs   int     `json:"maxEvaluateScaleLatencyMs" valid:"optional"`
	ScaleCalculationIntervalMs  int     `json:"scaleCalculationIntervalMs" valid:"optional"`
	IgnorePendingRequests       bool    `json:"ignorePendingRequests" valid:"optional"`

	RequestLatencyAggregationThreshold int `json:"requestLatencyAggregationThreshold" valid:"optional"`

	WithInstanceQueueTimeout     int      `json:"withInstanceQueueTimeout" valid:"optional"`
	MaxInstanceQueueTimeout      int      `json:"maxInstanceQueueTimeout" valid:"optional"`
	BreakBalanceQueueLengthLimit int      `json:"breakBalanceQueueLengthLimit" valid:"optional"`
	DisAbleReportFunctionList    []string `json:"disAbleReportFunctionList" valid:"optional"`
}

// WorkerMetricsTimeWindow defines configs to collect metrics of time window
type WorkerMetricsTimeWindow struct {
	TimeWindowLength int `json:"TimeWindowLength" valid:"optional"`
	NumberThreshold  int `json:"numberThreshold" valid:"optional"`
}

// MemoryEvaluatorConfig memory evaluator config
type MemoryEvaluatorConfig struct {
	RequestMemoryEvaluator  float64 `json:"requestMemoryEvaluator" valid:",optional"`
	ResponseMemoryEvaluator float64 `json:"responseMemoryEvaluator" valid:",optional"`
}

// AlarmConfig alarm config
type AlarmConfig struct {
	RouterEtcdLeaseDetectInterval int `json:"routerEtcdLeaseDetectInterval" valid:",optional"`
	RouterEtcdLeaseTimeout        int `json:"routerEtcdLeaseTimeout" valid:",optional"`
}

// BusServerConfig bus worker server config
type BusServerConfig struct {
	Protocol  string `yaml:"protocol" valid:"optional"`
	EnableTLS bool   `yaml:"enableTLS,omitempty" valid:",optional"`
}

// EtcdLeaseConfig etcd lease config
type EtcdLeaseConfig struct {
	LeaseTTL int `yaml:"leaseTTL" valid:"optional"`
	RenewTTL int `yaml:"renewTTL" valid:"optional"`
}

// WorkerManagerHealthCheck config of worker-manager health check
type WorkerManagerHealthCheck struct {
	Timeout   int  `json:"timeout" valid:"optional"`
	Interval  int  `json:"interval" valid:"optional"`
	Threshold int  `json:"threshold" valid:"optional"`
	Disable   bool `json:"disable" valid:"optional"`
}

// Configs bus configs
type Configs struct {
	EtcdConfig                   *etcd3.ClientConfig      `json:"etcd" valid:"optional"`
	RouterETCDConfig             *etcd3.ClientConfig      `json:"routerETCD" valid:"optional"`
	CAEMetaEtcdConfig            *etcd3.ClientConfig      `json:"-" valid:"optional"`
	CAERouterEtcdConfig          *etcd3.ClientConfig      `json:"-" valid:"optional"`
	MetaEtcdProxyConfig          *etcd3.ClientConfig      `json:"metaEtcdProxyConfig" valid:"optional"`
	CAEMetaEtcdProxyConfig       *etcd3.ClientConfig      `json:"caeMetaEtcdProxyConfig" valid:"optional"`
	EtcdLeaseConfig              *EtcdLeaseConfig         `json:"etcdLeaseConfig" valid:"optional"`
	RequestConfig                *RequestConfig           `json:"requestConfig" valid:"optional"`
	LocalAuth                    *localauth.AuthConfig    `json:"localAuth" valid:"optional"`
	HTTPSConfig                  *tls.InternalHTTPSConfig `json:"httpsConfig" valid:"optional"`
	MicroServiceSchedulingPolicy string                   `json:"msSchedulingPolicy" valid:"optional"`
	MetricsServerConfig          *MetricsServerConfig     `json:"metricsServer" valid:"optional"`
	WorkerMetricsTimeWindow      *WorkerMetricsTimeWindow `json:"workerMetricsTimeWindow" valid:"optional"`
	AutoScalerConfig             *AutoScalerConfig        `json:"autoScalerConfig" valid:"optional"`
	AuthenticationEnable         bool                     `json:"authenticationEnable" valid:"optional"`
	ConnectionAuthCheck          bool                     `json:"connectionAuthCheck,omitempty" valid:"optional"`
	IdleWorkerTimeout            int                      `json:"idleWorkerTimeout,omitempty" valid:",optional"`
	RequestQueueTimeout          int                      `json:"requestQueueTimeout,omitempty" valid:",optional"`
	DisableDialTimeout           int                      `json:"disableDialTimeout,omitempty" valid:"optional"`
	// Configure the IP address and port number for reporting alarms
	AlarmServerAddress         string                     `json:"alarmServerAddress" valid:"optional"`
	MaxRequestBodySize         int                        `json:"maxRequestBodySize" valid:"optional"`
	MemoryControlConfig        *types.MemoryControlConfig `json:"memoryControlConfig" valid:"optional"`
	MemoryEvaluatorConfig      *MemoryEvaluatorConfig     `json:"memoryEvaluatorConfig" valid:"optional"`
	AlarmConfig                *AlarmConfig               `json:"alarmConfig" valid:"optional"`
	BusWorkerServerConfig      *BusServerConfig           `json:"invokeServer" valid:"optional"`
	BusRuntimeServerConfig     *BusServerConfig           `json:"runtimeServer"`
	EtcdCacheConfig            *etcd3.EtcdCacheConfig     `json:"etcd_cache_config" valid:"optional"`
	WMHealthCheck              WorkerManagerHealthCheck   `json:"wmHealthCheck" valid:"optional"`
	ReportResourceUsageTick    int64                      `json:"reportResourceUsageTick" valid:"optional"`
	ReportResourceUsageDataLen int64                      `json:"reportResourceUsageDataLen" valid:"optional"`
	MaxRetryTime               int32                      `json:"maxRetryTime"`
	MaxWorkerDelay             int64                      `json:"maxWorkerDelay" valid:"optional"`
	// dynamic adjust keep-alive time for function instances
	DynamicKeepAliveConfig *DynamicKeepAliveConfig `json:"dynamicKeepAliveConfig" valid:"optional"`
}

// DynamicKeepAliveConfig -
type DynamicKeepAliveConfig struct {
	Enable        bool   `json:"enable" valid:"optional"`
	StartTime     int    `json:"startTime" valid:"optional"`
	StartTimeCron string `json:"-"`
	Retry         int    `json:"retry" valid:"optional"`
}

// MetricsServerConfig -
type MetricsServerConfig struct {
	ServerAddr  string                  `json:"serverAddr,omitempty" valid:",optional"`
	ServerMode  string                  `json:"serverMode,omitempty" valid:",optional"`
	Password    string                  `json:"password,omitempty" valid:",optional"`
	EnableTLS   bool                    `json:"enableTLS,omitempty" valid:",optional"`
	TimeoutConf redisclient.TimeoutConf `json:"timeoutConf,omitempty" valid:",optional"`
}

// InitConfig get config info from config.json
func InitConfig(configPath string) error {
	FConfig = &Configs{
		AuthenticationEnable:       strings.EqualFold(strings.TrimSpace(os.Getenv(AuthenticationEnableEnv)), "true"),
		LocalAuth:                  getLocalAuthConfig(),
		HTTPSConfig:                getHTTPSConfig(),
		RequestConfig:              getRequestConfig(),
		EtcdConfig:                 getMetaEtcdConfig(),
		RouterETCDConfig:           getRouterEtcdConfig(),
		CAEMetaEtcdConfig:          getCAEMetaEtcdConfig(),
		CAERouterEtcdConfig:        getCAERouterEtcdConfig(),
		MetaEtcdProxyConfig:        getMetaEtcdProxyConfig(),
		CAEMetaEtcdProxyConfig:     getCAEMetaEtcdProxyConfig(),
		AlarmServerAddress:         alarmServerAddress,
		MetricsServerConfig:        getMetricsServerConfig(),
		BusWorkerServerConfig:      getBusProxyServerConfig(),
		BusRuntimeServerConfig:     getBusRuntimeServerConfig(),
		EtcdLeaseConfig:            &EtcdLeaseConfig{},
		EtcdCacheConfig:            &etcd3.EtcdCacheConfig{},
		DynamicKeepAliveConfig:     getDynamicKeepAliveConfig(),
		ReportResourceUsageTick:    getReportResourceUsageTickConfig(),
		ReportResourceUsageDataLen: getReportResourceUsageDataLenConfig(),
	}
	InitDefaultConfig()
	// preload the configuration that needs to be hot loaded
	hotLoadConfig(configPath, monitor.Create)
	_, err := govalidator.ValidateStruct(FConfig)
	if err != nil {
		return err
	}
	fmt.Println("AuthenticationEnable:", FConfig.AuthenticationEnable)
	return nil
}

// IsCAERouterEtcdDefined Judge whether CAE Router ETCD is configured
func IsCAERouterEtcdDefined() bool {
	return FConfig.CAERouterEtcdConfig != nil && len(FConfig.CAERouterEtcdConfig.ServerList) > 0
}

func getDynamicKeepAliveConfig() *DynamicKeepAliveConfig {
	enableDynamicKeepAlivePolicy := false
	enableDynamicKeepAlivePolicyStr := os.Getenv(EnableDynamicKeepAlivePolicy)
	if enableDynamicKeepAlivePolicyStr != "" {
		enableDynamicKeepAlivePolicy = strings.EqualFold(strings.TrimSpace(enableDynamicKeepAlivePolicyStr), "true")
	}
	dynamicKeepAliveStartTime := DynamicKeepAliveTimeDefaultStartTime
	if envStartTimeStr := os.Getenv(DynamicKeepAliveStartTime); envStartTimeStr != "" {
		envStartTimeInt, err := strconv.Atoi(envStartTimeStr)
		if err == nil && envStartTimeInt >= 0 && envStartTimeInt < DynamicKeepAliveTimeHour {
			dynamicKeepAliveStartTime = envStartTimeInt
		}
	}
	dynamicKeepAliveRetry := DynamicKeepAliveTimeDefaultRetry
	if envRetry := os.Getenv(DynamicKeepAliveRetry); envRetry != "" {
		envRetry, err := strconv.Atoi(envRetry)
		if err == nil && envRetry >= 0 && envRetry < DynamicKeepAliveTimeHour-dynamicKeepAliveStartTime {
			dynamicKeepAliveRetry = envRetry
		}
	}
	dynamicKeepAliveStartTimeCron := strconv.Itoa(dynamicKeepAliveStartTime) + DynamicKeepAliveTimeCronSuffix
	return &DynamicKeepAliveConfig{
		Enable:        enableDynamicKeepAlivePolicy,
		StartTime:     dynamicKeepAliveStartTime,
		Retry:         dynamicKeepAliveRetry,
		StartTimeCron: dynamicKeepAliveStartTimeCron,
	}
}

func getReportResourceUsageTickConfig() int64 {
	reportResourceUsageTickEnv := os.Getenv(ReportResourceUsageTick)
	if reportResourceUsageTickEnv == "" {
		log.GetLogger().Warnf("failed to get ReportResourceUsageTick from env")
		return 0
	}

	reportResourceUsageTick, err := strconv.ParseInt(reportResourceUsageTickEnv, base, bitSize)
	if err != nil {
		log.GetLogger().Warnf("failed to convert ReportResourceUsageTick to int:%s", reportResourceUsageTickEnv)
		return 0
	}

	if reportResourceUsageTick <= 0 {
		return 0
	}
	return reportResourceUsageTick
}

func getReportResourceUsageDataLenConfig() int64 {
	reportResourceUsageDataLenEnv := os.Getenv(ReportResourceUsageDataLen)
	if reportResourceUsageDataLenEnv == "" {
		log.GetLogger().Warnf("failed to get ReportResourceUsageTick from env")
		return 0
	}

	reportResourceUsageDataLen, err := strconv.ParseInt(reportResourceUsageDataLenEnv, base, bitSize)
	if err != nil {
		log.GetLogger().Warnf("failed to convert reportResourceUsageDataLen to int:%s", reportResourceUsageDataLen)
		return 0
	}

	if reportResourceUsageDataLen <= 0 {
		return 0
	}
	return reportResourceUsageDataLen
}

func getBusProxyServerConfig() *BusServerConfig {
	datas := os.Getenv(BusWorkerServerEnv)
	config := &BusServerConfig{}
	if err := json.Unmarshal([]byte(datas), config); err != nil {
		log.GetLogger().Warnf("failed to unmarshal busWorker server config, use default, err: %s", err.Error())
		return config
	}
	return config
}

func getBusRuntimeServerConfig() *BusServerConfig {
	datas := os.Getenv(BusRuntimeServerEnv)
	config := &BusServerConfig{}
	if err := json.Unmarshal([]byte(datas), config); err != nil {
		log.GetLogger().Warnf("failed to unmarshal busWorker server config, use default, err: %s", err.Error())
		return config
	}
	return config
}

func getMetricsServerConfig() *MetricsServerConfig {
	datas := os.Getenv(MetricsServerEnv)
	config := new(MetricsServerConfig)
	if err := json.Unmarshal([]byte(datas), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal metaETCD config, err: %s", err.Error())
		return nil
	}
	return config
}

func getRouterEtcdConfig() *etcd3.ClientConfig {
	datas := os.Getenv(RouterETCDConfigEnv)
	config := new(etcd3.ClientConfig)
	if err := json.Unmarshal([]byte(datas), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal metaETCD config, err: %s", err.Error())
		return nil
	}
	return config
}

func getMetaEtcdConfig() *etcd3.ClientConfig {
	datas := os.Getenv(MetaETCDConfigEnv)
	config := new(etcd3.ClientConfig)
	if err := json.Unmarshal([]byte(datas), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal metaETCD config, err: %s", err.Error())
		return nil
	}
	return config
}

func getCAEMetaEtcdConfig() *etcd3.ClientConfig {
	config := new(etcd3.ClientConfig)
	if err := json.Unmarshal([]byte(os.Getenv("CAE_META_ETCD_CONFIG")), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal caeMetaETCD config, err: %s", err.Error())
		return nil
	}
	if len(config.ServerList) == 0 {
		log.GetLogger().Warnf("empty caeMetaETCD server list, skip")
		return nil
	}
	return config
}

func getMetaEtcdProxyConfig() *etcd3.ClientConfig {
	config := new(etcd3.ClientConfig)
	if err := json.Unmarshal([]byte(os.Getenv("META_ETCD_PROXY_CONFIG")), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal meta etcd proxy config, err: %s", err.Error())
		return nil
	}
	if len(config.ServerList) == 0 {
		log.GetLogger().Warnf("empty meta etcd proxy server list, skip")
		return nil
	}
	return config
}

func getCAEMetaEtcdProxyConfig() *etcd3.ClientConfig {
	config := new(etcd3.ClientConfig)
	if err := json.Unmarshal([]byte(os.Getenv("CAE_META_ETCD_PROXY_CONFIG")), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal cae meta etcd proxy config, err: %s", err.Error())
		return nil
	}
	if len(config.ServerList) == 0 {
		log.GetLogger().Warnf("empty cae meta etcd proxy server list, skip")
		return nil
	}
	return config
}

func getCAERouterEtcdConfig() *etcd3.ClientConfig {
	config := new(etcd3.ClientConfig)
	if err := json.Unmarshal([]byte(os.Getenv("CAE_ROUTER_ETCD_CONFIG")), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal caeRouterETCD config, err: %s", err.Error())
		return nil
	}
	if len(config.ServerList) == 0 {
		log.GetLogger().Warnf("empty caeRouterETCD server list, skip")
		return nil
	}
	return config
}

func getRequestConfig() *RequestConfig {
	datas := os.Getenv(RequestConfigEnv)
	config := new(RequestConfig)
	if err := json.Unmarshal([]byte(datas), config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal request config, err: %s", err.Error())
		return nil
	}
	return config
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

// GetHotloadMetricsConf  get real-time config info from config.json
func GetHotloadMetricsConf() (string, redisclient.TimeoutConf, error) {
	data, err := ioutil.ReadFile(commonConfigPath)
	if err != nil {
		return "", redisclient.GetDefaultTimeoutConf(), err
	}
	tmpConfig := &Configs{}
	err = json.Unmarshal(data, tmpConfig)
	if err != nil {
		return "", redisclient.GetDefaultTimeoutConf(), err
	}
	_, err = govalidator.ValidateStruct(tmpConfig)
	if err != nil {
		return "", redisclient.GetDefaultTimeoutConf(), err
	}
	log.GetLogger().Infof("get serverAddr: %s, timeoutConf: %v", tmpConfig.MetricsServerConfig.ServerAddr,
		tmpConfig.MetricsServerConfig.TimeoutConf)
	return tmpConfig.MetricsServerConfig.ServerAddr, tmpConfig.MetricsServerConfig.TimeoutConf, nil
}

func loadConfig(configPath string) (*Configs, error) {
	standardConfigPath, err := filepath.Abs(configPath)
	if err != nil {
		return nil, err
	}
	commonConfigPath = standardConfigPath
	data, err := ioutil.ReadFile(standardConfigPath)
	if err != nil {
		return nil, err
	}

	config := &Configs{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}
	_, err = govalidator.ValidateStruct(config)
	if err != nil {
		return nil, err
	}
	return config, nil
}

// InitDefaultAutoScalerConfig init default auto scaler config
func InitDefaultAutoScalerConfig() {
	if FConfig.AutoScalerConfig == nil {
		FConfig.AutoScalerConfig = &AutoScalerConfig{}
	}

	config := FConfig.AutoScalerConfig
	if config.MaxEvaluateScaleLatencyMs == 0 {
		config.MaxEvaluateScaleLatencyMs = defaultMaxEvaluatedScaleLatencyMs
	}
	if config.MaxEvaluatedLatencyMs == 0 {
		config.MaxEvaluatedLatencyMs = defaultEvaluatedMaxLatencyMs
	}
	if config.ScaleEvaluateWindowLengthMs == 0 {
		config.ScaleEvaluateWindowLengthMs = defaultScaleEvaluateWindowLengthMs
	}
	if config.StableScaleLimitPerFunction == 0 {
		config.StableScaleLimitPerFunction = defaultStableScaleLimitPerFunction
	}
	if config.BurstScaleLimitPerFunction == 0 {
		config.BurstScaleLimitPerFunction = defaultBurstScaleLimitPerFunction
	}
	if config.MaxUserTolerateDelayMs == 0 {
		config.MaxUserTolerateDelayMs = defaultMaxUserTolerateDelayMs
	}
	if config.ScaleCalculationIntervalMs == 0 {
		config.ScaleCalculationIntervalMs = defaultScaleCalculationIntervalMs
	}
	if config.WithInstanceQueueTimeout <= 0 {
		config.WithInstanceQueueTimeout = defaultWithInstanceQueueTimeout
	}
	if config.MaxInstanceQueueTimeout <= 0 {
		config.MaxInstanceQueueTimeout = defaultMaxInstanceQueueTimeout
	}
	if config.BreakBalanceQueueLengthLimit == 0 {
		config.BreakBalanceQueueLengthLimit = defaultBreakBalanceQueueLengthLimit
	}
}

// InitDefaultConfig init default config
func InitDefaultConfig() {
	initDefaultInternalConfig()
	initDefaultMemoryControlConfig()
	initDefaultMemoryEvaluatorConfig()
	InitDefaultAutoScalerConfig()
	initDefaultAlarmConfig()

	if FConfig.MaxRetryTime <= 0 {
		FConfig.MaxRetryTime = defaultMaxRetryTime
	}
	if FConfig.MaxWorkerDelay <= 0 {
		FConfig.MaxWorkerDelay = defaultMaxWorkerDelay
	}
}

func initDefaultInternalConfig() {
	if FConfig.IdleWorkerTimeout <= 0 {
		FConfig.IdleWorkerTimeout = defaultIdleWorkerTimeout
	}
	if FConfig.DisableDialTimeout <= 0 {
		FConfig.DisableDialTimeout = 0
	}
}

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
		FConfig.MemoryControlConfig.StatefulHighMemPercent = defaultStatefulHighMemPercent
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
	if FConfig.MemoryEvaluatorConfig.ResponseMemoryEvaluator <= 0 {
		FConfig.MemoryEvaluatorConfig.ResponseMemoryEvaluator = defaultResponseMemoryEvaluator
	}
	log.GetLogger().Infof("RequestMemoryEvaluator %f, ResponseMemoryEvaluator %f",
		FConfig.MemoryEvaluatorConfig.RequestMemoryEvaluator,
		FConfig.MemoryEvaluatorConfig.ResponseMemoryEvaluator)
}

func initDefaultAlarmConfig() {
	if FConfig.AlarmConfig == nil {
		FConfig.AlarmConfig = &AlarmConfig{}
	}

	if FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval <= 0 {
		FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval = defaultLeaseDetectInterval
	}

	if FConfig.AlarmConfig.RouterEtcdLeaseTimeout <= 0 {
		FConfig.AlarmConfig.RouterEtcdLeaseTimeout = defaultRouterEtcdLeaseTimeoutSec
	}
	log.GetLogger().Infof("RouterEtcdLeaseDetectInterval %d, RouterEtcdLeaseTimeoutSec %d",
		FConfig.AlarmConfig.RouterEtcdLeaseDetectInterval,
		FConfig.AlarmConfig.RouterEtcdLeaseTimeout)
}

// preload the configuration of redis server address
func preloadMetricsServerConfig(newConfig *Configs) {
	if newConfig.MetricsServerConfig == nil || FConfig.MetricsServerConfig == nil ||
		newConfig.MetricsServerConfig.ServerAddr == "" {
		return
	}
	log.GetLogger().Infof("MetricsServerConfig serverAddr is: %s", newConfig.MetricsServerConfig.ServerAddr)
	FConfig.MetricsServerConfig.ServerAddr = newConfig.MetricsServerConfig.ServerAddr
	FConfig.MetricsServerConfig.TimeoutConf = newConfig.MetricsServerConfig.TimeoutConf
}

func hotLoadInternalConfig(newConfig *Configs) {
	if newConfig.IdleWorkerTimeout > 0 {
		log.GetLogger().Infof("IdleWorkerTimeout update old: %d, new: %d",
			FConfig.IdleWorkerTimeout, newConfig.IdleWorkerTimeout)
		FConfig.IdleWorkerTimeout = newConfig.IdleWorkerTimeout
	}
	if newConfig.DisableDialTimeout != FConfig.DisableDialTimeout {
		log.GetLogger().Infof("DisableDialTimeout update old: %d, new: %d",
			FConfig.DisableDialTimeout, newConfig.DisableDialTimeout)
		FConfig.DisableDialTimeout = newConfig.DisableDialTimeout
	}
}

func hotLoadMicroServiceSchedulingPolicy(newConfig *Configs) {
	log.GetLogger().Debugf("MicroServiceSchedulingPolicy update new: %s", newConfig.MicroServiceSchedulingPolicy)
	if newConfig.MicroServiceSchedulingPolicy == "" ||
		(newConfig.MicroServiceSchedulingPolicy != RoundRobin &&
			newConfig.MicroServiceSchedulingPolicy != LeastConnections) {
		newConfig.MicroServiceSchedulingPolicy = RoundRobin
	}

	oldConfig := FConfig.MicroServiceSchedulingPolicy
	config := newConfig.MicroServiceSchedulingPolicy
	if oldConfig != config {
		log.GetLogger().Infof("MicroServiceSchedulingPolicy update old: %s, new: %s",
			oldConfig, config)
		oldConfig = config
	}
}

func hotLoadMemoryControlConfig(newAllConfig *Configs) {
	if newAllConfig.MemoryControlConfig == nil {
		return
	}
	utilities.UpdateMemoryControlConfig(newAllConfig.MemoryControlConfig, FConfig.MemoryControlConfig)
}

func hotLoadMaxRetryTime(newAllConfig *Configs) {
	if newAllConfig.MaxRetryTime <= 0 {
		return
	}
	old := atomic.LoadInt32(&FConfig.MaxRetryTime)
	if newAllConfig.MaxRetryTime != old {
		log.GetLogger().Infof("update MaxRetryTime from %d to %d", old, newAllConfig.MaxRetryTime)
		atomic.StoreInt32(&FConfig.MaxRetryTime, newAllConfig.MaxRetryTime)
	}
}

func hotLoadMemoryEvaluatorConfig(newAllConfig *Configs) {
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
	if newConfig.ResponseMemoryEvaluator > 0 {
		log.GetLogger().Infof("ResponseMemoryEvaluator update old: %f, new: %f",
			oldConfig.ResponseMemoryEvaluator, newConfig.ResponseMemoryEvaluator)
		oldConfig.ResponseMemoryEvaluator = newConfig.ResponseMemoryEvaluator
	}
}

func hotLoadAutoScalerConfig(newAllConfig *Configs) {
	if newAllConfig.AutoScalerConfig == nil {
		return
	}
	oldConfig := FConfig.AutoScalerConfig
	newConfig := newAllConfig.AutoScalerConfig
	if newConfig.MaxEvaluateScaleLatencyMs > 0 {
		log.GetLogger().Infof("MaxEvaluateScaleLatencyMs updated old: %d, new: %d",
			oldConfig.MaxEvaluateScaleLatencyMs, newConfig.MaxEvaluateScaleLatencyMs)
		oldConfig.MaxEvaluateScaleLatencyMs = newConfig.MaxEvaluateScaleLatencyMs
	}
	if newConfig.MaxEvaluatedLatencyMs > 0 {
		log.GetLogger().Infof("MaxEvaluatedLatencyMs updated old: %d, new: %d",
			oldConfig.MaxEvaluatedLatencyMs, newConfig.MaxEvaluatedLatencyMs)
		oldConfig.MaxEvaluatedLatencyMs = newConfig.MaxEvaluatedLatencyMs
	}
	if newConfig.ScaleEvaluateWindowLengthMs > 0 {
		log.GetLogger().Infof("ScaleEvaluateWindowLengthMs updated old: %d, new: %d",
			oldConfig.ScaleEvaluateWindowLengthMs, newConfig.ScaleEvaluateWindowLengthMs)
		oldConfig.ScaleEvaluateWindowLengthMs = newConfig.ScaleEvaluateWindowLengthMs
	}
	if newConfig.StableScaleLimitPerFunction > 0 {
		log.GetLogger().Infof("StableScaleLimitPerFunction updated old: %d, new: %d",
			oldConfig.StableScaleLimitPerFunction, newConfig.StableScaleLimitPerFunction)
		oldConfig.StableScaleLimitPerFunction = newConfig.StableScaleLimitPerFunction
	}
	if newConfig.BurstScaleLimitPerFunction > 0 {
		log.GetLogger().Infof("BurstScaleLimitPerFunction updated old: %d, new: %d",
			oldConfig.BurstScaleLimitPerFunction, newConfig.BurstScaleLimitPerFunction)
		oldConfig.BurstScaleLimitPerFunction = newConfig.BurstScaleLimitPerFunction
	}
	if newConfig.MaxUserTolerateDelayMs > 0 {
		log.GetLogger().Infof("MaxUserTolerateDelayMs updated old: %d, new: %d",
			oldConfig.MaxUserTolerateDelayMs, newConfig.MaxUserTolerateDelayMs)
		oldConfig.MaxUserTolerateDelayMs = newConfig.MaxUserTolerateDelayMs
	}
	if newConfig.ScaleCalculationIntervalMs > 0 {
		log.GetLogger().Infof("ScaleCalculationIntervalMs updated old: %d, new: %d",
			oldConfig.ScaleCalculationIntervalMs, newConfig.ScaleCalculationIntervalMs)
		oldConfig.ScaleCalculationIntervalMs = newConfig.ScaleCalculationIntervalMs
	}
	if newConfig.UserTolerateDelayRatio > 0 {
		log.GetLogger().Infof("UserTolerateDelayRatio updated old: %f, new: %f",
			oldConfig.UserTolerateDelayRatio, newConfig.UserTolerateDelayRatio)
		oldConfig.UserTolerateDelayRatio = newConfig.UserTolerateDelayRatio
	}
	if newConfig.IgnorePendingRequests != oldConfig.IgnorePendingRequests {
		log.GetLogger().Infof("IgnorePendingRequests updated old: %t, new: %t",
			oldConfig.IgnorePendingRequests, newConfig.IgnorePendingRequests)
		oldConfig.IgnorePendingRequests = newConfig.IgnorePendingRequests
	}
	if newConfig.WithInstanceQueueTimeout != oldConfig.WithInstanceQueueTimeout {
		log.GetLogger().Infof("WithInstanceQueueTimeout updated old: %d, new: %d",
			oldConfig.WithInstanceQueueTimeout, newConfig.WithInstanceQueueTimeout)
		oldConfig.WithInstanceQueueTimeout = newConfig.WithInstanceQueueTimeout
	}
	if newConfig.MaxInstanceQueueTimeout != oldConfig.MaxInstanceQueueTimeout {
		log.GetLogger().Infof("MaxInstanceQueueTimeout updated old: %d, new: %d",
			oldConfig.MaxInstanceQueueTimeout, newConfig.MaxInstanceQueueTimeout)
		oldConfig.MaxInstanceQueueTimeout = newConfig.MaxInstanceQueueTimeout
	}
	if len(newConfig.DisAbleReportFunctionList) != len(oldConfig.DisAbleReportFunctionList) {
		log.GetLogger().Infof("DisAbleReportFunctionList updated old: %v, new: %v",
			oldConfig.DisAbleReportFunctionList, newConfig.DisAbleReportFunctionList)
		oldConfig.DisAbleReportFunctionList = newConfig.DisAbleReportFunctionList
	}
	if newConfig.BreakBalanceQueueLengthLimit != oldConfig.BreakBalanceQueueLengthLimit {
		log.GetLogger().Infof("BreakBalanceQueueLengthLimit updated old: %t, new: %t",
			oldConfig.BreakBalanceQueueLengthLimit, newConfig.BreakBalanceQueueLengthLimit)
		oldConfig.BreakBalanceQueueLengthLimit = newConfig.BreakBalanceQueueLengthLimit
	}
}

func hotLoadAlarmConfig(newAllConfig *Configs) {
	if newAllConfig.AlarmConfig == nil {
		return
	}
	oldConfig := FConfig.AlarmConfig
	newConfig := newAllConfig.AlarmConfig
	if newConfig.RouterEtcdLeaseDetectInterval > 0 {
		log.GetLogger().Infof("RouterEtcdLeaseDetectInterval updated old: %d, new: %d",
			oldConfig.RouterEtcdLeaseDetectInterval, newConfig.RouterEtcdLeaseDetectInterval)
		oldConfig.RouterEtcdLeaseDetectInterval = newConfig.RouterEtcdLeaseDetectInterval
	}

	if newConfig.RouterEtcdLeaseTimeout > 0 {
		log.GetLogger().Infof("RouterEtcdLeaseTimeout updated old: %d, new: %d",
			oldConfig.RouterEtcdLeaseTimeout, newConfig.RouterEtcdLeaseTimeout)
		oldConfig.RouterEtcdLeaseTimeout = newConfig.RouterEtcdLeaseTimeout
	}
}

func hotLoadLeaseConfig(newConfig *Configs) {
	if newConfig.EtcdLeaseConfig == nil {
		return
	}
	oldConfig := FConfig.EtcdLeaseConfig
	if newConfig.EtcdLeaseConfig.LeaseTTL > 0 {
		log.GetLogger().Infof("EtcdLeaseConfig leaseTTL updated old: %d, new: %d",
			oldConfig.LeaseTTL, newConfig.EtcdLeaseConfig.LeaseTTL)
		oldConfig.LeaseTTL = newConfig.EtcdLeaseConfig.LeaseTTL
	}
	if newConfig.EtcdLeaseConfig.RenewTTL > 0 {
		log.GetLogger().Infof("EtcdLeaseConfig renewTTL updated old: %d, new: %d",
			oldConfig.RenewTTL, newConfig.EtcdLeaseConfig.RenewTTL)
		oldConfig.RenewTTL = newConfig.EtcdLeaseConfig.RenewTTL
	}
}

func hotLoadEtcdCacheConfig(newConfig *Configs) {
	if newConfig.EtcdCacheConfig == nil {
		return
	}
	oldConfig := FConfig.EtcdCacheConfig
	oldConfig.EnableEtcdCache = newConfig.EtcdCacheConfig.EnableEtcdCache
	if newConfig.EtcdCacheConfig.UpdateCacheFilePeriod > 0 {
		log.GetLogger().Infof("UpdateCacheFilePeriod update old: %d, new: %d",
			oldConfig.UpdateCacheFilePeriod, newConfig.EtcdCacheConfig.UpdateCacheFilePeriod)
		oldConfig.UpdateCacheFilePeriod = newConfig.EtcdCacheConfig.UpdateCacheFilePeriod
	}
	if newConfig.EtcdCacheConfig.FlushCacheThreshold > 0 {
		log.GetLogger().Infof("FlushCacheThreshold update old: %d, new: %d",
			oldConfig.FlushCacheThreshold, newConfig.EtcdCacheConfig.FlushCacheThreshold)
		oldConfig.FlushCacheThreshold = newConfig.EtcdCacheConfig.FlushCacheThreshold
	}
	if newConfig.EtcdCacheConfig.EtcdDiffSyncTimeout > 0 {
		log.GetLogger().Infof("EtcdDiffSyncTimeout update old: %d, new: %d",
			oldConfig.EtcdDiffSyncTimeout, newConfig.EtcdCacheConfig.EtcdDiffSyncTimeout)
		oldConfig.EtcdDiffSyncTimeout = newConfig.EtcdCacheConfig.EtcdDiffSyncTimeout
	}
	if newConfig.EtcdCacheConfig.CacheUpdateBufSize > 0 {
		log.GetLogger().Infof("CacheUpdateBufSize update old: %d, new: %d",
			oldConfig.CacheUpdateBufSize, newConfig.EtcdCacheConfig.CacheUpdateBufSize)
		oldConfig.CacheUpdateBufSize = newConfig.EtcdCacheConfig.CacheUpdateBufSize
	}
	if newConfig.EtcdCacheConfig.CacheFilePartitionSize > 0 {
		log.GetLogger().Infof("CacheFilePartitionSize update old: %d, new: %d",
			oldConfig.CacheFilePartitionSize, newConfig.EtcdCacheConfig.FlushCacheThreshold)
		oldConfig.CacheFilePartitionSize = newConfig.EtcdCacheConfig.CacheFilePartitionSize
	}
}

func hotLoadEtcdConfig(newConfig *Configs) {
	if newConfig.EtcdConfig == nil {
		return
	}
	oldConfig := FConfig.EtcdConfig
	if len(newConfig.EtcdConfig.ServerList) > 0 {
		log.GetLogger().Infof("Update etcd config update old: %v, new: %v",
			oldConfig.ServerList, newConfig.EtcdConfig.ServerList)
		oldConfig.ServerList = newConfig.EtcdConfig.ServerList
	}
}

func hotLoadDynamicAdjustKeepAliveTimeConfig(newConfig *Configs) {
	if newConfig.DynamicKeepAliveConfig == nil {
		return
	}
	triggerCallBack := false
	if newConfig.DynamicKeepAliveConfig.Enable != FConfig.DynamicKeepAliveConfig.Enable {
		log.GetLogger().Infof("dynamicKeepAlive enable update old: %t, new: %t",
			FConfig.DynamicKeepAliveConfig.Enable, newConfig.DynamicKeepAliveConfig.Enable)
		FConfig.DynamicKeepAliveConfig.Enable = newConfig.DynamicKeepAliveConfig.Enable
		triggerCallBack = true
	}
	if newConfig.DynamicKeepAliveConfig.StartTime != 0 && newConfig.DynamicKeepAliveConfig.StartTime >= 0 &&
		newConfig.DynamicKeepAliveConfig.StartTime < DynamicKeepAliveTimeHour &&
		newConfig.DynamicKeepAliveConfig.StartTime != FConfig.DynamicKeepAliveConfig.StartTime {
		log.GetLogger().Infof("dynamicKeepAlive startTime update old: %d, new: %d",
			FConfig.DynamicKeepAliveConfig.StartTime, newConfig.DynamicKeepAliveConfig.StartTime)
		FConfig.DynamicKeepAliveConfig.StartTime = newConfig.DynamicKeepAliveConfig.StartTime
		newCron := strconv.Itoa(FConfig.DynamicKeepAliveConfig.StartTime) + DynamicKeepAliveTimeCronSuffix
		log.GetLogger().Infof("dynamicKeepAlive cron update old: %s, new: %s",
			FConfig.DynamicKeepAliveConfig.StartTimeCron, newCron)
		FConfig.DynamicKeepAliveConfig.StartTimeCron = newCron
		triggerCallBack = true
	}
	newRetry := newConfig.DynamicKeepAliveConfig.Retry
	if newRetry != 0 && newConfig.DynamicKeepAliveConfig.Retry >= 0 &&
		newRetry != FConfig.DynamicKeepAliveConfig.Retry {
		if newRetry > DynamicKeepAliveTimeHour-newConfig.DynamicKeepAliveConfig.StartTime {
			newRetry = DynamicKeepAliveTimeHour - newConfig.DynamicKeepAliveConfig.StartTime
		}
		log.GetLogger().Infof("dynamicKeepAlive retry update old: %d, new: %d",
			FConfig.DynamicKeepAliveConfig.Retry, newRetry)
		FConfig.DynamicKeepAliveConfig.Retry = newRetry
		triggerCallBack = true
	}

	if triggerCallBack && adjustKeepaliveDynamicallyCallBack != nil {
		adjustKeepaliveDynamicallyCallBack(FConfig)
	}
}

func hotLoadMaxWorkerDelay(newConfig *Configs) {
	if newConfig.MaxWorkerDelay <= 0 {
		return
	}
	old := atomic.LoadInt64(&FConfig.MaxWorkerDelay)
	if newConfig.MaxWorkerDelay != old {
		log.GetLogger().Infof("MaxWorkerDelay update from %d to %d", old, newConfig.MaxWorkerDelay)
		atomic.StoreInt64(&FConfig.MaxWorkerDelay, newConfig.MaxWorkerDelay)
	}
}

func hotLoadConfig(filename string, opType monitor.OpType) {
	log.GetLogger().Infof("file %s hot load start", filename)
	newConfig, err := loadConfig(filename)
	if err != nil {
		log.GetLogger().Errorf("hotLoadConfig failed file: %s, opType: %d, err: %s",
			filename, opType, err.Error())
		return
	}

	if opType == monitor.Create {
		preloadMetricsServerConfig(newConfig)
	}
	hotLoadMicroServiceSchedulingPolicy(newConfig)
	hotLoadInternalConfig(newConfig)
	hotLoadMemoryControlConfig(newConfig)
	hotLoadMemoryEvaluatorConfig(newConfig)
	hotLoadAutoScalerConfig(newConfig)
	hotLoadAlarmConfig(newConfig)
	hotLoadLeaseConfig(newConfig)
	hotLoadEtcdCacheConfig(newConfig)
	hotLoadEtcdConfig(newConfig)
	hotLoadDynamicAdjustKeepAliveTimeConfig(newConfig)
	hotLoadMaxRetryTime(newConfig)
	hotLoadMaxWorkerDelay(newConfig)
	if workerManagerHealthCheckCallBack != nil {
		workerManagerHealthCheckCallBack(newConfig)
	}
	if configChangedCallback != nil {
		configChangedCallback()
	}
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

// WorkerMap a map to save the stateful invoke worker
var WorkerMap = &StatefulWorker{WMap: make(map[string]bool)}

// StatefulWorker -
type StatefulWorker struct {
	WMap map[string]bool
	sync.RWMutex
}

// SetVal set value
func (s *StatefulWorker) SetVal(workerID string) {
	s.Lock()
	defer s.Unlock()
	s.WMap[workerID] = true
	return
}

// CheckVal check value
func (s *StatefulWorker) CheckVal(workerID string) bool {
	s.Lock()
	defer s.Unlock()
	if _, ok := s.WMap[workerID]; !ok {
		return false
	}
	return true
}

// DelVal del value
func (s *StatefulWorker) DelVal(workerID string) {
	s.Lock()
	defer s.Unlock()
	delete(s.WMap, workerID)
	return
}
