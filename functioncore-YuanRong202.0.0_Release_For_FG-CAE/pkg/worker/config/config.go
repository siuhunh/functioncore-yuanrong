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

// Package config contains configuration of worker
package config

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sync"

	"github.com/asaskevich/govalidator"
	"gopkg.in/yaml.v2"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/logger/logservice"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/trace"
	"functioncore/pkg/common/types"
	commUtils "functioncore/pkg/common/utils"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/utils"
)

const (
	// DefaultTimeout define a defaultTimeout
	DefaultTimeout = 10000

	maxWorkerKeyLength = 255
)

// RequestBody request body
type RequestBody struct {
	Conf    *Configuration
	Envs    *utils.Environment
	Version string
}

// Configuration represents total config for app
type Configuration struct {
	StartArgs               `yaml:"commandLine" valid:"optional"`
	RuntimeConfig           `yaml:"runtime" valid:"required"`
	DeploymentConfig        `yaml:"deployment" valid:"required"`
	SnapshotConfig          `yaml:"snapshot" valid:"optional"`
	ServerConfig            `yaml:"server" valid:"required"`
	VPCConfig               `yaml:"vpcConfig" valid:"optional"`
	StorageConfig           `yaml:"storage" valid:"optional"`
	InvokeServer            `yaml:"invokeServer" valid:"optional"`
	BackgroundProcessLimit  `yaml:"backgroundProcessLimit" valid:"optional"`
	HeartbeatConfig         `yaml:"heartbeatConfig" valid:"optional"`
	UserLogTag              string `yaml:"userLogTag" valid:"optional"`
	FunctionNameSeparator   string `yaml:"functionNameSeparator" valid:"optional"`
	ReportTimeWindowsPeriod int    `yaml:"reportTimeWindowsPeriod" valid:"optional"`
	SyncRequestTimeout      int    `yaml:"syncRequestTimeout" valid:"optional"`
	ReconnectTimes          int    `yaml:"reconnectTimes" valid:"optional"`
	ClusterID               string `yaml:"clusterID" valid:"optional"`
	IsSafety                bool   `yaml:"is_safety" valid:"optional"`
	CacheManagerClusterID   string `yaml:"cacheManagerClusterID" valid:"optional"`
	DNSResolveAddr          string `yaml:"dnsResolveAddr" valid:"optional"`
}

// HeartbeatConfig -
type HeartbeatConfig struct {
	FailureThreshold   int `yaml:"failureThreshold" valid:"optional"`
	CheckPeriodSeconds int `yaml:"checkPeriodSeconds" valid:"optional"`
}

// InvokeServer -
type InvokeServer struct {
	Protocol string `yaml:"protocol" valid:"optional"`
	Port     int32  `yaml:"port" valid:"required"`
}

// StorageConfig the info of storage address
type StorageConfig struct {
	Addr     string `yaml:"addr" valid:"optional"`
	Password string `yaml:"password" valid:"optional"`
}

// SnapshotConfig the info of snapshot info
type SnapshotConfig struct {
	SnapshotBucketID  string      `yaml:"bucketID" valid:"optional"`
	SnapshotObjectID  string      `yaml:"objectID" valid:"optional"`
	SnapshotBucketURL string      `yaml:"bucketUrl" valid:"optional"`
	SnapshotSha256    string      `yaml:"snapshotSha256" valid:"optional"`
	SnapshotSize      int64       `yaml:"codeSize" valid:"optional"`
	SnapshotPackKey   string      `yaml:"packKey" valid:"optional"`
	RestoreHook       RestoreHook `yaml:"restoreHook" valid:"optional"`
	EnableSnapshot    string      `yaml:"enableSnapshot" valid:"optional"`
	IsSnapshotExpire  bool        `yaml:"isSnapshotExpire" valid:"optional"`
	SnapshotState     string      `yaml:"snapshotState" valid:"optional"`
	SnapshotCriuOpts  string      `yaml:"snapshotCriuOpts" valid:"optional"`
}

// RestoreHook include restorehook handler and timeout
type RestoreHook struct {
	Handler string            `json:"handler" valid:",optional"`
	Timeout int64             `json:"timeout" valid:",optional"`
	Extra   map[string]string `json:"extra" valid:"optional"`
}

// StartArgs represents arguments of start
type StartArgs struct {
	LogDir   string `yaml:"logDir" valid:"optional"`
	IsLoaded bool   `yaml:"isLoaded" valid:"-"`
}

// RuntimeConfig represents configuration of runtime configuration
type RuntimeConfig struct {
	FunctionType          string                    `yaml:"function_type" valid:"optional"`
	EnableAuthInHeader    bool                      `json:"enable_auth_in_header" valid:"optional"`
	Type                  string                    `yaml:"type" valid:"-"`
	URN                   string                    `yaml:"urn" valid:"optional"`
	FuncLogLevel          string                    `yaml:"loglevel" valid:"optional"`
	Timeout               int64                     `yaml:"timeout" valid:"optional"`
	LoadTime              int64                     `yaml:"loadTime" valid:"optional"`
	EnvKey                string                    `yaml:"envKey" valid:"optional"` // deprecated
	TraceEnv              map[string]interface{}    `yaml:"traceEnv" valid:"optional"`
	ExtraEnvJSON          []byte                    `yaml:"extraEnvJSON" valid:"optional"`
	ExtraUserData         []byte                    `yaml:"extraUserData" valid:"optional"`
	EnvInfo               string                    `yaml:"envInfo" valid:"optional"`
	EncryptedUserData     string                    `yaml:"encryptedUserData" valid:"optional"`
	EnvPlaintextKmsCipher string                    `yaml:"envPlaintextKmsCipher" valid:"optional"`
	EntryFile             string                    `yaml:"entryFile" valid:"optional"`
	HandlerName           string                    `yaml:"handlerName" valid:"optional"`
	RPCPort               int32                     `yaml:"rpcPort" valid:"required"`
	ConnectType           string                    `yaml:"connectType" valid:"optional"`
	Language              string                    `yaml:"language" valid:"optional"`
	CodeSha256            string                    `yaml:"codeSha256" valid:"optional"`
	CodeSha512            string                    `yaml:"codeSha512" valid:"optional"`
	ResourceMetaData      ResourceMetaData          `json:"resourceMetaData" valid:",optional"`
	Initializer           Initializer               `json:"initializer" valid:",optional"`
	LogTankService        logservice.LogTankService `json:"logTankService" valid:",optional"`
	TraceService          trace.TraceService        `json:"traceService" valid:",optional"`
	RMConfig              rmconfig.Config           `json:"rmConfig" valid:",optional"`
	FuncMountConfig       types.FuncMountConfig     `json:"mount_config" valid:",optional"`
	RuntimeLogDir         string                    `yaml:"runtimeLogDir" valid:"stringlength(1|255)"`
	RuntimeContainerID    string                    `json:"runtimeContainerID"`
	MaxStateSize          string                    `json:"maxStateSize"`
	Heartbeat             types.Heartbeat           `json:"heartbeat" valid:",optional"`
	StartupHeartbeat      types.Heartbeat           `json:"startupHeartbeat" valid:",optional"`
	DNSDomainCfg          []types.DNSDomainInfo     `json:"dns_domain_cfg" valid:",optional"`
	DNSConfig             types.DNSConfig           `json:"dns_config"`
	EndpointDomainCfg     []types.DNSDomainInfo     `json:"endpoint_domain_cfg" valid:",optional"`
	DNSDomainNames        map[string][]string       `json:"dns_domain_names" valid:",optional"`
	IsStreamEnable        bool                      `json:"isStreamEnable" valid:"optional"`
	StreamResponse        bool                      `json:"stream_response" valid:"optional"`
	RunningMode           string                    `json:"loadMode" valid:"optional"`
	EnableClassIsolation  bool                      `json:"enable_class_isolation"`
	PreStop               types.PreStop             `json:"pre_stop" valid:",optional"`
	CustomMetricsReport   types.CustomMetricsReport `json:"customMetricsReport" valid:",optional"`
	MaxRequestBodySize    string                    `json:"max_request_body_size" valid:",optional"`
	EnableGCLog           bool                      `json:"enable_gc_log" valid:"optional"`
	BusinessType          string                    `json:"businessType" valid:"optional"`
	StateID               string                    `json:"state_id" valid:"optional"`
	LogPolicy             types.LogPolicy           `json:"logPolicy" valid:"optional"`
	IsClusterUpgrading    string                    `json:"isClusterUpgrading" valid:"optional"`
	Properties            map[string]string         `json:"properties" valid:"optional"`
	LtsCustomTag          map[string]string         `json:"lts_custom_tag" valid:"optional"`
	RuntimeName           string                    `yaml:"runtime_name" valid:"optional"`
}

// ResourceMetaData include resource data such as cpu and memory
type ResourceMetaData struct {
	CPU                int64 `json:"cpu" valid:"optional"`
	Mem                int64 `json:"mem" valid:"optional"`
	GpuMemory          int64 `json:"gpu_memory" valid:"optional"`
	EphemeralStorage   int64 `json:"ephemeral_storage" valid:"optional"`
	EnableTmpExpansion bool  `json:"enable_tmp_expansion" valid:"optional"`
}

// Initializer include initializer handler and timeout
type Initializer struct {
	InitializerHandler string            `json:"initializer_handler" valid:",optional"`
	InitializerTimeout int64             `json:"initializer_timeout" valid:",optional"`
	Extra              map[string]string `json:"extra" valid:"optional"`
}

// DeploymentConfig represents configuration of deployment
type DeploymentConfig struct {
	BucketID               string `yaml:"bucketID" valid:"optional"`
	ObjectID               string `yaml:"objectID" valid:"optional"`
	CodePlaintextKmsCipher string `yaml:"code_plaintext_kms_cipher" valid:"optional"`
	CodeSize               int64
	Access                 string `yaml:"access" valid:"optional"`
	Secret                 string `yaml:"secret" valid:"optional"`
	SecurityToken          string `yaml:"securityToken" valid:"optional"`
	LayerInfo              string `yaml:"layerInfo" valid:"optional"`
	DeployDir              string `yaml:"depDir" valid:"stringlength(1|255)"`
	StorageType            string `yaml:"storageType" valid:"required"`
	InternalHTTPSConfig    `yaml:"httpsConfig" valid:"optional"`
	CodePackage            `yaml:"codePackage" valid:"optional"`
	S3Config               `yaml:"s3Config" valid:"optional"`
	localauth.AuthConfig   `yaml:"localAuth" valid:"optional"`
}

// InternalHTTPSConfig is for input config
type InternalHTTPSConfig struct {
	HTTPSEnable bool   `yaml:"httpsEnable" valid:"optional"`
	TLSProtocol string `yaml:"tlsProtocol" valid:"optional"`
	TLSCiphers  string `yaml:"tlsCiphers" valid:"optional"`
}

// CodePackage configuring Code Package Parameters
type CodePackage struct {
	FileCountsMax      int `yaml:"fileCountsMax" valid:"required"`
	ZipFileSizeMaxMB   int `yaml:"zipFileSizeMaxMB" valid:"required"`
	UnzipFileSizeMaxMB int `yaml:"unzipFileSizeMaxMB" valid:"required"`
	DirDepthMax        int `yaml:"dirDepthMax" valid:"required"`
}

// ServerConfig represents configuration of server
// it's contains many server parameters
type ServerConfig struct {
	HTTPSConfig    tls.InternalHTTPSConfig `yaml:"httpsConfig" valid:"optional"`
	ReadTimeout    int64                   `yaml:"readTimeout" valid:"required"`
	WriteTimeout   int64                   `yaml:"writeTimeout" valid:"required"`
	Port           int32                   `yaml:"port" valid:"required"`
	MaxHeaderBytes int                     `yaml:"maxHeaderBytes" valid:"required"`
	// IOReadTimeout is used to set the I/O wait time, the unit is millisecond
	IOReadTimeout int `yaml:"ioReadTimeout" valid:"required"`
}

// S3Config contains the parameters to connect Minio
type S3Config struct {
	UseSSL    bool   `yaml:"useSSL" valid:"optional"`
	AccessKey string `yaml:"accessKey" valid:"optional"`
	SecretKey string `yaml:"secretKey" valid:"optional"`
	Endpoint  string `yaml:"endpoint" valid:"required"`
	CaFile    string `yaml:"caFile" valid:"optional"`
	TrustedCA bool   `yaml:"trustedCA" valid:"optional"`
}

// VPCConfig contains the parameters for VPC
type VPCConfig struct {
	ReservedCidr string `yaml:"reservedCidr" valid:"optional"`
}

// BackgroundProcessLimit async process limit config
type BackgroundProcessLimit struct {
	LimitEnable bool  `yaml:"enable" valid:"optional"`
	IdleTime    int64 `yaml:"idleTime" valid:"optional"`
	RetryTimes  int   `yaml:"retryTimes" valid:"optional"`
}

// LogConfig log config of log config file
type LogConfig struct {
	LogPath            []string          `json:"logPath" valid:"optional"`
	PodName            string            `json:"podName" valid:"optional"`
	EnableMultiCollect bool              `json:"enableMultiCollect" valid:"optional"`
	MultiPattern       string            `json:"multiPattern" valid:"optional"`
	LogGroupID         string            `json:"logGroupId" valid:"optional"`
	LogStreamID        string            `json:"logStreamId" valid:"optional"`
	ProjectID          string            `json:"projectId" valid:"optional"`
	Properties         map[string]string `json:"properties" valid:"optional"`
	PodDelete          bool              `json:"podDeleted" valid:"optional"`
	ContainerID        string            `json:"containerId" valid:"optional"`
}

var (
	configSingleton = struct {
		sync.Once
		config *Configuration
	}{}
)

func getWorkerKey(config *Configuration) error {
	readKey := func(path string) (string, error) {
		key, err := ioutil.ReadFile(path)
		if err != nil {
			log.GetLogger().Errorf("failed to read worker key from %s, error: %s", path, err.Error())
			return "", err
		}
		if len(key) == 0 || len(key) > maxWorkerKeyLength {
			log.GetLogger().Errorf("failed to check key length from %s", path)
			return "", errors.New("invalid key length")
		}
		return string(key), nil
	}
	var err error
	if config.AuthConfig.AKey, err = readKey(worker.LocalAuthAKPath); err != nil {
		return err
	}
	if config.AuthConfig.SKey, err = readKey(worker.LocalAuthSKPath); err != nil {
		return err
	}
	// support rotated token, and compatible with fixed token
	if config.S3Config.AccessKey, err = readKey(worker.S3StorageAKPath); err != nil {
		log.GetLogger().Warnf("failed to read s3 storage key file: %s", err.Error())
	}
	if config.S3Config.SecretKey, err = readKey(worker.S3StorageSKPath); err != nil {
		log.GetLogger().Warnf("failed to read s3 storage key file: %s", err.Error())
	}

	if config.SnapshotPackKey, err = readKey(worker.SnapshotPackKeyPath); err != nil {
		return err
	}

	return nil
}

// CleanWorkerSecret clear worker secret on disk
func CleanWorkerSecret() {
	dir, err := ioutil.ReadDir(worker.ResourcePath)
	if err != nil {
		log.GetLogger().Warnf("failed to read dir %s error: %s", worker.ResourcePath, err.Error())
		return
	}
	// remove content in resource path except resource path itself
	for _, sonDir := range dir {
		if err = os.RemoveAll(path.Join([]string{worker.ResourcePath, sonDir.Name()}...)); err != nil {
			log.GetLogger().Warnf("failed to remove path %s error: %s",
				worker.ResourcePath+sonDir.Name(), err.Error())
			continue
		}
	}
}

// GetConfig singleton pattern thread safe
func GetConfig() (*Configuration, error) {
	configSingleton.Do(func() {
		serializedConfig, ok := os.LookupEnv(worker.ConfigEnvKey)
		if !ok {
			log.GetLogger().Errorf("config key %s does not exist", worker.ConfigEnvKey)
			return
		}

		configSingleton.config = &Configuration{}
		if err := yaml.Unmarshal([]byte(serializedConfig), configSingleton.config); err != nil {
			log.GetLogger().Errorf("failed to unmarshal the config file, error: %s", err.Error())
			configSingleton.config = nil
			return
		}
		commUtils.ValidateTimeout(&configSingleton.config.RuntimeConfig.Timeout, DefaultTimeout)
		_, err := govalidator.ValidateStruct(configSingleton.config)
		if err != nil {
			log.GetLogger().Errorf("configuration checking error: %s", err.Error())
			configSingleton.config = nil
			return
		}
		runtimeManagerConf, err := rmconfig.GetConfig()
		if err != nil {
			log.GetLogger().Errorf("failed to get runtime manager config with error: %s", err.Error())
			configSingleton.config = nil
			return
		}
		configSingleton.config.RuntimeConfig.FuncLogLevel = worker.FuncLogLevelInfo
		configSingleton.config.RuntimeConfig.Type = os.Getenv(worker.RuntimeTypeEnvKey)
		configSingleton.config.RuntimeConfig.RMConfig = *runtimeManagerConf

		if err = getWorkerKey(configSingleton.config); err != nil {
			log.GetLogger().Warnf("failed to get worker key: %s", err.Error())
			configSingleton.config = nil
			return
		}
	})

	if configSingleton.config == nil {
		return nil, errors.New("failed to get worker config")
	}
	return configSingleton.config, nil
}
