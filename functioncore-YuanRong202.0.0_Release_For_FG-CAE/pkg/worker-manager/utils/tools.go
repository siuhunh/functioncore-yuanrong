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

// Package utils gets function services from URNs
package utils

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"math"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asaskevich/govalidator"
	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serror "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/k8sclient"
	"functioncore/pkg/common/kms"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient/poderrormessage"
	"functioncore/pkg/common/snerror"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

const (
	// EncryptedUserDataKey -
	EncryptedUserDataKey = "USER_DATA"
	// EnvironmentKey -
	EnvironmentKey = "WORKER_ENV"

	// EnvEncryptKmsCipher -
	EnvEncryptKmsCipher = "ENV_ENCRYPT_KMS_CIPHER"

	defaultReplicasNum = 1
	// kubeClientQPS and kubeClientBurst is default flowControl parameter value
	// which limiting connections to the api-server of k8s from this client
	kubeClientQPS    = 100
	kubeClientBurst  = 100
	azSep            = "-#-"
	instanceIDLength = 2
	podNameLocation  = 1
	txnRetry         = 3
	// InstanceKeySeparator is used to separate instance keys
	InstanceKeySeparator = "/*/"
	validUrnLength       = 7

	// WatchPathPrefixWithAgency -
	WatchPathPrefixWithAgency = "/sn/agency/business/yrk/tenant/"
	memoryIndex               = 2
	// WatchClusterPrefix -
	WatchClusterPrefix = "/sn/function/available/clusters/"
	// SplicingCharacter -
	SplicingCharacter = "&"

	// DNSCacheFuncKeyPrefix dns cache
	DNSCacheFuncKeyPrefix = "func_"

	// DNSCacheSvcKeyPrefix -
	DNSCacheSvcKeyPrefix = "svcID_"

	keepConnAliveTTL = 3 * time.Second

	v2Version = "v2"
	v3Version = "v3"

	valueTypeMeta     = 0
	valueTypeInstance = 1
	valueTypeTenant   = 2

	basePercent  = 100
	notExistCode = "FSS.0404"
	urlV2        = "/v2/fgs/om/dns"
	urlV3        = "/v3/fgs/om/dns"

	routeKeySegLen     = 13
	routeKeyTenantIdx  = 6
	routeKeyClusterIdx = 11

	// DsWorkerTaintKey -
	DsWorkerTaintKey = "dsWorkerTaintKey"
	// DsWorkerTaintValue -
	DsWorkerTaintValue = "Failure"
	// DefaultDsWorkerTimeSleep -
	DefaultDsWorkerTimeSleep = 60
	//DsWorkerTimeoutEnvKey -
	DsWorkerTimeoutEnvKey = "DS_WORKER_TIMEOUT"
	// DsWorkerPodNameKey -
	DsWorkerPodNameKey = "ds-worker"

	dockerAuthConfigJSONFormat = `{"auths": {"%s": {"auth": "%s"}}}`
	imageURNMinSplitLen        = 2

	// Day time of one day
	Day = 24 * time.Hour

	// EnvDecType -
	EnvDecType = "env"

	// CodeDecType -
	CodeDecType = "code"

	// CipherCacheGap -
	CipherCacheGap = "#"
)

// cipher cache struct is tenantID#decType#userKeyID#domainID#agencyID like 1a2b3c#env#a1b2c3#1a23cs#56ac16a
const (
	tenantIDIndex int = iota

	decTypeIndex

	userKeyIDIndex

	domainIDIndex

	agencyIDIndex

	cipherKeyLength
)

var (
	functionServiceNamespace = ""

	// userAgencyCacheMap user agency cache
	userAgencyCacheMap sync.Map
	// functionClusterCacheMap available clusters per function cache
	functionClusterCacheMap sync.Map
	kubeClientOnce          sync.Once
	// KubeClientSet -
	KubeClientSet     kubernetes.Interface
	kubeMetricsClient metrics.Interface

	workerManagerDeploy *v1.Deployment

	// ErrK8SClientNil -
	ErrK8SClientNil           = errors.New("kubernetes client is nil")
	smartWarmerEnable         = false
	updateFunctionCacheEnable = false
	multiSnapshotConfig       config.MultiSnapshotConfig
	snapshotTenantIDsConfig   config.SnapshotTenantIDsConfig

	dnsCache = &DNSCache{}

	// ErrDomainInfoInvalid -
	ErrDomainInfoInvalid = errors.New("failed to get domain info")

	dnsVersion = v2Version

	clusterID = os.Getenv("CLUSTER_ID")

	// LocalMetaEtcdCache -
	LocalMetaEtcdCache = &LocalMetaEtcdCaches{}
	// MetaCacheOnce -
	MetaCacheOnce sync.Once
	// GetCacheOnce -
	GetCacheOnce sync.Once
	// GetDsWorkerTimeout -
	GetDsWorkerTimeout sync.Once
	dsWorkerTimeout    int64

	enableNewCaeBaseImage = os.Getenv("ENABLE_NEW_CAE_BASE_IMAGE") == "true"

	userCipherMap sync.Map
)

// UserCipherCache -
type UserCipherCache struct {
	LastUseTime time.Time
	Ciphertext  string
	Plaintext   string
}

// CheckKmsCipherInfo -
type CheckKmsCipherInfo struct {
	DecType       string
	TenantID      string
	KmsKeyID      string
	CipherKey     string
	EncryptCipher string
	TraceID       string
}

func init() {
	if os.Getenv("DNS_URL") == urlV3 {
		dnsVersion = v3Version
	}
}

// StartClearKmsCipherCacheTask -
func StartClearKmsCipherCacheTask(stopCh <-chan struct{}) {
	go clearInvalidsCipherCache(stopCh)
}

func clearInvalidsCipherCache(stopCh <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			userCipherMap.Range(func(key, value any) bool {
				cache, cacheKey, ok := transferCipherCacheKV(key, value)
				if !ok {
					userCipherMap.Delete(key)
					return true
				}
				if time.Now().Sub(cache.LastUseTime) >= 1*Day {
					log.GetLogger().Infof("get tenant %s's %s kms cache hasn't been used for 24 hours, last use time: %s,"+
						" will clear, key ID: %s", cacheKey[tenantIDIndex], cacheKey[decTypeIndex], cache.LastUseTime, cacheKey[userKeyIDIndex])
					userCipherMap.Delete(key)
					return true
				}
				tenantID := cacheKey[tenantIDIndex]
				agency := GetUserAgencyFromCacheMap(tenantID, cacheKey[domainIDIndex], cacheKey[agencyIDIndex])
				client, err := GetKmsClientByAgency(agency, tenantID)
				if err != nil {
					log.GetLogger().Warnf("failed to get kms client when decrypt tenant %s's %s kms cipher, err: %s, key ID: %s",
						tenantID, cacheKey[decTypeIndex], err.Error(), cacheKey[userKeyIDIndex])
					return true
				}
				resp, err := client.DecryptDataKeyWithRetry(cacheKey[userKeyIDIndex], cache.Ciphertext)
				if err != nil {
					if cache.Plaintext == constants.KmsAbnormalCipher {
						return true
					}
					log.GetLogger().Warnf("failed to decrypt tenant %s's %s kms cipher, will store abnormal kms cipher, err: %s",
						tenantID, cacheKey[decTypeIndex], err.Error())
					cache.Plaintext = constants.KmsAbnormalCipher
				} else {
					if cache.Plaintext == constants.KmsAbnormalCipher {
						log.GetLogger().Infof("succeed to decrypt tenant %s's %s kms cipher, will store correct kms cipher",
							tenantID, cacheKey[decTypeIndex])
					}
					cache.Plaintext = *resp.DataKey
				}
				userCipherMap.Store(key, cache)
				return true
			})
		case <-stopCh:
			log.GetLogger().Warnf("clearInvalidsCipherCache receives a terminating signal")
			return
		}
	}
}

func transferCipherCacheKV(key, value any) (UserCipherCache, []string, bool) {
	cache, ok := value.(UserCipherCache)
	if !ok {
		return UserCipherCache{}, []string{}, false
	}
	keyString, ok := key.(string)
	if !ok {
		return UserCipherCache{}, []string{}, false
	}
	cacheKey := strings.Split(keyString, CipherCacheGap)
	if len(cacheKey) != cipherKeyLength {
		return UserCipherCache{}, []string{}, false
	}
	return cache, cacheKey, true
}

// IsDNSV3Version -
func IsDNSV3Version() bool {
	return dnsVersion == v3Version
}

// DNSCache dns cache
type DNSCache struct {
	mux                sync.Mutex
	dnsCacheMap        sync.Map
	domainNotExistList []string
}

// UserAgency  user agency
type UserAgency struct {
	AccessKey     string `json:"accessKey"`
	SecretKey     string `json:"secretKey"`
	Token         string `json:"token"`
	SecurityAk    string `json:"securityAk"`
	SecuritySk    string `json:"securitySk"`
	SecurityToken string `json:"securityToken"`
	Source        etcd3.EtcdType
}

// EtcdClients -
type EtcdClients struct {
	MetadataEtcdClient *etcd3.EtcdClient
	RouterETCDClient   *etcd3.EtcdClient
}

// APIClients -
type APIClients struct {
	EtcdClients *EtcdClients
	KubeClient  kubernetes.Interface
}

// LocalMetaEtcdCaches local etcd cache
type LocalMetaEtcdCaches struct {
	mux              sync.Mutex
	metaEtcdCacheMap map[string]*etcd3.Event
}

// SetSmartWarmerEnable -
func SetSmartWarmerEnable(enable bool) {
	smartWarmerEnable = enable
}

// GetSmartWarmerEnable -
func GetSmartWarmerEnable() bool {
	return smartWarmerEnable
}

// SetUpdateFunctionCacheEnable -
func SetUpdateFunctionCacheEnable(enable bool) {
	updateFunctionCacheEnable = enable
}

// GetUpdateFunctionCacheEnable -
func GetUpdateFunctionCacheEnable() bool {
	return updateFunctionCacheEnable
}

// SetMultiSnapshotConfig -
func SetMultiSnapshotConfig(config config.MultiSnapshotConfig) {
	multiSnapshotConfig = config
}

// GetMultiSnapshotConfig -
func GetMultiSnapshotConfig() config.MultiSnapshotConfig {
	return multiSnapshotConfig
}

// SetSnapshotTenantIDsConfig -
func SetSnapshotTenantIDsConfig(config config.SnapshotTenantIDsConfig) {
	snapshotTenantIDsConfig = config
}

// GetSnapshotTenantIDsConfig -
func GetSnapshotTenantIDsConfig() config.SnapshotTenantIDsConfig {
	return snapshotTenantIDsConfig
}

// InitETCDClient -
func InitETCDClient(stopCh <-chan struct{}) error {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return err
	}
	return etcd3.InitParam().
		WithStopCh(stopCh).
		WithAlarmSwitch().
		WithMetaEtcdConfig(cfg.Etcd).
		WithRouterEtcdConfig(cfg.RouterETCD).
		WithCAEMetaEtcdConfig(cfg.CAEMetaEtcd).
		WithCaeRouterEtcdConfig(cfg.CAERouterEtcd).
		WithMetaEtcdProxyConfig(cfg.MetaEtcdProxy).
		WithCAEMetaEtcdProxyConfig(cfg.CAEMetaEtcdProxy).
		InitClient()
}

// TransferRepoMetaToFunctionService Transfer Repo Meta To function service
func TransferRepoMetaToFunctionService(source *config.FunctionMetaInfo, target *faasfunction.FunctionService) error {
	// version is function image name
	target.Version = source.ExtendedMetaData.ImageName

	err := decryptKmsCipherToCache(source)
	if err != nil {
		return err
	}
	target.Spec.Configuration.EnvCipherKeyID = source.EnvMetaData.UserDataEncryptKmsKeyID
	target.UpdateKmsKeyID(source.S3MetaData.CodeEncryptKmsKeyID)
	target.UpdateEncryptKmsCipher(source.S3MetaData.CodeEncryptKmsKeyCipher)

	// set function info meta to target
	transferFunctionInfoMeta(source, target)

	target.Spec.Code.CodeSha256 = source.FuncMetaData.CodeSha256
	target.Spec.Code.CodeSha512 = source.FuncMetaData.CodeSha512
	target.Spec.Code.CodeSize = int64(source.FuncMetaData.CodeSize)
	target.UpdateBucketID(source.S3MetaData.BucketID)
	target.UpdateObjectID(source.S3MetaData.ObjectID)
	target.UpdateAppID(source.S3MetaData.AppID)
	target.UpdateBucketURL(source.S3MetaData.BucketURL)
	target.Spec.SnapshotDeployment = source.SnapshotMetaData

	// set environment variables meta to target
	transferEnvVariablesMeta(source, target)

	// layers
	target.Spec.Configuration.Layers = source.FuncMetaData.Layers
	// set runtime meta to target
	err = transferRuntimeMeta(source, target)
	if err != nil {
		return err
	}

	return nil
}

func transferFunctionInfoMeta(source *config.FunctionMetaInfo, target *faasfunction.FunctionService) {
	functionInfo := GetFunctionInfoByURN(source.FuncMetaData.FunctionVersionURN)
	tenantID, funcName, version := functionInfo.Tenant, functionInfo.FuncName, functionInfo.Version
	target.ObjectMeta.ID = CrNameByURN(source.FuncMetaData.FunctionVersionURN)
	target.ObjectMeta.Namespace = FunctionNameSpace()
	target.ObjectMeta.Labels = make(map[string]string, DefaultMapSize)
	for k, v := range source.FuncMetaData.Tags {
		target.ObjectMeta.Labels[k] = v
	}
	// init the replicas is 1
	replicasPtr := int32(defaultReplicasNum)
	target.Spec.Replicas = &replicasPtr
	target.Spec.Configuration.FunctionName = funcName
	target.Spec.Configuration.TenantID = tenantID
	target.Spec.Configuration.FunctionVersion = version
	target.Spec.Configuration.FunctionURN = source.FuncMetaData.FunctionURN
	target.Spec.Configuration.Description = source.FuncMetaData.FunctionDescription
	target.Spec.Configuration.DeadLetterConfig.URL = source.FuncMetaData.DeadLetterConfig
	target.Spec.Configuration.FunctionVersionURN = source.FuncMetaData.FunctionVersionURN

	transferResourceInfo(target, source)
	target.Spec.Configuration.LastModified = source.FuncMetaData.FunctionUpdateTime
	target.Spec.Configuration.BusinessID = source.FuncMetaData.BusinessID
	target.Spec.Configuration.Timeout = source.FuncMetaData.Timeout
	target.Spec.Configuration.EnableAuthInHeader = source.FuncMetaData.EnableAuthInHeader
	target.Spec.Configuration.IsStatefulFunction = source.FuncMetaData.IsStatefulFunction
	target.Spec.Configuration.IsBridgeFunction = source.FuncMetaData.IsBridgeFunction

	target.Spec.Configuration.Initializer.Handler = source.ExtendedMetaData.Initializer.Handler
	target.Spec.Configuration.Initializer.Timeout = source.ExtendedMetaData.Initializer.Timeout

	transferFuncVpcInfo(target, source)
	transferEndpointTenantFuncVpcInfo(target, source)
	target.Spec.Configuration.Role.XRole = source.ExtendedMetaData.Role.XRole
	target.Spec.Configuration.Role.AppXRole = source.ExtendedMetaData.Role.AppXRole
	target.Spec.Configuration.DomainID = source.FuncMetaData.DomainID
	target.Spec.Configuration.LogTankService.GroupID = source.ExtendedMetaData.LogTankService.GroupID
	target.Spec.Configuration.LogTankService.StreamID = source.ExtendedMetaData.LogTankService.StreamID
	target.Spec.Configuration.TraceService.TraceAK = source.ExtendedMetaData.TraceService.TraceAK
	target.Spec.Configuration.TraceService.TraceSK = source.ExtendedMetaData.TraceService.TraceSK
	target.Spec.Configuration.TraceService.Enabled = source.ExtendedMetaData.TraceService.Enabled
	target.Spec.Configuration.TraceService.ProjectName = source.FuncMetaData.ProjectName
	target.Spec.Configuration.ProjectName = source.FuncMetaData.ProjectName
	target.Spec.Configuration.VPCTriggerImage = source.FuncMetaData.VPCTriggerImage
	target.Spec.Configuration.CustomContainerConfig = source.ExtendedMetaData.CustomContainerConfig
	target.Spec.Configuration.FuncMountConfig = getFuncMountConfig(source)
	target.Spec.Configuration.Heartbeat.Handler = source.ExtendedMetaData.Heartbeat.Handler
	target.Spec.Configuration.Heartbeat.Path = source.ExtendedMetaData.Heartbeat.Path
	target.Spec.Configuration.Heartbeat.HealthEnable = source.ExtendedMetaData.Heartbeat.HealthEnable
	target.Spec.Configuration.Heartbeat.PeriodSeconds = source.ExtendedMetaData.Heartbeat.PeriodSeconds
	target.Spec.Configuration.Heartbeat.FailureThreshold = source.ExtendedMetaData.Heartbeat.FailureThreshold
	target.Spec.Configuration.Heartbeat.FirstCheckDelay = source.ExtendedMetaData.Heartbeat.FirstCheckDelay
	target.Spec.Configuration.Heartbeat.SuccessThreshold = source.ExtendedMetaData.Heartbeat.SuccessThreshold
	target.Spec.Configuration.Heartbeat.Timeout = source.ExtendedMetaData.Heartbeat.Timeout
	target.Spec.Configuration.StartupHeartbeat.Handler = source.ExtendedMetaData.StartupHeartbeat.Handler
	target.Spec.Configuration.StartupHeartbeat.Path = source.ExtendedMetaData.StartupHeartbeat.Path
	target.Spec.Configuration.StartupHeartbeat.HealthEnable = source.ExtendedMetaData.StartupHeartbeat.HealthEnable
	target.Spec.Configuration.StartupHeartbeat.PeriodSeconds = source.ExtendedMetaData.StartupHeartbeat.PeriodSeconds
	target.Spec.Configuration.StartupHeartbeat.FailureThreshold = source.ExtendedMetaData.StartupHeartbeat.FailureThreshold
	target.Spec.Configuration.StartupHeartbeat.FirstCheckDelay = source.ExtendedMetaData.StartupHeartbeat.FirstCheckDelay
	target.Spec.Configuration.StartupHeartbeat.SuccessThreshold = source.ExtendedMetaData.StartupHeartbeat.SuccessThreshold
	target.Spec.Configuration.StartupHeartbeat.Timeout = source.ExtendedMetaData.StartupHeartbeat.Timeout
	target.Spec.Configuration.DNSDomainCfg = source.FuncMetaData.DNSDomainCfg
	target.Spec.Configuration.DNSConfig = source.FuncMetaData.DNSConfig
	target.Spec.Configuration.EndpointDomainCfg = source.FuncMetaData.EndpointDomainCfg
	target.Spec.Configuration.IsStreamEnable = source.FuncMetaData.IsStreamEnable
	target.Spec.Configuration.StreamResponse = source.FuncMetaData.StreamResponse
	target.Spec.Configuration.RestoreHook.Handler = source.ExtendedMetaData.RestoreHook.Handler
	target.Spec.Configuration.RestoreHook.Timeout = source.ExtendedMetaData.RestoreHook.Timeout
	target.Spec.Configuration.NetworkController.DisablePublicNetwork = source.ExtendedMetaData.
		NetworkController.DisablePublicNetwork
	target.Spec.Configuration.NetworkController.TriggerAccessVpcs = source.ExtendedMetaData.
		NetworkController.TriggerAccessVpcs

	target.Spec.Configuration.EnableClassIsolation = source.ExtendedMetaData.EnableClassIsolation
	target.Spec.Configuration.EnableGCLog = source.ExtendedMetaData.EnableGCLog
	target.Spec.Configuration.EnableSnapshot = source.FuncMetaData.EnableSnapshot
	target.Spec.Configuration.ApigRouteEnable = source.ExtendedMetaData.ApigRouteEnable

	target.Spec.Configuration.PreStop.Handler = source.ExtendedMetaData.PreStop.Handler
	target.Spec.Configuration.PreStop.Timeout = source.ExtendedMetaData.PreStop.Timeout
	target.Spec.Configuration.PreStop.Type = source.ExtendedMetaData.PreStop.Type

	target.Spec.Configuration.CustomMetricsReport.Handler = source.ExtendedMetaData.CustomMetricsReport.Handler
	target.Spec.Configuration.CustomMetricsReport.CollectTime = source.ExtendedMetaData.CustomMetricsReport.CollectTime
	target.Spec.Configuration.CustomMetricsReport.Timeout = source.ExtendedMetaData.CustomMetricsReport.Timeout

	target.Spec.Configuration.CustomJavaArgs = source.ExtendedMetaData.CustomJavaArgs

	target.Spec.Configuration.DisableInsecureAkSk = source.FuncMetaData.DisableInsecureAkSk
	target.Spec.Configuration.BusinessType = source.FuncMetaData.BusinessType
	target.Spec.Configuration.FileMountConfig = source.ExtendedMetaData.FileMountConfig

	target.Spec.Configuration.LogPolicy = source.FuncMetaData.LogPolicy

	target.Spec.Configuration.EnableRemoteDebug = source.FuncMetaData.EnableRemoteDebug
	target.Spec.Configuration.RemoteDebugArgs = source.FuncMetaData.RemoteDebugArgs

	target.Spec.Configuration.Properties = source.ExtendedMetaData.Properties
	target.Spec.Configuration.FgsTags = source.ExtendedMetaData.FgsTags
	target.Spec.Configuration.LtsCustomTag = source.FuncMetaData.LtsCustomTag
	target.Spec.Configuration.IsMcpFunction = source.ExtendedMetaData.IsMcpFunction
}

func getFuncMountConfig(source *config.FunctionMetaInfo) faasfunction.FuncMountConfig {
	funcMountUser := faasfunction.FuncMountUser{
		UserID:  source.ExtendedMetaData.FuncMountConfig.FuncMountUser.UserID,
		GroupID: source.ExtendedMetaData.FuncMountConfig.FuncMountUser.GroupID,
	}
	mounts := source.ExtendedMetaData.FuncMountConfig.FuncMounts
	var funcMounts []faasfunction.FuncMount
	for _, m := range mounts {
		funcMounts = append(funcMounts, faasfunction.FuncMount{
			Status:         m.Status,
			MountType:      m.MountType,
			MountSharePath: m.MountSharePath,
			LocalMountPath: m.LocalMountPath,
			MountResource:  m.MountResource,
		})
	}
	return faasfunction.FuncMountConfig{
		FuncMountUser: funcMountUser,
		FuncMounts:    funcMounts,
	}
}

func transferFuncVpcInfo(target *faasfunction.FunctionService, source *config.FunctionMetaInfo) {
	target.Spec.Configuration.FuncVpc.Namespace = source.ExtendedMetaData.FuncVpc.Namespace
	target.Spec.Configuration.FuncVpc.TenantCidr = source.ExtendedMetaData.FuncVpc.TenantCidr
	target.Spec.Configuration.FuncVpc.HostVMCidr = source.ExtendedMetaData.FuncVpc.HostVMCidr
	target.Spec.Configuration.FuncVpc.DomainID = source.ExtendedMetaData.FuncVpc.DomainID
	target.Spec.Configuration.FuncVpc.Gateway = source.ExtendedMetaData.FuncVpc.Gateway
	target.Spec.Configuration.FuncVpc.ID = source.ExtendedMetaData.FuncVpc.ID
	target.Spec.Configuration.FuncVpc.SubnetID = source.ExtendedMetaData.FuncVpc.SubnetID
	target.Spec.Configuration.FuncVpc.SubnetName = source.ExtendedMetaData.FuncVpc.SubnetName
	target.Spec.Configuration.FuncVpc.VpcID = source.ExtendedMetaData.FuncVpc.VpcID
	target.Spec.Configuration.FuncVpc.VpcName = source.ExtendedMetaData.FuncVpc.VpcName
	target.Spec.Configuration.FuncVpc.SecurityGroups = source.ExtendedMetaData.FuncVpc.SecurityGroups
	target.Spec.Configuration.FuncVpc.IPV6Enable = source.ExtendedMetaData.FuncVpc.IPV6Enable
	target.Spec.Configuration.FuncVpc.IPV6Gateway = source.ExtendedMetaData.FuncVpc.IPV6Gateway
	target.Spec.Configuration.FuncVpc.IPV6HostVMCidr = source.ExtendedMetaData.FuncVpc.IPV6HostVMCidr
	target.Spec.Configuration.FuncVpc.IPV6TenantCidr = source.ExtendedMetaData.FuncVpc.IPV6TenantCidr
	target.Spec.Configuration.FuncVpc.NeutronSubnetID = source.ExtendedMetaData.FuncVpc.NeutronSubnetID
	target.Spec.Configuration.FuncVpc.NeutronSubnetIDV6 = source.ExtendedMetaData.FuncVpc.NeutronSubnetIDV6
	target.Spec.Configuration.FuncVpc.IsSafety = source.ExtendedMetaData.FuncVpc.IsSafety
}

func transferEndpointTenantFuncVpcInfo(target *faasfunction.FunctionService, source *config.FunctionMetaInfo) {
	target.Spec.Configuration.EndpointTenantVpc.Namespace = source.ExtendedMetaData.EndpointTenantVpc.Namespace
	target.Spec.Configuration.EndpointTenantVpc.TenantCidr = source.ExtendedMetaData.EndpointTenantVpc.TenantCidr
	target.Spec.Configuration.EndpointTenantVpc.HostVMCidr = source.ExtendedMetaData.EndpointTenantVpc.HostVMCidr
	target.Spec.Configuration.EndpointTenantVpc.DomainID = source.ExtendedMetaData.EndpointTenantVpc.DomainID
	target.Spec.Configuration.EndpointTenantVpc.Gateway = source.ExtendedMetaData.EndpointTenantVpc.Gateway
	target.Spec.Configuration.EndpointTenantVpc.ID = source.ExtendedMetaData.EndpointTenantVpc.ID
	target.Spec.Configuration.EndpointTenantVpc.SubnetID = source.ExtendedMetaData.EndpointTenantVpc.SubnetID
	target.Spec.Configuration.EndpointTenantVpc.SubnetName = source.ExtendedMetaData.EndpointTenantVpc.SubnetName
	target.Spec.Configuration.EndpointTenantVpc.VpcID = source.ExtendedMetaData.EndpointTenantVpc.VpcID
	target.Spec.Configuration.EndpointTenantVpc.VpcName = source.ExtendedMetaData.EndpointTenantVpc.VpcName
	target.Spec.Configuration.EndpointTenantVpc.Xrole = source.ExtendedMetaData.EndpointTenantVpc.Xrole
	target.Spec.Configuration.EndpointTenantVpc.SecurityGroups = source.ExtendedMetaData.EndpointTenantVpc.SecurityGroups
	target.Spec.Configuration.EndpointTenantVpc.IPV6Enable = source.ExtendedMetaData.EndpointTenantVpc.IPV6Enable
	target.Spec.Configuration.EndpointTenantVpc.IPV6Gateway = source.ExtendedMetaData.EndpointTenantVpc.IPV6Gateway
	target.Spec.Configuration.EndpointTenantVpc.IPV6HostVMCidr = source.ExtendedMetaData.EndpointTenantVpc.IPV6HostVMCidr
	target.Spec.Configuration.EndpointTenantVpc.IPV6TenantCidr = source.ExtendedMetaData.EndpointTenantVpc.IPV6TenantCidr
	target.Spec.Configuration.EndpointTenantVpc.NeutronSubnetID = source.ExtendedMetaData.EndpointTenantVpc.NeutronSubnetID
	target.Spec.Configuration.EndpointTenantVpc.NeutronSubnetIDV6 = source.ExtendedMetaData.EndpointTenantVpc.NeutronSubnetIDV6
	target.Spec.Configuration.EndpointTenantVpc.IsSafety = source.ExtendedMetaData.EndpointTenantVpc.IsSafety
}

func transferResourceInfo(target *faasfunction.FunctionService, source *config.FunctionMetaInfo) {
	target.Spec.Configuration.Resource.CPU = source.ResourceMetaData.CPU
	target.Spec.Configuration.Resource.Mem = source.ResourceMetaData.Memory
	target.Spec.Configuration.Resource.GpuMemory = source.ResourceMetaData.GpuMemory
	target.Spec.Configuration.Resource.GpuType = source.ResourceMetaData.GpuType
	target.Spec.Configuration.Resource.CustomResources = source.ResourceMetaData.CustomResources
	target.Spec.Configuration.Resource.EnableTmpExpansion = source.ResourceMetaData.EnableTmpExpansion
	target.Spec.Configuration.Resource.EphemeralStorage = source.ResourceMetaData.EphemeralStorage
}

func transferEnvVariablesMeta(source *config.FunctionMetaInfo, target *faasfunction.FunctionService) {
	target.AppendEnvVariables("BUCKET_APPID", source.S3MetaData.AppID)
	target.AppendEnvVariables(EncryptedUserDataKey, source.EnvMetaData.EncryptedUserData)
	target.AppendEnvVariables(EnvironmentKey, source.EnvMetaData.Environment)
	target.AppendEnvVariables(EnvEncryptKmsCipher, source.EnvMetaData.UserDataEncryptKmsKeyCipher)
	target.AppendEnvVariables("BUCKET_ID", source.S3MetaData.BucketID)
	target.AppendEnvVariables("OBJECT_ID", source.S3MetaData.ObjectID)
	target.AppendEnvVariables("BUCKET_URL", source.S3MetaData.BucketURL)
}

func decryptKmsCipherToCache(source *config.FunctionMetaInfo) error {
	if notNeedKmsDecrypt(source) {
		return nil
	}
	agency := GetUserAgencyByFuncMetaInfo(source)
	if agency == nil {
		log.GetLogger().Errorf("function %s need kms decode but agency is empty", source.FuncMetaData.FunctionURN)
		return nil
	}

	envKeyID := source.EnvMetaData.UserDataEncryptKmsKeyID
	envCipher := source.EnvMetaData.UserDataEncryptKmsKeyCipher
	err := decryptKmsCipher(source, *agency, envKeyID, envCipher, EnvDecType)
	if err != nil {
		storeKmsAbnormalCipher(source, envKeyID, EnvDecType, envCipher)
	}

	codeKeyID := source.S3MetaData.CodeEncryptKmsKeyID
	codeCipher := source.S3MetaData.CodeEncryptKmsKeyCipher
	err = decryptKmsCipher(source, *agency, codeKeyID, codeCipher, CodeDecType)
	if err != nil {
		storeKmsAbnormalCipher(source, codeKeyID, CodeDecType, codeCipher)
	}

	return nil
}

func notNeedKmsDecrypt(source *config.FunctionMetaInfo) bool {
	return (source.EnvMetaData.UserDataEncryptKmsKeyID == "" || source.EnvMetaData.UserDataEncryptKmsKeyCipher == "") &&
		(source.S3MetaData.CodeEncryptKmsKeyCipher == "" || source.S3MetaData.CodeEncryptKmsKeyID == "")
}

func decryptKmsCipher(source *config.FunctionMetaInfo, agency UserAgency, keyID, cipher, decType string) error {
	if len(keyID) == 0 || len(cipher) == 0 {
		return nil
	}
	urn := source.FuncMetaData.FunctionURN
	info, err := urnutils.GetFunctionInfo(urn)
	if err != nil {
		log.GetLogger().Errorf("failed to get function %s info bye urn when decrypt kms cipher, err: %v", urn, err)
		return err
	}

	// agency ID has been checked when get agency
	cacheKey := strings.Join([]string{info.TenantID, decType, keyID,
		source.FuncMetaData.DomainID, getAgencyIDFromFunctionMeta(source)}, CipherCacheGap)

	if value, ok := userCipherMap.Load(cacheKey); ok {
		s, ok := value.(UserCipherCache)
		if ok {
			s.LastUseTime = time.Now()
			userCipherMap.Store(cacheKey, s)
			return nil
		}
	}
	log.GetLogger().Infof("doesn't get function %s kms cipher in cache, begin to decrypt, decType: %s", source.FuncMetaData.FunctionURN, decType)

	client, err := GetKmsClientByAgency(agency, GetTenantIDFromURN(source.FuncMetaData.FunctionVersionURN))
	if err != nil {
		log.GetLogger().Errorf("failed to get kms client by agency, function: %s, decType: %s, err: %s", urn, decType, err.Error())
		return err
	}
	resp, err := client.DecryptDataKey(keyID, cipher)
	if err != nil {
		log.GetLogger().Errorf("failed to Decrypt kms Data Key, function: %s, decType: %s, err: %s", urn, decType, err.Error())
		return err
	}
	if resp == nil {
		log.GetLogger().Errorf("failed to get kms key bytes, function: %s, decType: %s", urn, decType)
		return errors.New("get key bytes failed")
	}

	userCipherMap.Store(cacheKey, UserCipherCache{
		LastUseTime: time.Now(),
		Ciphertext:  cipher,
		Plaintext:   *resp.DataKey,
	})

	log.GetLogger().Infof("decrypt Kms Cipher success, function: %s, decType: %s", urn, decType)
	return nil
}

func storeKmsAbnormalCipher(source *config.FunctionMetaInfo, keyID, decType, cipher string) {
	urn := source.FuncMetaData.FunctionURN
	info, err := urnutils.GetFunctionInfo(urn)
	if err != nil {
		log.GetLogger().Errorf("failed to get function %s info by urn when store kms abnormal cipher, err: %v", urn, err)
		return
	}
	cacheKey := strings.Join([]string{info.TenantID, decType, keyID,
		source.FuncMetaData.DomainID, getAgencyIDFromFunctionMeta(source)}, CipherCacheGap)
	userCipherMap.Store(cacheKey, UserCipherCache{
		LastUseTime: time.Now(),
		Ciphertext:  cipher,
		Plaintext:   constants.KmsAbnormalCipher,
	})
}

// GetKmsClientByAgency -
func GetKmsClientByAgency(agency UserAgency, tenantID string) (*kms.Client, error) {
	securityAk, err := localauth.Decrypt(agency.SecurityAk)
	if err != nil {
		log.GetLogger().Warnf("failed to decrypt agency securityAk: %s", err.Error())
		return nil, err
	}
	securitySk, err := localauth.Decrypt(agency.SecuritySk)
	if err != nil {
		log.GetLogger().Warnf("failed to decrypt agency securitySk: %s", err.Error())
		return nil, err
	}
	securityToken, err := localauth.Decrypt(agency.SecurityToken)
	if err != nil {
		log.GetLogger().Warnf("failed to decrypt agency securityToken: %s", err.Error())
		return nil, err
	}
	client := kms.NewKmsClient(tenantID, string(securityAk), string(securitySk), string(securityToken))
	return client, nil
}

func setRuntimeMeta(source *config.FunctionMetaInfo, target *faasfunction.FunctionService, deploypath string) {
	switch {
	case strings.Contains(source.FuncMetaData.Runtime, "nodejs"):
		strs := strings.Split(source.FuncMetaData.Handler, ".")
		if len(strs) == 2 {
			target.Spec.Configuration.Handler.Module = deploypath + strs[0] + ".js"
			target.Spec.Configuration.Handler.Entry = strs[1]
		} else {
			target.Spec.Configuration.Handler.Module = deploypath + "handler.js"
			target.Spec.Configuration.Handler.Entry = source.FuncMetaData.Handler
		}
	case strings.Contains(source.FuncMetaData.Runtime, "java"):
		strs := strings.Split(source.FuncMetaData.Handler, "::")
		if len(strs) == 2 {
			target.Spec.Configuration.Handler.Module = strs[0]
			target.Spec.Configuration.Handler.Entry = strs[1]
		} else {
			target.Spec.Configuration.Handler.Module = source.FuncMetaData.Handler
		}
	case strings.Contains(source.FuncMetaData.Runtime, "python"):
		strs := strings.Split(source.FuncMetaData.Handler, ".")
		if len(strs) == 2 {
			target.Spec.Configuration.Handler.Module = deploypath + strs[0] + ".py"
			target.Spec.Configuration.Handler.Entry = strs[1]
		} else {
			target.Spec.Configuration.Handler.Module = deploypath + "handler.py"
			target.Spec.Configuration.Handler.Entry = source.FuncMetaData.Handler
		}
	case strings.Contains(source.FuncMetaData.Runtime, "cpp"):
		// Cpp runtime and cppbin runtime only have binary path
		target.Spec.Configuration.Handler.Module = deploypath + source.FuncMetaData.Handler
		target.Spec.Configuration.Handler.Entry = source.FuncMetaData.Handler
	case strings.Contains(source.FuncMetaData.Runtime, "cangjie"):
		target.Spec.Configuration.Handler.Module = deploypath + source.FuncMetaData.Handler
		target.Spec.Configuration.Handler.Entry = source.FuncMetaData.Handler
	case strings.Contains(source.FuncMetaData.Runtime, "custom-runtime"),
		strings.Contains(source.FuncMetaData.Runtime, "http"),
		strings.Contains(source.FuncMetaData.Runtime, "php"),
		strings.Contains(source.FuncMetaData.Runtime, "c#"),
		strings.Contains(source.FuncMetaData.Runtime, "go1.x"):
		target.Spec.Configuration.Handler.Entry = source.FuncMetaData.Handler
	case strings.Contains(source.FuncMetaData.Runtime, "custom image"):
		// do nothing
	default:
		log.GetLogger().Warnf("unsupported language %s", source.FuncMetaData.Runtime)
	}
}

func transferRuntimeMeta(source *config.FunctionMetaInfo, target *faasfunction.FunctionService) error {
	// add runtime images
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	// set bus protocol type to target
	target.AppendEnvVariables("BUS_PROTOCOL_TYPE", cfg.BusProtocolType)
	setSpecFromMeta(source, target)

	deploydir, err := filepath.Abs(cfg.DeployDir)
	if err != nil {
		log.GetLogger().Warnf("failed to get absolute path: %s", err.Error())
		return err
	}
	deploypath := deploydir + "/"
	setRuntimeMeta(source, target, deploypath)
	return nil
}

func setSpecFromMeta(source *config.FunctionMetaInfo, target *faasfunction.FunctionService) {
	if source.FuncMetaData.Runtime == "custom-runtime" && source.ExtendedMetaData.EnableCustomRPC {
		target.Spec.Configuration.Runtime.Language = "custom-runtime-rpc"
	} else {
		target.Spec.Configuration.Runtime.Language = source.FuncMetaData.Runtime
	}
	target.Spec.Configuration.AutoScalerConfig.IdleMode = source.InstanceMetaData.IdleMode
	target.Spec.Configuration.AutoScalerConfig.MinimumScale = source.InstanceMetaData.MinInstance
	target.Spec.Configuration.AutoScalerConfig.MaximumScale = source.InstanceMetaData.MaxInstance
	target.Spec.Configuration.AutoScalerConfig.InstanceType = source.InstanceMetaData.InstanceType
	target.Spec.Concurrency.ReservedValue = source.InstanceMetaData.ConcurrentNum
	target.Spec.Configuration.Initializer.Handler = source.ExtendedMetaData.Initializer.Handler
	target.Spec.Configuration.Initializer.Timeout = source.ExtendedMetaData.Initializer.Timeout
	target.Spec.Configuration.CustomMetricsReport.Handler = source.ExtendedMetaData.CustomMetricsReport.Handler
	target.Spec.Configuration.CustomMetricsReport.CollectTime = source.ExtendedMetaData.CustomMetricsReport.CollectTime
	target.Spec.Configuration.CustomMetricsReport.Timeout = source.ExtendedMetaData.CustomMetricsReport.Timeout
	target.Spec.Configuration.Role.XRole = source.ExtendedMetaData.Role.XRole
	target.Spec.Configuration.Role.AppXRole = source.ExtendedMetaData.Role.AppXRole
	target.Spec.Configuration.EnableClassIsolation = source.ExtendedMetaData.EnableClassIsolation
	target.Spec.Configuration.DNSConfig = source.FuncMetaData.DNSConfig
}

// FunctionNameSpace return function's namespace
func FunctionNameSpace() string {
	if functionServiceNamespace == "" {
		faasConfig, err := config.GetConfig()
		if err != nil {
			return ""
		}
		functionServiceNamespace = faasConfig.NameSpace
	}
	return functionServiceNamespace
}

// GetCRSignature -
func GetCRSignature(functionMeta *faasfunction.FunctionService) string {
	var configSpec faasfunction.ConfigurationSpec
	err := DeepCopy(&functionMeta.Spec.Configuration, &configSpec)
	if err != nil {
		return ""
	}
	// default value is used to calculate the CR signature and does not change the actual value.
	configSpec.AutoScalerConfig = faasfunction.AutoScalerConfig{}

	configStr, err := json.Marshal(configSpec)
	if err != nil {
		log.GetLogger().Errorf("function service configuration serialized to json string error: %s, funcName: %s",
			err.Error(), functionMeta.Spec.Configuration.FunctionName)
		return ""
	}

	return FnvHash(string(configStr))
}

// DeepCopy deal with src and dst
func DeepCopy(src interface{}, dst interface{}) error {
	if dst == nil {
		return fmt.Errorf("dst cannot be nil")
	}
	if src == nil {
		return fmt.Errorf("src cannot be nil")
	}

	bytes, err := json.Marshal(src)
	if err != nil {
		return fmt.Errorf("unable to marshal src: %s", err)
	}

	err = json.Unmarshal(bytes, dst)
	if err != nil {
		return fmt.Errorf("unable to unmarshal into dst: %s", err)
	}
	return nil
}

// FnvHash a hash function
func FnvHash(s string) string {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	if err != nil {
		return ""
	}

	// for 2 <= base <= 36. The result uses the lower-case letters 'a' to 'z'
	return strconv.FormatUint(uint64(h.Sum32()), 10)
}

func initKubeClient() {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config, error: %s", err)
		return
	}
	// get the config, either from kubeconfig or using our in-cluster service account
	kubeConfig, err := k8sclient.GetKubeConfig()
	if err != nil {
		log.GetLogger().Errorf("get k8s config error: %s when new resource lock cluster", err.Error())
		return
	}

	kubeConfig.QPS = kubeClientQPS
	kubeConfig.Burst = kubeClientBurst
	if cfg.KubeCfgTimeout >= 0 {
		kubeConfig.Timeout = time.Duration(cfg.KubeCfgTimeout) * time.Second
	}
	// creates the clientset
	KubeClientSet, err = kubernetes.NewForConfig(kubeConfig)
	if err != nil {
		log.GetLogger().Errorf("failed to create kubernetes clientset: %s", err)
		return
	}
	kubeMetricsClient, err = metrics.NewForConfig(kubeConfig)
	if err != nil {
		log.GetLogger().Errorf("failed to create kube metrics clientSet: %s", err.Error())
		return
	}
}

// GetKubernetesClient Get Kubernetes Client
func GetKubernetesClient() kubernetes.Interface {
	kubeClientOnce.Do(func() {
		initKubeClient()
	})
	return KubeClientSet
}

// GetKubernetesMetricsClient return k8s metrics client
func GetKubernetesMetricsClient() metrics.Interface {
	kubeClientOnce.Do(func() {
		initKubeClient()
	})
	return kubeMetricsClient
}

// GetWorkerManagerDeployUID get worker manager deploy uid
func GetWorkerManagerDeployUID() types.UID {
	if workerManagerDeploy == nil {
		return ""
	}
	return workerManagerDeploy.UID
}

// InitWorkerManagerDeploy init worker manager deploy
func InitWorkerManagerDeploy() error {
	var err error
	workerManagerDeploy, err = GetKubernetesClient().AppsV1().Deployments(utils.PodNameSpace()).
		Get(context.TODO(), WorkerManagerDeployName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	return nil
}

// GetFullFuncNameByPod get full function name by pod, 0@default@hello
func GetFullFuncNameByPod(pod *corev1.Pod) string {
	name := pod.Annotations[AnnotationFullFuncName]
	if name != "" {
		return name
	}
	name = getFuncName(pod.Labels[LabelFuncNamePrefix], pod.Labels[LabelServiceID], pod.Labels[LabelShortFuncName])
	if name == "@@" {
		return ""
	}
	return name
}

func getFuncName(prefix, serviceID, shortFuncName string) string {
	return prefix + FuncNameSeparator + serviceID + FuncNameSeparator + shortFuncName
}

// OwnedByFunctionService  define a function to get pod owner
func OwnedByFunctionService(pod *corev1.Pod) (string, bool) {
	for _, ref := range pod.GetOwnerReferences() {
		if ref.Name == WorkerManagerDeployName {
			funcName := GetFullFuncNameByPod(pod)
			funcSvcID := CrName(pod.Labels[LabelBusinessID], pod.Labels[LabelTenantID],
				funcName, pod.Labels[LabelFunctionVersion])
			log.GetLogger().Debugf("business: %s, tenantID: %s, funcName: %s, version: %s for func: %s",
				pod.Labels[LabelBusinessID], urnutils.AnonymizeTenantURN(pod.Labels[LabelTenantID]),
				funcName, pod.Labels[LabelFunctionVersion], funcSvcID)
			return funcSvcID, true
		}
		if ref.APIVersion == FaasAPIVersion && ref.Kind == FaasAPIName {
			return ref.Name, true
		}
	}
	return "", false
}

// SplitFuncName split function name return funcNamePrefix serviceID and shortFuncName
func SplitFuncName(funcName string) (string, string, string) {
	funcNameSplits := strings.Split(funcName, FuncNameSeparator)
	if len(funcNameSplits) < SplitFuncNameLen {
		return "", "", ""
	}
	return funcNameSplits[FuncNamePrefixIndex], funcNameSplits[ServiceIDIndex], funcNameSplits[ShortFuncNameIndex]
}

// IsPodReady checks if a pod is ready
func IsPodReady(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	condition := getPodCondition(&pod.Status, corev1.PodReady)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

// IsWorkerReady checks if worker is ready
func IsWorkerReady(pod *corev1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.Name == WorkerContainerName {
			if status.Ready {
				return true
			}
			break
		}
	}
	return false
}

// getPodCondition extracts the provided condition from the given status if exists
func getPodCondition(status *corev1.PodStatus,
	conditionType corev1.PodConditionType) *corev1.PodCondition {
	if status == nil {
		return nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return &status.Conditions[i]
		}
	}
	return nil
}

// GetPodNameFromInstanceID get podName from instanceID
func GetPodNameFromInstanceID(instanceID string) string {
	str := strings.Split(instanceID, azSep)
	if len(str) == instanceIDLength {
		return str[podNameLocation]
	}
	return ""
}

// GetUserAgencyFromCacheMap get user agency from cache
func GetUserAgencyFromCacheMap(tenant string, domainID string, agencyID string) UserAgency {
	var agency UserAgency
	key := WatchPathPrefixWithAgency + tenant + "/domain/" + domainID + "/agency/" + agencyID
	if value, ok := userAgencyCacheMap.Load(key); ok {
		agency, ok = value.(UserAgency)
		if !ok {
			log.GetLogger().Errorf("not a valid userAgency cache")
		}
	}
	return agency
}

var dnsAddress = fmt.Sprintf("https://dns.%s.myhuaweicloud.com", os.Getenv("REGION_ID"))

// GetDNSAddress -
func GetDNSAddress() string {
	return dnsAddress
}

// GetUserAgencyByFuncMeta get user agency by function meta
func GetUserAgencyByFuncMeta(funcMeta *faasfunction.FunctionService) UserAgency {
	if funcMeta == nil {
		log.GetLogger().Errorf("funcMeta is nil and agency is empty")
		return UserAgency{}
	}
	agencyID := GetAgencyIDFromFunctionService(funcMeta)
	if agencyID != "" {
		tenantID := GetTenantIDFromURN(funcMeta.Spec.Configuration.FunctionVersionURN)
		domainID := funcMeta.Spec.Configuration.DomainID
		return GetUserAgencyFromCacheMap(tenantID, domainID, agencyID)
	}
	return UserAgency{}
}

// GetAgencyIDFromFunctionService -
func GetAgencyIDFromFunctionService(funcMeta *faasfunction.FunctionService) string {
	agencyID := ""
	if funcMeta.Spec.Configuration.Role.AppXRole != "" {
		agencyID = funcMeta.Spec.Configuration.Role.AppXRole
	} else if funcMeta.Spec.Configuration.Role.XRole != "" {
		agencyID = funcMeta.Spec.Configuration.Role.XRole
	}
	return agencyID
}

// GetUserAgencyByFuncMetaInfo get user agency by function meta
func GetUserAgencyByFuncMetaInfo(source *config.FunctionMetaInfo) *UserAgency {
	if source == nil {
		log.GetLogger().Errorf("funcMeta is nil and agency is empty")
		return nil
	}
	agencyID := getAgencyIDFromFunctionMeta(source)
	if agencyID != "" {
		tenantID := GetTenantIDFromURN(source.FuncMetaData.FunctionVersionURN)
		domainID := source.FuncMetaData.DomainID
		cacheMap := GetUserAgencyFromCacheMap(tenantID, domainID, agencyID)
		return &cacheMap
	}
	return nil
}

func getAgencyIDFromFunctionMeta(source *config.FunctionMetaInfo) string {
	agencyID := ""
	if source.ExtendedMetaData.Role.AppXRole != "" {
		agencyID = source.ExtendedMetaData.Role.AppXRole
	} else if source.ExtendedMetaData.Role.XRole != "" {
		agencyID = source.ExtendedMetaData.Role.XRole
	}
	return agencyID
}

// GetFuncClusterByFuncMeta get function cluster by function meta
func GetFuncClusterByFuncMeta(funcMeta *faasfunction.FunctionService) []string {
	var clusters []string
	if funcMeta == nil {
		log.GetLogger().Errorf("funcMeta is nil and func cluster is empty")
		return nil
	}
	FuncURN := funcMeta.Spec.Configuration.FunctionVersionURN
	key := WatchClusterPrefix + FuncURN
	if value, ok := functionClusterCacheMap.Load(key); ok {
		clusters, ok = value.([]string)
		if !ok {
			log.GetLogger().Errorf("not a valid func cluster cache")
			return nil
		}
	}
	return clusters
}

// GetUserAgencyByVpcConfig get vpc user agency
func GetUserAgencyByVpcConfig(vpcConfig faasfunction.Vpc) UserAgency {
	return GetUserAgencyFromCacheMap(vpcConfig.Namespace, vpcConfig.DomainID, vpcConfig.Xrole)
}

// WatcherUserAgency watch etcd user agency path
func WatcherUserAgency(stopCh <-chan struct{}) error {
	go runWatcher(stopCh)
	go runCAEWatcher(stopCh)
	return nil
}

func initCacheMap(client *etcd3.EtcdClient) error {
	if err := initAgencyCacheMap(client); err != nil {
		return err
	}
	if err := initClusterCacheMap(client); err != nil {
		return err
	}
	return nil
}

func initAgencyCacheMap(client *etcd3.EtcdClient) error {
	agencyETCDCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := client.GetResponse(agencyETCDCtx, WatchPathPrefixWithAgency, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("list path(%s) error: %s", WatchPathPrefixWithAgency, err.Error())
		return err
	}
	for _, kv := range resp.Kvs {
		updateUserAgencyCacheMap(string(kv.Key), kv.Value, client.GetEtcdType())
	}

	return nil
}

func initClusterCacheMap(client *etcd3.EtcdClient) error {
	clusterETCDCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := client.GetResponse(clusterETCDCtx, WatchClusterPrefix, clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("list path(%s) error: %s", WatchClusterPrefix, err.Error())
		return err
	}
	for _, kv := range resp.Kvs {
		updateFunctionClusterCacheMap(string(kv.Key), kv.Value)
	}
	return nil
}

func runWatcher(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	metaClient := etcd3.GetMetaEtcdClient()
	if metaClient == nil {
		log.GetLogger().Errorf("FG meta client is nil")
		return
	}
	agencyWatchChan, clusterWatchChan := getWatchChan(metaClient)
	if agencyWatchChan == nil || clusterWatchChan == nil {
		return
	}
	for {
		select {
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Infof("received signal to quit")
				return
			}
		case event, ok := <-agencyWatchChan.ResultChan:
			if ok {
				if isUserAgencyEvent(event.Key) {
					parseUserAgencyEvent(event, etcd3.Meta)
				}
				continue
			}
			log.GetLogger().Warnf("the agency watch channel received the result may be closed and try to reconnect")
			agencyWatchChan.Stop()
			agencyWatchChan = metaClient.Watch(context.Background(), WatchPathPrefixWithAgency, 0, true)
		case event, ok := <-clusterWatchChan.ResultChan:
			if ok {
				if isFunctionClusterEvent(event.Key) {
					parseFunctionClusterEvent(event)
				}
				continue
			}
			log.GetLogger().Warnf("the cluster channel received the result may be closed and try to reconnect")
			clusterWatchChan.Stop()
			clusterWatchChan = metaClient.Watch(context.Background(), WatchClusterPrefix, 0, true)
		}
	}
}

func runCAEWatcher(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	CAEMetaClient := etcd3.GetCAEMetaEtcdClient()
	if CAEMetaClient == nil {
		log.GetLogger().Errorf("CAE meta client is nil")
		return
	}
	CAEAgencyWatchChan := getCAEWatchChan(CAEMetaClient)
	if CAEAgencyWatchChan == nil {
		return
	}
	for {
		select {
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Infof("stopCh closed, CAEWatcher quit")
				return
			}
			log.GetLogger().Infof("CAEWatcher received signal to quit")
			return
		case event, ok := <-CAEAgencyWatchChan.ResultChan:
			if ok {
				if isUserAgencyEvent(event.Key) {
					parseUserAgencyEvent(event, etcd3.CAEMeta)
				}
				continue
			}
			log.GetLogger().Warnf("the channel of cae meta received the result may be closed and try to reconnect")
			CAEAgencyWatchChan.Stop()
			CAEAgencyWatchChan = CAEMetaClient.Watch(context.Background(), WatchPathPrefixWithAgency, 0, true)
		}
	}
}

// key: /sn/agency/business/yrk/tenant/{tenantid}/domain/
func isUserAgencyEvent(key string) bool {
	return strings.HasPrefix(key, WatchPathPrefixWithAgency)
}

// key: /sn/function/available/clusters/{funcURN}
func isFunctionClusterEvent(key string) bool {
	return strings.HasPrefix(key, WatchClusterPrefix)
}

func getWatchChan(metaClient *etcd3.EtcdClient) (*etcd3.EtcdWatchChan, *etcd3.EtcdWatchChan) {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if metaClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			if err := initCacheMap(metaClient); err != nil {
				log.GetLogger().Errorf("init cache map  with error %s", err.Error())
				ticker.Stop()
				return nil, nil
			}
			agencyWatchChan := metaClient.Watch(context.Background(), WatchPathPrefixWithAgency, 0, true)
			clusterWatchChan := metaClient.Watch(context.Background(), WatchClusterPrefix, 0, true)
			log.GetLogger().Infof("success to get watch channel")
			ticker.Stop()
			return agencyWatchChan, clusterWatchChan
		}
	}
}

func getCAEWatchChan(CAEMetaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if CAEMetaClient.Client == nil {
				log.GetLogger().Warnf("failed to init CAE meta etcd client")
				continue
			}
			if err := initAgencyCacheMap(CAEMetaClient); err != nil {
				log.GetLogger().Errorf("init agency cache map with error %s", err.Error())
				ticker.Stop()
				return nil
			}
			agencyWatchChan := CAEMetaClient.Watch(context.Background(), WatchPathPrefixWithAgency, 0, true)
			log.GetLogger().Infof("success to get CAE watch channel")
			ticker.Stop()
			return agencyWatchChan
		}
	}
}

func canCachedAgencyBeChanged(key string, etcdType etcd3.EtcdType) bool {
	// fg etcd type is 0, cae etcd type is 3. if cached type < event type, no need to update
	if agency, ok := userAgencyCacheMap.Load(key); ok && agency != nil && agency.(UserAgency).Source < etcdType {
		log.GetLogger().Infof("no need to change cached agency info. key: %s, cache source: %s, event source: %s.",
			key, agency.(UserAgency).Source.String(), etcdType.String())
		return false
	}
	return true
}

func parseUserAgencyEvent(event *etcd3.Event, etcdType etcd3.EtcdType) {
	switch event.Type {
	case etcd3.PUT:
		log.GetLogger().Infof("userAgencyPut | %s", event.Key)
		updateUserAgencyCacheMap(event.Key, event.Value, etcdType)
	case etcd3.DELETE:
		log.GetLogger().Infof("user agency delete event, key: %s, event etcd type: %s", event.Key, etcdType.String())
		if canCachedAgencyBeChanged(event.Key, etcdType) {
			userAgencyCacheMap.Delete(event.Key)
		}
		return
	default:
		log.GetLogger().Infof("unknown event type: %d", event.Type)
	}
}

func parseFunctionClusterEvent(event *etcd3.Event) {
	switch event.Type {
	case etcd3.PUT:
		log.GetLogger().Infof("parse function cluster event with key: %s", event.Key)
		updateFunctionClusterCacheMap(event.Key, event.Value)
	case etcd3.DELETE:
		log.GetLogger().Infof("user function cluster delete event, key: %s", event.Key)
		functionClusterCacheMap.Delete(event.Key)
	default:
		log.GetLogger().Infof("unknown event type: %d", event.Type)
	}
}

func updateFunctionClusterCacheMap(key string, value []byte) {
	var clusters []string
	clusters = strings.Split(string(value), ",")
	if len(clusters) == 1 {
		clusters = []string{string(value)}
	}
	functionClusterCacheMap.Store(key, clusters)
}

func updateUserAgencyCacheMap(key string, value []byte, etcdType etcd3.EtcdType) {
	if !canCachedAgencyBeChanged(key, etcdType) {
		return
	}
	var agency = UserAgency{}
	if err := json.Unmarshal(value, &agency); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the json, error: %s", err.Error())
		return
	}
	agency.Source = etcdType
	userAgencyCacheMap.Delete(key)
	userAgencyCacheMap.Store(key, agency)
}

// IsWorkerPod checks if this pod is a worker pod
func IsWorkerPod(pod *corev1.Pod) bool {
	if _, exist := pod.Labels[IsPoolPodLabelKey]; exist {
		return true
	}
	return false
}

// IsPoolPod checks if this pod is a worker pod stays in pool
func IsPoolPod(pod *corev1.Pod) bool {
	if flag, exist := pod.Labels[IsPoolPodLabelKey]; exist {
		return flag == TrueString
	}
	return false
}

// IsInstancePod check if pod is instance(isPoolPod=false) will return true
func IsInstancePod(pod *corev1.Pod) bool {
	flag, exist := pod.Labels[IsPoolPodLabelKey]
	return exist && flag == FalseString
}

// IsReservedInstance checks if this pod is a reserved instance
func IsReservedInstance(pod *corev1.Pod) bool {
	return pod.Labels["applier"] == constants.WorkerManagerApplier
}

// IsDsWorker checks if this pod is ds-worker
func IsDsWorker(pod *corev1.Pod) bool {
	return strings.Contains(pod.Name, DsWorkerPodNameKey)
}

// GetNodeIPAddress gets ip of a node
func GetNodeIPAddress(node *corev1.Node) string {
	for _, nodeAddress := range node.Status.Addresses {
		if nodeAddress.Type == corev1.NodeInternalIP {
			return nodeAddress.Address
		}
	}
	return ""
}

// GetRandomIndex returns a random index from [0, n)
func GetRandomIndex(n int) int {
	randomIndex, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		log.GetLogger().Errorf("failed to generate random index %s", err.Error())
		return 0
	}
	return int(randomIndex.Int64())
}

// GetOwnerIPByRequest -
func GetOwnerIPByRequest(pod *corev1.Pod, requestInfo *config.RequestInfo) string {
	// pre-set ownerIP here so worker cache can get owner ip before hand
	ownerIP := pod.Status.HostIP
	if config.GetEnableRemoteWorkerFlag() {
		ownerIP = requestInfo.OwnerIP
	}
	return ownerIP
}

// MakeInitWorkerIns -
func MakeInitWorkerIns(pod *corev1.Pod, spec *faasfunction.FunctionServiceSpec) v1alpha1.WorkerInstance {
	cpu, memory := 0, 0
	if !utils.IsCAEFunc(spec.Configuration.BusinessType) {
		cpu = GetPodCPUValue(pod)
		memory = GetPodMemoryValue(pod)
	}
	return v1alpha1.WorkerInstance{
		InstanceID:   utils.GenerateInstanceID(pod.Name),
		PodName:      pod.Name,
		IP:           pod.Status.PodIP,
		Port:         "8080",
		Status:       "init",
		Cluster:      clusterID,
		P2pPort:      P2pPort,
		NodeIP:       pod.Status.HostIP,
		NodePort:     NodePort,
		NodeName:     pod.Spec.NodeName,
		CPU:          cpu,
		Memory:       memory,
		BusinessID:   spec.Configuration.BusinessID,
		FuncName:     spec.Configuration.FunctionName,
		TenantID:     spec.Configuration.TenantID,
		Version:      spec.Configuration.FunctionVersion,
		BusinessType: spec.Configuration.BusinessType,
	}
}

// MakeWorkerInsWithRequest make worker instance with request
func MakeWorkerInsWithRequest(pod *corev1.Pod, requestInfo *config.RequestInfo,
	crSignature, businessType string) v1alpha1.WorkerInstance {
	cpu, memory := 0, 0
	if !utils.IsCAEFunc(businessType) {
		cpu = GetPodCPUValue(pod)
		memory = GetPodMemoryValue(pod)
	}
	return v1alpha1.WorkerInstance{
		InstanceID:   utils.GenerateInstanceID(pod.Name),
		PodName:      pod.Name,
		IP:           pod.Status.PodIP,
		Port:         "8080",
		Status:       "ready",
		Cluster:      clusterID,
		P2pPort:      P2pPort,
		NodeIP:       pod.Status.HostIP,
		NodePort:     NodePort,
		NodeName:     pod.Spec.NodeName,
		StateID:      requestInfo.StateID,
		Applier:      requestInfo.Applier,
		OwnerIP:      GetOwnerIPByRequest(pod, requestInfo),
		CPU:          cpu,
		Memory:       memory,
		Signature:    crSignature,
		BusinessID:   requestInfo.BusinessID,
		FuncName:     requestInfo.FunctionName,
		TenantID:     requestInfo.TenantID,
		Version:      requestInfo.FunctionVersion,
		IsMigrating:  requestInfo.IsMigrating,
		MigratedFrom: requestInfo.MigratedFrom,
		Service:      pod.Labels["serviceID"],
		CreationTime: requestInfo.CreationTime,
		BusinessType: businessType,
	}
}

// MakeWorkerInsInfo transforms pod struct to workerInstance
func MakeWorkerInsInfo(pod *corev1.Pod) v1alpha1.WorkerInstance {
	ownerIP, exist := pod.Annotations[OwnerIPKey]
	if !exist {
		log.GetLogger().Warnf("owner ip for pod %s doesn't exist", pod.Name)
	}
	var creatorInfo string
	creatorFuncName, ok := pod.Labels[LabelCreatorFuncName]
	if ok {
		creatorInfo = creatorFuncName + ":" + pod.Labels[LabelCreatorFunctionVersion]
		creatorStateID, ok := pod.Labels[LabelCreatorStateID]
		if ok {
			creatorInfo += ":" + creatorStateID
		}
	}

	creationTime, err := strconv.ParseInt(pod.Labels[LabelCreationTime], 10, 64)
	if err != nil {
		log.GetLogger().Warnf("failed to parse int64 of creationTime: %s", pod.Labels[LabelCreationTime])
	}
	businessType := pod.Labels[LabelBusinessType]
	cpu, memory := 0, 0
	if !utils.IsCAEFunc(businessType) {
		cpu = GetPodCPUValue(pod)
		memory = GetPodMemoryValue(pod)
	}

	return v1alpha1.WorkerInstance{
		InstanceID:   utils.GenerateInstanceID(pod.Name),
		PodName:      pod.Name,
		IP:           pod.Status.PodIP,
		Port:         "8080",
		Status:       "ready",
		Cluster:      clusterID,
		P2pPort:      P2pPort,
		NodeIP:       pod.Status.HostIP,
		NodePort:     NodePort,
		NodeName:     pod.Spec.NodeName,
		StateID:      pod.Labels[LabelStateID],
		Applier:      pod.Labels[LabelApplier],
		BusinessID:   pod.Labels[LabelBusinessID],
		TenantID:     pod.Labels[LabelTenantID],
		Creator:      creatorInfo,
		FuncName:     GetFullFuncNameByPod(pod),
		Version:      pod.Labels[LabelFunctionVersion],
		OwnerIP:      ownerIP,
		CPU:          cpu,
		Memory:       memory,
		Signature:    GetPodCRSignature(pod),
		MigratedFrom: pod.Labels[LabelMigratedFrom],
		IsRunning:    pod.Status.Phase == corev1.PodRunning,
		CreationTime: creationTime,
		BusinessType: businessType,
		IsMCPFunc:    pod.Labels[LabelMCPFunc] == TrueString,
	}
}

// GetPodCRSignature returns function CR signature of worker pod
func GetPodCRSignature(pod *corev1.Pod) string {
	return pod.Annotations[CRSignatureKey]
}

// GetPodCPUValue return cpu
func GetPodCPUValue(pod *corev1.Pod) int {
	return getResourceValue(1, pod)
}

// GetPodMemoryValue return memory
func GetPodMemoryValue(pod *corev1.Pod) int {
	return getResourceValue(memoryIndex, pod)
}

func getResourceValue(index int, pod *corev1.Pod) int {
	s := strings.Split(pod.Name, "-")
	if len(s) > memoryIndex {
		atoi, err := strconv.Atoi(s[index])
		if err != nil {
			log.GetLogger().Errorf("failed to get value from pod name")
			return 0
		}
		return atoi
	}
	return 0
}

// TimeoutRetry will retry when error exist "timeout"
func TimeoutRetry(errorStr string) bool {
	if strings.Contains(errorStr, "timeout") {
		return true
	}
	return false
}

// PackPoolStandardByPoolConfig pack pool standards by pool config
func PackPoolStandardByPoolConfig(poolCfg []config.PoolConfig) []string {
	standards := make([]string, 0, len(poolCfg))
	for _, cfg := range poolCfg {
		standards = append(standards, PackPoolStandard(
			cfg.LimitCPU, cfg.LimitMemory, cfg.RunTimeName, cfg.CustomResources, cfg.Label))
	}
	return standards
}

// PackPoolStandard pack pool standard
func PackPoolStandard(cpu, mem int64, language string, customResource map[string]int64, labels map[string]string) string {
	var poolStandard string
	if len(customResource) != 0 {
		poolStandard = fmt.Sprintf("%d-%d-%s-%s", cpu, mem, language, mapToString(customResource))
	} else {
		poolStandard = fmt.Sprintf("%d-%d-%s", cpu, mem, language)
	}
	if !config.IsHeterogeneousScheduling() {
		return poolStandard
	}
	if _, ok := labels[LabelNodeType]; ok {
		poolStandard = fmt.Sprintf("%s-%s", poolStandard, labels[LabelNodeType])
	} else {
		poolStandard = fmt.Sprintf("%s-%s", poolStandard, DefaultNodeType)
	}
	return poolStandard
}

// PackPoolFullName pack pool full name
func PackPoolFullName(name string, cpu, mem int64, language string, labels map[string]string) string {
	name = strings.Split(name, "-")[0]
	if !config.IsHeterogeneousScheduling() {
		return fmt.Sprintf("%s-%d-%d-%s", name, cpu, mem, language)
	}
	if _, ok := labels[LabelNodeType]; ok {
		return fmt.Sprintf("%s-%d-%d-%s-%s", name, cpu, mem, language, labels[LabelNodeType])
	}
	return fmt.Sprintf("%s-%d-%d-%s-%s", name, cpu, mem, language, DefaultNodeType)
}

func mapToString(mapValue map[string]int64) string {
	// ["nivida/gpu":1] will be transformed to "-nivida-gpu-1" as postfix to identify this pool is using custom
	// resources
	rawStr := strings.TrimPrefix(fmt.Sprintf("%v", mapValue), "map")
	// "/" "-" are not valid deployment name
	replacer := strings.NewReplacer("[", "", "]", "", ",", "-", "/", "-", ":", "-")
	return replacer.Replace(rawStr)
}

// GetFuncKey get function key
func GetFuncKey(tenantID, serviceID, functionName, version string) string {
	if urnutils.IsComplexFuncName(functionName) {
		c := &urnutils.ComplexFuncName{}
		c.ParseFrom(functionName)
		functionName = c.FuncName
		serviceID = c.ServiceID
	}
	funcKey := fmt.Sprintf("%s@%s@%s@%s", tenantID, serviceID,
		functionName, version)
	return funcKey
}

// IsNodeReady check node status
func IsNodeReady(node *corev1.Node) bool {
	if len(node.Status.Conditions) == 0 {
		return false
	}
	for _, condition := range node.Status.Conditions {
		if condition.Type == "Ready" && condition.Status == "True" {
			return true
		}
	}
	return false
}

// PackRequestInfo pack request info
func PackRequestInfo(functionSvc *faasfunction.FunctionService, applier string, traceID string) *config.RequestInfo {
	return &config.RequestInfo{
		CPU:             functionSvc.Spec.Configuration.Resource.CPU,
		Memory:          functionSvc.Spec.Configuration.Resource.Mem,
		Namespace:       FunctionNameSpace(),
		BusinessID:      functionSvc.Spec.Configuration.BusinessID,
		TenantID:        functionSvc.Spec.Configuration.TenantID,
		FunctionName:    functionSvc.Spec.Configuration.FunctionName,
		FunctionVersion: functionSvc.Spec.Configuration.FunctionVersion,
		Applier:         applier,
		ForceDeploy:     "false",
		TraceID:         traceID,
	}
}

// BuildInstanceByPod build instance by pod
func BuildInstanceByPod(pod *corev1.Pod, requestInfo *config.RequestInfo) *commontypes.Instance {
	return &commontypes.Instance{
		IP:           pod.Status.PodIP,
		Port:         P2pPort,
		InstanceID:   utils.AzEnv() + utils.LayerListSep + pod.Name,
		DeployedIP:   pod.Status.HostIP,
		DeployedNode: pod.Spec.NodeName,
		OwnerIP:      GetOwnerIPByRequest(pod, requestInfo),
		IsReserved:   requestInfo.Applier == constants.WorkerManagerApplier,
	}
}

// BuildInstanceByWorkerIns build instance by worker instance
func BuildInstanceByWorkerIns(workerIns *v1alpha1.WorkerInstance) *commontypes.Instance {
	return &commontypes.Instance{
		IP:           workerIns.IP,
		Port:         workerIns.P2pPort,
		InstanceID:   workerIns.InstanceID,
		DeployedIP:   workerIns.NodeIP,
		DeployedNode: workerIns.NodeName,
		OwnerIP:      workerIns.OwnerIP,
		IsReserved:   workerIns.Applier == constants.WorkerManagerApplier,
	}
}

// IsCustomContainerSpec -
func IsCustomContainerSpec(spec *faasfunction.FunctionServiceSpec) bool {
	if spec == nil {
		log.GetLogger().Warnf("spec is nil")
		return false
	}
	return spec.Configuration.CustomContainerConfig.ControlPath != ""
}

// GetMetaInfoFromEtcd -
func GetMetaInfoFromEtcd(functionKey string) (*faasfunction.FunctionService, commontypes.ProfileMetaData,
	etcd3.EtcdType, error) {
	profile := commontypes.ProfileMetaData{}
	values, source, err := getValuesFromETCD(functionKey, valueTypeMeta)
	if err != nil {
		return nil, profile, source, err
	}
	if len(values) < 1 {
		log.GetLogger().Errorf("failed to get function %s", functionKey)
		return nil, profile, source, snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
	}

	var functionInfo = config.FunctionMetaInfo{}
	if err = json.Unmarshal([]byte(values[0]), &functionInfo); err != nil {
		log.GetLogger().Errorf("failed to Unmarshal meta func data %s", err.Error())
		return nil, profile, source, err
	}
	if err = setInstanceMetaFromETCD(functionKey, source, &functionInfo); err != nil {
		return nil, profile, source, err
	}
	if _, err = govalidator.ValidateStruct(functionInfo); err != nil {
		return nil, profile, source, err
	}
	targetFunctionService := &faasfunction.FunctionService{}
	if err = TransferRepoMetaToFunctionService(&functionInfo, targetFunctionService); err != nil {
		return nil, profile, source, err
	}

	return targetFunctionService, functionInfo.ProfileMetaData, source, nil
}

// CheckMetaInfoValid check whether function metadata info is valid
func CheckMetaInfoValid(value []byte) error {
	var functionInfo = config.FunctionMetaInfo{}
	if err := json.Unmarshal(value, &functionInfo); err != nil {
		return err
	}
	if _, err := govalidator.ValidateStruct(functionInfo); err != nil {
		return err
	}
	targetFunctionService := &faasfunction.FunctionService{}
	if err := TransferRepoMetaToFunctionService(&functionInfo, targetFunctionService); err != nil {
		return err
	}
	return nil
}

func setInstanceMetaFromETCD(functionKey string, source etcd3.EtcdType, functionInfo *config.FunctionMetaInfo) error {
	strs := strings.Split(functionKey, "/")
	if len(strs) < URNIndexEleven {
		log.GetLogger().Errorf("invalid function key: %s", functionKey)
		return snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
	}
	clusterID := os.Getenv("CLUSTER_ID")
	instanceKey := fmt.Sprintf("%s/%s/tenant/%s/function/%s/version/%s", InstanceMetadataPrefix,
		clusterID, strs[URNIndexSix], strs[URNIndexEight], strs[URNIndexTen])
	values, err := getValuesFromSelectETCD(source, instanceKey, valueTypeInstance)
	if len(values) < 1 {
		// no value is given to the default value.
		functionInfo.InstanceMetaData.MinInstance = DefaultMinInstanceNum
		functionInfo.InstanceMetaData.MaxInstance = DefaultMaxInstanceNum
		functionInfo.ProfileMetaData = commontypes.ProfileMetaData{}
		return nil
	}

	var instanceInfo = config.FunctionMetaInfo{}
	if err = json.Unmarshal([]byte(values[0]), &instanceInfo); err != nil {
		return err
	}
	if instanceInfo.InstanceMetaData.MinInstance < 0 {
		log.GetLogger().Errorf("min scale is invalid with value: %d", instanceInfo.InstanceMetaData.MinInstance)
		instanceInfo.InstanceMetaData.MinInstance = DefaultMinInstanceNum
	}
	if instanceInfo.InstanceMetaData.MaxInstance < 0 {
		log.GetLogger().Errorf("max scale is invalid with value: %d", instanceInfo.InstanceMetaData.MaxInstance)
		instanceInfo.InstanceMetaData.MaxInstance = DefaultMaxInstanceNum
	}
	functionInfo.InstanceMetaData.IdleMode = instanceInfo.InstanceMetaData.IdleMode
	functionInfo.InstanceMetaData.MinInstance = instanceInfo.InstanceMetaData.MinInstance
	functionInfo.InstanceMetaData.MaxInstance = instanceInfo.InstanceMetaData.MaxInstance
	functionInfo.InstanceMetaData.InstanceType = instanceInfo.InstanceMetaData.InstanceType
	functionInfo.ProfileMetaData = instanceInfo.ProfileMetaData
	return nil
}

func getValuesFromSelectETCD(selection etcd3.EtcdType, functionKey string, valueType int) ([]string, error) {
	var (
		values []string
		err    error
	)
	for i := 0; i < MaxEtcdRetryAttempts; i++ {
		if selection == etcd3.Meta && etcd3.GetETCDClient(selection).Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client")
			MetaCacheOnce.Do(func() {
				LocalMetaEtcdCache.InitMetaEtcdCache()
			})
			if valueType == valueTypeMeta {
				return LocalMetaEtcdCache.getStoreMetaInfo(functionKey), nil
			}
			time.Sleep(time.Second)
			continue
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		values, err = etcd3.GetETCDClient(selection).GetValues(defaultEtcdCtx, functionKey)
		if err == nil {
			break
		}
		time.Sleep(RetryInterval)
		log.GetLogger().Warnf("failed to get function %s info from %s etcd with error %s",
			etcdkey.AnonymizeTenantCommonEtcdKey(functionKey), selection, err.Error())
	}
	return values, err
}

func getValuesFromETCD(functionKey string, valueType int) ([]string, etcd3.EtcdType, error) {
	if valueType != valueTypeMeta && valueType != valueTypeInstance {
		values, err := getValuesFromSelectETCD(etcd3.Meta, functionKey, valueType)
		return values, etcd3.Meta, err
	}
	// first try get from meta etcd; if result is empty, then try to get from cae meta etcd
	// note: if result is error, we don't try to get from cae meta etcd
	values, err := getValuesFromSelectETCD(etcd3.Meta, functionKey, valueType)
	if err != nil || len(values) > 0 {
		return values, etcd3.Meta, err
	}
	// only when err == nil && len(values) == 0, then try to query from cae meta etcd
	if etcd3.GetETCDClient(etcd3.CAEMeta) == nil {
		return values, etcd3.Meta, err
	}
	values, err = getValuesFromSelectETCD(etcd3.CAEMeta, functionKey, valueType)
	return values, etcd3.CAEMeta, err
}

// InitMetaEtcdCache -
func (mc *LocalMetaEtcdCaches) InitMetaEtcdCache() {
	log.GetLogger().Infof("begin to init meta etcd cache")
	metaInfoPath := fmt.Sprintf("%s/%s", etcd3.DefaultMetaEtcdHomePath, etcd3.PartitionCacheMetaInfo)
	content, err := os.ReadFile(metaInfoPath)
	if err != nil {
		log.GetLogger().Errorf("failed to read meta info path")
		return
	}
	metaInfo := &etcd3.EtcdCacheMetaInfo{}
	err = json.Unmarshal(content, metaInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal etcd cache meta info, error: %s", err.Error())
		return
	}
	err = checkCacheMetaInfo(metaInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to check cache meta info, error: %s", err.Error())
		return
	}
	mc.storeMetaInfo(metaInfo)
}

func checkCacheMetaInfo(metaInfo *etcd3.EtcdCacheMetaInfo) error {
	if metaInfo.Version != etcd3.CacheVersion {
		return errors.New("cache file version not match")
	}
	for i := 0; i < metaInfo.PartitionSize; i++ {
		partitionName := fmt.Sprintf("%s%d", etcd3.PartitionCacheFilePrefix, i)
		if metaInfo.MD5s[partitionName] == "" {
			continue
		}
		partitionPath := fmt.Sprintf("%s/%s", etcd3.DefaultMetaEtcdHomePath, partitionName)
		md5, err := utils.FileMD5(partitionPath)
		if err != nil {
			return err
		}
		if metaInfo.MD5s[partitionName] != md5 {
			return errors.New("cache file md5 not match")
		}
	}
	return nil
}

func (mc *LocalMetaEtcdCaches) storeMetaInfo(metaInfo *etcd3.EtcdCacheMetaInfo) {
	mc.metaEtcdCacheMap = make(map[string]*etcd3.Event)
	for i := 0; i < metaInfo.PartitionSize; i++ {
		partitionName := fmt.Sprintf("%s%d", etcd3.PartitionCacheFilePrefix, i)
		if metaInfo.MD5s[partitionName] == "" {
			continue
		}
		Path := fmt.Sprintf("%s/%s", etcd3.DefaultMetaEtcdHomePath, partitionName)
		content, err := os.ReadFile(Path)
		if err != nil {
			log.GetLogger().Errorf("failed to read partition file, error: %s", err.Error())
			return
		}
		var etcdCache etcd3.EtcdCache
		err = json.Unmarshal(content, &etcdCache)
		if err != nil {
			return
		}
		events := etcdCache.Events
		for k, v := range events {
			mc.mux.Lock()
			mc.metaEtcdCacheMap[k] = v
			mc.mux.Unlock()
		}
	}
}

func (mc *LocalMetaEtcdCaches) getStoreMetaInfo(functionKey string) []string {
	mc.mux.Lock()
	for k, v := range mc.metaEtcdCacheMap {
		if k == functionKey {
			mc.mux.Unlock()
			return []string{string(v.Value)}
		}
	}
	mc.mux.Unlock()
	return nil
}

// GetAllStoreMetaInfo -
func (mc *LocalMetaEtcdCaches) GetAllStoreMetaInfo() []*etcd3.Event {
	var info []*etcd3.Event
	mc.mux.Lock()
	for key, v := range mc.metaEtcdCacheMap {
		v.Key = key
		info = append(info, v)
	}
	mc.mux.Unlock()
	return info
}

// ClearAllStoreMetaInfo -
func (mc *LocalMetaEtcdCaches) ClearAllStoreMetaInfo() {
	mc.mux.Lock()
	emptyCache := make(map[string]*etcd3.Event)
	mc.metaEtcdCacheMap = emptyCache
	log.GetLogger().Infof("success to clear all store local meta info")
	mc.mux.Unlock()
}

// DescribeOwnerReferences describe owner reference
func DescribeOwnerReferences() metav1.OwnerReference {
	t := true
	return metav1.OwnerReference{
		APIVersion:         "apps/v1",
		BlockOwnerDeletion: &t,
		Controller:         &t,
		Kind:               "Deployment",
		Name:               WorkerManagerDeployName,
		UID:                GetWorkerManagerDeployUID(),
	}
}

func buildPatchPodTemplate(ownerReference metav1.OwnerReference, requestInfo *config.RequestInfo) *corev1.Pod {
	patchPodTemplate := &corev1.Pod{}
	if utils.PodNameSpace() != constants.TrustworthinessNameSpace {
		patchPodTemplate.OwnerReferences = append(patchPodTemplate.OwnerReferences, ownerReference)
	}

	funcNamePrefix, serviceID, shortFuncName := SplitFuncName(requestInfo.FunctionName)
	patchPodTemplate.Labels = make(map[string]string)
	patchPodTemplate.Labels[LabelBusinessID] = requestInfo.BusinessID
	patchPodTemplate.Labels[LabelTenantID] = requestInfo.TenantID
	patchPodTemplate.Labels[LabelFuncNamePrefix] = funcNamePrefix
	patchPodTemplate.Labels[LabelServiceID] = serviceID
	patchPodTemplate.Labels[LabelShortFuncName] = shortFuncName
	patchPodTemplate.Labels[LabelFunctionVersion] = requestInfo.FunctionVersion

	return patchPodTemplate
}

// ChangeOldWorkerOwner change old worker owner reference
func ChangeOldWorkerOwner(pod *corev1.Pod, requestInfo *config.RequestInfo) error {
	for _, ref := range pod.GetOwnerReferences() {
		if ref.Name == WorkerManagerDeployName {
			// new worker no need to change owner reference
			return nil
		}
	}
	ownerReference := DescribeOwnerReferences()
	patchPodTemplate := buildPatchPodTemplate(ownerReference, requestInfo)
	patchLoad := map[string]interface{}{"metadata": patchPodTemplate.ObjectMeta}
	patchLoadBytes, err := json.Marshal(patchLoad)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal patch pod template data %s with error %s",
			patchLoadBytes, err.Error())
		return err
	}
	log.GetLogger().Debugf("set labels and ownerReference patchLoadBytes: %s instance %s", patchLoadBytes, pod.Name)
	ctx, cancel := context.WithTimeout(context.TODO(), K8sPatchTimeout)
	_, err = GetKubernetesClient().CoreV1().Pods(pod.Namespace).
		Patch(ctx, pod.Name, types.MergePatchType, patchLoadBytes, metav1.PatchOptions{})
	cancel()
	if err != nil {
		if k8serror.IsNotFound(err) {
			log.GetLogger().Warnf("set labels and ownerReference to instance %s, which is not found",
				pod.Name)
			return nil
		}
		log.GetLogger().Errorf("failed to set labels and ownerReference to instance %s with error %s",
			pod.Name, err.Error())
		return err
	}
	return nil
}

// DNSProxyMetadata dns proxy metadata
type DNSProxyMetadata struct {
	URL     string      `json:"url"`
	Method  string      `json:"method"`
	Header  http.Header `json:"header"`
	Payload []byte      `json:"payload"`
}

// DNSProxyRequestParam -
type DNSProxyRequestParam struct {
	DNSAddress  string
	TenantID    string
	TenantToken string
	VpcID       string
	XRole       string
	DomainID    string
	Info        commontypes.DNSDomainInfo
}

// OMSVCRequestParam -
type OMSVCRequestParam struct {
	version string
	address string
	ak      string
	sk      string
	param   DNSProxyRequestParam
}

func genDNSProxyMetadata(param DNSProxyRequestParam) DNSProxyMetadata {
	info := param.Info
	if len(info.Type) == 0 {
		info.Type = "A"
	}
	if len(info.ZoneType) == 0 {
		info.ZoneType = "private"
	}
	decryptedTokenBytes, _ := localauth.Decrypt(param.TenantToken)
	header := make(http.Header)
	header.Set("Content-Type", "application/json")
	header.Set(HeaderAuthToken, string(decryptedTokenBytes))

	return DNSProxyMetadata{
		URL: fmt.Sprintf("%s/v2/recordsets?zone_type=%s&type=%s&name=%s&vpc_id=%s",
			dnsAddress, info.ZoneType, info.Type, info.DomainName, param.VpcID),
		Method:  http.MethodGet,
		Header:  header,
		Payload: nil,
	}
}

// GetIPsByDNSByAddresses get ip list by dns
func GetIPsByDNSByAddresses(omsvcAddress, ak, sk string, param DNSProxyRequestParam) (map[string][]string, error) {
	omsvcAddresses := utils.ShuffleOneArray(strings.Split(omsvcAddress, ","))
	var err error
	var result map[string][]string
	for _, address := range omsvcAddresses {
		result, err = GetIPsByDNS(address, ak, sk, param)
		if err != nil {
			log.GetLogger().Warnf("failed to GetIPsByDNS from omsvc address: %s, err: %s ", address, err.Error())
			// Failed to retry the next URL.
			continue
		}
		return result, err
	}
	return result, err
}

// GetIPsByDNS get ip list by dns
func GetIPsByDNS(omsvcAddress, ak, sk string, param DNSProxyRequestParam) (map[string][]string, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), UpdateDomainNamesTimeout)
	requestParam := OMSVCRequestParam{
		address: omsvcAddress,
		version: dnsVersion,
		ak:      ak,
		sk:      sk,
		param:   param,
	}
	req, err := genDNSRequest(ctx, requestParam)
	if err != nil {
		log.GetLogger().Errorf("failed to gen dns request: %s", err.Error())
		cancelFunc()
		return nil, err
	}
	return requestOMSVC(req, requestParam, cancelFunc)
}

func requestOMSVC(req *http.Request, requestParam OMSVCRequestParam,
	cancelFunc context.CancelFunc) (map[string][]string, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.GetLogger().Errorf("failed to send dns request with requestURI: %s and error: %s",
			req.URL.String(), err.Error())
		cancelFunc()
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read body, requestURI: %s, statusCode: %s",
			req.URL.String(), resp.StatusCode)
		cancelFunc()
		return nil, err
	}
	cancelFunc()
	domainKey := requestParam.param.VpcID + SplicingCharacter + requestParam.param.Info.DomainName
	if resp.StatusCode != http.StatusOK {
		return processDNSExceptionRequest(resp, body, domainKey, requestParam)
	}
	return parseDomainNameResp(body, domainKey)
}

func parseDomainNameResp(body []byte, domainKey string) (map[string][]string, error) {
	var domainNameResp commontypes.DomainNameResp
	if err := json.Unmarshal(body, &domainNameResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal body with error: %s", err.Error())
		return nil, err
	}
	if len(domainNameResp.Recordsets) == 0 {
		log.GetLogger().Errorf("record sets is nil with domainKey: %s", domainKey)
		dnsCache.addDomainNotExist(domainKey)
		return nil, fmt.Errorf("record sets is nil")
	}

	if dnsCache.isDomainNotExist(domainKey) {
		dnsCache.removeDomainNotExist(domainKey)
	}
	result := make(map[string][]string)
	for _, record := range domainNameResp.Recordsets {
		if len(record.Records) > 0 {
			if record.Name[len(record.Name)-1] == '.' {
				record.Name = record.Name[0 : len(record.Name)-1]
			}
			result[record.Name] = record.Records[:1]
		}
	}
	return result, nil
}

func retryGetIPsByDNSByV2Req(requestParam OMSVCRequestParam, domainKey string) (map[string][]string, error) {
	log.GetLogger().Warnf("failed to get domain info by v3 request "+
		"and will retry by v2 request, domainKey: %s", domainKey)
	requestParam.version = v2Version
	ctx, cancelFunc := context.WithTimeout(context.Background(), UpdateDomainNamesTimeout)
	req, err := genDNSRequest(ctx, requestParam)
	if err != nil {
		log.GetLogger().Errorf("failed to gen dns request: %s", err.Error())
		cancelFunc()
		return nil, err
	}
	return requestOMSVC(req, requestParam, cancelFunc)
}

func genDNSRequest(ctx context.Context, requestParam OMSVCRequestParam) (*http.Request, error) {
	var requestURI string
	var reqBody []byte
	var body io.Reader
	var err error
	scheme := "https"
	var method string
	if requestParam.version == v3Version {
		requestURI = fmt.Sprintf("%s://%s%s?domain_id=%s&project_id=%s&xrole=%s&vpc_id=%s&domain=%s",
			scheme, requestParam.address, urlV3, requestParam.param.DomainID, requestParam.param.TenantID,
			requestParam.param.XRole, requestParam.param.VpcID, requestParam.param.Info.DomainName)
		method = http.MethodGet
	} else {
		metadata := genDNSProxyMetadata(requestParam.param)
		reqBody, err = json.Marshal(metadata)
		if err != nil {
			log.GetLogger().Errorf("failed to marshal dns proxy metadata with error: %s", err.Error())
			return nil, err
		}
		body = bytes.NewBuffer(reqBody)
		requestURI = fmt.Sprintf("%s://%s%s", scheme, requestParam.address, urlV2)
		method = http.MethodPost
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURI, body)
	if err != nil {
		log.GetLogger().Errorf("failed to gen dns request url: %s, error: %s", requestURI, err.Error())
		return nil, err
	}
	authorization, timestamp := localauth.SignOMSVC(requestParam.ak, requestParam.sk, req.URL.String(), reqBody)
	req.Header.Set(HeaderAlarmAuthorization, authorization)
	req.Header.Set(HeaderAlarmAuthTimestamp, timestamp)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	return req, nil
}

func processDNSExceptionRequest(resp *http.Response, body []byte, domainKey string,
	requestParam OMSVCRequestParam) (map[string][]string, error) {
	code := resp.StatusCode
	var url string
	if resp.Request != nil {
		url = resp.Request.URL.String()
	}
	log.GetLogger().Warnf("failed to get domain info with requestURL %s,"+
		" server returns statusCode: %d, body: %s", url, resp.StatusCode, string(body))
	if code == http.StatusNotFound {
		if requestParam.version == v3Version {
			var domainNameResp commontypes.DomainNameResp
			if err := json.Unmarshal(body, &domainNameResp); err != nil {
				return retryGetIPsByDNSByV2Req(requestParam, domainKey)
			}
			if domainNameResp.ErrorCode != notExistCode {
				return retryGetIPsByDNSByV2Req(requestParam, domainKey)
			}
		}
		dnsCache.addDomainNotExist(domainKey)
	} else if code == http.StatusInternalServerError && requestParam.version == v3Version {
		return retryGetIPsByDNSByV2Req(requestParam, domainKey)
	}
	return nil, fmt.Errorf("unexpected statusCode: %d", resp.StatusCode)
}

// addDomainNotExist add dns to dnsNotExistList
func (cache *DNSCache) addDomainNotExist(domainKey string) {
	cache.mux.Lock()
	if !containsKey(cache.domainNotExistList, domainKey) {
		cache.domainNotExistList = append(cache.domainNotExistList, domainKey)
	}
	cache.mux.Unlock()
}

// removeDomainNotExist remove dns from dnsNotExistList
func (cache *DNSCache) removeDomainNotExist(domainKey string) {
	cache.mux.Lock()
	cache.domainNotExistList = removeKey(cache.domainNotExistList, domainKey)
	cache.mux.Unlock()
}

// isDomainNotExist judge if DNS not exist
func (cache *DNSCache) isDomainNotExist(domainKey string) bool {
	return containsKey(cache.domainNotExistList, domainKey)
}

// GetDefaultTenantMetaInfoFromEtcd get default tenant meta info from etcd
func GetDefaultTenantMetaInfoFromEtcd(defaultTenantMetaKey string) (*config.TenantMetaInfo, error) {
	values, _, err := getValuesFromETCD(defaultTenantMetaKey, valueTypeTenant)
	if err != nil {
		return nil, err
	}
	if len(values) < 1 {
		return nil, fmt.Errorf("failed to get value from etcd, key: %s", defaultTenantMetaKey)
	}

	var tenantMetaInfo = config.TenantMetaInfo{}
	if err = json.Unmarshal([]byte(values[0]), &tenantMetaInfo); err != nil {
		return nil, err
	}
	if _, err = govalidator.ValidateStruct(tenantMetaInfo); err != nil {
		return nil, err
	}

	return &tenantMetaInfo, nil
}

// IsCustomContainerPod check if it is custom container pod by name
func IsCustomContainerPod(podName string) bool {
	return strings.Contains(podName, "custom-") || IsGPUCustomContainerPod(podName) ||
		IsCheckpointCustomContainerPod(podName)
}

// IsCheckpointCustomContainerPod check if it is custom container checkpoint pod by name
func IsCheckpointCustomContainerPod(podName string) bool {
	return strings.Contains(podName, "checkpoint.custom-")
}

// IsGPUCustomContainerPod check if it is custom container gpu pod by name
func IsGPUCustomContainerPod(podName string) bool {
	return strings.Contains(podName, "custom.image.gpu-")
}

// IsCustomRuntimeGPU check if it is custom runtime gpu
func IsCustomRuntimeGPU(podName string) bool {
	return strings.HasPrefix(podName, "custom.runtime.gpu")
}

// IsTmpExpansionPod check if it is tmp expansion pod by name
func IsTmpExpansionPod(podName string) bool {
	return strings.Contains(podName, "tmpexpansion-")
}

// IsOnDemandPod check if pod is created on-demand
func IsOnDemandPod(pod *corev1.Pod) bool {
	return IsCustomContainerPod(pod.Name) || IsCheckpointPod(pod) || IsNodeBalanceMigratedPod(pod) ||
		IsTmpExpansionPod(pod.Name) || IsCustomRuntimeGPU(pod.Name)
}

// GetDeployByK8S get deployment by k8s
func GetDeployByK8S(k8sClient kubernetes.Interface, deployName string) (*v1.Deployment, error) {
	if k8sClient == nil {
		return nil, ErrK8SClientNil
	}
	return k8sClient.AppsV1().Deployments(DefaultNamespace).Get(context.TODO(), deployName, metav1.GetOptions{})
}

// CreateDeployByK8S create deployment by k8s
func CreateDeployByK8S(k8sClient kubernetes.Interface, deploy *v1.Deployment) error {
	if k8sClient == nil {
		return ErrK8SClientNil
	}
	_, err := k8sClient.AppsV1().Deployments(DefaultNamespace).Create(context.TODO(), deploy, metav1.CreateOptions{})
	if err != nil && !k8serror.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// UpdateDeployByK8S update deployment by k8s
func UpdateDeployByK8S(k8sClient kubernetes.Interface, deploy *v1.Deployment) error {
	if k8sClient == nil {
		return ErrK8SClientNil
	}
	_, err := k8sClient.AppsV1().Deployments(DefaultNamespace).Update(context.TODO(), deploy, metav1.UpdateOptions{})
	if err != nil && !k8serror.IsAlreadyExists(err) {
		return err
	}
	return nil
}

// UpdateReplicasByK8s update replicas
func UpdateReplicasByK8s(deployName string, replicas int32) error {
	start := time.Now()
	patchBytes := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	ctx, cancel := context.WithTimeout(context.TODO(), K8sPatchTimeout)
	_, err := GetKubernetesClient().AppsV1().Deployments(DefaultNamespace).
		Patch(ctx, deployName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to update the deployment %s replicas as %d, error %s",
			deployName, replicas, err.Error())
		return err
	}
	log.GetLogger().Infof("the deployment %s replicas updated as %d, cost time %s",
		deployName, replicas, time.Now().Sub(start))
	return nil
}

// ListDeployByK8S list deployment by k8s
func ListDeployByK8S(k8sClient kubernetes.Interface, options metav1.ListOptions) (*v1.DeploymentList, error) {
	if k8sClient == nil {
		return nil, ErrK8SClientNil
	}
	return k8sClient.AppsV1().Deployments(DefaultNamespace).List(context.TODO(), options)
}

// DeleteDeployByK8S delete deployment by k8s
func DeleteDeployByK8S(k8sClient kubernetes.Interface, name string) error {
	if k8sClient == nil {
		return ErrK8SClientNil
	}
	return k8sClient.AppsV1().Deployments(DefaultNamespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// IsPullTriggerFunction check if it it pull trigger function
func IsPullTriggerFunction(funcMeta *faasfunction.FunctionService) bool {
	return funcMeta != nil && funcMeta.Spec.Configuration.VPCTriggerImage != ""
}

// UpdateAllPrewarmPod If smartwarmer becomes unavailable, delete all pods
func UpdateAllPrewarmPod() {
	if smartWarmerEnable {
		log.GetLogger().Infof("prewarm pod is enable, no need to update")
		return
	}
	podList, err := GetKubernetesClient().CoreV1().Pods(FunctionNameSpace()).List(
		context.TODO(), metav1.ListOptions{LabelSelector: "applier=prediction"})
	if err != nil {
		log.GetLogger().Errorf("failed to list all prediction pod, error: %s", err.Error())
		return
	}
	for _, pod := range podList.Items {
		DeletePod(&pod)
	}
}

// DescribeGPUNodeAffinity -
func DescribeGPUNodeAffinity(key string, values []string, gpuType string) *corev1.NodeAffinity {
	if gpuType == DefaultGpuType || gpuType == "" {
		return DescribeNodeAffinity(key, values)
	}
	return DescribeNodeAffinityWithGpuType(key, values, gpuType)
}

// DescribeNodeAffinity -
func DescribeNodeAffinity(key string, values []string) *corev1.NodeAffinity {
	return &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      key,
					Operator: corev1.NodeSelectorOpIn,
					Values:   values,
				}},
			},
			},
		},
	}
}

// DescribeNodeAffinityWithGpuType -
func DescribeNodeAffinityWithGpuType(key string, values []string, gpuType string) *corev1.NodeAffinity {
	var gpuTypeValue []string
	gpuTypeValue = append(gpuTypeValue, gpuType)
	return &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key:      key,
					Operator: corev1.NodeSelectorOpIn,
					Values:   values,
				}, {
					Key:      GpuTypeAffinityKey,
					Operator: corev1.NodeSelectorOpIn,
					Values:   gpuTypeValue,
				}},
			},
			},
		},
	}
}

// GetFilterByFuncProfile generate filter labels
func GetFilterByFuncProfile(profile commontypes.ProfileMetaData) map[string]string {
	labels := make(map[string]string, DefaultMapSize)
	if !config.IsHeterogeneousScheduling() {
		return labels
	}
	// NOTE: only cpu-bound function will be scheduled to high-end pool, regardless of other profile dimensions
	// the strategy is constantly optimized
	if config.IsForceStrategy() || !profile.IsInit || profile.ResourceModel.IsCPUBound {
		labels[LabelNodeType] = HighNodeType
	} else {
		labels[LabelNodeType] = LowNodeType
	}
	return labels
}

// IsNeedRetryScheduling judge if it is necessary to get pod in another pool when failed to get pod at first time
func IsNeedRetryScheduling(err error, labels map[string]string) bool {
	if err == nil || !config.IsHeterogeneousScheduling() {
		return false
	}
	// highend can not retreat to lowend
	if labels[LabelNodeType] == HighNodeType {
		return false
	}
	return true
}

// HasMigratingTaints check if it has migrating taints
func HasMigratingTaints(taints []corev1.Taint) bool {
	for _, taint := range taints {
		// migrateTaintKey is written by CA plugin
		// the TaintNodeUnschedulable is written by hand manually
		if taint.Key == MigrateTaintKey || taint.Key == corev1.TaintNodeUnschedulable {
			return true
		}
	}
	return false
}

// HasDsWorkerCrashTaints check if it has migrating taints
func HasDsWorkerCrashTaints(taints []corev1.Taint) bool {
	for _, taint := range taints {
		// dsWorkerTaintKey is written by ds-worker Pod condition
		// id Ready: false, set taint
		if taint.Key == DsWorkerTaintKey || taint.Key == corev1.TaintNodeUnschedulable {
			return true
		}
	}
	return false
}

// CopyTaints deep copy taints
func CopyTaints(taints []corev1.Taint) []corev1.Taint {
	var copyTaints []corev1.Taint
	for _, taint := range taints {
		copyTaints = append(copyTaints, taint)
	}
	return copyTaints
}

// GetDNSCache get DNSCache
func GetDNSCache() *DNSCache {
	return dnsCache
}

// GetDNSDomainNames get dns domain names by funcSvcID
func (cache *DNSCache) GetDNSDomainNames(funcSvcID string) (map[string][]string, error) {
	if funcSvcID == "" {
		return nil, ErrDomainInfoInvalid
	}
	val, ok := cache.dnsCacheMap.Load(DNSCacheSvcKeyPrefix + funcSvcID)
	if !ok {
		return nil, ErrDomainInfoInvalid
	}
	domainKeys, ok := val.([]string)
	if !ok || len(domainKeys) <= 0 {
		log.GetLogger().Errorf("failed to convert dnsCache value, "+
			"key: %s, value: %v", DNSCacheSvcKeyPrefix+funcSvcID, val)
		return nil, ErrDomainInfoInvalid
	}
	dnsDomainNames := make(map[string][]string, 0)
	for _, domainKey := range domainKeys {
		val, ok := cache.dnsCacheMap.Load(domainKey)
		if !ok {
			log.GetLogger().Errorf("failed to get domain info with domainKey %s", domainKey)
			return nil, ErrDomainInfoInvalid
		}
		domainMap, ok := val.(map[string][]string)
		if !ok {
			log.GetLogger().Errorf("failed to convert dnsCache value, "+
				"key: %s, value: %v", DNSCacheSvcKeyPrefix+funcSvcID, val)
			return nil, ErrDomainInfoInvalid
		}
		if len(domainMap) == 0 && !cache.isDomainNotExist(domainKey) {
			log.GetLogger().Errorf("failed to get domain info with domainKey %s", domainKey)
			return nil, ErrDomainInfoInvalid
		}
		for dnsName, ipList := range domainMap {
			dnsDomainNames[dnsName] = ipList
		}
	}
	return dnsDomainNames, nil
}

// UpdateDNSDomainNames update dns domain names
func (cache *DNSCache) UpdateDNSDomainNames(vpcID string, domainName string, dnsDomainMap map[string][]string) {
	if vpcID == "" || domainName == "" || len(dnsDomainMap) == 0 {
		return
	}
	domainKey := vpcID + SplicingCharacter + domainName
	cache.dnsCacheMap.Store(domainKey, dnsDomainMap)
}

// GetDNSDomainKeyMap  update dns domain names for all functions
func (cache *DNSCache) GetDNSDomainKeyMap() map[string]string {
	domainKeyMap := make(map[string]string, 0)
	cache.dnsCacheMap.Range(func(key, value interface{}) bool {
		domainKey, ok := key.(string)
		if !ok {
			log.GetLogger().Warnf("failed to covert interface to string for dnsCacheMap: %v", key)
			return true
		}
		if strings.HasPrefix(domainKey, DNSCacheSvcKeyPrefix) || strings.HasPrefix(domainKey, DNSCacheFuncKeyPrefix) {
			return true
		}
		funcSvcIDList, ok := cache.dnsCacheMap.Load(DNSCacheFuncKeyPrefix + domainKey)
		if !ok {
			log.GetLogger().Warnf("failed to find funcSvcID for domainKey: %v", domainKey)
			return true
		}
		funcSvcIDs, ok := funcSvcIDList.([]string)
		if !ok || len(funcSvcIDs) <= 0 {
			log.GetLogger().Warnf("failed to covert funcSvcIDList with value: %v", funcSvcIDList)
			return true
		}
		var funcSvcID string
		for _, val := range funcSvcIDs {
			if _, ok := cache.dnsCacheMap.Load(DNSCacheSvcKeyPrefix + val); ok {
				funcSvcID = val
				break
			}
		}
		if funcSvcID == "" {
			log.GetLogger().Warnf("failed to find funcSvcID for domainKey: %v, skip", domainKey)
			return true
		}
		domainKeyMap[domainKey] = funcSvcID
		return true
	})
	return domainKeyMap
}

// StoreDNSCache store dns cache
func (cache *DNSCache) StoreDNSCache(funcSvcID string, vpcID string,
	domainName string, dnsDomainMap map[string][]string) {
	if funcSvcID == "" || vpcID == "" || domainName == "" || len(dnsDomainMap) == 0 {
		return
	}
	domainKey := vpcID + SplicingCharacter + domainName
	flag := cache.storeFuncReflect(funcSvcID, domainKey)
	if flag {
		cache.dnsCacheMap.Store(domainKey, dnsDomainMap)
	}
}

// ProcessNewDomainConfig process function add event with domain config
func (cache *DNSCache) ProcessNewDomainConfig(funcSvcID string, vpcID string, domainName string) bool {
	if vpcID == "" || domainName == "" {
		return false
	}
	domainKey := vpcID + SplicingCharacter + domainName
	if _, flag := cache.dnsCacheMap.Load(domainKey); flag {
		cache.storeFuncReflect(funcSvcID, domainKey)
		return false
	}
	return true
}

// ProcessUpdateDomainConfig process function update event
func (cache *DNSCache) ProcessUpdateDomainConfig(funcSvcID string, curDomainKeys ...string) ([]string, bool) {
	if len(curDomainKeys) == 0 {
		if val, flag := cache.dnsCacheMap.Load(DNSCacheSvcKeyPrefix + funcSvcID); flag {
			preDomainList, ok := val.([]string)
			if !ok {
				log.GetLogger().Errorf("failed to convert dnsCache value, "+
					"key: %s, value: %v", DNSCacheFuncKeyPrefix+funcSvcID, val)
				return nil, false
			}
			for _, preDomainKey := range preDomainList {
				cache.deleteFuncReflect(funcSvcID, preDomainKey)
			}
		}
		return nil, false
	}
	val, flag := cache.dnsCacheMap.LoadOrStore(DNSCacheSvcKeyPrefix+funcSvcID, curDomainKeys)
	if flag {
		preDomainKeys, ok := val.([]string)
		if !ok {
			log.GetLogger().Errorf("failed to convert dnsCache value, "+
				"key: %s, value: %v", DNSCacheSvcKeyPrefix+funcSvcID, val)
			return nil, false
		}
		for _, domainKey := range preDomainKeys {
			if containsKey(curDomainKeys, domainKey) {
				continue
			}
			cache.deleteFuncReflect(funcSvcID, domainKey)
		}
	}
	var result []string
	for _, curDomainKey := range curDomainKeys {
		cache.storeFuncReflect(funcSvcID, curDomainKey)
		_, ok := cache.dnsCacheMap.LoadOrStore(curDomainKey, make(map[string][]string))
		if ok {
			continue
		}
		result = append(result, curDomainKey)
	}
	return result, len(result) > 0
}

// ProcessDeleteDomainConfig process function delete event
func (cache *DNSCache) ProcessDeleteDomainConfig(funcSvcID string, vpcID string, domainName string) {
	if vpcID == "" || domainName == "" {
		return
	}
	domainKey := vpcID + SplicingCharacter + domainName
	cache.deleteFuncReflect(funcSvcID, domainKey)
}

// StoreFuncReflect store function reflect
func (cache *DNSCache) storeFuncReflect(funcSvcID string, domainKey string) bool {
	cache.mux.Lock()
	domainList, flag := cache.dnsCacheMap.LoadOrStore(DNSCacheSvcKeyPrefix+funcSvcID, []string{domainKey})
	if flag {
		domainKeyList, ok := domainList.([]string)
		if !ok {
			log.GetLogger().Errorf("failed to convert dnsCache value, "+
				"key: %s, value: %v", DNSCacheSvcKeyPrefix+funcSvcID, domainList)
			cache.mux.Unlock()
			return false
		}
		if !containsKey(domainKeyList, domainKey) {
			domainKeyList = append(domainKeyList, domainKey)
			cache.dnsCacheMap.Store(DNSCacheSvcKeyPrefix+funcSvcID, domainKeyList)
		}
	}
	val, flag := cache.dnsCacheMap.LoadOrStore(DNSCacheFuncKeyPrefix+domainKey, []string{funcSvcID})
	if flag {
		slice, ok := val.([]string)
		if !ok {
			log.GetLogger().Errorf("failed to convert dnsCache value, "+
				"key: %s, value: %v", DNSCacheFuncKeyPrefix+domainKey, val)
			cache.mux.Unlock()
			return false
		}
		if !containsKey(slice, funcSvcID) {
			slice = append(slice, funcSvcID)
			cache.dnsCacheMap.Store(DNSCacheFuncKeyPrefix+domainKey, slice)
		}
	}
	cache.mux.Unlock()
	return true
}

// DeleteFuncReflect delete function reflect
func (cache *DNSCache) deleteFuncReflect(funcSvcID string, domainKey string) {
	if domainKey == "" {
		return
	}
	cache.mux.Lock()
	cache.dnsCacheMap.Delete(DNSCacheSvcKeyPrefix + funcSvcID)
	if val, flag := cache.dnsCacheMap.Load(DNSCacheFuncKeyPrefix + domainKey); flag {
		slice, ok := val.([]string)
		if !ok {
			log.GetLogger().Errorf("failed to convert dnsCache value, "+
				"key: %s, value: %v", DNSCacheFuncKeyPrefix+domainKey, val)
			cache.mux.Unlock()
			return
		}
		slice = removeKey(slice, funcSvcID)
		if len(slice) == 0 {
			cache.dnsCacheMap.Delete(domainKey)
			cache.dnsCacheMap.Delete(DNSCacheFuncKeyPrefix + domainKey)
			cache.mux.Unlock()
			log.GetLogger().Infof("succeed to delete domainKey %s from dnsCacheMap", domainKey)
			return
		}
		cache.dnsCacheMap.Store(DNSCacheFuncKeyPrefix+domainKey, slice)
	}
	cache.mux.Unlock()
}

func containsKey(slice []string, element string) bool {
	for _, e := range slice {
		if e == element {
			return true
		}
	}
	return false
}

func removeKey(slice []string, element string) []string {
	var tmp []string
	for _, e := range slice {
		if e != element {
			tmp = append(tmp, e)
		}
	}
	return tmp
}

// SubmitResult -
func SubmitResult(etcdKey string, traceID string, retryTime int, result []byte) error {
	log.GetLogger().Infof("%s | submit etcd for: %s", traceID, etcdKey)
	var err error
	for i := 0; i < retryTime; i++ {
		if etcd3.GetMetaEtcdClient().Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client")
			continue
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		err = etcd3.GetMetaEtcdClient().Put(defaultEtcdCtx, etcdKey, string(result))
		if err == nil {
			break
		}
	}
	return err
}

// ConstructIntersect compute the intersection of two lists of sorted strings
func ConstructIntersect(a, b []string) []string {
	result := []string{}
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

var enableGPUIsolation = ParseGPUIsolation()

// ParseGPUIsolation parse whether GPU isolation is enabled; default value is true
func ParseGPUIsolation() bool {
	val := os.Getenv("ENABLE_GPU_ISOLATION")
	if val == "" {
		return true
	}
	enable, err := strconv.ParseBool(val)
	if err != nil {
		return true
	}
	return enable
}

// ConfigGPUResource config resource of GPU; request MB, capacity GB
func ConfigGPUResource(res corev1.ResourceRequirements, request, capacity int64) {
	if enableGPUIsolation {
		configGPUIsolation(res, request, capacity)
		return
	}
	// round down based on 1000 to avoid waste of gpu memory caused by rounding up
	gpuNum := math.Floor(float64(request)*GpuBase/MbToGb/float64(capacity)) / GpuBase
	res.Requests[NvidiaGPU] = resource.MustParse(fmt.Sprintf("%f", gpuNum))
	res.Limits[NvidiaGPU] = resource.MustParse(fmt.Sprintf("%f", gpuNum))
}

// configGPUIsolation config gpu; request MB, capacity GB
func configGPUIsolation(res corev1.ResourceRequirements, request, capacity int64) {
	if capacity == 0 {
		return
	}
	if (request/MbToGb)%capacity == 0 {
		res.Limits[NvidiaGPU] = resource.MustParse(fmt.Sprintf("%d", request/MbToGb/capacity))
		return
	}
	mem := request / GpuIsolationMemStep
	if mem <= 0 {
		mem = 1
	}
	res.Limits[GpuIsolationMemKey] = resource.MustParse(fmt.Sprintf("%d", mem))

	percent := request * basePercent / capacity / MbToGb
	if percent%GpuIsolationCoreStep != 0 {
		percent = percent / GpuIsolationCoreStep * GpuIsolationCoreStep
	}
	if percent <= 0 {
		percent = GpuIsolationCoreStep
	}
	res.Limits[GpuIsolationCoreKey] = resource.MustParse(fmt.Sprintf("%d", percent))
}

// IsCheckpointPod is checkpoint pod
func IsCheckpointPod(pod *corev1.Pod) bool {
	standard, ok := pod.Labels[StandardLabelKey]
	if !ok {
		log.GetLogger().Warnf("cannot find label %s for pod %s", StandardLabelKey, pod.Name)
		return false
	}
	return standard == CheckpointPodStandard
}

// IsNodeBalanceMigratedPod is node balance migrated pod
func IsNodeBalanceMigratedPod(pod *corev1.Pod) bool {
	return pod.Labels[LabelMigratedFrom] != ""
}

// GenerateMigratePodName generate new pod name for migrate pod
func GenerateMigratePodName(name string) string {
	splits := strings.Split(name, "-")
	if len(splits) < minPodSplitLen {
		log.GetLogger().Warnf("invalid pod name %s", name)
		return name
	}
	suffix := "-migrate-" + utils.ShortUUID()
	// assert it's xpu pod (e.g. custom-300-128-[1024]-xxx)
	if _, err := strconv.Atoi(splits[xpuResourceIndex]); err == nil {
		return strings.Join(splits[:xpuResourceIndex+1], "-") + suffix
	}
	// normal cpu pod
	return strings.Join(splits[:xpuResourceIndex], "-") + suffix
}

// GeneratePodTemplate generate template for creating migrate pod
func GeneratePodTemplate(pod *corev1.Pod) *corev1.Pod {
	template := &corev1.Pod{}

	template.Spec = *pod.Spec.DeepCopy()
	template.ObjectMeta = metav1.ObjectMeta{
		Namespace: pod.Namespace,
		Labels: map[string]string{
			IsPoolPodLabelKey: TrueString,
			StandardLabelKey:  pod.Labels[StandardLabelKey],
		},
		Annotations: map[string]string{
			"used":                FalseString,
			constants.OndemandKey: TrueString,
		},
	}
	// must clear nodeName, otherwise kube-scheduler wouldn't take effect
	template.Spec.NodeName = ""
	template.Spec.RestartPolicy = corev1.RestartPolicyNever

	// clear old secrets
	k := 0
	for i, s := range template.Spec.ImagePullSecrets {
		if !strings.HasSuffix(s.Name, ".regcred") {
			if i != k {
				template.Spec.ImagePullSecrets[k] = template.Spec.ImagePullSecrets[i]
			}
			k++
		}
	}
	template.Spec.ImagePullSecrets = template.Spec.ImagePullSecrets[:k]

	return template
}

// ConfigDemandNodes sets demand nodes for pod;
func ConfigDemandNodes(pod *corev1.Pod, nodes []string) {
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for i := range terms {
		// first remove all old demand nodes
		k := 0
		for j := range terms[i].MatchExpressions {
			if terms[i].MatchExpressions[j].Key != corev1.LabelHostname {
				if j != k {
					terms[i].MatchExpressions[k] = terms[i].MatchExpressions[j]
				}
				k++
			}
		}
		terms[i].MatchExpressions = terms[i].MatchExpressions[:k]
		// NodeSelectorTerms are ORed, so we should append on-demand node expression for each term
		terms[i].MatchExpressions = append(terms[i].MatchExpressions, corev1.NodeSelectorRequirement{
			Key:      corev1.LabelHostname,
			Operator: corev1.NodeSelectorOpIn,
			Values:   nodes,
		})
	}
	// add demand nodes if no term configured
	if len(terms) == 0 {
		terms = append(terms, corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{{
				Key:      corev1.LabelHostname,
				Operator: corev1.NodeSelectorOpIn,
				Values:   nodes,
			}}})
	}
	pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = terms
}

// ParseDemandNodes parse demandNodes from pod spec
func ParseDemandNodes(pod *corev1.Pod) []string {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil {
		return nil
	}
	if pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	for i := range terms {
		for j := range terms[i].MatchExpressions {
			if terms[i].MatchExpressions[j].Key == corev1.LabelHostname {
				return terms[i].MatchExpressions[j].Values
			}
		}
	}
	return nil
}

// IsCurClusterRouteInfo judge is current cluster route info or not
func IsCurClusterRouteInfo(routeInfo []byte) bool {
	workerIns := &v1alpha1.WorkerInstance{}
	err := json.Unmarshal(routeInfo, workerIns)
	if err != nil {
		return false
	}
	return clusterID == workerIns.Cluster
}

// DeleteStateWorkersFromMetaETCD -
func DeleteStateWorkersFromMetaETCD(stateID string) {
	var err error
	path := StateWorkerKey + VersionLatest + "/" + utils.AzEnv() + "/" + stateID
	for i := 0; i < MaxEtcdRetryAttempts; i++ {
		if etcd3.GetETCDClient(etcd3.Meta) == nil || etcd3.GetETCDClient(etcd3.Meta).Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client")
			MetaCacheOnce.Do(func() {
				LocalMetaEtcdCache.InitMetaEtcdCache()
			})
			time.Sleep(time.Second)
			continue
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		err = etcd3.GetETCDClient(etcd3.Meta).Delete(defaultEtcdCtx, path)
		if err == nil {
			log.GetLogger().Infof("success to delete stateID: %s, key: %s from meta etcd",
				stateID, etcdkey.AnonymizeTenantCommonEtcdKey(path))
			break
		}
		time.Sleep(RetryInterval)
		log.GetLogger().Warnf("failed to delete stateID: %s, key: %s from meta etcd with error %s",
			stateID, etcdkey.AnonymizeTenantCommonEtcdKey(path), err.Error())
		continue
	}
}

// GetEnvDsWorkerCrashTimeout -
func GetEnvDsWorkerCrashTimeout() int64 {
	GetDsWorkerTimeout.Do(func() {
		t := os.Getenv(DsWorkerTimeoutEnvKey)
		if t == "" {
			dsWorkerTimeout = DefaultDsWorkerTimeSleep
			return
		}
		timeout, err := strconv.ParseInt(t, 10, 64)
		if err != nil {
			log.GetLogger().Errorf("convert env param: %s to int failed, err: %s",
				DsWorkerTimeoutEnvKey, err.Error())
			dsWorkerTimeout = DefaultDsWorkerTimeSleep
			return
		}
		dsWorkerTimeout = timeout
	})
	return dsWorkerTimeout
}

// IsDsWorkerTaint -
func IsDsWorkerTaint(t corev1.Taint) bool {
	return t.Key == DsWorkerTaintKey && t.Value == DsWorkerTaintValue && t.Effect == corev1.TaintEffectPreferNoSchedule
}

// GetPodErrorMsg -
func GetPodErrorMsg(err error) string {
	if err == nil {
		return ""
	}
	var errMsg string
	if snErr, ok := err.(snerror.SNError); ok {
		snErrPrefix := snerror.ErrText(snErr.Code()) + ", error: "
		errMsg = strings.TrimPrefix(snErr.Error(), snErrPrefix)
	} else {
		errMsg = err.Error()
	}
	return errMsg
}

// GetEnvClusterID -
func GetEnvClusterID() string {
	return clusterID
}

// IsBridgeInstanceByPodLabel -
func IsBridgeInstanceByPodLabel(label map[string]string) bool {
	if label == nil {
		return false
	}
	return label[LabelIsBridgeFunction] == TrueString
}

// EnableNewCaeBaseImage enable new cae base image
func EnableNewCaeBaseImage() bool {
	return enableNewCaeBaseImage
}

// IsXGPUStartError check if is XGPU start error
func IsXGPUStartError(err error) bool {
	var e snerror.SNError
	if errors.As(err, &e) {
		if e.Code() == snerror.GettingPodErrorCode && strings.Contains(e.Error(), "xgpu") {
			return true
		}
	}
	return false
}

// GeneratePodErrorInfo -
func GeneratePodErrorInfo(properties map[string]string, podName string, err error) poderrormessage.PodErrorInfo {
	errMsg := GetPodErrorMsg(err)
	return poderrormessage.PodErrorInfo{
		PodName:         podName,
		ErrorMessage:    errMsg,
		EnvName:         properties[constants.CAEPropertiesEnvName],
		ComponentName:   properties[constants.CAEPropertiesComponentName],
		ApplicationName: properties[constants.CAEPropertiesAppName],
		EnvID:           properties[constants.CAEPropertiesEnvID],
		ComponentID:     properties[constants.CAEPropertiesComponentID],
		ApplicationID:   properties[constants.CAEPropertiesAppID],
		UploadTime:      time.Now().Unix(),
	}
}

// IsInSlice -
func IsInSlice(s string, slice []string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

// GetFunctionPodsByInformer get all function pods query by pod informer
func GetFunctionPodsByInformer(funcMeta *faasfunction.FunctionService) []*corev1.Pod {
	informer := GetPodInformer()
	if informer == nil {
		return nil
	}
	funcNamePrefix, serviceID, shortFuncName := SplitFuncName(funcMeta.Spec.Configuration.FunctionName)
	if len(shortFuncName) > MaxLabelValueLength {
		shortFuncName = "X" + shortFuncName[len(shortFuncName)-MaxLabelValueLength+1:]
	}
	funcLabels := map[string]string{
		LabelTenantID:        funcMeta.Spec.Configuration.TenantID,
		LabelServiceID:       serviceID,
		LabelFuncNamePrefix:  funcNamePrefix,
		LabelShortFuncName:   shortFuncName,
		LabelFunctionVersion: funcMeta.Spec.Configuration.FunctionVersion,
	}
	s := labels.NewSelector()
	for k, v := range funcLabels {
		req, err := labels.NewRequirement(k, selection.Equals, []string{v})
		if err != nil {
			continue
		}
		s = s.Add(*req)
	}
	pods, err := informer.Lister().List(s)
	if err != nil {
		log.GetLogger().Warnf("failed to list all pod from cache for %s %s",
			funcMeta.Spec.Configuration.FunctionName, err.Error())
		return nil
	}
	return pods
}

const (
	defaultSWRTimeout = 10 * time.Second
)

// SWRErrRespHandlers handles swr err response
type SWRErrRespHandlers map[int]func(data []byte) error

var (
	swrClient = &http.Client{
		Timeout: defaultSWRTimeout,
	}
)

// DefaultSWRErrRespHandlers default swr err handler
var DefaultSWRErrRespHandlers = SWRErrRespHandlers{
	http.StatusUnauthorized: func(_ []byte) error {
		return snerror.NewSNError(snerror.FunctionExceptionCode,
			"agency does not have the permission to interact with swr service")
	},
}

// CreateSecretDataWithUserAuth create secret by
func CreateSecretDataWithUserAuth(userEnc, pwdEnc, image string) ([]byte, error) {
	splits := strings.Split(image, "/")
	if len(splits) < imageURNMinSplitLen {
		return nil, errors.New("invalid image urn: " + image)
	}
	user, err := localauth.Decrypt(userEnc)
	if err != nil {
		return nil, errors.New("failed to decrypt custom user, " + err.Error())
	}
	defer utils.ClearByteMemory(user)
	pwd, err := localauth.Decrypt(pwdEnc)
	if err != nil {
		return nil, errors.New("failed to decrypt custom pwd, " + err.Error())
	}
	defer utils.ClearByteMemory(pwd)
	rawData := append(user, ':')
	rawData = append(rawData, pwd...)
	defer utils.ClearByteMemory(rawData)

	dst := make([]byte, base64.StdEncoding.EncodedLen(len(rawData)))
	base64.StdEncoding.Encode(dst, rawData)

	return []byte(fmt.Sprintf(dockerAuthConfigJSONFormat, splits[0], dst)), nil
}

// RequestSWR request swr-api and return data
func RequestSWR(ctx context.Context, path, method string, agency UserAgency,
	extra SWRErrRespHandlers) ([]byte, error) {
	rawDecryptedToken, err := localauth.Decrypt(agency.Token)
	if err != nil {
		return nil, errors.New("failed to decrypt agency token, " + err.Error())
	}
	decryptedToken := string(rawDecryptedToken)
	utils.ClearByteMemory(rawDecryptedToken)

	swrURL := fmt.Sprintf("https://%s/%s", config.GetSWRAPI(), path)
	req, err := http.NewRequestWithContext(ctx, method, swrURL, nil)
	if err != nil {
		utils.ClearStringMemory(decryptedToken)
		return nil, errors.New("failed to new request to swr, " + err.Error())
	}
	req.Header.Set(constants.HeaderAuthToken, decryptedToken)
	resp, err := swrClient.Do(req)
	utils.ClearStringMemory(decryptedToken)
	if err != nil {
		return nil, fmt.Errorf("failed to request swr with url %s %s", swrURL, err.Error())
	}
	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read swr response body with status %d, %s", resp.StatusCode, err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return nil, handleErrorResponse(resp.StatusCode, data, extra)
	}
	return data, nil
}

func handleErrorResponse(statusCode int, data []byte, extra SWRErrRespHandlers) error {
	if extra != nil {
		if handler, ok := extra[statusCode]; ok {
			return handler(data)
		}
	}

	if handler, ok := DefaultSWRErrRespHandlers[statusCode]; ok {
		return handler(data)
	}

	// Unhandled error
	return fmt.Errorf("swr request error: %s", string(data))
}

// GetUserCipherCache -
func GetUserCipherCache(key string) (*UserCipherCache, bool) {
	cache, ok := userCipherMap.Load(key)
	if !ok {
		return nil, false
	}
	cipherCache, ok := cache.(UserCipherCache)
	return &cipherCache, ok
}

// CheckAndStoreCipherCache -
func CheckAndStoreCipherCache(functionService *faasfunction.FunctionService, info CheckKmsCipherInfo) (*UserCipherCache, error) {
	agency := GetUserAgencyByFuncMeta(functionService)
	if agency.SecurityAk == "" || agency.SecuritySk == "" {
		return nil, errors.New("failed to get agency")
	}
	client, err := GetKmsClientByAgency(agency, info.TenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to get kms client, traceID: %s, tenant: %s, function: %s, err: %v",
			info.TraceID, info.TenantID, functionService.ID, err)
	}

	resp, err := client.DecryptDataKey(info.KmsKeyID, info.EncryptCipher)
	dataKey := ""
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt data key, traceID: %s, tenant: %s, function: %s, err: %v",
			info.TraceID, info.TenantID, functionService.ID, err)
		dataKey = constants.KmsAbnormalCipher
	} else {
		dataKey = *resp.DataKey
	}
	cache := &UserCipherCache{
		LastUseTime: time.Now(),
		Ciphertext:  info.EncryptCipher,
		Plaintext:   dataKey,
	}
	userCipherMap.Store(info.CipherKey, cache)
	return cache, nil
}
