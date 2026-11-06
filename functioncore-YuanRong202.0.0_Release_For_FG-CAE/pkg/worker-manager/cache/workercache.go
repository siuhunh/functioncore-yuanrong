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

// Package cache is the cache for worker
package cache

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	dockertypes "github.com/docker/docker/api/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/localauth"
	logconfig "functioncore/pkg/common/logger/config"
	"functioncore/pkg/common/logger/log"
	commonmetrics "functioncore/pkg/common/metrics"
	"functioncore/pkg/common/singleflight"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/timeutil"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/autoscaler"
	"functioncore/pkg/worker-manager/breaker"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

const (
	// initCacheSize is the init map size of worker cache
	initCacheSize         = 10
	reserveInstanceSymbol = "applier"
	// Notice: init container is separately stored in a Pod object
	instanceContainerLength = 2

	vpcIDIndex            = 0
	domainNameIndex       = 1
	domainKeyLength       = 2
	stateWorkersKeyLength = 13
	stateIDIndex          = 12
	base                  = 10
	bitSize               = 64
	dockerLoginCmdLength  = 2
	pullImageTimeout      = 10
	requestTimeout        = 5

	// example for funcKeyWithRes : 0a0****ggg/0@default@func/latest/300-128
	defaultFuncInfoLen = 4
	versionIndex       = 2
	tenantIDIndex      = 0
	funcNameIndex      = 1
	activeFuncTimeInt  = 60
)

var (
	// WorkerCache is cache for worker
	WorkerCache = &workerCache{}
	// SingleFlight is cache for etcd query
	SingleFlight = singleflight.NewSingleFlight()
	workerMap    = make(map[string]string, 2)
	initMap      = make(map[string]string, 1)
	clusterID    = os.Getenv("CLUSTER_ID")
	// StateWorkersList /sn/stateworkers existed stateID list, only for this cluster
	StateWorkersList sync.Map
	// HasSynced do restart callback only after cache has synced
	HasSynced        = false
	httpclient       = &http.Client{Timeout: requestTimeout * time.Second}
	activenessCaches = &ActivenessCache{
		InactiveFunctionList: make(map[string]string),
		FunctionListLock:     sync.RWMutex{},
	}
	// ImageNodeCache -
	ImageNodeCache sync.Map
)

// ActivenessCache -
type ActivenessCache struct {
	InactiveFunctionList map[string]string
	FunctionListLock     sync.RWMutex
}

type apigIPCache struct {
	ips []string
	sync.Mutex
}

// PublicNetworkConfig stores public network bandwidth
type PublicNetworkConfig struct {
	Bandwidth string `json:"bandwidth"`
}

type workerCache struct {
	// key: funcSvcID  value: *functionCache
	functions sync.Map
	apigIP    apigIPCache
	// key: projectID  value: *PublicNetworkConfig
	publicNetworkConfig sync.Map
}

// PodAction -
type PodAction struct {
	FuncSvc        string
	StatefulAction []Action
	StatefulMeta   Metadata
	WorkerIns      v1alpha1.WorkerInstance
}

// PodAgency update agency to the pod
type PodAgency struct {
	FuncMeta  *faasfunction.FunctionService
	WorkerIns map[string]v1alpha1.WorkerInstance
}

// StateMetadata -
type StateMetadata struct {
	StateID    string
	FuncName   string
	TenantID   string
	BusinessID string
	Version    string
	ServiceID  string
	NodeID     string
	Creator    string
	CPU        int64
	Memory     int64
	Cluster    string
	Applier    string
}

type functionCache struct {
	funcMeta           *faasfunction.FunctionService
	autoScaler         *autoscaler.AutoScaler
	fsStatusUpdateMux  *SpinMutex
	breaker            breaker.Breaker
	reservedInsChan    chan v1alpha1.WorkerInstance
	deleteHook         []func(*functionCache)
	deleteInstanceHook []func(*functionCache, v1alpha1.WorkerInstance)
	instances          map[string]v1alpha1.WorkerInstance
	prewarmPool        []*corev1.Pod
	predictionNum      int
	scalingDone        map[string]int64
	// key: nodename value: last radical autoscaler scale down timestamp of this node
	autoScaleTimestamp map[string]int64
	dnsDomainCfg       []types.DNSDomainInfo
	// key: dns name value: ip address
	endpointDomainCfg []types.DNSDomainInfo
	funcVpcPats       []types.NATConfigure // the length must be 2
	endPointVpcPats   []types.NATConfigure // the length must be 2
	publicVpcPats     []types.NATConfigure // the length must be 2
	// [key: nodename value: reserved distribution times, reserved instance pre-allocation
	reservedInsNodePreDis map[string]int
	idleMode              bool
	minScale              int64
	maxScale              int64
	originalMaxScale      int64
	availableCluster      []string
	// minScaleUpdatedTime is for check whether we need to report an alarm of unreached minScale
	minScaleUpdatedTime time.Time
	// minScaleAlarmSign is the sign of whether we have sent an alarm of unreached minScale
	minScaleAlarmSign bool
	// isUserErrorAlarmSign is the sign of whether user caused the unreached minScale
	isUserErrorAlarmSign bool
	minScaleErr          error
	// scalingNum is used for checking max scale
	scalingNum int64
	// scalingOnDemandNum counts scaling on-demand instances
	scalingOnDemandNum int64
	// scalingReversed counts scaling reversed instances of current CR signature
	scalingReversed          int64
	scalingPrewarm           int
	crSignature              string
	funcSvcID                string
	info                     commonmetrics.FunctionInfo
	profile                  types.ProfileMetaData
	vpcID                    string
	endpointTenantVpcID      string
	stateMap                 sync.Map
	stateFulEventCheckRepeat sync.Map
	stateReloading           sync.Map
	expectedNum              int
	rollingInstances         map[string]v1alpha1.WorkerInstance
	initInstances            map[string]struct{}
	dockerClient             *engine.DockerClient
	CustomImgSize            int64
	funcInfo                 utils.FunctionInfo
	sync.RWMutex
	funcScheduleNodes PodScheduleInfo
}

func deepCopyPodScheduleInfo(psi PodScheduleInfo) PodScheduleInfo {
	newPsi := PodScheduleInfo{
		TaggedScheduleLabel: psi.TaggedScheduleLabel,
	}

	newPsi.AffinityNodes = make([]string, len(psi.AffinityNodes))
	copy(newPsi.AffinityNodes, psi.AffinityNodes)

	newPsi.PreferredAffinityNodes = make([]string, len(psi.PreferredAffinityNodes))
	copy(newPsi.PreferredAffinityNodes, psi.PreferredAffinityNodes)

	newPsi.AntiAffinityNodes = make([]string, len(psi.AntiAffinityNodes))
	copy(newPsi.AntiAffinityNodes, psi.AntiAffinityNodes)

	return newPsi
}

func updatePodScheduleInfoByNodeEvent(fgsTags map[string]string, node *corev1.Node,
	curSchInfo PodScheduleInfo) PodScheduleInfo {
	newSchInfo := deepCopyPodScheduleInfo(curSchInfo)
	var isScheduleNode bool
	var isNoScheduleNode bool
	var isPreferredScheduleNode bool
	if len(fgsTags) <= 0 {
		labelValue := node.Labels[DefaultLabelKey]
		if labelValue == DefaultLabelValue {
			if !utils.IsInSlice(node.Name, curSchInfo.AntiAffinityNodes) {
				newSchInfo.AntiAffinityNodes = append(newSchInfo.AntiAffinityNodes, node.Name)
			}
		}
		return newSchInfo
	}
	var isNeedSsdPool bool
	for key, expectValue := range fgsTags {
		value := node.Labels[key]
		if !isNeedSsdPool {
			isNeedSsdPool = checkIsNeedSsdPool(key, expectValue)
		}
		if value == expectValue+ScheduleLabelSuffix {
			isScheduleNode = true
			continue
		}
		if value == expectValue+NoScheduleLabelSuffix {
			isNoScheduleNode = true
			continue
		}
		if value == expectValue+PreferredScheduleLabelSuffix {
			isPreferredScheduleNode = true
		}
	}
	newSchInfo.MatchedSSDPoolLabel = isNeedSsdPool
	if isScheduleNode {
		newSchInfo.AffinityNodes = ensureUniqueAndAddNode(newSchInfo.AffinityNodes, node.Name)
	}
	if isPreferredScheduleNode {
		newSchInfo.PreferredAffinityNodes = ensureUniqueAndAddNode(newSchInfo.PreferredAffinityNodes, node.Name)
	}
	if isNoScheduleNode {
		newSchInfo.AntiAffinityNodes = ensureUniqueAndAddNode(newSchInfo.AntiAffinityNodes, node.Name)
	}

	newSchInfo.AffinityNodes = removeNodeIfNotRequired(newSchInfo.AffinityNodes, isScheduleNode, node.Name)
	newSchInfo.PreferredAffinityNodes = removeNodeIfNotRequired(newSchInfo.PreferredAffinityNodes,
		isPreferredScheduleNode, node.Name)
	newSchInfo.AntiAffinityNodes = removeNodeIfNotRequired(newSchInfo.AntiAffinityNodes, isNoScheduleNode, node.Name)
	log.GetLogger().Debugf("fgsTags %v, node name %s, label %v, newSchInfo %v",
		fgsTags, node.Name, node.Labels, newSchInfo)
	return newSchInfo
}

func ensureUniqueAndAddNode(slice []string, node string) []string {
	if len(slice) == 0 {
		return []string{node}
	}
	for index := 0; index < len(slice); index++ {
		if slice[index] == node {
			return slice
		}
	}
	return append(slice, node)
}

func removeNodeIfNotRequired(slice []string, required bool, node string) []string {
	if !required {
		for i, n := range slice {
			if n == node {
				return append(slice[:i], slice[i+1:]...)
			}
		}
	}
	return slice
}

// PodScheduleInfo obtaining scheduling nodes based on labels
type PodScheduleInfo struct {
	AffinityNodes          []string
	PreferredAffinityNodes []string
	AntiAffinityNodes      []string
	TaggedScheduleLabel    bool
	// determine is use the ssd pool
	MatchedSSDPoolLabel bool
}

func getScheduleNodesByLabels(fgsTags map[string]string) PodScheduleInfo {
	nodeCache.mux.RLock()
	defer nodeCache.mux.RUnlock()
	noScheduleNodes := make([]string, 0, utils.DefaultSliceSize)
	preferredScheduleNodes := make([]string, 0, utils.DefaultSliceSize)
	scheduleNodes := make([]string, 0, utils.DefaultSliceSize)
	if len(fgsTags) <= 0 {
		for nodeName, node := range nodeCache.NodeList {
			labelValue := node.Label[DefaultLabelKey]
			if labelValue == DefaultLabelValue {
				noScheduleNodes = append(noScheduleNodes, nodeName)
			}
		}
		return PodScheduleInfo{
			AntiAffinityNodes: noScheduleNodes,
		}
	}

	var isNeedSsdPool bool
	for nodeName, node := range nodeCache.NodeList {
		for key, expectValue := range fgsTags {
			value := node.Label[key]
			if !isNeedSsdPool {
				isNeedSsdPool = checkIsNeedSsdPool(key, expectValue)
			}
			if value == expectValue+ScheduleLabelSuffix {
				scheduleNodes = append(scheduleNodes, nodeName)
				continue
			}
			if value == expectValue+NoScheduleLabelSuffix {
				noScheduleNodes = append(noScheduleNodes, nodeName)
				continue
			}
			if value == expectValue+PreferredScheduleLabelSuffix {
				preferredScheduleNodes = append(preferredScheduleNodes, nodeName)
			}
		}
	}
	return PodScheduleInfo{
		AffinityNodes:          scheduleNodes,
		PreferredAffinityNodes: preferredScheduleNodes,
		AntiAffinityNodes:      noScheduleNodes,
		TaggedScheduleLabel:    true,
		MatchedSSDPoolLabel:    isNeedSsdPool,
	}
}

func checkIsNeedSsdPool(key, value string) bool {
	if val, ok := config.SSDPoolLabelsMap.Load(key); ok {
		if val.(string) == value {
			return true
		}
	}
	return false
}

func (fc *functionCache) getFuncScheduleNodes() PodScheduleInfo {
	fc.Lock()
	podSchedulerInfo := fc.funcScheduleNodes
	fc.Unlock()
	return podSchedulerInfo
}

func (fc *functionCache) getScalingNum() int64 {
	return fc.scalingNum
}

func (fc *functionCache) getScalingOnDemandNum() int64 {
	return fc.scalingOnDemandNum
}

func (fc *functionCache) getCurOnDemandNum() int64 {
	var num int64
	migratedFromPodList := make(map[string]struct{})
	for _, instance := range fc.instances {
		if instance.Applier != constants.WorkerManagerApplier {
			num++
			if instance.IsMigrating == true && instance.MigratedFrom != "" {
				migratedFromPodList[instance.MigratedFrom] = struct{}{}
			}
		}
	}
	if len(migratedFromPodList) == 0 {
		return num
	}
	migratingDeduct := int64(0)
	for _, instance := range fc.instances {
		_, exist := migratedFromPodList[instance.PodName]
		if exist {
			migratingDeduct++
		}
	}
	return num - migratingDeduct
}

func (fc *functionCache) resetScalingNum() {
	scalingOnDemandInstanceNum := fc.scalingOnDemandNum
	scalingReversedInstanceNum := fc.scalingNum - scalingOnDemandInstanceNum
	fc.scalingNum = 0
	fc.scalingOnDemandNum = 0
	fc.minScaleErr = nil
	TenantCache.resetScalingNumCallback(fc.info.TenantID, scalingOnDemandInstanceNum, scalingReversedInstanceNum)
}

func (fc *functionCache) increaseScalingNum(isReversed bool) {
	fc.scalingNum++
	if !isReversed {
		fc.scalingOnDemandNum++
	}
	TenantCache.increaseScalingNumCallback(fc.info.TenantID, isReversed)
}

func (fc *functionCache) decreaseScalingNum(isReversed bool) {
	fc.scalingNum--
	if !isReversed {
		fc.scalingOnDemandNum--
	}
	TenantCache.decreaseScalingNumCallback(fc.info.TenantID, isReversed)
}

func (fc *functionCache) addInstance(podName string, instance v1alpha1.WorkerInstance) {
	_, exist := fc.instances[podName]
	if exist && fc.instances[podName].MigratedFrom != "" && instance.MigratedFrom == "" {
		instance.IsMigrating = fc.instances[podName].IsMigrating
		instance.MigratedFrom = fc.instances[podName].MigratedFrom
	}
	log.GetLogger().Infof("addIns | %s | %s", podName, instance.BusinessType)
	fc.instances[podName] = instance
	if !exist && instance.Applier != constants.PredictionApplier {
		TenantCache.addInstanceCallback(fc.info.TenantID, instance.Applier == constants.WorkerManagerApplier)
	}
}

func (fc *functionCache) deleteInstance(podName string, isReversed bool) {
	_, exist := fc.instances[podName]
	delete(fc.instances, podName)
	log.GetLogger().Infof("delete instance, podName: %s", podName)
	if isReversed {
		if fc.countReversedInstance() < fc.minScale {
			fc.minScaleUpdatedTime = time.Now()
		}
	}
	if exist {
		TenantCache.deleteInstanceCallback(fc.info.TenantID, isReversed)
	}
}

func (fc *functionCache) getMigratingReservedIns() (v1alpha1.WorkerInstance, bool) {
	for _, instance := range fc.instances {
		if instance.IsMCPFunc {
			return v1alpha1.WorkerInstance{}, false
		}
		nodeInfo := GetNodeInfo(instance.NodeName)
		if nodeInfo == nil {
			continue
		}
		// instance is reserved and node has migrating taints will prefer to be scale down
		if instance.Applier == constants.WorkerManagerApplier && utils.HasMigratingTaints(nodeInfo.Taints) {
			return instance, true
		}
		// instance is reserved and node is not ready will prefer to be scale down
		if instance.Applier == constants.WorkerManagerApplier && !nodeInfo.IsReady {
			return instance, true
		}
	}
	return v1alpha1.WorkerInstance{}, false
}

func (fc *functionCache) resetInstance() {
	totalNum := int64(len(fc.instances))
	reversedInstanceNum := fc.countReversedInstance()
	onDemandInstanceNum := totalNum - reversedInstanceNum
	fc.instances = make(map[string]v1alpha1.WorkerInstance, initCacheSize)
	TenantCache.resetInstanceCallback(fc.info.TenantID, onDemandInstanceNum, reversedInstanceNum)
}

func (fc *functionCache) isAllowed() bool {
	if fc.breaker == nil {
		// breaker is nil, it will allow all requests
		return true
	}
	return fc.breaker.IsAllowed()
}

func (fc *functionCache) fail() {
	if fc.breaker == nil {
		return
	}
	fc.breaker.Fail()
}

func (fc *functionCache) succeed() {
	if fc.breaker == nil {
		return
	}
	fc.breaker.Succeed()
}

func (fc *functionCache) resetBreaker() {
	if fc.breaker == nil {
		return
	}
	fc.breaker.Reset()
}

// ProcessUpdateDomainConfig process update domain config
func (fc *functionCache) ProcessUpdateDomainConfig(funcSvcID string) {
	if fc.vpcID == "" && fc.endpointTenantVpcID == "" {
		utils.GetDNSCache().ProcessUpdateDomainConfig(funcSvcID)
		return
	}
	var domains []string
	if fc.vpcID != "" && len(fc.dnsDomainCfg) != 0 {
		for _, cfg := range fc.dnsDomainCfg {
			domainKey := fc.vpcID + utils.SplicingCharacter + cfg.DomainName
			domains = append(domains, domainKey)
		}
	}
	if fc.endpointTenantVpcID != "" && len(fc.endpointDomainCfg) != 0 {
		for _, cfg := range fc.endpointDomainCfg {
			domainKey := fc.endpointTenantVpcID + utils.SplicingCharacter + cfg.DomainName
			domains = append(domains, domainKey)
		}
	}
	newDomainKeys, ok := utils.GetDNSCache().ProcessUpdateDomainConfig(funcSvcID, domains...)
	if !ok {
		return
	}
	for _, domainKey := range newDomainKeys {
		list := strings.Split(domainKey, utils.SplicingCharacter)
		if len(list) != domainKeyLength {
			log.GetLogger().Errorf("failed to get dns domainNames with invalid domainKey: %s "+
				"for func: %s", domainKey, funcSvcID)
			continue
		}
		vpcID := list[vpcIDIndex]
		xRole, userAgency := fc.getXRoleOrAgency(vpcID)
		if xRole == "" && userAgency.Token == "" {
			log.GetLogger().Errorf("failed to get dns userAgency or xrole for func: %s,"+
				" domainKey: %s", funcSvcID, domainKey)
			continue
		}
		fc.processUpdateDNSInfo(funcSvcID, vpcID, list[domainNameIndex], xRole, userAgency)
	}
}

func (fc *functionCache) getXRoleOrAgency(vpcID string) (string, utils.UserAgency) {
	if fc.funcMeta != nil && fc.vpcID == vpcID {
		if !utils.IsDNSV3Version() {
			return "", utils.GetUserAgencyByFuncMeta(fc.funcMeta)
		}
		return fc.funcMeta.Spec.Configuration.Role.XRole, utils.GetUserAgencyByFuncMeta(fc.funcMeta)
	}
	if fc.funcMeta != nil && fc.endpointTenantVpcID == vpcID {
		if !utils.IsDNSV3Version() {
			return "", utils.GetUserAgencyByVpcConfig(fc.funcMeta.Spec.Configuration.EndpointTenantVpc)
		}
		return fc.funcMeta.Spec.Configuration.EndpointTenantVpc.Xrole,
			utils.GetUserAgencyByVpcConfig(fc.funcMeta.Spec.Configuration.EndpointTenantVpc)
	}
	return "", utils.UserAgency{}
}

func (fc *functionCache) updateFunctionClusters(clusterStatusMap map[string]clusterstatusutil.ClusterFlowState) {
	var availableCluster []string
	if fc.funcMeta != nil {
		fcClusters := utils.GetFuncClusterByFuncMeta(fc.funcMeta)
		if len(fcClusters) != 0 {
			fc.availableCluster = getClustersInFunCache(clusterStatusMap, fcClusters, availableCluster)
			log.GetLogger().Infof("function %s has available clusters: %v",
				fc.funcMeta.Spec.Configuration.FunctionName, fc.availableCluster)
			return
		}
		fc.availableCluster = getCurrentAvailableClusters()
	}
	sort.Strings(fc.availableCluster)
	log.GetLogger().Debugf("current clusters list is: %v", fc.availableCluster)
}

func getClustersInFunCache(clusterStatusMap map[string]clusterstatusutil.ClusterFlowState,
	fcClusters []string, availableCluster []string) []string {
	for _, cluster := range fcClusters {
		clusterStatus, exist := clusterStatusMap[cluster]
		if !exist {
			return nil
		}
		if clusterStatus.State == "running" {
			availableCluster = append(availableCluster, cluster)
		}
	}
	return availableCluster
}

func (fc *functionCache) updateClusterMaxScale() {
	if !utils.GetUpdateFunctionCacheEnable() {
		return
	}
	clusterAmount := len(fc.availableCluster)
	if clusterAmount == 0 {
		log.GetLogger().Errorf("function %s has no available clusters",
			fc.funcMeta.Spec.Configuration.FunctionName)
		return
	}

	fc.maxScale = int64(math.Floor(float64(fc.originalMaxScale) / float64(clusterAmount)))
	restValue := fc.originalMaxScale % int64(clusterAmount)
	for index, cluster := range fc.availableCluster {
		if int64(index) < restValue && cluster == clusterID {
			// arrange redundant instances of cluster for first mod cluster
			fc.maxScale++
			break
		}
	}
	log.GetLogger().Infof("update function maxScale from %d to %d of %s function.",
		fc.originalMaxScale, fc.maxScale, fc.funcSvcID)
}

func (fc *functionCache) processUpdateDNSInfo(
	funcSvcID string, vpcID string, domainName string, xRole string, agency utils.UserAgency) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return
	}
	if dnsDomainMap, flag := fc.UpdateDNSInfo(vpcID, domainName, xRole, cfg, agency); flag {
		utils.GetDNSCache().StoreDNSCache(funcSvcID, vpcID, domainName, dnsDomainMap)
		log.GetLogger().Infof("finished to acquire "+
			"domainNames: %v for func: %s, vpcID: %s", domainName, fc.funcMeta.ID, vpcID)
	}
}

// UpdateDNSInfo update dns map of cache
func (fc *functionCache) UpdateDNSInfo(vpcID string, domainName string, xRole string,
	cfg *config.FaasConfig, agency utils.UserAgency) (map[string][]string, bool) {
	dnsAddress := utils.GetDNSAddress()
	dnsDomainInfo, ok := fc.getDNSDomainCfg(vpcID, domainName)
	if !ok {
		log.GetLogger().Errorf("failed to get dns domain config for domain name %s,"+
			" vpcID: %s", domainName, vpcID)
		return nil, false
	}
	param := utils.DNSProxyRequestParam{
		XRole:       xRole,
		DNSAddress:  dnsAddress,
		TenantToken: agency.Token,
		TenantID:    fc.info.TenantID,
		VpcID:       vpcID,
		DomainID:    fc.funcMeta.Spec.Configuration.DomainID,
		Info:        dnsDomainInfo,
	}
	dnsDomainMap, err := utils.GetIPsByDNSByAddresses(cfg.AlarmServerAddress,
		cfg.LocalAuth.AKey, cfg.LocalAuth.SKey, param)
	if err != nil {
		log.GetLogger().Warnf("failed to get ips by dns by interval"+
			" for domain name: %s, vpcID: %s with error: %s",
			dnsDomainInfo.DomainName, vpcID, err.Error())
		return nil, false
	}
	return dnsDomainMap, true
}

func (fc *functionCache) getDNSDomainCfg(vpcID string, domainName string) (types.DNSDomainInfo, bool) {
	if vpcID == fc.vpcID {
		for _, dnsDomainInfo := range fc.dnsDomainCfg {
			if dnsDomainInfo.DomainName == domainName {
				return dnsDomainInfo, true
			}
		}
	}
	if vpcID == fc.endpointTenantVpcID {
		for _, dnsDomainInfo := range fc.endpointDomainCfg {
			if dnsDomainInfo.DomainName == domainName {
				return dnsDomainInfo, true
			}
		}
	}
	return types.DNSDomainInfo{}, false
}

func deletePod(pod *corev1.Pod) {
	err := utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).Delete(context.TODO(), pod.Name,
		metav1.DeleteOptions{})
	if err != nil {
		log.GetLogger().Errorf("failed to delete pod %s error %s", pod.Name, err.Error())
		return
	}
	log.GetLogger().Infof("succeed to delete pod %s for %s", pod.Name, utils.GetFullFuncNameByPod(pod))
}

func pullFunctionCache(pod *corev1.Pod, functionName string) *functionCache {
	etcdFuncKey := utils.GetETCDFuncKeyByPod(pod)
	funcMeta, profileMeta, source, err := utils.GetMetaInfoFromEtcd(etcdFuncKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get func: %s, pod: %s, error: %s",
			functionName, pod.Name, err.Error())
		return nil
	}
	if !CheckEventSource(eventsource.Event{
		SourceType: source.String(),
		EventType:  eventsource.Added,
		Kind:       utils.FunctionRepoEventKind,
	}, functionName) {
		log.GetLogger().Errorf("failed to check event source of func %s pod %s source %s",
			functionName, pod.Name, source)
		return nil
	}

	return loadOrStoreFuncCache(functionName, funcMeta, profileMeta)
}

func loadOrStoreFuncCache(funcSvcID string, funcMeta *faasfunction.FunctionService,
	profileMeta types.ProfileMetaData) *functionCache {
	signature := utils.GetCRSignature(funcMeta)
	fcIf, _ := WorkerCache.functions.LoadOrStore(funcSvcID, &functionCache{
		breaker:             breaker.NewCircuitBreaker(breaker.GetRule()),
		minScale:            funcMeta.Spec.Configuration.AutoScalerConfig.MinimumScale,
		maxScale:            funcMeta.Spec.Configuration.AutoScalerConfig.MaximumScale,
		originalMaxScale:    funcMeta.Spec.Configuration.AutoScalerConfig.MaximumScale,
		availableCluster:    getCurrentAvailableClusters(),
		minScaleUpdatedTime: time.Now(),
		instances:           make(map[string]v1alpha1.WorkerInstance, initCacheSize),
		crSignature:         signature,
		funcSvcID:           funcSvcID,
		funcMeta:            funcMeta,
		info: commonmetrics.FunctionInfo{
			FunctionName: funcMeta.Spec.Configuration.FunctionName,
			TenantID:     funcMeta.Spec.Configuration.TenantID,
			BusinessID:   funcMeta.Spec.Configuration.BusinessID,
			Version:      funcMeta.Spec.Configuration.FunctionVersion,
		},
		reservedInsChan:       make(chan v1alpha1.WorkerInstance),
		reservedInsNodePreDis: make(map[string]int, initCacheSize),
		autoScaleTimestamp:    make(map[string]int64, initCacheSize),
		scalingDone:           make(map[string]int64, initCacheSize),
		predictionNum:         -1,
		profile:               profileMeta,
		rollingInstances:      make(map[string]v1alpha1.WorkerInstance),
		initInstances:         map[string]struct{}{},
		funcScheduleNodes:     getScheduleNodesByLabels(funcMeta.Spec.Configuration.FgsTags),
	})
	fc, ok := fcIf.(*functionCache)
	if !ok {
		log.GetLogger().Errorf("failed to build function cache for funcSvcID: %s", funcSvcID)
		return nil
	}
	fc.Lock()
	fc.funcMeta = funcMeta
	fc.crSignature = signature
	fc.profile = profileMeta
	fc.Unlock()
	fc.updateFunctionClusters(clusterstatusutil.ClusterStatusCache.ClusterStatusMap)
	fc.updateClusterMaxScale()
	return fc
}

// UpdateFuncCache Update function cache
func UpdateFuncCache(funcName, businessID, tenantID, version string) {
	updateFuncCache(funcName, businessID, tenantID, version, false)
}

// UpdateSilentFuncCache -
func UpdateSilentFuncCache(funcName, businessID, tenantID, version string) {
	updateFuncCache(funcName, businessID, tenantID, version, true)
}

func updateFuncCache(funcName, businessID, tenantID, version string, isSilentFunc bool) {
	etcdFuncKey := utils.FunctionKey(businessID, tenantID, funcName, version)
	if isSilentFunc {
		etcdFuncKey = utils.SilentFunctionKey(businessID, tenantID, funcName, version)
	}
	log.GetLogger().Infof("start to get funcMeta by %s for %s", etcdFuncKey, funcName)
	funcMeta, profileMeta, source, err := utils.GetMetaInfoFromEtcd(etcdFuncKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get funcMeta for %s, error: %s", etcdFuncKey, err.Error())
		return
	}
	funcSvcID := utils.CrName(businessID, tenantID, funcName, version)
	if !CheckEventSource(eventsource.Event{
		SourceType: source.String(),
		EventType:  eventsource.Added,
		Kind:       utils.FunctionRepoEventKind,
	}, funcSvcID) {
		log.GetLogger().Warnf("failed to check event source for func %s source %s", funcSvcID, source)
		return
	}
	loadOrStoreFuncCache(funcSvcID, funcMeta, profileMeta)
}

func getUpdatedFuncCache(stateMetaInfo StateMetadata) (*functionCache, error) {
	etcdFuncKey := "/sn/functions/business/" + stateMetaInfo.BusinessID + "/tenant/" + stateMetaInfo.TenantID +
		"/function/" + stateMetaInfo.ServiceID + "@" + stateMetaInfo.FuncName + "/version/" + stateMetaInfo.Version

	funcSvcID := utils.CrName(stateMetaInfo.BusinessID, stateMetaInfo.TenantID,
		stateMetaInfo.ServiceID+"@"+stateMetaInfo.FuncName, stateMetaInfo.Version)

	funcMeta, profileMeta, source, err := utils.GetMetaInfoFromEtcd(etcdFuncKey)
	if err != nil {
		log.GetLogger().Errorf("failed to get func: %s, error: %s",
			funcSvcID, err.Error())
		return nil, err
	}
	if !CheckEventSource(eventsource.Event{
		SourceType: source.String(),
		EventType:  eventsource.Added,
		Kind:       utils.FunctionRepoEventKind,
	}, stateMetaInfo.ServiceID+"@"+stateMetaInfo.FuncName) {
		log.GetLogger().Errorf("failed to check event source of func %s source %s",
			funcSvcID, source)
		return nil, nil
	}
	return loadOrStoreFuncCache(funcSvcID, funcMeta, profileMeta), nil
}

func initWorkerCacheWithPod(pod *corev1.Pod) *commonmetrics.FunctionInfo {
	err := checkPodImageTag(pod)
	if err != nil {
		log.GetLogger().Warnf("delete instance %s because of: %s", pod.Name, err.Error())
		return nil
	}

	functionName, ok := utils.OwnedByFunctionService(pod)
	if !ok {
		log.GetLogger().Infof("pod %s is not associated with function service, delete it now", pod.Name)
		return nil
	}
	if !checkPodStateIDExist(pod.Labels["stateID"]) {
		log.GetLogger().Warnf("pod: %s with stateID: %s is not exist in Meta ETCD", pod.Name, pod.Labels["stateID"])
		return nil
	}
	fc, ok := WorkerCache.getFunctionCache(functionName)
	if !ok {
		fc = pullFunctionCache(pod, functionName)
		if fc == nil {
			log.GetLogger().Errorf("PullFunctionCache failed")
			return nil
		}
	}
	fc.Lock()
	if fc.crSignature != utils.GetPodCRSignature(pod) {
		if !commonutils.IsCAEFunc(fc.funcMeta.Spec.Configuration.BusinessType) {
			fc.Unlock()
			log.GetLogger().Warnf("pod signature mismatched, function: %s, pod: %s", functionName, pod.Name)
			return nil
		}
		fc.rollingInstances[pod.Name] = utils.MakeWorkerInsInfo(pod)
	} else {
		fc.initInstances[pod.Name] = struct{}{}
	}
	if pod.Labels[reserveInstanceSymbol] == constants.WorkerManagerApplier &&
		(pod.DeletionTimestamp == nil && utils.IsPodReady(pod)) {
		fc.scalingReversed++
	}
	if pod.Labels["stateID"] != "" {
		fc.stateMap.LoadOrStore(pod.Labels["stateID"], 1)
		fc.stateFulEventCheckRepeat.LoadOrStore(pod.Labels["stateID"], pod.Name)
	}
	info := fc.info
	fc.Unlock()
	return &info
}

func checkPodStateIDExist(stateID string) bool {
	if stateID == "" {
		return true
	}
	_, ok := StateWorkersList.Load(stateID)
	return ok
}

func checkPodImageTag(pod *corev1.Pod) error {
	if len(pod.Spec.Containers) != instanceContainerLength {
		return errors.New("wrong length of containers")
	}

	isCustomContainer := utils.IsCustomContainerPod(pod.Name)
	if isCustomContainer {
		err := checkCustomContainerTag(pod)
		if err != nil {
			return err
		}
		return nil
	}

	for _, c := range pod.Spec.Containers {
		if excepted, ok := workerMap[c.Name]; ok {
			if c.Image != excepted {
				return errors.New("wrong container image version")
			}
		}
	}
	if err := checkInitContainerImage(pod.Spec); err != nil {
		return err
	}
	return nil
}

func checkCustomContainerTag(pod *corev1.Pod) error {
	for _, c := range pod.Spec.Containers {
		if c.Name != utils.WorkerContainerName {
			continue
		}
		if excepted, ok := workerMap[c.Name]; ok {
			if c.Image != excepted {
				return errors.New("wrong container image version")
			}
		}
	}
	if err := checkInitContainerImage(pod.Spec); err != nil {
		return err
	}
	return nil
}

func checkInitContainerImage(spec corev1.PodSpec) error {
	for _, c := range spec.InitContainers {
		if excepted, ok := initMap[c.Name]; ok {
			if c.Image != excepted {
				return errors.New("wrong init container image version")
			}
		}
	}
	return nil
}

type syncCompare struct {
	instance v1alpha1.WorkerInstance
	path     string
}

func getInstanceListFromEtcd() ([]syncCompare, error) {
	var instanceList []syncCompare
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetRouterEtcdClient()
	resp, err := client.GetResponse(defaultCtx, "/sn/workers", clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("failed to connect to etcd, err: %s", err.Error())
		return nil, err
	}

	for _, kv := range resp.Kvs {
		key := etcdkey.WorkerInstanceKey{}
		if err := key.ParseFrom(string(kv.Key)); err != nil {
			log.GetLogger().Errorf("invalid worker instance key: %s, err: %s", string(kv.Key), err.Error())
			continue
		}
		if key.TenantID == "0" {
			continue
		}
		var instance v1alpha1.WorkerInstance
		err = json.Unmarshal(kv.Value, &instance)
		if err != nil {
			log.GetLogger().Errorf("invalid worker instance value: %s, err: %s", string(kv.Key), err.Error())
			continue
		}
		var ins = etcdkey.WorkerInstanceKey{}
		err = ins.ParseFrom(string(kv.Key))
		if err != nil {
			log.GetLogger().Errorf("invalid worker instance value: %s, err: %s", string(kv.Key), err.Error())
			continue
		}
		instance.InstanceID = ins.Instance
		instanceList = append(instanceList, syncCompare{
			instance: instance,
			path:     string(kv.Key),
		})
	}
	return instanceList, nil
}

func getStateMetaDataFromEtcd() error {
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetStateEtcdClient()
	if client == nil || client.Client == nil {
		log.GetLogger().Warnf("state etcd client is nil")
		return nil
	}
	resp, err := client.GetResponse(defaultCtx, "/sn/stateworkers/business/", clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("failed to connect to state etcd, err: %s", err.Error())
		return err
	}

	for _, kv := range resp.Kvs {
		stateID, err := parseStateETCDKey(string(kv.Key))
		if err != nil {
			log.GetLogger().Errorf("invalid state workers key: %s, err: %s", string(kv.Key), err.Error())
			continue
		}
		stateIDFuncMeta := StateMetadata{}
		if err = stateIDFuncMeta.parseStateWorkerValue(kv.Value, stateID); err != nil {
			log.GetLogger().Errorf("invalid state workers value: %s, err: %s", string(kv.Key), err.Error())
			continue
		}
		if stateIDFuncMeta.Cluster == utils.GetEnvClusterID() {
			StateWorkersList.Store(stateID, stateIDFuncMeta)
		}
	}
	return nil
}

func parseStateETCDKey(stateKey string) (string, error) {
	components := strings.Split(stateKey, "/")
	if len(components) != stateWorkersKeyLength {
		return "", errors.New("the length of state workers key is not 12")
	}
	if components[stateIDIndex] == "" {
		return "", errors.New("stateID is null string")
	}
	return components[stateIDIndex], nil
}

func (s *StateMetadata) parseStateWorkerValue(stateVal []byte, stateID string) error {
	stateValue := make(map[string]string, 11)
	if err := json.Unmarshal(stateVal, &stateValue); err != nil {
		log.GetLogger().Errorf("invalid worker instance value: %s, err: %s", string(stateVal), err.Error())
		return err
	}
	s.StateID = stateID
	s.ServiceID = stateValue[constants.ServiceIDKey+stateID]
	s.FuncName = stateValue[constants.FuncNameKey+stateID]
	s.TenantID = stateValue[constants.TenantIDKey+stateID]
	s.BusinessID = stateValue[constants.BusinessIDKey+stateID]
	s.Version = stateValue[constants.VersionKey+stateID]
	s.Creator = stateValue[constants.CreatorKey+stateID]
	s.NodeID = stateValue[stateID]
	s.Cluster = stateValue[constants.ClusterKey]
	s.Applier = stateValue[constants.ApplierKey]
	dynamicCPU, err := strconv.ParseInt(stateValue[constants.CPUKey+stateID], base, bitSize)
	if err != nil {
		dynamicCPU = 0
	}
	dynamicMem, err := strconv.ParseInt(stateValue[constants.MemoryKey+stateID], base, bitSize)
	if err != nil {
		dynamicMem = 0
	}
	s.CPU = dynamicCPU
	s.Memory = dynamicMem
	return nil
}

// ClusterStatusStatefulFlowOut -
func ClusterStatusStatefulFlowOut() []PodAction {
	clusterstatusutil.ClusterStatusCache.Lock()
	status := clusterstatusutil.ClusterStatusCache.ClusterStatusMap[clusterID].State
	clusterstatusutil.ClusterStatusCache.Unlock()
	var stateAction []PodAction
	if status == clusterstatusutil.ClusterFlowOutingStatus || status == clusterstatusutil.ClusterFlowOutFinishStatus {
		WorkerCache.functions.Range(func(funcSvcID, funcCache interface{}) bool {
			fc, ok := funcCache.(*functionCache)
			if !ok {
				return true
			}
			// metadata is nil or is not stateful function, return
			// return if bridge function, because we need bridge instance to deal with request
			// we can delete the bridge instance when cluster in state router is changed(method: handlePodWithoutReload)
			if fc.funcMeta == nil || !fc.funcMeta.Spec.Configuration.IsStatefulFunction ||
				fc.funcMeta.Spec.Configuration.IsBridgeFunction {
				return true
			}
			fc.Lock()
			defer fc.Unlock()
			if len(fc.instances) == 0 {
				return true
			}
			for _, instance := range fc.instances {
				log.GetLogger().Infof("stateID: %s instance: %s exist, start to flow out delete",
					instance.StateID, instance.PodName)
				metadata := Metadata{Status: NotChanged}
				metadata.FuncMeta = fc.funcMeta
				metadata.CRSignature = fc.crSignature
				metadata.Creator = instance.Creator
				metadata.StateID = instance.StateID
				actions, metadata := handleStatefulInstanceDelete(metadata,
					&InstanceMeta{Instance: instance})
				stateAction = append(stateAction, PodAction{FuncSvc: funcSvcID.(string), StatefulAction: actions,
					StatefulMeta: metadata, WorkerIns: instance})
				fc.stateFulEventCheckRepeat.Delete(instance.StateID)
				fc.stateMap.Delete(instance.StateID)
			}
			return true
		})
	}
	return stateAction
}

func handleStatefulInstanceDelete(metadata Metadata, event *InstanceMeta) ([]Action, Metadata) {
	metadata.StateID = event.Instance.StateID
	var actions []Action
	actions = append(actions, Action{Type: ActionStatefulInstanceFlowOutDelete, NeedPull: 0,
		NeedReduce: []v1alpha1.WorkerInstance{event.Instance}})
	return actions, metadata
}

func handleDifference(expect, actual []syncCompare) error {
	expectInsMap := make(map[string]syncCompare, initCacheSize)
	actualInsMap := make(map[string]syncCompare, initCacheSize)

	for _, ins := range expect {
		expectInsMap[ins.instance.InstanceID] = ins
	}
	for _, ins := range actual {
		actualInsMap[ins.instance.InstanceID] = ins
	}

	for key, expectIns := range expectInsMap {
		var err error
		if act, ok := actualInsMap[key]; ok {
			if act.instance != expectIns.instance && expectIns.instance.IsRunning {
				err = putInstanceToEtcd(expectIns.instance, expectIns.path)
			}
		} else if expectIns.instance.IsRunning {
			err = putInstanceToEtcd(expectIns.instance, expectIns.path)
		}
		if err != nil {
			log.GetLogger().Errorf("failed to put excepted instance to etcd, instance: %s", key)
			return err
		}
	}

	for key, actIns := range actualInsMap {
		if _, ok := expectInsMap[key]; !ok {
			log.GetLogger().Infof("start to delete instance from etcd: %s, %s", key, actIns.path)
			err := deleteFromEtcd(actIns.path)
			if err != nil {
				log.GetLogger().Errorf("failed to delete instance from etcd, instance: %s", key)
				return err
			}

			if !commonutils.IsCAEFunc(actIns.instance.BusinessType) {
				continue
			}
			err = deleteFromCAERouterEtcd(actIns.path)
			if err != nil {
				log.GetLogger().Errorf("failed to delete instance from cae router etcd, instance: %s", key)
				return err
			}
		}
	}
	return nil
}

func generateEtcdKey(instance v1alpha1.WorkerInstance, info commonmetrics.FunctionInfo) string {
	business := info.BusinessID
	tenant := info.TenantID
	funcName := info.FunctionName
	version := info.Version

	path := "/sn/workers/business/" + business + "/tenant/" + tenant + "/function/" +
		funcName + "/version/" + utils.VersionRestore(version) + "/" + commonutils.AzEnv() + "/"
	return path + instance.InstanceID
}

func putInstanceToEtcd(instance v1alpha1.WorkerInstance, path string) error {
	data, err := json.Marshal(instance)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal instance data, err: %s", err.Error())
		return err
	}
	etcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err = etcd3.GetRouterEtcdClient().Put(etcdCtx, path, string(data))
	if err != nil {
		log.GetLogger().Errorf("failed to put data to etcd: %s", err.Error())
		return err
	}
	if !commonutils.IsCAEFunc(instance.BusinessType) {
		return nil
	}
	if etcd3.GetCaeRouterEtcdClient() == nil || etcd3.GetCaeRouterEtcdClient().Client == nil {
		log.GetLogger().Warn("cae router etcd client is nil when putInstanceToEtcd")
		return nil
	}
	caeEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err = etcd3.GetCaeRouterEtcdClient().Put(caeEtcdCtx, path, string(data))
	if err != nil {
		log.GetLogger().Errorf("failed to put data to cae router etcd: %s", err.Error())
		return err
	}
	return nil
}

func deleteFromEtcd(path string) error {
	etcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err := etcd3.GetRouterEtcdClient().Delete(etcdCtx, path)
	if err != nil {
		log.GetLogger().Errorf("failed to delete data from etcd: %s", err.Error())
		return err
	}
	return nil
}

func deleteFromCAERouterEtcd(path string) error {
	etcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	if etcd3.GetCaeRouterEtcdClient() == nil || etcd3.GetCaeRouterEtcdClient().Client == nil {
		log.GetLogger().Warn("cae router etcd client is nil when deleteFromCAERouterEtcd")
		return nil
	}
	err := etcd3.GetCaeRouterEtcdClient().Delete(etcdCtx, path)
	if err != nil {
		log.GetLogger().Errorf("failed to delete data from cae router etcd: %s", err.Error())
		return err
	}
	return nil
}

// InitWorkerCache inits worker cache
func InitWorkerCache(instancePods []corev1.Pod) error {
	etcdList, err := getInstanceListFromEtcd()
	if err != nil {
		log.GetLogger().Errorf("failed to list all instance from etcd, err: %s", err.Error())
		return err
	}

	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to initialize worker cache while getting config, err: %s", err.Error())
		return err
	}
	workerMap[utils.WorkerContainerName] = conf.PoolMgrConfig.PoolContainerTags.WorkerImageTag
	workerMap[utils.RuntimeContainerName] = conf.PoolMgrConfig.PoolContainerTags.RuntimeImageTag
	initMap[utils.WorkerInitContainerName] = conf.PoolMgrConfig.PoolContainerTags.WorkerInitImageTag

	var cacheList []syncCompare
	start := time.Now()
	for _, pod := range instancePods {
		if info := initWorkerCacheWithPod(&pod); info != nil {
			// it must use index of pod list items to append to instance pods slice
			// use &pod will make a bug that it will store many same pod in slice
			if pod.Labels["applier"] != constants.PredictionApplier {
				instance := utils.MakeWorkerInsInfo(&pod)
				cacheList = append(cacheList, syncCompare{
					instance: instance,
					path:     generateEtcdKey(instance, *info),
				})
			} else {
				log.GetLogger().Infof("find prediction pod, name: %s", pod.Name)
			}
			reqInfo := &config.RequestInfo{
				BusinessID:      info.BusinessID,
				TenantID:        info.TenantID,
				FunctionName:    info.FunctionName,
				FunctionVersion: info.Version,
			}
			if err = utils.ChangeOldWorkerOwner(&pod, reqInfo); err != nil {
				log.GetLogger().Errorf("failed to change old worker: %s owner with error: %s", pod.Name, err.Error())
			}
		} else {
			deletePod(&pod)
		}
	}
	log.GetLogger().Infof("finished to deal with instancePods cost time %s",
		time.Since(start).String())
	startHandleDifference := time.Now()
	if err = handleDifference(cacheList, etcdList); err != nil {
		log.GetLogger().Errorf("failed to init cache when sync with etcd, err: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("finished to run the handleDifference func cost time %s",
		time.Since(startHandleDifference).String())
	log.GetLogger().Infof("successfully init worker cache")
	return nil
}

// InitStateWorkersFromETCD inits stateful cache
func InitStateWorkersFromETCD() error {
	if err := getStateMetaDataFromEtcd(); err != nil {
		log.GetLogger().Errorf("failed to list all instance from etcd, err: %s", err.Error())
		return err
	}
	return nil
}

// InitStatefulCache -
func InitStatefulCache() []PodAction {
	var stateAction []PodAction
	StateWorkersList.Range(func(stateID, value interface{}) bool {
		stateMetaInfo, err := checkStateMetaInfo(value)
		if err != nil {
			return true
		}
		funcSvcID := utils.CrName(stateMetaInfo.BusinessID, stateMetaInfo.TenantID,
			stateMetaInfo.ServiceID+"@"+stateMetaInfo.FuncName, stateMetaInfo.Version)
		fc, ok := WorkerCache.getFunctionCache(funcSvcID)
		if !ok {
			var err error
			fc, err = getUpdatedFuncCache(stateMetaInfo)
			if err != nil && err.Error() == snerror.ErrText(snerror.FuncMetaNotFound) {
				utils.DeleteStateWorkersFromMetaETCD(stateMetaInfo.StateID)
				return true
			}
			if fc == nil {
				log.GetLogger().Warnf("failed to get functionCache, funcSvcID: %s, funcName: %s",
					funcSvcID, stateMetaInfo.ServiceID+"@"+stateMetaInfo.FuncName)
				return true
			}
		}
		fc.Lock()
		defer fc.Unlock()
		if len(fc.instances) == 0 || !isStateIDExistInInstances(fc.instances, stateMetaInfo.StateID) {
			log.GetLogger().Warnf("stateID: %s exist but no stateful instance, start to generalize",
				stateMetaInfo.StateID)
			metadata := initStatefulMetadata(fc, stateMetaInfo)
			actions, metadata := WorkerCache.statefulFunctionReload(metadata,
				&InstanceMeta{Instance: v1alpha1.WorkerInstance{StateID: stateMetaInfo.StateID}})
			stateAction = append(stateAction, PodAction{FuncSvc: funcSvcID, StatefulAction: actions, StatefulMeta: metadata})
		}
		return true
	})
	log.GetLogger().Infof("init stateful cache finished")
	return stateAction
}

func isStateIDExistInInstances(instances map[string]v1alpha1.WorkerInstance, stateID string) bool {
	if instances == nil {
		return false
	}
	var isExist bool
	for _, inst := range instances {
		if inst.StateID == stateID {
			isExist = true
			break
		}
	}
	return isExist
}

func checkStateMetaInfo(value interface{}) (StateMetadata, error) {
	stateMetaInfo, ok := value.(StateMetadata)
	if !ok {
		return StateMetadata{}, errors.New("failed to convert the data type")
	}
	if stateMetaInfo.Version == "" || stateMetaInfo.TenantID == "" || stateMetaInfo.BusinessID == "" ||
		stateMetaInfo.ServiceID == "" || stateMetaInfo.FuncName == "" || stateMetaInfo.Cluster == "" {
		log.GetLogger().Warnf("StateID: %s state worker components has null string", stateMetaInfo.StateID)
		return StateMetadata{}, errors.New("failed to check state meta info")
	}
	if stateMetaInfo.Cluster != os.Getenv("CLUSTER_ID") {
		log.GetLogger().Warnf("cluster ID not match, route: %s, local: %s, stateID: %s",
			stateMetaInfo.Cluster, clusterID, stateMetaInfo.StateID)
		return StateMetadata{}, errors.New("failed to check cluster")
	}
	return stateMetaInfo, nil
}

// GetCPUMemoryOfStateRouters Get stateful function's cpu and memory
func GetCPUMemoryOfStateRouters(value interface{}) (int64, int64) {
	stateMetaInfo, err := checkStateMetaInfo(value)
	if err != nil {
		return 0, 0
	}
	return stateMetaInfo.CPU, stateMetaInfo.Memory
}

func initStatefulMetadata(fc *functionCache, stateMetaInfo StateMetadata) Metadata {
	metadata := Metadata{Status: NotChanged}
	metadata.FuncMeta = fc.funcMeta
	metadata.CRSignature = fc.crSignature
	metadata.Creator = stateMetaInfo.Creator
	metadata.StateID = stateMetaInfo.StateID
	if stateMetaInfo.CPU != 0 || stateMetaInfo.Memory != 0 {
		metadata.CreateOptions = &types.StateCreateOptions{
			CPU:    stateMetaInfo.CPU,
			Memory: stateMetaInfo.Memory,
		}
	}
	return metadata
}

func (w *workerCache) getFunctionCache(funcSvcID string) (*functionCache, bool) {
	fc, ok := w.functions.Load(funcSvcID)
	if !ok {
		return nil, false
	}
	fci, ok := fc.(*functionCache)
	return fci, ok
}

// GetWorkerInstanceMap get worker instance map
func (w *workerCache) GetWorkerInstanceMap() map[string]*v1alpha1.WorkerInstance {
	workerInstances := make(map[string]*v1alpha1.WorkerInstance)
	w.functions.Range(func(serviceID, functionCacheIf any) bool {
		funcCache, ok := functionCacheIf.(*functionCache)
		if !ok {
			return true
		}
		funcCache.RLock()
		defer funcCache.RUnlock()
		for _, workerInstance := range funcCache.instances {
			t := workerInstance
			instanceKey := utils.GetWorkerKeyByInstance(t)
			workerInstances[instanceKey] = &t
		}
		return true
	})
	return workerInstances
}

func (w *workerCache) createFunctionCache(funcSvcID string) *functionCache {
	return w.CreateFunctionCacheByFuncMeta(funcSvcID, nil)
}

// CreateFunctionCacheByFuncMeta -
func (w *workerCache) CreateFunctionCacheByFuncMeta(funcSvcID string,
	funcMeta *faasfunction.FunctionService) *functionCache {
	fcIf, _ := w.functions.LoadOrStore(funcSvcID, &functionCache{
		funcMeta:              funcMeta,
		breaker:               breaker.NewCircuitBreaker(breaker.GetRule()),
		minScale:              utils.DefaultMinInstanceNum,
		maxScale:              utils.DefaultMaxInstanceNum,
		availableCluster:      getCurrentAvailableClusters(),
		originalMaxScale:      utils.DefaultMaxInstanceNum,
		minScaleUpdatedTime:   time.Now(),
		instances:             make(map[string]v1alpha1.WorkerInstance, initCacheSize),
		reservedInsNodePreDis: make(map[string]int, initCacheSize),
		autoScaleTimestamp:    make(map[string]int64, initCacheSize),
		scalingDone:           make(map[string]int64, initCacheSize),
		funcSvcID:             funcSvcID,
		reservedInsChan:       make(chan v1alpha1.WorkerInstance),
		predictionNum:         -1,
		rollingInstances:      make(map[string]v1alpha1.WorkerInstance),
		initInstances:         map[string]struct{}{},
	})
	fc, ok := fcIf.(*functionCache)
	if !ok {
		return nil
	}
	fc.updateFunctionClusters(clusterstatusutil.ClusterStatusCache.ClusterStatusMap)
	fc.updateClusterMaxScale()
	return fc
}

// DeleteFunctionCache -
func (w *workerCache) DeleteFunctionCache(funcSvcID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	fc.resetInstance()
	fc.resetScalingNum()
	fc.Unlock()
	w.functions.Delete(funcSvcID)
}

func (w *workerCache) createSimpleFunctionCache(funcSvcID string, insMetadata config.InstanceMetaData,
	profileMeta types.ProfileMetaData, funcInfo utils.FunctionInfo) *functionCache {
	fc, _ := w.functions.LoadOrStore(funcSvcID, &functionCache{
		idleMode:              insMetadata.IdleMode,
		minScale:              insMetadata.MinInstance,
		maxScale:              insMetadata.MaxInstance,
		originalMaxScale:      utils.DefaultMaxInstanceNum,
		availableCluster:      getCurrentAvailableClusters(),
		minScaleUpdatedTime:   time.Now(),
		instances:             make(map[string]v1alpha1.WorkerInstance, initCacheSize),
		funcSvcID:             funcSvcID,
		reservedInsChan:       make(chan v1alpha1.WorkerInstance),
		reservedInsNodePreDis: make(map[string]int, initCacheSize),
		autoScaleTimestamp:    make(map[string]int64, initCacheSize),
		scalingDone:           make(map[string]int64, initCacheSize),
		predictionNum:         -1,
		profile:               profileMeta,
		breaker:               breaker.NewCircuitBreaker(breaker.GetRule()),
		rollingInstances:      make(map[string]v1alpha1.WorkerInstance),
		initInstances:         map[string]struct{}{},
		funcInfo:              funcInfo,
	})
	return fc.(*functionCache)
}

func (w *workerCache) createFunctionCacheIfNotExist(funcSvcID, crSignature string,
	info commonmetrics.FunctionInfo) *functionCache {
	fc, _ := w.functions.LoadOrStore(funcSvcID, &functionCache{
		breaker:               breaker.NewCircuitBreaker(breaker.GetRule()),
		minScale:              utils.DefaultMinInstanceNum,
		maxScale:              utils.DefaultMaxInstanceNum,
		originalMaxScale:      utils.DefaultMaxInstanceNum,
		availableCluster:      getCurrentAvailableClusters(),
		minScaleUpdatedTime:   time.Now(),
		instances:             make(map[string]v1alpha1.WorkerInstance, initCacheSize),
		reservedInsNodePreDis: make(map[string]int, initCacheSize),
		autoScaleTimestamp:    make(map[string]int64, initCacheSize),
		scalingDone:           make(map[string]int64, initCacheSize),
		crSignature:           crSignature,
		funcSvcID:             funcSvcID,
		info:                  info,
		reservedInsChan:       make(chan v1alpha1.WorkerInstance),
		rollingInstances:      make(map[string]v1alpha1.WorkerInstance),
		initInstances:         map[string]struct{}{},
		predictionNum:         -1,
	})
	return fc.(*functionCache)
}

func waitScalingReservedIns(ctx context.Context, reservedInsChan chan v1alpha1.WorkerInstance,
	funcSvcID, traceID string) (*v1alpha1.WorkerInstance, error) {
	if reservedInsChan == nil {
		return nil, fmt.Errorf("reservedInsChan is nil for %s", funcSvcID)
	}
	// wait for a scaling reserved instance
	log.GetLogger().Infof("begin to wait reserved instance for: %s, traceID: %s", funcSvcID, traceID)
	select {
	case workerIns, ok := <-reservedInsChan:
		if !ok {
			log.GetLogger().Errorf("reserved instance channel is closed for: %s, traceID: %s", funcSvcID, traceID)
			return nil, snerror.New(snerror.FuncMetaNotFound, snerror.ErrText(snerror.FuncMetaNotFound))
		}
		if workerIns.PodName == "" {
			err := WorkerCache.GetMinScaleError(funcSvcID)
			if err != nil {
				log.GetLogger().Warnf("failed to wait reserved instance for: %s, traceID: %s, error: %s",
					funcSvcID, traceID, err.Error())
				return nil, err
			}
			// return getting pod error to make bus retry when pod name is empty and min scale error is nil
			return nil, snerror.New(snerror.GettingPodErrorCode, snerror.ErrText(snerror.GettingPodErrorCode))
		}
		log.GetLogger().Infof("succeed to wait reserved instance: %s for: %s, traceID: %s",
			workerIns.PodName, funcSvcID, traceID)
		return &workerIns, nil
	case <-ctx.Done():
		log.GetLogger().Warnf("function %s is updated or deleted with traceID %s", funcSvcID, traceID)
		return nil, utils.ErrCancelGeneralizePod
	}
}

// GetMinScaleError returns minScaleErr of function
func (w *workerCache) GetMinScaleError(funcSvcID string) error {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.RLock()
	defer fc.RUnlock()
	return fc.minScaleErr
}

// SetMinScaleError set last error returned by scaling reserved instance (maybe nil)
func (w *workerCache) SetMinScaleError(funcSvcID, crSignature string, err error) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	defer fc.Unlock()
	if fc.crSignature != crSignature {
		return
	}
	fc.minScaleErr = err
}

// BroadcastScaleResult broadcast scale result to all waiting requests
func (w *workerCache) BroadcastScaleResult(funcSvcID, crSignature string, err error) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	defer fc.Unlock()
	if fc.crSignature != crSignature {
		return
	}
	if err != nil {
		// broadcast an empty worker instance to all waiting requests
		fc.broadcastReservedIns(v1alpha1.WorkerInstance{})
		return
	}
	// select an instance when succeed to scale
	selectIns := v1alpha1.WorkerInstance{}
	for _, instance := range fc.instances {
		if instance.Applier == constants.WorkerManagerApplier {
			selectIns = instance
			break
		}
	}
	fc.broadcastReservedIns(selectIns)
}

// GetOrWaitReservedIns get existing instance or wait pulling reserved instance
func (w *workerCache) GetOrWaitReservedIns(ctx context.Context, funcSvcID string,
	requestInfo *config.RequestInfo) (*v1alpha1.WorkerInstance, error) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil, fmt.Errorf("failed to get function cache with %s", funcSvcID)
	}
	fc.RLock()

	var selectedIns *v1alpha1.WorkerInstance

	now := timeutil.NowUnixMillisecond()
	delta := config.GetRadicalRedirectTimeout().Milliseconds()
	if config.EnableRadicalAutoscaler() && now-fc.autoScaleTimestamp[requestInfo.DeployNode] < delta {
		for _, ins := range fc.instances {
			if now-fc.autoScaleTimestamp[ins.NodeName] > delta {
				selectedIns = &ins
				break
			}
		}
	}
	if selectedIns != nil {
		fc.RUnlock()
		log.GetLogger().Infof("after radical autoscaler get ins %s for %s, traceID: %s",
			selectedIns.InstanceID, funcSvcID, requestInfo.TraceID)
		return selectedIns, nil
	}

	if requestInfo.OwnedWorkerView.OwnedNum != 0 && fc.scalingReversed == 0 {
		fc.RUnlock()
		// scale up a normal instance to deal with request
		return nil, nil
	}
	// there is no instance in fc cache
	if fc.scalingReversed > 0 {
		fc.RUnlock()
		return waitScalingReservedIns(ctx, fc.reservedInsChan, funcSvcID, requestInfo.TraceID)
	}
	// select an instance from exist instance list
	for _, ins := range fc.instances {
		// prefer to select reserved instance
		if ins.Applier == constants.WorkerManagerApplier {
			selectedIns = &ins
			log.GetLogger().Infof("succeed to get reserved instance: %s for: %s, traceID: %s",
				selectedIns.InstanceID, funcSvcID, requestInfo.TraceID)
			break
		}
	}
	fc.RUnlock()

	// if selected instance is nil, it will return nil, nil and scale up a new normal instance
	return selectedIns, nil
}

// GetWorkerInstanceToDeleteByASBRes get one worker instance from worker record map
func (w *workerCache) GetWorkerInstanceToDeleteByASBRes(funcSvcID string) (v1alpha1.WorkerInstance, bool) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return v1alpha1.WorkerInstance{}, false
	}
	fc.Lock()
	defer fc.Unlock()
	var res = v1alpha1.WorkerInstance{}

	reduceID := pickWorkInstanceToBeReduced(fc)
	if len(reduceID) != 0 {
		res = fc.instances[reduceID]
		res.WillDelete = true
		fc.instances[reduceID] = res
	} else {
		log.GetLogger().Warnf("Failed to pick a valid ASBRes worker instance, funcSvcID: %s.", funcSvcID)
		return v1alpha1.WorkerInstance{}, false
	}
	return res, ok
}

// pick one CAE worker instance (elastic > other > fixed)
func pickWorkInstanceToBeReduced(fc *functionCache) string {
	var fixedInsID, otherInsID, elasticInsID string
	nodeCache.mux.RLock()
	defer nodeCache.mux.RUnlock()
	for k := range fc.instances {
		if fc.instances[k].Applier == constants.ASBResApplier && !fc.instances[k].WillDelete {
			node := nodeCache.NodeList[fc.instances[k].NodeName]
			if node == nil && len(otherInsID) == 0 {
				otherInsID = k
			} else if node.Label[utils.PoolNodeLabelKey] == utils.CAENodeAffinityValue && len(fixedInsID) == 0 {
				fixedInsID = k
			} else if node.Label[utils.PoolNodeLabelKey] == utils.CAENodeAntiAffinityValue && len(elasticInsID) == 0 {
				elasticInsID = k
			} else if len(otherInsID) == 0 {
				otherInsID = k
			}
		}
		if len(elasticInsID) != 0 {
			break
		}
	}
	if len(elasticInsID) != 0 {
		return elasticInsID
	}
	if len(otherInsID) != 0 {
		return otherInsID
	}
	return fixedInsID
}

// GetWorkerInstance get worker instance from worker record map
func (w *workerCache) GetWorkerInstance(funcSvcID, podName string) (v1alpha1.WorkerInstance, bool) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return v1alpha1.WorkerInstance{}, false
	}
	fc.RLock()
	res, ok := fc.instances[podName]
	fc.RUnlock()
	return res, ok
}

// GetFunctionPreStopHandler get function preStop handler from worker record map
func (w *workerCache) GetFunctionPreStopHandler(funcSvcID string) string {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return ""
	}

	fc.RLock()
	defer fc.RUnlock()
	if fc.funcMeta == nil {
		return ""
	}
	handler := fc.funcMeta.Spec.Configuration.PreStop.Handler
	return handler
}

// GetWorkerInstanceCount -
func (w *workerCache) GetWorkerInstanceCount() map[string]int {
	// key is FuncKey, value is Func account
	records := make(map[string]int, initCacheSize)
	w.functions.Range(func(key, value interface{}) bool {
		fc, ok := value.(*functionCache)
		if !ok || fc == nil || fc.funcMeta == nil {
			return true
		}
		fc.RLock()
		funcKey := utils.GetFuncKeyFromURN(fc.funcMeta.Spec.Configuration.FunctionVersionURN)
		if funcKey == "" {
			fc.RUnlock()
			return true
		}
		for _, instance := range fc.instances {
			if instance.Applier != constants.WorkerManagerApplier {
				records[funcKey]++
			}
		}
		fc.RUnlock()
		return true
	})
	return records
}

// StoreWorkerInstance -
func (w *workerCache) StoreWorkerInstance(funcSvcID string, instance v1alpha1.WorkerInstance) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		fc = w.createFunctionCache(funcSvcID)
	}
	fc.addInstance(instance.PodName, instance)
}

// DeleteWorkerInstance -
func (w *workerCache) DeleteWorkerInstance(funcSvcID, podName string, isReserved bool) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.deleteInstance(podName, isReserved)
}

// CheckReachedMaxInstance check whether the number of instances reaches the maximum
func (w *workerCache) CheckReachedMaxInstance(functionService *faasfunction.FunctionService,
	requestInfo *config.RequestInfo) bool {
	fc, ok := w.getFunctionCache(functionService.ID)
	if !ok {
		log.GetLogger().Warnf("failed to get function cache, func: %s with traceID: %s",
			functionService.ID, requestInfo.TraceID)
		return false
	}
	fc.Lock()
	if requestInfo.Applier == constants.WorkerManagerApplier {
		fc.increaseScalingNum(true)
		fc.Unlock()
		return false
	}
	curInstance := len(fc.instances)
	scalingInstance := fc.getScalingNum()
	curOnDemandInstance := fc.getCurOnDemandNum()
	scalingOnDemandInstance := fc.getScalingOnDemandNum()
	maxInstance := fc.maxScale
	if maxInstance < 0 {
		maxInstance = functionService.Spec.Configuration.AutoScalerConfig.MaximumScale
	}
	isFirstInstance := (int64(curInstance) + scalingInstance) == 0
	requestInfo.IsFirstInstance = isFirstInstance
	// only increase scalingNum when not reach max instance; and caller should call CheckMaxInstanceCallback to
	// decrease scalingNum after scaling
	fc.increaseScalingNum(requestInfo.Applier == constants.WorkerManagerApplier)
	if curOnDemandInstance+scalingOnDemandInstance >= maxInstance {
		fc.Unlock()
		log.GetLogger().Warnf("firstInstance: %t, curInstance: %d, scalingInstance: %d, maxInstance: %d for "+
			"func: %s with traceID: %s",
			isFirstInstance, curInstance, scalingInstance, maxInstance, functionService.ID, requestInfo.TraceID)
		return true
	}
	fc.Unlock()
	log.GetLogger().Debugf("firstInstance: %t, curInstance: %d, scalingInstance: %d, maxInstance: %d for "+
		"func: %s with traceID: %s",
		isFirstInstance, curInstance, scalingInstance+1, maxInstance, functionService.ID, requestInfo.TraceID)
	return false
}

// CheckMaxInstanceCallback is the callback of CheckReachedMaxInstance and need to be called when outer logic is done
func (w *workerCache) CheckMaxInstanceCallback(functionService *faasfunction.FunctionService, signature string,
	requestInfo *config.RequestInfo) {
	fc, ok := w.getFunctionCache(functionService.ID)
	if !ok {
		// normally triggered by function deletion
		log.GetLogger().Infof("decreasing scaling num but function not exists, func: %s", functionService.ID)
		return
	}
	fc.Lock()
	// should avoid decrease updated function
	if fc.crSignature == signature {
		fc.decreaseScalingNum(requestInfo.Applier == constants.WorkerManagerApplier)
	}
	fc.Unlock()
}

// CheckFunctionExist check if function ID exist
func (w *workerCache) CheckFunctionExist(targetFuncID string) bool {
	_, ok := w.getFunctionCache(targetFuncID)
	return ok
}

// GetMaxScale gets max scale
func (w *workerCache) GetMaxScale(functionName string) int64 {
	fc, ok := w.getFunctionCache(functionName)
	if !ok {
		log.GetLogger().Warnf("failed to get function cache, func: %s", functionName)
		return 0
	}
	fc.RLock()
	maxScale := fc.maxScale
	fc.RUnlock()
	return maxScale
}

// GetMinScale gets min scale
func (w *workerCache) GetMinScale(funcSvcID string) int64 {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get function cache, func: %s", funcSvcID)
		return 0
	}
	fc.RLock()
	minScale := fc.minScale
	fc.RUnlock()
	return minScale
}

// GetIdleMode get idle mode by funcSvcID
func (w *workerCache) GetIdleMode(funcSvcID string) bool {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return false
	}
	fc.RLock()
	idleMode := fc.idleMode
	fc.RUnlock()
	return idleMode
}

// GetProfile get profile meta by funcSvcID
func (w *workerCache) GetProfile(funcSvcID string) types.ProfileMetaData {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return types.ProfileMetaData{}
	}
	fc.RLock()
	profile := fc.profile
	fc.RUnlock()
	return profile
}

// GetReservedInsNodeDis get reserved instance antiAffinity node by funcSvcID and nodeList
func (w *workerCache) GetReservedInsNodeDis(funcSvcID string, nodeList []string, traceID string) []string {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.Lock()

	// precompute reserved node distribution in fc.instances and fc.reservedInsNodePreDis
	reversedDistributionInPoolNode := make(map[string]int, len(nodeList))

	// compute reserved node distribution in fc.instances
	for _, instance := range fc.instances {
		if instance.Applier == constants.WorkerManagerApplier {
			reversedDistributionInPoolNode[instance.NodeName]++
		}
	}

	for _, nodeName := range nodeList {
		reversedDistributionInPoolNode[nodeName] = fc.reservedInsNodePreDis[nodeName] +
			reversedDistributionInPoolNode[nodeName]
	}
	for _, nodeName := range nodeList {
		distributionTimes, ok := reversedDistributionInPoolNode[nodeName]
		if !ok || distributionTimes == 0 {
			fc.reservedInsNodePreDis[nodeName]++
			fc.Unlock()
			return []string{nodeName}
		}
	}

	fc.Unlock()
	return []string{}
}

// GetScheduleLabelNodes -
func (w *workerCache) GetScheduleLabelNodes(funcSvcID string) PodScheduleInfo {
	var podSchedulerInfo PodScheduleInfo
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return podSchedulerInfo
	}
	fc.RLock()
	podSchedulerInfo = fc.funcScheduleNodes
	fc.RUnlock()
	return podSchedulerInfo
}

// DeleteReservedInsNodeDis delete fcCache reservedInsNodePreDis allocation times
func (w *workerCache) DeleteReservedInsNodeDis(funcSvcID string, nodeName string) error {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.Lock()
	for nodeNameInMap, preAllocateTimes := range fc.reservedInsNodePreDis {
		if nodeNameInMap == nodeName && preAllocateTimes > 0 {
			preAllocateTimes--
		}
	}
	fc.Unlock()
	return nil
}

// GetInstanceList get worker record
func (w *workerCache) GetInstanceList(functionName string) []v1alpha1.WorkerInstance {
	fc, ok := w.getFunctionCache(functionName)
	if !ok {
		return nil
	}
	fc.RLock()
	instanceList := make([]v1alpha1.WorkerInstance, 0, len(fc.instances))
	for _, instance := range fc.instances {
		instanceList = append(instanceList, instance)
	}
	fc.RUnlock()
	return instanceList
}

// GetCAEBusInstance get cae bus intance
func (w *workerCache) GetCAEBusInstance(funcSvcID string) *corev1.Pod {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.RLock()
	defer fc.RUnlock()
	for _, instance := range fc.instances {
		if !strings.Contains(instance.Applier, "functiontask") {
			continue
		}
		labels := make(map[string]string)
		labels["serviceID"] = instance.Service
		if instance.IsDirectFunc {
			labels[utils.LabelDirect] = utils.TrueString
		}
		pod := &corev1.Pod{
			Status: corev1.PodStatus{
				PodIP:  instance.IP,
				HostIP: instance.NodeIP,
			},
			Spec: corev1.PodSpec{
				NodeName: instance.NodeName,
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:   instance.PodName,
				Labels: labels,
			},
		}
		return pod
	}
	return nil
}

// RemovePrewarmInstances -
func (w *workerCache) RemovePrewarmInstances(functionName string, podName string) bool {
	fc, ok := w.getFunctionCache(functionName)
	if !ok {
		return false
	}
	fc.Lock()
	defer fc.Unlock()
	return w.removePrewarmInstances(fc, podName)
}

func (w *workerCache) removePrewarmInstances(fc *functionCache, podName string) bool {
	for i, pod := range fc.prewarmPool {
		if pod.Name == podName {
			fc.prewarmPool = append(fc.prewarmPool[:i], fc.prewarmPool[i+1:]...)
			return true
		}
	}
	return false
}

// GetPrewarmPod -
func (w *workerCache) GetPrewarmPod(funcSvcID, podName string) (*corev1.Pod, bool) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil, false
	}
	fc.RLock()
	pod, ok := fc.getPrewarmPod(podName)
	fc.RUnlock()
	return pod, ok
}

func (fc *functionCache) getPrewarmPod(podName string) (*corev1.Pod, bool) {
	for i := range fc.prewarmPool {
		if fc.prewarmPool[i].Name == podName {
			return fc.prewarmPool[i], true
		}
	}
	return nil, false
}

// GetWorkerInsOrPrewarmPod -
func (w *workerCache) GetWorkerInsOrPrewarmPod(funcSvcID, podName string) (v1alpha1.WorkerInstance, bool) {
	workerIns, workerInsOK := w.GetWorkerInstance(funcSvcID, podName)
	if workerInsOK {
		return workerIns, true
	}
	// there is no worker instance, it will find in preWarm pool by pod name
	preWarmPod, preWarmPodOK := w.GetPrewarmPod(funcSvcID, podName)
	if preWarmPodOK {
		return utils.MakeWorkerInsInfo(preWarmPod), true
	}
	return v1alpha1.WorkerInstance{}, false
}

type functionCacheHandle func(fc *functionCache)

// IncreaseScalingPrewarmNum -
func (w *workerCache) IncreaseScalingPrewarmNum(funcSvcID string) {
	w.processFunctionCacheWithLock(funcSvcID, func(fc *functionCache) {
		fc.scalingPrewarm++
	})
}

func (w *workerCache) processFunctionCacheWithLock(funcSvcID string, fn functionCacheHandle) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get function cache, funcSvcID: %s", funcSvcID)
		return
	}
	fc.Lock()
	fn(fc)
	fc.Unlock()
}

// DecreaseScalingPrewarmNum -
func (w *workerCache) DecreaseScalingPrewarmNum(funcSvcID string) {
	w.processFunctionCacheWithLock(funcSvcID, func(fc *functionCache) {
		fc.scalingPrewarm--
	})
}

// AddPrewarmInstances -
func (w *workerCache) AddPrewarmInstances(funcSvcID string, pod *corev1.Pod) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get function cache, funcSvcID: %s", funcSvcID)
		return
	}
	fc.Lock()
	fc.prewarmPool = append(fc.prewarmPool, pod)
	log.GetLogger().Infof("add prewarm pod: %s, funcSvcID: %s, prewarm len is: %d", pod.Name, funcSvcID,
		len(fc.prewarmPool))
	fc.Unlock()
}

// UpdatePrewarmInstancesIfNotExist -
func (w *workerCache) UpdatePrewarmInstancesIfNotExist(funcSvcID string, pod *corev1.Pod) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Errorf("failed to get function cache, funcSvcID: %s", funcSvcID)
		return
	}
	fc.Lock()
	isExist := false
	for index, p := range fc.prewarmPool {
		if p.Name == pod.Name {
			fc.prewarmPool[index] = pod
			isExist = true
		}
	}
	if !isExist {
		fc.prewarmPool = append(fc.prewarmPool, pod)
		log.GetLogger().Infof("add discover prewarm pod: %s, funcSvcID: %s, prewarm len is: %d", pod.Name,
			funcSvcID, len(fc.prewarmPool))
	}
	fc.Unlock()
}

// GetInstanceMap get worker record
func (w *workerCache) GetInstanceMap(funcSvcName string) map[string]v1alpha1.WorkerInstance {
	fc, ok := w.getFunctionCache(funcSvcName)
	if !ok {
		return nil
	}
	fc.RLock()
	newMap := make(map[string]v1alpha1.WorkerInstance, len(fc.instances))
	for _, instance := range fc.instances {
		newMap[instance.InstanceID] = instance
	}
	fc.RUnlock()
	return newMap
}

// GetWorkerRecords gets function worker number
func (w *workerCache) GetWorkerRecords() int {
	recordNum := 0
	w.functions.Range(func(key, value interface{}) bool {
		fc, ok := value.(*functionCache)
		if !ok {
			return true
		}
		fc.RLock()
		recordNum += len(fc.instances)
		fc.RUnlock()
		return true
	})
	return recordNum
}

// GetReservedInstanceRecords gets all function reserved instance number
func (w *workerCache) GetReservedInstanceRecords() map[string]int {
	records := make(map[string]int, initCacheSize)
	w.functions.Range(func(key, value interface{}) bool {
		fc, ok := value.(*functionCache)
		if !ok || fc == nil || fc.funcMeta == nil {
			return true
		}
		fc.RLock()
		urn := fc.funcMeta.Spec.Configuration.FunctionVersionURN
		for _, instance := range fc.instances {
			if instance.Applier == constants.WorkerManagerApplier {
				records[urn]++
			}
		}
		fc.RUnlock()
		return true
	})
	return records
}

// GetReservedInstanceMap gets all function reserved instance number
func (w *workerCache) GetReservedInstanceMap() map[string]int {
	// key is FuncKey, value is Func account
	records := make(map[string]int, initCacheSize)
	w.functions.Range(func(key, value interface{}) bool {
		fc, ok := value.(*functionCache)
		if !ok || fc == nil || fc.funcMeta == nil {
			return true
		}
		fc.RLock()
		funcKey := utils.GetFuncKeyFromURN(fc.funcMeta.Spec.Configuration.FunctionVersionURN)
		if funcKey == "" {
			fc.RUnlock()
			return true
		}
		for _, instance := range fc.instances {
			if instance.Applier == constants.WorkerManagerApplier {
				records[funcKey]++
			}
		}
		fc.RUnlock()
		return true
	})
	return records
}

// GetFuncMeta get functionMeta by funcSvcID
func (w *workerCache) GetFuncMeta(funcSvcID string) *faasfunction.FunctionService {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.RLock()
	funcMeta := fc.funcMeta
	fc.RUnlock()
	return funcMeta
}

// UpdateFuncScheduleNodes update func schedule nodes with node labels
func (w *workerCache) UpdateFuncScheduleNodes(node *corev1.Node) {
	w.functions.Range(func(key, value interface{}) bool {
		fc, ok := value.(*functionCache)
		if !ok {
			return true
		}
		if fc == nil || fc.funcMeta == nil {
			log.GetLogger().Warnf("fc is nil or funcMeta is nil")
			return true
		}
		fc.Lock()
		fgsTags := fc.funcMeta.Spec.Configuration.FgsTags
		curSchInfo := fc.funcScheduleNodes
		podScheduleInfo := updatePodScheduleInfoByNodeEvent(fgsTags, node, curSchInfo)
		log.GetLogger().Debugf("funcName %s, podScheduleInfo %v, fgsTags %v",
			fc.funcMeta.Spec.Configuration.FunctionName, podScheduleInfo, fgsTags)
		fc.funcScheduleNodes = podScheduleInfo
		fc.Unlock()
		return true
	})
	return
}

// UpdateAllFuncDNSDomain -
func UpdateAllFuncDNSDomain(stopCh <-chan struct{}, interval time.Duration) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop chan is nil")
		return
	}
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return
	}
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-ticker.C:
			WorkerCache.UpdateDNSDomainNames(cfg, interval)
		case _, ok := <-stopCh:
			if !ok {
				ticker.Stop()
				return
			}
		}
	}
}

// UpdateDNSDomainNames update dns domain names for all functions
func (w *workerCache) UpdateDNSDomainNames(cfg *config.FaasConfig, interval time.Duration) {
	updatedDNSNum := 0
	log.GetLogger().Infof("begin to update dns domain names by interval: %v", interval)
	domainKeyMap := utils.GetDNSCache().GetDNSDomainKeyMap()
	if len(domainKeyMap) == 0 {
		log.GetLogger().Infof("finished to update dns domain names by "+
			"interval with updated dns nums: %d", updatedDNSNum)
		return
	}
	for domainKey, funcSvcID := range domainKeyMap {
		fc, ok := w.getFunctionCache(funcSvcID)
		if !ok {
			log.GetLogger().Warnf("failed to get function cache for func: %s,"+
				" domainKey: %s", funcSvcID, domainKey)
			continue
		}
		if fc.dnsDomainCfg == nil && fc.endpointDomainCfg == nil {
			log.GetLogger().Warnf("failed to get dns domain config for func: %s, "+
				"domainKey: %s", funcSvcID, domainKey)
			continue
		}
		list := strings.Split(domainKey, utils.SplicingCharacter)
		if len(list) != domainKeyLength {
			log.GetLogger().Errorf("failed to get dns domainNames with invalid domainKey: %s", domainKey)
			continue
		}
		vpcID := list[vpcIDIndex]
		xRole, userAgency := fc.getXRoleOrAgency(vpcID)
		if xRole == "" && userAgency.Token == "" {
			log.GetLogger().Errorf("failed to get dns userAgency or xrole for func: %s,"+
				" domainKey: %s", funcSvcID, domainKey)
			continue
		}
		if dnsDomainMap, ok := fc.UpdateDNSInfo(vpcID, list[domainNameIndex], xRole, cfg, userAgency); ok {
			utils.GetDNSCache().UpdateDNSDomainNames(vpcID, list[domainNameIndex], dnsDomainMap)
			updatedDNSNum++
		}
	}
	log.GetLogger().Infof("finished to update dns domain names"+
		" by interval with updated dns nums: %d", updatedDNSNum)
}

func (w *workerCache) getFunctionMap() map[string][]*functionCache {
	funcTenantMap := make(map[string][]*functionCache, initCacheSize)
	f := func(k, v interface{}) bool {
		fc, ok := v.(*functionCache)
		if !ok {
			return true
		}
		fc.Lock()
		tenantID := fc.info.TenantID
		if fcList, ok := funcTenantMap[tenantID]; !ok {
			initList := make([]*functionCache, 0, initCacheSize)
			initList = append(initList, fc)
			funcTenantMap[tenantID] = initList
		} else {
			fcList = append(fcList, fc)
			funcTenantMap[tenantID] = fcList
		}
		fc.Unlock()
		return true
	}
	w.functions.Range(f)
	return funcTenantMap
}

// takeOutPrewarmPod take from prewarm pool
func (fc *functionCache) takeOutPrewarmPod(num int) []*corev1.Pod {
	if num <= 0 {
		return nil
	}
	var pods []*corev1.Pod
	prewarmPoolLen := len(fc.prewarmPool)
	log.GetLogger().Infof("current prewarm pod len is: %d, scale down num is: %d", prewarmPoolLen, num)
	if len(fc.prewarmPool) > num {
		pods = fc.prewarmPool[prewarmPoolLen-num : prewarmPoolLen]
		fc.prewarmPool = fc.prewarmPool[0 : prewarmPoolLen-num]
		return pods
	}
	pods = fc.prewarmPool
	fc.prewarmPool = nil
	return pods
}

// GetPodFromPrewarmPool get pod from prewarm pool
func (w *workerCache) GetPodFromPrewarmPool(funcSvcID string) *corev1.Pod {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.Lock()
	defer fc.Unlock()
	if len(fc.prewarmPool) == 0 {
		return nil
	}

	pod := fc.prewarmPool[0]
	fc.prewarmPool = fc.prewarmPool[1:len(fc.prewarmPool)]
	return pod
}

// GetUnpredictedPods -
func (w *workerCache) GetUnpredictedPods(predictionMap map[string]*config.PredictionInfo) []*corev1.Pod {
	var pods []*corev1.Pod
	w.functions.Range(func(key, value interface{}) bool {
		fc, ok := value.(*functionCache)
		if !ok {
			return true
		}
		fc.Lock()
		defer fc.Unlock()
		if fc.predictionNum <= 0 {
			return true
		}
		if fc.funcMeta == nil {
			return true
		}
		configuration := fc.funcMeta.Spec.Configuration
		redisKey := fmt.Sprintf("%s/%s/%s/%s", configuration.BusinessID, configuration.TenantID,
			configuration.FunctionName, configuration.FunctionVersion)
		// the function is not available for this prediction
		if _, exist := predictionMap[redisKey]; !exist {
			fc.predictionNum = 0
			if len(fc.prewarmPool) != 0 {
				pods = append(pods, fc.prewarmPool...)
				fc.prewarmPool = nil
			}
		}
		return true
	})
	return pods
}

// SetFuncVpcPats -
func (w *workerCache) SetFuncVpcPats(funcSvcID string, patPods []types.NATConfigure) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	fc.funcVpcPats = patPods
	fc.Unlock()
}

// GetFuncVpcPats -
func (w *workerCache) GetFuncVpcPats(funcSvcID string) []types.NATConfigure {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.RLock()
	patPods := fc.funcVpcPats
	fc.RUnlock()
	return patPods
}

// GetCustomImgSize -
func (w *workerCache) GetCustomImgSize(funcSvcID string) int64 {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return 0
	}
	fc.RLock()
	customImageSize := fc.CustomImgSize
	fc.RUnlock()
	return customImageSize
}

// SetEndpointVpcPats -
func (w *workerCache) SetEndpointVpcPats(funcSvcID string, patPods []types.NATConfigure) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	fc.endPointVpcPats = patPods
	fc.Unlock()
}

// SetPublicVpcPats -
func (w *workerCache) SetPublicVpcPats(funcSvcID string, patPods []types.NATConfigure) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	fc.publicVpcPats = patPods
	fc.Unlock()
}

// GetEndpointVpcPats -
func (w *workerCache) GetEndpointVpcPats(funcSvcID string) []types.NATConfigure {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.RLock()
	patPods := fc.endPointVpcPats
	fc.RUnlock()
	return patPods
}

// GetPublicVpcPats -
func (w *workerCache) GetPublicVpcPats(funcSvcID string) []types.NATConfigure {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.RLock()
	patPods := fc.publicVpcPats
	fc.RUnlock()
	return patPods
}

// GetInstanceNum -
func (w *workerCache) GetInstanceNum(funcSvcID string) (int64, int64) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return 0, 0
	}
	fc.RLock()
	curNum := len(fc.instances)
	scalingNum := fc.scalingNum
	fc.RUnlock()
	return int64(curNum), scalingNum
}

// IsAllowed -
func (w *workerCache) IsAllowed(funcSvcID string) bool {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok || fc == nil {
		// no breaker, all specialized requests are allowed
		return true
	}
	return fc.isAllowed()
}

// Fail -
func (w *workerCache) Fail(funcSvcID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok || fc == nil {
		// no breaker, do noting
		return
	}
	fc.fail()
}

// Succeed -
func (w *workerCache) Succeed(funcSvcID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok || fc == nil {
		// no breaker, do noting
		return
	}
	fc.succeed()
}

// RemoveStateIDFromFuncCache -
func (w *workerCache) RemoveStateIDFromFuncCache(funcSvcID, stateID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	defer fc.Unlock()
	fc.stateMap.Delete(stateID)
	return
}

func (fc *functionCache) delHashKeyInMap(stateID string) {
	fc.stateFulEventCheckRepeat.Delete(stateID)
	return
}

// InsertHashKeyInMap -
func (w *workerCache) InsertHashKeyInMap(funcSvcID, podName, stateID string) {
	if stateID == "" {
		return
	}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.stateFulEventCheckRepeat.Store(stateID, podName)
	return
}

// CheckHashKeyInMap -
func (fc *functionCache) CheckHashKeyInMap(stateID, podName string) bool {
	name, ok := fc.stateFulEventCheckRepeat.Load(stateID)
	if !ok {
		return false
	}
	name, ok = name.(string)
	if !ok {
		return false
	}
	return name == podName
}

// CheckAndInsertStateIDMaxInstance -
func (w *workerCache) CheckAndInsertStateIDMaxInstance(funcSvcID, stateID string) bool {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return false
	}
	fc.Lock()
	defer fc.Unlock()
	if !fc.checkStateIDMaxInstance(stateID) {
		return fc.insertStateIDMap(stateID)
	}
	return false
}

func (w *workerCache) GetExistedStatePod(funcSvcID, stateID string) *corev1.Pod {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return nil
	}
	fc.RLock()
	defer fc.RUnlock()
	for _, instance := range fc.instances {
		if instance.StateID == stateID {
			pod := &corev1.Pod{
				Spec: corev1.PodSpec{
					NodeName: instance.NodeName,
				},
				Status: corev1.PodStatus{
					HostIP: instance.NodeIP,
					PodIP:  instance.IP,
				},
				ObjectMeta: metav1.ObjectMeta{Name: instance.PodName},
			}
			return pod
		}
	}
	return nil
}

func (w *workerCache) CheckStatefulReloading(funcSvcID, stateID string) bool {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return false
	}
	_, ok = fc.stateReloading.Load(stateID)
	if ok {
		return true
	}
	return false
}

func (w *workerCache) DeleteReloadDoneState(funcSvcID, stateID string) {
	if stateID == "" {
		return
	}
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.stateReloading.Delete(stateID)
}

// ResetStateIDMap -
func (w *workerCache) ResetStateIDMap(funcSvcID, stateID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return
	}
	fc.Lock()
	defer fc.Unlock()
	fc.resetStateIDMap(stateID)
	return
}

func (fc *functionCache) insertStateIDMap(stateID string) bool {
	val, ok := fc.stateMap.Load(stateID)
	if !ok {
		fc.stateMap.Store(stateID, 1)
		return true
	}
	intVal, ok := val.(int)
	if !ok {
		return false
	}
	if intVal == 0 {
		fc.stateMap.Store(stateID, 1)
		return true
	}
	return false
}

func (fc *functionCache) resetStateIDMap(stateID string) {
	if _, ok := fc.stateMap.Load(stateID); !ok {
		return
	}
	fc.stateMap.Store(stateID, 0)
	return
}

func (fc *functionCache) checkStateIDKeyExist(stateID string) bool {
	_, ok := fc.stateMap.Load(stateID)
	return ok
}

func (fc *functionCache) checkStateIDMaxInstance(stateID string) bool {
	val, ok := fc.stateMap.Load(stateID)
	if !ok {
		return false
	}
	intVal, ok := val.(int)
	if !ok {
		return false
	}
	if intVal == 0 {
		return false
	}
	return true
}

func (fc *functionCache) storeReloadingState(stateID string) {
	if stateID == "" {
		return
	}
	fc.stateReloading.Store(stateID, 0)
}

// sort reserved instance array,
func (fc *functionCache) sortWorkerInstance(reserved []v1alpha1.WorkerInstance) []v1alpha1.WorkerInstance {
	if fc.funcMeta == nil || !commonutils.IsCAEFunc(fc.funcMeta.Spec.Configuration.BusinessType) {
		return reserved
	}
	fixedReserved := make([]v1alpha1.WorkerInstance, 0, len(reserved))
	elasticReserved := make([]v1alpha1.WorkerInstance, 0, len(reserved))
	otherReserved := make([]v1alpha1.WorkerInstance, 0, len(reserved))
	nodeCache.mux.RLock()
	defer nodeCache.mux.RUnlock()
	for i := 0; i < len(reserved); i++ {
		node := nodeCache.NodeList[reserved[i].NodeName]
		if node == nil {
			otherReserved = append(otherReserved, reserved[i])
			continue
		}
		if node.Label[utils.PoolNodeLabelKey] == utils.CAENodeAffinityValue {
			fixedReserved = append(fixedReserved, reserved[i])
			continue
		}
		if node.Label[utils.PoolNodeLabelKey] == utils.CAENodeAntiAffinityValue {
			elasticReserved = append(elasticReserved, reserved[i])
			continue
		}
		otherReserved = append(otherReserved, reserved[i])
	}
	elasticReserved = append(elasticReserved, otherReserved...)
	elasticReserved = append(elasticReserved, fixedReserved...)
	return elasticReserved
}

func (fc *functionCache) reduceRollingWhenDisabled() []v1alpha1.WorkerInstance {
	ret := make([]v1alpha1.WorkerInstance, 0)
	for _, v := range fc.rollingInstances {
		if fc.minScale == 0 && v.Applier == constants.WorkerManagerApplier {
			ret = append(ret, v)
		} else if fc.maxScale == 0 && v.Applier != constants.WorkerManagerApplier {
			ret = append(ret, v)
		}
	}
	return ret
}

// SetFuncExpectedNum -
func (w *workerCache) SetFuncExpectedNum(funcSvcID string, expectNum int) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get function cache, funcSvcID: %s", funcSvcID)
		return
	}
	fc.Lock()
	fc.expectedNum = expectNum
	fc.Unlock()
}

// DecreaseFuncExpectedNum -
func (w *workerCache) DecreaseFuncExpectedNum(funcSvcID string) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		log.GetLogger().Warnf("failed to get function cache, funcSvcID: %s", funcSvcID)
		return
	}
	fc.Lock()
	log.GetLogger().Infof("decrease expectedNum, fc.expectedNum %d, funcSvcID %s", fc.expectedNum, funcSvcID)
	if fc.expectedNum > int(fc.minScale) {
		fc.expectedNum--
	}
	fc.Unlock()
}

// GetFuncExpectedNum -
func (w *workerCache) GetFuncExpectedNum(funcSvcID string) (int, error) {
	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return 0, fmt.Errorf("failed to get function cache, funcSvcID: %s", funcSvcID)
	}
	fc.Lock()
	defer fc.Unlock()
	return fc.expectedNum, nil
}

func routerEtcdRestartCallback(etcdClient *etcd3.EtcdClient) {
	if !HasSynced {
		log.GetLogger().Warnf("current worker-manager cache hasn't synced, do nothing")
		return
	}
	etcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := etcdClient.GetResponse(etcdCtx, "/sn/workers/business", clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("routerEtcdRestartCallback failed: %s", err.Error())
		return
	}
	ops := make([]clientv3.Op, 0)
	localInstanceMap := WorkerCache.GetWorkerInstanceMap()
	logWorkerInstanceInfo(localInstanceMap)
	remoteInstanceMap := make(map[string]string)
	for _, kv := range resp.Kvs {
		if utils.IsCurClusterRouteInfo(kv.Value) {
			remoteInstanceMap[string(kv.Key)] = string(kv.Value)
		}
	}
	ops = judgeDeletedRouteInfo(remoteInstanceMap, localInstanceMap, ops)
	ops = judgeUpdatedRouteInfo(localInstanceMap, remoteInstanceMap, ops)
	etcdCtx = etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	err = etcdClient.Txn(etcdCtx, ops...)
	if err != nil {
		log.GetLogger().Errorf("routerEtcdRestartCallback failed: %s", err)
	}
}

func judgeUpdatedRouteInfo(localInstanceMap map[string]*v1alpha1.WorkerInstance, remoteInstanceMap map[string]string,
	ops []clientv3.Op) []clientv3.Op {
	for key, value := range localInstanceMap {
		val := remoteInstanceMap[key]
		if val == "" {
			jsonBytes, err := json.Marshal(value)
			if err != nil {
				log.GetLogger().Warnf("routerEtcdRestartCallback failed: %s, ignore it", err.Error())
				continue
			}
			ops = append(ops, clientv3.OpPut(key, string(jsonBytes)))
			continue
		}
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			log.GetLogger().Warnf("routerEtcdRestartCallback failed: %s, ignore it", err.Error())
			continue
		}
		if val != string(jsonBytes) {
			ops = append(ops, clientv3.OpPut(key, string(jsonBytes)))
		}
	}
	return ops
}

func judgeDeletedRouteInfo(remoteInstanceMap map[string]string, localInstanceMap map[string]*v1alpha1.WorkerInstance,
	ops []clientv3.Op) []clientv3.Op {
	for key := range remoteInstanceMap {
		val := localInstanceMap[key]
		if val == nil {
			ops = append(ops, clientv3.OpDelete(key))
			continue
		}
	}
	return ops
}

func logWorkerInstanceInfo(localInstanceMap map[string]*v1alpha1.WorkerInstance) {
	if logconfig.LogLevel == zapcore.DebugLevel {
		for k, v := range localInstanceMap {
			log.GetLogger().Debugf("cached worker instance:key %s -> value: %v", k, v)
		}
	}
}

// RegisterRouteEtcdRestartCallback register route etcd restart callback
func RegisterRouteEtcdRestartCallback() {
	etcd3.RegisterRouteEtcdRestartCallback(routerEtcdRestartCallback)
}

// UpdateStateWorkerList query from etcd and update stateworker list if the stateID is in this cluster
func UpdateStateWorkerList(stateID string) (bool, error) {
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		log.GetLogger().Warnf("failed to init state ETCD client")
		return false, errors.New("failed to init state ETCD client")
	}
	stateWorkerKey := etcdkey.GetStateETCDPath(stateID)
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	values, err := stateClient.GetValues(defaultEtcdCtx, stateWorkerKey)
	if err != nil {
		return false, err
	}
	if len(values) == 0 {
		return false, errors.New("stateID does not exist in state ETCD")
	}
	stateIDFuncMeta := StateMetadata{}
	if err = stateIDFuncMeta.parseStateWorkerValue([]byte(values[0]), stateID); err != nil {
		return false, err
	}
	if stateIDFuncMeta.Cluster == utils.GetEnvClusterID() {
		StateWorkersList.Store(stateID, stateIDFuncMeta)
		log.GetLogger().Infof("Store stateID: %s in StateWorkersList", stateID)
		return true, nil
	}
	log.GetLogger().Warnf("StateID: %s exists in cluster: %s", stateID, stateIDFuncMeta.Cluster)
	return false, nil
}

func (fc *functionCache) watchEvent(ctx context.Context, imageName string) {
	timeoutCtx, cancel := context.WithTimeout(ctx, pullImageTimeout*time.Minute)
	defer cancel()
	msgChan, errChan := fc.dockerClient.Event(timeoutCtx)
	if msgChan == nil || errChan == nil {
		log.GetLogger().Warnf("failed to get docker event chan, func: %s", fc.funcSvcID)
		return
	}
	log.GetLogger().Infof("start watch docker event, func: %s", fc.funcSvcID)
	for {
		select {
		case msg := <-msgChan:
			if msg.Type == "image" && msg.Action == "pull" && msg.Actor.ID == imageName {
				image, _, err := fc.dockerClient.ImageInspectWithRaw(timeoutCtx, imageName)
				if err != nil {
					log.GetLogger().Warnf("failed to inspect docker image, func:%s, err: %s", fc.funcSvcID,
						err.Error())
					return
				}
				fc.CustomImgSize = image.Size
				log.GetLogger().Infof("image size computes completed, func:%s, imageName: %s, size: %d",
					fc.funcSvcID, imageName, fc.CustomImgSize)
				_, err = fc.dockerClient.ImageRemove(timeoutCtx, imageName, dockertypes.ImageRemoveOptions{})
				if err != nil {
					log.GetLogger().Warnf("failed to delete custom image, func: %s, imageName: %s, err: %s",
						fc.funcSvcID, imageName, err.Error())
				}
				etcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
				err = etcd3.GetMetaEtcdClient().Put(etcdCtx, getETCDCustomImageSizeKey(fc.funcMeta),
					strconv.FormatInt(fc.CustomImgSize, base))
				if err != nil {
					log.GetLogger().Warnf("failed to put docker image size to etcd, func:%s, err: %s",
						fc.funcSvcID, err.Error())
				}
				return
			}
		case err := <-errChan:
			log.GetLogger().Warnf("receive docker event error: %s, func: %s", err.Error(), fc.funcSvcID)
			return
		case <-timeoutCtx.Done():
			log.GetLogger().Warnf("watch event timeout or cancel, func: %s", fc.funcSvcID)
			return
		}
	}
}

func (fc *functionCache) getAndSetImageSize(imageName string, s *faasfunction.FunctionService) {
	if fc.dockerClient == nil {
		cli, err := engine.NewTCPDockerClient(imgDownloadNodeIP)
		if err != nil {
			log.GetLogger().Warnf("get download image docker client failed, func: %s, err: %s", fc.funcSvcID,
				err.Error())
			return
		}
		fc.dockerClient = cli
	}
	defer fc.dockerClient.Close()

	encodingAuth, err := getSWRLoginAuth(context.TODO(), s, fc.funcSvcID)
	if err != nil {
		log.GetLogger().Warnf("failed to get swr login auth, func: %s, err: %s", fc.funcSvcID, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), pullImageTimeout*time.Minute)
	defer cancel()
	err = fc.dockerClient.PullPrivateImage(ctx, imageName, encodingAuth)
	if err != nil {
		log.GetLogger().Warnf("failed to pull images, func: %s, err: %s", fc.funcSvcID, err.Error())
		return
	}
	fc.watchEvent(context.Background(), imageName)
}

func (fc *functionCache) recoverCustomImageSize() {
	if etcd3.GetMetaEtcdClient().Client == nil {
		log.GetLogger().Warnf("failed to init meta ETCD client when recovering custom image size, func: %s",
			fc.funcSvcID)
		return
	}
	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	response, err := etcd3.GetMetaEtcdClient().GetResponse(ctx, getETCDCustomImageSizeKey(fc.funcMeta))
	if err != nil || len(response.Kvs) == 0 {
		return
	}
	size, err := strconv.ParseInt(string(response.Kvs[0].Value), base, bitSize)
	if err != nil {
		log.GetLogger().Warnf("invalid image size: %s, func:%s, err: %s", size, fc.funcSvcID, err.Error())
		return
	}
	fc.CustomImgSize = size
	log.GetLogger().Infof("success to recover custom image size: %d, func: %s", size, fc.funcSvcID)
}

func (fc *functionCache) broadcastReservedIns(instance v1alpha1.WorkerInstance) {
	if fc.reservedInsChan == nil {
		return
	}
	// broadcast to all waiting requests
	for {
		// push reserved instance to channel non-blocking
		select {
		case fc.reservedInsChan <- instance:
			log.GetLogger().Infof("broadcast pod: %s to reservedInsChan for: %s", instance.PodName, fc.funcSvcID)
		default:
			// no waiting request
			return
		}
	}
}

func getSWRLoginAuth(ctx context.Context, s *faasfunction.FunctionService, funcSvcID string) (
	string, error) {
	agencyID := s.Spec.Configuration.Role.XRole
	if s.Spec.Configuration.Role.AppXRole != "" {
		agencyID = s.Spec.Configuration.Role.AppXRole
	}
	agency := utils.GetUserAgencyFromCacheMap(s.Spec.Configuration.TenantID, s.Spec.Configuration.DomainID, agencyID)
	rawDecryptedToken, err := localauth.Decrypt(agency.Token)
	if err != nil {
		log.GetLogger().Warnf("failed to decrypt agency token: %s, func: %s", err.Error(), funcSvcID)
		return "", err
	}
	decryptedToken := string(rawDecryptedToken)
	commonutils.ClearByteMemory(rawDecryptedToken)
	secretPath := fmt.Sprintf("v2/manage/utils/secret?projectname=%s", s.Spec.Configuration.ProjectName)
	swrURL := fmt.Sprintf("https://%s/%s", config.GetSWRAPI(), secretPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, swrURL, nil)
	if err != nil {
		commonutils.ClearStringMemory(decryptedToken)
		log.GetLogger().Warnf("getSWRLoginAuth failed to new request: %s, func: %s", err.Error(), funcSvcID)
		return "", err
	}
	req.Header.Set(constants.HeaderAuthToken, decryptedToken)
	resp, err := httpclient.Do(req)
	commonutils.ClearStringMemory(decryptedToken)
	if err != nil {
		log.GetLogger().Warnf("getSWRLoginAuth failed to request swr with url %s: %s, func: %s", swrURL,
			err.Error(), funcSvcID)
		return "", err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.GetLogger().Warnf("failed to close response body: %s, func: %s", err.Error(), funcSvcID)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.GetLogger().Errorf("failed to read swr response body with status %d, %s, func: %s",
				resp.StatusCode, err.Error(), funcSvcID)
			return "", err
		}
		log.GetLogger().Warnf("failed to get swr loginAuth, func: %s, statusCode: %d, err: %s",
			funcSvcID, resp.StatusCode, string(data))
		return "", errors.New(string(data))
	}
	jsonData, err := parseSwrDockerLogin(resp)
	if err != nil {
		log.GetLogger().Warnf("failed to parse X-Swr-Dockerlogin, func: %s, err: %s", funcSvcID, err.Error())
		return "", err
	}
	return base64.StdEncoding.EncodeToString(jsonData), nil
}

func parseSwrDockerLogin(resp *http.Response) ([]byte, error) {
	// docker login -u cn-north-6@*****CCWJLPQ7SKMA1R8 -p 998025c6e53180360f79fb95429****************9cb10535b68bbfb573334
	loginStr := resp.Header.Get("X-Swr-Dockerlogin")
	if loginStr == "" {
		return nil, errors.New("swr response header X-Swr-Dockerlogin is empty")
	}
	args := strings.Split(loginStr, " -u ")
	if len(args) < dockerLoginCmdLength {
		return nil, errors.New("failed to split swr response header X-Swr-Dockerlogin")
	}
	args = strings.Split(args[1], " -p ")
	if len(args) < dockerLoginCmdLength {
		return nil, errors.New("failed to split swr response header X-Swr-Dockerlogin")
	}
	username := args[0]
	password := args[1]
	commonutils.ClearStringMemory(loginStr)

	authConfig := dockertypes.AuthConfig{
		Username: username,
		Password: password,
	}

	jsonData, err := json.Marshal(authConfig)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal docker authConfig, err: %s", err.Error())
		return nil, err
	}
	return jsonData, nil
}

func getETCDCustomImageSizeKey(funcMeta *faasfunction.FunctionService) string {
	return utils.FuncCustomImgSizePrefix + "/yrk/tenant/" + funcMeta.Spec.Configuration.TenantID +
		"/function/" + funcMeta.Spec.Configuration.FunctionName +
		"/version/" + funcMeta.Spec.Configuration.FunctionVersion
}

// GetAlreadyExistNodes get existNodes
func (w *workerCache) GetAlreadyExistNodes(funcSvcID string) map[string]struct{} {
	alreadyExistNodes := make(map[string]struct{}, 0)

	fc, ok := w.getFunctionCache(funcSvcID)
	if !ok {
		return alreadyExistNodes
	}
	fc.RLock()
	defer fc.RUnlock()
	// first to get websocket reserved instance
	for _, instance := range fc.instances {
		alreadyExistNodes[instance.NodeIP] = struct{}{}
	}

	return alreadyExistNodes
}

func needUpdateAgency(meta *faasfunction.FunctionService) bool {
	if meta == nil {
		return false
	}
	return commonutils.CheckJavaVersion(meta.Spec.Configuration.Runtime.Language)
}

// GetWorkerAgencyMap get worker agency from cache
func (w *workerCache) GetWorkerAgencyMap() map[string]PodAgency {
	workerAgencyMap := make(map[string]PodAgency, initCacheSize)
	w.functions.Range(func(key, value interface{}) bool {
		fc, ok := value.(*functionCache)
		if !ok || fc == nil {
			return true
		}
		fc.RLock()
		// only take java instances
		needUpdate := needUpdateAgency(fc.funcMeta)
		meta := fc.funcMeta
		fc.RUnlock()
		if !needUpdate {
			return true
		}
		podAgency := PodAgency{}
		podAgency.WorkerIns = make(map[string]v1alpha1.WorkerInstance)
		podAgency.FuncMeta = meta

		pods := utils.GetFunctionPodsByInformer(meta)
		for i := range pods {
			ins := utils.MakeWorkerInsInfo(pods[i])
			podAgency.WorkerIns[pods[i].Name] = ins
		}

		workerAgencyMap[meta.Spec.Configuration.FunctionVersionURN] = podAgency
		return true
	})
	return workerAgencyMap
}

func handleCacheForImageAcceleration(funcSvcID, crSignature string, funcMeta *faasfunction.FunctionService) {
	if funcMeta == nil {
		log.GetLogger().Warnf("funcMeta is nil of func %s", funcSvcID)
		return
	}
	faaSConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Warnf("failed to get faaS config for image acceleration: %s", err.Error())
		return
	}
	if faaSConfig.ImageAffinityCacheConfig.DisableImageAffinityCache != 0 {
		log.GetLogger().Warnf("disable image affinity cache")
		return
	}
	functionInfo := utils.GetFunctionInfoByURN(funcMeta.Spec.Configuration.FunctionVersionURN)
	whiteListEffect := true
	if len(faaSConfig.ImageAffinityCacheConfig.TenantWhiteList) != 0 {
		whiteListEffect = false
		for _, value := range faaSConfig.ImageAffinityCacheConfig.TenantWhiteList {
			if functionInfo.Tenant == value {
				whiteListEffect = true
			}
		}
	}
	if whiteListEffect && isActiveFunction(functionInfo.Tenant,
		functionInfo.FuncName, functionInfo.Version) {
		cacheList := ObtainingNodesSuitableForCache(faaSConfig.ImageAffinityCacheConfig.CacheNodesPercentage,
			faaSConfig.ImageAffinityCacheConfig.CacheNodesNumber)
		ImageNodeCache.Store(funcMeta.Spec.Configuration.FunctionVersionURN, cacheList)
		go UpdateImageCache(funcSvcID, crSignature, funcMeta, cacheList)
		log.GetLogger().Infof("the tenant: %s is in the whiteList and %s is active function, cacheList: %v",
			functionInfo.Tenant, functionInfo.FuncName, cacheList)
		return
	}
	ImageNodeCache.Delete(funcMeta.Spec.Configuration.FunctionVersionURN)
	go DeleteImageCache(funcSvcID)
	log.GetLogger().Infof("the tenant: %s is not in the whiteList or %s is not active function",
		functionInfo.Tenant, functionInfo.FuncName)
	return
}

// GetActivenessCaches -
func GetActivenessCaches() *ActivenessCache {
	return activenessCaches
}

func isActiveFunction(tenantID, funcName, version string) bool {
	// example for funcKeyWithRes : 0a0****ggg/0@default@func/latest/300-128
	activenessCaches.FunctionListLock.RLock()
	defer activenessCaches.FunctionListLock.RUnlock()
	for funcKeyWithRes, timeStr := range activenessCaches.InactiveFunctionList {
		funcInfo := strings.Split(funcKeyWithRes, "/")
		if len(funcInfo) != defaultFuncInfoLen {
			continue
		}
		// the tenant ID in the Redis is anonymized: 0a0****ggg
		anonymizeTenantID := urnutils.Anonymize(tenantID)
		if version == funcInfo[versionIndex] && funcName == funcInfo[funcNameIndex] &&
			anonymizeTenantID == funcInfo[tenantIDIndex] {
			timeInt, err := strconv.Atoi(timeStr)
			if err != nil {
				log.GetLogger().Warnf("funcKeyWithRes: %s | failed to convert keepalive time from string: %s",
					funcKeyWithRes, err.Error())
				return false
			}
			log.GetLogger().Infof("funcKeyWithRes: %s | success convert keepalive time: %d from string",
				funcKeyWithRes, timeInt)
			return timeInt == activeFuncTimeInt
		}
	}
	log.GetLogger().Infof("the corresponding function: %s, version: %s is not found in the list", funcName, version)
	// default return ture
	return true
}
