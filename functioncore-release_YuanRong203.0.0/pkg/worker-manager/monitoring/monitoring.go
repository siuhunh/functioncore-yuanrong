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

// Package monitoring report monitoring metrics to the redis
package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	listercorev1 "k8s.io/client-go/listers/core/v1"
	resourcehelper "k8s.io/kubectl/pkg/util/resource"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/timeutil"
	commontypes "functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/cache/willdeleteworker"
	"functioncore/pkg/worker-manager/config"
	tools "functioncore/pkg/worker-manager/utils"
)

const (
	base                          = 10
	writeTickerTime               = 5
	reportTickerTime              = 1
	metricsKeyTTL                 = 2
	defaultKeyLength              = 11
	businessLocation              = 4
	tenantIDLocation              = 6
	instanceIDLocation            = 12
	functionNameLocation          = 8
	versionLocation               = 10
	oneMinute                     = 60
	convertToMilliseconds         = 1000
	etcdlable                     = "node-etcd"
	disableReservedInsFee         = "DISABLE_RESERVED_INS_FEE"
	redisCxtTimeout               = 10 * time.Second
	minusOneMinute                = -1 * time.Minute
	retryTimes                    = 2
	typeRouter                    = "route"
	typeMeta                      = "meta"
	etcdStatusHealthy             = 1
	etcdStatusUnhealthy           = 0
	metaEtcdRedisKey              = "fss-meta-etcd-status"
	routerEtcdRedisKey            = "fss-route-etcd-status-%s"
	redisReconnectionInternal     = 10 * time.Second
	fssCPUFeatureFormat           = "fss-cpu-feature:%s:%s"
	cpuFeatureMatchKeyFormat      = "ClusterState_CpuFeature_%s"
	reportCPUMatchRatioTickerTime = 10
	nodeRoleLabel                 = "node-role"
	retryUpdateRedisTimes         = 3
	clearExpiredDataTimes         = 1
	failedInfoClearTimes          = 60
	maxValues                     = 3
	coldStartMetricsExpireMin     = 60
	migratingMetricsExpireTime    = 60 * time.Minute
	bufferSize                    = 10000
	logLimitFrequency             = 1000000
	defaultMigratingTickerMin     = 10
	instanceCronSpec              = "10 * * * * ?"
)

var (
	client            = &monitoringClient{}
	ratio             ratioMonitor
	coldStartKey      = fmt.Sprintf("/sn/metrics/coldstart/%s/worker-manager", os.Getenv(constants.ClusterID))
	nodeSpaceKey      = fmt.Sprintf("/sn/metrics/space/%s/worker-manager", os.Getenv(constants.ClusterID))
	metricsChan       = make(chan ColdStartMetrics, bufferSize)
	reachMaxChanCount = 0
	totalMetrics      totalColdStartMetrics
	// migrating metrics config
	migratingTickerMin = os.Getenv("MIGRATING_TICKER_MIN")
	migratingKey       = fmt.Sprintf("/sn/metrics/migrating/%s/worker-manager", clusterID)
	migratingMetrics   MigratingMetrics
)

type monitoringClient struct {
	c              *redisclient.Client
	lock           sync.RWMutex
	config         config.MonitoringRedisServerConfig
	redisCFG       config.MonitoringRedisServerConfig
	recordingCFG   config.PoolRecordingConfig
	podLister      listercorev1.PodLister
	nodeLister     listercorev1.NodeLister
	redisConnected bool
}

type reportedReserveInstanceInfo struct {
	FunctionURN string             `json:"functionURN" valid:"optional"`
	PodName     string             `json:"podName" valid:"optional"`
	ClusterID   string             `json:"clusterID" valid:"optional"`
	TimeWindows []billingTimeStamp `json:"timeWindows" valid:"optional"`
}

type billingTimeStamp struct {
	StartTime       int64 `json:"startTime" valid:"optional"`
	EndTime         int64 `json:"endTime" valid:"optional"`
	BillingDuration int64 `json:"billingDuration" valid:"optional"`
}

// requestNum record proxy request number and podAddNum record pool add number
// true ratioStatus mean proxy request over pool supply
type ratioMonitor struct {
	requestNum  int32
	podAddNum   int32
	ratioStatus int32
}

// ColdStartMetrics -
type ColdStartMetrics struct {
	TimeStamp int64   `json:"timeStamp"`
	Count     int     `json:"count"`
	Delay     float64 `json:"delay"`
}

// totalColdStartMetrics -
type totalColdStartMetrics struct {
	lock       sync.RWMutex
	totalCount int
	totalDelay float64
}

// MigratingMetrics -
type MigratingMetrics struct {
	lock         sync.RWMutex
	TimeStamp    int64 `json:"timeStamp"`
	SucceedCount int   `json:"succeedCount"`
	FailedCount  int   `json:"failedCount"`
}

// AddMigratingMetrics -
func AddMigratingMetrics(isSucceed bool) {
	migratingMetrics.lock.Lock()
	if isSucceed {
		migratingMetrics.SucceedCount++
	} else {
		migratingMetrics.FailedCount++
	}
	migratingMetrics.lock.Unlock()
}

func cleanMigratingMetrics() {
	migratingMetrics.lock.Lock()
	migratingMetrics.TimeStamp = 0
	migratingMetrics.SucceedCount = 0
	migratingMetrics.FailedCount = 0
	migratingMetrics.lock.Unlock()
}

// CreateMonitoringClient method to create a monitoring client
func CreateMonitoringClient(redisConfig config.MonitoringRedisServerConfig,
	recordingConfig config.PoolRecordingConfig, stopCh <-chan struct{}) error {
	client.redisCFG = redisConfig
	client.recordingCFG = recordingConfig
	client.podLister = tools.GetPodInformer().Lister()
	client.nodeLister = tools.GetNodeInformer().Lister()
	client.redisConnected = true
	var err error
	config.MonitoringPasswd, err = localauth.Decrypt(redisConfig.Password)
	if err != nil {
		return err
	}
	if err := client.initClient(stopCh); err != nil {
		log.GetLogger().Errorf("failed to connect the monitoring server %s, %s", redisConfig.ServerAddr, err.Error())
		return err
	}
	log.GetLogger().Infof("succeeded to connect the monitoring server %s", redisConfig.ServerAddr)

	return nil
}

// InitMetricsCache -
func InitMetricsCache() error {
	etcdClient := etcd3.GetRouterEtcdClient()
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := etcdClient.GetResponse(defaultEtcdCtx, "/sn/workers/business/", clientv3.WithPrefix())
	if err != nil {
		return err
	}
	for _, kv := range resp.Kvs {
		var innerInstance v1alpha1.WorkerInstance
		if err = json.Unmarshal(kv.Value, &innerInstance); err != nil {
			continue
		}
		strs := strings.Split(string(kv.Key), "/")
		var businessID, functionName, functionVersion, tenantID string
		if len(strs) < defaultKeyLength {
			continue
		}
		tenantID = strs[tenantIDLocation]
		if tenantID == "0" {
			continue
		}
		innerInstance.InstanceID = strs[instanceIDLocation]
		businessID = strs[businessLocation]
		functionName = strs[functionNameLocation]
		functionVersion = strs[versionLocation]
		funcKey := tools.GetFuncKey(tenantID, "", functionName, functionVersion)
		funcSvcName := tools.CrName(businessID, tenantID, functionName, functionVersion)
		cache.IncFuncInstanceCurrentCount(funcKey)
		if innerInstance.WillDelete {
			willdeleteworker.GetCache().AddWillDeleteWorkerInstances(funcSvcName,
				[]v1alpha1.WorkerInstance{innerInstance})
		}
	}
	return nil
}

// ReportMetricsToServer -
func ReportMetricsToServer(stopChan <-chan struct{}) {
	go reportMetricsToServer(stopChan)
	go reportPoolInfoToServer(stopChan)
	go reportNodeInfoToServer(stopChan)
	go recordRatio(stopChan)
	go reportFunctionInstances(stopChan)
	go reportEtcdStatusToRedis(stopChan)
	go reportCPUFlagsMatchingRatioTicker(stopChan)
	go clearExpiredData(stopChan)
	go reportWorkerManagerMetricsToRedis(stopChan)
	go reportMigratingMetricsToRedis(stopChan)
}

func clearExpiredData(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	log.GetLogger().Infof("start to delete expired snapshot circuit breaking data")
	clearTick := time.NewTicker(clearExpiredDataTimes * time.Minute)

	for {
		select {
		case <-clearTick.C:
			clearExpiredDataInternal()
		case _, ok := <-stopChan:
			if !ok {
				clearTick.Stop()
				log.GetLogger().Infof("stop clear  expired snapshot data")
				return
			}
		}
	}
}

func clearExpiredDataInternal() {
	result, err := getFailedRestoreInfo()
	if err != nil {
		log.GetLogger().Errorf("Get all restoreFailInfo failed %s", err.Error())
		return
	}
	info := &commontypes.FailedInfo{}
	var clearExpiredDataList []string
	needDel := false
	for key, value := range result {
		err = json.Unmarshal([]byte(value), info)
		if err != nil {
			log.GetLogger().Errorf("Unmarshal info failed!error: %s", err.Error())
			return
		}
		t := time.Unix(info.Time, 0)

		// 计算当前时间与时间戳之间的分钟差异
		minutesDiff := time.Since(t).Minutes()
		if minutesDiff >= failedInfoClearTimes {
			needDel = true
			clearExpiredDataList = append(clearExpiredDataList, key)
		}
	}
	if len(clearExpiredDataList) == 0 {
		log.GetLogger().Warnf("clearExpiredDataList is null, len is 0!")
		return
	}
	if needDel {
		log.GetLogger().Info("Need to clear expired data!")
		delFailedRestoreInfo(err, clearExpiredDataList)
	}
}

func getFailedRestoreInfo() (map[string]string, error) {
	var err error
	for i := 0; i < redisRetryTimes; i++ {
		ctx, _ := context.WithTimeout(context.Background(), redisCxtTimeout)
		redisCli := client.GetRedisClient()
		if redisCli == nil {
			log.GetLogger().Errorf("[getFailedRestoreInfo]redis is not ready")
			continue
		}
		result, err := redisCli.HGetAll(ctx, constants.RestoreFailInfo).Result()
		if err == nil {
			log.GetLogger().Debugf("Success to get key: %s, value: %s", constants.RestoreFailInfo, result)
			return result, nil
		}
		log.GetLogger().Errorf("failed to get key: %s, err: %s, retry times %d", constants.RestoreFailInfo, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
	return nil, err
}

func delFailedRestoreInfo(err error, clearExpiredDataList []string) {
	for i := 0; i < retryUpdateRedisTimes; i++ {
		ctx, _ := context.WithTimeout(context.Background(), redisCxtTimeout)
		redisCli := client.GetRedisClient()
		if redisCli == nil {
			log.GetLogger().Errorf("redis is not ready when upload function instance")
			continue
		}
		err = redisCli.HDel(ctx, constants.RestoreFailInfo, clearExpiredDataList...).Err()
		if err == nil {
			log.GetLogger().Infof("clear expired date %s .", clearExpiredDataList)
			break
		}
		log.GetLogger().Errorf("failed to push key: %s , with err: %s , and try %d times",
			constants.RestoreFailInfo, err, i+1)
		time.Sleep(redisRetryInterval)
	}
}

func reportNodeInfoToServer(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	if client.podLister == nil {
		log.GetLogger().Warnf("pod lister is nil")
		return
	}
	if client.nodeLister == nil {
		log.GetLogger().Warnf("node lister is nil")
		return
	}
	if client.recordingCFG.RecordingInterval <= 0 {
		client.recordingCFG.RecordingInterval = constants.DefaultRecordingInterval
	}
	if client.recordingCFG.RecordExpiredTime <= 0 {
		client.recordingCFG.RecordExpiredTime = constants.DefaultRecordExpiredTime
	}
	writeTick := time.NewTicker(time.Duration(client.recordingCFG.RecordingInterval) * time.Second)

	for {
		select {
		case <-writeTick.C:
			if err := client.writeNodeInfo(); err != nil {
				log.GetLogger().Warnf("failed to write CPU and MEMORY to redis, error: %s",
					err.Error())
			}
		case _, ok := <-stopChan:
			if !ok {
				writeTick.Stop()
				log.GetLogger().Debugf("stop reporting CPU and MEMORY to server")
				return
			}
		}
	}
}

func reportFunctionInstances(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	log.GetLogger().Infof("start to report function instances metrics to server")
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc(instanceCronSpec, func() {
		instanceCountMap := cache.WorkerCache.GetWorkerInstanceCount()
		instanceKey := "metrics_function_instance_count_" + client.redisCFG.ClusterID
		client.uploadFunctionInstance(instanceKey, instanceCountMap)
		reversedCountMap := cache.WorkerCache.GetReservedInstanceMap()
		reversedKey := "metrics_function_reserved_instance_count_" + client.redisCFG.ClusterID
		client.uploadFunctionInstance(reversedKey, reversedCountMap)
	})
	if err != nil {
		log.GetLogger().Errorf("failed to start reportFunctionInstances cron, err: %s", err.Error())
		return
	}
	c.Start()
	defer c.Stop()
	<-stopChan
	log.GetLogger().Infof("stop report function instance to server")
}

func reportMetricsToServer(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	log.GetLogger().Infof("start to report metrics to server")
	reportTick := time.NewTicker(reportTickerTime * time.Minute)

	for {
		select {
		case <-reportTick.C:
			if err := client.reportFunctionInstanceCurrentCount(); err != nil {
				log.GetLogger().Errorf("failed to report Function Instance Current Count, error: %s",
					err.Error())
			}
			client.reportFunctionReservedInstanceCurrentCount()
		case _, ok := <-stopChan:
			if !ok {
				reportTick.Stop()
				log.GetLogger().Infof("stop report metrics to server")
				return
			}
		}
	}
}

func reportPoolInfoToServer(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stop channel is nil")
		return
	}
	if client.podLister == nil {
		log.GetLogger().Errorf("pod lister is nil")
		return
	}
	if client.podLister == nil {
		log.GetLogger().Errorf("node lister is nil")
		return
	}
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config error: %s", err.Error())
		return
	}
	if cfg == nil {
		log.GetLogger().Errorf("config is nil")
		return
	}
	log.GetLogger().Infof("start to report pool info to server")
	poolInfoKey := constants.PoolInfoPrefix + constants.PoolInfoSep + os.Getenv(constants.ClusterIDKey)
	if client.recordingCFG.RecordingInterval <= 0 {
		client.recordingCFG.RecordingInterval = constants.DefaultRecordingInterval
	}
	if client.recordingCFG.RecordExpiredTime <= 0 {
		client.recordingCFG.RecordExpiredTime = constants.DefaultRecordExpiredTime
	}
	tick := time.NewTicker(time.Duration(client.recordingCFG.RecordingInterval) * time.Second)
	for {
		select {
		case <-tick.C:
			redisCli := client.GetRedisClient()
			if redisCli == nil {
				log.GetLogger().Errorf("redis is not ready when report pool info to server")
				continue
			}
			if err = client.reportPoolInfo(cfg.PoolMgrConfig.PoolList.Pool, poolInfoKey); err != nil {
				log.GetLogger().Errorf("report pool info to redis server: %s error: %s",
					redisclient.GetRedisAddress(redisCli), err.Error())
			}
		case _, ok := <-stopChan:
			if !ok {
				tick.Stop()
				log.GetLogger().Infof("stop reporting pool info to server")
				return
			}
		}
	}
}

// GetRedisClient -
func GetRedisClient() *redisclient.Client {
	return client.GetRedisClient()
}

// GetRedisClient -
func (mc *monitoringClient) GetRedisClient() *redisclient.Client {
	mc.lock.RLock()
	redisCli := mc.c
	mc.lock.RUnlock()
	return redisCli
}

// SetRedisClient -
func (mc *monitoringClient) SetRedisClient(redisCli *redisclient.Client) {
	mc.lock.Lock()
	mc.c = redisCli
	mc.lock.Unlock()
}

func (mc *monitoringClient) initRedisClient(stopCh <-chan struct{}) error {
	clientConfig := mc.redisCFG
	monitoringPasswd := string(config.MonitoringPasswd)
	redisCli, err := redisclient.New(redisclient.NewRedisClientParam{
		ServerMode: clientConfig.ServerMode,
		ServerAddr: clientConfig.ServerAddr,
		Password:   monitoringPasswd,
		Timeout:    clientConfig.TimeoutConf,
	}, stopCh, redisclient.SetEnableTLS(clientConfig.EnableTLS), redisclient.AlarmSwitchOn(),
		redisclient.SetGetRealTimeServerAddrFunc(config.GetHotloadMonitorConf))
	if err != nil {
		log.GetLogger().Errorf("failed to new a redis client, %s", err.Error())
		return err
	}
	if redisCli == nil {
		log.GetLogger().Errorf("redis client is nil")
		return errors.New("redis client is nil")
	}
	redisclient.SetRedisCmd(redisCli)
	mc.SetRedisClient(redisCli)
	return nil
}

func (mc *monitoringClient) initClient(stopCh <-chan struct{}) error {
	err := mc.initRedisClient(stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to new a redis client, %s", err.Error())
	}
	go mc.checkRedisConnectivity(stopCh)
	checkSWRAndReport(stopCh)
	checkVpcAndReport(stopCh)
	getAndMonitorActiveFunction(stopCh)
	getAndMonitorFunctionProfiling(stopCh)
	return nil
}

func (mc *monitoringClient) checkRedisConnectivity(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	ticker := time.NewTicker(redisReconnectionInternal)
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debug("redis check redis connection start")
			redisCli := mc.GetRedisClient()
			if redisCli != nil {
				_, err := redisCli.Ping(context.TODO()).Result()
				if err == nil {
					mc.redisConnected = true
					log.GetLogger().Debug("redis periodically checks availability")
					continue
				}
				redisCli.Close()
			}
			err := mc.initRedisClient(stopCh)
			if err != nil {
				mc.redisConnected = false
				log.GetLogger().Errorf("failed to init redis client, err:%s", err.Error())
				continue
			}
			mc.redisConnected = true
			log.GetLogger().Infof("redis connect succeed, clear the redis alarm")
			alarm.GetClient().ClearAlarm(alarm.RedisConnection)
		case <-stopCh:
			log.GetLogger().Errorf("module process exit")
			ticker.Stop()
			return
		}
	}
}

func checkSWRAndReport(stopCh <-chan struct{}) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return
	}

	alarm.GetClient().RegisterAlarmType(alarm.SWRConnection)
	go swrHealthCheckAndReport(cfg.AlarmServerAddress, cfg.LocalAuth.AKey, cfg.LocalAuth.SKey, config.GetSWRAPI(),
		stopCh)
}

func calRequestCPUAndMemory(pods []*corev1.Pod, nodeName string) (int64, int64, int64) {
	nodeRequestCPU := int64(0)
	nodeRequestMemory := int64(0)
	usedPods := int64(0)
	for i := 0; i < len(pods); i++ {
		if pods[i].Spec.NodeName == nodeName {
			podReqs, _ := resourcehelper.PodRequestsAndLimits(pods[i])
			nodeRequestCPU += podReqs.Cpu().MilliValue()
			nodeRequestMemory += podReqs.Memory().Value()
			usedPods++
		}
	}
	return nodeRequestCPU, nodeRequestMemory, usedPods
}

func calRequestResourceOfNode(pods []*corev1.Pod, nodeName string) *config.NodeInfo {
	info := &config.NodeInfo{}
	for i := 0; i < len(pods); i++ {
		if pods[i].Spec.NodeName == nodeName {
			podReqs, _ := resourcehelper.PodRequestsAndLimits(pods[i])
			info.CPUUsed += podReqs.Cpu().MilliValue()
			info.MemoryUsed += podReqs.Memory().Value()
			info.UsedPods++
			info.RequestNvidiaGPU += podReqs.Name(tools.NvidiaGPU, resource.DecimalSI).Value()
			info.RequestGPUPercentage += podReqs.Name(tools.GpuIsolationCoreKey, resource.DecimalSI).Value()
			info.RequestGPUMem128 += podReqs.Name(tools.GpuIsolationMemKey, resource.DecimalSI).Value()
		}
	}
	return info
}

func (mc *monitoringClient) writeNodeInfo() error {
	clusterID := os.Getenv("CLUSTER_ID")
	nodes, err := mc.nodeLister.List(labels.Everything())
	if err != nil {
		log.GetLogger().Errorf("failed to list all nodes with error: %s", err.Error())
		return err
	}
	pods, err := mc.podLister.List(labels.Everything())
	if err != nil {
		log.GetLogger().Errorf("failed to get pods: %s", err.Error())
		return err
	}
	var clusterCPUTotal int64
	var clusterCPUUsed int64
	expiredTime := time.Duration(mc.recordingCFG.RecordExpiredTime) * time.Second
	for _, node := range nodes {
		if !tools.IsNodeReady(node) {
			continue
		}
		// skip the CPU and MEMORY info of etcd node
		_, ok := node.ObjectMeta.Labels[etcdlable]
		if ok {
			continue
		}
		nodeInfo := calRequestResourceOfNode(pods, node.Name)
		nodeInfo.CPUTotal = node.Status.Capacity.Cpu().MilliValue()
		nodeInfo.MemoryTotal = node.Status.Capacity.Memory().Value()
		clusterCPUTotal += nodeInfo.CPUTotal
		clusterCPUUsed += nodeInfo.CPUUsed
		// update cache with request cpu and memory
		nodeIP := tools.GetNodeIPAddress(node)
		key := "ClusterState_CpuMem_" + clusterID + "_" + nodeIP
		cache.UpdateNodeInfoByNodeID(node.Name, nodeInfo)
		data, err := json.Marshal(nodeInfo)
		if err != nil {
			log.GetLogger().Warnf("failed to json marshal nodeInfoMap, %s", err.Error())
			return err
		}
		redisCli := mc.GetRedisClient()
		if redisCli == nil {
			log.GetLogger().Warnf("redis is not ready")
			return errors.New("redis is not ready")
		}
		_, err = redisCli.Set(context.TODO(), key, string(data), expiredTime).Result()
		if err != nil {
			return err
		}
	}
	err = mc.updateClusterCPUMetric(clusterCPUTotal, clusterCPUUsed, clusterID, expiredTime)
	if err != nil {
		return err
	}
	return nil
}

func (mc *monitoringClient) updateClusterCPUMetric(clusterCPUTotal int64,
	clusterCPUUsed int64, clusterID string, expiredTime time.Duration) error {
	clusterUsage := config.ClusterUsage{}
	clusterUsage.CPUTotal = clusterCPUTotal
	clusterUsage.CPUUsed = clusterCPUUsed
	clusterUsageStr, err := json.Marshal(clusterUsage)
	if err != nil {
		log.GetLogger().Warnf("failed to json marshal clusterUsage, %s", err.Error())
		return err
	}
	clusterCPUStateKey := "ClusterState_Metric_" + clusterID
	redisCli := mc.GetRedisClient()
	if redisCli == nil {
		log.GetLogger().Warnf("redis is not ready")
		return errors.New("redis is not ready")
	}
	_, err = redisCli.Set(context.TODO(), clusterCPUStateKey, string(clusterUsageStr), expiredTime).Result()
	if err != nil {
		return err
	}
	return nil
}

// FunctionInstances -
type FunctionInstances struct {
	TimeWindows *functionTimeWindow `json:"time_windows"`
	FunctionURN string              `json:"func_urn"`
}

type functionTimeWindow struct {
	StartTime int64 `json:"start_time"`
	EndTime   int64 `json:"end_time"`
	Value     int   `json:"value"`
}

func composeTimeWindow(function string, instances int) FunctionInstances {
	funInstance := FunctionInstances{TimeWindows: &functionTimeWindow{}}
	funInstance.FunctionURN = function
	funInstance.TimeWindows.EndTime = time.Now().Unix()
	funInstance.TimeWindows.StartTime = time.Now().Add(minusOneMinute).Unix()
	funInstance.TimeWindows.Value = instances
	return funInstance
}

func (mc *monitoringClient) uploadFunctionInstance(key string, countMap map[string]int) {
	for function, instance := range countMap {
		ctx, _ := context.WithTimeout(context.Background(), redisCxtTimeout)
		funInstance := composeTimeWindow(function, instance)
		data, err := json.Marshal(funInstance)
		if err != nil {
			log.GetLogger().Errorf("failed to json marshal function TimeWindow, %s.", err.Error())
		}
		for i := 0; i < retryTimes; i++ {
			redisCli := mc.GetRedisClient()
			if redisCli == nil {
				log.GetLogger().Warnf("redis is not ready when upload function instance")
				continue
			}
			_, err = redisCli.RPush(ctx, key, string(data)).Result()
			if err == nil {
				break
			}
			log.GetLogger().Errorf("failed to push key: %s , with err: %s , and try %d times", key, err, i+1)
			time.Sleep(1 * time.Second)
		}
	}
}

func (mc *monitoringClient) reportFunctionInstanceCurrentCount() error {
	data, err := cache.MarshalFuncInsCurrentCount()
	if err != nil {
		log.GetLogger().Warnf("failed to json marshal functionInstanceCurrentCount, %s", err.Error())
		return err
	}
	key := "metrics_function_instance_" + mc.redisCFG.ClusterID
	redisCli := mc.GetRedisClient()
	if redisCli == nil {
		log.GetLogger().Warnf("redis is not ready")
		return errors.New("redis is not ready")
	}
	_, err = redisCli.Set(context.TODO(), key, string(data), metricsKeyTTL*time.Minute).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to set key: %s with ttl, %s", key, err)
		return err
	}
	log.GetLogger().Debugf("succeeded to report functionInstanceCurrentCount %s to server %s", key,
		mc.redisCFG.ServerAddr)
	return nil
}

func (mc *monitoringClient) reportFunctionReservedInstanceCurrentCount() {
	m := cache.WorkerCache.GetReservedInstanceRecords()
	data, err := json.Marshal(m)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal reserved instance records, err: %s", err.Error())
		return
	}
	key := "metrics_function_reserved_instance_" + mc.redisCFG.ClusterID
	redisCli := mc.GetRedisClient()
	if redisCli == nil {
		log.GetLogger().Warnf("redis is not ready")
		return
	}
	_, err = redisCli.Set(context.TODO(), key, string(data), metricsKeyTTL*time.Minute).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to set key: %s with ttl, %s", key, err)
		return
	}
	log.GetLogger().Debugf("succeeded to report function reserved instance records: %s to redis: %s", key,
		mc.redisCFG.ServerAddr)
}

// PoolInfo pool info
type PoolInfo struct {
	GeneralizedPodNums int `json:"generalized_pod_nums"`
	PoolPodNums        int `json:"pool_pod_nums"`
}

func (mc *monitoringClient) reportPoolInfo(poolCfg []config.PoolConfig, key string) error {
	// poolInfos record standard and pool info, so length of poolInfos is twice of pool config length
	poolInfos := make([]interface{}, 0, len(poolCfg)*2)
	info := &PoolInfo{}
	standards := tools.PackPoolStandardByPoolConfig(poolCfg)
	for _, standard := range standards {
		label := map[string]string{"isPoolPod": "false", "standard": standard}
		generalizedPods, err := mc.podLister.List(labels.SelectorFromSet(label))
		if err != nil {
			log.GetLogger().Errorf("get pod list by label error: %s", err.Error())
			return err
		}
		label = map[string]string{"isPoolPod": "true", "standard": standard}
		poolPods, err := mc.podLister.List(labels.SelectorFromSet(label))
		if err != nil {
			log.GetLogger().Errorf("get pool pod list by label error: %s", err.Error())
			return err
		}
		info.GeneralizedPodNums = len(generalizedPods)
		info.PoolPodNums = len(poolPods)
		infoBytes, err := json.Marshal(info)
		if err != nil {
			log.GetLogger().Errorf("json marshal error: %s", err.Error())
			return err
		}
		poolInfos = append(poolInfos, standard, string(infoBytes))
	}
	redisCli := mc.GetRedisClient()
	if redisCli == nil {
		log.GetLogger().Warnf("redis is not ready")
		return errors.New("redis is not ready")
	}
	if _, err := redisCli.HMSet(context.TODO(), key, poolInfos...).Result(); err != nil {
		log.GetLogger().Errorf("failed to hmset key: %s, value: %v, error: %s", key, poolInfos, err.Error())
		return err
	}
	expiredTime := time.Duration(mc.recordingCFG.RecordExpiredTime) * time.Second
	if _, err := redisCli.Expire(context.TODO(), key, expiredTime).Result(); err != nil {
		log.GetLogger().Errorf("failed to expire key: %s with time: %v, error: %s", key, expiredTime, err.Error())
		return err
	}
	return nil
}

// ReportFailureFee -
func ReportFailureFee(requestInfo *config.RequestInfo, err error) {
	sErr, ok := err.(snerror.SNError)
	if !ok {
		return
	}
	if !snerror.IsStartUpError(sErr.Code()) {
		return
	}
	initializerErrorMessage := snerror.FailureFee{}
	if err = json.Unmarshal([]byte(sErr.Error()), &initializerErrorMessage); err != nil {
		log.GetLogger().Warnf("failed to to unmarshal initializerErrorMessage json, %s", err.Error())
		return
	}
	complexFuncName := urnutils.ComplexFuncName{}
	complexFuncName.ParseFrom(requestInfo.FunctionName)
	memorySize := strconv.Itoa(initializerErrorMessage.Summary.MemorySize)
	timestamp := strconv.FormatInt(timeutil.NowUnixMillisecond(), base)
	// Failure-Fee-projectID@package@functionName@version@memsize@timestamp
	key := "Failure-Fee-" + strings.Join([]string{requestInfo.TenantID, complexFuncName.ServiceID,
		complexFuncName.FuncName, requestInfo.FunctionVersion,
		memorySize, timestamp}, "@")
	data, err := json.Marshal(initializerErrorMessage.Summary)
	if err != nil {
		log.GetLogger().Warnf("failed to to marshal summary, %s", err.Error())
		return
	}
	redisCli := client.GetRedisClient()
	if redisCli == nil {
		log.GetLogger().Warnf("redis is not ready")
		persistData("set", key, string(data))
		return
	}
	_, err = redisCli.Set(context.TODO(), key, string(data), metricsKeyTTL*time.Minute).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to set key: %s with ttl, %s", key, err.Error())
		persistData("set", key, string(data))
		return
	}
	if !client.redisConnected {
		restoreRedis(redisCli)
	}
	log.GetLogger().Debugf("succeeded to report failure fee %s to server %s", key,
		client.redisCFG.ServerAddr)
}

func persistData(method, key, value string) {
	var err error
	client.redisConnected = false
	redisCache := redisclient.RedisCache{Method: method,
		Key: key, Value: value}
	err = redisclient.WriteToDisk(redisCache)
	if err != nil {
		log.GetLogger().Errorf("failed to write data to disk, %s", err.Error())
	}
}

func restoreRedis(c *redisclient.Client) {
	log.GetLogger().Infof("redis connected.start to restore cache to redis.")
	err := redisclient.TakeCacheToRedis(c)
	if err != nil {
		log.GetLogger().Errorf("failed to take cache to redis, error: %s", err.Error())
		return
	}
	client.redisConnected = true
}

func recordRatio(stopChan <-chan struct{}) {
	if stopChan == nil {
		return
	}
	timeTick := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-timeTick.C:
			setRatioStatus()
			atomic.StoreInt32(&ratio.requestNum, 0)
			atomic.StoreInt32(&ratio.podAddNum, 0)
		case _, ok := <-stopChan:
			if !ok {
				timeTick.Stop()
				return
			}
		}
	}
}

func setRatioStatus() {
	podAddNum := atomic.LoadInt32(&ratio.podAddNum)
	if podAddNum == 0 {
		atomic.StoreInt32(&ratio.ratioStatus, 0)
		return
	}
	if atomic.LoadInt32(&ratio.requestNum)/podAddNum >= 1 {
		atomic.StoreInt32(&ratio.ratioStatus, 1)
		return
	}
	atomic.StoreInt32(&ratio.ratioStatus, 0)
}

// AddRatioNum add request or pod num to monitor
func AddRatioNum(addType string) {
	if addType == tools.TypeRequestNum {
		atomic.AddInt32(&ratio.requestNum, 1)
		return
	}
	if addType == tools.TypePodNum {
		atomic.AddInt32(&ratio.podAddNum, 1)
		return
	}
	return
}

// GetRatioStatus get ratio status from monitor
func GetRatioStatus() bool {
	return atomic.LoadInt32(&ratio.ratioStatus) == 1
}

func reportEtcdStatusToRedis(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	log.GetLogger().Infof("start to report etcd status to redis")
	reportTick := time.NewTicker(reportTickerTime * time.Minute)
	for {
		select {
		case <-reportTick.C:
			saveEtcdNodesStatusToRedis(typeRouter, etcd3.GetRouterEtcdClient().CheckEtcdNodeStatus())
			if etcd3.GetMetaEtcdClient().Client == nil {
				log.GetLogger().Warnf("meta etcd client is nil")
				continue
			}
			saveEtcdNodesStatusToRedis(typeMeta, etcd3.GetMetaEtcdClient().CheckEtcdNodeStatus())
		case _, ok := <-stopChan:
			if !ok {
				reportTick.Stop()
				log.GetLogger().Infof("stop report etcd status to redis")
				return
			}
		}
	}
}

func saveEtcdNodesStatusToRedis(etcdType string, etcdNodeStatusOK bool) {
	redisCli := client.GetRedisClient()
	if client == nil || redisCli == nil {
		log.GetLogger().Warn("redis client is nil")
		return
	}
	redisKey := metaEtcdRedisKey
	if etcdType == typeRouter {
		redisKey = fmt.Sprintf(routerEtcdRedisKey, clusterID)
	}
	redisValue := etcdStatusHealthy
	if !etcdNodeStatusOK {
		redisValue = etcdStatusUnhealthy
	}
	setRedisKeyWithRetry(redisKey, redisValue)
}

func setRedisKeyWithRetry(redisKey string, redisValue interface{}) {
	putValueToRedisWithExpireTime(redisKey, redisValue, metricsKeyTTL*time.Minute)
}

func reportCPUFlagsMatchingRatioTicker(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil, reportCPUFlagsMatchingRatioTicker not start")
		return
	}
	reportCPUFlagsMatchingRatio()
	reportTick := time.NewTicker(reportCPUMatchRatioTickerTime * time.Minute)
	for {
		select {
		case <-reportTick.C:
			reportCPUFlagsMatchingRatio()
		case _, ok := <-stopChan:
			if !ok {
				reportTick.Stop()
				log.GetLogger().Debugf("stop reporting CPUFlagsMatchingRatio to server")
				return
			}
		}
	}
}

func reportCPUFlagsMatchingRatio() bool {
	log.GetLogger().Debugf("Start to reportCPUFlagsMatchingRatio")
	begin := time.Now()
	nodes, isNodeListValid := validateNodeLister()
	if !isNodeListValid {
		return false
	}
	clusterID := os.Getenv("CLUSTER_ID")
	cpuflagsMap := make(map[string][]string, len(nodes))

	coreFlags := getCoreCPUFlags()
	cpuMatchedKey := fmt.Sprintf(cpuFeatureMatchKeyFormat, clusterID)
	log.GetLogger().Debugf("[reportCPUFlagsMatchingRatio]getCoreCPUFlags is %v", coreFlags)
	if coreFlags == nil {
		log.GetLogger().Errorf("[reportCPUFlagsMatchingRatio]failed to get core cpu flags")
		// Only occurs when the core cpu feature set is empty, meaning that all nodes do not match.
		redisValue := "0.00%##all_nodes"
		putValueToRedisWithExpireTime(cpuMatchedKey, redisValue, 0)
		return false
	}

	finalMatchedFlags, matchedRatio := calculateNodesCPUMatchedRatio(nodes, clusterID, coreFlags, cpuflagsMap, begin)
	var unmatchedList []string
	for k, v := range cpuflagsMap {
		if strings.Compare(finalMatchedFlags, k) != 0 {
			for _, node := range v {
				unmatchedList = append(unmatchedList, node)
			}
			log.GetLogger().Infof("Unmatched flags list is %s, node list is %v", k, v)
		}
	}

	// 95.00%##127.0.0.1,192.168.0.1
	redisValue := fmt.Sprintf("%.2f%%", matchedRatio) + "##" + strings.Join(unmatchedList, ",")
	putValueToRedisWithExpireTime(cpuMatchedKey, redisValue, 0)
	return true
}

// Get core cpu flags from worker-manager-configmap, if get failed, return nil
func getCoreCPUFlags() []string {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config error: %s", err.Error())
		return nil
	}
	if cfg == nil {
		log.GetLogger().Errorf("config is nil")
		return nil
	}
	if cfg.PoolMgrConfig.CoreCPUFeatures == "" {
		log.GetLogger().Errorf("CoreCPUFeatures is nil")
		return nil
	}
	flags := strings.Fields(cfg.PoolMgrConfig.CoreCPUFeatures)
	sort.Strings(flags)
	return flags
}

func validateNodeLister() ([]*corev1.Node, bool) {
	if client.nodeLister == nil {
		log.GetLogger().Warnf("[reportCPUFlagsMatchingRatio]node lister is nil")
		return nil, false
	}
	// maybe includes node-role.kubernetes.io
	selector, err := labels.Parse(nodeRoleLabel)
	if err != nil {
		log.GetLogger().Errorf("[reportCPUFlagsMatchingRatio]node selector parse failed, error is %s", err.Error())
		return nil, false
	}

	nodes, err := client.nodeLister.List(selector)

	if err != nil {
		log.GetLogger().Errorf("[reportCPUFlagsMatchingRatio]failed to list all nodes with error: %s", err.Error())
		return nil, false
	}
	if len(nodes) <= 0 {
		log.GetLogger().Errorf("[reportCPUFlagsMatchingRatio]node list length is invalid, length is %d", len(nodes))
		return nil, false
	}
	return nodes, true
}

// Computes the instruction set that has the most equal intersections of nodes cpu flags
// with the core cpu features, and the match rate
// Gets nodes cpu flags from redis
func calculateNodesCPUMatchedRatio(nodes []*corev1.Node, clusterID string, coreFlags []string,
	cpuflagsMap map[string][]string, begin time.Time) (string, float32) {
	maxMatched := 0
	finalMatchedFlags := "no_intersect_flags"
	totalNodes := len(nodes)
	for _, node := range nodes {
		nodeIP := tools.GetNodeIPAddress(node)
		_, exist := node.Labels[nodeRoleLabel]
		if !exist {
			log.GetLogger().Warnf("Node label is no correct, node ip is %s, node label is %v", nodeIP, node.Labels)
			totalNodes--
			continue
		}
		fssCPUFlagsKey := fmt.Sprintf(fssCPUFeatureFormat, clusterID, nodeIP)
		tempMatched, intersectFlags := compareNodeCPUFlags(nodeIP, coreFlags, cpuflagsMap, fssCPUFlagsKey)
		if tempMatched > maxMatched && intersectFlags != "" {
			maxMatched = tempMatched
			finalMatchedFlags = intersectFlags
		}
	}
	matchedRatio := float32(maxMatched) / float32(totalNodes) * percent
	log.GetLogger().Infof("Total nodes count is %d, matched ratio is %.2f%%, flags list is %s, cost time %v",
		totalNodes, matchedRatio, finalMatchedFlags, time.Since(begin))
	return finalMatchedFlags, matchedRatio
}

func putValueToRedisWithExpireTime(redisKey string, redisValue interface{}, expireTime time.Duration) {
	for i := 0; i < redisRetryTimes; i++ {
		redisCli := client.GetRedisClient()
		if redisCli == nil {
			log.GetLogger().Warnf("[reportCPUFlagsMatchingRatio]redis is not ready")
			continue
		}
		_, err := redisCli.Set(context.TODO(), redisKey, redisValue, expireTime).Result()
		if err == nil {
			log.GetLogger().Infof("Success to set key: %s, value:%d", redisKey, redisValue)
			return
		}
		log.GetLogger().Errorf("failed to set key: %s, err: %s, retry time %d", redisKey, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
}

// When the key does not exist in redis, return nil
func getNodeCPUFlagsFromRedis(redisKey string) []string {
	for i := 0; i < redisRetryTimes; i++ {
		redisCli := client.GetRedisClient()
		if redisCli == nil {
			log.GetLogger().Warnf("[getNodeCPUFlagsFromRedis]redis is not ready")
			continue
		}
		nodeCPUFlags, err := redisCli.Get(context.TODO(), redisKey).Result()
		if err == nil {
			log.GetLogger().Debugf("Success to get key: %s, value: %s", redisKey, nodeCPUFlags)
			return strings.Split(nodeCPUFlags, ",")
		}
		log.GetLogger().Errorf("failed to get key: %s, err: %s, retry times %d", redisKey, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
	return nil
}

// Calculates the intersection and updates the node corresponding to the intersection
func compareNodeCPUFlags(nodeIP string, coreFlags []string, flagsMap map[string][]string,
	redisKey string) (int, string) {
	emptyFlags := ""
	if flagsMap == nil {
		flagsMap = make(map[string][]string)
	}
	nodeFlagList := getNodeCPUFlagsFromRedis(redisKey)
	if nodeFlagList != nil {
		intersectFlags := tools.ConstructIntersect(nodeFlagList, coreFlags)
		flagString := strings.Join(intersectFlags, ",") // Convert to comma-separated string
		flagsMap[flagString] = append(flagsMap[flagString], nodeIP)
		return len(flagsMap[flagString]), flagString
	}
	flagsMap[emptyFlags] = append(flagsMap[emptyFlags], nodeIP)
	return 0, emptyFlags
}

// DeleteNodeCPUInfoFromRedis When node is offline, deletes the record from redis to avoid dirty data
func DeleteNodeCPUInfoFromRedis(nodeIP string) {
	log.GetLogger().Infof("Start to delete node cpu info from redis, node ip: %s", nodeIP)
	clusterID := os.Getenv("CLUSTER_ID")
	fssCPUFlagsKey := fmt.Sprintf(fssCPUFeatureFormat, clusterID, nodeIP)
	for i := 0; i < redisRetryTimes; i++ {
		redisCli := client.GetRedisClient()
		if redisCli == nil {
			log.GetLogger().Warnf("[getNodeCPUFlagsFromRedis]redis is not ready")
			continue
		}
		_, err := redisCli.Del(context.TODO(), fssCPUFlagsKey).Result()
		if err == nil {
			log.GetLogger().Debugf("Success to delete key: %s", fssCPUFlagsKey)
			return
		}
		log.GetLogger().Errorf("failed to delete key: %s, err: %s, retry time %d", fssCPUFlagsKey, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
}

func reportWorkerManagerMetricsToRedis(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	log.GetLogger().Infof("start to report worker-manager metrics to redis")
	go subscribeColdStartMetrics(stopChan)
	reportTick := time.NewTicker(reportTickerTime * time.Minute)
	for {
		select {
		case <-reportTick.C:
			saveColdStartMetricsToRedis()
		case _, ok := <-stopChan:
			if !ok {
				reportTick.Stop()
				log.GetLogger().Infof("stop report worker-manager metrics to redis")
				return
			}
		}
	}
}

func getMigratingReportInterval() time.Duration {
	defaultInterval := time.Duration(defaultMigratingTickerMin) * time.Minute
	if migratingTickerMin == "" {
		return defaultInterval
	}
	tickerMin, err := strconv.Atoi(migratingTickerMin)
	if err != nil {
		log.GetLogger().Warnf("use default ticker time when failed to covert string to int for %s with err %s",
			migratingTickerMin, err.Error())
		return defaultInterval
	}
	if tickerMin <= 0 {
		return defaultInterval
	}
	return time.Duration(tickerMin) * time.Minute
}

func reportMigratingMetricsToRedis(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Warnf("stop channel is nil")
		return
	}
	interval := getMigratingReportInterval()
	log.GetLogger().Infof("start to report migrating metrics to redis with %s minute interval", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			// save migrating metrics to redis
			saveMigratingMetricsToRedis()
			// clean migrating metrics in cache after save to redis
			cleanMigratingMetrics()
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed")
				return
			}
		}
	}
}

func saveColdStartMetricsToRedis() {
	redisCli := client.GetRedisClient()
	if client == nil || redisCli == nil {
		log.GetLogger().Errorf("redis client is nil")
		return
	}
	totalMetrics.lock.Lock()
	if totalMetrics.totalCount <= 0 {
		totalMetrics.lock.Unlock()
		return
	}
	coldStartMetrics := ColdStartMetrics{}
	now := time.Now()
	coldStartMetrics.TimeStamp = now.Unix() - int64(now.Second())
	coldStartMetrics.Count = totalMetrics.totalCount
	coldStartMetrics.Delay = totalMetrics.totalDelay / float64(totalMetrics.totalCount)
	totalMetrics.totalCount = 0
	totalMetrics.totalDelay = 0
	totalMetrics.lock.Unlock()
	jsonData, err := json.Marshal(coldStartMetrics)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal coldStart metrics, err: %s", err.Error())
		return
	}
	err = redisclient.ZADDMetricsToRedis(coldStartKey, jsonData, maxValues, coldStartMetricsExpireMin*time.Minute)
	if err != nil {
		log.GetLogger().Errorf("ZADDMetricsToRedis error: %s", err.Error())
		return
	}
}

func saveMigratingMetricsToRedis() {
	redisCli := client.GetRedisClient()
	if client == nil || redisCli == nil {
		log.GetLogger().Errorf("redis client is nil")
		return
	}
	migratingMetrics.lock.Lock()
	migratingMetrics.TimeStamp = time.Now().Unix()
	reportMsg, err := json.Marshal(migratingMetrics)
	migratingMetrics.lock.Unlock()
	if err != nil {
		log.GetLogger().Warnf("failed to marshal migrating metrics with error %s", err.Error())
		return
	}
	err = redisclient.ZADDMetricsToRedis(migratingKey, reportMsg, maxValues, migratingMetricsExpireTime)
	if err != nil {
		log.GetLogger().Warnf("failed to add metrics to redis for %s with err %s", migratingKey, err.Error())
		return
	}
	log.GetLogger().Debugf("succeed to add migrating metrics to redis with %s, %s", migratingKey, string(reportMsg))
}

// PublishColdStartMetrics -
func PublishColdStartMetrics(metrics ColdStartMetrics) {
	if len(metricsChan) < bufferSize {
		metricsChan <- metrics
		return
	}
	if reachMaxChanCount%logLimitFrequency == 0 {
		reachMaxChanCount = 1
		log.GetLogger().Warnf("metricsChan reaches capacity and will discard the cold start metric statistics")
		return
	}
	reachMaxChanCount++
}

// subscribeMetrics -
func subscribeColdStartMetrics(stopChan <-chan struct{}) {
	if stopChan == nil {
		log.GetLogger().Warnf("stopChan is nil")
		return
	}
	for {
		select {
		case msg, ok := <-metricsChan:
			if !ok {
				log.GetLogger().Errorf("metrics channel is closed")
				return
			}
			totalMetrics.lock.Lock()
			totalMetrics.totalCount++
			totalMetrics.totalDelay += msg.Delay
			totalMetrics.lock.Unlock()
		case <-stopChan:
			log.GetLogger().Warnf("subscribeColdStartMetrics Received signal to quit")
			return
		}
	}
}
