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

// Package workerevaluator -
package workerevaluator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/types/workertypes"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/invokemetric"
	"functioncore/pkg/functionbus/utils"
)

const (
	// refer to time interval of pulling data(5min)
	asyncResponseKeyTTL              = 30 * time.Minute
	reportAsyncRespMaxRetryTimes     = 10
	fssMultipleConcurrentBillingData = "fss-multiple-concurrent-billing-data"
	fssReservedInstanceBillingData   = "fss-reserve-instance-billing-data"
	fssCPUFeatureFormat              = "fss-cpu-feature:%s:%s"
	redisContextTimeout              = 50 * time.Millisecond
	cacheDetectionInterval           = 5 * time.Minute
	redisReconnectionInternal        = 10 * time.Second
	reportCPUFlagInternal            = 5 * time.Minute
	reportDiskUsageInternal          = 10 * time.Minute
	// CPUFlagKeyTimeout refer to survival time of fss-cpu-feature:cluseterID:nodeIP
	CPUFlagKeyTimeout   = 24 * time.Hour
	diskUsageKeyTimeout = 15 * time.Minute

	int64Base    = 10
	int64BitSize = 64
)

var (
	// Client -
	Client                       = &metricsClient{}
	reportAsyncRespRetryInterval = 3 * time.Second
	clusterID                    = os.Getenv("CLUSTER_ID")
	nodeIP                       = os.Getenv("NODE_IP")
	nodeSpaceKey                 = fmt.Sprintf("/sn/metrics/space/%s/%s", os.Getenv(constants.ClusterID), nodeIP)
	uploadResourceKey            = fmt.Sprintf("fss-resource-metrics-%s", os.Getenv(constants.ClusterID))
	uploadCaeMetricsKey          = fmt.Sprintf("cae-resource-metrics-%s", os.Getenv(constants.ClusterID))
	uploadBridgeMetricsKey       = fmt.Sprintf("bridge-resource-metrics-%s", os.Getenv(constants.ClusterID))
	cronJobClient                = &cronJobMgr{}
)

type cronJobMgr struct {
	mgr      *cron.Cron
	entryID  *cron.EntryID
	stopChan <-chan struct{}
}

type metricsClient struct {
	config               *common.MetricsServerConfig
	c                    *redisclient.Client
	redisConnected       bool
	keepAliveTimeMetrics *keepAliveMetrics
}

type diskUsage struct {
	Filesystem string
	Size       string
	Used       string
	Avail      string
	Percentage string
	MountOn    string
}

type diskUsageMetrics struct {
	TimeStamp int64  `json:"timeStamp,omitempty"`
	Size      string `json:"size,omitempty"`
	Avail     string `json:"avail,omitempty"`
}

type keepAliveMetrics struct {
	dynamicKeepAliveTimeMap     map[string]string
	dynamicKeepAliveTimeMapLock sync.RWMutex
	keepaliveObserver           common.KeepAliveObserver
}

// RegisterKeepAliveObserver -
func (client *metricsClient) RegisterKeepAliveObserver(o common.KeepAliveObserver) {
	if client.keepAliveTimeMetrics == nil {
		log.GetLogger().Errorf("failed to register keep alive observer for bus, " +
			"err: client.keepAliveTimeMetrics is nil")
		return
	}
	client.keepAliveTimeMetrics.keepaliveObserver = o
}

func setReportAsyncRespRetryInterval(interval time.Duration) {
	reportAsyncRespRetryInterval = interval
}

// GetRedisClient -
func (client *metricsClient) GetRedisClient() *redisclient.Client {
	return client.c
}

// CreateMetricsClient method to create a monitoring Client
func CreateMetricsClient(c *common.MetricsServerConfig, stopCh <-chan struct{}) error {
	Client.config = c
	if err := Client.initClient(stopCh); err != nil {
		log.GetLogger().Errorf("failed to connect the metrics server %s, %s", c.ServerAddr, err.Error())
		alarm.GetClient().ReportAlarm(alarm.RedisConnection, fmt.Sprintf("busProxy failed to "+
			"connect redis, error msg=%s,", err.Error()))
	}
	go Client.checkRedisConnectivity(stopCh)
	go Client.reportFromPersistentCacheToRedis(stopCh)
	log.GetLogger().Infof("succeeded to connect the metrics server %s", c.ServerAddr)
	invokemetric.ReportInvokeResultMetric(Client.c, stopCh)
	initCronjobClient(stopCh)
	go Client.reportCPUFlagsToRedisTicker(stopCh)
	go Client.reportNodeDiskUsageTicker(stopCh)
	return nil
}

func initCronjobClient(stopCh <-chan struct{}) {
	cronJobClient.mgr = cron.New()
	cronJobClient.mgr.Start()
	cronJobClient.stopChan = stopCh
	RetrieveDynamicKeepAliveTimeForWorker(common.FConfig)
	common.RegisterAdjustKeepaliveDynamicallyCallBack(RetrieveDynamicKeepAliveTimeForWorker)
	go cronjobTearDown()
}

func cronjobTearDown() {
	select {
	case <-cronJobClient.stopChan:
		cronJobClient.mgr.Stop()
		log.GetLogger().Info("stop cronjob")
		return
	}
}

// RetrieveDynamicKeepAliveTimeForWorker periodically download Keep-Alive time from Redis
func RetrieveDynamicKeepAliveTimeForWorker(config *common.Configs) {
	if Client.c == nil {
		log.GetLogger().Errorf("err: redis client is nil | failed to start cronjob")
		return
	}
	if cronJobClient.stopChan == nil {
		log.GetLogger().Errorf("err: stopChan is nil | failed to start cronjob")
		return
	}
	if Client.keepAliveTimeMetrics == nil || Client.keepAliveTimeMetrics.keepaliveObserver == nil {
		log.GetLogger().Errorf("err: keepAliveTimeMetrics or keepaliveObserver is nil")
		return
	}
	if !config.DynamicKeepAliveConfig.Enable {
		log.GetLogger().Info("skip to register cronjob when config.DynamicKeepAliveConfig.Enable is false")
		return
	}
	if cronJobClient.entryID != nil {
		cronJobClient.mgr.Remove(*cronJobClient.entryID)
		log.GetLogger().Infof("remove cronjob entryID: %d", *cronJobClient.entryID)
	}
	cronJobStartTimeCron := config.DynamicKeepAliveConfig.StartTimeCron
	entryID, err := cronJobClient.mgr.AddFunc(cronJobStartTimeCron, func() {
		success := false
		attempts := -1
		threshold := config.DynamicKeepAliveConfig.Retry
		for !success && attempts < threshold {
			attempts++
			success = Client.dynamicAdjustKeepAliveTimeForWorker()
			if success {
				metrics := Client.keepAliveTimeMetrics
				if metrics.keepaliveObserver != nil {
					metrics.dynamicKeepAliveTimeMapLock.RLock()
					metrics.keepaliveObserver.UpdateKeepAliveTime(metrics.dynamicKeepAliveTimeMap)
					metrics.dynamicKeepAliveTimeMapLock.RUnlock()
				}
				log.GetLogger().Infof("succeed to run cronjob at attempt: %d", attempts)
				break
			}
			time.Sleep(time.Minute)
		}
	})
	if err != nil {
		log.GetLogger().Errorf("err: %s | failed to start cronjob", err.Error())
		return
	}
	cronJobClient.entryID = &entryID
	log.GetLogger().Infof("start a new cronjob entryID: %d, cron: %s", entryID, cronJobStartTimeCron)
	return
}

func (client *metricsClient) dynamicAdjustKeepAliveTimeForWorker() bool {
	if client.keepAliveTimeMetrics == nil {
		log.GetLogger().Errorf("keepAliveTimeMetrics is nil and failed to init keepalive metrics instance in " +
			"metrics client")
		return false
	}
	if !common.FConfig.DynamicKeepAliveConfig.Enable {
		return false
	}
	if client.c == nil {
		log.GetLogger().Errorf("redis is not ready")
		return false
	}
	client.keepAliveTimeMetrics.dynamicKeepAliveTimeMapLock.Lock()
	defer client.keepAliveTimeMetrics.dynamicKeepAliveTimeMapLock.Unlock()
	newDynamicKeepAliveTimeMap, err := client.c.HGetAll(context.TODO(), common.DynamicKeepAliveTimeRedisKey).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to get keepAliveTimeMetrics redis key: %s, err: %s",
			common.DynamicKeepAliveTimeRedisKey, err.Error())
		client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap = make(map[string]string)
		return false
	}
	latestUpdateTime, ok := newDynamicKeepAliveTimeMap[common.LastUpdateTimeKey]
	if !ok {
		log.GetLogger().Errorf("bad format for dynamicKeepAliveTimeMap which downloaded from Redis")
		client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap = make(map[string]string)
		return false
	}

	latestUpdateTimeInt, err := strconv.ParseInt(latestUpdateTime, int64Base, int64BitSize)
	if err != nil {
		log.GetLogger().Errorf("failed to parse lastUpdateTime, err: %s", err.Error())
		client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap = make(map[string]string)
		return false
	}
	gap := time.Now().Unix() - latestUpdateTimeInt
	if gap > common.DynamicKeepAliveTimeValidTimeWindow {
		client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap = make(map[string]string)
		log.GetLogger().Warnf("reset dynamicKeepAliveTimeMap because lastUpdateData is one hour ago")
		return false
	}
	client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap = newDynamicKeepAliveTimeMap
	log.GetLogger().Debugf("updated dynamicKeepAliveTimeMap: %v",
		client.keepAliveTimeMetrics.dynamicKeepAliveTimeMap)
	return true
}

func (client *metricsClient) initClient(stopCh <-chan struct{}) error {
	var err error
	clientConfig := client.config
	passwdByte, err := localauth.Decrypt(clientConfig.Password)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt password, %s", err.Error())
		return err
	}
	passwdString := string(passwdByte)
	commonutils.ClearByteMemory(passwdByte)
	client.keepAliveTimeMetrics = &keepAliveMetrics{
		dynamicKeepAliveTimeMap:     make(map[string]string),
		dynamicKeepAliveTimeMapLock: sync.RWMutex{},
	}
	client.c, err = redisclient.New(redisclient.NewRedisClientParam{
		ServerMode: clientConfig.ServerMode,
		ServerAddr: clientConfig.ServerAddr,
		Password:   passwdString,
		Timeout:    clientConfig.TimeoutConf,
	}, stopCh, redisclient.SetEnableTLS(clientConfig.EnableTLS), redisclient.AlarmSwitchOn(),
		redisclient.SetGetRealTimeServerAddrFunc(common.GetHotloadMetricsConf))
	if err != nil {
		log.GetLogger().Errorf("failed to new a redis Client, %s", err.Error())
		return err
	}
	Client.redisConnected = true
	redisclient.SetRedisCmd(client.c)
	return nil
}

func (client *metricsClient) checkRedisConnectivity(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	ticker := time.NewTicker(redisReconnectionInternal)
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debug("functionBus redis check redis connection start")
			if client.GetRedisClient() != nil {
				_, err := client.GetRedisClient().Ping(context.TODO()).Result()
				if err == nil {
					Client.redisConnected = true
					log.GetLogger().Debug("functionBus redis periodically checks availability")
					continue
				}
				client.GetRedisClient().Close()
			}
			if err := Client.initClient(stopCh); err != nil {
				log.GetLogger().Errorf("failed to init redis client, err:%s", err.Error())
				Client.redisConnected = false
				continue
			}
			Client.redisConnected = true
			log.GetLogger().Infof("functionBus redis connect succeed, clear the redis alarm")
			alarm.GetClient().ClearAlarm(alarm.RedisConnection)
		case <-stopCh:
			log.GetLogger().Errorf("functionBus process exit")
			ticker.Stop()
			return
		}
	}
}

func (client *metricsClient) reportFromPersistentCacheToRedis(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	ticker := time.NewTicker(cacheDetectionInterval)
	for {
		select {
		case <-ticker.C:
			if client.c == nil {
				log.GetLogger().Warnf("redis is not ready when report persistence cache to redis")
				continue
			}
			err := redisclient.TakeCacheToRedis(client.c)
			if err != nil {
				log.GetLogger().Errorf("failed to flush cache to redis, err: %s", err.Error())
			}
		case <-stopCh:
			return
		}
	}
}

func (client *metricsClient) processRedisCacheInfoToDisk(redisKey, value string) error {
	redisCache := redisclient.RedisCache{Method: "push",
		Key: redisKey, Value: value}
	writeErr := redisclient.WriteToDisk(redisCache)
	if writeErr != nil {
		log.GetLogger().Errorf("failed to write redis cache to disk, %s", writeErr.Error())
		return writeErr
	}
	return nil
}

func (client *metricsClient) pushBillingDataToRedis(redisKey, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	if client.c == nil {
		client.redisConnected = false
		log.GetLogger().Errorf("redis is not ready when pushBillingDataToRedis")
		return client.processRedisCacheInfoToDisk(redisKey, value)
	}
	if _, err := client.c.RPush(ctx, redisKey, value).Result(); err != nil {
		log.GetLogger().Errorf("failed to RPush key: %s data: %s, err: %s, try to persist data to disk",
			redisKey, value, err.Error())
		client.redisConnected = false
		return client.processRedisCacheInfoToDisk(redisKey, value)
	}
	return nil
}

func (client *metricsClient) ReportUsedResourceTimeWindows(metricsProperty common.RuntimeMetricsProperty,
	funcKeyWithResource string) error {
	funcVersionURN, err := urnutils.GetFunctionInfo(metricsProperty.FunctionURN)
	if err != nil {
		log.GetLogger().Errorf("URN %s in wrong format: %s", metricsProperty.FunctionURN, err.Error())
		return err
	}
	data, err := getUsedResourceTimeWindowsData(metricsProperty, funcVersionURN, funcKeyWithResource)
	if err != nil {
		log.GetLogger().Errorf("failed to get usedResourceTimeWindowsData, %s", err.Error())
		return err
	}

	// For a triggered bridge function, MetricsUsedResourceTimeWindow is used regardless of the metric name.
	if metricsProperty.MetricsName == constants.MetricsUsedResourceTimeWindow ||
		metricsProperty.IsTriggeredBridgeFunction {
		err := client.pushBillingDataToRedis(fssMultipleConcurrentBillingData, data)
		if err != nil {
			log.GetLogger().Errorf("failed to push multiple concurrent billing data to Redis, %s", err.Error())
			return err
		}
	} else if metricsProperty.MetricsName == constants.ReservedInstanceMetricsTimeWindow {
		err := client.pushBillingDataToRedis(fssReservedInstanceBillingData, data)
		if err != nil {
			log.GetLogger().Errorf("failed to push reserved instances billing data to Redis, %s", err.Error())
			return err
		}
	}
	/* after the next successful report, if the connection status is abnormal,
	the system attempts to restore data from the disk.
	*/
	if !client.redisConnected {
		err := redisclient.TakeCacheToRedis(client.c)
		if err != nil {
			log.GetLogger().Errorf("failed to take cache to redis, error: %s", err.Error())
			return err
		}
		client.redisConnected = true
	}
	log.GetLogger().Debugf("succeeded to report usedResourceTimeWindows %s ", data)
	return nil
}

func getUsedResourceTimeWindowsData(metricsProperty common.RuntimeMetricsProperty,
	funcVersionURN urnutils.BaseURN, funcKeyWithResource string) (string, error) {
	resource := utils.GetResourceFromFuncKeyWithResource(funcKeyWithResource)
	memory := utils.GetMemoryFromFuncKeyResource(resource)
	chargeType := ""
	if metricsProperty.IsBridgeFunction {
		chargeType = "bridge"
	}
	if metricsProperty.MetricsName == constants.MetricsUsedResourceTimeWindow {
		usedResourceTimeWindows := UsedResourceTimeWindows{
			FunctionUrn:      urnutils.GetBillingFunctionURN(funcVersionURN, resource.Memory),
			PodName:          metricsProperty.PodName,
			Memory:           memory,
			GPUMemory:        int(metricsProperty.GPUMem),
			EphemeralStorage: int(metricsProperty.EphemeralStorage),
			Type:             chargeType,
		}
		var timeWindows []workertypes.TimeWindow
		if err := json.Unmarshal([]byte(metricsProperty.MetricsValue), &timeWindows); err != nil {
			log.GetLogger().Errorf("failed to json unmarshal used resource timeWindows, %s", err.Error())
			return "", err
		}
		usedResourceTimeWindows.TimeWindows = timeWindows
		data, err := json.Marshal(usedResourceTimeWindows)
		if err != nil {
			log.GetLogger().Errorf("failed to json marshal usedResourceTimeWindows, %s", err.Error())
			return "", err
		}
		return string(data), nil
	}
	reservedInstanceTimeWindows := ReservedInstanceTimeWindows{
		FunctionUrn:      urnutils.GetBillingFunctionURN(funcVersionURN, resource.Memory),
		PodName:          metricsProperty.PodName,
		ClusterID:        clusterID,
		Memory:           memory,
		GPUMemory:        int(metricsProperty.GPUMem),
		EphemeralStorage: int(metricsProperty.EphemeralStorage),
		Type:             chargeType,
	}
	var resTimeWindows []workertypes.TimeWindow
	if err := json.Unmarshal([]byte(metricsProperty.MetricsValue), &resTimeWindows); err != nil {
		log.GetLogger().Errorf("failed to json unmarshal reserved instance timeWindows, %s", err.Error())
		return "", err
	}
	reservedInstanceTimeWindows.TimeWindows = resTimeWindows
	data, err := json.Marshal(reservedInstanceTimeWindows)
	if err != nil {
		log.GetLogger().Errorf("failed to json marshal reservedInstanceTimeWindows, %s", err.Error())
		return "", err
	}
	return string(data), nil
}

// UsedResourceTimeWindows is used for single-instance multi-concurrency charging windows.
type UsedResourceTimeWindows struct {
	FunctionUrn      string                   `json:"functionURN"`
	PodName          string                   `json:"podName"`
	Memory           int                      `json:"mem"`
	GPUMemory        int                      `json:"gpuMem"`
	EphemeralStorage int                      `json:"ephemeralStorage"`
	TimeWindows      []workertypes.TimeWindow `json:"timeWindows"`
	Type             string                   `json:"type"`
}

// ReservedInstanceTimeWindows is used to reserve the charging window of the instance.
type ReservedInstanceTimeWindows struct {
	FunctionUrn      string                   `json:"functionURN"`
	PodName          string                   `json:"podName"`
	ClusterID        string                   `json:"clusterID"`
	Memory           int                      `json:"mem"`
	GPUMemory        int                      `json:"gpuMem"`
	EphemeralStorage int                      `json:"ephemeralStorage"`
	TimeWindows      []workertypes.TimeWindow `json:"timeWindows"`
	Type             string                   `json:"type"`
}

type asyncResponse struct {
	StatusCode int               `json:"statusCode"`
	Body       []byte            `json:"body"`
	Headers    map[string]string `json:"headers"`
}

func (client *metricsClient) reportAsyncResponse(id string, resp *asyncResponse) error {
	res, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	log.GetLogger().Debugf("report async response: %s", res)
	for i := 0; i < reportAsyncRespMaxRetryTimes; i++ {
		if client.c == nil {
			err = errors.New("redis is not ready")
			log.GetLogger().Infof("redis is not ready, traceID: %s", id)
			continue
		}
		// key: "fss-async-invoke-result:${x-request-id}"
		if _, err = client.c.Set(context.TODO(), fmt.Sprintf("fss-async-invoke-result:%s", id),
			res, asyncResponseKeyTTL).Result(); err == nil {
			log.GetLogger().Debugf("succeed to report async response, traceID: %s", id)
			if !client.redisConnected {
				err := redisclient.TakeCacheToRedis(client.c)
				if err != nil {
					log.GetLogger().Errorf("failed to take cache to redis,with error: %s", err.Error())
				}
			}
			return nil
		}
		time.Sleep(reportAsyncRespRetryInterval)
	}
	client.redisConnected = false
	log.GetLogger().Warnf("failed to report async to redis,the data,%s.", err.Error())
	redisCache := redisclient.RedisCache{Method: "set",
		Key: fmt.Sprintf("fss-async-invoke-result:%s", id), Value: string(res)}
	clientErr := redisclient.WriteToDisk(redisCache)
	if clientErr != nil {
		log.GetLogger().Errorf("failed to write redis cache to disk, %s", clientErr.Error())
		return clientErr
	}
	return err
}

func (client *metricsClient) reportCPUFlagsToRedisTicker(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Infof("reportCPUFlagsToRedisTicker not start")
		return
	}
	// Immediate operation
	reportCPUFlagsToRedis(client.GetRedisClient())
	ticker := time.NewTicker(reportCPUFlagInternal)
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debugf("Start run task [reportCpuFlagsToRedis]")
			reportCPUFlagsToRedis(client.GetRedisClient())
		case <-stopCh:
			ticker.Stop()
			log.GetLogger().Errorf("[reportCpuFlagsToRedis]functionBus process exit")
			return
		}
	}
}

func reportCPUFlagsToRedis(c *redisclient.Client) {
	if c == nil {
		log.GetLogger().Errorf("redis is not ready when save to redis")
		return
	}
	cpuFlags := utils.GetCPUFlagList()
	if cpuFlags != nil {
		flagString := strings.Join(cpuFlags, ",") // Convert to comma-separated string
		for i := 0; i < common.DefaultRetryTimes; i++ {
			if c == nil {
				log.GetLogger().Errorf("redis is not ready when save to redis")
				continue
			}
			// redis key is fss-cpu-feature:{cluster-name}:{node-name}
			fssCPUFlagsKey := fmt.Sprintf(fssCPUFeatureFormat, clusterID, nodeIP)
			err := c.Set(context.TODO(), fssCPUFlagsKey, flagString, CPUFlagKeyTimeout).Err()
			if err == nil {
				log.GetLogger().Debugf("Success to set key: %s, value: %s", fssCPUFlagsKey, flagString)
				break
			}
			log.GetLogger().Errorf("failed to set key: %s, err: %s , retry times %d",
				fssCPUFlagsKey, err.Error(), i)
			time.Sleep(common.DefaultRetryInterval)
		}
	} else {
		log.GetLogger().Warnf("No CPU flags retrieved.")
	}
}

func reportNodeDiskUsageToRedis(c *redisclient.Client) {
	if c == nil {
		log.GetLogger().Errorf("redis is not ready when save to redis")
		return
	}
	nodeDiskUsage, err := getDiskUsage()
	if err != nil {
		log.GetLogger().Errorf("failed to get node disk usage, error: %s", err.Error())
		return
	}
	metrics := &diskUsageMetrics{
		TimeStamp: time.Now().Unix(),
		Size:      nodeDiskUsage.Size,
		Avail:     nodeDiskUsage.Avail,
	}
	data, err := json.Marshal(metrics)
	if err != nil {
		log.GetLogger().Errorf("function json marshal response error:%s", err.Error())
		return
	}
	for i := 0; i < common.DefaultRetryTimes; i++ {
		err = c.Set(context.TODO(), nodeSpaceKey, data, diskUsageKeyTimeout).Err()
		if err == nil {
			log.GetLogger().Debugf("Success to set key: %s, value: %s", nodeSpaceKey, data)
			break
		}
		log.GetLogger().Errorf("failed to set node disk usage to redis, error: %s", err.Error())
		time.Sleep(common.DefaultRetryInterval)
	}
}

func (client *metricsClient) reportNodeDiskUsageTicker(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Infof("reportNodeDiskUsageTicker not start")
		return
	}
	ticker := time.NewTicker(reportDiskUsageInternal)
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debugf("Start run task [reportNodeDiskUsageTicker]")
			reportNodeDiskUsageToRedis(client.GetRedisClient())
		case <-stopCh:
			ticker.Stop()
			log.GetLogger().Warnf("[reportNodeDiskUsageTicker]stop channel is closed")
			return
		}
	}
}

// Filesystem      Size       Used    Available  Use%  Mounted on
// /dev/sda1       1048576     512     1048064    1%   /
// /dev/sdb1       2097152    1024     2096128    1%   /data
func parseDiskUsage(output string) diskUsage {
	lines := strings.Split(output, "\n")
	for i := 1; i < len(lines); i++ {
		fields := strings.Fields(lines[i])
		if fields[5] == "/" {
			return diskUsage{
				Filesystem: fields[0],
				Size:       fields[1],
				Used:       fields[2],
				Avail:      fields[3],
				Percentage: fields[4],
				MountOn:    fields[5],
			}
		}
	}
	return diskUsage{}
}

func getDiskUsage() (diskUsage, error) {
	cmd := exec.Command("df", "-k")
	output, err := cmd.Output()
	if err != nil {
		return diskUsage{}, err
	}
	usage := parseDiskUsage(string(output))
	return usage, err
}

// ReportResourceUsageToRedis -
func ReportResourceUsageToRedis(c *redisclient.Client, resourceBytes []byte) {
	reportMetricsToRedis(c, uploadResourceKey, resourceBytes)
}

// ReportCaeMetricsToRedis -
func ReportCaeMetricsToRedis(c *redisclient.Client, caeMetricsBytes []byte) {
	reportMetricsToRedis(c, uploadCaeMetricsKey, caeMetricsBytes)
}

// ReportBridgeMetricsToRedis -
func ReportBridgeMetricsToRedis(c *redisclient.Client, bridgeMetricsBytes []byte) {
	reportMetricsToRedis(c, uploadBridgeMetricsKey, bridgeMetricsBytes)
}

// reportMetricsToRedis metrics in redis: {redisKey: {nodeIP: value}}
func reportMetricsToRedis(c *redisclient.Client, redisKey string, redisValueBytes []byte) {
	if c == nil {
		log.GetLogger().Errorf("redis is not ready when save resource usage to redis")
		return
	}

	if len(redisValueBytes) == 0 {
		log.GetLogger().Warnf("redisValueBytes is empty, do nothing")
		return
	}

	for i := 0; i < common.DefaultRetryTimes; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
		_, err := c.HSet(ctx, redisKey, nodeIP, redisValueBytes).Result()
		cancel()
		if err == nil {
			log.GetLogger().Debugf("Success to set redis key: %s, hash key: %s, value: %s",
				redisKey, nodeIP, redisValueBytes)
			break
		}
		log.GetLogger().Warnf("failed to set redis key: %s, hash key: %s, value: %s, err: %s",
			redisKey, nodeIP, redisValueBytes, err.Error())
		time.Sleep(common.DefaultRetryInterval)
	}
}
