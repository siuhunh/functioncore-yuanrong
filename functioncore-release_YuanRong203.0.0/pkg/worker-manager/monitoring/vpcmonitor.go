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

package monitoring

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	tools "functioncore/pkg/worker-manager/utils"
)

const (
	patPodPrefix        = "fss/patvm/fss-patvm-"
	monitorInterval     = time.Minute
	nodeNicNum          = 7
	podIdClusterIndex   = 1
	nicPercentThreshold = 90
	percent             = 100
	defaultNicNum       = 10
	redisRetryTimes     = 3
	redisRetryInterval  = 100 * time.Millisecond
)

var (
	clusterID = os.Getenv("CLUSTER_ID")
)

type patPod struct {
	IsUsed  bool   `json:"is_used"`
	MacAddr string `json:"t_mac_addr"`
	PodId   string `json:"pat_pod_id"`
}

func getPatPodFromETCD() ([]string, error) {
	var (
		values []string
		err    error
	)
	for i := 0; i < tools.MaxEtcdRetryAttempts; i++ {
		if etcd3.GetMetaEtcdClient().Client == nil {
			log.GetLogger().Warnf("failed to init meta ETCD client, retry times: %d", i+1)
			time.Sleep(time.Second)
			continue
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		values, err = etcd3.GetMetaEtcdClient().GetValues(defaultEtcdCtx, etcd3.HandleCellPrefix(patPodPrefix),
			clientv3.WithPrefix())
		if err != nil {
			log.GetLogger().Warnf("failed to get pat pod %s info from etcd with error %s, retry times: %d",
				patPodPrefix, err.Error(), i+1)
			time.Sleep(tools.RetryInterval)
			continue
		}
		break
	}
	return values, err
}

func getNicNum() (int, error) {
	pods, err := getPatPodFromETCD()
	if err != nil {
		return 0, err
	}
	var nic = make(map[string]int, defaultNicNum)
	for index := range pods {
		var pod = &patPod{}
		err := json.Unmarshal([]byte(pods[index]), pod)
		if err != nil {
			log.GetLogger().Warnf("failed to unmarshal, err: %s", err.Error())
			continue
		}
		// "pat_pod_id" = "fss-pat:cce-cluster-yuanrong03:default:cff-pat-pod-2a290840-4197-4516-9d2:192.168.129.97"
		id := strings.Split(pod.PodId, ":")
		if len(id) < podIdClusterIndex+1 || id[podIdClusterIndex] != clusterID {
			continue
		}
		if pod.IsUsed {
			nic[pod.MacAddr]++
		}
	}
	return len(nic), nil
}

func getNodeNum() (int, error) {
	zSetKey := fmt.Sprintf("fss-agent-{%s}-pod", clusterID)
	redisCli := client.GetRedisClient()
	if client == nil || redisCli == nil {
		log.GetLogger().Warn("redis client is nil")
		return 0, errors.New("redis client is nil")
	}
	nodeNum, err := redisCli.ZCard(context.TODO(), zSetKey).Result()
	if err != nil {
		log.GetLogger().Warnf("failed to get pat node number, err: %s", err.Error())
	}
	return int(nodeNum), err
}

func getNicUsage() (int, error) {
	nicNum, err := getNicNum()
	if err != nil {
		return 0, err
	}
	saveNicNumToRedis(nicNum)
	nodeNum, err := getNodeNum()
	if err != nil || nodeNum == 0 {
		return 0, err
	}
	return (nicNum * percent) / (nodeNum * nodeNicNum), nil
}

func saveNicNumToRedis(nicNum int) {
	nicKey := fmt.Sprintf("fss-nic-count-%s", clusterID)
	redisCli := client.GetRedisClient()
	if client == nil || redisCli == nil {
		log.GetLogger().Warn("redis client is nil")
		return
	}
	setRedisKeyWithRetry(nicKey, nicNum)
}

func initVpcMonitor(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Warn("stop channel is nil")
		return
	}
	ticker := time.NewTicker(monitorInterval)
	log.GetLogger().Infof("start to monitor vpc nic")
	for {
		select {
		case <-ticker.C:
			usage, err := getNicUsage()
			if err != nil {
				log.GetLogger().Warnf("failed to get vpc nic usage, err: %s", err.Error())
				continue
			}
			msg := fmt.Sprintf("VpcNicUsage=%d%%", usage)
			if usage > nicPercentThreshold {
				log.GetLogger().Warn(msg)
				alarm.GetClient().ReportAlarm(alarm.VPCExceededThreshold, msg)
			} else {
				log.GetLogger().Debug(msg)
				alarm.GetClient().ClearAlarm(alarm.VPCExceededThreshold)
			}
		case <-stopCh:
			log.GetLogger().Infof("vpc monitor graceful exit")
			ticker.Stop()
			return
		}
	}
}

func checkVpcAndReport(stopCh <-chan struct{}) {
	alarm.GetClient().RegisterAlarmType(alarm.VPCExceededThreshold)
	go initVpcMonitor(stopCh)
}
