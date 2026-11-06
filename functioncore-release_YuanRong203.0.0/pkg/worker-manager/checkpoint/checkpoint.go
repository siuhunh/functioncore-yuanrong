/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package checkpoint provides user process checkpoint
package checkpoint

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

	fhttp "github.com/valyala/fasthttp"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/iam"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
)

const (
	watchPathPrefix    = "/snapshot/business/yrk/cluster/"
	queryResultTimeout = 3 * time.Second
	workerPort         = "8081"

	defaultCheckpointRetryTimes = 3
	// consider function meta recovered
	defaultCheckpointRetryInterval = 3 * time.Second
	defaultSubmitResultRetryTimes  = 3
	defaultWaitPodReadyTimeout     = 60
	defaultCheckpointAlarmInterval = 10
	defaultAlarmThresholdNumber    = 10
	checkpointAlarmIntervalEnvKey  = "CHECKPOINT_ALARM_INTERVAL"

	// default retry for 5min
	defaultFailureRetryInterval  = 10
	defaultFailureRetryTimes     = 30
	defaultGetFuncMetaRetryTimes = 18

	defaultMaxConcurrency        = 10
	defaultSnapshotVersionLength = 4
	defaultLogTime               = 5

	versionSeparator   = ".VERSION"
	succeedStatus      = "SUCCEED"
	failedStatus       = "FAILED"
	checkpointCostTime = 5

	redisCxtTimeout       = 1 * time.Second
	redisRetryInterval    = 1 * time.Second
	redisConnRetryTimes   = 30
	redisReqRetryTime     = 3
	snapshotRedisKey      = "/snapshot/%s/%s"
	snapshotKeyExpireTime = 5 * time.Minute
	maxHistorySnapshotNum = 1

	eventCancel                = "cancel"
	eventDone                  = "done"
	eventUpgrade               = "upgrade"
	defaultCancelRetryInterval = 3 * time.Second
	defaultCancelRetryTimes    = 1

	snapshotQueueAlarmTime = 2 * time.Minute
)

var clusterID = os.Getenv("CLUSTER_ID")
var (
	checkpointMgr      *manager
	checkpointMgrGuard sync.Mutex
	logProgressLast    float64
	errLockRedisKey    = errors.New("redis key operate failed with lock")
	errConnRedisClient = errors.New("redis client is empty or not ready")
)

type manager struct {
	workerClient *fhttp.Client
	VpcHandler   *vpcmanager.VpcHandler
	cfg          *config.FaasConfig
	wg           *sync.WaitGroup
	start        time.Time
	finishNum    int64
	successNum   int64
	failedNum    int64
	totalNum     int64
	summary      sync.Map
	needSnapshot sync.Map
	ConcurrencyLimiter
	redisCli          *redisclient.Client
	needCheckPointMap map[string]*types.SnapshotDeploymentSpec
}

// InitManager init manager and enable checkpoint function
func InitManager(workerClient *fhttp.Client, stopCh <-chan struct{}) error {
	if clusterID == "" {
		return fmt.Errorf("no cluster id found")
	}

	vpcController, err := vpcmanager.GetVPCController()
	if err != nil {
		return err
	}
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	initCheckpointPodConfig(cfg)
	maxConcurrency := cfg.PoolMgrConfig.CheckpointPodConfig.MaxConcurrency
	checkpointMgr = &manager{
		workerClient:       workerClient,
		VpcHandler:         vpcController,
		cfg:                cfg,
		wg:                 &sync.WaitGroup{},
		ConcurrencyLimiter: CreateConcurrencyLimiter(maxConcurrency),
		needCheckPointMap:  make(map[string]*types.SnapshotDeploymentSpec),
		redisCli:           monitoring.GetRedisClient(),
	}
	if !snapshotmanager.GetSnapshotManagerMode() {
		// watch request from etcd
		checkpointMgr.runWatcher(stopCh)
		go checkpointMgr.monitorCheckpointPods(stopCh)
	} else {
		checkpointMgr.runUpdating()
		checkpointMgr.wg.Wait()
	}
	return nil
}

func (m *manager) monitorCheckpointPods(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	monitorInterval := getMonitorInterval()
	log.GetLogger().Infof("checkpoint alarm interval set to %d min", monitorInterval)
	ticker := time.NewTicker(time.Duration(monitorInterval) * time.Minute)
	isAlarmed := false
	for {
		select {
		case <-stopCh:
			log.GetLogger().Warnf("stop channel is closed")
			ticker.Stop()
			return
		case <-ticker.C:
			failedNum := m.getSummaryContByValue(failedStatus)
			log.GetLogger().Debugf("there are %d functions failed and is alarmed: %t", failedNum, isAlarmed)
			if failedNum > defaultAlarmThresholdNumber && !isAlarmed {
				isAlarmed = true
				alarm.GetClient().ReportAlarm(alarm.CheckpointAbnormalContinuously,
					fmt.Sprintf("error msg=failed checkpoint pods num is %d,", failedNum))
				m.printSummary()
			} else if failedNum == 0 && isAlarmed {
				isAlarmed = false
				alarm.GetClient().ClearAlarm(alarm.CheckpointAbnormalContinuously)
			}
			m.resetSummaryContByValue(failedStatus)
		}
	}
}

func getMonitorInterval() int {
	monitorInterval := defaultCheckpointAlarmInterval
	checkpointAlarmIntervalStr := os.Getenv(checkpointAlarmIntervalEnvKey)
	interval, err := strconv.Atoi(checkpointAlarmIntervalStr)
	if err == nil && interval > 0 {
		monitorInterval = interval
	}
	return monitorInterval
}

func (m *manager) resetSummaryContByValue(targetValue string) {
	m.summary.Range(func(key, value interface{}) bool {
		if value == targetValue {
			m.summary.Delete(key)
		}
		return true
	})
}

func (m *manager) getSummaryContByValue(targetValue string) int {
	count := 0
	m.summary.Range(func(key, value interface{}) bool {
		if value == targetValue {
			count++
		}
		return true
	})
	return count
}

func initCheckpointPodConfig(cfg *config.FaasConfig) {
	// initCheckpointPodConfig
	if cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryInterval <= 0 {
		cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryInterval = defaultFailureRetryInterval
	}
	if cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryTimes <= 0 {
		cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryTimes = defaultFailureRetryTimes
	}

	if cfg.PoolMgrConfig.CheckpointPodConfig.MaxConcurrency <= 0 {
		cfg.PoolMgrConfig.CheckpointPodConfig.MaxConcurrency = defaultMaxConcurrency
	}

	if cfg.PoolMgrConfig.CheckpointPodConfig.WaitReadyTimeout <= 0 {
		cfg.PoolMgrConfig.CheckpointPodConfig.WaitReadyTimeout = defaultWaitPodReadyTimeout
	}
	return
}

func (m *manager) logProgress() {
	log.GetLogger().Infof("ReCheckpoint | %s | %d snapshot need to update", m.getRuntimeManagerImageTag(),
		atomic.LoadInt64(&m.totalNum))
	if atomic.LoadInt64(&m.totalNum) == 0 {
		log.GetLogger().Infof("ReCheckpoint | %s | No need to update snapshot, end ReCheckPoint",
			m.getRuntimeManagerImageTag())
		m.wg.Done()
		return
	}
	// progress logger survive time is totalNum * 5 min / concurrencyLimit or 10min
	if atomic.LoadInt64(&m.totalNum) > 1 {
		logProgressLast = float64(atomic.LoadInt64(&m.totalNum)*checkpointCostTime) / float64(m.ConcurrencyLimiter.limit)
	} else {
		logProgressLast = 10
	}
	// avoid logger survive time is too short
	if logProgressLast < checkpointCostTime {
		logProgressLast = checkpointCostTime
	}

	log.GetLogger().Infof("ReCheckpoint | %s | log progress will run up to %f mins",
		m.getRuntimeManagerImageTag(), logProgressLast)
	for {
		if atomic.LoadInt64(&m.finishNum) > 0 {
			avgTime := time.Since(m.start).Seconds() / float64(atomic.LoadInt64(&m.finishNum))
			leftTime := avgTime * float64(atomic.LoadInt64(&m.totalNum)-atomic.LoadInt64(&m.finishNum))
			log.GetLogger().Infof("ReCheckpoint | %s | Snapshot is updating. total is %d, %d snapshot is finished,"+
				" %d is not finished, %d is successful, %d is failed. Update avg time %.2f s, %.2f s left to finish",
				m.getRuntimeManagerImageTag(), atomic.LoadInt64(&m.totalNum), atomic.LoadInt64(&m.finishNum),
				atomic.LoadInt64(&m.totalNum)-atomic.LoadInt64(&m.finishNum),
				atomic.LoadInt64(&m.finishNum)-atomic.LoadInt64(&m.failedNum),
				atomic.LoadInt64(&m.failedNum), avgTime, leftTime)
		}
		if atomic.LoadInt64(&m.totalNum) == atomic.LoadInt64(&m.finishNum) {
			log.GetLogger().Infof("ReCheckpoint | %s | %.2f s | Updating snapshot is finished. Total task is %d, "+
				"%d is successful, %d is failed", m.getRuntimeManagerImageTag(), time.Since(m.start).Seconds(),
				atomic.LoadInt64(&m.totalNum), atomic.LoadInt64(&m.finishNum)-atomic.LoadInt64(&m.failedNum),
				atomic.LoadInt64(&m.failedNum))
			m.wg.Done()
			break
		}
		if time.Since(m.start).Minutes() >= logProgressLast {
			log.GetLogger().Infof("ReCheckpoint | %s | %.2f s | Updating Snapshot is not finished. "+
				"Total task is %d, %d is successful, %d is failed, %d is not finished, Time out and exit",
				m.getRuntimeManagerImageTag(), time.Since(m.start).Seconds(), atomic.LoadInt64(&m.totalNum),
				atomic.LoadInt64(&m.finishNum)-atomic.LoadInt64(&m.failedNum), atomic.LoadInt64(&m.failedNum),
				atomic.LoadInt64(&m.totalNum)-atomic.LoadInt64(&m.finishNum))
			m.wg.Done()
			break
		}
		// print progress every 5s
		time.Sleep(defaultLogTime * time.Second)
	}
	// print re-checkpoint summary
	m.printSummary()
}

func (m *manager) printSummary() {
	var success []string
	var failed []string
	m.summary.Range(func(key, value interface{}) bool {
		if value == succeedStatus {
			success = append(success, key.(string))
		} else if value == failedStatus {
			failed = append(failed, key.(string))
		}
		return true
	})

	successStr := strings.Join(success, ",")
	failedStr := strings.Join(failed, ",")
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("ReCheckpoint Summary | %s | %d | %d | %d |Summary succeed functions: [%s], failed functions:"+
			" [%s]", m.getRuntimeManagerImageTag(), atomic.LoadInt64(&m.totalNum),
			atomic.LoadInt64(&m.finishNum)-atomic.LoadInt64(&m.failedNum), atomic.LoadInt64(&m.failedNum),
			successStr, failedStr)
	} else {
		log.GetLogger().Infof("Summary succeed functions: [%s], failed functions: [%s]", successStr, failedStr)
	}
}

// GetCheckpointManager returns the checkpoint manager
func GetCheckpointManager() *manager {
	return checkpointMgr
}

type duration struct {
	receiveRequest        time.Time
	startGeneralizePod    time.Time
	finishedGeneralizePod time.Time
}

func (d *duration) print(setResponseTime time.Time) string {
	return fmt.Sprintf("totalTime:%v|queuedTime:%v|generalizePodTime:%v|queryCheckpointResultTime:%v",
		setResponseTime.Sub(d.receiveRequest), d.startGeneralizePod.Sub(d.startGeneralizePod),
		d.finishedGeneralizePod.Sub(d.startGeneralizePod), setResponseTime.Sub(d.finishedGeneralizePod))
}

type snapshotEtcdKey struct {
	businessID  string
	tenantID    string
	funcName    string
	funcVersion string
	snapshotID  string
	clusterID   string
}

// SnapshotEvent -
type SnapshotEvent struct {
	snapshotEtcdKey snapshotEtcdKey
	snapshotResult  *types.SnapshotDeploymentSpec
	resultKey       string
	etcdType        etcd3.EtcdType
	needDelSnapshot bool
	action          string
	uuid            string
}

const (
	keyElementsLength = 14
	keepConnAliveTTL  = 3 * time.Second
)

// parseFromKey: key example as follow
// /snapshot/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{name}/version/{ver}/id/{snapshotID}
func parseFromKey(key string) snapshotEtcdKey {
	elements := strings.Split(key, "/")
	if len(elements) != keyElementsLength {
		return snapshotEtcdKey{}
	}
	return snapshotEtcdKey{
		businessID:  elements[3],
		clusterID:   elements[5],
		tenantID:    elements[7],
		funcName:    elements[9],
		funcVersion: elements[11],
		snapshotID:  elements[13],
	}
}

func anonymousKey(key string) string {
	const tenantIDIndex = 7
	elements := strings.Split(key, "/")
	if len(elements) != keyElementsLength {
		return key
	}
	elements[tenantIDIndex] = urnutils.Anonymize(elements[tenantIDIndex])
	return strings.Join(elements, "/")
}

func parseCheckpointEvent(event *etcd3.Event, etcdType etcd3.EtcdType) *SnapshotEvent {
	switch event.Type {
	case etcd3.DELETE:
		log.GetLogger().Infof("receive checkpoint delete event type, key: %s", anonymousKey(event.Key))
		eventObj := &SnapshotEvent{}
		eventObj.resultKey = event.Key
		eventObj.etcdType = etcdType
		keyObj := parseFromKey(event.Key)
		if keyObj.funcName == "" {
			log.GetLogger().Errorf("failed to parse from etcd event key: %s", anonymousKey(event.Key))
			return eventObj
		}
		eventObj.snapshotEtcdKey = keyObj
		return eventObj
	case etcd3.PUT:
		log.GetLogger().Infof("receive checkpoint put event, key: %s", anonymousKey(event.Key))
		eventObj := &SnapshotEvent{}
		eventObj.resultKey = event.Key
		eventObj.etcdType = etcdType
		keyObj := parseFromKey(event.Key)
		if keyObj.funcName == "" {
			log.GetLogger().Errorf("failed to parse from etcd event key: %s", anonymousKey(event.Key))
			return eventObj
		}
		eventObj.snapshotEtcdKey = keyObj
		snapshotDeployment := &types.SnapshotDeploymentSpec{}
		if err := json.Unmarshal(event.Value, snapshotDeployment); err != nil {
			log.GetLogger().Warnf("failed to unmarshal the json, error: %s", err.Error())
			return eventObj
		}
		eventObj.snapshotResult = snapshotDeployment
		log.GetLogger().Infof("checkpoint funcName: %s, event: %s, taskID: %s.",
			keyObj.funcName, snapshotDeployment.Event, snapshotDeployment.TaskID)
		return eventObj
	default:
		log.GetLogger().Infof("unconcerned event type: %d", event.Type)
		return &SnapshotEvent{}
	}
}

func (m *manager) needCheckpointEvent(snapshotEvent *SnapshotEvent) bool {
	if snapshotEvent.snapshotEtcdKey.clusterID != utils.GetEnvClusterID() {
		return false
	}
	// upgrade event contains snapshotVersions. put it into 'finishedCheckpointEvent' function will get false as return
	if snapshotEvent.snapshotResult != nil && snapshotEvent.snapshotResult.Event == eventUpgrade {
		currentImageTag := m.getRuntimeManagerImageTag()
		snapshotCheckInfo := m.cfg.PoolMgrConfig.MultiSnapshotConfig.SnapshotCheckInfo
		keyObj := snapshotEvent.snapshotEtcdKey
		funcSvcID := utils.CrName(keyObj.businessID, keyObj.tenantID, keyObj.funcName, keyObj.funcVersion)
		existNewestVersion, existMatchVersion := m.judgeSnapshotVersion(snapshotEvent.snapshotResult, currentImageTag,
			snapshotCheckInfo, funcSvcID)

		if !existNewestVersion && !existMatchVersion {
			getNeedRemoveObjectIDs(snapshotEvent)
			log.GetLogger().Infof("No matching version, upgrade snapshot imageTag: %s, funcSvcID: %s, NeedRemoveObjectIDs:%s.",
				m.getRuntimeManagerImageTag(), funcSvcID, snapshotEvent.snapshotResult.NeedDeleteObjectIDs)
			return true
		}
		log.GetLogger().Infof("existing snapshot info in current imageTag. Tag: %s, snapshotCheckInfo: %+v.",
			currentImageTag, snapshotCheckInfo)
		// main snapshot info should be updated when upgrade action rollback
		updateMainSnapshotResult(snapshotEvent.snapshotResult, currentImageTag)
		traceID := "upgrade-checkpoint-" + commonutils.UniqueID()
		err := m.handleCheckpointResult(nil, traceID, snapshotEvent.snapshotResult,
			snapshotEvent, constants.CheckpointModeUpgrade)
		if err != nil {
			// do nothing when handle result failed, CAE will catch a timeout error. then retry by sre using serviceCM
			log.GetLogger().Errorf("handleCheckpointResult when upgrade snapshot, err: %s.", err.Error())
		}
		return false
	}
	return snapshotEvent.snapshotResult != nil && !finishedCheckpointEvent(snapshotEvent)
}

func updateMainSnapshotResult(result *types.SnapshotDeploymentSpec, currentImageTag string) {
	for _, snapshotVersion := range result.SnapshotVersions {
		// change main snapshot info to the one of correct image tag
		if snapshotVersion.SnapshotImageTag == currentImageTag &&
			snapshotVersion.SnapshotSha256 != result.SnapshotSha256 {
			log.GetLogger().Infof("found same imageTag but with different sha256. imageTag: %s, main "+
				"SnapshotSha256: %s, actual SnapshotSha256: %s.",
				currentImageTag, result.SnapshotSha256, snapshotVersion.SnapshotSha256)
			result.ObjectID = snapshotVersion.ObjectID
			result.SnapshotSha256 = snapshotVersion.SnapshotSha256
			result.SnapshotSize = snapshotVersion.SnapshotSize
			result.SnapshotCheckInfo = &snapshotVersion.SnapshotCheckInfo
			break
		}
	}
}

func finishedCheckpointEvent(snapshotEvent *SnapshotEvent) bool {
	snapshotDeployment := snapshotEvent.snapshotResult
	if snapshotDeployment == nil {
		return true
	}
	snapshotVersions := snapshotDeployment.SnapshotVersions

	// SnapshotVersions is nil, use the original process
	if snapshotVersions == nil || len(snapshotVersions) == 0 {
		return snapshotDeployment.SnapshotSize != 0 || snapshotDeployment.BucketID == ""
	}

	// the first checkpoint will only have one version
	if len(snapshotDeployment.SnapshotVersions) != 1 {
		log.GetLogger().Warnf("received checkpoint finished event, ignore: %s", anonymousKey(snapshotEvent.resultKey))
		return true
	}

	snapshotVersion := snapshotDeployment.SnapshotVersions[0]

	isSnapshotVersionEmpty := snapshotVersion.SnapshotSize == 0 && snapshotVersion.SnapshotSha256 == "" &&
		snapshotVersion.ErrCode == 0 && snapshotVersion.ErrMessage == ""

	return !isSnapshotVersionEmpty
}

// watchOption is the option for watcher
type watchOption struct {
	// Handler process the event
	Handler func(*etcd3.Event) error
	// Filter determine whether to process the event
	Filter        func(string) bool
	Path          string
	StopCh        <-chan struct{}
	SuccessCh     chan string
	ETCDSelection etcd3.EtcdType
	RecordRev     bool
}

func (m *manager) runWatcher(stopCh <-chan struct{}) {
	// snapshot-manager mode no need to watch snapshot event
	if snapshotmanager.GetSnapshotManagerMode() {
		return
	}
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	m.getCheckPointRedisClientWithRetry()
	m.processCheckpointEventByMetaETCD(stopCh)
	m.processCheckpointEventByCAEMetaETCD(stopCh)
}

func (m *manager) processCheckpointEventByMetaETCD(stopCh <-chan struct{}) {
	go m.processCheckpointWatcher(&watchOption{
		ETCDSelection: etcd3.Meta,
		Path:          watchPathPrefix,
		StopCh:        stopCh,
	})
}

func (m *manager) processCheckpointEventByCAEMetaETCD(stopCh <-chan struct{}) {
	go m.processCheckpointWatcher(&watchOption{
		ETCDSelection: etcd3.CAEMeta,
		Path:          watchPathPrefix,
		StopCh:        stopCh,
	})
}

func (m *manager) processCheckpointWatcher(opt *watchOption) {
	client := etcd3.GetETCDClient(opt.ETCDSelection)
	if client == nil {
		log.GetLogger().Warnf("ETCDClient is nil, ETCDSelection is %s", opt.ETCDSelection.String())
		return
	}
	watchChan := getWatchChan(client)
	for {
		select {
		case _, ok := <-opt.StopCh:
			if !ok {
				log.GetLogger().Infof("received signal to quit")
				return
			}
		case event, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("the channel received the result may be closed and try to reconnect")
				watchChan.Stop()
				ctx := context.WithValue(context.Background(), etcd3.CellEnable, true)
				watchChan = client.Watch(ctx, opt.Path, 0, true)
				continue
			}

			checkpointEvent := parseCheckpointEvent(event, opt.ETCDSelection)
			dur := &duration{receiveRequest: time.Now()}

			snapshotKey := checkpointEvent.snapshotEtcdKey
			setSnapshotMetaByFunSvcID(snapshotKey, checkpointEvent)

			if event.Type == etcd3.DELETE {
				go m.DeleteSnapShotDataByWatchEtcd(checkpointEvent)
				continue
			}

			if needUploadSnapshotToDS(checkpointEvent) {
				go m.uploadSnapShotToDS(checkpointEvent, dur)
			} else {
				log.GetLogger().Infof("no need to handle upload snapshot to ds, ignore it")
			}
			if checkpointEvent.snapshotEtcdKey.clusterID == utils.GetEnvClusterID() {
				m.updateSnapshotCache(event, snapshotKey)
			}

			if !m.needCheckpointEvent(checkpointEvent) {
				log.GetLogger().Infof("no need to handle snapshot event key %s, ignore it", anonymousKey(event.Key))
				continue
			}

			if checkpointEvent.snapshotResult.Event == eventCancel {
				go cancelCheckpoint(checkpointEvent.snapshotResult.TaskID)
				continue
			}
			updateTaskInfo(checkpointEvent.snapshotResult.TaskID)

			go m.startCheckpoint(checkpointEvent, dur, getCheckpointMode(checkpointEvent))
		}
	}
}

func getCheckpointMode(checkpointEvent *SnapshotEvent) string {
	checkpointMode := constants.CheckpointModeCreate
	if checkpointEvent.snapshotResult.Event == eventUpgrade {
		checkpointMode = constants.CheckpointModeUpgrade
	}
	return checkpointMode
}

func updateTaskInfo(taskID string) {
	// taskID may be empty when CAE is in a former version
	if taskID == "" {
		log.GetLogger().Info("taskID is empty")
		return
	}
	cache.GetCheckpointCache().AddCheckpointTaskInfo(taskID)
}

func cancelCheckpoint(taskID string) {
	// taskID shall not be empty when cancel
	if taskID == "" {
		log.GetLogger().Errorf("taskID is empty when try to cancel checkpoint")
		return
	}
	cache.GetCheckpointCache().SetTaskInfoCancel(taskID)
	if !cache.GetCheckpointCache().IsTaskExist(taskID) {
		log.GetLogger().Errorf("cancel after task is done, taskID: %s.", taskID)
		return
	}
	var podName string
	for loop := 0; loop <= defaultCancelRetryTimes; loop++ {
		podName = cache.GetCheckpointCache().GetTaskInfoPodName(taskID)
		if podName != "" || loop == defaultCancelRetryTimes {
			break
		}
		log.GetLogger().Warnf("can not find checkpoint podName for taskID: %s, retryTimes: %d.", taskID, loop)
		time.Sleep(defaultCancelRetryInterval)
	}

	if podName == "" {
		log.GetLogger().Warnf("can not find checkpoint podName for taskID: %s after retry.", taskID)
		return
	}
	podfetcher.GetPodFetcher().DeletePodByK8S(podName, "cancel-checkpoint")
}

func setSnapshotMetaByFunSvcID(snapshotKey snapshotEtcdKey, checkpointEvent *SnapshotEvent) {
	funcSvcID := utils.CrName(snapshotKey.businessID, snapshotKey.tenantID, snapshotKey.funcName,
		snapshotKey.funcVersion)
	cache.WorkerCache.SetSnapshotMeta(funcSvcID, &types.SnapshotMeta{
		ClusterID:      snapshotKey.clusterID,
		ETCDKey:        checkpointEvent.resultKey,
		SnapshotResult: checkpointEvent.snapshotResult,
	})
	log.GetLogger().Infof("success to set snapshot meta for func %s with key %s", funcSvcID, checkpointEvent.resultKey)
}

func (m *manager) updateSnapshotCache(event *etcd3.Event, key snapshotEtcdKey) {
	funcSvcID := utils.CrName(key.businessID, key.tenantID, key.funcName, key.funcVersion)
	if event.Type == etcd3.PUT {
		m.needSnapshot.Store(funcSvcID, struct{}{})
	} else if event.Type == etcd3.DELETE {
		m.needSnapshot.Delete(funcSvcID)
	}
}

// GetSnapshotCount -
func (m *manager) GetSnapshotCount(funcSvcID string) int {
	if m == nil {
		return 0
	}
	_, loaded := m.needSnapshot.Load(funcSvcID)
	if loaded {
		return 1
	}
	return 0
}

func needUploadSnapshotToDS(snapshotEvent *SnapshotEvent) bool {
	snapshotDeployment := snapshotEvent.snapshotResult
	if snapshotDeployment == nil {
		return false
	}

	if snapshotDeployment.SnapshotVersions == nil || len(snapshotDeployment.SnapshotVersions) == 0 {
		return false
	}

	if len(snapshotDeployment.SnapshotVersions) == 1 && snapshotDeployment.SnapshotVersions[0].
		SnapshotSize == 0 && snapshotDeployment.SnapshotVersions[0].SnapshotSha256 == "" {
		return false
	}

	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Warnf("failed to get config: %s", err.Error())
		return false
	}

	result := snapshotEvent.snapshotEtcdKey.clusterID != utils.GetEnvClusterID() &&
		cfg.PoolMgrConfig.CheckpointPodConfig.SnapshotStorageType == constants.DataSystemStorageType &&
		!isAlreadyUploaded(snapshotEvent)
	return result
}

func isAlreadyUploaded(snapshotEvent *SnapshotEvent) bool {
	if snapshotEvent.snapshotResult == nil || snapshotEvent.snapshotResult.UploadedDataSystem == nil {
		return false
	}
	return snapshotEvent.snapshotResult.UploadedDataSystem[utils.GetEnvClusterID()]
}

func (m *manager) uploadSnapShotToDS(checkpointEvent *SnapshotEvent, dur *duration) {
	checkpointEvent.action = constants.UploadSnapshot
	checkpointKey := checkpointEvent.snapshotEtcdKey
	funcSvcID := utils.CrName(checkpointKey.businessID, checkpointKey.tenantID,
		checkpointKey.funcName, checkpointKey.funcVersion)
	traceID := "checkpoint-upload-" + commonutils.UniqueID()
	functionService := m.getFunctionMetaWithRetry(traceID, funcSvcID)
	if functionService == nil {
		log.GetLogger().Warnf("failed to get function meta from cache for %s, traceID: %s",
			funcSvcID, traceID)
		return
	}
	m.generalizePodForSnapshot(checkpointEvent, functionService, dur, funcSvcID, traceID)
}

func (m *manager) generalizePodForSnapshot(checkpointEvent *SnapshotEvent,
	functionService *faasfunction.FunctionService, dur *duration, funcSvcID, traceID string) {
	log.GetLogger().Infof("generalizePodWithRetry for func: %s with traceID: %s", funcSvcID, traceID)
	pod, requestInfo, needReturn := m.generalizePodWithRetry(checkpointEvent, dur, traceID, funcSvcID, functionService)
	if needReturn {
		return
	}

	defer func() {
		if pod != nil {
			podfetcher.GetPodFetcher().DeletePodByK8S(pod.Name, requestInfo.TraceID)
		}
	}()

	var language string
	if functionService != nil {
		language = functionService.Spec.Configuration.Runtime.Language
	} else {
		language = "java"
	}
	requestInfo.SnapshotDeployment = checkpointEvent.snapshotResult
	err := m.sendSnapshotRequestToWorker(pod, requestInfo, language, checkpointEvent.action)
	if err != nil {
		log.GetLogger().Errorf("failed to %s snapshot to ds : %s, func: %s, traceID: %s, error: %s, duration: "+
			"%s", checkpointEvent.action, pod.Name, functionService.ID, requestInfo.TraceID, err.Error(), dur.print(time.Now()))
		return
	}
	if checkpointEvent.action == constants.UploadSnapshot {
		m.recordUploadDSEvent(checkpointEvent, functionService, pod, traceID)
	}
}

func (m *manager) recordUploadDSEvent(checkpointEvent *SnapshotEvent, functionService *faasfunction.FunctionService,
	pod *corev1.Pod, traceID string) {
	// write upload data system result to snapshot etcd key
	if checkpointEvent.snapshotResult.UploadedDataSystem == nil {
		checkpointEvent.snapshotResult.UploadedDataSystem = make(map[string]bool)
	}
	// record cluster id and succeed to upload data system
	checkpointEvent.snapshotResult.UploadedDataSystem[utils.GetEnvClusterID()] = true
	value, err := json.Marshal(checkpointEvent.snapshotResult)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal snapshot result for pod: %s, func: %s, traceID: %s, error: %s",
			pod.Name, functionService.ID, traceID, err.Error())
		return
	}
	if err = m.submitResult(checkpointEvent.resultKey, traceID, value, checkpointEvent.etcdType); err != nil {
		log.GetLogger().Warnf("%s | failed to submit result and error: %s", traceID, err.Error())
	}
}

func (m *manager) DeleteSnapShotData(funcMetaData *faasfunction.FunctionService, snapshotMeta *types.SnapshotMeta,
	funcSvcID string) {
	traceID := "checkpoint-delete-" + commonutils.UniqueID()
	snapshotKey := snapshotEtcdKey{
		businessID:  funcMetaData.Spec.Configuration.BusinessID,
		tenantID:    funcMetaData.Spec.Configuration.TenantID,
		funcName:    funcMetaData.Spec.Configuration.FunctionName,
		funcVersion: funcMetaData.Spec.Configuration.FunctionVersion,
	}
	log.GetLogger().Infof("recieve delete snapshot event by func %s del, traceID: %s", funcSvcID, traceID)
	snapshotKey.clusterID = snapshotMeta.ClusterID
	if snapshotKey.clusterID != utils.GetEnvClusterID() &&
		m.cfg.PoolMgrConfig.CheckpointPodConfig.SnapshotStorageType != constants.DataSystemStorageType {
		log.GetLogger().Infof("no need generate pod for func: %s in cluster: %s, with traceID: %s", funcSvcID,
			snapshotKey.clusterID, traceID)
		return
	}
	checkpointEvent := &SnapshotEvent{
		snapshotEtcdKey: snapshotKey,
		snapshotResult:  &funcMetaData.Spec.SnapshotDeployment,
		needDelSnapshot: true,
		action:          constants.DeleteSnapshot,
	}
	dur := &duration{receiveRequest: time.Now()}
	m.deleteSnapshotByRedisKey(funcMetaData, checkpointEvent, dur, funcSvcID, traceID)
}

func (m *manager) deleteSnapshotByRedisKey(funcMetaData *faasfunction.FunctionService, checkpointEvent *SnapshotEvent,
	dur *duration, funcSvcID, traceID string) {
	funcNameWithVersion := fmt.Sprintf("%s-%s", checkpointEvent.snapshotEtcdKey.funcName,
		checkpointEvent.snapshotEtcdKey.funcVersion)
	redisKey := fmt.Sprintf(snapshotRedisKey, funcNameWithVersion, m.getRuntimeManagerImageTag())
	redisVal := uuid.New().String()
	err := m.setCheckpointRedisKeyWithRetry(redisKey, redisVal, traceID)
	if err != nil {
		log.GetLogger().Errorf("[setCheckpointRedisKey] for delete snapshot pod failed for %s, traceID: %s, error: %s",
			funcSvcID, traceID, err.Error())
		return
	}
	checkpointEvent.uuid = redisVal
	m.generalizePodForSnapshot(checkpointEvent, funcMetaData, dur, funcSvcID, traceID)
	m.deleteCheckpointRedisKey(checkpointEvent, redisKey, traceID)
}

func (m *manager) DeleteSnapShotDataByWatchEtcd(snapshotEvent *SnapshotEvent) {
	dur := &duration{receiveRequest: time.Now()}
	checkpointKey := snapshotEvent.snapshotEtcdKey
	funcSvcID := utils.CrName(checkpointKey.businessID, checkpointKey.tenantID,
		checkpointKey.funcName, checkpointKey.funcVersion)
	traceID := "checkpoint-delete-" + commonutils.UniqueID()
	log.GetLogger().Infof("recieve delete snapshot event by etcd for func %s, traceID: %s", funcSvcID, traceID)
	if checkpointKey.clusterID != utils.GetEnvClusterID() &&
		m.cfg.PoolMgrConfig.CheckpointPodConfig.SnapshotStorageType != constants.DataSystemStorageType {
		log.GetLogger().Infof("no need generate pod for func: %s in cluster: %s, with traceID: %s", funcSvcID,
			checkpointKey.clusterID, traceID)
		return
	}
	functionService := m.getFunctionMetaWithRetry(traceID, funcSvcID)
	if functionService == nil {
		log.GetLogger().Warnf("failed to get function meta from cache for %s, traceID: %s",
			funcSvcID, traceID)
		return
	}

	if functionService.Spec.SnapshotDeployment.BucketID == "" || functionService.Spec.SnapshotDeployment.ObjectID == "" {
		log.GetLogger().Errorf("unable to get function snapshot from cache for %s, traceID: %s",
			funcSvcID, traceID)
		return
	}
	snapshotEvent.action = constants.DeleteSnapshot
	snapshotEvent.needDelSnapshot = true
	snapshotEvent.snapshotResult = &functionService.Spec.SnapshotDeployment
	m.deleteSnapshotByRedisKey(functionService, snapshotEvent, dur, funcSvcID, traceID)
}

func (m *manager) generalizePodWithRetry(checkpointEvent *SnapshotEvent, dur *duration, traceID string,
	funcSvcID string, functionService *faasfunction.FunctionService) (*corev1.Pod, *config.RequestInfo, bool) {
	var pod *corev1.Pod
	var err error
	var requestInfo = &config.RequestInfo{}
	for i := 0; i < defaultCheckpointRetryTimes; i++ {
		log.GetLogger().Infof("%s | send generalize checkpoint request %d times for %s",
			traceID, i, funcSvcID)
		if i != 0 {
			time.Sleep(defaultCheckpointRetryInterval)
		}
		requestInfo = makeCheckpointRequestInfo(traceID, functionService)
		requestInfo.SnapshotDeployment = checkpointEvent.snapshotResult
		requestInfo.ClusterID = checkpointEvent.snapshotEtcdKey.clusterID
		requestInfo.IsNeedDeleteSnapshot = checkpointEvent.needDelSnapshot

		pod, err = m.generalizePod(requestInfo, functionService)
		if err != nil {
			log.GetLogger().Warnf("%s | failed to get checkpoint instance for funcName: %s ",
				requestInfo.TraceID, requestInfo.FunctionName)
			continue
		}

		dur.finishedGeneralizePod = time.Now()
		break
	}

	if pod == nil && err != nil {
		log.GetLogger().Warnf("%s | retry three times, failed to get checkpoint instance for funcName: %s ",
			requestInfo.TraceID, requestInfo.FunctionName)
		return nil, nil, true
	}
	return pod, requestInfo, false
}

func getWatchChan(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if metaClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, true)
			watchChan := metaClient.Watch(ctx, watchPathPrefix, 0, true)
			log.GetLogger().Infof("success to init meta etcd watch channel")
			ticker.Stop()
			return watchChan
		}
	}
}

func makeCheckpointRequestInfo(traceID string, functionSvc *faasfunction.FunctionService) *config.RequestInfo {
	return &config.RequestInfo{
		CPU:             functionSvc.Spec.Configuration.Resource.CPU,
		Memory:          functionSvc.Spec.Configuration.Resource.Mem,
		Namespace:       utils.FunctionNameSpace(),
		BusinessID:      functionSvc.Spec.Configuration.BusinessID,
		TenantID:        functionSvc.Spec.Configuration.TenantID,
		FunctionName:    functionSvc.Spec.Configuration.FunctionName,
		FunctionVersion: functionSvc.Spec.Configuration.FunctionVersion,
		URN:             functionSvc.Spec.Configuration.FunctionURN,
		Applier:         constants.WorkerManagerApplier,
		ForceDeploy:     "false",
		TraceID:         traceID,
		StartTime:       time.Now(),
		BusinessType:    functionSvc.Spec.Configuration.BusinessType,
	}
}

// ErrorResult request error result
type ErrorResult struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (m *manager) startCheckpoint(checkpointEvent *SnapshotEvent, dur *duration, checkpointMode string) {
	// block until idle
	m.Apply()
	checkpointKey := checkpointEvent.snapshotEtcdKey
	funcSvcID := utils.CrName(checkpointKey.businessID, checkpointKey.tenantID,
		checkpointKey.funcName, checkpointKey.funcVersion)
	snapshotSpec := checkpointEvent.snapshotResult
	snapshotSpec.FastStartEnable = config.IsFastStartEnable(checkpointKey.tenantID)
	resultKey := checkpointEvent.resultKey
	start := time.Now()
	if checkpointMode == constants.CheckpointModeUpdate {
		// snapshot-manager mode, needs to wait all the checkpointing finish
		defer func() {
			log.GetLogger().Infof("ReCheckpoint | %s | %.2f s | function %s snapshot is updated.",
				m.getRuntimeManagerImageTag(), time.Since(start).Seconds(), anonymousKey(snapshotSpec.ObjectID))
			m.wg.Done()
			atomic.AddInt64(&m.finishNum, 1)
		}()
	}
	defer m.Release()
	if isSnapshotDisabled() {
		log.GetLogger().Warnf("checkpoint request when snapshot is disabled! funcName: %s.", checkpointKey.funcName)
		m.submitResultWhenSnapshotDisable(checkpointEvent)
		return
	}

	traceID := "checkpoint-" + commonutils.UniqueID()
	funcNameWithVersion := fmt.Sprintf("%s-%s", checkpointKey.funcName, checkpointKey.funcVersion)
	redisKey := fmt.Sprintf(snapshotRedisKey, funcNameWithVersion, m.getRuntimeManagerImageTag())
	if !m.needRedoCheckpointForSnapshotModule(checkpointEvent, funcSvcID, redisKey, checkpointMode, traceID) {
		log.GetLogger().Infof("not necessary for func %s redo snapshot, traceID: %s", funcSvcID, traceID)
		return
	}

	m.checkOrWaitSnapshotKeyExpire(checkpointEvent, redisKey, checkpointMode, traceID)
	log.GetLogger().Infof("startCheckpoint mode: %s for func: %s with resultKey: %s", checkpointMode,
		funcSvcID, resultKey)
	snapshotResult, err := m.executeCheckpoint(snapshotSpec, dur, funcSvcID, checkpointMode, traceID)
	// if err is not nil, then snapshotResult must be nil
	gotErr := false
	if err != nil {
		// return the original snapshotSpec with error code to console
		snapshotResult = snapshotSpec
		gotErr = m.addFailNumExecuteCheckpointFailed(checkpointMode, gotErr)
		m.recordExecuteCheckPointFailed(checkpointMode, funcSvcID, traceID, err)
	}
	m.deleteCheckpointRedisKey(checkpointEvent, redisKey, traceID)
	err = m.handleCheckpointResult(err, traceID, snapshotResult, checkpointEvent, checkpointMode)
	if err != nil {
		m.addFailNumHandleCheckpointFailed(checkpointMode, gotErr)
		m.recordHandleCheckpointResultFailed(checkpointMode, funcSvcID, traceID, err)
		return
	}
}

func isSnapshotDisabled() bool {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("get config err when prepare to start checkpoint, err: %s", err.Error())
		return false
	}
	return cfg.DisableSnapshot
}

// needRedoCheckpointForSnapshotModule used to determine whether snapshot-manager need redo snapshot.
// snapshot-manager supports only the CAE function, for worker-manager module, ignore it.
func (m *manager) needRedoCheckpointForSnapshotModule(checkpointEvent *SnapshotEvent, funcSvcID, redisKey,
	checkpointMode, traceID string) bool {
	if !utils.IsSnapShotModel() {
		return true
	}
	functionService := m.getFunctionMetaWithRetry(traceID, funcSvcID)
	if functionService == nil {
		log.GetLogger().Warnf("failed to get function meta from cache for %s, traceID: %s",
			funcSvcID, traceID)
		return false
	}
	if !commonutils.IsCAEFunc(functionService.Spec.Configuration.BusinessType) {
		return false
	}

	resultKey := checkpointEvent.resultKey
	if checkpointMode == constants.CheckpointModeUpdate {
		redisVal := uuid.New().String()
		err := m.setCheckpointRedisKeyWithRetry(redisKey, redisVal, traceID)
		if err == nil {
			checkpointEvent.uuid = redisVal
			return true
		}
		if errors.Is(err, errLockRedisKey) {
			log.GetLogger().Warnf("snapshot key %s event is doing by worker-manager", resultKey)
			return false
		}
		log.GetLogger().Errorf("snapshot key %s event set redis key error: %s", resultKey, err.Error())
	}

	return true
}

func (m *manager) checkOrWaitSnapshotKeyExpire(checkpointEvent *SnapshotEvent, redisKey, checkpointMode,
	traceID string) {
	resultKey := checkpointEvent.resultKey
	if checkpointMode == constants.CheckpointModeCreate {
		redisVal := uuid.New().String()
		err := m.setCheckpointRedisKeyWithRetry(redisKey, redisVal, traceID)
		if err == nil {
			checkpointEvent.uuid = redisVal
			return
		}
		if errors.Is(err, errLockRedisKey) {
			log.GetLogger().Warnf("snapshot key %s event it doing by other owner, traceID: %s", resultKey, traceID)
			m.waitKeyDeletedOrTimeout(checkpointEvent, redisKey, traceID)
			return
		}
		log.GetLogger().Errorf("snapshot key %s event set redis key error: %s, traceID: %s", resultKey,
			err.Error(), traceID)
	}
}

func (m *manager) waitKeyDeletedOrTimeout(checkpointEvent *SnapshotEvent, redisKey, traceID string) {
	if m.redisCli == nil {
		log.GetLogger().Warnf("[setCheckpointRedisKey]redis is not ready")
		return
	}
	ticker := time.NewTicker(redisRetryInterval)
	timer := time.NewTimer(snapshotKeyExpireTime)
	retryTimes := 0
	defer ticker.Stop()
	defer timer.Stop()
	for {
		select {
		case <-ticker.C:
			redisVal := uuid.New().String()
			err := m.setCheckpointRedisKey(redisKey, redisVal, traceID)
			if err == nil {
				checkpointEvent.uuid = redisVal
				return
			}
			if !errors.Is(err, errLockRedisKey) {
				retryTimes++
			}
			if retryTimes == redisReqRetryTime {
				log.GetLogger().Errorf("set redis key %s failed with retry %d times, error: %s, traceID: %s",
					redisKey, retryTimes, err.Error(), traceID)
				return
			}
		case <-timer.C:
			log.GetLogger().Errorf("failed to get redis key after %f timeout, traceID: %s",
				snapshotKeyExpireTime.Seconds(), traceID)
			return
		}
	}
}

func (m *manager) setCheckpointRedisKeyWithRetry(redisKey, redisVal, traceID string) error {
	if m.redisCli == nil {
		log.GetLogger().Warnf("[setCheckpointRedisKey]redis is not ready")
		return errConnRedisClient
	}
	var err error
	for i := 0; i < redisReqRetryTime; i++ {
		err = m.setCheckpointRedisKey(redisKey, redisVal, traceID)
		if err == nil || errors.Is(err, errLockRedisKey) {
			return err
		}
		if i == redisReqRetryTime-1 {
			return err
		}
		time.Sleep(redisRetryInterval)
	}

	return err
}

func (m *manager) setCheckpointRedisKey(redisKey, redisVal, traceID string) error {
	if m.redisCli == nil {
		log.GetLogger().Warnf("[setCheckpointRedisKey]redis is not ready")
		return errConnRedisClient
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer cancel()
	res, err := m.redisCli.SetNX(ctx, redisKey, redisVal, snapshotKeyExpireTime).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to set key: %s, error: %s, traceID: %s", redisKey, err.Error(), traceID)
		return err
	}
	if !res {
		return errLockRedisKey
	}
	log.GetLogger().Infof("[setCheckpointRedisKey] success, redis key: %s, val %s, traceID: %s", redisKey,
		redisVal, traceID)
	return nil
}

func (m *manager) deleteCheckpointRedisKey(checkpointEvent *SnapshotEvent, redisKey, traceID string) {
	if m.redisCli == nil {
		log.GetLogger().Warnf("[delCheckpointRedisKey]redis is not ready")
		return
	}
	if checkpointEvent.uuid == "" {
		log.GetLogger().Errorf("the event %s is not triggered by redis, traceID: %s", checkpointEvent.resultKey, traceID)
		return
	}
	getCtx, getCancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer getCancel()
	val, err := m.redisCli.Get(getCtx, redisKey).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to del key: %s with error: %s, traceID: %s", redisKey, err.Error(), traceID)
		return
	}
	if checkpointEvent.uuid != val {
		log.GetLogger().Errorf("the redis key: %s is locked by other, traceID: %s", redisKey, traceID)
		return
	}

	delCtx, delCancel := context.WithTimeout(context.Background(), redisCxtTimeout)
	defer delCancel()
	_, err = m.redisCli.Del(delCtx, redisKey).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to del key: %s with error: %s, traceID: %s", redisKey, err.Error(), traceID)
		return
	}
	log.GetLogger().Infof("success to release redis key: %s lock, traceID: %s", redisKey, traceID)
}

func (m *manager) getCheckPointRedisClientWithRetry() {
	if m.redisCli != nil {
		return
	}

	for i := 0; i < redisConnRetryTimes; i++ {
		redisCli := monitoring.GetRedisClient()
		if redisCli == nil {
			log.GetLogger().Errorf("[getCheckPointRedisClient]redis is not ready")
			time.Sleep(redisRetryInterval)
			continue
		} else {
			m.redisCli = redisCli
			return
		}
	}
}

func (m *manager) addFailNumHandleCheckpointFailed(checkpointMode string, gotErr bool) {
	if checkpointMode == constants.CheckpointModeUpdate {
		// Execute checkpoint fail, not add failNum
		if !gotErr {
			atomic.AddInt64(&m.failedNum, 1)
		}
	}
}

func (m *manager) addFailNumExecuteCheckpointFailed(checkpointMode string, gotErr bool) bool {
	if checkpointMode == constants.CheckpointModeUpdate {
		atomic.AddInt64(&m.failedNum, 1)
		// Execute checkpoint fail add failNum and mark got err
		gotErr = true
	}
	return gotErr
}

func (m *manager) recordExecuteCheckPointFailed(checkpointMode string,
	funcSvcID string, traceID string, err error) {
	if checkpointMode == constants.CheckpointModeUpdate {
		log.GetLogger().Warnf("ReCheckpoint | %s | failed to execute checkpoint for func: %s with "+
			"traceID: %s and error: %s", m.getRuntimeManagerImageTag(),
			funcSvcID, traceID, err.Error())
	} else {
		log.GetLogger().Warnf("failed to execute checkpoint for func: %s with "+
			"traceID: %s and error: %s", funcSvcID, traceID, err.Error())
	}
	m.summary.Store(funcSvcID, failedStatus)
}

func (m *manager) recordHandleCheckpointResultFailed(checkpointMode string, funcSvcID string,
	traceID string, err error) {
	if checkpointMode == constants.CheckpointModeUpdate {
		log.GetLogger().Errorf("ReCheckpoint | %s | failed to handle checkpoint result for "+
			"func: %s with traceID: %s and error: %s",
			m.getRuntimeManagerImageTag(),
			funcSvcID, traceID, err.Error())
	} else {
		log.GetLogger().Warnf("%s | failed to handle checkpoint result for func: %s and error: %s",
			traceID, funcSvcID, err.Error())
	}
}

func (m *manager) submitResultWhenSnapshotDisable(checkpointEvent *SnapshotEvent) {
	snapshotResult := checkpointEvent.snapshotResult
	var value []byte
	var version types.SnapshotVersion
	version.ErrMessage = "snapshot disabled"
	version.Timestamp = time.Now().Unix()
	snapshotResult.SnapshotVersions = []types.SnapshotVersion{version}
	value, err := json.Marshal(snapshotResult)
	if err != nil {
		log.GetLogger().Warnf("failed to processMultiVersion marshal snapshotResult: %s", err.Error())
	}

	tempTrace := "snapshot-disabled"
	if err := m.submitResult(checkpointEvent.resultKey, tempTrace, value, checkpointEvent.etcdType); err != nil {
		log.GetLogger().Warnf("%s | failed to submit result  and error: %s",
			tempTrace, err.Error())
	}
}

func (m *manager) handleCheckpointResult(err error, traceID string, snapshotResult *types.SnapshotDeploymentSpec,
	snapshotEvent *SnapshotEvent, checkpointMode string) error {
	// result submitted by data-plane marks event as "done"
	if snapshotResult != nil {
		snapshotResult.Event = eventDone
	}
	var value []byte
	resultKey := snapshotEvent.resultKey
	etcdType := snapshotEvent.etcdType
	if snapshotResult != nil && snapshotResult.SnapshotVersions != nil {
		value, err = m.processMultiVersion(err, snapshotResult, checkpointMode)
	} else {
		value, err = m.processSingleVersion(err, snapshotResult)
	}

	if err != nil {
		log.GetLogger().Warnf("%s | failed to process snapshot version before submit and error: %s",
			traceID, err.Error())
	}

	// write checkpoint result
	if err = m.submitResult(resultKey, traceID, value, etcdType); err != nil {
		log.GetLogger().Warnf("%s | failed to submit result  and error: %s",
			traceID, err.Error())
	}

	cache.GetCheckpointCache().DelCheckpointTaskInfo(snapshotResult.TaskID)
	return nil
}

func (m *manager) processMultiVersion(err error, snapshotResult *types.SnapshotDeploymentSpec,
	checkpointMode string) ([]byte, error) {
	imageTag := m.getRuntimeManagerImageTag()
	if err != nil {
		return handleMultiVersionError(err, imageTag, snapshotResult)
	}

	m.appendMultiVersion(snapshotResult, checkpointMode)
	value, err := json.Marshal(snapshotResult)
	if err != nil {
		log.GetLogger().Errorf("failed to processMultiVersion marshal snapshotResult: %s", err.Error())
		return nil, snerror.New(utils.JSONParseError, "failed to marshal multi version snapshot result")
	}

	return value, nil
}

func handleMultiVersionError(err error, imageTag string, snapshotResult *types.SnapshotDeploymentSpec) ([]byte, error) {
	var value []byte
	var version types.SnapshotVersion
	version.ErrCode = utils.StatusInternalServerError
	version.ErrMessage = err.Error()
	if snErr, ok := err.(snerror.SNError); ok {
		version.ErrCode = snErr.Code()
		_, _, version.ErrMessage = snerror.DecodeFromErrMSG(uint32(snErr.Code()), snErr.Error())
	}
	version.Timestamp = time.Now().Unix()
	version.SnapshotImageTag = imageTag
	snapshotResult.SnapshotVersions = append(snapshotResult.SnapshotVersions, version)
	// only keep the newest 4 elements
	checkAndUpdateVersionLength(snapshotResult)
	value, err = json.Marshal(snapshotResult)
	if err != nil {
		log.GetLogger().Errorf("failed to processMultiVersion marshal snapshotResult: %s", err.Error())
		return nil, snerror.New(utils.JSONParseError, "failed to marshal multi version snapshot result")
	}
	return value, nil
}

func (m *manager) processSingleVersion(err error, snapshotResult *types.SnapshotDeploymentSpec) ([]byte, error) {
	if err != nil {
		return handleSingleVersionError(err)
	}

	// guaranteed compatibility for SnapshotCheckInfo data structure
	snapshotResult.SnapshotCheckInfo = nil
	value, err := json.Marshal(snapshotResult)
	if err != nil {
		log.GetLogger().Errorf("failed to processSingleVersion marshal snapshotResult: %s, traceID: %s", err.Error())
		return nil, snerror.New(utils.JSONParseError, "failed to marshal single version snapshot result")
	}

	return value, nil
}

func handleSingleVersionError(err error) ([]byte, error) {
	var value []byte
	errObj := ErrorResult{
		Code:    utils.StatusInternalServerError,
		Message: err.Error(),
	}
	if snErr, ok := err.(snerror.SNError); ok {
		errObj.Code = snErr.Code()
		_, _, errObj.Message = snerror.DecodeFromErrMSG(uint32(snErr.Code()), snErr.Error())
	}
	value, err = json.Marshal(errObj)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal error: %s", err.Error())
		return nil, snerror.New(utils.JSONParseError, "failed to marshal single version snapshot result")
	}
	return value, nil
}

func (m *manager) appendMultiVersion(snapshotResult *types.SnapshotDeploymentSpec, checkpointMode string) {
	var snapshotCheckInfo types.SnapshotCheckInfo
	if snapshotResult.SnapshotCheckInfo != nil {
		snapshotCheckInfo = *snapshotResult.SnapshotCheckInfo
	}
	switch checkpointMode {
	case constants.CheckpointModeCreate:
		for index, version := range snapshotResult.SnapshotVersions {
			if version.SnapshotSize == 0 && version.SnapshotSha256 == "" {
				version.ObjectID = snapshotResult.ObjectID
				version.SnapshotSize = snapshotResult.SnapshotSize
				version.SnapshotSha256 = snapshotResult.SnapshotSha256
				version.SnapshotCheckInfo = snapshotCheckInfo
				version.SnapshotImageTag = m.getRuntimeManagerImageTag()
				version.Timestamp = time.Now().Unix()
				snapshotResult.SnapshotVersions[index] = version
			}
		}
	case constants.CheckpointModeUpdate, constants.CheckpointModeUpgrade:
		var version types.SnapshotVersion
		version.ObjectID = snapshotResult.ObjectID
		version.SnapshotSize = snapshotResult.SnapshotSize
		version.SnapshotSha256 = snapshotResult.SnapshotSha256
		version.SnapshotCheckInfo = snapshotCheckInfo
		version.SnapshotImageTag = m.getRuntimeManagerImageTag()
		version.Timestamp = time.Now().Unix()
		checkAndAppendSnapshotVersions(snapshotResult, version)
	default:
		log.GetLogger().Errorf("failed to appendMultiVersion, invalid checkpointMode")
	}
}

func checkAndAppendSnapshotVersions(snapshotResult *types.SnapshotDeploymentSpec, version types.SnapshotVersion) {
	previousVersion := types.SnapshotVersion{}

	// clear historical data
	snapshotResult.NeedDeleteObjectIDs = nil
	var isExistPreviousVersion bool
	for _, v := range snapshotResult.SnapshotVersions {
		if v.SnapshotImageTag == "" {
			continue
		}
		// Snapshots of the same version will be replaced if they fail to be created.
		if !isExistPreviousVersion && v.SnapshotImageTag != version.SnapshotImageTag {
			previousVersion = v
			isExistPreviousVersion = true
		}
		if previousVersion.SnapshotImageTag == "" {
			continue
		}

		if v.SnapshotImageTag != previousVersion.SnapshotImageTag && v.SnapshotImageTag != version.SnapshotImageTag {
			snapshotResult.NeedDeleteObjectIDs = append(snapshotResult.NeedDeleteObjectIDs, v.ObjectID)
		}
	}
	snapshotResult.SnapshotVersions = nil
	if isExistPreviousVersion {
		// Snapshots of the two versions will be retained.
		snapshotResult.SnapshotVersions = append(snapshotResult.SnapshotVersions, version, previousVersion)
	} else {
		snapshotResult.SnapshotVersions = append(snapshotResult.SnapshotVersions, version)
	}
}

func checkAndUpdateVersionLength(snapshotResult *types.SnapshotDeploymentSpec) {
	// sort the array by timestamp
	sort.Slice(snapshotResult.SnapshotVersions, func(i, j int) bool {
		return snapshotResult.SnapshotVersions[i].Timestamp > snapshotResult.SnapshotVersions[j].Timestamp
	})
	if len(snapshotResult.SnapshotVersions) > defaultSnapshotVersionLength {
		snapshotResult.SnapshotVersions = snapshotResult.SnapshotVersions[:defaultSnapshotVersionLength]
	}
}

func (m *manager) executeCheckpoint(spec *types.SnapshotDeploymentSpec, dur *duration, funcSvcID, checkpointMode,
	traceID string) (*types.SnapshotDeploymentSpec, error) {
	log.GetLogger().Infof("%s | start to checkpoint request for %s", traceID, funcSvcID)
	dur.startGeneralizePod = time.Now()
	var snapshotDeployment *types.SnapshotDeploymentSpec
	var err error
	imageTag := m.getRuntimeManagerImageTag()
	// ensure append imageTag only once
	if !strings.Contains(spec.ObjectID, imageTag) {
		objectID := spec.ObjectID
		index := strings.Index(objectID, versionSeparator)
		if index >= 0 {
			objectID = objectID[:index]
		}
		spec.ObjectID = objectID + versionSeparator + imageTag
	}

	if !strings.HasPrefix(spec.ObjectID, "snapshot/") {
		spec.ObjectID = "snapshot/" + spec.ObjectID
	}

	for i := 0; i < defaultCheckpointRetryTimes; i++ {
		log.GetLogger().Infof("%s | send checkpoint request %d times for %s",
			traceID, i, funcSvcID)
		if i != 0 {
			time.Sleep(defaultCheckpointRetryInterval)
		}
		snapshotDeployment, err = m.checkpointInternal(traceID, funcSvcID, spec, dur, checkpointMode)
		if err == nil {
			log.GetLogger().Infof("%s | succeed to checkpointInternal for %s snapshotDeployment: %+v",
				traceID, funcSvcID, snapshotDeployment)
			m.summary.Store(funcSvcID, succeedStatus)
			break
		}
		if cache.GetCheckpointCache().IsTaskCanceled(spec.TaskID) {
			log.GetLogger().Infof("checkpoint task has been canceled, skip retry. taskID: %s, traceID: %s.",
				spec.TaskID, traceID)
			break
		}
	}
	if snapshotDeployment == nil {
		return nil, err
	}
	m.postProcessCheckpointResult(spec, snapshotDeployment, funcSvcID)

	return snapshotDeployment, err
}

func (m *manager) postProcessCheckpointResult(spec *types.SnapshotDeploymentSpec,
	snapshotDeployment *types.SnapshotDeploymentSpec, funcSvcID string) {
	if spec.NeedDeleteObjectIDs != nil {
		snapshotDeployment.NeedDeleteObjectIDs = spec.NeedDeleteObjectIDs
	}
	if spec.FastStartEnable {
		snapshotDeployment.FastStartEnable = spec.FastStartEnable
	}

	if spec.SnapshotVersions != nil {
		snapshotDeployment.SnapshotVersions = spec.SnapshotVersions
	}

	snapshotDeployment.TaskID = spec.TaskID
	snapshotDeployment.Event = eventDone
}

func (m *manager) checkpointInternal(traceID, funcSvcID string, spec *types.SnapshotDeploymentSpec, dur *duration,
	checkpointMode string) (*types.SnapshotDeploymentSpec, error) {
	functionService := m.getFunctionMetaWithRetry(traceID, funcSvcID)
	if functionService == nil {
		log.GetLogger().Errorf("failed to get function meta from cache for %s, traceID: %s",
			funcSvcID, traceID)
		return nil, fmt.Errorf("failed to get function meta")
	}

	requestInfo := makeCheckpointRequestInfo(traceID, functionService)
	requestInfo.SnapshotDeployment = spec
	if checkpointMode == constants.CheckpointModeUpdate || spec.Event == eventUpgrade {
		// re do checkpoint
		requestInfo.SnapshotDeployment.SnapshotSize = 0
		requestInfo.SnapshotDeployment.SnapshotSha256 = ""
		requestInfo.SnapshotDeployment.UploadedDataSystem = nil
	}
	pod, err := m.generalizePod(requestInfo, functionService)
	if err != nil {
		return nil, err
	}

	dur.finishedGeneralizePod = time.Now()
	defer podfetcher.GetPodFetcher().DeletePodByK8S(pod.Name, requestInfo.TraceID)
	// get checkpoint result
	result, err := m.queryCheckpointResult(pod, requestInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to query checkpoint result: %s, func: %s, traceID: %s, error: %s, "+
			"duration: %s", pod.Name, functionService.ID, requestInfo.TraceID, err.Error(), dur.print(time.Now()))
		return nil, err
	}
	log.GetLogger().Infof("%s | succeed to query checkpoint result: %v, duration: %s", requestInfo.TraceID, result,
		dur.print(time.Now()))
	return result, nil
}

func (m *manager) getFunctionMetaWithRetry(traceID string, funcSvcID string) *faasfunction.FunctionService {
	// To prevent delayed cache synchronization, we will add retries here with a maximum wait time of 3 minutes.
	var functionService *faasfunction.FunctionService
	for i := 0; i < defaultGetFuncMetaRetryTimes; i++ {
		functionService = cache.WorkerCache.GetFuncMeta(funcSvcID)
		if functionService != nil {
			log.GetLogger().Infof("succeed to get function meta after "+
				"retry %d times for %s, traceID: %s", i+1, funcSvcID, traceID)
			break
		}
		time.Sleep(defaultFailureRetryInterval * time.Second)
	}
	return functionService
}

func (m *manager) generalizePod(requestInfo *config.RequestInfo,
	functionService *faasfunction.FunctionService) (*corev1.Pod, error) {
	pod, err := podfetcher.GetPodFetcher().WithContext(
		podfetcher.GetPodFetcher().GeneralizePodInternal, requestInfo, functionService)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Infof("%s | succeed to get checkpoint instance for funcName: %s ",
		requestInfo.TraceID, requestInfo.FunctionName)
	return pod, nil
}

func (m *manager) fillInRequest(req *fhttp.Request) {
	req.Header.ResetConnectionClose()
	authorization, timestamp := localauth.SignLocally(m.cfg.LocalAuth.AKey, m.cfg.LocalAuth.SKey,
		utils.AppID, m.cfg.LocalAuth.Duration)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.Authorization, authorization)
}

func (m *manager) queryCheckpointResult(pod *corev1.Pod, reqInfo *config.RequestInfo) (*types.SnapshotDeploymentSpec,
	error) {
	req, response := fhttp.AcquireRequest(), fhttp.AcquireResponse()
	scheme := "https"
	if !m.cfg.HTTPSConfig.HTTPSEnable {
		scheme = "http"
	}
	req.SetRequestURI(fmt.Sprintf("%s//%s:%s/checkpoint", scheme, pod.Status.PodIP, workerPort))
	req.Header.SetMethod(fhttp.MethodGet)
	req.Header.Set(constants.HeaderTraceID, reqInfo.TraceID)
	setIAMToken(req, reqInfo)

	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)

	var err error
	result := &types.SnapshotDeploymentSpec{}
	for i := 0; i < m.cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryTimes; i++ {
		m.fillInRequest(req)
		if i != 0 {
			time.Sleep(time.Duration(m.cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryInterval) * time.Second)
		}
		log.GetLogger().Infof("send query checkpoint request to worker %s, times: %d", pod.Name, i)
		if err = m.workerClient.DoTimeout(req, response, queryResultTimeout); err != nil {
			break
		}
		if response.Header.StatusCode() == fhttp.StatusOK {
			unmarshalErr := json.Unmarshal(response.Body(), result)
			if unmarshalErr != nil {
				log.GetLogger().Warnf("unmarshal failed: %s", unmarshalErr.Error())
			}
			if result.SnapshotSize == 0 {
				// checkpoint is not finished
				log.GetLogger().Infof("query checkpoint response: %s, traceID: %s",
					string(response.Body()), reqInfo.TraceID)
				err = errors.New(string(response.Body()))
				continue
			} else {
				return m.fillResultAndReturn(result)
			}
		} else if response.Header.StatusCode() == fhttp.StatusNotFound {
			err = fmt.Errorf("worker unsupported checkpoint request")
			break
		} else {
			if response.Body() != nil {
				err = fmt.Errorf("failed checkpoint response: %v", string(response.Body()))
				break
			}
			err = fmt.Errorf("failed checkpoint response: %v", response)
			break
		}
	}

	return result, err
}

func setIAMToken(req *fhttp.Request, reqInfo *config.RequestInfo) {
	if !utils.IsIAMTokenFeatureEnable() || reqInfo.TenantID == "" || !commonutils.IsCAEFunc(reqInfo.BusinessType) {
		return
	}
	iam.SetIAMTokenRequestHeader(req, reqInfo.TenantID, reqInfo.URN)
}

func (m *manager) fillResultAndReturn(result *types.SnapshotDeploymentSpec) (*types.SnapshotDeploymentSpec, error) {
	snapshotCheckInfo, err := GetSnapshotCheckInfo()
	if err != nil {
		log.GetLogger().Errorf("failed to get snapshot check info from pool config fail! Err is %s", err)
		return result, err
	}
	result.SnapshotCheckInfo = &snapshotCheckInfo
	return result, nil
}

// GetSnapshotCheckInfo get snapshot checkinfo
func GetSnapshotCheckInfo() (types.SnapshotCheckInfo, error) {
	conf, err := config.GetConfig()
	if err != nil {
		return types.SnapshotCheckInfo{}, errors.New("failed to get config err: " + err.Error())
	}
	poolSnapshotConfig := conf.PoolMgrConfig.MultiSnapshotConfig.SnapshotCheckInfo
	if conf == nil || poolSnapshotConfig.ImageOS == "" ||
		poolSnapshotConfig.RunTimeSize == 0 || poolSnapshotConfig.JavaRuntimeHash == "" ||
		poolSnapshotConfig.JavaRuntimeSize == 0 || poolSnapshotConfig.ApmJavaAgentSize == 0 {
		log.GetLogger().Errorf("ImageOS is %s, RunTimeSize is %d, JavaRuntimeSize is %d, "+
			"ApmJavaAgentSize is %d, hash is %s", poolSnapshotConfig.ImageOS, poolSnapshotConfig.RunTimeSize,
			poolSnapshotConfig.JavaRuntimeSize, poolSnapshotConfig.ApmJavaAgentSize, poolSnapshotConfig.JavaRuntimeHash)
		return types.SnapshotCheckInfo{}, errors.New("config value is wrong")
	}
	snapshotConfig := types.SnapshotCheckInfo{}
	snapshotConfig.ImageOS = poolSnapshotConfig.ImageOS
	snapshotConfig.JavaRuntimeHash = poolSnapshotConfig.JavaRuntimeHash
	snapshotConfig.RunTimeSize = poolSnapshotConfig.RunTimeSize
	snapshotConfig.JavaRuntimeSize = poolSnapshotConfig.JavaRuntimeSize
	snapshotConfig.ApmJavaAgentSize = poolSnapshotConfig.ApmJavaAgentSize
	return snapshotConfig, nil
}

func (m *manager) submitResult(resultKey, traceID string, result []byte, etcdType etcd3.EtcdType) error {
	log.GetLogger().Infof("%s | submit checkpoint result for: %s", traceID, anonymousKey(resultKey))
	var err error
	for i := 0; i < defaultSubmitResultRetryTimes; i++ {
		client := etcd3.GetETCDClient(etcdType)
		if client == nil {
			log.GetLogger().Errorf("failed to init %s ETCD client", etcdType.String())
			time.Sleep(time.Second)
			continue
		}
		if client.Client == nil {
			log.GetLogger().Warnf("failed to init %s ETCD client", etcdType.String())
			time.Sleep(time.Second)
			continue
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		err = client.Put(defaultEtcdCtx, etcd3.HandleCellPrefix(resultKey), string(result))
		if err == nil {
			break
		}
	}
	return err
}

// ProcessMultiSnapshotEnableUpdate process multi snapshot enable update event
func (m *manager) ProcessMultiSnapshotEnableUpdate() {
	if m.cfg.PoolMgrConfig.MultiSnapshotConfig.Enable {
		log.GetLogger().Infof("detect MultiSnapshotConfig changed from %s to %s",
			utils.GetMultiSnapshotConfig(), m.cfg.PoolMgrConfig.MultiSnapshotConfig)
		m.updateSnapshotVersion()
	}
}

func (m *manager) updateSnapshotVersion() {
	err := m.updateSnapshotListFromEtcd()
	if err != nil {
		log.GetLogger().Warnf("update meta SnapshotVersion failed, err: %s", err.Error())
	}

	err = m.updateSnapshotListFromCAEEtcd()
	if err != nil {
		log.GetLogger().Warnf("update cae meta SnapshotVersion failed, err: %s", err.Error())
	}
}

func (m *manager) updateSnapshotListFromEtcd() error {
	imageTag := m.getRuntimeManagerImageTag()

	snapshotList, err := m.getSnapshotListFromEtcd()
	if err != nil {
		return err
	}

	snapshotCheckInfo := m.cfg.PoolMgrConfig.MultiSnapshotConfig.SnapshotCheckInfo
	return m.updateVersion(snapshotList, imageTag, snapshotCheckInfo, etcd3.Meta)
}

func (m *manager) updateSnapshotListFromCAEEtcd() error {
	imageTag := m.getRuntimeManagerImageTag()

	snapshotList, err := m.getSnapshotListFromCAEEtcd()
	if err != nil {
		return err
	}

	snapshotCheckInfo := m.cfg.PoolMgrConfig.MultiSnapshotConfig.SnapshotCheckInfo
	return m.updateVersion(snapshotList, imageTag, snapshotCheckInfo, etcd3.CAEMeta)
}

func (m *manager) getSnapshotListFromEtcd() (*clientv3.GetResponse, error) {
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || client.Client == nil {
		log.GetLogger().Warnf("failed to init cae meta ETCD client")
		return nil, errors.New("failed to init cae meta ETCD client")
	}
	snapshotList, err := client.GetResponse(defaultCtx, etcd3.HandleCellPrefix(watchPathPrefix), clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("failed to connect to etcd, err: %s", err.Error())
		return nil, err
	}
	return snapshotList, nil
}

func (m *manager) getSnapshotListFromCAEEtcd() (*clientv3.GetResponse, error) {
	defaultCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	client := etcd3.GetCAEMetaEtcdClient()
	if client == nil || client.Client == nil {
		log.GetLogger().Warnf("failed to init cae meta ETCD client")
		return nil, errors.New("failed to init cae meta ETCD client")
	}
	snapshotList, err := client.GetResponse(defaultCtx, etcd3.HandleCellPrefix(watchPathPrefix), clientv3.WithPrefix())
	if err != nil {
		log.GetLogger().Errorf("failed to connect to etcd, err: %s", err.Error())
		return nil, err
	}
	return snapshotList, nil
}

func (m *manager) updateVersion(snapshotList *clientv3.GetResponse, imageTag string,
	snapshotCheckInfo types.SnapshotCheckInfo, etcdType etcd3.EtcdType) error {
	for _, kv := range snapshotList.Kvs {
		snapshotDeployment := &types.SnapshotDeploymentSpec{}
		if err := json.Unmarshal(kv.Value, snapshotDeployment); err != nil {
			log.GetLogger().Errorf("getSnapshotListFromEtcd failed to unmarshal the json, error: %s", err.Error())
			continue
		}
		snapshotKey := parseFromKey(string(kv.Key))
		if snapshotDeployment.SnapshotVersions != nil {
			log.GetLogger().Infof("current snapshot version exists %+v"+
				"no need to add version", snapshotDeployment.SnapshotVersions)
			continue
		}

		addVersion(snapshotDeployment, imageTag, snapshotCheckInfo)
		traceID := "updateSnapshot-" + commonutils.UniqueID()
		value, err := json.Marshal(snapshotDeployment)
		if err != nil {
			log.GetLogger().Errorf("failed to marshal snapshotDeployment: %s, traceID: %s", err.Error(), traceID)
			continue
		}
		functionName := snapshotKey.funcName
		snapshotID := snapshotKey.snapshotID
		log.GetLogger().Infof("start to add snapshot version for %s, id: %s, traceID: %s",
			functionName, snapshotID, traceID)

		// write snapshot version result
		if err = m.submitResult(string(kv.Key), traceID, value, etcdType); err != nil {
			log.GetLogger().Errorf("failed to submit result for func: %s with traceID: %s and error: %s",
				functionName, traceID, err.Error())
		}
	}
	log.GetLogger().Infof("finish to add snapshot version for %d functions", len(snapshotList.Kvs))
	return nil
}

func addVersion(snapshotDeployment *types.SnapshotDeploymentSpec, imageTag string,
	snapshotCheckInfo types.SnapshotCheckInfo) {
	newVersion := types.SnapshotVersion{}
	newVersion.ObjectID = snapshotDeployment.ObjectID
	newVersion.SnapshotImageTag = imageTag
	newVersion.SnapshotCheckInfo = snapshotCheckInfo
	newVersion.SnapshotSize = snapshotDeployment.SnapshotSize
	newVersion.SnapshotSha256 = snapshotDeployment.SnapshotSha256
	newVersion.Timestamp = time.Now().Unix()
	snapshotDeployment.SnapshotVersions = append(snapshotDeployment.SnapshotVersions, newVersion)
}

func (m *manager) getRuntimeManagerImageTag() string {
	runtimeImageTag := m.cfg.PoolMgrConfig.PoolContainerTags.RuntimeImageTag
	return utils.SplitImageTagByColon(runtimeImageTag)
}

func (m *manager) runUpdating() {
	m.start = time.Now()
	imageTag := m.getRuntimeManagerImageTag()
	log.GetLogger().Infof("ReCheckpoint | %s | Start to update snapshot.", imageTag)
	snapshotList, err := m.getSnapshotListFromCAEEtcd()
	if err != nil {
		return
	}
	m.getCheckPointRedisClientWithRetry()
	snapshotCheckInfo := m.cfg.PoolMgrConfig.MultiSnapshotConfig.SnapshotCheckInfo
	err = m.reCheckpointSnapshotList(snapshotList, imageTag, snapshotCheckInfo)
	if err != nil {
		log.GetLogger().Errorf("ReCheckpoint | %s | failed to reCheckpointSnapshotList: %s",
			imageTag, err.Error())
		return
	}
}

func (m *manager) reCheckpointSnapshotList(snapshotList *clientv3.GetResponse,
	imageTag string, info types.SnapshotCheckInfo) error {
	m.generateNeedCheckpointMap(snapshotList, imageTag, info)
	// wait for logProgress
	m.wg.Add(1)
	go m.logProgress()
	// block until idle
	if m.needCheckPointMap != nil && len(m.needCheckPointMap) > 0 {
		for etcdKey, snapshotDeployment := range m.needCheckPointMap {
			if snapshotDeployment == nil {
				m.wg.Done()
				log.GetLogger().Errorf("ETCD key: %s, snapshotDeployment is null!", etcdKey)
				continue
			}
			dur := &duration{receiveRequest: time.Now()}
			keyObj := parseFromKey(etcdKey)
			snapshotEvent := &SnapshotEvent{}
			snapshotEvent.resultKey = etcdKey
			snapshotEvent.etcdType = etcd3.CAEMeta
			snapshotEvent.snapshotEtcdKey = keyObj
			snapshotEvent.snapshotResult = snapshotDeployment

			getNeedRemoveObjectIDs(snapshotEvent)
			go m.startCheckpoint(snapshotEvent, dur, constants.CheckpointModeUpdate)
		}
	}
	return nil
}

func getNeedRemoveObjectIDs(checkpointEvent *SnapshotEvent) {
	// clear historical residual data
	checkpointEvent.snapshotResult.NeedDeleteObjectIDs = nil
	if len(checkpointEvent.snapshotResult.SnapshotVersions) <= maxHistorySnapshotNum {
		return
	}
	snapshotVersions := checkpointEvent.snapshotResult.SnapshotVersions

	for i := maxHistorySnapshotNum; i < len(snapshotVersions); i++ {
		if snapshotVersions[i].ObjectID != "" {
			checkpointEvent.snapshotResult.NeedDeleteObjectIDs = append(checkpointEvent.snapshotResult.NeedDeleteObjectIDs,
				snapshotVersions[i].ObjectID)
		}
	}
}

func (m *manager) generateNeedCheckpointMap(snapshotList *clientv3.GetResponse,
	imageTag string, info types.SnapshotCheckInfo) {
	tenantIds := utils.GetSnapshotTenantIDsConfig().TenantIDs
	enableWhiteList := utils.GetSnapshotTenantIDsConfig().Enable
	for _, kv := range snapshotList.Kvs {
		// only whiteList user can go checkpoint by snapshot-manager
		if enableWhiteList {
			tenantID := parseFromKey(string(kv.Key)).tenantID
			if tenantID == "" {
				log.GetLogger().Errorf("failed to parse from etcd tenantID : %s", anonymousKey(tenantID))
				continue
			}
			if !contains(tenantIds, tenantID) {
				log.GetLogger().Infof("ReCheckpoint | %s | %s is not in white list. Skip it.",
					imageTag, anonymousKey(string(kv.Key)))
				continue
			}
		}
		snapshotDeployment := &types.SnapshotDeploymentSpec{}
		if err := json.Unmarshal(kv.Value, snapshotDeployment); err != nil {
			log.GetLogger().Errorf("ReCheckpoint | %s | getSnapshotListFromEtcd failed to unmarshal the json, "+
				"error: %s", imageTag, err.Error())
			continue
		}

		if snapshotDeployment.SnapshotVersions == nil {
			log.GetLogger().Errorf("ReCheckpoint | %s | not support multi snapshot version", imageTag)
			continue
		}
		keyObj := parseFromKey(string(kv.Key))
		m.reCheckpointByVersion(snapshotDeployment, imageTag, info, keyObj, kv)
		log.GetLogger().Infof("ReCheckpoint | %s | current snapshot version exists %+v", imageTag,
			snapshotDeployment.SnapshotVersions)
	}
}

func contains(arr []string, target string) bool {
	for _, value := range arr {
		if value == target {
			return true
		}
	}
	return false
}

func (m *manager) reCheckpointByVersion(snapshotDeployment *types.SnapshotDeploymentSpec,
	currentImageTag string, info types.SnapshotCheckInfo, keyObj snapshotEtcdKey, kv *mvccpb.KeyValue) {
	funcSvcID := utils.CrName(keyObj.businessID, keyObj.tenantID, keyObj.funcName, keyObj.funcVersion)
	// re-checkpoint triggers only both condition meets:
	// 1. There exists no version has the same imageTag of Snapshot-Manager in SnapshotVersions
	// 2. There exists a version has different javaRuntimeSize compared with the one configured
	// in worker-manager-service-configmap
	existNewestVersion, existMatchVersion := m.judgeSnapshotVersion(snapshotDeployment, currentImageTag, info, funcSvcID)

	if !existNewestVersion && !existMatchVersion {
		log.GetLogger().Infof("ReCheckpoint | %s | %s | current snapshot version and java-runtime is different, "+
			"start re-do checkpoint", m.getRuntimeManagerImageTag(), funcSvcID)
		if keyObj.funcName == "" {
			log.GetLogger().Errorf("ReCheckpoint | %s | failed to parse from etcd event key: %s",
				m.getRuntimeManagerImageTag(),
				anonymousKey(string(kv.Key)))
			return
		}
		// find snapshot that need to update and start record update log
		m.wg.Add(1)
		atomic.AddInt64(&m.totalNum, 1)
		m.needCheckPointMap[string(kv.Key)] = snapshotDeployment
	}
}

func (m *manager) judgeSnapshotVersion(snapshotDeployment *types.SnapshotDeploymentSpec, currentImageTag string,
	info types.SnapshotCheckInfo, funcSvcID string) (bool, bool) {
	existNewestVersion := false
	existMatchVersion := false
	for index, snapshotVersion := range snapshotDeployment.SnapshotVersions {
		// find if exists currentImageTag
		if snapshotVersion.SnapshotImageTag == currentImageTag &&
			snapshotVersion.SnapshotSha256 != "" &&
			snapshotVersion.SnapshotSize != 0 {
			log.GetLogger().Infof("ReCheckpoint | %s | %s | current snapshot version stays the same. "+
				"skip re-do checkpoint", m.getRuntimeManagerImageTag(), funcSvcID)
			existNewestVersion = true
			break
		}

		// find if exists a version has matched SnapshotCheckInfo
		if snapshotVersion.SnapshotCheckInfo.JavaRuntimeSize == info.JavaRuntimeSize &&
			snapshotVersion.SnapshotCheckInfo.ApmJavaAgentSize == info.ApmJavaAgentSize &&
			snapshotVersion.SnapshotCheckInfo.RunTimeSize == info.RunTimeSize &&
			snapshotVersion.SnapshotCheckInfo.ImageOS == info.ImageOS {
			log.GetLogger().Infof("ReCheckpoint | %s | %s | current snapshot java-runtime stays the same. "+
				"skip re-do checkpoint", m.getRuntimeManagerImageTag(), funcSvcID)
			existMatchVersion = true
			// snapshotImageTag does not equal but snapshotCheckInfo equals, snapshotImageTag needs to be the newest as well
			snapshotVersion.SnapshotImageTag = currentImageTag
			snapshotDeployment.SnapshotVersions[index] = snapshotVersion
			break
		}
	}
	return existNewestVersion, existMatchVersion
}

func (m *manager) UpdateCheckpointPodConfig(newCfg *config.FaasConfig) {
	checkpointMgrGuard.Lock()
	defer checkpointMgrGuard.Unlock()
	oldMaxConCurrency := m.ConcurrencyLimiter.limit
	newMaxConCurrency := m.cfg.PoolMgrConfig.CheckpointPodConfig.MaxConcurrency
	if oldMaxConCurrency != newMaxConCurrency {
		log.GetLogger().Infof("detect CheckpointPodConfig maxConcurrency changed from %d to %d",
			oldMaxConCurrency, newMaxConCurrency)
		m.Reset(newMaxConCurrency)
	}
	m.cfg = newCfg
	return
}

func (m *manager) sendSnapshotRequestToWorker(pod *corev1.Pod, reqInfo *config.RequestInfo, language,
	action string) error {
	req, response := fhttp.AcquireRequest(), fhttp.AcquireResponse()
	scheme := "https"
	if !m.cfg.HTTPSConfig.HTTPSEnable {
		scheme = "http"
	}
	err := m.setRequestURL(pod, req, action, scheme)
	if err != nil {
		return err
	}

	req.Header.SetMethod(fhttp.MethodGet)
	req.Header.Set(constants.HeaderTraceID, reqInfo.TraceID)
	req.Header.Set(constants.Language, language)
	setIAMToken(req, reqInfo)
	snapshotDeployment := reqInfo.SnapshotDeployment
	bodyValue, err := json.Marshal(snapshotDeployment)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal reqInfo to byte, err: %s", err.Error())
		return err
	}
	req.SetBody(bodyValue)
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)

	for i := 0; i < m.cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryTimes; i++ {
		m.fillInRequest(req)
		if i != 0 {
			time.Sleep(time.Duration(m.cfg.PoolMgrConfig.CheckpointPodConfig.FailureRetryInterval) * time.Second)
		}
		log.GetLogger().Infof("send %s ds request to worker %s, times: %d", action, pod.Name, i)
		if err = m.workerClient.DoTimeout(req, response, queryResultTimeout); err != nil {
			log.GetLogger().Warnf("failed to send upload ds request to worker %s, err: %s",
				pod.Name, err.Error())
			continue
		}

		if response.Header.StatusCode() == fhttp.StatusOK {
			return nil
		} else if response.Header.StatusCode() == fhttp.StatusNotFound {
			err = fmt.Errorf("worker unsupported checkpoint request")
			break
		} else {
			if response.Body() != nil {
				log.GetLogger().Warnf("failed to %s snapshot to ds, response: %s", action, string(response.Body()))
				err = fmt.Errorf("failed to %s snapshot to ds, response: %s", action, string(response.Body()))
			} else {
				err = fmt.Errorf("failed to %s snapshot to ds", action)
			}
			continue
		}
	}

	return err
}

func (m *manager) setRequestURL(pod *corev1.Pod, req *fhttp.Request, action, scheme string) error {
	switch action {
	case constants.UploadSnapshot:
		req.SetRequestURI(fmt.Sprintf("%s//%s:%s/uploadSnapshotToDS", scheme, pod.Status.PodIP, workerPort))
	case constants.DeleteSnapshot:
		req.SetRequestURI(fmt.Sprintf("%s//%s:%s/deleteSnapshotData", scheme, pod.Status.PodIP, workerPort))
	default:
		return errors.New("unsupported snapshot event")
	}
	return nil
}
