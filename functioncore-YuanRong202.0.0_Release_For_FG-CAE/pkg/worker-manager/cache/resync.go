/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package cache

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	informer "k8s.io/client-go/informers/core/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

const defaultResyncTime = 60 * 60 * 24

var (
	defaultResyncer *Resyncer
)

func init() {
	config.RegisterHotLoadResyncCallBack(hotLoadResync)
}

func hotLoadResync(config *config.FaasConfig, loadConfig *config.HotLoadConfig) {
	if loadConfig.Resync.Interval <= 0 {
		loadConfig.Resync.Interval = defaultResyncTime
	}

	oldInterval := atomic.LoadInt64(&config.Resync.Interval)
	newInterval := loadConfig.Resync.Interval
	if oldInterval != newInterval {
		log.GetLogger().Infof("update resync interval from %d to %d", oldInterval, newInterval)
		atomic.StoreInt64(&config.Resync.Interval, newInterval)
		UpdateResyncInterval(newInterval)
	}
	oldSyncNow := atomic.LoadInt64(&config.Resync.SyncNow)
	newSyncNow := loadConfig.Resync.SyncNow
	if oldSyncNow != newSyncNow {
		log.GetLogger().Infof("update resync syncNow from %d to %d", oldSyncNow, newSyncNow)
		atomic.StoreInt64(&config.Resync.SyncNow, newSyncNow)
		ResyncNow()
	}

	oldDisabled := atomic.LoadUint32(&config.Resync.Disabled)
	newDisabled := loadConfig.Resync.Disabled
	if oldDisabled != newDisabled {
		log.GetLogger().Infof("update resync disabled from %d to %d", oldDisabled, newDisabled)
		atomic.StoreUint32(&config.Resync.Disabled, newDisabled)
		UpdateDisabled(newDisabled)
	}
}

// RunDefaultResyncer init and runs default resyncer
func RunDefaultResyncer(stop <-chan struct{}) {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config, err: %s", err.Error())
		return
	}

	t := defaultResyncTime * time.Second
	if cfg.Resync.Interval != 0 {
		t = time.Duration(cfg.Resync.Interval) * time.Second
	}

	defaultResyncer = NewResyncer(t, utils.GetPodInformer(), stop)
	go defaultResyncer.Run()
}

// ResyncNow resync immediately with default resyncer
func ResyncNow() {
	if defaultResyncer == nil {
		return
	}
	defaultResyncer.ResyncNow()
}

// UpdateResyncInterval update default resync's interval
func UpdateResyncInterval(t int64) {
	if defaultResyncer == nil {
		return
	}
	defaultResyncer.UpdateResyncInterval(t)
}

// UpdateDisabled update default resync's disabled status
func UpdateDisabled(disabled uint32) {
	if defaultResyncer == nil {
		return
	}
	defaultResyncer.UpdateDisabled(disabled)
}

// NewInstanceSelector creates instance pod's label selector
func NewInstanceSelector() labels.Selector {
	req, err := labels.NewRequirement("isPoolPod", selection.Equals, []string{"false"})
	if err != nil {
		log.GetLogger().Errorf("failed to new instance requirement, err: %s", err.Error())
		return labels.Nothing()
	}
	s := labels.NewSelector()
	// NOTE: selector.Add() is similar to append(), we should use the return value
	return s.Add(*req)
}

// NewResyncer create new resyncer
func NewResyncer(resyncTime time.Duration, podInformer informer.PodInformer, stop <-chan struct{}) *Resyncer {
	return &Resyncer{
		informer:    podInformer,
		podSelector: NewInstanceSelector(),
		etcdCache:   make(map[string]*syncCompare),
		ticker:      time.NewTicker(resyncTime),
		syncNow:     make(chan struct{}, 1),
		stop:        stop,
	}
}

// Resyncer compares router etcd info and actual running pod
type Resyncer struct {
	etcdCache    map[string]*syncCompare
	etcdCacheMux sync.RWMutex

	toDelete map[string]*syncCompare
	toAdd    map[string]*syncCompare

	informer informer.PodInformer

	podSelector labels.Selector

	ticker *time.Ticker

	stop <-chan struct{}

	disabled uint32

	syncNow chan struct{}
}

// Run starts resyncer
func (r *Resyncer) Run() {
	defer r.ticker.Stop()
	go r.startWatch()
	for {
		select {
		case <-r.ticker.C:
			r.reSync()
		case <-r.stop:
			log.GetLogger().Warnf("resyncer stopped")
			return
		case _, ok := <-r.syncNow:
			if !ok {
				log.GetLogger().Warnf("resyncer syncNow closed")
				return
			}
			r.reSync()
		}
	}
}

// ResyncNow resync immediately
func (r *Resyncer) ResyncNow() {
	select {
	case r.syncNow <- struct{}{}:
	default:
	}
}

// UpdateResyncInterval update resyncer's interval
func (r *Resyncer) UpdateResyncInterval(t int64) {
	r.ticker.Reset(time.Duration(t) * time.Second)
}

// UpdateDisabled update resyncer's disabled status
func (r *Resyncer) UpdateDisabled(disabled uint32) {
	atomic.StoreUint32(&r.disabled, disabled)
}

func (r *Resyncer) addInstance(key string, value []byte) {
	data := parseEtcdInfo(key, value)
	if data == nil {
		return
	}
	r.etcdCacheMux.Lock()
	r.etcdCache[data.instance.InstanceID] = data
	r.etcdCacheMux.Unlock()
}

func (r *Resyncer) deleteInstance(key string, value []byte) {
	data := parseEtcdInfo(key, value)
	if data == nil {
		return
	}
	r.etcdCacheMux.Lock()
	delete(r.etcdCache, data.instance.InstanceID)
	r.etcdCacheMux.Unlock()
}

func (r *Resyncer) compareWithCache() (map[string]*syncCompare, map[string]*syncCompare) {
	pods, err := r.informer.Lister().List(r.podSelector)
	if err != nil {
		log.GetLogger().Errorf("failed to list pod from informer: %s", err.Error())
		return nil, nil
	}

	expectIns := make(map[string]*syncCompare)

	for i := range pods {
		// check and delete stuck terminating pod
		if utils.IsPodStuckTerminating(pods[i]) {
			log.GetLogger().Infof("start to force delete pod %s", pods[i].Name)
			err := utils.ForciblyDeletePod(pods[i])
			if err != nil {
				log.GetLogger().Warnf("failed to force delete pod %s", pods[i].Name)
			}
		}
		// ignore terminating pod or pods that has no funcName (pod is cold starting)
		if pods[i].DeletionTimestamp != nil || utils.GetFullFuncNameByPod(pods[i]) == "" {
			continue
		}
		info := parsePodInfo(pods[i])
		if info == nil {
			continue
		}
		expectIns[info.instance.InstanceID] = info
	}

	toAdd := make(map[string]*syncCompare)
	toDelete := make(map[string]*syncCompare)

	r.etcdCacheMux.RLock()
	for key, expect := range expectIns {
		if _, ok := r.etcdCache[key]; !ok {
			toAdd[key] = expect
		}
	}
	for key, act := range r.etcdCache {
		if _, ok := expectIns[key]; !ok {
			toDelete[key] = act
		}
	}
	r.etcdCacheMux.RUnlock()
	return toAdd, toDelete
}

func (r *Resyncer) reSync() {
	log.GetLogger().Infof("start to resync")
	toAdd, toDelete := r.compareWithCache()

	disabled := atomic.LoadUint32(&r.disabled) != 0

	for key, expect := range toAdd {
		if _, ok := r.toAdd[key]; ok {
			log.GetLogger().Infof("sync put to etcd with disabled %t %s", disabled, expect.path)
			if disabled {
				continue
			}
			if err := putInstanceToEtcd(expect.instance, expect.path); err != nil {
				log.GetLogger().Errorf("failed to sync put to etcd %s %s", expect.path, err.Error())
			}
		}
	}

	for key, expect := range toDelete {
		if _, ok := r.toDelete[key]; ok {
			log.GetLogger().Infof("sync delete from etcd with disabled %t %s", disabled, expect.path)
			if disabled {
				continue
			}
			if err := deleteFromEtcd(expect.path); err != nil {
				log.GetLogger().Errorf("failed to sync delete from etcd %s %s", expect.path, err.Error())
			}
		}
	}
	r.toAdd = toAdd
	r.toDelete = toDelete
}

func parseEtcdInfo(path string, value []byte) *syncCompare {
	key := etcdkey.WorkerInstanceKey{}
	if err := key.ParseFrom(path); err != nil {
		log.GetLogger().Errorf("invalid worker instance key: %s, err: %s", path, err.Error())
		return nil
	}
	if key.TenantID == "0" {
		return nil
	}
	var instance v1alpha1.WorkerInstance
	err := json.Unmarshal(value, &instance)
	if err != nil {
		log.GetLogger().Errorf("invalid worker instance value: %s, err: %s", path, err.Error())
		return nil
	}
	var ins = etcdkey.WorkerInstanceKey{}
	err = ins.ParseFrom(path)
	if err != nil {
		log.GetLogger().Errorf("invalid worker instance value: %s, err: %s", path, err.Error())
		return nil
	}
	instance.InstanceID = ins.Instance
	return &syncCompare{
		instance: instance,
		path:     path,
	}
}

func parsePodInfo(pod *corev1.Pod) *syncCompare {
	if pod.Labels["applier"] == constants.PredictionApplier {
		return nil
	}
	ins := utils.MakeWorkerInsInfo(pod)
	path := "/sn/workers/business/" + ins.BusinessID + "/tenant/" + ins.TenantID + "/function/" + ins.FuncName +
		"/version/" + utils.VersionRestore(ins.Version) + "/" + commonutils.AzEnv() + "/" + ins.InstanceID
	return &syncCompare{
		instance: utils.MakeWorkerInsInfo(pod),
		path:     path,
	}
}

func (r *Resyncer) createWatcher() *etcd3.EtcdWatchChan {
	watcher := etcd3.GetRouterEtcdClient()
	watchChan := watcher.Watch(context.Background(), "/sn/workers", 0, true)
	return watchChan
}

func (r *Resyncer) startWatch() {
	watchChan := r.createWatcher()
	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("resultChan closed, try to reconnect")
				watchChan.Stop()
				watchChan = r.createWatcher()
				continue
			}
			r.processWatchEvent(event)
		case <-r.stop:
			log.GetLogger().Warnf("resyncer watcher stopped")
			return
		}
	}
}

func (r *Resyncer) processWatchEvent(event *etcd3.Event) {
	switch event.Type {
	case etcd3.PUT:
		r.addInstance(event.Key, event.Value)
	case etcd3.DELETE:
		r.deleteInstance(event.Key, event.PrevValue)
	default:
		log.GetLogger().Warnf("unknown event type: %d %s", event.Type, event.Key)
	}
}
