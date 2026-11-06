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

// Package etcdevents handle etcd
package etcdevents

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

var (
	// ProcessSilentEvent -
	ProcessSilentEvent func(event *etcd3.Event)
	// wait for the ProcessSilentEvent method to register successfully
	readyChan chan struct{}
)

const keepConnAliveTTL = 3 * time.Second

// RegisterReadyChan -
func RegisterReadyChan() {
	readyChan = make(chan struct{})
}

// readyChanDone -
func readyChanDone() {
	close(readyChan)
}

// AwaitReadyChanDone -
func AwaitReadyChanDone() {
	if readyChan == nil {
		return
	}
	<-readyChan
}

// Provisioner define etcd event provision
type Provisioner struct {

	// Namespace is the kubernetes namespace to watch for events from.
	Namespace string
	// SinkURI is the URI messages will be forwarded on to.
	SinkQueue workqueue.RateLimitingInterface

	// SourceType : which etcd to connect
	SourceType etcd3.EtcdType

	Kind string

	lastRev int64
}

// NewProvisioner New Provisioner
func NewProvisioner(sourcetype etcd3.EtcdType, kind string, namespace string,
	queue workqueue.RateLimitingInterface) *Provisioner {

	imp := &Provisioner{
		Namespace:  namespace,
		SinkQueue:  queue,
		SourceType: sourcetype,
		Kind:       kind,
	}
	return imp
}

func (a *Provisioner) createWatcher() *etcd3.EtcdWatchChan {
	client := etcd3.GetETCDClient(a.SourceType)
	if a.SourceType == etcd3.Meta && client.Client == nil {
		utils.MetaCacheOnce.Do(func() {
			log.GetLogger().Warnf("failed to init meta etcd client")
			utils.LocalMetaEtcdCache.InitMetaEtcdCache()
		})
		if a.Kind == utils.FunctionRepoEventKind {
			utils.GetCacheOnce.Do(func() {
				for _, event := range utils.LocalMetaEtcdCache.GetAllStoreMetaInfo() {
					log.GetLogger().Infof("success to get local meta data: %s", event.Key)
					a.processWatchEvent(event)
				}
			})
		}
	}
	watchChan := a.getWatchChan(client)
	return watchChan
}

func (a *Provisioner) getWatchChan(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if metaClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			rev := int64(0)
			cfg, err := config.GetConfig()
			if err == nil && atomic.LoadUint32(&cfg.DisableEtcdRev) == 0 {
				rev = a.lastRev
			}
			log.GetLogger().Infof("rewatch using rev %d", rev)
			if a.Namespace == utils.ClusterMetadataKey {
				ctx := context.WithValue(context.Background(), etcd3.CellEnable, a.isCellEnable())
				watchChannel := metaClient.Watch(ctx, a.Namespace, rev, false)
				ticker.Stop()
				return watchChannel
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, a.isCellEnable())
			watchChannel := metaClient.Watch(ctx, a.Namespace, rev, true)
			ticker.Stop()
			return watchChannel
		}
	}
}

// Start Run starts the kubernetes
// receive adapter and sends events produced in the
// configured namespace and send them to the sink uri provided.
func (a *Provisioner) Start(stopCh <-chan struct{}) error {
	go a.StartWatch(stopCh)
	return nil
}

// StartWatch Start watch Channel
func (a *Provisioner) StartWatch(stop <-chan struct{}) {
	if stop == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	watchChan := a.createWatcher()
	if watchChan == nil {
		log.GetLogger().Errorf("failed to create watcher")
		return
	}
	if a.Kind == utils.FunctionRepoEventKind && a.SourceType == etcd3.Meta {
		ProcessSilentEvent = a.processEvent
		readyChanDone()
		log.GetLogger().Infof("The processSilentEvent function has been registered")
	}
	utils.LocalMetaEtcdCache.ClearAllStoreMetaInfo()
	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("it's going to reconnect as ResultChan may closed")
				watchChan.Stop()
				watchChan = a.createWatcher()
				continue
			}
			a.processWatchEvent(event)
			if event.Rev > a.lastRev {
				a.lastRev = event.Rev
			}
			if event.Type == etcd3.ERROR {
				errMsg := string(event.Value)
				log.GetLogger().Errorf("etcd event has error: %s", errMsg)
				if strings.Contains(errMsg, "compacted") {
					log.GetLogger().Warnf("revision has been compacted, reset rev to 0")
					a.lastRev = 0
				}
			}
		case _, ok := <-stop:
			if !ok {
				log.GetLogger().Errorf("start watch stop channel may be closed")
				return
			}
			log.GetLogger().Warnf("received signal quit")
			return
		}
	}
}

func (a *Provisioner) processWatchEvent(event *etcd3.Event) {
	if !a.isFunctionRepo(event.Key) && !a.isNativeInstance(event.Key) && !a.isInstanceMetadata(event.Key) &&
		!a.isTenantMetadata(event.Key) && !a.isDefaultTenantMetadata(event.Key) && !a.isTenantVPCRouter(event.Key) &&
		!a.isInstancePrediction(event.Key) && !a.isAPIGRouteInfo(event.Key) && !a.isClusterInfo(event.Key) &&
		!a.isStateRouter(event.Key) && !a.isPublicNetworkInfo(event.Key) && !a.isCaeConfig(event.Key) &&
		!a.isPreload(event.Key) && !a.isHCSLicenseMetadata(event.Key) {
		log.GetLogger().Warnf("skipped the event that was not a chain/repo/native instance/instanceMetadata/" +
			"TenantMetadata/defaultTenantMetadata/tenantVPCRouter/instancePrediction/apigRouteInfo/clusterInfo/" +
			"stateRouter/publicNetworkInfo/caeConfig/preload/hcsLicenseMetadata")
		return
	}
	a.processEvent(event)
}

func (a *Provisioner) processEvent(event *etcd3.Event) {
	switch event.Type {
	case etcd3.PUT:
		eventItem := a.generateEvents(event.Key, eventsource.Added, event)
		a.SinkQueue.Add(eventItem)
		log.GetLogger().Infof("etcd add event, key: %s", etcdkey.AnonymizeTenantCommonEtcdKey(event.Key))
	case etcd3.DELETE:
		eventItem := a.generateEvents(event.Key, eventsource.Deleted, event)
		a.SinkQueue.Add(eventItem)
		log.GetLogger().Infof("etcd delete event, key: %s", etcdkey.AnonymizeTenantCommonEtcdKey(event.Key))
	case etcd3.ERROR:
		log.GetLogger().Errorf("receive etcd error event, err: %s", string(event.Value))
	default:
		log.GetLogger().Warnf("etcd unknown event")
	}
}

func (a *Provisioner) isFunctionRepo(key string) bool {
	strs := strings.Split(key, "/")

	// 11 is the length of function repo string
	if len(strs) != utils.URNIndexEleven {
		return false
	}

	if strs[utils.URNIndexSeven] != "function" || strs[utils.URNIndexNine] != "version" ||
		strs[utils.URNIndexTen] == "" {
		return false
	}

	return true
}

// /sn/workers/business/iot/tenant/j0f4413f7b4b4c33be576d432f7ee085/function/functest/version/$latest
// /cn-north-1a/cn-north-1a-#-ws-j0f4413f7b-functest-faaslatest-deployment-55b5f9dcb7-r2dsv
func (a *Provisioner) isNativeInstance(key string) bool {
	strs := strings.Split(key, "/")

	if len(strs) != utils.URNIndexThirteen {
		return false
	}

	if strs[utils.URNIndexSeven] != "function" || strs[utils.URNIndexNine] != "version" ||
		strs[utils.URNIndexTwelve] == "" {
		return false
	}

	if strs[utils.URNIndexEleven] != commonutils.AzEnv() {
		return false
	}

	return true
}

func (a *Provisioner) isInstancePrediction(key string) bool {
	elements := strings.Split(key, "/")
	if len(elements) != utils.URNIndexFive {
		return false
	}
	if elements[utils.URNIndexOne] != "prediction" || elements[utils.URNIndexTwo] != "cluster" {
		return false
	}
	return true
}

// apig meta info key in etcd is /sn/apig/address
func (a *Provisioner) isAPIGRouteInfo(key string) bool {
	if key != "/sn/apig/address" {
		return false
	}
	return true
}

// public network meta info key in etcd is /sn/publicnetwork/$project_id
func (a *Provisioner) isPublicNetworkInfo(key string) bool {
	elements := strings.Split(key, "/")
	if len(elements) != utils.URNIndexFour {
		return false
	}
	if elements[utils.URNIndexOne] != "sn" || elements[utils.URNIndexTwo] != "publicnetwork" {
		return false
	}
	return true
}

// cluster meta info key in etcd is fss_cluster_flow_state
func (a *Provisioner) isClusterInfo(key string) bool {
	if key != utils.ClusterMetadataKey {
		return false
	}
	return true
}

func (a *Provisioner) isInstanceMetadata(key string) bool {
	strs := strings.Split(key, "/")
	if len(strs) != utils.URNIndexTwelve {
		return false
	}
	if strs[utils.URNIndexEight] != "function" || strs[utils.URNIndexTen] != "version" {
		return false
	}
	return true
}

func (a *Provisioner) isTenantMetadata(etcdRef string) bool {
	// An example of a tenant metadata key:
	// /sn/quota/cluster/<clusterID>/tenant/<tenantID>/instancemetadata
	strs := strings.Split(etcdRef, "/")
	if len(strs) != utils.URNIndexEight {
		return false
	}
	if strs[utils.URNIndexTwo] != "quota" || strs[utils.URNIndexSeven] != "instancemetadata" {
		return false
	}
	return true
}

func (a *Provisioner) isDefaultTenantMetadata(etcdRef string) bool {
	// An example of default tenant metadata key:
	// /sn/quota/cluster/<clusterID>/default/instancemetadata
	strs := strings.Split(etcdRef, "/")
	if len(strs) != utils.URNIndexSeven {
		return false
	}
	if strs[utils.URNIndexTwo] != "quota" || strs[utils.URNIndexSix] != "instancemetadata" {
		return false
	}
	return true
}

func (a *Provisioner) isHCSLicenseMetadata(etcdRef string) bool {
	// /hcs/license/resource/quota/cluster/functiongraph-tenant-1
	clusterID := os.Getenv("CLUSTER_ID")
	strs := strings.Split(etcdRef, "/")
	if len(strs) != utils.URNIndexSeven {
		return false
	}
	if strs[utils.URNIndexSix] != clusterID || strs[utils.URNIndexTwo] != "license" {
		return false
	}
	return true
}

func (a *Provisioner) isTenantVPCRouter(key string) bool {
	return strings.HasPrefix(key, utils.TenantVPCRouterPrefix)
}

func (a *Provisioner) isStateRouter(key string) bool {
	return strings.HasPrefix(key, utils.StateWorkerKey)
}

func (a *Provisioner) isCaeConfig(key string) bool {
	return strings.HasPrefix(key, utils.CaeConfigMetadataPrefix)
}

func (a *Provisioner) isPreload(key string) bool {
	return strings.HasPrefix(key, utils.PreloadPrefix)
}

func (a *Provisioner) isCellEnable() bool {
	return a.SourceType == etcd3.Meta || a.SourceType == etcd3.CAEMeta
}

func (a *Provisioner) generateEvents(key string, sourcetype string, value interface{}) eventsource.Event {
	if a.Kind == utils.InstanceMetadataEventKind {
		key = convertToFunctionKey(key)
	}
	return eventsource.Event{
		SourceType: a.SourceType.String(),
		EventType:  sourcetype,
		Kind:       a.Kind,
		KeyRef:     key,
		Object:     value,
		StartTime:  time.Now(),
	}
}

/*
convertToFunctionKey Converts the key of instance metadata to the key of function metadata.

	/instances/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}
	/sn/functions/business/yrk/tenant/{tenantID}/function/{functionName}/version/{version}
*/
func convertToFunctionKey(key string) string {
	strs := strings.Split(key, "/")
	if len(strs) < utils.URNIndexTwelve {
		log.GetLogger().Debugf("instance key is err. key: %s", key)
		return ""
	}
	return fmt.Sprintf("%s/%s/tenant/%s/function/%s/version/%s", utils.FunctionMetadataPrefix, "yrk",
		strs[utils.URNIndexSeven], strs[utils.URNIndexNine], strs[utils.URNIndexEleven])
}
