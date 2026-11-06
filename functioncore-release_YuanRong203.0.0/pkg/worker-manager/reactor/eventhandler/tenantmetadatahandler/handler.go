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

// Package tenantmetadatahandler -
package tenantmetadatahandler

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

// TenantMetadataHandler define tenant metadata handler
type TenantMetadataHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewEventHandler new event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *TenantMetadataHandler {
	handler := &TenantMetadataHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

// HandleEvent handle event
func (handler *TenantMetadataHandler) HandleEvent(event evs.Event) error {
	log.GetLogger().Infof("start to process tenant metadata event, resource key: %s, type %s",
		event.KeyRef, event.EventType)
	if event.Kind != utils.TenantMetadataEventKind {
		return errors.New("failed to match event kind to TenantMetadata")
	}

	// check key format
	strs := strings.Split(event.KeyRef, "/")
	if len(strs) == utils.URNIndexSeven && strs[utils.URNIndexSix] == "instancemetadata" {
		log.GetLogger().Infof("default tenant metadata key changed: %s", event.KeyRef)
		err := handleDefaultEvent(event)
		return err
	} else if len(strs) == utils.URNIndexEight {
		tenantID := strs[utils.URNIndexSix]
		err := handleTenantMetadataEvent(event, tenantID)
		return err
	}
	log.GetLogger().Errorf("invalid tenant metadata key: %s", event.KeyRef)
	return errors.New("invalid tenant metadata key")
}

func handleTenantMetadataEvent(event evs.Event, tenantID string) error {
	start := time.Now()
	var (
		tenantMetaInfo = config.TenantMetaInfo{}
		err            error
	)

	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("tenant metadata value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantMetadataEtcdKey(event.KeyRef), event.Object)
		return errors.New("invalid event object")
	}

	// check value
	if event.EventType == evs.Deleted {
		err = json.Unmarshal(ev.PrevValue, &tenantMetaInfo)
	} else {
		err = json.Unmarshal(ev.Value, &tenantMetaInfo)
	}
	if err != nil {
		log.GetLogger().Errorf("unmarshal tenant metadata info failed, key: %s and error: %s",
			etcdkey.AnonymizeTenantMetadataEtcdKey(event.KeyRef), err.Error())
		return err
	}

	tenant := cache.Tenant{
		TenantID:       tenantID,
		TenantMetaInfo: tenantMetaInfo,
	}

	switch event.EventType {
	case evs.Added:
		log.GetLogger().Infof("tenant metadata add event type %s", event.EventType)
		cache.TenantCache.UpdateOrAddTenantCache(tenant)
	case evs.Updated:
		log.GetLogger().Infof("tenant metadata update event type %s", event.EventType)
		cache.TenantCache.UpdateOrAddTenantCache(tenant)
	case evs.Deleted:
		log.GetLogger().Infof("tenant metadata delete event type %s", event.EventType)
		cache.TenantCache.DeleteTenantCache(tenant.TenantID)
	default:
		log.GetLogger().Errorf("tenant metadata unsupported event type %s", event.EventType)
		return errors.New("unsupported event type for tenant metadata")
	}
	log.GetLogger().Infof("finished to process tenant metadata event, resource key: %s, type %s, cost: %s",
		event.KeyRef, event.EventType, time.Since(start))
	return nil
}

func handleDefaultEvent(event evs.Event) error {
	start := time.Now()
	var (
		tenantMetaInfo = config.TenantMetaInfo{}
		err            error
	)

	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("default tenant metadata value type from etcd is invalid, key: %s, type: %T",
			event.KeyRef, event.Object)
		return errors.New("invalid event object")
	}

	// check value
	if event.EventType == evs.Deleted {
		err = json.Unmarshal(ev.PrevValue, &tenantMetaInfo)
	} else {
		err = json.Unmarshal(ev.Value, &tenantMetaInfo)
	}
	if err != nil {
		log.GetLogger().Errorf("unmarshal default tenant metadata info failed, key: %s and error: %s",
			event.KeyRef, err.Error())
		return err
	}
	log.GetLogger().Infof("default tenant metadata info: %+v", tenantMetaInfo)

	tenant := cache.Tenant{
		TenantID:       "default",
		TenantMetaInfo: tenantMetaInfo,
	}

	switch event.EventType {
	case evs.Added:
		log.GetLogger().Infof("default tenant metadata add event type %s", event.EventType)
		cache.TenantCache.UpdateDefaultQuota(tenant)
	case evs.Updated:
		log.GetLogger().Infof("default tenant metadata update event type %s", event.EventType)
		cache.TenantCache.UpdateDefaultQuota(tenant)
	case evs.Deleted:
		log.GetLogger().Infof("default tenant metadata delete event type %s, ignore", event.EventType)
	default:
		log.GetLogger().Errorf("default tenant metadata unsupported event type %s", event.EventType)
		return errors.New("unsupported event type for tenant metadata")
	}
	log.GetLogger().Infof("finished to process default tenant metadata event, resource key: %s, type %s, cost: %s",
		event.KeyRef, event.EventType, time.Since(start))
	return nil
}
