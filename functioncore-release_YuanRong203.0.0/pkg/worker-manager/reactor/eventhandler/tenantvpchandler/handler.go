/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package tenantvpchandler defines handler of tenant vpc
package tenantvpchandler

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

// Handler handle etcd event of tenant vpc router
type Handler struct {
}

// NewEventHandler create new Handler
func NewEventHandler() *Handler {
	return &Handler{}
}

// HandleEvent handle tenant vpc router event
func (handler *Handler) HandleEvent(event eventsource.Event) error {
	log.GetLogger().Infof("process %s event, type %s, key %s from %s etcd, since receive: %v", event.Kind,
		event.EventType, event.KeyRef, event.SourceType, time.Now().Sub(event.StartTime))
	tenantID, err := parseTenantID(event.KeyRef)
	if err != nil {
		return err
	}
	tenantVPCRouter, err := parseTenantVPCRouter(event)
	if err != nil {
		return err
	}
	switch event.EventType {
	case eventsource.Added, eventsource.Updated:
		log.GetLogger().Infof("update tenantVPCRouter for %s", tenantID)
		cache.GetTenantVPCCache().Set(tenantID, tenantVPCRouter)
	case eventsource.Deleted:
		log.GetLogger().Infof("delete tenantVPCRouter for %s", tenantID)
		cache.GetTenantVPCCache().Delete(tenantID)
	default:
		log.GetLogger().Errorf("unknown event type %s", event.EventType)
	}
	return nil
}

// /sn/business/yrk/tenant/xxxxx/router
func parseTenantID(key string) (string, error) {
	strs := strings.Split(key, "/")
	if len(strs) != utils.URNIndexSeven {
		log.GetLogger().Errorf("invalid tenant vpc router key %s", key)
		return "", errors.New("invalid tenant vpc router key")
	}
	return strs[utils.URNIndexFive], nil
}

func parseTenantVPCRouter(event eventsource.Event) (*config.TenantVpcRouter, error) {
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("invalid event object %s", event.KeyRef)
		return nil, errors.New("invalid event object")
	}
	var (
		tenantVPCRouter = new(config.TenantVpcRouter)
		err             error
	)
	if event.EventType == eventsource.Deleted {
		err = json.Unmarshal(ev.PrevValue, tenantVPCRouter)
	} else {
		err = json.Unmarshal(ev.Value, tenantVPCRouter)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal tenant vpc router %s, err %s", event.KeyRef, err.Error())
		return nil, err
	}
	return tenantVPCRouter, nil
}
