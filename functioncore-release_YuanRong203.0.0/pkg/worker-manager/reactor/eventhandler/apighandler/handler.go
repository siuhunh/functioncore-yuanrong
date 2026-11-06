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

// Package apighandler -
package apighandler

import (
	"encoding/json"
	"errors"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/cache"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

// RouteHandler define apig route info handler
type RouteHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// RouteInfo stores apig ip
type RouteInfo struct {
	RouteIP []string `json:"apigIps"`
}

// NewEventHandler new event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *RouteHandler {
	handler := &RouteHandler{
		EtcdClient: etcdctl,
	}
	return handler
}

func (handler *RouteHandler) parseAPIGIPMetaInfo(event evs.Event) (RouteInfo, error) {
	apigIP := RouteInfo{}
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("apigIPs value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantCommonEtcdKey(event.KeyRef), event.Object)
		return RouteInfo{}, errors.New("invalid parameter")
	}
	err := json.Unmarshal(ev.Value, &apigIP)
	if err != nil {
		log.GetLogger().Errorf("unmarshal apig meta info with key: %s, error: %s", ev.Key, err.Error())
		return RouteInfo{}, err
	}
	log.GetLogger().Infof("apig ips key %s, keyType %s, value %v in etcd", ev.Key, event.EventType, apigIP)
	return apigIP, nil
}

// HandleEvent handle event
func (handler *RouteHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.APIGEventKind {
		return errors.New("failed to match")
	}
	apigIP, err := handler.parseAPIGIPMetaInfo(event)
	if err != nil {
		log.GetLogger().Errorf("failed to parse apig meta info, err:%s", err.Error())
		return errors.New("invalid apig event value")
	}
	log.GetLogger().Infof("apig ips keyType %s, value %v in etcd", event.EventType, apigIP)
	switch event.EventType {
	case evs.Added, evs.Updated:
		cache.WorkerCache.UpdateAPIGIP(apigIP.RouteIP)
	case evs.Deleted:
		cache.WorkerCache.DeleteAPIGIP()
	default:
		log.GetLogger().Errorf("unsupported event type %s", event.EventType)
		return errors.New("unsupported event type")
	}
	return nil
}
