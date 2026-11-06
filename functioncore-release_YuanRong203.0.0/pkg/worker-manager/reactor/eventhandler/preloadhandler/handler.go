/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package preloadhandler -
package preloadhandler

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/preload"
	"functioncore/pkg/worker-manager/utils"
)

const (
	// /cold-start-preload/business/yrk/tenant/${tenantID}/function/0@default@${funcName}/version/latest
	preloadFieldsSize = 10
	tenantIDIndex     = 5
	typeIndex         = 6
	nameIndex         = 7
	versionIndex      = 9
)

// Handler define preload handler
type Handler struct {
	EtcdClient *etcd3.EtcdClient
}

type preloadKeyParseInfo struct {
	projectID string
	typeStr   string
	name      string
	version   string
}

// NewEventHandler new event handler
func NewEventHandler(etcdctl *etcd3.EtcdClient) *Handler {
	return &Handler{
		EtcdClient: etcdctl,
	}
}

func (handler *Handler) parsePreloadInfo(event evs.Event) (config.PreloadInfo, error) {
	preloadInfo := config.PreloadInfo{}
	ev, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("cold start preload value type from etcd is invalid, key: %s, type: %T",
			etcdkey.AnonymizeTenantCommonEtcdKey(event.KeyRef), event.Object)
		return preloadInfo, errors.New("invalid parameter")
	}
	if err := json.Unmarshal(ev.Value, &preloadInfo); err != nil {
		log.GetLogger().Errorf("failed to unmarshal preload info with key: %s, error: %s", ev.Key, err.Error())
		return preloadInfo, err
	}
	log.GetLogger().Infof("cold start preload key %s, keyType %s, value %v in etcd", ev.Key, event.EventType,
		preloadInfo)
	return preloadInfo, nil
}

func parserPreLoadKey(preloadKey string) (*preloadKeyParseInfo, error) {
	if !strings.HasPrefix(preloadKey, utils.PreloadPrefix) {
		return nil, fmt.Errorf("invalid clod start preload key %s in etcd", preloadKey)

	}
	keyFields := strings.Split(preloadKey, "/")
	if len(keyFields) != preloadFieldsSize {
		return nil, fmt.Errorf("invalid clod start preload key %s length in etcd", preloadKey)
	}
	return &preloadKeyParseInfo{
		projectID: keyFields[tenantIDIndex],
		typeStr:   keyFields[typeIndex],
		name:      keyFields[nameIndex],
		version:   keyFields[versionIndex],
	}, nil
}

// HandleEvent handle event
func (handler *Handler) HandleEvent(event evs.Event) error {
	log.GetLogger().Infof("process %s event, type %s, key %s from %s etcd, since receive: %v",
		event.Kind, event.EventType, event.KeyRef, event.SourceType, time.Now().Sub(event.StartTime))
	if event.Kind != utils.PreloadEventKind {
		return errors.New("failed to match")
	}
	if !preload.IsEnable() {
		log.GetLogger().Warnf("preload is not enable")
		return nil
	}
	preloadKeyParse, err := parserPreLoadKey(event.KeyRef)
	if err != nil {
		log.GetLogger().Errorf("failed to get preloadKeyParse by key, err:%s", err.Error())
		return errors.New("invalid preload key")
	}
	switch event.EventType {
	case evs.Added, evs.Updated:
		preloadConf, err := handler.parsePreloadInfo(event)
		if err != nil {
			log.GetLogger().Errorf("failed to parse cold start preload meta info, err:%s", err.Error())
			return errors.New("invalid clod start preload event value")
		}
		return addOrUpdatePreLoad(preloadKeyParse.projectID, preloadKeyParse.typeStr,
			preloadKeyParse.name, preloadKeyParse.version, preloadConf)
	case evs.Deleted:
		return deletePreload(preloadKeyParse.projectID, preloadKeyParse.typeStr,
			preloadKeyParse.name, preloadKeyParse.version)
	default:
		log.GetLogger().Errorf("unsupported event type %s", event.EventType)
		return errors.New("unsupported event type")
	}
}

func deletePreload(tenantID, typeStr, name, version string) error {
	switch typeStr {
	case constants.PreloadFunctionType:
		service, funcName, err := urnutils.GetFunctionNameAndServiceName(name)
		if err != nil {
			return err
		}
		functionVersionName := commonUtils.GetPreloadFunctionVersionName(service, funcName, version)
		if err := cache.GetPreloadCache().RemoveFunction(tenantID, functionVersionName); err != nil {
			log.GetLogger().Errorf("failed to remove function %s preload info to cache, err %s", functionVersionName,
				err.Error())
			return err
		}
		preload.GetClient().StartDelPreloadOfALLNodes(tenantID, typeStr, functionVersionName)
		return nil
	case constants.PreloadLayerType:
		layerVersionName := commonUtils.GetPreloadLayerVersionName(name, version)
		if err := cache.GetPreloadCache().RemoveLayer(tenantID, layerVersionName); err != nil {
			log.GetLogger().Errorf("failed to remove function %s preload info to cache, err %s", layerVersionName,
				err.Error())
			return err
		}
		preload.GetClient().StartDelPreloadOfALLNodes(tenantID, typeStr, layerVersionName)
		return nil
	default:
		return fmt.Errorf("not support preload type %s", typeStr)
	}
}

// addOrUpdatePreLoad -
func addOrUpdatePreLoad(tenantID, typeStr, name, version string, conf config.PreloadInfo) error {
	switch typeStr {
	case constants.PreloadFunctionType:
		service, funcName, err := urnutils.GetFunctionNameAndServiceName(name)
		if err != nil {
			return err
		}
		functionVersionName := commonUtils.GetPreloadFunctionVersionName(service, funcName, version)
		if err = cache.GetPreloadCache().AddFunction(tenantID, functionVersionName, conf); err != nil {
			log.GetLogger().Errorf("failed to add function %s preload info to cache, err %s", functionVersionName,
				err.Error())
			return err
		}
		conf.VersionUrn = functionVersionName
		preload.GetClient().StartAddPreloadToAllNodes(tenantID, typeStr, conf)
		return nil
	case constants.PreloadLayerType:
		layerVersionName := commonUtils.GetPreloadLayerVersionName(name, version)
		if err := cache.GetPreloadCache().AddLayer(tenantID, layerVersionName, conf); err != nil {
			log.GetLogger().Errorf("failed to add layer %s preload info to cache, err %s", layerVersionName,
				err.Error())
			return err
		}
		conf.VersionUrn = layerVersionName
		preload.GetClient().StartAddPreloadToAllNodes(tenantID, typeStr, conf)
		return nil
	default:
		return fmt.Errorf("not support preload type %s", typeStr)
	}
}
