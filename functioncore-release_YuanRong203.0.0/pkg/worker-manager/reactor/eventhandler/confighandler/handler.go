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

// Package confighandler -
package confighandler

import (
	"encoding/json"
	"errors"
	"fmt"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/config"
	evs "functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/utils"
)

// CaeConfigHandler define cae config handler
type CaeConfigHandler struct {
}

// NewEventHandler new event handler
func NewEventHandler() *CaeConfigHandler {
	return &CaeConfigHandler{}
}

// HandleEvent handle event
func (handler *CaeConfigHandler) HandleEvent(event evs.Event) error {
	if event.Kind != utils.CaeConfigEventKind {
		return errors.New("CaeConfigHandler received unmatched event, ignore it")
	}
	switch event.EventType {
	case evs.Added:
		ev, ok := event.Object.(*etcd3.Event)
		if !ok {
			return errors.New("CaeConfigHandler assert etcd3.Event failed")
		}
		caeConfig := &config.CaeConfig{}
		err := json.Unmarshal(ev.Value, caeConfig)
		if err != nil {
			return fmt.Errorf("CaeConfigHandler unmarshal etcd3.Event failed:%s", err.Error())
		}
		updateCaePoolImage(caeConfig)
	case evs.Deleted:
		log.GetLogger().Warnf("CaeConfigHandler received config delete event, ignore it")
	default:
		log.GetLogger().Warnf("CaeConfigHandler received unsupported event, ignore it")
	}
	return nil
}

func updateCaePoolImage(caeConfig *config.CaeConfig) {
	poolManager := poolmanager.GetPoolManager()
	poolList := poolManager.GetPoolList()
	for _, pool := range poolList {
		if pool == nil {
			continue
		}
		if pool.IsCaePool() {
			if err := utils.UpdateRuntimeImageByK8s(pool.Deployment.Name, caeConfig.CaeBaseImage); err != nil {
				log.GetLogger().Warnf("CaeConfigHandler update deployment failed:%s", err.Error())
				continue
			}
			containers := pool.Deployment.Spec.Template.Spec.Containers
			for idx := range containers {
				if containers[idx].Name == utils.RuntimeContainerName {
					containers[idx].Image = caeConfig.CaeBaseImage
				}
			}
		}
	}
}
