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

// Package hcslicensehandler -
package hcslicensehandler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

const (
	baseTen = 10
	// the default is 2 minutes, and the time for ManageOne to call the FG management plane is 5 minutes.
	defaultInterval = 2 * time.Minute
)

var once sync.Once

// LicenseHandler define license handler in the hcs scenario
type LicenseHandler struct {
	EtcdClient *etcd3.EtcdClient
}

// NewHandler used to create new HCSLicenseHandler
func NewHandler(client *etcd3.EtcdClient) *LicenseHandler {
	return &LicenseHandler{
		EtcdClient: client,
	}
}

// HandleEvent used to handle hcs license event
func (handler *LicenseHandler) HandleEvent(event eventsource.Event) error {
	log.GetLogger().Infof("start to process hcs license event, resource key: %s", event.KeyRef)
	// check event type
	if event.Kind != utils.HCSLicenseEventKind {
		return errors.New("event type does not match " + utils.HCSLicenseEventKind + " invalid event")
	}
	licenseMetaData := config.HCSLicenseMetaData{}

	obj, ok := event.Object.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("the value of hcs license is invalid")
		return errors.New("invalid event object")
	}
	decryptData, err := localauth.Decrypt(string(obj.Value))
	if err != nil {
		log.GetLogger().Errorf("decrypt hcs license meta data failed, key: %s and error: %s",
			obj.Key, err.Error())
		return err
	}
	err = json.Unmarshal(decryptData, &licenseMetaData)
	if err != nil {
		log.GetLogger().Errorf("unmarshal hcs license meta data failed, key: %s and error: %s",
			obj.Key, err.Error())
		return err
	}
	return handleLicenseEvent(event, licenseMetaData)
}

func handleLicenseEvent(event eventsource.Event, licenseMetaData config.HCSLicenseMetaData) error {
	switch event.EventType {
	case eventsource.Added:
		if !config.EnableHCSLicense {
			config.EnableHCSLicense = true
		}
		config.CPUQuota = licenseMetaData.CPU
		log.GetLogger().Infof("added event, current hcs license's cpu quota: %v", config.CPUQuota)
		once.Do(func() {
			go reportCPUUsageRegularly(signals.WaitForSignal())
		})
	case eventsource.Updated:
		if !config.EnableHCSLicense {
			config.EnableHCSLicense = true
		}
		config.CPUQuota = licenseMetaData.CPU
		log.GetLogger().Infof("updated event, current hcs license's cpu quota: %v", config.CPUQuota)
	case eventsource.Deleted:
		turnOffHCSLicense()
	default:
	}
	log.GetLogger().Infof("successfully processed hcs license event event, resource key: %s", event.KeyRef)
	return nil
}

func reportCPUUsageRegularly(stopCh <-chan struct{}) {
	ticker := time.NewTicker(defaultInterval)
	for {
		select {
		case <-stopCh:
			log.GetLogger().Infof("graceful exit")
			ticker.Stop()
			return
		case <-ticker.C:
			err := reportClusterLicenseUsed()
			if err != nil {
				log.GetLogger().Warnf("This cpu usage data reporting failed: %s", err)
			}
		}
	}
}

func reportClusterLicenseUsed() error {
	if !config.EnableHCSLicense {
		return nil
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), baseTen)
	licenseUsedData := config.HCSLicenseMetaData{
		CPU:       config.CurrentCPUUsage,
		TimeStamp: timestamp,
	}
	data, err := json.Marshal(licenseUsedData)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal used hcs license data")
		return err
	}
	clusterID := os.Getenv("CLUSTER_ID")
	usedLicenseKey := fmt.Sprintf("%s/%s", utils.HCSLicenseUsedPrefix, clusterID)
	value, err := localauth.Encrypt(string(data))
	if err != nil {
		log.GetLogger().Errorf("failed to encrypt used hcs license data")
		return err
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	return etcd3.GetMetaEtcdClient().Put(defaultEtcdCtx, usedLicenseKey, value)
}

func turnOffHCSLicense() {
	config.EnableHCSLicense = false
	config.CPUQuota = 0
	config.CPUMu.Lock()
	defer config.CPUMu.Unlock()
	config.CurrentCPUUsage = 0
}
