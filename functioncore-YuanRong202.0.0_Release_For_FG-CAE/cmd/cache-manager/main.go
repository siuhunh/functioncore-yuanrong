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

// Package main is entry of cache-manager process
package main

import (
	"fmt"
	"os"

	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/cache-manager/pkg/cache"
	"functioncore/pkg/cache-manager/pkg/server"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/autogc"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/healthlog"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/tokenrotation"
)

const (
	logFileName = "cache-manager"
)

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	cacheConfig := config.GetConfig()
	if cacheConfig == nil {
		return
	}
	err := log.InitRunLog(logFileName, true)
	if err != nil {
		fmt.Print("init logger error: " + err.Error())
		return
	}
	autogc.InitAutoGOGC()
	if err := monitor.InitMemMonitor(); err != nil {
		log.GetLogger().Errorf("failed to init mem monitor, err:%s", err.Error())
		return
	}
	stopCh := signals.WaitForSignal()

	if cacheConfig.HTTPSConfig.HTTPSEnable {
		err = tls.InitTLSConfig(cacheConfig.HTTPSConfig.TLSProtocol, cacheConfig.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return
		}
	}

	// init config watcher, which failed do not return, just config hot load not enable
	if err := config.WatchConfig(stopCh, syncConfig); err != nil {
		log.GetLogger().Errorf("failed to start config watcher, %s", err)
	}

	if err = alarm.NewClient(cacheConfig.AlarmServerAddress, constants.CacheManager, stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth(cacheConfig.LocalAuth.AKey,
			cacheConfig.LocalAuth.SKey)); err != nil {
		fmt.Printf("failed to start rotated token alarm client: %s\n", err.Error())
		return
	}
	alarm.GetClient().RegisterAlarmType(alarm.OBSConnection)

	if err = tokenrotation.StartTokenRotation(stopCh); err != nil {
		fmt.Printf("failed to start token rotation: %s\n", err.Error())
		return
	}

	if err := initMetadataEtcdClient(cacheConfig, stopCh); err != nil {
		fmt.Printf("init etcd client error, msg: %s\n", err.Error())
	}
	err = cache.WatcherUserFunction(cacheConfig, stopCh)
	if err != nil {
		fmt.Print("watcher user function error: " + err.Error())
		return
	}

	go healthlog.PrintHealthLog(stopCh, server.PrintInputLog, logFileName)

	err = server.HTTPServerCreate(cacheConfig)
	if err != nil {
		fmt.Print(err, "server error: %s\n", err.Error())
		log.GetLogger().Sync()
		os.Exit(-1)
	}
	return
}

func initMetadataEtcdClient(config *config.CacheManagerConfig, stopCh <-chan struct{}) error {
	return etcd3.InitParam().
		WithStopCh(stopCh).
		WithAlarmSwitch().
		WithMetaEtcdConfig(config.EtcdConfig).
		InitClient()
}

func syncConfig() {
	newConfig := config.GetConfig()
	if newConfig == nil {
		log.GetLogger().Errorf("failed to get new config")
		return
	}
	log.GetLogger().Infof("succeed to sync cache-manager MemoryControlConfig config update %v",
		newConfig.MemoryControlConfig)
	monitor.SetMemoryControlConfig(newConfig.MemoryControlConfig)
	log.GetLogger().Infof("succeed to sync cache-manager config update")
}
