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

// Package main Init all in this package, exit if init failed
package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/healthlog"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/state-manager/config"
	"functioncore/pkg/state-manager/exceptionhandler"
	"functioncore/pkg/state-manager/healthcheck"
	"functioncore/pkg/state-manager/resourcemanager"
	"functioncore/pkg/state-manager/scheduler"
	"functioncore/pkg/state-manager/server"
	"functioncore/pkg/state-manager/state/bridgefunc"
	"functioncore/pkg/state-manager/state/counter"
	"functioncore/pkg/state-manager/state/instancemanager"
	"functioncore/pkg/state-manager/utils"
)

const (
	logFileName               = "state-manager"
	redisReconnectionInternal = 10 * time.Second
)

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	stopChan := signals.WaitForSignal()

	// Init config
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return
	}
	// WatchConfig failed do not return, just config hot load not enable
	if err := config.WatchConfig(stopChan, updateConfig); err != nil {
		log.GetLogger().Warnf("WatchConfig failed, err %s", err.Error())
	}
	if conf.HTTPSConfig.HTTPSEnable {
		err = tls.InitTLSConfig(conf.HTTPSConfig.TLSProtocol, conf.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return
		}
	}
	if err = alarm.NewClient(conf.AlarmServerAddress, constants.StateManager, stopChan,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth(conf.LocalAuth.AKey,
			conf.LocalAuth.SKey)); err != nil {
		log.GetLogger().Errorf("failed to init alarm client, error: %s", err.Error())
		return
	}
	alarm.GetClient().RegisterAlarmType(alarm.RedisConnection)
	alarm.GetClient().RegisterAlarmType(alarm.MigrateStateIDFailed)
	// Init etcd client
	if err := utils.InitETCDClient(stopChan, conf); err != nil {
		fmt.Printf("init etcd client error, msg: %s\n", err.Error())
		return
	}

	clusterstatusutil.InitClusterStatusCache()

	err = utils.InitRedisCmd(stopChan, conf)
	if err != nil {
		log.GetLogger().Errorf("failed to new redis client with error: %s", err.Error())
		alarm.GetClient().ReportAlarm(alarm.RedisConnection, fmt.Sprintf("state-manager failed to "+
			"connect redis, error msg=%s,", err.Error()))
	}
	redisConf, err := getRedisConfiguration(conf)
	if err != nil {
		log.GetLogger().Errorf("failed to get redis configuration: %s", err.Error())
		return
	}
	go redisclient.CheckRedisConnectivity(redisConf, utils.GetRedisCmd(), stopChan)
	// Init logger
	err = log.InitRunLog(logFileName, true)
	if err != nil {
		log.GetLogger().Errorf("failed to init log with error: %s", err.Error())
		return
	}

	// init scheduler
	// scheduler module sync metadata table map with etcd by InitSync method
	// so init scheduler in main method is ok
	// but init resource manager and instance manager is not recommended
	// because resource and instance manager module is not sync with etcd
	// init resource and instance manager need to do in leader node after election
	if err := scheduler.InitScheduler(conf); err != nil {
		log.GetLogger().Errorf("failed to init scheduler %s", err.Error())
		return
	}

	// register router of all module
	registerRouter()

	setStateNumLimit()
	if err := counter.NewCounter(conf, stopChan); err != nil {
		log.GetLogger().Errorf("failed to new state counter, %s", err.Error())
		return
	}
	go healthlog.PrintHealthLog(stopChan, printInputLog, logFileName)

	// start http server
	if err := server.StartHTTPServer(conf, stopChan, startWatch); err != nil {
		log.GetLogger().Errorf("failed to Start HTTP Server %s", err.Error())
		return
	}

	log.GetLogger().Infof("state manager started working...")
}

func getRedisConfiguration(conf *config.FConfig) (*redisclient.NewRedisClientParam, error) {
	pwd, err := localauth.Decrypt(conf.StateCounterCfg.Password)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt redis password, %s", err.Error())
		return nil, err
	}
	o := redisclient.NewRedisClientParam{
		ServerMode:      conf.StateCounterCfg.ServerMode,
		ServerAddr:      conf.StateCounterCfg.ServerAddr,
		Password:        string(pwd),
		EnableTLS:       conf.StateCounterCfg.EnableTLS,
		HotloadConfFunc: config.GetHotloadMetricsConf,
		Timeout:         conf.StateCounterCfg.TimeoutConf,
	}
	return &o, nil
}

func setStateNumLimit() {
	totalLimit, err := strconv.Atoi(os.Getenv("TENANT_STATE_LIMIT"))
	if err != nil {
		log.GetLogger().Errorf("failed to get env TENANT_STATE_LIMIT, %s", err.Error())
		return
	}
	config.TenantStateLimit = int64(totalLimit)
	localLimit, err := strconv.Atoi(os.Getenv("TENANT_STATE_LOCAL_LIMIT"))
	if err != nil {
		log.GetLogger().Errorf("failed to get env TENANT_STATE_LOCAL_LIMIT, %s", err.Error())
		return
	}
	config.TenantStateLocalLimit = int64(localLimit)
}

// startWatch for cluster standby callback
func startWatch(stopper <-chan struct{}) error {
	// init resourcemanager
	err := resourcemanager.Start(stopper)
	if err != nil {
		log.GetLogger().Errorf("resource manager start error: %s", err.Error())
		return err
	}

	// init proxy checker
	// process data when busproxy is offline
	if err := exceptionhandler.InitProxyChecker(stopper); err != nil {
		log.GetLogger().Errorf("exception handler init error: %s", err.Error())
		return err
	}

	// init instance manager
	if err := instancemanager.Initialize(stopper); err != nil {
		log.GetLogger().Errorf("instance manager init error: %s", err.Error())
		return err
	}

	if err := bridgefunc.Initialize(); err != nil {
		log.GetLogger().Errorf("bridge manager handler init error: %s", err.Error())
		return err
	}

	// process cluster status change
	if err := exceptionhandler.InitClusterChecker(stopper); err != nil {
		log.GetLogger().Errorf("exception handler ClusterChecker init error: %s", err.Error())
		return err
	}
	return nil
}

func registerRouter() {
	// init instancemanager router
	instancemanager.RegisterRouter()

	// init bridgefunc router
	bridgefunc.RegisterRouter()

	// init healthcheck router
	healthcheck.InitHealthCheck()
}

func printInputLog() {
	log.GetLogger().Infof("%s is alive. The number of stateIDs is %d", logFileName,
		instancemanager.GetStateCacheMapNum())
	return
}

func updateConfig() {
	log.GetLogger().Infof("state-manager updateConfig success")
}
