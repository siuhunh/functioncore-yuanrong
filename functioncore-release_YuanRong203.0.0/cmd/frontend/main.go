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

// Package main is entry of frontend process
package main

import (
	"fmt"

	_ "go.uber.org/automaxprocs"

	"functioncore/pkg/common/autogc"
	"functioncore/pkg/common/logger/healthlog"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/frontend"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/loadbalancer/common"
	"functioncore/pkg/frontend-invoke/loadbalancer/etcdadapter"
	"functioncore/pkg/frontend-invoke/loadbalancer/fasthttpclient"
	"functioncore/pkg/frontend-invoke/loadbalancer/remotescheduler"
	"functioncore/pkg/frontend-invoke/metrics"
)

const (
	logFileName = "frontend"
)

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	// init logger config
	err := log.InitRunLog(logFileName, true)
	if err != nil {
		fmt.Print("init logger error: " + err.Error())
		return
	}
	shutdown := func() { fmt.Println("common tracer is not initialized") }
	go tracer.InitCommonTracer(shutdown, "frontend")
	defer func() {
		if shutdown != nil {
			shutdown()
		}
	}()
	// init constant
	err = constant.InitConstant()
	if err != nil {
		log.GetLogger().Errorf("init constant error: %s", err.Error())
		return
	}
	updateConfig()

	autogc.InitAutoGOGC()
	if err := monitor.InitMemMonitor(); err != nil {
		log.GetLogger().Errorf("failed to init mem monitor")
		return
	}

	urnutils.SetSeparator(constant.FConfig.FunctionNameSeparator)

	if constant.FConfig.HTTPSConfig.HTTPSEnable {
		err = tls.InitTLSConfig(constant.FConfig.HTTPSConfig.TLSProtocol, constant.FConfig.HTTPSConfig.TLSCiphers)
		if err != nil {
			log.GetLogger().Errorf("failed to init the HTTPS config: %s", err.Error())
			return
		}
	}

	stopCh := signals.WaitForSignal()
	go healthlog.PrintHealthLog(stopCh, printInputLog, logFileName)
	go metrics.StartReportMetrics(stopCh)

	// WatchConfig failed do not return, just config hot load not enable
	if err := constant.WatchConfig(constant.ConfigFilePath, stopCh, updateConfig); err != nil {
		log.GetLogger().Warnf("WatchConfig %s failed, err %s", constant.ConfigFilePath, err.Error())
	}

	err = initLoadBalancer()
	if err != nil {
		log.GetLogger().Errorf("failed to initLoadBalancer, err %s", err.Error())
		return
	}

	err = frontend.StartHTTPServer(stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to start http server: %s", err.Error())
	}

}

func printInputLog() {
	nodeNum := functionlb.NodeRoute.GetNodeNum()
	log.GetLogger().Infof("%s is alive. The number of nodes is %d", logFileName, nodeNum)
}

func updateConfig() {
	monitor.SetMemoryControlConfig(constant.FConfig.MemoryControlConfig)
	frontend.UpdateLeaseConfig(constant.FConfig.EtcdLeaseConfig.LeaseTTL,
		constant.FConfig.EtcdLeaseConfig.RenewTTL)
	functionlb.ResetHeartbeatTasks()
	remotescheduler.GetWhitelist().UpdateFuncWhitelist(constant.FConfig.MultiConcurrencySchedule.FuncWhiteList)
	remotescheduler.GetWhitelist().UpdateTenantWhitelist(constant.FConfig.MultiConcurrencySchedule.TenantWhiteList)
}

func initLoadBalancer() error {
	err := common.CreatePoolInstance()
	if err != nil {
		return err
	}
	remotescheduler.CreateInstance()
	etcdadapter.SetSchedulerInsHandler(remotescheduler.GetInstance())

	common.SetRPCClientCreator(fasthttpclient.CreateClient)

	return nil
}
