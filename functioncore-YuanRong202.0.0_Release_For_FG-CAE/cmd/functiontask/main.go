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

// Package main start the service.
package main

import (
	"fmt"
	"net/http"

	_ "go.uber.org/automaxprocs"

	"functioncore/pkg/common/autogc"
	"functioncore/pkg/common/logger/healthlog"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/core-sdk/communication"
	"functioncore/pkg/core-sdk/config"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functionbus/proxy/workerevaluator"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/util"
	"functioncore/pkg/functiontask/taskimpl/coreadaptor"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
	"functioncore/pkg/functiontask/taskimpl/router"
)

const componentName = "function-task"

func initCoreSDK(taskConfig *coreconfig.Config, coreAdaptor *coreadaptor.CoreAdaptor, stopCh <-chan struct{}) error {
	var err error
	// get function instance id and ip.
	funcInstanceID := coreconfig.NodeID
	ip := coreconfig.PodIP
	log.GetLogger().Infof("POD_IP, value: %s", ip)

	// register function information.
	functionCoreConf := functioncore.RegisterInfo{
		FunctionName:       coreconfig.NameOfFunctionTask,
		FunctionVersion:    "$latest",
		FunctionInstanceID: funcInstanceID,
		IP:                 ip,
		Tenant:             coreconfig.TenantID,
	}
	if err = functioncore.RegFuncInfo(functionCoreConf); err != nil {
		log.GetLogger().Errorf("regFuncInfo, funcName: %s, funcVersion: %s, funcInstanceID: %s, ip: %s, tenant: %s",
			functionCoreConf.FunctionName, functionCoreConf.FunctionVersion, functionCoreConf.FunctionInstanceID,
			functionCoreConf.IP, urnutils.Anonymize(functionCoreConf.Tenant))
		return err
	}

	if err = functioncore.Init(stopCh); err != nil {
		log.GetLogger().Errorf("init CoreSDK err, err: %s", err)
		return err
	}

	if taskConfig.InitStateClient {
		// init state client.
		stateClientConfig := functioncore.StateClientConfig{
			ServerAddr:             taskConfig.StateServerAddr,
			ServerMode:             taskConfig.StateServerMode,
			Password:               coreconfig.GetRedisPassword(taskConfig),
			EnableTLS:              taskConfig.StateServerEnableTLS,
			AKey:                   config.FConfigs.LocalAuth.AKey,
			SKey:                   config.FConfigs.LocalAuth.SKey,
			ExpiredCb:              coreAdaptor.ExpiredCb,
			GetRealTimeStateServer: coreconfig.GetHotloadStateServerConf,
		}
		if err = functioncore.InitStateClient(stateClientConfig, stopCh); err != nil {
			log.GetLogger().Errorf("init StateClient err, err: %s", err)
			return err
		}
	}

	// register message handler.
	if err = functioncore.RegMsgHandler(coreAdaptor.MsgHandler); err != nil {
		log.GetLogger().Errorf("register message handler err, err: %s", err)
		return err
	}
	return nil
}

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	// read configuration.
	taskConfig, err := coreconfig.Init()
	if err != nil {
		util.DelayExit("taskConfig", err)
		return
	}
	shutdown := func() { fmt.Println("common tracer is not initialized") }
	go tracer.InitCommonTracer(shutdown, "function-task")
	defer func() {
		if shutdown != nil {
			shutdown()
		}
	}()
	urnutils.SetSeparator(taskConfig.FunctionNameSeparator)
	router.Init()
	coreAdaptor := coreadaptor.Init(taskConfig)
	stopCh := signals.WaitForSignal()

	autogc.InitAutoGOGC()

	// init function core.
	err = initCoreSDK(taskConfig, coreAdaptor, stopCh)
	if err != nil {
		util.DelayExit("functionCore", err)
		return
	}

	err = communication.InitHTTPClient()
	if err != nil {
		util.DelayExit("functionCore", err)
		return
	}

	go healthlog.PrintHealthLog(stopCh, printInputLog, componentName)
	go clearStateQueue(stopCh, coreAdaptor)

	podIP := coreconfig.PodIP
	err = http.ListenAndServe(podIP+":8888", nil)
	if err != nil {
		util.DelayExit("httpListen", err)
		return
	}
}

func clearStateQueue(stopCh <-chan struct{}, adaptor *coreadaptor.CoreAdaptor) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}

	for {
		select {
		case srcMetadata := <-functioncore.StateQueueMetadataChannel:
			if (adaptor.StateQueueMap.GetQueue(&metadata.Metadata{
				TenantID:  srcMetadata.TenantID,
				ServiceID: srcMetadata.ServiceID,
				StateID:   srcMetadata.StateID,
			}) != nil) {
				stateID := metadata.Gen(srcMetadata.TenantID, srcMetadata.ServiceID, metadata.StateIDPrefix, srcMetadata.StateID)
				log.GetLogger().Infof("clear state queue, stateIDKey: %s", stateID)
				adaptor.StateQueueMap.DelState(stateID)
			}
		case <-stopCh:
			log.GetLogger().Warnf("receives a terminating signal")
			return
		}
	}
}

// printInputLog prints health log
func printInputLog() {
	poolNum := workerevaluator.GetInstance().GetWorkerNum()
	log.GetLogger().Infof("%s is alive. The number of owned workers is %d", componentName, poolNum)
	coreconfig.Logger.Infof("%s is alive", componentName)
}
