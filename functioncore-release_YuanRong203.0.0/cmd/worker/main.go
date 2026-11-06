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

// Package main is entry of worker process
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"

	_ "go.uber.org/automaxprocs"

	"functioncore/pkg/common/autogc"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/healthlog"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/urnutils"
	commonUtils "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/deploy"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/network"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/fusion"
	golangV1 "functioncore/pkg/worker/runtime/golang/v1"
	"functioncore/pkg/worker/runtime/rpc"
	"functioncore/pkg/worker/runtime/web"
	workerServer "functioncore/pkg/worker/server"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
)

const (
	defaultLogFileName = "worker"
	workerInitFilePath = "/home/sn/worker-init/has_worker_init"
	fileMode           = 0640
)

func main() {
	debug.SetTraceback("single")

	defer func() {
		log.GetLogger().Sync()
	}()
	err := log.InitRunLog(defaultLogFileName, true)
	if err != nil {
		fmt.Print("init logger error: " + err.Error())
		return
	}
	shutdown := func() { fmt.Println("common tracer is not initialized") }
	go tracer.InitCommonTracer(shutdown, "worker")
	defer func() {
		if shutdown != nil {
			shutdown()
		}
	}()
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("config loading error: %s", err.Error())
		return
	}

	registerRuntime()

	urnutils.SetSeparator(conf.FunctionNameSeparator)

	conf.StartArgs = parseStartArgs()
	conf.RunningMode = os.Getenv("RUNNING_MODE")

	server, err := workerServer.NewServer(conf)
	if err != nil {
		log.GetLogger().Errorf("management server creation error: %s", err.Error())
		return
	}

	if err = runtime.Registry.Prestart(conf.RuntimeConfig.Type, &conf.RuntimeConfig); err != nil {
		log.GetLogger().Errorf("failed to preStart runtime, error: %s", err.Error())
		return
	}

	status.Set(status.Waiting)

	autogc.InitAutoGOGC()

	if err = server.Start(); err != nil {
		log.GetLogger().Errorf("failed to warm start, error: %s", err.Error())
		return
	}
	utils.GetResourceLimitsMemory()

	createWorkerInitFile(workerInitFilePath)

	stopCh := signals.WaitForSignal()
	go healthlog.PrintHealthLog(stopCh, printInputLog, defaultLogFileName)
	gracefulShutdown(server, stopCh, conf)
}

func gracefulShutdown(server *workerServer.Server, stopCh <-chan struct{}, conf *config.Configuration) {
	terminationChan := server.GetTerminationChan()
	if terminationChan == nil {
		log.GetLogger().Errorf("termination channel is nil")
		return
	}
	select {
	case _, ok := <-terminationChan:
		if !ok {
			log.GetLogger().Warnf("received termination signal from worker-manager, shutdown")
		}
	case _, ok := <-stopCh:
		if !ok {
			log.GetLogger().Warnf("received termination signal, shutdown")
		}
	case _, ok := <-runtime.ProcessExitSignal:
		if !ok {
			log.GetLogger().Infof("the runtime processed the exit signal and started to graceful exit")
		}
	}
	server.GracefulShutdown()

	if runtime.RuntimeManagerClient != nil {
		runtime.RuntimeManagerClient.SendMessageWithNoResp(tcpmsg.BuildMessage(tcpmsg.StopRuntimeManager, nil))
	}
	log.GetLogger().Info("shutdown vpc")
	network.VpcGracefulShutdown()

	if server.IsLoaded() {
		functionlog.Sync(conf)
	}
	if conf.EnableGCLog {
		log.GetLogger().Info("remove gc log")
		utils.RemoveGCLog()
	}
	// Note: After the runtime process exits, clear the mounting.
	// Otherwise, the device is busy and the mounting cannot be deleted.
	deploy.GracefulClean()
	log.GetLogger().Infof("shutdown after processing graceful exit")
	log.GetLogger().Sync()
	os.Exit(0)
}

func parseStartArgs() config.StartArgs {
	c := config.StartArgs{}

	homePath := os.Getenv("HOME")
	if err := commonUtils.ValidateFilePath(homePath); err != nil {
		log.GetLogger().Warnf("failed to valid home path, err: %s", err.Error())
		// If the home path verification fails, the default path is assigned.
		homePath = constants.DefaultHomePath
	}
	c.LogDir = filepath.Join(homePath, "log/user")
	return c
}

func registerRuntime() {
	web.RegisterRuntime()
	golangV1.RegisterRuntime()
	fusion.RegisterRuntime()
	rpc.RegisterRPCRuntime("custom-runtime-rpc")
	rpc.RegisterRPCRuntime("python2.7")
	rpc.RegisterRPCRuntime("python3.6")
	rpc.RegisterRPCRuntime("python3.9")
	rpc.RegisterRPCRuntime("python3.10")
	rpc.RegisterRPCRuntime("python3.11")
	rpc.RegisterRPCRuntime("python3.12")
	rpc.RegisterRPCRuntime("nodejs6.10")
	rpc.RegisterRPCRuntime("nodejs8.10")
	rpc.RegisterRPCRuntime("nodejs10.16")
	rpc.RegisterRPCRuntime("nodejs12.13")
	rpc.RegisterRPCRuntime("nodejs14.18")
	rpc.RegisterRPCRuntime("nodejs16.17")
	rpc.RegisterRPCRuntime("nodejs18.15")
	rpc.RegisterRPCRuntime("nodejs20.15")
	rpc.RegisterRPCRuntime("java8")
	rpc.RegisterRPCRuntime("java11")
	rpc.RegisterRPCRuntime("java17")
	rpc.RegisterRPCRuntime("java21")
	rpc.RegisterRPCRuntime("javax")
	rpc.RegisterRPCRuntime("php7.3")
	rpc.RegisterRPCRuntime("php8.3")
	rpc.RegisterRPCRuntime("c#(.net core 3.1)")
	rpc.RegisterRPCRuntime("c#(.net core 2.1)")
	rpc.RegisterRPCRuntime("cangjie1.0")
	rpc.RegisterRPCRuntime("cangjie0.52.3")
	rpc.RegisterRPCRuntime("cangjie0.60.x")
	rpc.RegisterRPCRuntime("c#(.net core 6.0)")
	rpc.RegisterRPCRuntime("c#(.net core 8.0)")
}

func printInputLog() {
	executingReq := runtime.RunMetrics.GetRequestCount()
	log.GetLogger().Infof("workerAlive | executingReq %d", executingReq)
}

func createWorkerInitFile(filePath string) {
	file, err := os.Create(filePath)
	if err != nil {
		log.GetLogger().Errorf("failed to create the has_worker_init file")
		return
	}
	defer file.Close()
	err = file.Chmod(fileMode)
	if err != nil {
		log.GetLogger().Errorf("failed to change mode of has_worker_init file, err: %s", err.Error())
		return
	}
}
