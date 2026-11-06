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

// Package main is entry of runtime manager process
package main

import (
	"fmt"
	"os"
	"runtime"

	_ "go.uber.org/automaxprocs"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/healthlog"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/runtime-manager/process"
	"functioncore/pkg/runtime-manager/server"
	"functioncore/pkg/runtime-manager/utils"
)

const (
	defaultLogFileName = "runtime-manager"
	functionCodePerm   = 0750
)

func init() {
	runtime.LockOSThread()
}

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	runningMode := os.Getenv("RUNNING_MODE")
	log.GetLogger().Infof("RUNTIME MANAGER runningMode: %s", runningMode)

	if err := fixMount(os.FileMode(functionCodePerm)); err != nil {
		fmt.Printf("failed to fix mount: %s\n", err.Error())
		return
	}
	if err := log.InitRunLog(defaultLogFileName, false); err != nil {
		fmt.Printf("failed to init logger: %s\n", err.Error())
		return
	}
	if err := utils.PrintContainerID(); err != nil {
		log.GetLogger().Warnf("failed to print contain id: %s", err.Error())
	}
	if err := utils.DisableOOM(); err != nil {
		log.GetLogger().Errorf("failed to disable oom: %s", err.Error())
		return
	}
	rmConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err)
		return
	}
	stopCh := signals.WaitForSignal()
	process.CheckSnuserDirSize(rmConfig.SnuserDirSizeLimit, stopCh)
	address := utils.GetServerAddress(rmConfig.ServerPort)
	if err = server.StartListening(address); err != nil {
		log.GetLogger().Errorf("failed to listen on %s, err: %s", address, err)
		return
	}
	if runningMode != constants.RunningModeCheckpoint {
		process.PreStartRuntimeProcesses(rmConfig)
	}
	go healthlog.PrintHealthLog(stopCh, process.PrintInputLog, defaultLogFileName)
	process.GracefulShutdown(stopCh)
}

func fixMount(functionCodePerm os.FileMode) error {
	if err := os.Chown("/home/sn/log", constants.DefaultWorkerUID, constants.DefaultWorkerGID); err != nil {
		return fmt.Errorf("chown /home/sn/log: %s", err.Error())
	}
	if err := os.Chown("/home/snuser/log", constants.DefaultRuntimeUID, constants.DefaultRuntimeGID); err != nil {
		return fmt.Errorf("chown /home/snuser/log: %s", err.Error())
	}
	if err := os.Chown("/opt/function/code", constants.DefaultWorkerUID, constants.DefaultWorkerGID); err != nil {
		return fmt.Errorf("chown /opt/function/code: %s", err.Error())
	}
	if err := os.Chmod("/opt/function/code", functionCodePerm); err != nil {
		return fmt.Errorf("chmod /opt/function/code: %s", err.Error())
	}
	if err := os.MkdirAll("/opt/function/code/lib", functionCodePerm); err != nil {
		return fmt.Errorf("failed to mkdir /opt/function/code/lib, error: %s", err.Error())
	}
	if err := os.Chown("/opt/function/code/lib", constants.DefaultWorkerUID, constants.DefaultWorkerGID); err != nil {
		return fmt.Errorf("failed to chown /opt/function/code/lib, error: %s", err.Error())
	}
	if err := os.Chown("/home/sn/metrics", constants.DefaultWorkerUID, constants.DefaultWorkerGID); err != nil {
		return fmt.Errorf("chown /home/sn/metrics: %s", err.Error())
	}
	if err := os.Chmod("/home/sn/metrics", functionCodePerm); err != nil {
		return fmt.Errorf("chmod /home/sn/metrics: %s", err.Error())
	}
	return nil
}
