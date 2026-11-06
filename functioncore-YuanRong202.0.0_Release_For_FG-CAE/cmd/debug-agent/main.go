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

// Package main is entry of debug-agent process
package main

import (
	"fmt"
	"os"

	"functioncore/pkg/common/autogc"
	"functioncore/pkg/common/logger/healthlog"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/debug-agent/config"
	"functioncore/pkg/debug-agent/server"
	"functioncore/pkg/debug-agent/websocket"
)

const (
	logFileName = "debug-agent"
)

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	if err := log.InitRunLog(logFileName, true); err != nil {
		fmt.Printf("failed to init log, err: %s", err.Error())
		return
	}

	autogc.InitAutoGOGC()

	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to load config, err: %s", err.Error())
		return
	}

	stopCh := signals.WaitForSignal()

	go healthlog.PrintHealthLog(stopCh, printInputLog, logFileName)

	if err = websocket.CreateConnPair(websocket.BuildStreamOption(conf)); err != nil {
		log.GetLogger().Errorf("failed to create websocket server or socket client, err: %s", err.Error())
		return
	}

	if err := server.CreateHTTPServer(conf); err != nil {
		log.GetLogger().Sync()
		os.Exit(-1)
	}
	return
}

func printInputLog() {
	log.GetLogger().Infof("debug agent is alive")
}
