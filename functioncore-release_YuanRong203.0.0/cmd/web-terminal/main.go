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

// Package main is the main of web-terminal
package main

import (
	"fmt"
	"os"

	"functioncore/pkg/common/autogc"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/web-terminal/server"
)

const (
	logFileName = "web-terminal"
)

func main() {
	defer func() {
		log.GetLogger().Sync()
	}()
	err := log.InitRunLog(logFileName, true)
	if err != nil {
		fmt.Print("init logger error: " + err.Error())
		return
	}
	autogc.InitAutoGOGC()

	stopCh := signals.WaitForSignal()
	command := server.ParseOptions(stopCh)
	if command == nil {
		log.GetLogger().Errorf("failed to create apiServer command")
		return
	}

	if err := command.Execute(); err != nil {
		_, _ = fmt.Fprint(os.Stderr, "error: ", err)
		log.GetLogger().Errorf("web-terminal startup error: %s and exit now", err.Error())
		log.GetLogger().Sync()
		os.Exit(1)
	}
}
