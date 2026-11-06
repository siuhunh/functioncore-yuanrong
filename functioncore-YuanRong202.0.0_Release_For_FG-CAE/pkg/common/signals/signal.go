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
package signals

import (
	"os"
	"os/signal"
	"syscall"

	"functioncore/pkg/common/logger/log"
)

var (
	shutdownSignals      = []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGKILL}
	onlyOneSignalHandler = make(chan struct{})
	shutdownHandler      chan os.Signal
	stopCh               = make(chan struct{})
)

const channelCount = 2

func init() {
	// 2 is the length of shutdown Handler channel
	shutdownHandler = make(chan os.Signal, channelCount)

	signal.Notify(shutdownHandler, shutdownSignals...)

	go func() {
		<-shutdownHandler
		close(stopCh)
		<-shutdownHandler
		log.GetLogger().Sync()
		os.Exit(1)
	}()
}

// WaitForSignal defines signal handler process.
func WaitForSignal() <-chan struct{} {
	return stopCh
}
