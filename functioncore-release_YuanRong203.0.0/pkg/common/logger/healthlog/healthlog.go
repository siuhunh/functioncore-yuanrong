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

// Package healthlog is for printing health logs
package healthlog

import (
	"time"

	"functioncore/pkg/common/logger/log"
)

const logInterval = 5 * time.Minute

// PrintHealthLog prints timing health logs of components
func PrintHealthLog(stopCh <-chan struct{}, inputLog func(), name string) {
	if stopCh == nil {
		log.GetLogger().Errorf("stop channel is nil")
		return
	}
	ticker := time.NewTicker(logInterval)
	defer ticker.Stop()
	time.After(logInterval)
	for {
		select {
		case <-ticker.C:
			inputLog()
		case <-stopCh:
			log.GetLogger().Warnf("%s | terminatingSignal", name)
			return
		}
	}
}
