/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package action

import (
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functionbus/utils"
)

const retryScaleDownInterval = time.Minute

var (
	once      sync.Once
	instances sync.Map
)

func addToRetryList(instanceID, functionKey string, isBrokenConnection bool) {
	once.Do(func() {
		instances = sync.Map{}
		go periodRetryScaleDown(isBrokenConnection)
	})
	instances.Store(instanceID, functionKey)
}

func periodRetryScaleDown(isBrokenConnection bool) {
	timeTicker := time.NewTicker(retryScaleDownInterval)
	for {
		select {
		case <-timeTicker.C:
			// NOTE: Range method returns snapshot of sync map
			instances.Range(func(k, v interface{}) bool {
				instanceID, instOk := k.(string)
				functionKey, funcOk := v.(string)
				if !instOk || !funcOk {
					return false
				}
				if ok, _, _ := ScaleDown(instanceID, functionKey, isBrokenConnection); ok {
					instances.Delete(instanceID)
				}
				return true
			})
		case <-utils.GetGlobalStopCh():
			log.GetLogger().Warn("stop channel is closed")
			timeTicker.Stop()
			return
		}
	}
}
