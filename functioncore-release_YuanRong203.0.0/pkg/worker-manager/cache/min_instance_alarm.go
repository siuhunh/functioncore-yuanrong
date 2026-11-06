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

package cache

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	commonUtils "functioncore/pkg/common/utils"
	tools "functioncore/pkg/worker-manager/utils"
)

var (
	interval                   time.Duration
	defaultMinInsCheckInterval = 15 * time.Minute
)

// CheckMinInsAndReport -
func CheckMinInsAndReport(stopCh <-chan struct{}) {
	alarm.GetClient().RegisterAlarmType(alarm.InsufficientMinInstance)
	alarm.GetClient().RegisterAlarmType(alarm.UserInsufficientMinInstance)
	go checkMinInsRegularly(stopCh)
}

func checkMinInsRegularly(stopCh <-chan struct{}) {
	intervalStr := os.Getenv("MININS_CHECK_INTERVAL")
	if intervalStr == "" {
		log.GetLogger().Warnf("set default interval of min instance check")
		interval = defaultMinInsCheckInterval
	} else {
		num, err := strconv.Atoi(intervalStr)
		if err != nil {
			log.GetLogger().Warnf("set default interval of min instance check")
			interval = defaultMinInsCheckInterval
		} else if num <= 0 {
			log.GetLogger().Warnf("stop min instance check")
			return
		} else {
			interval = time.Duration(num) * time.Minute
		}
	}

	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	clusterID := os.Getenv("CLUSTER_ID")
	if clusterID == "" {
		log.GetLogger().Warnf("failed to get cluster ID")
	}

	log.GetLogger().Infof("start to check min instance with interval %v", interval)

	time.AfterFunc(defaultMinInsCheckInterval, func() {
		ticker := time.NewTicker(interval)
		for {
			select {
			case <-stopCh:
				log.GetLogger().Infof("graceful exit")
				ticker.Stop()
				return
			case <-ticker.C:
				judgeAndReport(clusterID)
			}
		}
	})
}

func judgeAndReport(clusterID string) {
	insufficientNumBySystem, insufficientNumByUser := WorkerCache.checkMinScale(clusterID)
	if insufficientNumBySystem > 0 {
		errInfo := fmt.Sprintf("insufficient number of reserved instances %d", insufficientNumBySystem)
		alarm.GetClient().ReportAlarm(alarm.InsufficientMinInstance,
			fmt.Sprintf("error msg=%s,", errInfo))
		log.GetLogger().Errorf(errInfo + ", cluster" + clusterID)
	} else {
		alarm.GetClient().ClearAlarm(alarm.InsufficientMinInstance)
	}
	if insufficientNumByUser > 0 {
		errInfo := fmt.Sprintf("insufficient number of reserved instances %d", insufficientNumByUser)
		alarm.GetClient().ReportAlarm(alarm.UserInsufficientMinInstance,
			fmt.Sprintf("error msg=%s,", errInfo))
	} else {
		alarm.GetClient().ClearAlarm(alarm.UserInsufficientMinInstance)
	}
}

func (w *workerCache) checkMinScale(clusterID string) (int64, int64) {
	insufficientNumBySystem := int64(0)
	insufficientNumByUser := int64(0)
	w.functions.Range(func(funcSvcID, fci interface{}) bool {
		fc, ok := fci.(*functionCache)
		if !ok {
			log.GetLogger().Warnf("failed to covert interface to function cache for func: %s", funcSvcID)
			return true
		}

		fc.RLock()
		defer fc.RUnlock()
		if fc.minScale == 0 {
			return true
		}
		if fc.funcMeta == nil {
			log.GetLogger().Warnf("function %s with minScale configured but no funcMeta, cluster %s",
				funcSvcID, clusterID)
			return true
		}
		if tools.IsPullTriggerFunction(fc.funcMeta) {
			return true
		}
		if commonUtils.IsCAEFunc(fc.funcMeta.Spec.Configuration.BusinessType) {
			return true
		}

		if time.Now().Sub(fc.minScaleUpdatedTime) > interval {
			actualNum := fc.countReversedInstance()
			if actualNum < fc.minScale {
				if fc.isUserErrorAlarmSign {
					insufficientNumByUser += fc.minScale - actualNum
				} else {
					insufficientNumBySystem += fc.minScale - actualNum
				}
				log.GetLogger().Errorf("function %s, actual reserved instance num %d, configured minScale"+
					" num %d, cluster %s", funcSvcID, actualNum, fc.minScale, clusterID)
				fc.minScaleAlarmSign = true
			} else if fc.minScaleAlarmSign == true {
				log.GetLogger().Warnf("function %s, the number of reserved instances of reaches the set"+
					" value %d, cluster %s", funcSvcID, fc.minScale, clusterID)
				fc.minScaleAlarmSign = false
			}
		}
		return true
	})
	return insufficientNumBySystem, insufficientNumByUser
}
