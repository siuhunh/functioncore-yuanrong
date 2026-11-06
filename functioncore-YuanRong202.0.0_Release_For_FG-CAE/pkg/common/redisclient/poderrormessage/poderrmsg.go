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

// Package poderrormessage pod error message
package poderrormessage

import (
	"context"
	"encoding/json"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
)

const (
	defaultLen          = 10
	redisContextTimeout = 100 * time.Millisecond
)

// PodErrorInfo -
type PodErrorInfo struct {
	PodName         string `json:"podName"`
	ErrorMessage    string `json:"errorMessage"`
	EnvName         string `json:"envName"`
	EnvID           string `json:"envId"`
	ComponentName   string `json:"componentName"`
	ComponentID     string `json:"componentId"`
	ApplicationName string `json:"applicationName"`
	ApplicationID   string `json:"applicationId"`
	UploadTime      int64  `json:"uploadTime"`
}

// ReportPodErrMsgToRedis -
func ReportPodErrMsgToRedis(c *redisclient.Client, URNWithVersion string, minInstanceNum int64,
	podErrInfo PodErrorInfo) {
	log.GetLogger().Debugf("report pod errMsg To Redis, URNWithVersion %s, podName %s, errMsg %s,"+
		" minInstanceNum %d", URNWithVersion, podErrInfo.PodName, podErrInfo.ErrorMessage, minInstanceNum)
	if c == nil {
		log.GetLogger().Errorf("redis is not ready when save error message to redis")
		return
	}
	data, err := json.Marshal(podErrInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to json marshal PodErrorInfo, %s", err.Error())
		return
	}
	// In normal cases, the Redis lock is required. Currently, the Redis lock is not added. Considering that the pop
	// header element is displayed in the case of multiple concurrent requests, this problem does not affect the system.
	ctx := context.Background()
	lenCtx, cancel := context.WithTimeout(ctx, redisContextTimeout)
	curlength, err := c.LLen(lenCtx, URNWithVersion).Result()
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to LLen, urn: %s, podName: %s, err: %s", URNWithVersion,
			podErrInfo.PodName, err.Error())
		return
	}
	pushCtx, cancel := context.WithTimeout(ctx, redisContextTimeout)
	err = c.RPush(pushCtx, URNWithVersion, data).Err()
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to RPush, urn: %s, podName: %s, err: %s",
			URNWithVersion, podErrInfo.PodName, err.Error())
		return
	}
	length := minInstanceNum + defaultLen
	if curlength+1 > length {
		popNum := int(curlength + 1 - length)
		log.GetLogger().Debugf("urn %s, popNum %d, minInstanceNum %d", URNWithVersion, popNum, minInstanceNum)
		for ; popNum > 0; popNum-- {
			popCtx, cancel := context.WithTimeout(ctx, redisContextTimeout)
			err = c.LPop(popCtx, URNWithVersion).Err()
			cancel()
			if err != nil {
				log.GetLogger().Errorf("failed to LPopCount, urn: %s, podName: %s, err: %s",
					URNWithVersion, podErrInfo.PodName, err.Error())
				continue
			}
		}
	}
}
