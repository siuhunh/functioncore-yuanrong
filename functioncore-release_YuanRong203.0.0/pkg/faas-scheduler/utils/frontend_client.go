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

package utils

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

const (
	frontendServiceName = "frontend"
	frontendServicePort = "8888"

	url = "/serverless/v1/functions/instances/recover"

	requestTimeOut = 5 * time.Second
)

var (
	instancesRes = schedulerapi.RecoverInstancesRes{InstancesRes: make(map[string]*schedulerapi.CheckInstanceReq)}
)

// getFrontendBaseURL -
func getFrontendRequestURL() string {
	scheme := tls.GetURLScheme(config.FConfig.HTTPSConfig.HTTPSEnable)
	return scheme + "://" + frontendServiceName + ":" + frontendServicePort + url
}

// GetRecoverInstances get recover instances
func GetRecoverInstances() schedulerapi.RecoverInstancesRes {
	return instancesRes
}

// ResetRecoverInstances reset recover instances
func ResetRecoverInstances() {
	instancesRes = schedulerapi.RecoverInstancesRes{InstancesRes: make(map[string]*schedulerapi.CheckInstanceReq)}
}

// GetRecoverInstancesMsg get recover instances
func GetRecoverInstancesMsg() {
	ctx, cancel := context.WithTimeout(context.Background(), requestTimeOut)
	defer cancel()

	request, err := http.NewRequestWithContext(ctx, "POST", getFrontendRequestURL(), nil)
	if err != nil {
		log.GetLogger().Warnf("failed to send get instance request to frontend, err %s", err.Error())
		return
	}
	authorization, timestamp := localauth.SignLocally(
		config.FConfig.LocalAuth.AKey,
		config.FConfig.LocalAuth.SKey,
		signAppID,
		config.FConfig.LocalAuth.Duration,
	)
	request.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	request.Header.Set(constants.Authorization, authorization)
	request.Header.Set(constants.HeaderEventSourceID, signAppID)
	resp, err := GetWorkerManagerClient().Do(request)
	if err != nil {
		log.GetLogger().Warnf("failed to send get instance request to frontend, err %s", err.Error())
		return
	}
	handleInstanceMsg(resp)
}

func handleInstanceMsg(resp *http.Response) {
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Warnf("failed to get instance msg from frontend, statusCode %d", resp.StatusCode)
		return
	}
	respBody, err := io.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Warnf("failed to read instance msg from frontend, err %s", err.Error())
		return
	}
	if err = json.Unmarshal(respBody, &instancesRes); err != nil {
		log.GetLogger().Errorf("failed to unmarshal instance msg response: %s", err.Error())
		return
	}
	log.GetLogger().Infof("succeed to get recover instance msg: %s", string(respBody))
}
