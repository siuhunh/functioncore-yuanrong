/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package wmhealth do health check of worker-manager
package wmhealth

import (
	"context"
	"errors"
	"net/http"
	"time"

	"functioncore/pkg/common/healthcheck"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/utils"
)

const (
	defaultHealthCheckTimeout   = 3
	defaultHealthCheckInterval  = 3
	defaultHealthCheckThreshold = 3
)

var (
	workerManagerHealthCtx *healthcheck.Context
	scheme                 = "http://"
)

func init() {
	common.RegisterWorkerManagerHealthCheckCallBack(hotLoadWorkerManagerHealthCheckConfig)
}

// GetWorkerManagerBaseURL return basic url of worker-manger's leader
func GetWorkerManagerBaseURL() string {
	return scheme + getWmAddr()
}

// GetContext return context with worker-manager's health
func GetContext() context.Context {
	if workerManagerHealthCtx == nil {
		return context.Background()
	}
	return workerManagerHealthCtx.GetContext()
}

func hotLoadWorkerManagerHealthCheckConfig(cfg *common.Configs) {
	config := cfg.WMHealthCheck
	if config.Timeout <= 0 {
		config.Timeout = defaultHealthCheckTimeout
	}
	if config.Interval <= 0 {
		config.Interval = defaultHealthCheckInterval
	}
	if config.Threshold <= 0 {
		config.Threshold = defaultHealthCheckThreshold
	}

	if config.Timeout != common.FConfig.WMHealthCheck.Timeout {
		log.GetLogger().Infof("update WMHealthCheck.Timeout from %d to %d",
			common.FConfig.WMHealthCheck.Timeout, config.Timeout)
		common.FConfig.WMHealthCheck.Timeout = config.Timeout
	}
	if config.Interval != common.FConfig.WMHealthCheck.Interval {
		log.GetLogger().Infof("update WMHealthCheck.Interval from %d to %d",
			common.FConfig.WMHealthCheck.Interval, config.Interval)
		common.FConfig.WMHealthCheck.Interval = config.Interval
	}
	if config.Threshold != common.FConfig.WMHealthCheck.Threshold {
		log.GetLogger().Infof("update WMHealthCheck.Threshold from %d to %d",
			common.FConfig.WMHealthCheck.Threshold, config.Threshold)
		common.FConfig.WMHealthCheck.Threshold = config.Threshold
	}
	if config.Disable != common.FConfig.WMHealthCheck.Disable {
		log.GetLogger().Infof("update WMHealthCheck.Disable from %t to %t",
			common.FConfig.WMHealthCheck.Disable, config.Disable)
		common.FConfig.WMHealthCheck.Disable = config.Disable
	}

	if workerManagerHealthCtx == nil {
		return
	}
	workerManagerHealthCtx.UpdateConfig(healthcheck.Config{
		Timeout:   time.Duration(config.Timeout) * time.Second,
		Interval:  time.Duration(config.Interval) * time.Second,
		Threshold: config.Threshold,
		Disable:   config.Disable,
	})
}

// StartWorkerManagerHealthCheck starts worker-manager health check
func StartWorkerManagerHealthCheck(stopCh <-chan struct{}) error {
	err := initLeaseInformer(stopCh)
	if err != nil {
		return err
	}
	if common.FConfig.HTTPSConfig != nil && common.FConfig.HTTPSConfig.HTTPSEnable {
		scheme = "https://"
	}
	workerManagerHealthCtx = healthcheck.NewContext(healthcheck.Config{
		Timeout:   time.Duration(common.FConfig.WMHealthCheck.Timeout) * time.Second,
		Interval:  time.Duration(common.FConfig.WMHealthCheck.Interval) * time.Second,
		Threshold: common.FConfig.WMHealthCheck.Threshold,
	}, checkWorkerManagerHealth)
	go workerManagerHealthCtx.Run(stopCh)
	return nil
}

func checkWorkerManagerHealth(ctx context.Context) error {
	wmAddr := getWmAddr()
	if wmAddr == "" {
		return errors.New("no worker-manager address found")
	}

	url := scheme + wmAddr + "/componenthealth"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to new request to worker-manager: %s", err.Error())
		return err
	}
	utils.FillInWorkerManagerRequestHeaders(req)
	resp, err := utils.GetWorkerManagerClient().Do(req)
	if err != nil {
		log.GetLogger().Errorf("failed to do request to worker-manager: %s", err.Error())
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("worker-manager health check status ok not ok: %d", resp.StatusCode)
		return errors.New("")
	}
	return nil
}
