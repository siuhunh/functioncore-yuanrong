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

// Package healthcheck provides health check context
package healthcheck

import (
	"context"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
)

// CheckFunc functions that do actual health check
type CheckFunc func(ctx context.Context) error

// Config of health check
type Config struct {
	Timeout   time.Duration
	Interval  time.Duration
	Threshold int
	Disable   bool
}

// NewContext creates Context
func NewContext(cfg Config, healthCheck CheckFunc) *Context {
	ctx, cancel := context.WithCancel(context.Background())
	return &Context{
		ctx:         ctx,
		cancel:      cancel,
		timeout:     cfg.Timeout,
		interval:    cfg.Interval,
		threshold:   cfg.Threshold,
		healthCheck: healthCheck,
		updateCh:    make(chan Config, 1),
	}
}

// Context wrappers context.Context with health check logic:
// if health check failed, the wrapper context will auto canceled
// if then health check succeed, a new wrapper context will auto created
type Context struct {
	ctx         context.Context
	cancel      context.CancelFunc
	healthCheck CheckFunc
	updateCh    chan Config
	timeout     time.Duration
	interval    time.Duration
	threshold   int
	failedCount int
	disable     bool
	sync.RWMutex
}

// GetContext returns inner wrapper context
func (hc *Context) GetContext() context.Context {
	hc.RLock()
	ctx := hc.ctx
	hc.RUnlock()
	return ctx
}

// UpdateConfig update config of health check
func (hc *Context) UpdateConfig(cfg Config) {
	hc.updateCh <- cfg
}

func (hc *Context) cancelContext() {
	hc.RLock()
	hc.cancel()
	hc.RUnlock()
}

// CancelAndCreateNewContext cancels old context and creates new one
func (hc *Context) CancelAndCreateNewContext() {
	hc.Lock()
	hc.cancel()
	hc.ctx, hc.cancel = context.WithCancel(context.Background())
	hc.Unlock()
}

// Run runs health check and update status of context
func (hc *Context) Run(stopCh <-chan struct{}) {
	if stopCh == nil {
		return
	}
	ticker := time.NewTicker(hc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if hc.disable {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), hc.timeout)
			err := hc.healthCheck(ctx)
			cancel()
			if err == nil {
				if hc.failedCount != 0 {
					hc.failedCount = 0
					log.GetLogger().Infof("health check success and create new context")
					hc.CancelAndCreateNewContext()
				}
				continue
			}
			hc.failedCount++
			if hc.failedCount >= hc.threshold {
				log.GetLogger().Errorf("health check failed with count %d err %s", hc.failedCount, err.Error())
				hc.cancelContext()
			}
		case cfg, ok := <-hc.updateCh:
			if !ok {
				return
			}
			hc.threshold = cfg.Threshold
			hc.interval = cfg.Interval
			hc.timeout = cfg.Timeout
			ticker.Reset(hc.interval)
			if !hc.disable && cfg.Disable {
				hc.Lock()
				hc.ctx, hc.cancel = context.WithCancel(context.Background())
				hc.Unlock()
			}
			hc.disable = cfg.Disable
		case <-stopCh:
			return
		}
	}
}
