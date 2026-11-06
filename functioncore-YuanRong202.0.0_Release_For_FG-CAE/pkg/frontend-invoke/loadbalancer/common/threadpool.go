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

package common

import (
	"github.com/panjf2000/ants/v2"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/frontend-invoke/constant"
)

var (
	threadPoolInstance *ThreadPool
)

// ThreadPool -
type ThreadPool struct {
	pool *ants.Pool
}

// CreatePoolInstance -
func CreatePoolInstance() error {
	pool, err := newPool()
	if err != nil {
		return err
	}
	threadPoolInstance = &ThreadPool{
		pool: pool,
	}
	return nil
}

// GetPoolInstance -
func GetPoolInstance() *ThreadPool {
	return threadPoolInstance
}

// Submit -
func (p *ThreadPool) Submit(task func()) {
	err := p.pool.Submit(task)
	if err != nil {
		log.GetLogger().Errorf("failed to submit task, err %s", err.Error())
	}
}

func newPool() (*ants.Pool, error) {
	pool, err := ants.NewPool(constant.FConfig.LoadBalanceConfig.ThreadPoolSize,
		ants.WithPreAlloc(true), ants.WithNonblocking(true))
	if err != nil {
		return nil, err
	}
	return pool, nil
}
