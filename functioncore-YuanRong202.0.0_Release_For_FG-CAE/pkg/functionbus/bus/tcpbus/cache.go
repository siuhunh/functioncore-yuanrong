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

package tcpbus

import (
	"sync"

	"functioncore/pkg/common/logger/log"
)

var (
	defaultPodConnCache = &ConnCache{}
)

// DefaultPodConnCache return default conn cache for direct runtime pod
func DefaultPodConnCache() *ConnCache {
	return defaultPodConnCache
}

// RuntimeReport report msg of runtime
type RuntimeReport struct {
	Stage string
	Conn  *Connection
}

// ConnCache cached connection
type ConnCache struct {
	m sync.Map
}

// SetOrUpdateConnReady update conn ready status
func (cc *ConnCache) SetOrUpdateConnReady(name string, report RuntimeReport) {
	chanIf, loaded := cc.m.Load(name)
	if !loaded {
		log.GetLogger().Infof("create conn to conn cache of %s, stage %s", name, report.Stage)
		chanIf, _ = cc.m.LoadOrStore(name, make(chan RuntimeReport, 1))
	}
	c, ok := chanIf.(chan RuntimeReport)
	if ok && c != nil {
		c <- report
	}
}

// GetOrCreateConnWaiter create Connection channel if not exist and return it
func (cc *ConnCache) GetOrCreateConnWaiter(name string) <-chan RuntimeReport {
	chanIf, loaded := cc.m.Load(name)
	if !loaded {
		log.GetLogger().Infof("create conn to conn cache of %s", name)
		chanIf, _ = cc.m.LoadOrStore(name, make(chan RuntimeReport, 1))
	}
	ch, ok := chanIf.(chan RuntimeReport)
	if !ok {
		return nil
	}
	return ch
}

// Delete remove cached conn
func (cc *ConnCache) Delete(name string) {
	log.GetLogger().Infof("delete conn from conn cache of %s", name)
	cc.m.Delete(name)
}
