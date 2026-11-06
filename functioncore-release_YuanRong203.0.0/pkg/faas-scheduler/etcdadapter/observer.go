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

// Package etcdadapter contains etcd interaction
package etcdadapter

import (
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

// Observer defines observer
type Observer interface {
	Selection() etcd3.EtcdType
	Object() interface{}
	HandleUpdate(interface{})
	HandleDelete(interface{})
	HandleSync(interface{})
}

// EtcdObserver is the observer for etcd
type EtcdObserver struct {
	SyncHandler   func()
	UpdateHandler func(*etcd3.Event)
	DeleteHandler func(*etcd3.Event)
	Filter        func(string) bool
	WatchPath     string
	// Selection to specify the routing etcd or metadata etcd to use
	EtcdSelection etcd3.EtcdType
	HasSynced     bool
}

// Selection returns the etcd which is selected
func (eo *EtcdObserver) Selection() etcd3.EtcdType {
	return eo.EtcdSelection
}

// Object returns watch object
func (eo *EtcdObserver) Object() interface{} {
	return eo.WatchPath
}

// HandleUpdate handles update event
func (eo *EtcdObserver) HandleUpdate(eventMsg interface{}) {
	event, ok := eventMsg.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("not a valid etcd event")
		return
	}
	if !eo.Filter(event.Key) {
		return
	}
	eo.UpdateHandler(event)
}

// HandleDelete handles delete event
func (eo *EtcdObserver) HandleDelete(eventMsg interface{}) {
	event, ok := eventMsg.(*etcd3.Event)
	if !ok {
		log.GetLogger().Errorf("not a valid etcd event")
		return
	}
	if !eo.Filter(event.Key) {
		return
	}
	eo.DeleteHandler(event)
}

// HandleSync handles sync event
func (eo *EtcdObserver) HandleSync(eventMsg interface{}) {
	if eo.SyncHandler == nil {
		return
	}
	if !eo.HasSynced {
		eo.SyncHandler()
	}
	eo.HasSynced = true
}
