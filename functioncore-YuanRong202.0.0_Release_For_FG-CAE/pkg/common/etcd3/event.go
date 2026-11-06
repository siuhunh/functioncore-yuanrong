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

// Package etcd3 implements crud and watch operations based etcd clientv3
package etcd3

import (
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	// PUT event
	PUT = iota
	// DELETE event
	DELETE
	// ERROR unexpected event
	ERROR
	// SYNCED synced event
	SYNCED
)

// Event of databases
type Event struct {
	Type      int
	Key       string `json:"-"`
	Value     []byte
	PrevValue []byte `json:"-"`
	Rev       int64
	Source    EtcdType
}

// parseKV converts a KeyValue retrieved from an initial sync() listing to a synthetic isCreated event.
func parseKV(kv *mvccpb.KeyValue, source EtcdType) *Event {
	return &Event{
		Type:      PUT,
		Key:       string(kv.Key),
		Value:     kv.Value,
		PrevValue: nil,
		Rev:       kv.ModRevision,
		Source:    source,
	}
}

func parseEvent(e *clientv3.Event, source EtcdType) *Event {
	eType := 0
	if e.Type == clientv3.EventTypeDelete {
		eType = DELETE
	}
	ret := &Event{
		Type:   eType,
		Key:    string(e.Kv.Key),
		Value:  e.Kv.Value,
		Rev:    e.Kv.ModRevision,
		Source: source,
	}
	if e.PrevKv != nil {
		ret.PrevValue = e.PrevKv.Value
	}
	return ret
}

func parseErr(err error, source EtcdType) *Event {
	return &Event{Type: ERROR, Value: []byte(err.Error()), Source: source}
}

func parseSync(t time.Time, source EtcdType) *Event {
	return &Event{Type: SYNCED, Value: []byte(t.String()), Source: source}
}
