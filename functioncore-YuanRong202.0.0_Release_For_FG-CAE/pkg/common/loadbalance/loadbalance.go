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

// Package loadbalance provides load balancing algorithm
package loadbalance

import "time"

const (
	// RoundRobinNginx represents type of Round Robin Nginx
	RoundRobinNginx LBType = iota
	// RoundRobinLVS represents type of Round Robin LVS
	RoundRobinLVS
	// ConsistentHashGeneric represents type of Generic Consistent Hash
	ConsistentHashGeneric
	// RandomRoundRobin represents RR strategy
	RandomRoundRobin
	// ConcurrentConsistentHashGeneric represents one of load-balancing algorithm
	ConcurrentConsistentHashGeneric
	// Decentralized represents one of load-balancing algorithm
	Decentralized
)

// LBType is the type of load loadbalance algorithm
type LBType int

const defaultCHGenericConcurrency = 100

// Request -
type Request struct {
	Name           string
	TraceID        string
	Timestamp      time.Time
	TargetInstance string
	RequestNum     uint64
}

// LoadBalance is the interface of loadbalance algorithm
type LoadBalance interface {
	// move parameter controls whether the hash loop moves
	Next(name string, move bool) interface{}
	Add(node interface{}, weight int)
	Remove(node interface{})
	RemoveAll()
	Reset()
	Done(node interface{}, cost time.Duration)

	NextWithRequest(req *Request, move bool) interface{}
	SetConcurrency(concurrency int)
	SetIsLongRun(isLongRun bool)
	Start()
	Stop()
	NoLock() bool
}

// LBFactory is the factory of loadbalance algorithm
func LBFactory(t LBType) LoadBalance {
	switch t {
	case RoundRobinNginx:
		return &WNGINX{}
	case RoundRobinLVS:
		return &WLVS{}
	case ConsistentHashGeneric:
		return NewCHGeneric()
	case RandomRoundRobin:
		return NewRandomRR()
	case ConcurrentConsistentHashGeneric:
		return NewConcurrentCHGeneric(defaultCHGenericConcurrency)
	case Decentralized:
		return NewDecentralizedLB()
	default:
		return NewCHGeneric()
	}
}
