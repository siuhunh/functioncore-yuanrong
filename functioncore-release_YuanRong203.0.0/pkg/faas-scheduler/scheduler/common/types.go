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

// Package common contains service common define
package common

import (
	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/faas-scheduler/types"
)

// InstanceEventType instance event type
type InstanceEventType uint32

const (
	// AddInstance -
	AddInstance = 0
	// DeleteInstance -
	DeleteInstance = 1
	// RecoverInstance -
	RecoverInstance = 2
)

const (
	// InstancePlaceHolderPrefix prefix for placeholder function instance
	InstancePlaceHolderPrefix = "MOCK-"
	// WorkerDefaultWebSocketPort websocket function instance port
	WorkerDefaultWebSocketPort = "8000"
	// WorkerServerPort -
	WorkerServerPort = "8081"
)

// Instance instance info
type Instance struct {
	InstanceKey    string
	Applier        string
	PodIP          string
	PodPort        string
	NodeIP         string
	NodePort       string
	WillDelete     bool
	IsWebsocketCae bool
	InstanceReq    *schedulerapi.CheckInstanceReq
}

// Observer instance observer
type Observer interface {
	OnObserved(instance *Instance, eventType InstanceEventType)
}

// AddObserver add instance observer
type AddObserver func(funcKeyWithRes string, observer Observer)

// DeleteObserver delete instance observer interface
type DeleteObserver func(funcKeyWithRes string)

// FuncMetaFetcher fetch function meta interface
type FuncMetaFetcher func(funcKey string) (*types.FunctionMetaInfo, error)
