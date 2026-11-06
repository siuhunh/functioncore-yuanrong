/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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

// Package bridgetypes -
package bridgetypes

import "sync"

// ClientInstance define bridgeFunc client instance info
type ClientInstance struct {
	FuncInfo            *BridgeFuncInfo `json:"bridgeFuncInfo"`
	PodName             string          `json:"podName"`
	AssignStateKey      string          `json:"assignStateKey,omitempty"`
	AssignStateID       string          `json:"assignStateID,omitempty"`
	LastModifyClusterID string          `json:"LastModifyClusterID,omitempty"`
	FuncUseInfoLock     sync.RWMutex
	// map key is stateID, value identify stateID useCount
	FuncUseInfo map[string]int `json:"funcUseInfo,omitempty"`
	FuncName    string         `json:"funcName,omitempty"`
	Version     string         `json:"version,omitempty"`
	TenantID    string         `json:"tenantID,omitempty"`
}

// BridgeFuncInfo define bridgeFunc info
type BridgeFuncInfo struct {
	TenantID       string `json:"tenantID"`
	ServiceID      string `json:"serviceID"`
	BridgeFuncName string `json:"bridgeFuncName"`
}

// BridgeInstance define bridge instance info
type BridgeInstance struct {
	PodName       string
	BridgeFuncUrn string
}

// FuncRequestInfo define bridgeFunc request info
type FuncRequestInfo struct {
	TraceID        string
	FunctionName   string
	ClientInstance ClientInstance
}

// PodInfo define bridgeFunc Pod Info
type PodInfo struct {
	ClientPodName  string
	BridgeFuncName string
	TenantID       string
}

// StateIDInfo define stateId Info
type StateIDInfo struct {
	StateKey              string
	StateID               string
	PodName               string
	IsNewStateKeyScenario bool
}

// SpecificBridgeScaleData -
type SpecificBridgeScaleData struct {
	ScaleUpTolerance   float64                     `json:"scaleUpTolerance" valid:"optional"`
	ScaleDownTolerance float64                     `json:"scaleDownTolerance" valid:"optional"`
	ScaleRules         map[string]map[string]int64 `json:"scaleRules" valid:"optional"`
}

// BridgeManagerConfig -
type BridgeManagerConfig struct {
	// second
	PullInstanceTimeSpan int `json:"pullInstanceTimeSpan,omitempty" valid:",optional"`
	// specific bridge functions support,
	// {"bridge1": {"scaleUpTolerance": 0.2,"scaleDownTolerance": 0.3,
	// "scaleRules":{"cache":{"cpu":30,"memory":40,"connections":40}}}}
	BridgeFuncWhiteList map[string]SpecificBridgeScaleData `json:"bridgeFuncWhiteList" valid:"optional"`
}
