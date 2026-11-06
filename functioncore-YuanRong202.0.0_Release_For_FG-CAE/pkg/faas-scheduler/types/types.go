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

// Package types contains base type defines of scheduler
package types

import "functioncore/pkg/common/etcd3"

// FunctionMetaInfo saves functionMeta Info
type FunctionMetaInfo struct {
	FuncMetaData     FuncMetaData     `json:"funcMetaData" valid:",optional"`
	InstanceMetaData InstanceMetaData `json:"instanceMetaData" valid:",optional"`
	ResourceMetaData ResourceMetaData `json:"resourceMetaData" valid:"optional"`
	Source           etcd3.EtcdType   `json:"-"`
}

// FuncMetaData define meta data of functions
type FuncMetaData struct {
	// "functionVersionUrn":"sn:cn:yrk:i1fe539427b24702acc11fbb4e134e17:function:0-mytest-functionb:$latest"
	FunctionVersionURN string `json:"functionVersionUrn"`
	BusinessType       string `json:"businessType"`
	EnableRemoteDebug  bool   `json:"enableRemoteDebug"`
}

// InstanceMetaData define instance meta data of functions
type InstanceMetaData struct {
	ConcurrentNum int `json:"concurrentNum"`
}

// ResourceMetaData define instance metadata of resource for cpu and memory
type ResourceMetaData struct {
	CPU    int `json:"cpu"`
	Memory int `json:"memory"`
}

// WorkerInstance saves worker instance value
type WorkerInstance struct {
	InstanceID string `json:"instanceID,omitempty"`
	StateID    string `json:"stateID,omitempty"`
	Applier    string `json:"applier,omitempty"`
	NodeIP     string `json:"nodeIP,omitempty"`
	NodePort   string `json:"nodePort,omitempty"`
	CPU        int    `json:"cpu,omitempty"`
	Memory     int    `json:"memory,omitempty"`
	PodIP      string `json:"ip,omitempty"`
	PodPort    string `json:"port,omitempty"`
}
