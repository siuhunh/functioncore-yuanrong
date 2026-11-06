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

// Package v1alpha1 is the v1alpha1 version of the sn API
package v1alpha1

import (
	"functioncore/pkg/common/vpa"
)

// WorkerInstance define worker instance
type WorkerInstance struct {
	InstanceID      string               `json:"-"`
	PodName         string               `json:"-"`
	IP              string               `json:"ip,omitempty"`
	Port            string               `json:"port,omitempty"`
	Cluster         string               `json:"cluster,omitempty"`
	Status          string               `json:"status,omitempty"`
	P2pPort         string               `json:"p2pPort,omitempty"`
	NodeIP          string               `json:"nodeIP,omitempty"`
	NodePort        string               `json:"nodePort,omitempty"`
	NodeName        string               `json:"-"`
	StateID         string               `json:"stateID,omitempty"`
	Applier         string               `json:"applier,omitempty"`
	BusinessID      string               `json:"-"`
	TenantID        string               `json:"-"`
	FuncName        string               `json:"-"`
	Version         string               `json:"-"`
	OwnerIP         string               `json:"ownerIP,omitempty"`
	CPU             int                  `json:"cpu,omitempty"`
	Memory          int                  `json:"memory,omitempty"`
	WillDelete      bool                 `json:"willDelete,omitempty"`
	Signature       string               `json:"-"`
	IsMigrating     bool                 `json:"-"`
	IsStartMigrate  bool                 `json:"-"`
	MigratedFrom    string               `json:"-"`
	BusinessType    string               `json:"businessType,omitempty"`
	Creator         string               `json:"-"`
	Service         string               `json:"service,omitempty"`
	IsDirectFunc    bool                 `json:"isDirectFunc,omitempty"`
	HasInitializer  bool                 `json:"hasInitializer,omitempty"`
	IsRunning       bool                 `json:"-"`
	CreationTime    int64                `json:"creationTime,omitempty"`
	IsBridgeFunc    bool                 `json:"-"`
	PodUID          string               `json:"podUID,omitempty"`
	ContainerIDs    vpa.ContainerInfo    `json:"containerIDs,omitempty"`
	Resource        vpa.PodResourceInfo  `json:"resource,omitempty"`
	ViewResource    vpa.ViewResourceInfo `json:"viewResource,omitempty"`
	EnableWebsocket bool                 `json:"enable_websocket" valid:",optional"`
}
