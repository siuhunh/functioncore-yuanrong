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

// Package common -
package common

import (
	"k8s.io/api/core/v1"

	"functioncore/pkg/common/types"
)

// PodRequest define pod standard
type PodRequest struct {
	SnapshotDeployment       *types.SnapshotDeploymentSpec
	PodTemplate              *v1.Pod
	EnableTMPExpansion       bool
	NodeLabel                []map[string]string `json:"deployLabel" valid:"optional"`
	ForceDeploy              bool                `json:"forceDeploy" valid:"optional"`
	CPU                      int64               `json:"cpu" valid:"optional"`
	Mem                      int64               `json:"mem" valid:"optional"`
	FuncName                 string              `json:"funcName" valid:"optional"`
	TraceID                  string              `json:"traceID" valid:"optional"`
	CurrentWorkerNum         int                 `json:"currentWorkerNum" valid:"optional"`
	ScalingWorkersNum        int                 `json:"scalingWorkersNum" valid:"optional"`
	FunSvcID                 string              `json:"funSvcID" valid:"optional"`
	Applier                  string              `json:"applier" valid:"optional"`
	Priority                 int
	Timestamp                int64
	GpuMemory                int64
	GpuType                  string
	CustomResource           map[string]int64
	MigratedFrom             string
	IsMigrating              bool
	PatPodInfo               []types.NATConfigure
	EndpointTalentPatPodInfo []types.NATConfigure
	PublicNetworkPatPodInfo  []types.NATConfigure
	RouteInfo                types.RouteInfoData
	FuncMountConfig          types.FuncMountConfig
	SvcCidr                  string // SvcCidr is cluster service network segment
}

const (
	// FailedToPullUpImageOfNoSpace -
	FailedToPullUpImageOfNoSpace = "failed to pull up image because no space left on device"
)
