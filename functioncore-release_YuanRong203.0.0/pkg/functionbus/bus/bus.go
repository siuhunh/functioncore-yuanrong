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

// Package bus contains interface of bus and basic codes
package bus

import (
	"context"

	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/bus/channelbus"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
)

// CommunicationBus interface provides call method
type CommunicationBus interface {
	RegisterBus(property common.FunctionProperty) error
	// RegisterCallback called by others
	RegisterCallback(cb common.Call)
	RegisterCancel(cb common.Cancel)
	RegisterRecover(cb common.Recover)
	Init(ctx context.Context, concurrency int) (*types.TCPResponse, error)
	// Call call others
	Call(ctx context.Context, dst *common.FunctionProperty, req *types.TCPRequest,
		isSendingRemoteProxy bool) (*types.TCPResponse, error)
	Cancel(ctx context.Context, dst *common.CancelProperty, req *types.TCPRequest) (*types.TCPResponse, error)
	CheckRuntime(ctx context.Context, traceID string, shouldWait bool) (*types.TCPResponse, error)
	Recover(ctx context.Context, req *types.TCPRequest) (*types.TCPResponse, error)
	SetResult(resp *types.TCPResponse) error
	SetCancelResult(resp *types.TCPResponse) error
	SetRecoverResult(resp *types.TCPResponse) error
	CheckHealth() bool
	CheckRuntimeConnect() bool
	CheckAuth(key string, source string) bool
	WaitRequestsFinished(needReport bool, directExit <-chan struct{}) error
	Stop()
	StartRecvMessage()
	WaitBusExit()
	ReportRuntimeMetrics(ctx context.Context,
		metricsProperty *common.RuntimeMetricsProperty) (*types.TCPResponse, error)
	RegisterReportRuntimeMetrics(rr common.ReportRuntimeMetrics)
	RegisterStoreResourceUsage(rr common.StoreResourceUsage)
	RegisterStoreCaeMetrics(storeCaeMetrics common.StoreCaeMetrics)
	RegisterReportBridgeFuncMetrics(reportBridgeFuncMetrics common.ReportBridgeFuncMetrics)
}

// NewBus new bus for worker and coresdk
func NewBus(busType common.BusType, busOption common.BusOption) (CommunicationBus, error) {
	if busType == common.ChannelBusType {
		return channelbus.NewChannelBus(busOption)
	}

	return tcpbus.NewTCPBus(busOption)
}
