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

// Package cancel provide cancel manager, which record which worker or proxy the request was sent to
package cancel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/common"
	"functioncore/pkg/functionbus/proxy/internal"
	"functioncore/pkg/functionbus/proxyerrors"
)

const (
	watchPathSize    = 4
	etcdTimeout      = 3 * time.Second
	maxInvokeTimeout = 15 * time.Minute
)

var (
	manager = newManager()
)

// GetInstance return cancel-manager instance
func GetInstance() *Manager {
	return manager
}

// Manager -
type Manager struct {
	store    map[string]*request
	observed map[string]*observeValue
	handler  WorkerCancelHandler
	mux      sync.Mutex
}

// WorkerCancelHandler -
type WorkerCancelHandler func(workerID string, dst *common.CancelProperty, requestID uint64, traceID string,
) *types.TCPResponse

type request struct {
	dispatcherReq *types.DispatcherRequest
	workerID      string
	async         bool
}

type observeValue struct {
	common.CancelProperty
	TraceID string `json:"traceID"`
	Applier string `json:"applier"`
}

func newManager() *Manager {
	return &Manager{
		store:    make(map[string]*request),
		observed: make(map[string]*observeValue),
	}
}

// RegisterCancelHandler -
func (m *Manager) RegisterCancelHandler(handler WorkerCancelHandler) {
	m.handler = handler
}

// HandleCancel handles cancel request from worker.
func (m *Manager) HandleCancel(parent context.Context, traceID string, dst *common.CancelProperty,
) *types.TCPResponse {
	log.GetLogger().Infof("handle cancel request from worker: %+v, traceID: %s", dst, traceID)

	etcdClient := etcd3.GetMetaEtcdClient()
	if etcdClient.Client == nil {
		log.GetLogger().Warnf("failed to init meta ETCD client")
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}
	val := observeValue{
		CancelProperty: *dst,
		TraceID:        traceID,
		Applier:        internal.HostName,
	}
	b, err := json.Marshal(val)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal observe value: %s, traceID: %s", err.Error(), traceID)
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), etcdTimeout)
	resp, err := etcdClient.Client.Grant(ctx, int64(maxInvokeTimeout.Seconds()))
	cancel()
	if err != nil {
		log.GetLogger().Errorf("failed to grant lease from etcd: %s, traceID: %s", err.Error(), traceID)
		return &types.TCPResponse{
			ErrorCode:    proxyerrors.InternalErrCode,
			ErrorMessage: proxyerrors.InternalErr,
		}
	}

	etcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcdTimeout)
	_ = etcdClient.Put(etcdCtx, etcd3.HandleCellPrefix(common.WatchPathPrefixWithCancel+"/"+dst.FutureID),
		string(b), clientv3.WithLease(resp.ID))

	log.GetLogger().Infof("successfully canceled: %+v, traceID: %s", dst, traceID)

	return &types.TCPResponse{
		ErrorCode:    proxyerrors.StateOKCode,
		ErrorMessage: "success",
	}
}

// AddRequest -
func (m *Manager) AddRequest(futureID string, dispatcherReq *types.DispatcherRequest) {
	if futureID == "" {
		return
	}

	log.GetLogger().Debugf("add request for futureID: %s", futureID)

	m.mux.Lock()
	defer m.mux.Unlock()
	// Request has already been canceled.
	if _, ok := m.observed[futureID]; ok {
		log.GetLogger().Infof("futureID %s has already been canceled", futureID)
		dispatcherReq.Canceled = true
		return
	}

	if _, ok := m.store[futureID]; ok {
		log.GetLogger().Warnf("futureID %s already exist", futureID)
		return
	}
	m.store[futureID] = &request{
		dispatcherReq: dispatcherReq,
	}
}

// UpdateRequest -
func (m *Manager) UpdateRequest(futureID, workerID string) {
	if futureID == "" {
		return
	}

	log.GetLogger().Debugf("update request for futureID: %s, workerID: %s", futureID, workerID)

	m.mux.Lock()
	req, ok := m.store[futureID]
	if !ok {
		log.GetLogger().Warnf("failed to find request with futureID: %s", futureID)
		m.mux.Unlock()
		return
	}
	req.workerID = workerID
	m.mux.Unlock()
}

// DeleteRequest -
func (m *Manager) DeleteRequest(futureID string) {
	if futureID == "" {
		return
	}

	log.GetLogger().Debugf("delete request for futureID: %s", futureID)

	m.mux.Lock()
	defer m.mux.Unlock()
	req, ok := m.store[futureID]
	if !ok {
		return
	}
	// "async" field is true iff worker's nodeIP == proxy's nodeIP && worker has returned RequestAccepted.
	// Possible situations are:
	// 1. worker's ownerIP == worker's nodeIP
	// 	 We are responsible for handling the cancel of this request in the future.
	// 2. worker's ownerIP != worker's nodeIP
	//   "async" field must be false. The other proxy who has worker's nodeIP == proxy's nodeIP will be responsible
	//   for handling the cancel of the request.
	if !req.async {
		delete(m.store, futureID)
	}
}

// DeleteAbandonedRequest -
func (m *Manager) DeleteAbandonedRequest(futureID string) {
	if futureID == "" {
		return
	}

	log.GetLogger().Debugf("delete async/abandoned request for futureID: %s", futureID)

	m.mux.Lock()
	delete(m.store, futureID)
	m.mux.Unlock()
}

// IsCancel -
func (m *Manager) IsCancel(path string) bool {
	return true
}

// UpdateOnObserve -
func (m *Manager) UpdateOnObserve(ev *etcd3.Event) {
	val, err := parseObservedEvent(ev)
	if err != nil {
		log.GetLogger().Warnf("failed to parse observe event: %s", err.Error())
		return
	}

	m.mux.Lock()
	m.observed[val.CancelProperty.FutureID] = &val

	req, ok := m.store[val.CancelProperty.FutureID]
	if !ok {
		m.mux.Unlock()
		return
	}

	if val.TraceID == "" {
		val.TraceID = commonutils.UniqueID()
	}
	log.GetLogger().Infof("start to cancel request: %+v, traceID: %s, workerID: %s", val.CancelProperty,
		val.TraceID, req.workerID)

	// Request has sent to worker. Tell worker to cancel.
	if req.workerID != "" {
		m.mux.Unlock()
		resp := m.handler(req.workerID, &val.CancelProperty, 0, val.TraceID)
		log.GetLogger().Infof("cancel %+v, traceID: %s, workerID: %s, response: %d %s", val.CancelProperty,
			val.TraceID, req.workerID, resp.ErrorCode, resp.ErrorMessage)
		return
	}

	// Request is still in proxy.
	req.dispatcherReq.Canceled = true
	m.mux.Unlock()
}

// DeleteOnObserve -
func (m *Manager) DeleteOnObserve(ev *etcd3.Event) {
	val, err := parseObservedEvent(ev)
	if err != nil {
		log.GetLogger().Warnf("failed to parse observe event: %s", err.Error())
		return
	}

	m.mux.Lock()
	delete(m.observed, val.CancelProperty.FutureID)
	m.mux.Unlock()
}

// RecoverRequests -
func (m *Manager) RecoverRequests(workerID string, futureIDs ...string) {
	for _, futureID := range futureIDs {
		m.recoverRequest(workerID, futureID, false)
	}
}

// RecordAsyncRequests for non-owned local workers (worker's nodeIP == proxy's nodeIP).
func (m *Manager) RecordAsyncRequests(workerID string, futureIDs ...string) {
	for _, futureID := range futureIDs {
		m.recoverRequest(workerID, futureID, true)
	}
}

func (m *Manager) recoverRequest(workerID, futureID string, recordAsync bool) {
	if futureID == "" {
		return
	}

	m.mux.Lock()
	// Request has already been canceled.
	if val, ok := m.observed[futureID]; ok {
		m.mux.Unlock()
		if val.TraceID == "" {
			val.TraceID = commonutils.UniqueID()
		}
		log.GetLogger().Infof("start to cancel request: %+v, traceID: %s, workerID: %s", val.CancelProperty,
			val.TraceID, workerID)
		resp := m.handler(workerID, &val.CancelProperty, 0, val.TraceID)
		log.GetLogger().Infof("cancel %+v, traceID: %s, workerID: %s, response: %d %s", val.CancelProperty,
			val.TraceID, workerID, resp.ErrorCode, resp.ErrorMessage)
		return
	}
	defer m.mux.Unlock()

	if _, ok := m.store[futureID]; ok {
		if recordAsync {
			m.store[futureID].async = true
			return
		}
		log.GetLogger().Warnf("futureID %s already exist", futureID)
		return
	}
	m.store[futureID] = &request{
		workerID: workerID,
		async:    recordAsync,
	}
}

func parseObservedEvent(ev *etcd3.Event) (observeValue, error) {
	tokens := strings.Split(ev.Key, "/")
	if len(tokens) != watchPathSize {
		return observeValue{}, fmt.Errorf("invalid watch key: %s", ev.Key)
	}
	futureID := tokens[len(tokens)-1]
	if futureID == "" {
		return observeValue{}, errors.New("empty futureID")
	}

	val := observeValue{}
	if err := json.Unmarshal(ev.Value, &val); err != nil {
		return observeValue{}, err
	}
	if val.CancelProperty.FutureID != "" && futureID != val.CancelProperty.FutureID {
		return observeValue{}, fmt.Errorf("futureID does not match. key has %s, yet val has %s",
			futureID, val.CancelProperty.FutureID)
	}
	if val.CancelProperty.FutureID == "" {
		val.CancelProperty.FutureID = futureID
	}

	return val, nil
}
