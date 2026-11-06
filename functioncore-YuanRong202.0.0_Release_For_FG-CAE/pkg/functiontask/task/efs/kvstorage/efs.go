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

// Package kvstorage implements external future store in a key-value storage.
package kvstorage

import (
	"encoding/json"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/efs"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
	"functioncore/pkg/functiontask/task/util/short"
)

const (
	metadataPrefix = "metadata"
	valuePrefix    = "value"
)

// FutureMetadata represents Metadata for Future .
type FutureMetadata struct {
	Type        int      `json:"type"`
	ID          string   `json:"id"`
	ReqFutureID string   `json:"reqFutureID"`
	ServiceID   string   `json:"serviceID"`
	FuncName    string   `json:"funcName"`
	Version     string   `json:"version"`
	StateID     string   `json:"stateID"`
	TraceID     string   `json:"traceID"`
	ReqIDList   []string `json:"reqIDList"`
}

// FutureValue represents Future value according to response.
type FutureValue struct {
	Code    int32  `json:"code"`
	Message string `json:"message"`
	Result  string `json:"result"`
	StateID string `json:"stateID"`
	State   string `json:"state"`
	Log     string `json:"log"`
}

// EFS represents an external future store in key-value storage.
type EFS struct {
	storage Storage
}

// New returns a new external future store in key-value storage.
func New(storage Storage) efs.ExtFutureStore {
	return &EFS{storage: storage}
}

// GetRequest recovers request from external future store.
func (e *EFS) GetRequest(tenantID string, serviceID string,
	id snfuture.ID, reqIDList common.ReqIDList) *snfuture.Future {
	data, err := e.storage.Get(metadataPrefix + id)
	if err != nil {
		return nil
	}
	log.GetLogger().Debugf("efs.GetRequest.Metadata, data: %s, reqIDList: %s", data, short.List(reqIDList))
	metaData := new(FutureMetadata)
	if err := json.Unmarshal([]byte(data), metaData); err != nil {
		log.GetLogger().Errorf("metaData Unmarshal, reqIDList: %s, err: %s", short.List(reqIDList), err.Error())
		return nil
	}
	recoverFuture := snfuture.NewInvoke(snfuture.Metadata{
		ServiceID: metaData.ServiceID,
		FuncName:  metaData.FuncName,
		Version:   metaData.Version,
		StateID:   metaData.StateID,
		ReqIDList: metaData.ReqIDList,
	})
	key := getKey(recoverFuture.StateID, tenantID, serviceID, id)
	valueData, err := e.storage.Get(key)
	if err != nil {
		log.GetLogger().Warnf("efs.noFutureValue, key: %s, reqIDList: %s, err: %s", key, short.List(reqIDList),
			err.Error())
		return recoverFuture
	}
	log.GetLogger().Debugf("efs.GetRequest.FutureValue, key: %s, data: %s, reqIDList: %s",
		key, data, short.List(reqIDList))
	if valueData == "" {
		return recoverFuture
	}
	resp := new(common.Response)
	if err := json.Unmarshal([]byte(valueData), resp); err != nil {
		log.GetLogger().Errorf("metaData Unmarshal, reqIDList: %s, err: %s", short.List(reqIDList), err.Error())
		return nil
	}
	log.GetLogger().Debugf("efs.RecoverResult, reqIDList: %s, code: %d, message: %s, stateID: %s, state: %s, "+
		"result: %s, log: %s", short.List(reqIDList), resp.Code, resp.Message, resp.StateID, resp.State,
		resp.Result, resp.Log)
	recoverFuture.SetResponse(resp)
	return recoverFuture
}

// SetResult sets result to external future store.
func (e *EFS) SetResult(tenantID string, serviceID string,
	sncFuture *snfuture.Future, result *common.Response) common.TaskError {
	if result.Code != 0 {
		log.GetLogger().Infof("efs.SetErrResult, code: %d, message: %s, result: %s, stateID: %s, state: %s, log: %s",
			result.Code, result.Message, result.Result, result.StateID, result.State, result.Log)
		return nil
	}
	data, err := json.Marshal(result)
	if err != nil {
		log.GetLogger().Errorf("future Value Marshal, reqIDList: %s, err: %s", short.List(sncFuture.ReqIDList),
			err.Error())
		return common.NewTaskError(common.ESetValue, err.Error())
	}
	key := getKey(sncFuture.StateID, tenantID, serviceID, sncFuture.ID)
	log.GetLogger().Debugf("efs.SetResult, key: %s, data: %s, reqIDList: %s",
		key, data, short.List(sncFuture.ReqIDList))
	setErr := e.storage.Set(key, string(data), int(config.Conf.ExternalFutureTTL))
	if setErr != nil {
		log.GetLogger().Errorf("setFutureValue, reqIDList: %s, err: %s", short.List(sncFuture.ReqIDList),
			setErr.Error())
		return setErr
	}
	return nil
}

// ClearResult clears result to external future store.
func (e *EFS) ClearResult(tenantID string, serviceID string,
	sncFuture *snfuture.Future) common.TaskError {
	key := getKey(sncFuture.StateID, tenantID, serviceID, sncFuture.ID)
	log.GetLogger().Debugf("efs for clears result, key: %s, reqIDList: %s", key, short.List(sncFuture.ReqIDList))
	setErr := e.storage.Delete(key)
	if setErr != nil && setErr.Error() != "redigo: nil returned" {
		log.GetLogger().Errorf("clears result, reqIDList: %s, err: %s", short.List(sncFuture.ReqIDList),
			setErr.Error())
		return setErr
	}
	return nil
}

// SetRequest sets request to external future store.
func (e *EFS) SetRequest(tenantID string, serviceID string, sncFuture *snfuture.Future,
	reqFutureID string) common.TaskError {
	metaData := &FutureMetadata{
		Type:        sncFuture.GetType(),
		ID:          sncFuture.ID,
		ReqFutureID: reqFutureID,
		ServiceID:   sncFuture.ServiceID,
		FuncName:    sncFuture.FuncName,
		Version:     sncFuture.Version,
		StateID:     sncFuture.StateID,
		TraceID:     sncFuture.TraceID,
		ReqIDList:   sncFuture.ReqIDList,
	}
	data, err := json.Marshal(metaData)
	if err != nil {
		log.GetLogger().Errorf("metaData Marshal, reqIDList: %s, err: %s", short.List(sncFuture.ReqIDList),
			err.Error())
		return common.NewTaskError(common.ESetMetadata, err.Error())
	}
	log.GetLogger().Debugf("efs.SetRequest, data: %s, reqIDList: %s", data, short.List(sncFuture.ReqIDList))
	setErr := e.storage.Set(metadataPrefix+sncFuture.ID, string(data), int(config.Conf.ExternalFutureTTL))
	if setErr != nil {
		log.GetLogger().Errorf("setMetadata, reqIDList: %s, err: %s", short.List(sncFuture.ReqIDList),
			setErr.Error())
		return setErr
	}
	return nil
}

func getKey(stateID string, tenantID string, serviceID string, id snfuture.ID) string {
	if stateID == "" {
		return valuePrefix + id
	}
	return metadata.Gen(tenantID, serviceID, valuePrefix, id)
}
