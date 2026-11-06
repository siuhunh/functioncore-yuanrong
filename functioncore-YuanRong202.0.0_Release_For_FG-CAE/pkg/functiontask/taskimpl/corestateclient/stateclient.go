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

// Package corestateclient implements state client interface.
package corestateclient

import (
	"encoding/json"

	"functioncore/pkg/common/logger/log"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/pkg/functiontask/task/common/metadata"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/efs/kvstorage"
	"functioncore/pkg/functiontask/task/stateclient"
	"functioncore/pkg/functiontask/task/util/short"
	"functioncore/pkg/functiontask/taskimpl/coreconfig"
	"functioncore/pkg/functiontask/taskimpl/corestorage"
)

type stateClient struct {
	storage kvstorage.Storage
}

// New creates a new state client.
func New() stateclient.StateClient {
	return &stateClient{
		storage: corestorage.NewStorage(),
	}
}

// Get the state.
func (s *stateClient) Get(key string) (string, common.TaskError) {
	return s.storage.Get(key)
}

// Set adds stateID to stateIDHash, adds the state with ttl(seconds) if PersistentState==true.
// to make sure stateIDHash contains all stateIDs, stateIDHash's adding should before state's adding
func (s *stateClient) Set(key string, state string, TTL int) common.TaskError {
	if !config.Conf.PersistentState {
		return nil
	}

	err := functioncore.SaveState(key, state, &functioncore.SaveOption{TTL: TTL})
	if err != nil {
		return common.NewTaskError(int32(err.Code()), err.Error())
	}
	return nil
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

// SetStateAndResult adds stateID to stateIDHash, Sets the state and result if PersistentState == true.
func (s *stateClient) SetStateAndResult(metadata *metadata.Metadata, state string, TTL int,
	req *common.Request, result *common.Response) common.TaskError {
	if result.Code != 0 {
		log.GetLogger().Infof("extFutureStore.SetErrResult code: %d, message: %s, "+
			"result: %s, stateID: %s, status: %s, log: %s",
			result.Code, result.Message, result.Result, result.StateID, result.State, result.Log)
		return s.Set(metadata.Key(), state, TTL)
	}
	_, err := functioncore.HSetNX(metadata.StateHashKey(), metadata.StateID, metadata.StateKey)
	if err != nil {
		return common.NewTaskError(int32(err.Code()), err.Error())
	}
	log.GetLogger().Debugf("adds stateID to stateIDHash, stateHashKey: %s, stateID: %s, stateKey: %s",
		metadata.StateHashKey(), metadata.StateID, metadata.StateKey)
	if !config.Conf.PersistentState {
		return nil
	}
	value := &FutureValue{
		Code:    result.Code,
		Message: result.Message,
		Result:  result.Result,
		StateID: result.StateID,
		State:   result.State,
		Log:     result.Log,
	}
	data, mErr := json.Marshal(value)
	if mErr != nil {
		log.GetLogger().Errorf("failed to marshal future value, reqIDList: %s, err: %s",
			short.List(req.ReqIDList), mErr.Error())
		return common.NewTaskError(common.ESetValue, mErr.Error())
	}
	log.GetLogger().Debugf("SaveStateAndResult data: %s, key: %s, reqIDList: %s",
		data, metadata.Slot()+":value:"+req.FutureID, short.List(req.ReqIDList))
	err = functioncore.SaveStateAndResult(functioncore.SetCmdAndArg{
		Key:   metadata.Key(),
		Value: state,
		TTL:   TTL,
	}, functioncore.SetCmdAndArg{
		Key:   metadata.Slot() + ":value:" + req.FutureID,
		Value: string(data),
		TTL:   int(config.Conf.ExternalFutureTTL),
	})
	if err != nil {
		log.GetLogger().Errorf("failed to set future value, reqIDList: %s, err: %s", short.List(req.ReqIDList), err)
		return common.NewTaskError(int32(err.Code()), err.Error())
	}
	return nil
}

// DeleteStateValueAndStateMapping delete the state and state mapping.
// delete state if PersistentState == true
func (s *stateClient) DeleteStateValueAndStateMapping(key string) common.TaskError {
	tenantID, _, stateID := metadata.ExtractMetaKey(key)
	dErr := functioncore.DeleteStateMapping(stateID,
		tenantID, coreconfig.NameOfFunctionTask)
	if dErr != nil {
		log.GetLogger().Warnf("deleteStateMapping, err: %s", dErr.Error())
		return common.NewTaskError(int32(dErr.Code()), dErr.Error())
	}
	err := s.DeleteState(key, "")
	if err != nil {
		log.GetLogger().Warnf("delete state value key in redis, error: %s", err.Error())
	}
	return nil
}

// DeleteState delete the stateID from the stateIDHash.
// delete state if PersistentState == true
// to make sure stateIDHash contains all stateIDs, state's deleting should before stateIDHash's deleting
func (s *stateClient) DeleteState(key, funcName string) common.TaskError {
	if config.Conf.PersistentState {
		err := functioncore.DeleteState(key)
		if err != nil {
			return common.NewTaskError(int32(err.Code()), err.Error())
		}
	}
	tenantID, serviceID, stateID := metadata.ExtractMetaKey(key)
	stateHashKey := metadata.GeneStateHashKey(tenantID, serviceID, funcName)
	_, err := functioncore.HDel(stateHashKey, stateID)
	if err != nil {
		return common.NewTaskError(int32(err.Code()), err.Error())
	}
	return nil
}

// SetTTL sets the ttl for the state.
func (s *stateClient) SetTTL(key string, ttl int) common.TaskError {
	err := functioncore.ResetExpiredTTL(key, ttl)
	if err != nil {
		return common.NewTaskError(int32(err.Code()), err.Error())
	}
	return nil
}

// Scan returns state ids with specific service id.
// notice: no need to truncate state anymore. now return stateID rather than metaKey(tenantId-serviceID-stateID)
func (s *stateClient) Scan(pattern string) (map[string]string, common.TaskError) {
	tenantID, serviceID, _ := metadata.ExtractMetaKey(pattern)
	stateHashKey := metadata.GeneStateHashKey(tenantID, serviceID, "")
	ss, err := functioncore.HGetAll(stateHashKey)
	if err != nil {
		return nil, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return ss, nil
}

// GetNumsByServiceID get nums of stateID by serviceID
func (s *stateClient) GetNumsByServiceID(pattern string) (int, common.TaskError) {
	tenantID, serviceID, _ := metadata.ExtractMetaKey(pattern)
	stateHashKey := metadata.GeneStateHashKey(tenantID, serviceID, "")
	result, err := functioncore.HLen(stateHashKey)
	if err != nil {
		return 0, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return result, nil
}

// IsNotExist check whether it is a state not found error.
func (s *stateClient) IsNotExist(err error) bool {
	return functioncore.IsNotExist(err)
}

// StateExists get stateID status directly
func (s *stateClient) StateExists(key string) (bool, common.TaskError) {
	return s.storage.Exists(key)
}

// IsStateIDInHash check stateID is in hash
func (s *stateClient) IsStateIDInHash(stateHashKey string, stateID string) (bool, common.TaskError) {
	exist, err := functioncore.HExists(stateHashKey, stateID)
	if err != nil {
		return exist, common.NewTaskError(int32(err.Code()), err.Error())
	}
	return exist, nil
}
