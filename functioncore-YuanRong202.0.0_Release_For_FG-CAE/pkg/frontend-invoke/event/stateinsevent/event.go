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

// Package stateinsevent To handle state routing events
package stateinsevent

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/frontend-invoke/functionlb"
)

// IsStateInstance /sn/stateworkers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz
func IsStateInstance(key string) bool {
	if !strings.HasPrefix(key, etcdkey.StateRoutePrefix) {
		return false
	}
	stateIns := &etcdkey.StateWorkersKey{}
	if err := stateIns.ParseFrom(key); err != nil {
		return false
	}
	return true
}

// ProcessStateInsEvent -
func ProcessStateInsEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		return processAddEvent(event)
	case etcd3.DELETE:
		return processDelEvent(event)
	case etcd3.ERROR:
		return errors.New("get unexpected result when watching etcd")
	default:
		return errors.New("unknown etcd event")
	}
}

func processAddEvent(event *etcd3.Event) error {
	stateInsKey := &etcdkey.StateWorkersKey{}
	if err := stateInsKey.ParseFrom(event.Key); err != nil {
		return err
	}
	// parse workerInstanceConnection
	curStates := make(map[string]string, 1)
	err := json.Unmarshal(event.Value, &curStates)
	if err != nil {
		log.GetLogger().Errorf("invalid state info, err: %s", err.Error())
		return err
	}
	instance := curStates[stateInsKey.StateID]
	taskWorkerKey := etcdkey.GetTaskETCDPath(instance)
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient.Client == nil {
		log.GetLogger().Warnf("failed to init state ETCD client")
		return errors.New("failed to init state ETCD client")
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	values, err := stateClient.GetValues(defaultEtcdCtx, taskWorkerKey)
	if err != nil {
		log.GetLogger().Errorf("failed to query nodeIP from etcd by stateID: %s, err: %s",
			stateInsKey.StateID, err.Error())
		return err
	}
	if len(values) == 0 {
		log.GetLogger().Errorf("failed to query nodeIP from etcd by stateID: %s, err: %s",
			stateInsKey.StateID, errors.New("NOT FOUND"))
		return errors.New(snerror.InternalErrorMsg)
	}
	node := &functionlb.Node{}
	err = json.Unmarshal([]byte(values[0]), node)
	if err != nil {
		log.GetLogger().Errorf("invalid function-task info, err: %s", err.Error())
		return err
	}
	insConnection := functionlb.NewWorkerInstanceConnection(node.NodeIP, node.NodePort, instance, "")
	functionlb.FunctionLB.GetStateIns().WorkerInstancesConn.Store(stateInsKey.StateID, insConnection)
	functionlb.FunctionLB.GetStateIns().GetStateSingleFlight().Remove(stateInsKey.StateID)
	log.GetLogger().Infof("succeed to add state instance, stateID: %s, route node ip: %s",
		stateInsKey.StateID, node.NodeIP)
	return nil
}

func processDelEvent(event *etcd3.Event) error {
	stateInsKey := &etcdkey.StateWorkersKey{}
	if err := stateInsKey.ParseFrom(event.Key); err != nil {
		return err
	}
	functionlb.FunctionLB.GetStateIns().WorkerInstancesConn.Delete(stateInsKey.StateID)
	functionlb.FunctionLB.GetStateIns().GetStateSingleFlight().Remove(stateInsKey.StateID)
	log.GetLogger().Infof("succeed to delete state instance, stateID: %s", stateInsKey.StateID)
	return nil
}
