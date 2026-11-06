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

// Package etcdadapter contains loadbalancer etcd adapter
package etcdadapter

import (
	"encoding/json"
	"errors"
	"strings"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/types"
	"functioncore/pkg/frontend-invoke/loadbalancer/service"
)

const (
	// SchedulerPrefix -
	SchedulerPrefix = "/sn/faas-scheduler"
)

var (
	schedulerInsHandler service.SchedulerInsHandler
)

// SetSchedulerInsHandler -
func SetSchedulerInsHandler(handler service.SchedulerInsHandler) {
	schedulerInsHandler = handler
}

// IsFaaSScheduler sn/faas-scheduler/instances/cluster001/7.218.100.25/faas-scheduler-59ddbc4b75-8xdjf
func IsFaaSScheduler(key string) bool {
	return strings.Contains(key, SchedulerPrefix)
}

// ProcessSchedulerEvent -
func ProcessSchedulerEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		if err := handleUpdate(event); err != nil {
			return err
		}
		return nil
	case etcd3.DELETE:
		if err := handleDelete(event); err != nil {
			return err
		}
		return nil
	case etcd3.ERROR:
		return errors.New("get unexpected result when watching etcd")
	default:
		return errors.New("undefined etcd event")
	}
}

func handleUpdate(ev *etcd3.Event) error {
	scheduler := types.FaaSServiceInfo{}
	err := json.Unmarshal(ev.Value, &scheduler)
	if err != nil {
		return err
	}
	key := scheduler.InstanceID
	schedulerInsHandler.AddScheduler(key, &scheduler)
	return nil
}

func handleDelete(ev *etcd3.Event) error {
	scheduler := types.FaaSServiceInfo{}
	err := json.Unmarshal(ev.PrevValue, &scheduler)
	if err != nil {
		return err
	}
	key := scheduler.InstanceID
	schedulerInsHandler.DeleteScheduler(key, &scheduler)
	return nil
}
