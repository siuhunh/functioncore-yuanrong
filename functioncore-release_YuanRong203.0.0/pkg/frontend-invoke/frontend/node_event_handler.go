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

package frontend

import (
	"encoding/json"
	"strings"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/functionlb"
)

const (
	// NodeEtcdPrefix is the etcd prefix for live node
	NodeEtcdPrefix = "/sn/workers/business/yrk/tenant/0/function/function-task"
	// ValidNodePathCount is the seprator count of Node Path
	ValidNodePathCount = 13
)

func processNodeEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		node := &functionlb.Node{}
		err := json.Unmarshal(event.Value, node)
		if err != nil {
			return err
		}
		functionlb.NodeRoute.AddNode(node, event.Key)

	case etcd3.DELETE:
		functionlb.NodeRoute.RemoveNode(event.Key)

	case etcd3.ERROR:
		log.GetLogger().Warnf("etcd error event: %s", event.Value)
	default:
		log.GetLogger().Warnf("unsupported event: %s", event.Value)
	}
	return nil
}

// IsTaskNode /sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/node01
func IsTaskNode(eventKey string) bool {
	strs := strings.Split(eventKey, constant.KeySeparator)
	if len(strs) != ValidNodePathCount {
		return false
	}
	if strs[6] != "0" || strs[8] != "function-task" {
		return false
	}
	return true
}
