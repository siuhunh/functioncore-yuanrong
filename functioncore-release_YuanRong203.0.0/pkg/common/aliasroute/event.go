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

package aliasroute

import (
	"encoding/json"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
)

// ProcessAliasEvent handles alias event from etcd
func ProcessAliasEvent(event *etcd3.Event) error {
	switch event.Type {
	case etcd3.PUT:
		alias := &Alias{}
		err := json.Unmarshal(event.Value, alias)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal alias event, err: %s", err.Error())
			return err
		}

		AliasRoute.AddAlias(alias)
	case etcd3.DELETE:
		// not try to get object alias from event.Value when type of event is Delete
		// build an alias URN from event.Key
		aliasURN := BuildURNFromAliasKey(event.Key)
		AliasRoute.RemoveAlias(aliasURN)
	case etcd3.ERROR:
		log.GetLogger().Warnf("etcd error event: %s", event.Value)
	default:
		log.GetLogger().Warnf("unsupported event: %s", event.Value)
	}
	return nil
}
