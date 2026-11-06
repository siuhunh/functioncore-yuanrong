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
	"strings"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/trafficlimit"
)

const (
	// QOSEtcdPrefix is the etcd prefix for tenant qos limit
	QOSEtcdPrefix = "/sn/qos/business/yrk"
	// ValidTenantPathCount is the separator count of tenant Path
	ValidTenantPathCount = 7
	// tenantMark is mark of etcd of tenant
	tenantMark = "tenant"
)

func processTenantEvent(event *etcd3.Event) error {
	var err error
	switch event.Type {
	case etcd3.PUT:
		if err = trafficlimit.TenantBuf.ProcessUpdate(event); err != nil {
			return err
		}
	case etcd3.DELETE:
		trafficlimit.TenantBuf.ProcessDelete(event)
	case etcd3.ERROR:
		log.GetLogger().Warnf("etcd error event: %s", event.Value)
	default:
		log.GetLogger().Warnf("unsupported event: %s", event.Value)
	}
	return nil
}

// IsTenantQOS - key: /sn/qos/business/yrk/tenant/<tenantid>
func IsTenantQOS(key string) bool {
	strs := strings.Split(key, constant.KeySeparator)
	if len(strs) != ValidTenantPathCount {
		return false
	}
	if strs[5] != tenantMark {
		return false
	}

	return true
}
