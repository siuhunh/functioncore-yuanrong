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
	"functioncore/pkg/common/aliasroute"
	"functioncore/pkg/common/etcd3"
)

const (
	// AliasEtcdPrefix is the etcd prefix for alias
	AliasEtcdPrefix = "/sn/aliases"
)

func processAliasEvent(event *etcd3.Event) error {
	return aliasroute.ProcessAliasEvent(event)
}
