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

package instance

import "functioncore/pkg/frontend-invoke/loadbalancer/common"

const (
	disabled = 1
)

// notify notify disabled processor
type notify func(p *common.Processor)

type retainer interface {
	addProcessor(p *common.Processor)
	deleteProcessor(p *common.Processor)
	retainLease()
}

func createRetainer(funcKeyWithRes string, instanceKey string, notifier notify) retainer {
	return createDefaultRetainer(funcKeyWithRes, instanceKey, notifier)
}
