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

// Package statetype stores state types for fsm.
package statetype

// State types for fsm.
// meanings:
//     Success: final result for success.
//     Error: final result for failure.
//     Start: Start dealing request.
//     RecoverFromMemoryFutureStore: recovers the request from the memory future store.
//     RecoverFromExtFutureStore: recovers the request from the external future store.
//     NotRecovered: not recovered, deal with it as a stateless request.
//     Recovered: recovered, return success.
//     SaveRequest: saves request in redis when first time call.
//     BeforeStateful: prepare stage, update the state info.
//     Stateful: creates state for the request.
const (
	Success = iota
	Error
	Start
	RecoverFromMemoryFutureStore
	RecoverFromExtFutureStore
	NotRecovered
	Recovered
	SaveRequest

	BeforeStateful
	Stateful
)
