/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package workertypes define common types of worker
package workertypes

// TimeWindow -
type TimeWindow struct {
	StartTime       int64  `json:"startTime"`       // unit: ms
	EndTime         int64  `json:"endTime"`         // unit: ms
	BillingDuration int    `json:"billingDuration"` // Charging time (unit: ms)
	BusyDuration    int    `json:"busyDuration"`    // instance busy time (unit: ms)
	IdleDuration    int    `json:"idleDuration"`    // instance idle time (unit: ms)
	Stage           string `json:"stage"`           // invoke or
}
