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

// Package common -
package common

const (
	// ApplicationJSONUTF8 -
	ApplicationJSONUTF8 = "application/json;charset=UTF-8"
)

const (
	// HeaderTraceID -
	HeaderTraceID = "x-request-id"
	// HeaderAuthorization -
	HeaderAuthorization = "authorization"
	// HeaderContentType -
	HeaderContentType = "Content-Type"
	// HeaderLog -
	HeaderLog = "x-log-type"
	// HeaderStateKey -
	HeaderStateKey = "X-State-Key"
	// HeaderCPUSize is cpu size specified by invoke
	HeaderCPUSize = "X-Instance-CPU"
	// HeaderMemorySize is cpu memory specified by invoke
	HeaderMemorySize = "X-Instance-Memory"
	// HeaderAuthTimestamp -
	HeaderAuthTimestamp = "X-Timestamp-Auth"
)
