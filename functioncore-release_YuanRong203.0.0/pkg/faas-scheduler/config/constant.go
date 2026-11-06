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

// Package config contains scheduler config
package config

const (
	// WatchPathPrefixWithFunction is a function path perfix
	WatchPathPrefixWithFunction = "/sn/functions/business/yrk/tenant"
	// SilentWatchPathPrefixWithFunction -
	SilentWatchPathPrefixWithFunction = "/silent/sn/functions/business/yrk/tenant"
	// WatchPathPrefixWithWorkers is a workers path prefix
	WatchPathPrefixWithWorkers = "/sn/workers/business/yrk/tenant"
	// ConfigPath -
	ConfigPath = "/home/sn/config/conf.json"

	defaultServicePort        = "8889"
	defaultHealthCheckPort    = "9994"
	defaultLeaseCheckInterval = 2000
	defaultLeaseSpan          = 60000

	// FaaSScheduler -
	FaaSScheduler = "FaaSScheduler"
)
