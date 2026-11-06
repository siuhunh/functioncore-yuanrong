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

// Package constant -
package constant

// Build input parameter variables
var (
	// CliName cli tool name
	CliName = "yrctl"
	// PlatformName service platform name
	PlatformName = "FunctionGraph"
	// ServiceNameUsage service name
	ServiceNameUsage = PlatformName + " service name"
)

const (
	// ZipSuffix -
	ZipSuffix = ".zip"
	// ServicePKG -
	ServicePKG = "pkg"
	// Service -
	Service = "function"
	// Function -
	Function = "function"
	// EmptyFunction -
	EmptyFunction = "empty"
	// Interactive -
	Interactive = true
	// NonInteractive -
	NonInteractive = false
	// Java -
	Java = "java"
	// Java8 -
	Java8 = "java8"
	// Java11 -
	Java11 = "java11"
	// DefaultService -
	DefaultService = "default"
	// LatestVersion -
	LatestVersion = "latest"
	// DefaultMaxInstance -
	DefaultMaxInstance = 100
	// MaxMaxInstance -
	MaxMaxInstance = 1000
	// MaxConcurrentNum -
	MaxConcurrentNum = 100
	// FunctionYaml -
	FunctionYaml = "function.yaml"
)

const (
	// FunctionVersionUsage -
	FunctionVersionUsage = "function version"
)
