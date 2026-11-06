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

// Package test is used for unit tests
package test

// Test -
type Test struct {
	Test Info `yaml:"test"`
}

// Info -
type Info struct {
	Service   string     `yaml:"service,omitempty"`
	Functions []Function `yaml:"functions,omitempty"`
}

// Function -
type Function struct {
	Name               string `yaml:"name,omitempty"`
	Qualifier          string `yaml:"qualifier,omitempty"`
	Event              string `yaml:"event,omitempty"`
	ExpectResult       string `yaml:"expectResult,omitempty"`
	ExpectErrorCode    int    `yaml:"expectErrorCode,omitempty"`
	ExpectErrorMessage string `yaml:"expectErrorMessage,omitempty"`
}
