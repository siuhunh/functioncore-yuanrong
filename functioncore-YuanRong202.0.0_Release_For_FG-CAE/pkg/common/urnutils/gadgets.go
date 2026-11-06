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

package urnutils

import (
	"strings"
)

// Separator is the current system special
var Separator = "-"

const (
	// ServiceIDPrefix is the prefix of the function with serviceID.
	ServiceIDPrefix = "0"

	// DefaultSeparator is a character that separates functions and services.
	DefaultSeparator = "-"

	// ServicePrefix is the prefix of the function with serviceID.
	ServicePrefix = "0@"

	// TenantProductSplitStr separator between a tenant and a product
	TenantProductSplitStr = "@"

	minEleSize = 3
)

// ComplexFuncName contains service ID and raw function name
type ComplexFuncName struct {
	prefix    string
	ServiceID string
	FuncName  string
}

// NewComplexFuncName -
func NewComplexFuncName(svcID, funcName string) *ComplexFuncName {
	return &ComplexFuncName{
		prefix:    ServiceIDPrefix,
		ServiceID: svcID,
		FuncName:  funcName,
	}
}

// IsComplexFuncName -
func IsComplexFuncName(funcName string) bool {
	return strings.Contains(funcName, Separator)
}

// ParseFrom parse ComplexFuncName from string
func (c *ComplexFuncName) ParseFrom(name string) *ComplexFuncName {
	fields := strings.Split(name, Separator)
	if len(fields) < minEleSize || fields[0] != ServiceIDPrefix {
		c.prefix = ""
		c.ServiceID = ""
		c.FuncName = name
		return c
	}
	idx := 0
	c.prefix = fields[idx]
	idx++
	c.ServiceID = fields[idx]
	// $prefix$separator$ServiceID$separator$FuncName equals name
	c.FuncName = name[(len(c.prefix) + len(Separator) + len(c.ServiceID) + len(Separator)):]
	return c
}

// String -
func (c *ComplexFuncName) String() string {
	return strings.Join([]string{c.prefix, c.ServiceID, c.FuncName}, Separator)
}

// GetSvcIDWithPrefix get serviceID with prefix from function name
func (c *ComplexFuncName) GetSvcIDWithPrefix() string {
	return c.prefix + Separator + c.ServiceID
}

// SetSeparator -
func SetSeparator(separator string) {
	if separator != "" {
		Separator = separator
	}
}
