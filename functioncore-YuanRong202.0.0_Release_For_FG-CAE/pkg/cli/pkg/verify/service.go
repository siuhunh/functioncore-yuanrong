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

// Package verify is verification of function params accuracy
package verify

import (
	"errors"
	"regexp"
)

const (
	// ServiceNameRegex ServiceName正则表达式 1-32位数字或字母
	ServiceNameRegex = "^[0-9a-z]{1,32}$"
	// ServiceMaxLength ServiceName 最长32位
	ServiceMaxLength = 32
	// ServiceMinLength ServiceName 最短1位
	ServiceMinLength = 1
)

var (
	// ErrServiceNameFormat service name format error
	ErrServiceNameFormat = errors.New("service name can contain a maximum of 32 characters, " +
		"including lowercase letters and numbers")
	// ErrServiceNameRequired service name is required
	ErrServiceNameRequired = errors.New("service is required and not blank")
)

// MatchServiceName verify ServiceName is match ServiceNameRegex
func MatchServiceName(SNServiceName string) bool {
	isMatch, err := regexp.MatchString(ServiceNameRegex, SNServiceName)
	if err != nil {
		return false
	}
	if len(SNServiceName) < ServiceMinLength || len(SNServiceName) > ServiceMaxLength {
		return false
	}
	return isMatch
}

// CheckServiceName check service name is empty and correct
func CheckServiceName(serviceName string) error {
	if serviceName == "" {
		return ErrServiceNameRequired
	}
	if !MatchServiceName(serviceName) {
		return ErrServiceNameFormat
	}
	return nil
}
