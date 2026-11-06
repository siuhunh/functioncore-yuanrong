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
	"fmt"
	"regexp"
)

const (
	requestIDRegex     = "^[a-zA-Z0-9-]+$"
	requestIDMaxLength = 128
	stateKeyRegex      = "^[a-zA-Z0-9]+$"
	stateKeyMaxLength  = 128
)

// CheckRequestID check requestID
func CheckRequestID(requestID string) error {
	if len(requestID) > requestIDMaxLength {
		return fmt.Errorf("request ID contains more than %d characters", requestIDMaxLength)
	}
	isMatch, err := regexp.MatchString(requestIDRegex, requestID)
	if err != nil || !isMatch {
		return errors.New("request ID must be uppercase letters, lowercase letters, digits, and hyphens (-)")
	}
	return nil
}

// CheckStateKey check stateKey
func CheckStateKey(stateKey string) error {
	if len(stateKey) > stateKeyMaxLength {
		return fmt.Errorf("stateKey contains more than %d characters", stateKeyMaxLength)
	}
	isMatch, err := regexp.MatchString(stateKeyRegex, stateKey)
	if err != nil || !isMatch {
		return errors.New("stateKey must be uppercase letters, lowercase letters and digits")
	}
	return nil
}
