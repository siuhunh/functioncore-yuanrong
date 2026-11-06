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

// Package snerror is basic information contained in the SN error.
package snerror

import (
	"fmt"
)

const (
	// UserErrorMax is maximum value of user error
	UserErrorMax = 10000
	// ErrorSeparator split error codes and error information.
	ErrorSeparator = "|"
)

// SNError defines the action contained in the SN error information.
type SNError interface {
	// Code Returned error code
	Code() int

	Error() string
}

type snError struct {
	code    int
	message string
}

// New returns an error.
// message is a complete English sentence with punctuation.
func New(code int, message string) SNError {
	return &snError{
		code:    code,
		message: message,
	}
}

// NewWithFmtMsg The message can contain placeholders.
func NewWithFmtMsg(code int, fmtMessage string, paras ...interface{}) SNError {
	return &snError{
		code:    code,
		message: fmt.Sprintf(fmtMessage, paras...),
	}
}

// NewWithError err not nil.
func NewWithError(code int, err error) SNError {
	var message = ""
	if err != nil {
		message = err.Error()
	}
	return &snError{
		code:    code,
		message: message,
	}
}

// Code Returned error code
func (s *snError) Code() int {
	return s.code
}

// Error Implement the native error interface.
func (s *snError) Error() string {
	return s.message
}

// IsUserError true if a user error occurs
func IsUserError(s SNError) bool {
	// The user error is a four-digit integer.
	if s.Code() < UserErrorMax && s.Code() != ExtensionExecFailed {
		return true
	}
	return false
}

// NeedRetryInCluster -
func NeedRetryInCluster(s SNError) bool {
	if s.Code() == WorkerExitErrCode || s.Code() == UserFuncIsUpdatedCode {
		return true
	}
	return false
}

// IsOverloadError true if a overload error occurs
func IsOverloadError(s SNError) bool {
	if s.Code() == ClusterOverloadCode || s.Code() == VIPClusterOverloadCode || s.Code() == ReachMaxInstancesCode ||
		s.Code() == ReachMaxOnDemandInstancesPerTenant {
		return true
	}
	return false
}

// IsOOMErrorCode -
func IsOOMErrorCode(err error) bool {
	if err != nil {
		sErr, ok := err.(SNError)
		if ok && sErr.Code() == RuntimeMemoryExceedLimit {
			return true
		}
	}
	return false
}

// NewSNError -
func NewSNError(code int, msg string) SNError {
	return New(code, ErrText(code)+", error: "+msg)
}
