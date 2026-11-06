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

// Package common defines Error struct for function task.
package common

// TaskError represents error info in whole task calling.
type TaskError interface {
	Code() int32
	Error() string
}

type taskError struct {
	code    int32
	message string
}

// Code return code
func (e taskError) Code() int32 {
	return e.code
}

// Error return error message.
func (e taskError) Error() string {
	return e.message
}

// NewTaskError new TaskError with code and message
func NewTaskError(code int32, message string) TaskError {
	return &taskError{code: code, message: message}
}
