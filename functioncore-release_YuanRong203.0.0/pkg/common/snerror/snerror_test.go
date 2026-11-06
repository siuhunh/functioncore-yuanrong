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
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewError
func TestNewError(t *testing.T) {

	e := New(InternalError, InternalErrorMsg)
	fmt.Println(e.Code())
	assert.Equal(t, InternalError, e.Code())

	e = New(InternalErrorRetry, InternalErrorMsg)
	fmt.Println(e.Code())
	assert.Equal(t, InternalErrorRetry, e.Code())

	e = NewWithFmtMsg(InternalError, "test %s,%d", "test", 1)
	fmt.Println(e.Code())
	assert.Equal(t, InternalError, e.Code())

	r := ConvertError(e)
	assert.Equal(t, InternalError, r.Code)

	e = NewWithError(10086, errors.New("aaa"))
	fmt.Println(e.Code())
	assert.Equal(t, 10086, e.Code())
	assert.Equal(t, false, IsUserError(e))
}

// TestError
func TestError(t *testing.T) {
	err := errors.New("aaa")
	fmt.Println(err.Error())
	e := NewWithError(10086, errors.New("aaa"))
	fmt.Println(e.Code())
	fmt.Println(e.Error())
	assert.Equal(t, 10086, e.Code())

	e1 := getErr()
	fmt.Println(e1.Error())

	e2 := getErr().(SNError)
	fmt.Println(e2.Error())
	fmt.Println(e2.Code())
	assert.Equal(t, 10086, e.Code())
}

func getErr() error {
	return NewWithError(10086, errors.New("aaa"))
}

// TestMarshal
func TestMarshal(t *testing.T) {
	e := NewWithError(10086, errors.New("aaa"))
	b := Marshal(e)
	var re BadResponse
	json.Unmarshal(b, &re)
	assert.Equal(t, re.Code, 10086)
}

// TestNeedRetryInCluster
func TestNeedRetryInCluster(t *testing.T) {
	err := New(WorkerExitErrCode, "")
	assert.True(t, NeedRetryInCluster(err))
	err = New(InternalError, InternalErrorMsg)
	assert.False(t, NeedRetryInCluster(err))
}

// TestIsOverloadError
func TestIsOverloadError(t *testing.T) {
	err := New(ClusterOverloadCode, ErrText(ClusterOverloadCode))
	assert.True(t, IsOverloadError(err))
	err = New(VIPClusterOverloadCode, ErrText(VIPClusterOverloadCode))
	assert.True(t, IsOverloadError(err))
	err = New(InternalError, InternalErrorMsg)
	assert.False(t, IsOverloadError(err))
}

// TestNewSNError
func TestNewSNError(t *testing.T) {
	err := NewSNError(ClusterOverloadCode, "overload")
	result := "the cluster is overload and unavailable now, error: overload"
	assert.Equal(t, result, err.Error())
}

func TestIsOOMErrorCode(t *testing.T) {
	err := NewSNError(RuntimeMemoryExceedLimit, "test oom")
	assert.Equal(t, true, IsOOMErrorCode(err))

	err = NewSNError(ClusterOverloadCode, "overload")
	assert.Equal(t, false, IsOOMErrorCode(err))
}

func TestIsUserErr(t *testing.T) {
	err := NewSNError(NoSuchInstanceName, "test user")
	assert.Equal(t, true, IsUserErr(uint32(err.Code())))

	err = NewSNError(ClusterOverloadCode, "overload")
	assert.Equal(t, false, IsUserErr(uint32(err.Code())))
}

func TestErrEventMessageText(t *testing.T) {
	errMsg := ErrEventMessageText(RuntimeMemoryExceedLimit)
	assert.Equal(t, errMsg, "memory limit exceeded")

	errMsg = ErrEventMessageText(FunctionEntryNotFound)
	assert.Equal(t, errMsg, "")
}
