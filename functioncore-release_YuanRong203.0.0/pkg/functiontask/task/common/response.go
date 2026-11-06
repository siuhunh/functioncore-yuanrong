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

package common

// Response represents the invoke response.
type Response struct {
	Code        int32  `json:"code"`
	Message     string `json:"message"`
	Result      Result `json:"result"`
	StateID     string `json:"stateID"`
	State       string `json:"state"`
	Log         string `json:"log"`
	DcCallStack []byte `json:"dcCallStack"`
}

// Success returns a successful response.
func Success(result Result, stateID string, state string) *Response {
	return &Response{
		Code:    EOK,
		Result:  result,
		StateID: stateID,
		State:   state,
	}
}

// SystematicError returns a systematic error.
func SystematicError(code int32, message string) *Response {
	return &Response{
		Code:    code,
		Message: message,
	}
}

// UserCodeError returns a panic error.
func UserCodeError(message string, state string) *Response {
	return &Response{
		Code:    EPanic,
		Message: message,
		State:   state,
	}
}
