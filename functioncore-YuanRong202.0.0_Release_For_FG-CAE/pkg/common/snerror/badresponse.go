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
	"fmt"
)

// BadResponse HTTP request message that does not return 200
type BadResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// ConvertError Convert SNError to BadResponse
func ConvertError(error SNError) BadResponse {
	return BadResponse{
		Code:    error.Code(),
		Message: error.Error(),
	}
}

// Marshal Marshal SNError to byte
func Marshal(error SNError) []byte {
	b, e := json.Marshal(ConvertError(error))
	if e != nil {
		return []byte(fmt.Sprintf("marshal snError failed %s.snerror/badresponse:33", e))
	}
	return b
}
