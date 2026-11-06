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

// Package statuscode define status code of Frontend
package statuscode

import (
	fhttp "github.com/valyala/fasthttp"
)

const (
	// Ok ok code
	Ok = 200200
	// Accepted -
	Accepted = 200202
	// NoContent -
	NoContent = 200204

	// BadRequest -
	BadRequest = 200400
	// UnAuthorized -
	UnAuthorized = 200401
	// Forbidden -
	Forbidden = 200403
	// NotFound -
	NotFound = 200404
	// RequestEntityTooLarge -
	RequestEntityTooLarge = 200413
	// LabelUnavailable -
	LabelUnavailable = 200422
	// TooManyRequests -
	TooManyRequests = 200429
	// FuncRequestExceedLimit -
	FuncRequestExceedLimit = 200430

	// InternalError -
	InternalError = 200500

	// TrafficLimitEffective is the error code for traffic limitation
	TrafficLimitEffective = 4021

	// RetryError retried failed for system error, need not retry anymore
	RetryError = 110600
	// TaskIsUpdatingCode -
	TaskIsUpdatingCode = 211410

	// HeavyLoadCode indicate the server's memory usage reaches threshold
	HeavyLoadCode = 214503
)

// FrontendStatusCode frontend status code type
type FrontendStatusCode int

// HTTPStatusCode http status code type
type HTTPStatusCode int

var (
	// ErrMap frontend code map to http code
	ErrMap = map[int]int{
		Ok:        200,
		Accepted:  202,
		NoContent: 204,

		BadRequest:            400,
		UnAuthorized:          401,
		Forbidden:             403,
		NotFound:              404,
		RequestEntityTooLarge: 413,
		TooManyRequests:       429,

		InternalError: 500,
		HeavyLoadCode: 500,
	}
)

// Code trans frontend code to http code
func Code(frontendCode int) int {
	return ErrMap[frontendCode]
}

// Message trans frontend code to message
func Message(frontendCode int) string {
	httpCode, exist := ErrMap[frontendCode]
	if !exist {
		return ""
	}

	return fhttp.StatusMessage(int(httpCode))
}
