//go:build !windows
// +build !windows

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

// Package colorprint Print with color
package colorprint

const (
	// None colorless
	None int = iota
	// Black color
	Black
	// Red color
	Red
	// Green color
	Green
	// Yellow color
	Yellow
	// Blue color
	Blue
	// Purple color
	Purple
	// Cyan color
	Cyan
	// White color
	White
	// Gray color
	Gray
)

const (
	defaultConfig  = "\x1b[0m"
	frontMask      = 30
	backgroundMask = 40
	light          = 60
	controllerMask = "\x1b[%sm"
)
