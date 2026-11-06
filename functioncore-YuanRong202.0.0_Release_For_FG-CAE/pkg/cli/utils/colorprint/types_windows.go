//go:build windows
// +build windows

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
	None int = iota
	// Black color
	Black
	// Blue color
	Blue
	// Green color
	Green
	// Cyan color
	Cyan
	// Red color
	Red
	// Purple color
	Purple
	// Yellow color
	Yellow
	lightGray
	// Gray color
	Gray
	lightBlue
	lightGreen
	lightCyan
	lightRed
	lightPurple
	lightYellow
	// White color
	White
)

const (
	frontMask      = 0b00001111
	backgroundMask = 0b11110000
)

var lightColor = map[int]int{
	Black:  None,
	Blue:   lightBlue,
	Green:  lightGreen,
	Cyan:   lightCyan,
	Red:    lightRed,
	Purple: lightPurple,
	Yellow: lightYellow,
	Gray:   lightGray,
	White:  None,
}
