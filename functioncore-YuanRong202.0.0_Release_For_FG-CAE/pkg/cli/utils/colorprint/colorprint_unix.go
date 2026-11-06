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

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

func printColorString(w io.Writer, str string, color ColorType) {
	if color == Colorless {
		fmt.Fprint(w, str)
		return
	}
	result := getPrintString(str, color)
	fmt.Fprint(w, result)
}

func getColorCode(color ColorType) string {
	var fontCode int
	var backgroundCode int
	color = checkColor(color)
	var colorCode strings.Builder
	if color.FontColor == None && color.BackgroundColor == None {
		return "0"
	}
	if color.FontColor != None {
		fontCode = color.FontColor - 1 + frontMask
		if color.FontLight {
			fontCode += light
		}
		colorCode.WriteString(strconv.Itoa(fontCode))
	}

	if color.BackgroundColor != None {
		backgroundCode = color.BackgroundColor - 1 + backgroundMask
		if color.BackgroundLight {
			backgroundCode += light
		}
		if colorCode.Len() != 0 {
			colorCode.WriteString(";")
		}
		colorCode.WriteString(strconv.Itoa(backgroundCode))
	}
	return colorCode.String()
}

func checkColor(color ColorType) ColorType {
	// linux does not support gray
	if color.FontColor == Gray {
		color.FontColor = None
	}
	if color.BackgroundColor == Gray {
		color.BackgroundColor = None
	}
	return color
}

func getPrintString(str string, color ColorType) string {
	if color == Colorless {
		return str
	}
	var printString strings.Builder
	colorCode := getColorCode(color)
	printString.WriteString(fmt.Sprintf(controllerMask, colorCode))
	printString.WriteString(str)
	printString.WriteString(defaultConfig)
	return printString.String()
}
