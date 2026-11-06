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

import (
	"fmt"
	"io"
	"sync"
	"syscall"
	"unsafe"
)

type coord struct {
	X int16
	Y int16
}

type smallRect struct {
	Left   int16
	Top    int16
	Right  int16
	Bottom int16
}

type terminalBufferInfo struct {
	DWSize              coord
	DWCursorPosition    coord
	WAttributes         uint16
	SRWindow            smallRect
	DWMaximumWindowSize coord
}

var (
	kernel32          = syscall.NewLazyDLL(`kernel32.dll`)
	proc              = kernel32.NewProc(`SetConsoleTextAttribute`)
	defaultScreenInfo terminalBufferInfo
	defaultColor      ColorType
	backgroundShift   = 4
	defaultConfigLoad sync.Once
)

func getDefaultConfig() {
	defaultConfigLoad.Do(func() {
		procGetStdHandle := kernel32.NewProc("GetStdHandle")
		hStdout, _, _ := procGetStdHandle.Call(uintptr(-11 & 0xFFFFFFFF))
		procGetConsoleScreenBufferInfo := kernel32.NewProc("GetConsoleScreenBufferInfo")
		if ret, _, _ := procGetConsoleScreenBufferInfo.Call(hStdout,
			uintptr(unsafe.Pointer(&defaultScreenInfo))); ret == 0 {
			return
		}
		frontColor := defaultScreenInfo.WAttributes & frontMask
		backgroundColor := (defaultScreenInfo.WAttributes & backgroundMask) >> backgroundShift
		defaultColor.FontColor, defaultColor.FontLight = changeToColorType(int(frontColor) + 1)
		defaultColor.BackgroundColor, defaultColor.BackgroundLight = changeToColorType(int(backgroundColor) + 1)
	})
}

func printColorString(w io.Writer, str string, color ColorType) {
	getDefaultConfig()
	if color == Colorless {
		fmt.Fprint(w, str)
		return
	}
	colorCode := getColorCode(color)
	proc.Call(uintptr(syscall.Stdout), uintptr(colorCode))
	fmt.Fprint(w, str)
	proc.Call(uintptr(syscall.Stdout), uintptr(defaultScreenInfo.WAttributes))
}

func changeToColorType(color int) (int, bool) {
	_, ok := lightColor[color]
	if ok {
		return color, false
	}
	for colorKey, value := range lightColor {
		if value == color {
			return colorKey, true
		}
	}
	return Black, false
}

func getColorCode(color ColorType) uint16 {
	needPrintColor := color
	var fontCode int
	var backgroundCode int

	if color.FontColor == None {
		needPrintColor.FontColor = defaultColor.FontColor
		needPrintColor.FontLight = defaultColor.FontLight
	}
	if color.BackgroundColor == None {
		needPrintColor.BackgroundColor = defaultColor.BackgroundColor
		needPrintColor.BackgroundLight = defaultColor.BackgroundLight
	}

	if needPrintColor.FontLight {
		fontCode = lightColor[needPrintColor.FontColor]
	} else {
		fontCode = needPrintColor.FontColor
	}
	if needPrintColor.BackgroundLight {
		backgroundCode = lightColor[needPrintColor.BackgroundColor]
	} else {
		backgroundCode = needPrintColor.BackgroundColor
	}
	colorCode := ((backgroundCode - 1) << backgroundShift) | (fontCode - 1)
	return uint16(colorCode)
}

func getPrintString(str string, color ColorType) string {
	return str
}
