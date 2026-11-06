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

// ColorType Font color setting structure
type ColorType struct {
	FontColor       int
	FontLight       bool
	BackgroundColor int
	BackgroundLight bool
}

// StringColorInfo Keyword color
type StringColorInfo struct {
	Str   string
	Color ColorType
}

// PrintStringConfig Keywords printing structure
type PrintStringConfig struct {
	ColorConfig map[string]ColorType
}

var defaultColorConfig = map[string]ColorType{
	"success":   {FontColor: Green},
	"succeed":   {FontColor: Green},
	"succeeded": {FontColor: Green},
	"Succeeded": {FontColor: Green},
	"failed":    {FontColor: Red},
	"fail":      {FontColor: Red},
}

// Colorless Default Color
var Colorless = ColorType{
	FontColor:       None,
	FontLight:       false,
	BackgroundColor: None,
	BackgroundLight: false,
}

// SuccessColor Success Statement Color
var SuccessColor = ColorType{
	FontColor:       Green,
	FontLight:       false,
	BackgroundColor: None,
	BackgroundLight: false,
}

// ErrorColor Error Statement Color
var ErrorColor = ColorType{
	FontColor:       Red,
	FontLight:       false,
	BackgroundColor: None,
	BackgroundLight: false,
}

// InteractiveColor Interactive Statement Color
var InteractiveColor = ColorType{
	FontColor:       Yellow,
	FontLight:       true,
	BackgroundColor: None,
	BackgroundLight: false,
}

// KeywordColor Keyword Statement Color
var KeywordColor = ColorType{
	FontColor:       Cyan,
	FontLight:       false,
	BackgroundColor: None,
	BackgroundLight: false,
}
