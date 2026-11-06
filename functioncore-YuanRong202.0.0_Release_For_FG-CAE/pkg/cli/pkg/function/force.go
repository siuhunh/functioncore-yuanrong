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

package function

import (
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/utils/colorprint"
	"functioncore/pkg/cli/utils/terminal"
)

const (
	retryCountLimit = 3
)

// Force force delete or not
func Force(cmdIO *cmdio.CmdIO, force bool, arr []string, frontStr string, deleteAllType bool) bool {
	if force {
		// Forcible deletion
		return true
	}

	if len(arr) == 0 {
		return false
	}
	length := 0
	if deleteAllType {
		colorprint.NewColorConfig().
			AddColorConfig("ALL", colorprint.KeywordColor).
			PrintStringUsingColorConfig(cmdIO.Out, frontStr)
		length = len(frontStr)
	} else {
		colorMap := make([]colorprint.StringColorInfo, 0, len(arr)*2+2)
		colorMap, length = getColorMap(colorMap, arr, frontStr)
		colorprint.PrintKeywords(cmdIO.Out, colorMap)
	}
	line := terminal.ScanLine(cmdIO.In, cmdIO.Out, length)
	retryCount := 0
	for {
		if line == "y" {
			return true
		}
		if line == "n" {
			return false
		}
		if retryCount >= retryCountLimit {
			break
		}
		showStr := "Please type 'y' or 'n': "
		colorprint.PrintInteractive(cmdIO.Out, showStr)
		line = terminal.ScanLine(cmdIO.In, cmdIO.Out, len(showStr))
		retryCount++
	}
	return false
}

func getColorMap(colorMap []colorprint.StringColorInfo, arr []string, frontStr string) (
	[]colorprint.StringColorInfo, int) {
	colorMap = append(colorMap, colorprint.StringColorInfo{Str: frontStr + "[", Color: colorprint.Colorless})
	for i, value := range arr {
		colorMap = append(colorMap, colorprint.StringColorInfo{Str: value, Color: colorprint.KeywordColor})
		if i != len(arr)-1 {
			colorMap = append(colorMap, colorprint.StringColorInfo{Str: "|", Color: colorprint.Colorless})
		}
	}
	colorMap = append(colorMap, colorprint.StringColorInfo{Str: "]? \n", Color: colorprint.Colorless})
	str := "Please input (y/n): "
	colorMap = append(colorMap, colorprint.StringColorInfo{Str: str, Color: colorprint.InteractiveColor})
	return colorMap, len(str)
}
