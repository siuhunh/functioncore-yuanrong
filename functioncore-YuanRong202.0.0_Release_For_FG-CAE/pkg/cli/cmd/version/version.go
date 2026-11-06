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

// Package version is version cmd of CLI
package version

import (
	"github.com/spf13/cobra"

	"functioncore/pkg/cli/build"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/cli/utils/colorprint"
)

type options struct {
	cmdIO *cmdio.CmdIO
}

var opts = &options{}

var cmd = &cobra.Command{
	Use:   "version",
	Short: "print the cli's version information",
	Long:  `print the cli's version information`,
	Args:  utils.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		colorMap := []colorprint.StringColorInfo{
			{Str: "CLI version: ", Color: colorprint.Colorless},
			{Str: build.Version, Color: colorprint.KeywordColor},
			{Str: ", build time: ", Color: colorprint.Colorless},
			{Str: build.Date, Color: colorprint.KeywordColor},
			{Str: ".\n", Color: colorprint.Colorless},
		}
		colorprint.PrintKeywords(opts.cmdIO.Out, colorMap)
	},
}

// InitVersionCMD init cmd for version
func InitVersionCMD(cio *cmdio.CmdIO) *cobra.Command {
	opts.cmdIO = cio
	return cmd
}
