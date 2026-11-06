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

// Package list is list cmd of CLI
package list

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"

	"functioncore/pkg/cli/cmd/list/functions"
	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/function"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/cli/utils/colorprint"
)

type options struct {
	CmdIO    *cmdio.CmdIO
	client   client.Client
	tenantID string
}

var opts = &options{}

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "used to list services",
	Long: fmt.Sprintf(`run the "%s list" command to list services on %s service project.`,
		constant.CliName, constant.PlatformName),
	Example: utils.RawFormat(fmt.Sprintf(`
    List all services:
    $ %s list 
	`, constant.CliName)),
	Args: utils.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if err := listService(opts); err != nil {
			colorprint.PrintFail(opts.CmdIO.ErrOut, "Failed to get services, ", err.Error(), ".\n")
		}
	},
}

// InitCMD init cmd of list
func InitCMD(c client.Client, cio *cmdio.CmdIO) *cobra.Command {
	opts.CmdIO = cio
	opts.client = c
	opts.tenantID = config.FConfig.CLIConfig.TenantID
	listCmd.AddCommand(functions.NewCmdListFunctions(c, cio))
	return listCmd
}

func listService(opts *options) error {
	services, err := function.QueryServices(opts.client, opts.tenantID)
	if err != nil {
		return err
	}

	nums := len(services)
	if nums > 0 {
		sort.Strings(services)
		colorMap := []colorprint.StringColorInfo{
			{Str: fmt.Sprintf("service list[%d]:\n", nums), Color: colorprint.Colorless},
			{Str: fmt.Sprintf("%s\n", services), Color: colorprint.KeywordColor},
		}
		colorprint.PrintKeywords(opts.CmdIO.Out, colorMap)
	} else {
		fmt.Fprintf(opts.CmdIO.Out, "service list: null\n")
	}

	return nil
}
