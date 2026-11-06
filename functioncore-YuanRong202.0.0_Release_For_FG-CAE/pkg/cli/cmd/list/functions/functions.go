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

// Package functions is list functions cmd of CLI
package functions

import (
	"errors"
	"fmt"
	"sort"

	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/function"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/cli/utils/colorprint"
)

type listFunctionCmdOptions struct {
	cmdIO       *cmdio.CmdIO
	client      client.Client
	serviceName string
	tenantID    string
}

var opts = &listFunctionCmdOptions{}

var cmd = &cobra.Command{
	Use:   "functions",
	Short: "used to get the list of functions.",
	Long: fmt.Sprintf(`run the "%s list functions" command to get the list of functions`,
		constant.CliName),
	Example: utils.RawFormat(fmt.Sprintf(`
	Get list of deployed functions of the default service:
	$ %s list functions

	Get list of deployed functions by specifying service name:
	$ %s list functions -s serviceName
	`, constant.CliName, constant.CliName)),
	Args: utils.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		if err := listFunctions(opts); err != nil {
			colorprint.PrintFail(opts.cmdIO.ErrOut,
				fmt.Sprintf("Failed to list deployed functions of the service(%s), ", opts.serviceName),
				err.Error(), ".\n")
		}
	},
}

// NewCmdListFunctions cmd for list functions
func NewCmdListFunctions(c client.Client, cio *cmdio.CmdIO) *cobra.Command {
	opts.cmdIO = cio
	opts.client = c
	opts.tenantID = config.FConfig.CLIConfig.TenantID

	cmd.Flags().StringVarP(&opts.serviceName, "serviceName", "s", constant.DefaultService, constant.ServiceNameUsage)
	return cmd
}

func listFunctions(opts *listFunctionCmdOptions) error {
	functionWithVersionsMap, err := function.QueryFunctionWithVersions(opts.client, opts.tenantID, opts.serviceName)
	if err != nil {
		return err
	}
	if len(functionWithVersionsMap) == 0 {
		return errors.New("no deployed functions")
	}
	functions := make([]string, 0, len(functionWithVersionsMap))
	for k := range functionWithVersionsMap {
		functions = append(functions, k)
	}
	sort.Strings(functions)
	table := utils.NewTable(opts.cmdIO.Out, true, true, true)
	table.SetAlignment(tablewriter.ALIGN_CENTER)
	table.SetAutoMergeCellsByColumnIndex([]int{0, 1})
	table.SetHeader([]string{"Service", "FunctionName", "Version"})
	for _, functionName := range functions {
		versions := functionWithVersionsMap[functionName]
		bulk := getFunctionBulk(opts, functionName, versions)
		table.AppendBulk(bulk)
	}
	table.Render()

	return nil
}

func getFunctionBulk(opts *listFunctionCmdOptions, functionName string, versions []string) [][]string {
	var functionBulk [][]string
	for _, functionVersion := range versions {
		functionBulk = append(functionBulk, []string{opts.serviceName, functionName,
			functionVersion})
	}
	return functionBulk
}
