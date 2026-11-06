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

// Package get is get cmd of CLI
package get

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/spf13/cobra"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/function"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/cli/utils/colorprint"
)

// Options get function info options
type Options struct {
	CmdIO        *cmdio.CmdIO
	client       client.Client
	tenantID     string
	ServiceName  string
	FunctionName string
	Version      string
}

var opts = &Options{}

var cmd = &cobra.Command{
	Use:   "get FUNCTION",
	Short: "used to display function info",
	Long:  fmt.Sprintf(`run the "%s get" command to display function info.`, constant.CliName),
	Example: utils.RawFormat(fmt.Sprintf(`
    Display helloWorld function information.
    $ %s get helloWorld
	`, constant.CliName)),
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		opts.FunctionName = args[0]
		functionInfo, err := function.Query(opts.client, opts.tenantID,
			opts.ServiceName, opts.FunctionName, opts.Version)
		if err != nil {
			colorprint.PrintFail(opts.CmdIO.ErrOut,
				fmt.Sprintf("Failed to get info about the function %s, ", opts.FunctionName),
				err.Error(), ".\n")
			return
		}
		if err := DisplayFunctionInfo(opts.CmdIO.Out, functionInfo); err != nil {
			colorprint.PrintFail(opts.CmdIO.ErrOut,
				fmt.Sprintf("Failed to display info about the function %s, ", opts.FunctionName),
				err.Error(), ".\n")
		}
	},
}

// NewCmdGetFunction cmd for getting functions
func NewCmdGetFunction(c client.Client, cio *cmdio.CmdIO) *cobra.Command {
	opts.CmdIO = cio
	opts.client = c
	opts.tenantID = config.FConfig.CLIConfig.TenantID

	cmd.Flags().StringVarP(&opts.ServiceName, "serviceName", "s", constant.DefaultService, constant.ServiceNameUsage)
	cmd.Flags().StringVarP(&opts.Version, "version", "v", constant.LatestVersion, constant.FunctionVersionUsage)

	return cmd
}

// DisplayFunctionInfo display function information
func DisplayFunctionInfo(out io.Writer, functionInfo *function.Function) error {
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetIndent("", "    ")
	jsonEncoder.SetEscapeHTML(false)
	if err := jsonEncoder.Encode(functionInfo); err != nil {
		return err
	}
	fmt.Fprintf(out, "Function info:\n")
	colorprint.NewColorConfig().
		AddColorConfig(functionInfo.Name, colorprint.KeywordColor).
		AddColorConfig(functionInfo.ID, colorprint.KeywordColor).
		AddColorConfig(functionInfo.Runtime, colorprint.KeywordColor).
		AddColorConfig(functionInfo.DownloadURL, colorprint.KeywordColor).
		PrintStringUsingColorConfig(out, bf.String())
	return nil
}
