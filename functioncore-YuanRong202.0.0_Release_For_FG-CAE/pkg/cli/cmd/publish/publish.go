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

// Package publish -
package publish

import (
	"fmt"

	"github.com/spf13/cobra"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/function"
	"functioncore/pkg/cli/utils"
	"functioncore/pkg/cli/utils/colorprint"
)

type options struct {
	cmdIO        *cmdio.CmdIO
	client       client.Client
	serviceName  string
	functionName string
	version      string
	tenantID     string
}

var (
	opts = &options{}
)

var cmd = &cobra.Command{
	Use:   "publish FUNCTION",
	Short: "used to publish function",
	Long: fmt.Sprintf(`run the "%s publish" command to publish a version of a function.`,
		constant.CliName),
	Example: utils.RawFormat(fmt.Sprintf(`
    publish v1 version of the function:
    $ %s publish functionname -v v1

    publish v1 version of the function:
    $ %s publish functionname -s serviceName -v v1
	`, constant.CliName, constant.CliName)),
	Args: cobra.ExactArgs(1),
	Run:  publish,
}

// InitPublishCMD init cmd of publish
func InitPublishCMD(c client.Client, cio *cmdio.CmdIO) *cobra.Command {
	opts.cmdIO = cio
	opts.client = c
	opts.tenantID = config.FConfig.CLIConfig.TenantID

	cmd.Flags().StringVarP(&opts.serviceName, "serviceName", "s", constant.DefaultService,
		"serviceName")
	cmd.Flags().StringVarP(&opts.version, "version", "v", "",
		"the version of the function to publish")
	return cmd
}

func publish(cmd *cobra.Command, args []string) {
	functionName := args[0]
	serviceName := opts.serviceName
	version := opts.version
	err := function.PublishVersion(opts.client, functionName, serviceName, version, opts.tenantID)
	if err != nil {
		colorprint.PrintFail(opts.cmdIO.ErrOut,
			fmt.Sprintf("Failed to publish the function %s, ", functionName),
			err.Error(), ".\n")
		return
	}
	colorprint.PrintSuccess(opts.cmdIO.Out, "Succeeded",
		fmt.Sprintf(" to publish the function %s!", functionName))
}
