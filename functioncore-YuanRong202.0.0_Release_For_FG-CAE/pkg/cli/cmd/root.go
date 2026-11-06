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

package cmd

import (
	"github.com/spf13/cobra"

	"functioncore/pkg/cli/cmd/completion"
	"functioncore/pkg/cli/cmd/create"
	del "functioncore/pkg/cli/cmd/delete"
	"functioncore/pkg/cli/cmd/deploy"
	"functioncore/pkg/cli/cmd/get"
	"functioncore/pkg/cli/cmd/invoke"
	"functioncore/pkg/cli/cmd/list"
	"functioncore/pkg/cli/cmd/publish"
	"functioncore/pkg/cli/cmd/version"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
)

// NewCmdRoot new root cmd
func NewCmdRoot() (*cobra.Command, error) {
	cmd := &cobra.Command{
		Use:   constant.CliName,
		Short: "",
		Long:  "",
	}

	c, err := client.Init()
	if err != nil {
		return nil, err
	}
	cio := cmdio.System()

	cmd.AddCommand(invoke.InitInvokeCMD(c, cio))
	cmd.AddCommand(deploy.InitDeployCMD(c, cio))
	cmd.AddCommand(create.InitCreateCMD(cio))
	cmd.AddCommand(publish.InitPublishCMD(c, cio))
	cmd.AddCommand(del.InitCMD(c, cio))
	cmd.AddCommand(list.InitCMD(c, cio))
	cmd.AddCommand(get.NewCmdGetFunction(c, cio))
	cmd.AddCommand(completion.InitCmd(cio))
	cmd.AddCommand(version.InitVersionCMD(cio))

	return cmd, nil
}
