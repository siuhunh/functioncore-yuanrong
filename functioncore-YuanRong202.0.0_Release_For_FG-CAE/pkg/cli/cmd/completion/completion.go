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

// Package completion is cmd of CLI
package completion

import (
	"fmt"

	"github.com/spf13/cobra"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/cmdio"
)

var completionCmd = &cobra.Command{
	Use:   "completion [bash|powershell]",
	Short: "Generate completion script",
	Long: fmt.Sprintf(`To load completions:

Bash:

# To load completions for each session, execute once:
Linux:
  $ %s completion bash > ~/.bash_profile

Powershell:

# To load completions for each session, execute once:
Windows:
  $ %s completion powershell | Out-String | Invoke-Expression

Powershell version 7.1 or later is recommended.
`, constant.CliName, constant.CliName),
	DisableFlagsInUseLine: true,
	ValidArgs:             []string{"bash", "powershell"},
	Args:                  cobra.ExactValidArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return cmd.Root().GenBashCompletion(opts.cmdIO.Out)
		case "powershell":
			return cmd.Root().GenPowerShellCompletion(opts.cmdIO.Out)
		default:
			return fmt.Errorf("unsupported shell type %s", args[0])
		}
	},
}

type options struct {
	cmdIO *cmdio.CmdIO
}

var opts = &options{}

// InitCmd cmd for completion
func InitCmd(cio *cmdio.CmdIO) *cobra.Command {
	opts.cmdIO = cio
	return completionCmd
}
