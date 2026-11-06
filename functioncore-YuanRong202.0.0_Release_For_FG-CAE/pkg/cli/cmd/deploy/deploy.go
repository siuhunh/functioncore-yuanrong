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

// Package deploy -
package deploy

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/constant"
	"functioncore/pkg/cli/pkg/builder"
	"functioncore/pkg/cli/pkg/client"
	"functioncore/pkg/cli/pkg/cmdio"
	"functioncore/pkg/cli/pkg/deploy"
	"functioncore/pkg/cli/pkg/function"
	"functioncore/pkg/cli/pkg/progress"
	"functioncore/pkg/cli/pkg/verify"
	"functioncore/pkg/cli/utils/colorprint"
)

const (
	memoryBase    = 128
	cpuAllocation = 100
	cpuBase       = 200
)

type options struct {
	cmdIO  *cmdio.CmdIO
	client client.Client
}

var opts = &options{}

var deployCmd = &cobra.Command{
	Use:   "deploy",
	Short: "used to deploy function",
	Long:  fmt.Sprintf(`run the deploy command to deploy a function.`),
	Example: fmt.Sprintf(fmt.Sprintf(`
    Deploy a function:
    $ %s deploy
	`, constant.CliName)),
	Args: cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		functionInfo, err := deploy.GetFunctionInfoInCurrentDir()
		if err != nil {
			colorprint.PrintFail(os.Stderr, fmt.Sprintf("Failed to deploy function"), err.Error(), ".\n")
			return
		}
		functionName := functionInfo.Name
		if err := verify.CheckFunctionName(functionName); err != nil {
			colorprint.PrintFail(os.Stderr,
				fmt.Sprintf("Failed to deploy the function %s, ", functionName), err.Error(), ".\n")
			return
		}
		if err := deployFunction(opts.client, functionName, functionInfo); err != nil {
			colorprint.PrintFail(os.Stderr,
				fmt.Sprintf("Failed to deploy the function %s, ", functionName), err.Error(), ".\n")
			return
		}
		colorprint.PrintSuccess(os.Stdout,
			fmt.Sprintf("Succeeded to deploy the function %s", functionName), ".\n")
	},
}

// InitDeployCMD init cmd of deploy
func InitDeployCMD(c client.Client, cio *cmdio.CmdIO) *cobra.Command {
	opts.cmdIO = cio
	opts.client = c

	return deployCmd
}

func deployFunction(c client.Client, functionName string, functionConfig *deploy.FunctionConfig) error {
	colorprint.PrintSuccess(os.Stdout,
		fmt.Sprintf("Started to deploy the function %s, runtime %s",
			functionName, functionConfig.Runtime), ".\n")
	pkgPath := ""
	if functionConfig.FunctionCodePath != "" {
		colorprint.PrintSuccess(os.Stdout,
			fmt.Sprintf("functionCodePath has been configured in the function.yaml, "+
				"skip to package the function code."), ".\n")
		pkgPath = functionConfig.FunctionCodePath
	} else {
		currentPath, err := os.Getwd()
		if err != nil {
			return err
		}
		packageProcess := progress.CreateAndDoing("Function code packaging")
		pkgPath, err = packageFunctionCode(currentPath, functionConfig)
		if err != nil {
			packageProcess.Done(progress.FailedDone)
			return err
		}
		packageProcess.Done("Function code package")
	}

	uploadProcess := progress.CreateAndDoing("Function package uploading")

	if err := verify.CheckFunctionName(functionName); err != nil {
		return err
	}
	functionConfig.Version = constant.LatestVersion
	functionConfig.TenantID = config.FConfig.CLIConfig.TenantID
	functionConfig.CPU = functionConfig.MemorySize/memoryBase*cpuAllocation + cpuBase
	if err := function.Deploy(c, pkgPath, functionConfig); err != nil {
		uploadProcess.Done(progress.FailedDone)
		return err
	}
	uploadProcess.Done("Function package upload")
	return nil
}

func packageFunctionCode(currentPath string, functionConfig *deploy.FunctionConfig) (string, error) {
	var packingPath string
	var err error

	functionBuild := builder.GetBuildByRuntime(functionConfig.Runtime, functionConfig.Handler)
	if packingPath, err = functionBuild.Build(currentPath, functionConfig.Name); err != nil {
		return "", fmt.Errorf("package function code error: %s", err)
	}

	return packingPath, nil
}
