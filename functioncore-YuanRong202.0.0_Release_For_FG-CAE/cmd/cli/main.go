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

// Package main is main function of serverless native
package main

import (
	"fmt"

	"functioncore/pkg/cli/cmd"
	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/pkg/cert"
)

func main() {
	if err := config.LoadConfig(); err != nil {
		fmt.Printf("Failed to load the config, %s.\n", err)
		return
	}

	if err := cert.Load(); err != nil {
		fmt.Printf("Failed to load the cert, %s.\n", err)
		return
	}

	if err := config.LoadFunctionConfig(); err != nil {
		fmt.Printf("Failed to load function config, %s.\n", err)
		return
	}

	rootCmd, err := cmd.NewCmdRoot()
	if err != nil {
		fmt.Printf("Failed to new root cmd, %s.\n", err)
		return
	}

	if err := rootCmd.Execute(); err != nil {
		fmt.Printf("Failed to Execute root cmd, %s.\n", err)
		return
	}
}
