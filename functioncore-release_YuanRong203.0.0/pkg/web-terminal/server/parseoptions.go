/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package server define function to parse options
package server

import (
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/web-terminal/constant"
	"functioncore/pkg/web-terminal/options"
)

// ParseOptions -
func ParseOptions(stopCh <-chan struct{}) *cobra.Command {
	asConfig := options.NewWebTerminalOptions()
	cmd := &cobra.Command{
		Use:  "web-terminal",
		Long: `the web-terminal is used for webshell between CAE and YuanRong cluster`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("starting web-terminal server!")
			return Run(asConfig, stopCh)
		},
	}
	cmd.SetArgs(os.Args)
	fs := cmd.Flags()
	podIP := os.Getenv(constants.PodIPEnvKey)
	if net.ParseIP(podIP) == nil {
		log.GetLogger().Errorf("failed to get pod ip of web-terminal from environment")
		return nil
	}
	activePort := os.Getenv(constant.WebTerminalActivePortKey)
	fs.StringP("listen-address", "l", podIP+":"+activePort, "the web-terminal server listen address")
	fs.StringP("config-file", "c", constant.ConfigFilePath,
		"The web-terminal server config file path")

	if err := fs.Parse(os.Args); err != nil {
		log.GetLogger().Errorf("parsed args of os with error %s", err.Error())
		return nil
	}
	asConfig.ListenAddress, _ = fs.GetString("listen-address")
	asConfig.ConfigFile, _ = fs.GetString("config-file")
	return cmd
}
