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

// Package app New API Server Command
package app

import (
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"

	"functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/server"
)

// functionActivePortKey is target port for worker-manager
const functionActivePortKey = "FUNCTION_ACTIVE_PORT"

// NewAPIServerCommand New API Server Command
func NewAPIServerCommand(stopCh <-chan struct{}) *cobra.Command {
	s := options.NewAPIServerOptions()
	cmd := &cobra.Command{
		Use: "worker-manager",
		Long: `The worker-manager API Server validates and configures data for serverless functions and actives
               functions with RESTFul operations`,
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("starting worker-manager api server!")

			return server.Run(s, stopCh)
		},
	}

	cmd.SetArgs(os.Args)
	fs := cmd.Flags()
	podIP := os.Getenv(constants.PodIPEnvKey)
	if net.ParseIP(podIP) == nil {
		log.GetLogger().Errorf("failed to get pod ip of worker-manager from environment")
		return nil
	}
	funcActivePort := os.Getenv(functionActivePortKey)
	fs.StringP("listen-address", "l", podIP+":"+funcActivePort, "the worker-manager api server listen address")
	fs.BoolP("enable-apiv1", "", true, "enable api v1")
	fs.BoolP("enable-tenant", "", false, "enable tenant support")
	fs.StringP("protocol-type", "p", "http", "the worker-manager api server protocol type")
	fs.BoolP("is-cluster-mode", "", false, "is enable cluster mode, default use etcd standby cluster mode")
	fs.BoolP("is-snapshot-mode", "", false, "is enable snapshot mode, default closed")
	fs.StringP("cluster-type", "", "etcd", "the cluster type of worker-manager")
	fs.StringP("config-file", "c", "/home/sn/config/worker-manager.conf",
		"The worker manager api server config file path")
	fs.StringP("log-path", "", "/home/sn/log", "the worker-manager log path")

	if err := fs.Parse(os.Args); err != nil {
		log.GetLogger().Errorf("parsed args of os with error %s", err.Error())
		return nil
	}
	s.ListenAddress, _ = fs.GetString("listen-address")
	s.EnableAPIV1, _ = fs.GetBool("enable-apiv1")
	s.StorageType, _ = fs.GetString("storage-type")
	s.EnableTenant, _ = fs.GetBool("enable-tenant")
	s.ProtocolType, _ = fs.GetString("protocol-type")
	s.ConfigFile, _ = fs.GetString("config-file")
	s.IsClusterMode, _ = fs.GetBool("is-cluster-mode")
	s.ClusterType, _ = fs.GetString("cluster-type")
	s.LogPath, _ = fs.GetString("log-path")
	s.IsSnapshotMode, _ = fs.GetBool("is-snapshot-mode")
	return cmd
}
