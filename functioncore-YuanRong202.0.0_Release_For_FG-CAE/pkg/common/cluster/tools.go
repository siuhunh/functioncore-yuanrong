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

// Package cluster get Proxy URL
package cluster

import (
	"fmt"
	"net"
	"os"

	"functioncore/pkg/common/constants"
)

// this func return a advertiseHost to put on the etcd, let nodes get the correct leader info
func getAdvertiseURL(ListenAddress string) string {
	// get the port from options
	host, err := net.ResolveTCPAddr("tcp4", ListenAddress)
	if err != nil {
		return err.Error()
	}
	port := host.Port

	// get the advertise ip from env
	ip := os.Getenv(constants.PodIPEnvKey)
	if net.ParseIP(ip) == nil {
		return fmt.Sprint("failed to get pod ip")
	}

	url := fmt.Sprintf("%s:%d", ip, port)
	return url
}
