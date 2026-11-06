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

package server

import "time"

const (
	// LoadDurationFmt -
	LoadDurationFmt = "getCodeAndDecompress:%dms(decompress:%dms)/deployLayer:%dms/workerTotal:%dms"

	loadErrorExitCode = 2

	extraHeaderMapSize = 1

	multiplePatNumber = 2

	funcTunnelName     = "tunl_fgs_vpc"
	endpointTunnelName = "tunl_end_vpc"

	httpServerShutdownTimeout = 5 * time.Second

	defaultRetryTimes    = 600
	defaultRetryInterval = 500 * time.Millisecond

	failedGetRuntimeContainerID = "failed to get RuntimeContainerID"

	logConfigFileMode = 0640

	iptablesCmdSucceed = 0
	iptablesCmdFailed  = 2
	iptablesExists     = "exists"

	vpcInterfaceName      = "tunl_fgs_vpc"
	defaultOnlink         = "onlink"
	defaultDestination    = "100.64.0.0/10"
	retryTime             = 3
	retryInterval         = 1 * time.Second
	watchHeapDumpPath     = "/home/sn/runtime/fg_dump"
	watchCoreDumpPath     = "/home/sn/runtime/coredump"
	mountFilePerm         = 0555
	workerRuntimeShareDir = "/opt/function/share"
)
