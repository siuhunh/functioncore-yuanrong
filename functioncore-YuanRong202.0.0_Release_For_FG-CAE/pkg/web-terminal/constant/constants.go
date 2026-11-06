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

// Package constant define constant of web-terminal
package constant

import "time"

const (
	// ConfigFilePath define configmap file in web-terminal
	ConfigFilePath = "/home/sn/config/web-terminal.conf"
	// WebTerminalActivePortKey is target port for web-terminal
	WebTerminalActivePortKey = "WEB_TERMINAL_ACTIVE_PORT"
	// MaxTraceIDLength is the max length of traceID
	MaxTraceIDLength = 128
	// ClusterIDURNParam -
	ClusterIDURNParam = "cluster_id"
	// NamespaceIDURNParam -
	NamespaceIDURNParam = "namespace_id"
	// PodNameURNParam -
	PodNameURNParam = "pod_name"
	// RuntimeContainerName is the container name of runtime
	RuntimeContainerName = "runtime"
	// QueryCommand is the query param "command" of request
	QueryCommand = "command"
	// HandshakeTimeOut is the handshake timeout of websocket
	HandshakeTimeOut = 10 * time.Second
	// DefaultBufferSize websocket default buffer size
	DefaultBufferSize = 32 * 1024
	// ClusterCertPathPrefix is path prefix of cluster cert
	ClusterCertPathPrefix = "/home/sn/resource/cluster"
	// CACrt -
	CACrt = "ca.crt"
	// ClientCrt -
	ClientCrt = "client.crt"
	// ClientKey -
	ClientKey = "client.key"
	// SubProtocols -
	SubProtocols = "base64.channel.k8s.io"
	// ContainerRuntime -
	ContainerRuntime = "runtime"
	// MaxSendRetryTimes is max send retry times
	MaxSendRetryTimes = 3
	// SchemeWSS -
	SchemeWSS = "wss"
	// APIServerExecRequestURL -
	APIServerExecRequestURL = "/api/v1/namespaces/%s/pods/%s/exec"
	// DialInterval is dial interval
	DialInterval = 1 * time.Second
	// DefaultMaxSessions max session single instance
	DefaultMaxSessions = 200
	// DefaultPingPongInterval define default ping pong interval seconds
	DefaultPingPongInterval = 60
)
