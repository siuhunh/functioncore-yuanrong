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

// Package worker -
package worker

const (
	// LocalServerIP is local loop IP
	LocalServerIP = "127.0.0.1"

	// DeployFileMode mode of function and layer file, contains custom binary file possibly
	DeployFileMode = 0750

	// DeployFileModForMcp mod of function and layer file for mcp
	DeployFileModForMcp = 0755

	// PersistFileMode -
	PersistFileMode = 0600

	// RuntimeTypeEnvKey -
	RuntimeTypeEnvKey = "RUNTIME_TYPE"
	// RuntimeEnvPrefix environment prefix of runtime
	RuntimeEnvPrefix = "func-"

	// LibPathPrefix -
	LibPathPrefix = "LD_LIBRARY_PATH"

	// RuntimeLogPath log path of runtime
	RuntimeLogPath = "/home/sn/log/"
	// ConfigEnvKey -
	ConfigEnvKey = "WORKER_CONFIG"
	// RestartConfigPath is path of config file that used when restart
	RestartConfigPath = "/home/sn/restart/config.yaml"

	// ResourcePath is path of worker secrets
	ResourcePath = "/home/sn/resource"
	// LocalAuthAKPath is path of local auth access key
	LocalAuthAKPath = "/home/sn/resource/localauth-ak"
	// LocalAuthSKPath is path of local auth secret key
	LocalAuthSKPath = "/home/sn/resource/localauth-sk"
	// S3StorageAKPath is path of s3 storage access key
	S3StorageAKPath = "/home/sn/resource/s3-storage-ak"
	// S3StorageSKPath is path of s3 storage secret key
	S3StorageSKPath = "/home/sn/resource/s3-storage-sk"
	// SnapshotPackKeyPath -
	SnapshotPackKeyPath = "/home/sn/resource/localauth-sk"

	// RuntimeRespSizeLimit limit (byte) size of response from runtime
	RuntimeRespSizeLimit = 3584 * 1024

	// CustomRuntimeLanguage -
	CustomRuntimeLanguage = "custom-runtime"
	// WebRuntimeLanguage -
	WebRuntimeLanguage = "http"
	// GOV1RuntimeLanguage notice: forbidden to modify "go1.x"
	// If the "go1.x" is modified, it must be adapted to the management plane.
	GOV1RuntimeLanguage = "go1.x"
	// CppBinRuntimeLanguage -
	CppBinRuntimeLanguage = "cpp11-bin"
	// CustomContainerLanguage -
	CustomContainerLanguage = "custom-container"
	// ExtensionScript -
	ExtensionScript = "extension"

	// NodeJs18Path -
	NodeJs18Path = "/opt/function/runtime/nodejs18.15/rtsp/nodejs/bin"
	// NodeJs20Path -
	NodeJs20Path = "/opt/function/runtime/nodejs20.15/rtsp/nodejs/bin:" +
		"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	// NpmConfigCache -
	NpmConfigCache = "/tmp/.npm_cache"
	// NpmConfigLogsDir -
	NpmConfigLogsDir = "/tmp/_logs"
	// UVCacheDir -
	UVCacheDir = "/tmp"
	// UVPythonInstallDir -
	UVPythonInstallDir = "/tmp"
	// UVToolDir -
	UVToolDir = "/tmp"
)

const (
	// FuncLogLevelDebug -
	FuncLogLevelDebug = "DEBUG"
	// FuncLogLevelInfo -
	FuncLogLevelInfo = "INFO"
	// FuncLogLevelWarn -
	FuncLogLevelWarn = "WARN"
	// FuncLogLevelWarning -
	FuncLogLevelWarning = "WARNING"
	// FuncLogLevelError -
	FuncLogLevelError = "ERROR"
)

const (
	// LoadedFilename -
	LoadedFilename = "__loaded__"
)

const (
	// NodejsDependenciesPath -
	NodejsDependenciesPath = "/opt/function/code/node_modules/"
	// PublicDependenciesPath -
	PublicDependenciesPath = "/opt/function/dependencies/"
)

const (
	// DefaultSocketType -
	DefaultSocketType = "tcp"
	// UnixDomainSocketType -
	UnixDomainSocketType = "unix"
)

const (
	// TimeTraceTotal -
	TimeTraceTotal = "total"
	// TimeTraceRuntime -
	TimeTraceRuntime = "runtime"
)

const (
	// LogConfigFilePath path of log config file
	LogConfigFilePath = "/home/sn/log/logconfig"
	// LogConfigFileName name of log config file
	LogConfigFileName = "logconfig.json"
)
