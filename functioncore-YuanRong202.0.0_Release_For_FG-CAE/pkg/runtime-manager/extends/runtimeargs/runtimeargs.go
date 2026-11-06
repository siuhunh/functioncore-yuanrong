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

// Package runtimeargs is used for building runtime args
package runtimeargs

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	tools "functioncore/pkg/common/utils"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/runtime-manager/trace"
	"functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker/config"
	workerutils "functioncore/pkg/worker/utils"
)

const (
	// NodeJsWrapperPath NodeJsWrapper nodejs runtime wrapper path
	NodeJsWrapperPath = "/home/snuser/runtime/wrapper.js"
	// PhpWrapperPath php runtime wrapper path
	PhpWrapperPath = "/home/snuser/runtime/php/server.php"
	// CSharp21WrapperPath csharp runtime 2.1 wrapper path
	CSharp21WrapperPath = "/home/snuser/runtime/netcoreapp2.1/CSharpRuntime.Bootstrap.dll"
	// CSharp31WrapperPath csharp runtime 3.1 wrapper path
	CSharp31WrapperPath = "/home/snuser/runtime/netcoreapp3.1/CSharpRuntime.Bootstrap.dll"
	// CSharp60WrapperPath csharp runtime 6.0 wrapper path
	CSharp60WrapperPath = "/home/snuser/runtime/netcoreapp6.0/CSharpRuntime.Bootstrap.dll"
	// CSharp80WrapperPath csharp runtime 8.0 wrapper path
	CSharp80WrapperPath = "/home/snuser/runtime/netcoreapp8.0/CSharpRuntime.Bootstrap.dll"
	// CangJie10WrapperPath cangjie runtime 1.0 wrapper path
	CangJie10WrapperPath = "/home/snuser/runtime/cangjie1.0/main"
	// CangJie0523WrapperPath cangjie runtime 0.52.3 wrapper path
	CangJie0523WrapperPath = "/home/snuser/runtime/cangjie0.52.3/main"
	// CangJie060xWrapperPath cangjie runtime 0.52.3 wrapper path
	CangJie060xWrapperPath = "/home/snuser/runtime/cangjie0.60.x/main"
	// HandlerFilePath handler file path
	HandlerFilePath = ""
	// JavaMainClass java main class
	JavaMainClass = "com.huawei.services.runtime.rpc.RuntimeServer"
	// HandlerName handler name
	HandlerName = ""

	// 2560Mi divide different default jvm args.
	jvmArgsDivider = 2560
	localhost      = "127.0.0.1"
	occupy         = 0.9
	depsArgs       = "--depsfile"

	csharpHandlerSeparator = "::"
	csharpHandlerLength    = 3
	assemblyIndex          = 0

	// UseParallelGC Collector
	UseParallelGC = "-XX:+UseParallelGC"
	// UseSerialGC Collector
	UseSerialGC = "-XX:+UseSerialGC"

	skywalkingInstancePattern = `(skywalking\.agent\.instance_name=)[^ ]+`
)

var (
	// javaSystemProperty java system property
	javaSystemProperty = "-Dlog4j2.configurationFile=file:/home/snuser/config/log4j2.xml"
	// avoid CRIU deleted memory mapped files on netty
	nettyProperty = "-Dio.netty.native.deleteLibAfterLoading=false"
	// enableRemoteDebug remote debug switch
	enableRemoteDebug = os.Getenv("EnableRemoteDebug") == "true"
	remoteDebugArgs   = os.Getenv("RemoteDebugArgs")
)

type javaArgsPars struct {
	javaVersion string
	layerPath   string
	address     string
	sockType    string
	runningMode string
}

// BuildArgsPars _
type BuildArgsPars struct {
	Lang        string
	Port        int32
	SockType    string
	RunningMode string
}

// GetBuildArgs build runtime args
func GetBuildArgs(par BuildArgsPars, config *config.RuntimeConfig) ([]string, error) {
	rmConfig := &config.RMConfig
	layerPath, err := filepath.Abs(os.Getenv("DEPLOY_DIR"))
	if err != nil {
		log.GetLogger().Warnf("failed to get absolute path: %s", err.Error())
		return nil, err
	}

	address := fmt.Sprintf("%s:%d", localhost, par.Port)
	switch par.Lang {
	case utils.Nodejs:
		return getNodejsArgs(address), nil
	case utils.Python27, utils.Python36, utils.Python39, utils.Python310, utils.Python311, utils.Python312:
		return []string{"/home/snuser/runtime/" + par.Lang + "/server.py", address, layerPath}, nil
	case utils.Java8, utils.Java11, utils.Java17, utils.Java21, utils.Javax:
		return getJavaArgs(javaArgsPars{
			javaVersion: par.Lang,
			layerPath:   layerPath,
			address:     address,
			sockType:    par.SockType,
			runningMode: par.RunningMode,
		}, rmConfig, config.EnableGCLog)
	case utils.Php:
		return []string{PhpWrapperPath, address}, nil
	case utils.NetCore21:
		depsJSON, ok := getCSharpDepsJSONArgs(layerPath, config.HandlerName)
		if ok && depsJSON != "" {
			return []string{"exec", depsArgs, depsJSON, CSharp21WrapperPath, address}, nil
		}
		return []string{"exec", CSharp21WrapperPath, address}, nil
	case utils.NetCore31:
		depsJSON, ok := getCSharpDepsJSONArgs(layerPath, config.HandlerName)
		if ok && depsJSON != "" {
			return []string{"exec", depsArgs, depsJSON, CSharp31WrapperPath, address}, nil
		}
		return []string{"exec", CSharp31WrapperPath, address}, nil
	case utils.NetCore60:
		depsJSON, ok := getCSharpDepsJSONArgs(layerPath, config.HandlerName)
		if ok && depsJSON != "" {
			return []string{"exec", depsArgs, depsJSON, CSharp60WrapperPath, address}, nil
		}
		return []string{"exec", CSharp60WrapperPath, address}, nil
	case utils.NetCore80:
		depsJSON, ok := getCSharpDepsJSONArgs(layerPath, config.HandlerName)
		if ok && depsJSON != "" {
			return []string{"exec", depsArgs, depsJSON, CSharp80WrapperPath, address}, nil
		}
		return []string{"exec", CSharp80WrapperPath, address}, nil
	case utils.CangJie10:
		return []string{"-exec", fmt.Sprintf("%s %s", CangJie10WrapperPath, address)}, nil
	case utils.CangJie0523:
		return []string{"-exec", fmt.Sprintf("%s %s", CangJie0523WrapperPath, address)}, nil
	case utils.CangJie060x:
		return []string{"-exec", fmt.Sprintf("%s %s", CangJie060xWrapperPath, address)}, nil
	case utils.CustomRuntimeRPC:
		return []string{"-exec", fmt.Sprintf("%s/%s %d", layerPath, config.HandlerName, par.Port)}, nil
	default:
		return nil, nil
	}
}

func getCSharpDepsJSONArgs(deployDir string, funcHandler string) (string, bool) {
	handlers := strings.Split(funcHandler, csharpHandlerSeparator)
	if len(handlers) != csharpHandlerLength {
		log.GetLogger().Warnf("invalid runtime handler format : %s , "+
			"and will start runtime without deps.json param", funcHandler)
		return "", false
	}

	filePath := fmt.Sprintf("%s/%s.deps.json", deployDir, handlers[assemblyIndex])
	if _, err := os.Stat(filePath); err != nil && !os.IsExist(err) {
		log.GetLogger().Warnf("deps.json file %s is not exist , "+
			"and will start runtime without deps.json param", filePath)
		return "", false
	}
	return filePath, true
}

func getNodejsArgs(address string) []string {
	_, podMemory := workerutils.GetCPUMemory()
	maxOldSpaceSize := "--max-old-space-size=" + getNodejsMaxOldSpaceSize(podMemory)
	args := []string{maxOldSpaceSize}
	args = append(args, []string{NodeJsWrapperPath, address, HandlerFilePath, HandlerName}...)
	return args
}

func getNodejsMaxOldSpaceSize(podMemory int64) string {
	return fmt.Sprintf("%.0f", math.Floor(float64(podMemory)*occupy))
}

func getJavaArgs(par javaArgsPars, rmConfig *rmconfig.Config, enableGCLog bool) ([]string, error) {
	javaClassPath := "/home/snuser/runtime/java/*:" + par.layerPath
	javaCustomArgs := rmConfig.RuntimeConfigs[par.javaVersion].CustomArgs
	jvmArgs, err := getJvmArgs(par, rmConfig, enableGCLog)
	if err != nil {
		log.GetLogger().Errorf("user defined java args err")
		return nil, err
	}

	javaDisableTrace := isRuntimeDisableTrace(rmConfig.RuntimeConfigs[par.javaVersion])
	var args []string
	if tools.IsCAEFunc(os.Getenv(constants.BusinessType)) {
		args = append(jvmArgs, []string{"-cp", javaClassPath,
			nettyProperty, JavaMainClass, par.address, par.javaVersion, par.sockType}...)
	} else {
		args = append(jvmArgs, []string{"-cp", javaClassPath,
			javaSystemProperty, nettyProperty, JavaMainClass, par.address, par.javaVersion, par.sockType}...)
	}
	if javaCustomArgs != nil && !javaDisableTrace {
		log.GetLogger().Infof("start to modify apm config")
		traceAddress := rmConfig.RuntimeConfigs[par.javaVersion].TraceMasterAddress
		configFile := "/home/snuser/apm-javaagent/apm.config"
		if err := trace.EditTraceConfig(traceAddress, configFile); err != nil {
			log.GetLogger().Warnf("failed to editTraceAPMConfig, %s", err.Error())
		}
		log.GetLogger().Infof("judged to use apm trace")
		return append(javaCustomArgs, args...), nil
	}
	return args, nil
}

func getJvmArgs(par javaArgsPars, rmConfig *rmconfig.Config, enableGCLog bool) ([]string, error) {
	customJavaArgs := os.Getenv(constants.CustomJavaArgs)
	jvmArgs, customJvmArgs, err := parseCustomJavaArgsFromEnv(customJavaArgs)
	if err != nil {
		return nil, err
	}
	if len(customJvmArgs) > 0 {
		return jvmArgs, nil
	}

	addition := rmConfig.RuntimeConfigs[par.javaVersion].Addition
	if addition != nil {
		_, mem := workerutils.GetCPUMemory()
		jvmArgsKey := strings.Join([]string{par.javaVersion, strconv.FormatInt(mem, 10), "jvm", "options"}, "-")
		customJvmArgs, exist := addition[jvmArgsKey]
		if exist {
			jvmArgs = strings.Split(customJvmArgs, " ")
		}
	}
	if len(jvmArgs) == 0 {
		jvmArgs = getDefaultJvmArgs(par.javaVersion)
	}
	dumpFileName := fmt.Sprintf("heapDump_%d.hprof", time.Now().UnixNano())
	jvmArgs = append(jvmArgs, "-XX:-TieredCompilation", "-XX:+HeapDumpOnOutOfMemoryError",
		"-XX:HeapDumpPath=/home/fg_dump/"+dumpFileName)
	jvmArgs = addJvmSpecialConf(par.javaVersion, jvmArgs)
	if enableGCLog {
		if par.javaVersion == utils.Java11 || par.javaVersion == utils.Java17 || par.javaVersion == utils.Java21 {
			jvmArgs = append(jvmArgs,
				"-Xlog:gc*:file=/home/sn/log/runtime/gc.log:time,level,tags:"+
					"filecount=5,filesize=20M")
		} else if par.javaVersion == utils.Java8 {
			jvmArgs = append(jvmArgs, "-Xloggc:/home/sn/log/runtime/gc.log",
				"-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-XX:+UseGCLogFileRotation",
				"-XX:NumberOfGCLogFiles=5", "-XX:GCLogFileSize=20M")
		}
		log.GetLogger().Infof("success to enable GC log")
	}
	if enableRemoteDebug && remoteDebugArgs != "" {
		jvmArgs = append(jvmArgs, remoteDebugArgs)
	}
	return jvmArgs, nil
}

func addJvmSpecialConf(javaVersion string, jvmArgs []string) []string {
	switch javaVersion {
	case utils.Java17, utils.Java21:
		return append(jvmArgs, "--add-opens=java.base/java.util=ALL-UNNAMED",
			"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.net=ALL-UNNAMED",
			"--add-opens=java.base/java.io=ALL-UNNAMED", "--add-opens=java.base/java.math=ALL-UNNAMED",
			"--add-opens=java.base/java.time=ALL-UNNAMED", "--add-opens=java.base/java.text=ALL-UNNAMED ",
			"--enable-preview")
	default:
		return jvmArgs
	}
}

func getDefaultJvmArgs(javaVersion string) []string {
	_, mem := workerutils.GetCPUMemory()
	if mem < jvmArgsDivider {
		return []string{"-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0",
			"-XX:+UseSerialGC"}
	}
	if javaVersion == utils.Java8 {
		return []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0",
			"-XX:+UseConcMarkSweepGC", "-XX:+CMSClassUnloadingEnabled", "-XX:+CMSIncrementalMode",
			"-XX:+CMSScavengeBeforeRemark", "-XX:+UseCMSInitiatingOccupancyOnly", "-XX:CMSInitiatingOccupancyFraction=70",
			"-XX:CMSFullGCsBeforeCompaction=5", "-XX:MaxGCPauseMillis=200", "-XX:+ExplicitGCInvokesConcurrent",
			"-XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses"}
	}
	if javaVersion == utils.Java11 || javaVersion == utils.Java17 || javaVersion == utils.Java21 {
		return []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC"}
	}
	return []string{"-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0"}
}

func isRuntimeDisableTrace(config rmconfig.RuntimeConfig) bool {
	_, podMemory := workerutils.GetCPUMemory()
	for index := range config.DisableTrace {
		if config.DisableTrace[index] == podMemory {
			return true
		}
	}
	return false
}

func getCustomJavaArgs(customJavaArgs string, jvmArgs *string, javaagentArgs *[]string, dArgs *string) bool {
	var data map[string]string
	err := json.Unmarshal([]byte(customJavaArgs), &data)
	if err != nil {
		log.GetLogger().Warnf("custom java args parse err : ", err)
		return false
	}

	if val, ok := data["jvm"]; ok {
		*jvmArgs = val
	}
	if val, ok := data["javaagent"]; ok {
		arrVal := strings.Split(val, " ")
		*javaagentArgs = append(*javaagentArgs, arrVal...)
	}
	if val, ok := data["-D"]; ok {
		*dArgs = val
	}
	instanceName := os.Getenv(constants.HostNameEnvKey)
	re := regexp.MustCompile(skywalkingInstancePattern)
	*dArgs = re.ReplaceAllString(*dArgs, "${1}"+instanceName)

	return true
}

func parseCustomJavaArgsFromEnv(customJavaArgs string) ([]string, string, error) {
	var jvmArgs []string
	var customJvmArgs string
	var javaagentArgs []string
	var dArgs string
	if customJavaArgs != "" {
		isInWhiteList := getCustomJavaArgs(customJavaArgs, &customJvmArgs, &javaagentArgs, &dArgs)
		if len(javaagentArgs) != 0 {
			jvmArgs = append(jvmArgs, javaagentArgs...)
		}
		if dArgs != "" {
			dArr := strings.Split(dArgs, " ")
			for _, val := range dArr {
				jvmArgs = append(jvmArgs, val)
			}
		}
		if !isInWhiteList {
			return nil, "", errors.New("customJavaArgs is invalid : " + customJvmArgs)
		}
		if customJvmArgs != "" {
			jvmArr := strings.Split(customJvmArgs, " ")
			if err := checkConflictArgs(jvmArr); err != nil {
				log.GetLogger().Warnf("custom jvm args invalid %s", customJavaArgs)
				return nil, "", err
			}
			for _, val := range jvmArr {
				jvmArgs = append(jvmArgs, val)
			}
		}
	}

	return jvmArgs, customJvmArgs, nil
}

func checkConflictArgs(args []string) error {
	argMap := make(map[string]bool)
	for _, key := range args {
		argMap[key] = true
	}
	if argMap[UseParallelGC] && argMap[UseSerialGC] {
		errMsg := fmt.Sprintf("invalid jvm args: %v", args)
		return errors.New(errMsg)
	}

	return nil
}
