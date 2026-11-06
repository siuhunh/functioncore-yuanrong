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

package runtimeargs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/constants"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/runtime-manager/trace"
	"functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker/config"
	workerutils "functioncore/pkg/worker/utils"
)

func Test_getBuildArgs(t *testing.T) {
	port := int32(-1)
	address := fmt.Sprintf("%s:%d", localhost, port)
	patches := gomonkey.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
		return int64(300), int64(128)
	})
	buildArgsPars := BuildArgsPars{
		Lang:        utils.Nodejs,
		Port:        port,
		SockType:    "",
		RunningMode: constants.RunningModeNormal,
	}
	val, _ := GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})

	assert.Equal(t, true, reflect.DeepEqual(val, []string{
		"--max-old-space-size=115", NodeJsWrapperPath, address, HandlerFilePath, HandlerName}))
	buildArgsPars.Lang = utils.Python39

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, "/home/snuser/runtime/"+utils.Python39+"/server.py", val[0])
	patches.ApplyFunc(os.Getenv, func(key string) string {
		if key == "TRACE_DISABLE" {
			return "ENABLE"
		}
		if key == "DEPLOY_DIR" {
			return "/opt/function/code"
		}
		v, _ := syscall.Getenv(key)
		return v
	})
	patches = patches.ApplyFunc(trace.EditTraceConfig, func(string, string) error { return nil })
	defer patches.Reset()

	buildArgsPars.Lang = utils.Java8
	buildArgsPars.SockType = "unix"

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = utils.Java8
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = utils.Java11
	buildArgsPars.SockType = "unix"

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = utils.Java11
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = utils.Php
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = utils.NetCore31
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = utils.NetCore21
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = utils.NetCore60
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)

	buildArgsPars.Lang = utils.NetCore80
	buildArgsPars.SockType = ""
	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)

	buildArgsPars.Lang = utils.CangJie10
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, len(val) > 0)
	buildArgsPars.Lang = "unknownLang"
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, val == nil)
	patches.ApplyFunc(filepath.Abs, func(path string) (string, error) { return "", errors.New("get absolute path failed") })
	// failed to GetBuildArgs, return nil
	buildArgsPars.Lang = utils.Python39
	buildArgsPars.SockType = ""

	val, _ = GetBuildArgs(buildArgsPars, &config.RuntimeConfig{})
	assert.Equal(t, true, val == nil)
	patches.Reset()
}

func Test_getNodejsMaxOldSpaceSize(t *testing.T) {
	actual := getNodejsMaxOldSpaceSize(int64(128))
	assert.Equal(t, "115", actual)
	actual = getNodejsMaxOldSpaceSize(int64(256))
	assert.Equal(t, "230", actual)
	actual = getNodejsMaxOldSpaceSize(int64(512))
	assert.Equal(t, "460", actual)
}

func Test_getJavaArgs(t *testing.T) {
	patches := gomonkey.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
		return int64(300), int64(128)
	})
	defer patches.Reset()
	rmConfig := &rmconfig.Config{RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
		utils.Java11: {CustomArgs: []string{"CustomArgs"}, DisableTrace: []int64{512},
			Addition: map[string]string{"java11-128-jvm-options": "-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=80.0 -XX:+UseSerialGC"}},
		utils.Java21: {CustomArgs: []string{"CustomArgs"}, DisableTrace: []int64{512},
			Addition: map[string]string{"java11-128-jvm-options": "-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=80.0 -XX:+UseSerialGC"}},
	}}

	jP := javaArgsPars{
		javaVersion: "java11",
		layerPath:   "/home/layer",
		address:     "127.1.1.1",
		sockType:    "tcp",
		runningMode: constants.RunningModeNormal,
	}

	args2, _ := getJavaArgs(jP, rmConfig, true)
	assert.Equal(t, []string{"CustomArgs", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0", "-XX:+UseSerialGC", "-XX:-TieredCompilation",
		"-XX:+HeapDumpOnOutOfMemoryError", args2[6], "-Xlog:gc*:file=/home/sn/log/runtime/gc.log:time,level,tags:filecount=5,filesize=20M",
		"-cp", "/home/snuser/runtime/java/*:/home/layer", "-Dlog4j2.configurationFile=file:/home/snuser/config/log4j2.xml", "-Dio.netty.native.deleteLibAfterLoading=false",
		"com.huawei.services.runtime.rpc.RuntimeServer", "127.1.1.1", "java11", "tcp"}, args2)

	_ = os.Setenv(constants.CustomJavaArgs, "{\"jvm\":\"-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=80.0 "+
		"-XX:+UseSerialGC -XX:-TieredCompilation -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/fg_dump/heapDump_1712988147493738385.hprof\"}")
	args3, _ := getJavaArgs(jP, rmConfig, true)
	assert.Equal(t, []string{"CustomArgs", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0", "-XX:+UseSerialGC", "-XX:-TieredCompilation",
		"-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/home/fg_dump/heapDump_1712988147493738385.hprof", "-cp",
		"/home/snuser/runtime/java/*:/home/layer", "-Dlog4j2.configurationFile=file:/home/snuser/config/log4j2.xml", "-Dio.netty.native.deleteLibAfterLoading=false",
		"com.huawei.services.runtime.rpc.RuntimeServer", "127.1.1.1", "java11", "tcp"}, args3)

	// cae function, no -Dlog4j2.configurationFile
	os.Setenv(constants.BusinessType, constants.BusinessTypeCAE)
	args4, _ := getJavaArgs(jP, rmConfig, true)
	assert.Equal(t, []string{"CustomArgs", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0", "-XX:+UseSerialGC", "-XX:-TieredCompilation",
		"-XX:+HeapDumpOnOutOfMemoryError", "-XX:HeapDumpPath=/home/fg_dump/heapDump_1712988147493738385.hprof", "-cp",
		"/home/snuser/runtime/java/*:/home/layer", "-Dio.netty.native.deleteLibAfterLoading=false",
		"com.huawei.services.runtime.rpc.RuntimeServer", "127.1.1.1", "java11", "tcp"}, args4)
	os.Setenv(constants.BusinessType, "")

	japJava21 := javaArgsPars{
		javaVersion: "java21",
		layerPath:   "/home/layer",
		address:     "127.1.1.1",
		sockType:    "unix",
		runningMode: constants.RunningModeNormal,
	}
	_ = os.Setenv(constants.CustomJavaArgs, "")
	args21, _ := getJavaArgs(japJava21, rmConfig, true)
	assert.Equal(t, []string{"CustomArgs", "-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0",
		"-XX:+UseSerialGC", "-XX:-TieredCompilation", "-XX:+HeapDumpOnOutOfMemoryError", args21[6],
		"--add-opens=java.base/java.util=ALL-UNNAMED",
		"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.net=ALL-UNNAMED",
		"--add-opens=java.base/java.io=ALL-UNNAMED", "--add-opens=java.base/java.math=ALL-UNNAMED",
		"--add-opens=java.base/java.time=ALL-UNNAMED", "--add-opens=java.base/java.text=ALL-UNNAMED ", "--enable-preview",
		"-Xlog:gc*:file=/home/sn/log/runtime/gc.log:time,level,tags:filecount=5,filesize=20M",
		"-cp", "/home/snuser/runtime/java/*:/home/layer", "-Dlog4j2.configurationFile=file:/home/snuser/config/log4j2.xml",
		"-Dio.netty.native.deleteLibAfterLoading=false",
		"com.huawei.services.runtime.rpc.RuntimeServer", "127.1.1.1", "java21", "unix"}, args21)
}

func Test_getSuitableJvmArgs(t *testing.T) {
	patches := gomonkey.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
		return int64(2200), int64(2560)
	})
	defer patches.Reset()
	java8Args := getDefaultJvmArgs("java8")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseConcMarkSweepGC", "-XX:+CMSClassUnloadingEnabled",
		"-XX:+CMSIncrementalMode", "-XX:+CMSScavengeBeforeRemark", "-XX:+UseCMSInitiatingOccupancyOnly", "-XX:CMSInitiatingOccupancyFraction=70",
		"-XX:CMSFullGCsBeforeCompaction=5", "-XX:MaxGCPauseMillis=200", "-XX:+ExplicitGCInvokesConcurrent", "-XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses"}, java8Args)
	java11Args := getDefaultJvmArgs("java11")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC"}, java11Args)
	java17Args := getDefaultJvmArgs("java17")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC"}, java17Args)
	java21Args := getDefaultJvmArgs("java21")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC"}, java21Args)
}

func Test_isRuntimeDisableTrace(t *testing.T) {
	patches := gomonkey.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
		return int64(300), int64(128)
	})
	defer patches.Reset()
	rmConfig := rmconfig.RuntimeConfig{CustomArgs: []string{"CustomArgs"}, DisableTrace: []int64{128, 256}}

	args2 := isRuntimeDisableTrace(rmConfig)
	assert.Equal(t, true, args2)
}

func TestGetCSharpDepsJsonArgs(t *testing.T) {
	file, ok := getCSharpDepsJSONArgs("/opt/func/code", "HelloCSharp:HelloCSharp.Kafka.KafkaTest::MFunc")
	assert.Equal(t, "", file)
	assert.Equal(t, false, ok)

	file, ok = getCSharpDepsJSONArgs("/opt/func/code", "HelloCSharp::HelloCSharp.Kafka.KafkaTest::MFunc")
	assert.Equal(t, "", file)
	assert.Equal(t, false, ok)
}

func TestGetCustomJavaArgs(t *testing.T) {
	var jvmArgs string
	var javaagentArgs []string
	var dArgs string

	customJavaArgs := "{\"jvm\":\"-XX:CompressedClassSpaceSize=528482304 " +
		"-XX:ErrorFile=/applog/jvm-log/jvm-error-1710899811.log -XX:GCLogFileSize=104857600 " +
		"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/applog/jvm-log/com.huawei.erp.hfa.fa-heapdump-1710899811.bin " +
		"-XX:InitialHeapSize=2093796556 -XX:InitialRAMPercentage=65.000000 -XX:MaxHeapSize=2093796556 " +
		"-XX:MaxMetaspaceSize=536870912 -XX:MaxRAMPercentage=65.000000 -XX:MetaspaceSize=268435456 " +
		"-XX:NumberOfGCLogFiles=5 -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCDetails " +
		"-XX:+PrintGCTimeStamps -XX:+UseCompressedClassPointers -XX:+UseCompressedOops " +
		"-XX:+UseContainerSupport -XX:+UseParallelGC \",\"javaagent\":\"-javaagent:path/to/your-agent.jar\"," +
		"\"-D\":\"-Dk1=v1 -Dk2=v2\"}"

	isInWhitelist := getCustomJavaArgs(customJavaArgs, &jvmArgs, &javaagentArgs, &dArgs)
	assert.Equal(t, "-XX:CompressedClassSpaceSize=528482304 -XX:ErrorFile=/applog/jvm-log/jvm-error-1710899811.log "+
		"-XX:GCLogFileSize=104857600 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/applog/jvm-log/com.huawei.erp.hfa.fa-heapdump-1710899811.bin "+
		"-XX:InitialHeapSize=2093796556 -XX:InitialRAMPercentage=65.000000 -XX:MaxHeapSize=2093796556 "+
		"-XX:MaxMetaspaceSize=536870912 -XX:MaxRAMPercentage=65.000000 -XX:MetaspaceSize=268435456 "+
		"-XX:NumberOfGCLogFiles=5 -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCTimeStamps "+
		"-XX:+UseCompressedClassPointers -XX:+UseCompressedOops -XX:+UseContainerSupport -XX:+UseParallelGC ", jvmArgs)
	assert.Equal(t, "-javaagent:path/to/your-agent.jar", strings.Join(javaagentArgs, " "))
	assert.Equal(t, "-Dk1=v1 -Dk2=v2", dArgs)
	assert.Equal(t, true, isInWhitelist)
}

func TestGetCustomJavaArgsMultiplyAgent(t *testing.T) {
	var jvmArgs string
	var javaagentArgs []string
	var dArgs string

	customJavaArgs := "{\"jvm\":\"-XX:CompressedClassSpaceSize=528482304 " +
		"-XX:ErrorFile=/applog/jvm-log/jvm-error-1710899811.log -XX:GCLogFileSize=104857600 " +
		"-XX:+UseContainerSupport -XX:+UseParallelGC \",\"javaagent\":\"-javaagent:/a.jar -javaagent:/b.jar\"," +
		"\"-D\":\"-Dk1=v1;v3 -Dk2=v2\"}"

	_ = getCustomJavaArgs(customJavaArgs, &jvmArgs, &javaagentArgs, &dArgs)
	assert.Equal(t, 2, len(javaagentArgs))
}

func TestCheckConflictArgs(t *testing.T) {
	args := []string{
		"-XX:CompressedClassSpaceSize",
		"-XX:ErrorFile",
		"-XX:GCLogFileSize",
		"-XX:HeapDumpPath",
		"-XX:InitialHeapSize",
		"-XX:+UseParallelGC",
		"-XX:+UseSerialGC",
		"-XX:-TieredCompilation",
	}

	err := checkConflictArgs(args)
	assert.Equal(t, true, err != nil)

	args = []string{
		"-XX:CompressedClassSpaceSize",
		"-XX:ErrorFile",
		"-XX:GCLogFileSize",
		"-XX:HeapDumpPath",
		"-XX:InitialHeapSize",
		"-XX:-TieredCompilation",
	}

	err = checkConflictArgs(args)
	assert.Equal(t, true, err == nil)
}
