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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	rmconfig "functioncore/pkg/common/config"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/runtime-manager/trace"
	"functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker/config"
	workerutils "functioncore/pkg/worker/utils"
	mockUtils "functioncore/test/utils"
)

var MockUid uint32 = 1000

type mockFile struct{}

func (mockFile) Name() string       { return "" }
func (mockFile) Size() int64        { return 0 }
func (mockFile) Mode() fs.FileMode  { return 0755 }
func (mockFile) ModTime() time.Time { return time.Time{} }
func (mockFile) IsDir() bool        { return false }
func (mockFile) Sys() interface{} {
	return &syscall.Stat_t{
		Uid: MockUid,
	}
}

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
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0", "-XX:+UseSerialGC", "-XX:-TieredCompilation",
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

func Test_getJava8DefaultArgs(t *testing.T) {
	patches := gomonkey.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
		return int64(300), int64(128)
	})
	defer patches.Reset()
	rmConfig := &rmconfig.Config{RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
		utils.Java8: {CustomArgs: []string{"CustomArgs"}, DisableTrace: []int64{512},
			Addition: map[string]string{"java8-128-jvm-options": "-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=80.0 -XX:+UseSerialGC"}},
	}}
	jP := javaArgsPars{
		javaVersion: "java8",
		layerPath:   "/home/layer",
		address:     "127.1.1.1",
		sockType:    "tcp",
		runningMode: constants.RunningModeNormal,
	}
	os.Setenv(constants.BusinessType, "CAE")
	os.Setenv(constants.CustomJavaArgs, "")
	args, _ := getJavaArgs(jP, rmConfig, true)
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0", "-XX:+UseSerialGC",
		"-XX:-TieredCompilation", "-XX:+HeapDumpOnOutOfMemoryError", args[5],
		"-Xloggc:/home/sn/log/runtime/gc.log", "-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-XX:+UseGCLogFileRotation", "-XX:NumberOfGCLogFiles=5", "-XX:GCLogFileSize=20M",
		"-cp", "/home/snuser/runtime/java/*:/home/layer", "-Dio.netty.native.deleteLibAfterLoading=false", "com.huawei.services.runtime.rpc.RuntimeServer",
		"127.1.1.1", "java8", "tcp"}, args)
}

func Test_getSuitableJvmArgs(t *testing.T) {
	patches := gomonkey.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
		return int64(2200), int64(2560)
	})
	defer patches.Reset()
	java8Args := getDefaultJvmArgs("java8")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseConcMarkSweepGC", "-XX:+CMSClassUnloadingEnabled",
		"-XX:+CMSIncrementalMode", "-XX:+CMSScavengeBeforeRemark", "-XX:+UseCMSInitiatingOccupancyOnly", "-XX:CMSInitiatingOccupancyFraction=70",
		"-XX:CMSFullGCsBeforeCompaction=5", "-XX:MaxGCPauseMillis=200", "-XX:+ExplicitGCInvokesConcurrent", "-XX:+ExplicitGCInvokesConcurrentAndUnloadsClasses",
		"-XX:ReservedCodeCacheSize=256m", "-XX:InitialCodeCacheSize=256m"}, java8Args)
	java11Args := getDefaultJvmArgs("java11")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC",
		"-XX:ReservedCodeCacheSize=256m", "-XX:InitialCodeCacheSize=256m"}, java11Args)
	java17Args := getDefaultJvmArgs("java17")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC",
		"-XX:ReservedCodeCacheSize=256m", "-XX:InitialCodeCacheSize=256m"}, java17Args)
	java21Args := getDefaultJvmArgs("java21")
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=35.0", "-XX:MaxRAMPercentage=75.0", "-XX:+UseG1GC",
		"-XX:ReservedCodeCacheSize=256m", "-XX:InitialCodeCacheSize=256m"}, java21Args)
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
	var jvmArgs []string
	var javaagentArgs []string
	var dArgs []string

	customJavaArgs := "{\"jvm\":\"-XX:CompressedClassSpaceSize=528482304 " +
		"-XX:ErrorFile=/applog/jvm-log/jvm-error-1710899811.log -XX:GCLogFileSize=104857600 " +
		"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/applog/jvm-log/com.huawei.erp.hfa.fa-heapdump-1710899811.bin " +
		"-XX:InitialHeapSize=2093796556 -XX:InitialRAMPercentage=65.000000 -XX:MaxHeapSize=2093796556 " +
		"-XX:MaxMetaspaceSize=536870912 -XX:MaxRAMPercentage=65.000000 -XX:MetaspaceSize=268435456 " +
		"-XX:NumberOfGCLogFiles=5 -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCDetails " +
		"-XX:+PrintGCTimeStamps -XX:+UseCompressedClassPointers -XX:+UseCompressedOops " +
		"-XX:+UseContainerSupport -XX:+UseParallelGC \",\"javaagent\":\"-javaagent:path/to/your-agent.jar\"," +
		"\"-D\":\"-Dk1=v1 -Dk2=v2\"}"

	err := getCustomJavaArgs(customJavaArgs, &jvmArgs, &javaagentArgs, &dArgs)
	assert.Equal(t, "-XX:CompressedClassSpaceSize=528482304 -XX:ErrorFile=/applog/jvm-log/jvm-error-1710899811.log "+
		"-XX:GCLogFileSize=104857600 -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/applog/jvm-log/com.huawei.erp.hfa.fa-heapdump-1710899811.bin "+
		"-XX:InitialHeapSize=2093796556 -XX:InitialRAMPercentage=65.000000 -XX:MaxHeapSize=2093796556 "+
		"-XX:MaxMetaspaceSize=536870912 -XX:MaxRAMPercentage=65.000000 -XX:MetaspaceSize=268435456 "+
		"-XX:NumberOfGCLogFiles=5 -XX:+PrintGC -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGCTimeStamps "+
		"-XX:+UseCompressedClassPointers -XX:+UseCompressedOops -XX:+UseContainerSupport -XX:+UseParallelGC ",
		strings.Join(jvmArgs, " "))
	assert.Equal(t, "-javaagent:path/to/your-agent.jar", strings.Join(javaagentArgs, " "))
	assert.Equal(t, "-Dk1=v1 -Dk2=v2", strings.Join(dArgs, " "))
	assert.Equal(t, nil, err)

	err = getCustomJavaArgs("{testerr", &jvmArgs, &javaagentArgs, &dArgs)
	assert.Equal(t, true, err != nil)
}

func TestParseCustomJavaArgsFromEnv(t *testing.T) {
	convey.Convey("postprocess specific item", t, func() {
		customJavaArgs := "{\"jvm\":\"-XX:CompressedClassSpaceSize=528482304 " +
			"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/applog/jvm-log/com.huawei.erp.hfa.fa-heapdump-1710899811.bin " +
			"-XX:MaxMetaspaceSize=536870912 -XX:MaxRAMPercentage=65.000000 -XX:MetaspaceSize=268435456 " +
			"-XX:+PrintGCTimeStamps -XX:+UseCompressedClassPointers -XX:+UseCompressedOops " +
			"-XX:+UseContainerSupport -XX:+UseParallelGC\",\"javaagent\":\"-javaagent:path/to/your-agent.jar\"," +
			"\"-D\":\"-Dk1=v1 -Dk2=v2 -Dskywalking.agent.instance_name=x.x.x.x\"}"
		patch := gomonkey.ApplyFunc(os.Getenv,
			func(key string) string {
				return "custom-xxxx"
			})
		defer patch.Reset()
		javaArgs, err := parseCustomJavaArgsFromEnv(customJavaArgs, utils.Java8)
		expectStr := "-javaagent:path/to/your-agent.jar -Dk1=v1 -Dk2=v2 -Dskywalking.agent.instance_name=custom-xxxx " +
			"-XX:CompressedClassSpaceSize=528482304 " +
			"-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/fg_dump/com.huawei.erp.hfa.fa-heapdump-1710899811.bin " +
			"-XX:MaxMetaspaceSize=536870912 -XX:MaxRAMPercentage=65.000000 -XX:MetaspaceSize=268435456 " +
			"-XX:+PrintGCTimeStamps -XX:+UseCompressedClassPointers -XX:+UseCompressedOops " +
			"-XX:+UseContainerSupport -XX:+UseParallelGC"
		convey.So(expectStr, convey.ShouldEqual, strings.Join(javaArgs, " "))
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("empty str input", t, func() {
		javaArgs, err := parseCustomJavaArgsFromEnv("", utils.Java8)
		convey.So(0, convey.ShouldEqual, len(javaArgs))
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestGetCustomJavaArgsMultiplyAgent(t *testing.T) {
	var jvmArgs []string
	var javaagentArgs []string
	var dArgs []string

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

func TestGetJavaArgsFromJavaCmd(t *testing.T) {
	convey.Convey("Test get normal", t, func() {
		javaCmd := "java -javaagent:/rasp/rasp.jar -DxArmor.backend.allowInsecure=true -Xlog:gc*:file=/applog/gclog:tags -jar /home/app.jar"
		javaAgentArgs, customJvmArgs, dArgs := getJavaArgsFromJavaCmd(javaCmd)
		convey.So(reflect.DeepEqual([]string{"-javaagent:/rasp/rasp.jar"}, javaAgentArgs),
			convey.ShouldBeTrue)
		convey.So(reflect.DeepEqual([]string{"-Xlog:gc*:file=/applog/gclog:tags"}, customJvmArgs),
			convey.ShouldBeTrue)
		convey.So(reflect.DeepEqual([]string{"-DxArmor.backend.allowInsecure=true"}, dArgs),
			convey.ShouldBeTrue)
	})
	convey.Convey("Test get normal", t, func() {
		javaCmd := "java -jar -cp RuntimeServer /home/app.jar"
		javaAgentArgs, customJvmArgs, dArgs := getJavaArgsFromJavaCmd(javaCmd)
		convey.So(len(javaAgentArgs) == 0, convey.ShouldBeTrue)
		convey.So(len(customJvmArgs) == 0, convey.ShouldBeTrue)
		convey.So(len(dArgs) == 0, convey.ShouldBeTrue)
	})
}

func TestGetCustomJavaArgsFromScript(t *testing.T) {
	convey.Convey("Test not exist script", t, func() {
		patch := gomonkey.ApplyFunc(os.Stat, func(_ string) (os.FileInfo, error) {
			return nil, errors.New("error")
		})
		defer patch.Reset()

		javaArgs, err := getCustomJavaArgsFromScript(utils.Java8)
		convey.So(reflect.DeepEqual([]string{}, javaArgs), convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test run script success", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(os.Stat, func(_ string) (os.FileInfo, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(os.Chown, func(_ string, _, _ int) error {
				return nil
			}),
			gomonkey.ApplyFunc(os.Chmod, func(_ string, _ os.FileMode) error {
				return nil
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Run, func(_ *exec.Cmd) error {
				return nil
			}),
			gomonkey.ApplyFunc(exec.CommandContext, func(_ context.Context, _ string, _ ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc(workerutils.SetCmdCredByID, func(_ *exec.Cmd, _, _ int) error {
				return nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
				return []byte("java -javaagent:/apm.jar -DApm20=apm -XX:+UseContainerSupport -jar app.jar"), nil
			}),
		})
		defer patches.ResetAll()

		javaArgs, err := getCustomJavaArgsFromScript(utils.Java8)
		convey.So(reflect.DeepEqual([]string{"-javaagent:/apm.jar", "-DApm20=apm", "-XX:+UseContainerSupport"}, javaArgs), convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test chown err", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(os.Stat, func(_ string) (os.FileInfo, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(os.Chown, func(_ string, _, _ int) error {
				return errors.New("error")
			}),
		})
		defer patches.ResetAll()
		javaArgs, err := getCustomJavaArgsFromScript(utils.Java8)
		convey.So(len(javaArgs), convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test run script success with java17", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(os.Stat, func(_ string) (os.FileInfo, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(os.Chown, func(_ string, _, _ int) error {
				return nil
			}),
			gomonkey.ApplyFunc(os.Chmod, func(_ string, _ os.FileMode) error {
				return nil
			}),
			gomonkey.ApplyFunc((*exec.Cmd).Run, func(_ *exec.Cmd) error {
				return nil
			}),
			gomonkey.ApplyFunc(exec.CommandContext, func(_ context.Context, _ string, _ ...string) *exec.Cmd {
				return &exec.Cmd{}
			}),
			gomonkey.ApplyFunc(workerutils.SetCmdCredByID, func(_ *exec.Cmd, _, _ int) error {
				return nil
			}),
			gomonkey.ApplyFunc(ioutil.ReadFile, func(_ string) ([]byte, error) {
				return []byte("java -javaagent:/apm.jar -DApm20=apm -XX:+UseContainerSupport -jar app.jar"), nil
			}),
		})
		defer patches.ResetAll()

		javaArgs, err := getCustomJavaArgsFromScript(utils.Java17)
		convey.So(reflect.DeepEqual([]string{"-javaagent:/apm.jar", "-DApm20=apm", "-XX:+UseContainerSupport", "--add-opens=java.base/java.util=ALL-UNNAMED",
			"--add-opens=java.base/java.lang=ALL-UNNAMED", "--add-opens=java.base/java.net=ALL-UNNAMED",
			"--add-opens=java.base/java.io=ALL-UNNAMED", "--add-opens=java.base/java.math=ALL-UNNAMED",
			"--add-opens=java.base/java.time=ALL-UNNAMED", "--add-opens=java.base/java.text=ALL-UNNAMED ",
			"--enable-preview"}, javaArgs), convey.ShouldBeTrue)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test isFileOwnerSnuser", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(os.Stat, func(_ string) (os.FileInfo, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(os.Chown, func(_ string, _, _ int) error {
				return errors.New("error")
			}),
		})
		defer patches.ResetAll()
		MockUid = 1000
		ret := isFileOwnerSnuser(mockFile{})
		convey.So(ret, convey.ShouldBeFalse)
		MockUid = 1003
		ret = isFileOwnerSnuser(mockFile{})
		convey.So(ret, convey.ShouldBeTrue)
	})
}

func TestCustomJavaArgsWhenEnableRemoteDebug(t *testing.T) {
	patches := gomonkey.NewPatches()
	patches.ApplyFunc(workerutils.GetCPUMemory, func() (int64, int64) {
		return int64(300), int64(128)
	})
	patches.ApplyGlobalVar(&enableRemoteDebug, true)
	patches.ApplyGlobalVar(&remoteDebugArgs, "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8800")
	defer patches.Reset()

	rmConfig := &rmconfig.Config{RuntimeConfigs: map[string]rmconfig.RuntimeConfig{
		utils.Java8: {CustomArgs: []string{"CustomArgs"}, DisableTrace: []int64{512},
			Addition: map[string]string{"java8-128-jvm-options": "-XX:InitialRAMPercentage=25.0 -XX:MaxRAMPercentage=80.0 -XX:+UseSerialGC"}},
	}}

	jP := javaArgsPars{
		javaVersion: utils.Java8,
		layerPath:   "/home/layer",
		address:     "127.1.1.1",
		sockType:    "tcp",
		runningMode: constants.RunningModeNormal,
	}

	// cae function, no -Dlog4j2.configurationFile
	_ = os.Setenv(constants.BusinessType, constants.BusinessTypeCAE)
	// default jvm 8
	args1, _ := getJavaArgs(jP, rmConfig, true)
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0", "-XX:+UseSerialGC", "-XX:-TieredCompilation",
		"-XX:+HeapDumpOnOutOfMemoryError", args1[5], "-Xloggc:/home/sn/log/runtime/gc.log", "-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps",
		"-XX:+UseGCLogFileRotation", "-XX:NumberOfGCLogFiles=5", "-XX:GCLogFileSize=20M",
		"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8800",
		"-cp", "/home/snuser/runtime/java/*:/home/layer", "-Dio.netty.native.deleteLibAfterLoading=false",
		"com.huawei.services.runtime.rpc.RuntimeServer", "127.1.1.1", utils.Java8, "tcp"}, args1)

	_ = os.Setenv(constants.CustomJavaArgs, "{\"jvm\":\"-XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m\","+
		"\"-D\":\"-Dskywalking.agent.service_name=hdev_schedule-fdn -Dskywalking.logging.dir=/applog/logs/apm20.log\"}")
	args2, _ := getJavaArgs(jP, rmConfig, true)
	assert.Equal(t, []string{"-Dskywalking.agent.service_name=hdev_schedule-fdn", "-Dskywalking.logging.dir=/applog/logs/apm20.log",
		"-XX:MetaspaceSize=256m", "-XX:MaxMetaspaceSize=512m",
		"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8800",
		"-cp", "/home/snuser/runtime/java/*:/home/layer", "-Dio.netty.native.deleteLibAfterLoading=false",
		"com.huawei.services.runtime.rpc.RuntimeServer", "127.1.1.1", utils.Java8, "tcp"}, args2)

	// default jvm 8 when BusinessTypeCAE
	_ = os.Setenv(constants.CustomJavaArgs, "")
	args3, _ := getJavaArgs(jP, rmConfig, true)
	assert.Equal(t, []string{"-XX:InitialRAMPercentage=25.0", "-XX:MaxRAMPercentage=80.0", "-XX:+UseSerialGC",
		"-XX:-TieredCompilation", "-XX:+HeapDumpOnOutOfMemoryError", args3[5], "-Xloggc:/home/sn/log/runtime/gc.log",
		"-XX:+PrintGCDetails", "-XX:+PrintGCDateStamps", "-XX:+UseGCLogFileRotation",
		"-XX:NumberOfGCLogFiles=5", "-XX:GCLogFileSize=20M",
		"-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=8800",
		"-cp", "/home/snuser/runtime/java/*:/home/layer",
		"-Dio.netty.native.deleteLibAfterLoading=false",
		"com.huawei.services.runtime.rpc.RuntimeServer", "127.1.1.1", utils.Java8, "tcp"}, args3)
}
