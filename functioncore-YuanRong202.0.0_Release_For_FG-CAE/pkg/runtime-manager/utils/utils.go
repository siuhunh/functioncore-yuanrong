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

// Package utils defines utility tools
package utils

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	utilities "functioncore/pkg/common/utils"
	rmconfig "functioncore/pkg/runtime-manager/config"
	workerconfig "functioncore/pkg/worker/config"
)

const (
	shiftTwo = 2
	// Nodejs define runtime type Nodejs
	Nodejs = "nodejs"
	// Nodejs610 define runtime type Nodejs
	Nodejs610 = "nodejs6.10"
	// Nodejs810 define runtime type Nodejs
	Nodejs810 = "nodejs8.10"
	// Nodejs1016 define runtime type Nodejs
	Nodejs1016 = "nodejs10.16"
	// Nodejs1213 define runtime type Nodejs
	Nodejs1213 = "nodejs12.13"
	// Nodejs1418 define runtime type Nodejs
	Nodejs1418 = "nodejs14.18"
	// Nodejs1617 define runtime type Nodejs
	Nodejs1617 = "nodejs16.17"
	// Nodejs1815 define runtime type Nodejs
	Nodejs1815 = "nodejs18.15"
	// Nodejs2015 define runtime type Nodejs
	Nodejs2015 = "nodejs20.15"
	// Nodejs2218 define runtime type Nodejs
	Nodejs2218 = "nodejs22.18"
	// Nodejs2223 define runtime type Nodejs
	Nodejs2223 = "nodejs22.23"
	// Nodejs2422 define runtime type Nodejs
	Nodejs2422 = "nodejs24.22"
	// Python define runtime type Python
	Python = "python"
	// CSharp define runtime type CSharp
	CSharp = "c#"
	// Python27 define runtime type Python27
	Python27 = "python2.7"
	// Python36 define runtime type Python36
	Python36 = "python3.6"
	// Python39 define runtime type Python39
	Python39 = "python3.9"
	// Python310 define runtime type Python310
	Python310 = "python3.10"
	// Python311 define runtime type Python311
	Python311 = "python3.11"
	// Python312 define runtime type Python312
	Python312 = "python3.12"
	// Java8 define runtime type Java8
	Java8 = "java8"
	// Javax define runtime type Javax
	Javax = "javax"
	// Java11 define runtime type Java11
	Java11 = "java11"
	// Java17 define runtime type Java17
	Java17 = "java17"
	// Java21 define runtime type java21
	Java21 = "java21"
	// NetCore31 define runtime type CSharp
	NetCore31 = "c#(.net core 3.1)"
	// NetCore21 define runtime type CSharp
	NetCore21 = "c#(.net core 2.1)"
	// NetCore60 define runtime type CSharp
	NetCore60 = "c#(.net core 6.0)"
	// NetCore80 define runtime type CSharp
	NetCore80 = "c#(.net core 8.0)"
	// Dotnet31 define runtime type CSharp
	Dotnet31 = "dotnet3.1"
	// Dotnet60 define runtime type CSharp
	Dotnet60 = "dotnet6.0"
	// Dotnet80 define runtime type CSharp
	Dotnet80 = "dotnet8.0"
	// Dotnet21 define runtime type CSharp
	Dotnet21 = "/usr/bin/dotnet2.1"
	// Php define runtime type php
	Php = "php"
	// Php73 define runtime type php
	Php73 = "php7.3"
	// Php83 define runtime type php
	Php83 = "php8.3"
	// CangJie define runtime type cangjie
	CangJie = "cangjie"
	// CangJie10 define runtime type cangjie
	CangJie10 = "cangjie1.0"
	// CangJie0523 define runtime type cangjie0.52.3
	CangJie0523 = "cangjie0.52.3"
	// CangJie060x define runtime type cangjie0.60.x
	CangJie060x = "cangjie0.60.x"
	// CustomRuntimeRPC custom runtime rpc
	CustomRuntimeRPC   = "custom-runtime-rpc"
	tcpSocketType      = "tcp"
	unixSocketType     = "unix"
	rwPermFileMode     = 0600
	cgroupElementsLen  = 5
	containerIDLen     = 12
	mkdirPermFileMode  = 0640
	podNameMemoryIndex = 2
	podNameLen         = 6

	zombieState = 90
	memoryGap   = 1 // MB
)

// RuntimeProcessEntry runtime process entry
type RuntimeProcessEntry struct {
	SelfProcessEntry     *UnixProcessEntry
	SubProcessEntries    []*UnixProcessEntry
	OrphanProcessEntries []*UnixProcessEntry
}

// UnixProcessEntry unix process entry
type UnixProcessEntry struct {
	ParentProcessEntry  *UnixProcessEntry
	ChildProcessEntries map[int]*UnixProcessEntry
	Pid                 int
	Ppid                int
	UID                 int
}

// ProcessInfo -
type ProcessInfo struct {
	Pid   int
	Ppid  int
	UID   int
	State rune
}

// GetServerAddress get server address
func GetServerAddress(port int32) string {
	return fmt.Sprintf("%s:%d", "127.0.0.1", port)
}

// GetSelfAndSubAndOrphanProcessEntries Get all child processes (including grandchild processes) and all orphan
// processes of the specified pid
func GetSelfAndSubAndOrphanProcessEntries(uid, pid int) (*RuntimeProcessEntry, error) {
	processEntries, err := getProcessEntries()
	if err != nil {
		return nil, err
	}
	var (
		selfProcessEntry     *UnixProcessEntry
		subProcessEntries    = make([]*UnixProcessEntry, 0, 1)
		orphanProcessEntries = make([]*UnixProcessEntry, 0, 1)
	)
	for _, processEntry := range processEntries {
		if uid != -1 && processEntry.UID != uid {
			continue
		}
		if pid >= 0 && processEntry.Pid == pid {
			selfProcessEntry = processEntry
		}
		if processEntry.Ppid == 1 {
			// Orphan process
			orphanProcessEntries = append(orphanProcessEntries, processEntry)
			if processEntry.ChildProcessEntries != nil && len(processEntry.ChildProcessEntries) > 0 {
				orphanProcessEntries = appendSubProcessEntries(orphanProcessEntries, processEntry)
			}
			continue
		}
		if pid != -1 && processEntry.Ppid == pid {
			subProcessEntries = append(subProcessEntries, processEntry)
			if processEntry.ChildProcessEntries != nil && len(processEntry.ChildProcessEntries) > 0 {
				subProcessEntries = appendSubProcessEntries(subProcessEntries, processEntry)
			}
		}
	}
	runtimeProcessEntry := &RuntimeProcessEntry{
		SelfProcessEntry:     selfProcessEntry,
		SubProcessEntries:    subProcessEntries,
		OrphanProcessEntries: orphanProcessEntries,
	}
	return runtimeProcessEntry, nil
}

func getProcessEntries() (map[int]*UnixProcessEntry, error) {
	fileInfos, err := ioutil.ReadDir("/proc")
	if err != nil {
		log.GetLogger().Errorf("failed to list fileInfos of the dir /proc.")
		return nil, err
	}
	processEntries := make(map[int]*UnixProcessEntry, 1)
	for _, fileInfo := range fileInfos {
		info := GetFileInfo(fileInfo)
		if info.Pid == 0 && info.Ppid == 0 && info.UID == 0 {
			continue
		}
		processEntries = updateProcessEntry(processEntries, info.Pid, info.Ppid, info.UID)
	}
	return processEntries, nil
}

// GetFileInfo -
func GetFileInfo(fileInfo fs.FileInfo) ProcessInfo {
	fileName := fileInfo.Name()
	pid, err := strconv.Atoi(fileName)
	if err != nil {
		log.GetLogger().Debugf("fileName '%s' is not a int number", fileName)
		return ProcessInfo{Pid: 0, Ppid: 0, UID: 0, State: 0}
	}
	sysFileInfo := fileInfo.Sys()
	if sysFileInfo == nil {
		log.GetLogger().Warnf("failed to get file '%s' sys info.", fileName)
		return ProcessInfo{Pid: 0, Ppid: 0, UID: 0, State: 0}
	}
	sysInfo, ok := sysFileInfo.(*syscall.Stat_t)
	if !ok {
		log.GetLogger().Debugf("sysFileInfo of file '%s' can not be converted to syscall.Stat_t", fileName)
		return ProcessInfo{Pid: 0, Ppid: 0, UID: 0, State: 0}
	}
	puid := int(sysInfo.Uid)
	procStatFilePath := fmt.Sprintf("/proc/%d/stat", pid)
	dataBytes, err := ioutil.ReadFile(procStatFilePath)
	if err != nil {
		log.GetLogger().Warnf("failed to read file '%s'", procStatFilePath)
		return ProcessInfo{Pid: 0, Ppid: 0, UID: 0, State: 0}
	}
	// /proc/pid/stat demo："138 (python3) S 69 1 1 34822 1 4218880 5494 0 0 0 29 8 0 0 20 0 1 0 ..."
	data := string(dataBytes)
	lastRightParenthesisIndex := strings.LastIndex(data, ")")
	if !strings.HasPrefix(data, fmt.Sprintf("%d ", pid)) || lastRightParenthesisIndex == -1 {
		log.GetLogger().Warnf("invalid stat file '%s', content '%s'.", procStatFilePath, data)
		return ProcessInfo{Pid: 0, Ppid: 0, UID: 0, State: 0}
	}
	var (
		ppid  int
		state rune
	)
	_, err = fmt.Sscanf(string(dataBytes[lastRightParenthesisIndex+shiftTwo:]), "%c %d", &state, &ppid)
	if err != nil {
		log.GetLogger().Warnf("invalid stat file '%s', content '%s'.", procStatFilePath, data)
		return ProcessInfo{Pid: 0, Ppid: 0, UID: 0, State: 0}
	}
	if state == rune(zombieState) {
		log.GetLogger().Warnf("pid %d is a zombie process, state %d", pid, state)
		return ProcessInfo{Pid: 0, Ppid: 0, UID: 0, State: 0}
	}
	return ProcessInfo{Pid: pid, Ppid: ppid, UID: puid, State: state}
}

func updateProcessEntry(processEntries map[int]*UnixProcessEntry,
	pid int, ppid int, puid int) map[int]*UnixProcessEntry {
	if processEntries == nil {
		processEntries = make(map[int]*UnixProcessEntry, 1)
	}
	processEntry, exist := processEntries[pid]
	if exist {
		processEntry.Pid = pid
		processEntry.Ppid = ppid
		processEntry.UID = puid
		processEntry.UID = puid
	} else {
		processEntry = &UnixProcessEntry{Pid: pid, Ppid: ppid, UID: puid}
		processEntries[pid] = processEntry
	}
	parentProecessEntry, exist := processEntries[ppid]
	if exist {
		if parentProecessEntry.ChildProcessEntries == nil {
			parentProecessEntry.ChildProcessEntries = make(map[int]*UnixProcessEntry)
		}
		parentProecessEntry.ChildProcessEntries[pid] = processEntry
	} else {
		parentProecessEntry = &UnixProcessEntry{Pid: ppid}
		processEntries[ppid] = parentProecessEntry
	}
	processEntry.ParentProcessEntry = parentProecessEntry
	return processEntries
}

func appendSubProcessEntries(result []*UnixProcessEntry, processEntry *UnixProcessEntry) []*UnixProcessEntry {
	for _, subProcessEntry := range processEntry.ChildProcessEntries {
		result = append(result, subProcessEntry)
		if subProcessEntry.ChildProcessEntries != nil && len(subProcessEntry.ChildProcessEntries) > 0 {
			result = appendSubProcessEntries(result, subProcessEntry)
		}
	}
	return result
}

// AdjustRuntimeProcess -
func AdjustRuntimeProcess(process *os.Process) error {
	if err := fixRuntimeOOMScoreAdj(process); err != nil {
		log.GetLogger().Errorf("failed to fix runtime oom_score_adj for pid: %d, %s", process.Pid, err.Error())
		return err
	}

	log.GetLogger().Infof("adjustRuntime | pid %d", process.Pid)

	return nil
}

// fixRuntimeOOMScoreAdj resets child process's oom_score_adj to the default value (992). otherwise child processes
// will inherit our oom_score_adj.
func fixRuntimeOOMScoreAdj(process *os.Process) error {
	pid := strconv.Itoa(process.Pid)
	return setOOMScoreAdj(pid, "992")
}

// DisableOOM -
func DisableOOM() error {
	// linux system use -1000 to disable oom kill of a process
	return setOOMScoreAdj("self", "-1000")
}

func setOOMScoreAdj(pid, value string) error {
	fileName := fmt.Sprintf("/proc/%s/oom_score_adj", pid)
	if err := ioutil.WriteFile(fileName, []byte(value), 0); err != nil {
		log.GetLogger().Errorf("failed to write file: %s, %s", fileName, err.Error())
		return err
	}
	return nil
}

// AppendToFile append string to file
func AppendToFile(fileName string, line string) error {
	file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(line)
	return err
}

var podName = ""

// default value
var memorySize = "256"

var defaultMemorySizeInt = 256

// ExtractEnv extract environment
func ExtractEnv(version string, debugEnv []string, rmConfig *rmconfig.Config) []string {
	var env []string
	if env = getEnv(version, rmConfig); env != nil {
		log.GetLogger().Debugf("set env: %+v", env)
	} else {
		// if cmd env is nil, runtime will use runtime manager env
		// so set runtime env as empty string slice
		env = []string{}
	}
	if dcCallStackEnv := getDcCallStackEnv(version, rmConfig); dcCallStackEnv != nil {
		env = append(env, dcCallStackEnv...)
		log.GetLogger().Infof("add dcCallStackEnv %+v for %s", env, version)
	}

	if debugEnv != nil {
		env = append(env, debugEnv...)
		log.GetLogger().Debugf("debug cmd.env is %+v", env)
	}
	if podName == "" {
		podName = os.Getenv(constants.HostNameEnvKey)
		if podName != "" {
			// pool22-300-128-fusion-68ddd95f44-5tj4z
			// pool34-3400-4096-fusion-5444c8f48b-ncpjl
			podNameArr := strings.Split(podName, "-")
			if len(podNameArr) == podNameLen {
				memorySize = podNameArr[podNameMemoryIndex]
			}
		}
	}

	memorySizeInt, err := strconv.Atoi(memorySize)
	if err != nil {
		memorySizeInt = defaultMemorySizeInt
	}
	// less than memory limit, hardLimit - memoryGap - 1
	memorySizeInt -= memoryGap
	memorySizeInt--

	gcLimitSizeInt := memorySizeInt >> 1

	log.GetLogger().Debugf("cjHeapSize is: %sMB", memorySize)
	libraryPath := getLibraryPath(version)

	env = append(env,
		// inherit system encoding env
		fmt.Sprintf("LANG=%s", os.Getenv("LANG")),
		fmt.Sprintf("%s=%s", "LD_LIBRARY_PATH", libraryPath),
		fmt.Sprintf("%s=%s", constants.RuntimeLogLevel, os.Getenv(constants.RuntimeLogLevel)),
		fmt.Sprintf("%s=%s", constants.RuntimeLogPath, os.Getenv(constants.RuntimeLogPath)),
		fmt.Sprintf("%s=%s", constants.MaxRequestBodySize, os.Getenv(constants.MaxRequestBodySize)),
		fmt.Sprintf("%s=%s", constants.DataSystemPort, constants.DefaultDataSystemPort),
		fmt.Sprintf("%s=%s", constants.DataSystemHost, os.Getenv("HOST_IP")),
		fmt.Sprintf("%s=%s", constants.DataSystemJniPath, "/tmp"),
		fmt.Sprintf("%s=%s", constants.BusinessType, os.Getenv(constants.BusinessType)),
		fmt.Sprintf("%s=%s", "cjHeapSize", strconv.Itoa(memorySizeInt)+"MB"),
		fmt.Sprintf("%s=%s", "cjGCThreshold", strconv.Itoa(gcLimitSizeInt)+"MB"))

	if strings.Contains(version, CangJie) && len(rmConfig.RuntimeConfigs[version].CustomArgs) != 0 {
		env = append(env, rmConfig.RuntimeConfigs[version].CustomArgs...)
	}
	return env
}

func getLibraryPath(version string) string {
	runtimeLibPath := ""
	if strings.Contains(version, CangJie10) {
		runtimeLibPath = constants.CangJie10LdLibraryPath
	}
	if strings.Contains(version, CangJie0523) {
		runtimeLibPath = constants.CangJie0523LdLibraryPath
	}
	if strings.Contains(version, CangJie060x) {
		runtimeLibPath = constants.CangJie060xLdLibraryPath
	}
	if strings.Contains(version, CustomRuntimeRPC) {
		runtimeLibPath = "/opt/function/runtime/java8/rtsp/jre/lib/amd64/server/:" +
			"/opt/function/runtime/java8/rtsp/jre/lib/amd64/:"
	}
	return os.ExpandEnv(fmt.Sprintf("%s%s", runtimeLibPath, constants.CommonLdLibraryPath))
}

func getEnv(version string, rmConfig *rmconfig.Config) []string {
	if strings.HasPrefix(version, Python) {
		return []string{"LANG=en_US.UTF-8"}
	}
	if !strings.HasPrefix(version, Nodejs) {
		return nil
	}
	s := strings.Split(version, ".")[0]
	var env []string
	env = append(env, "NODE_VERSION="+strings.Replace(s, Nodejs, "", 1))
	if cfg, ok := rmConfig.RuntimeConfigs[version]; ok {
		env = append(env, "MASTER_ADDRESS="+cfg.TraceMasterAddress)
	}
	nodePath := getNodePath(version)
	if nodePath != "" {
		env = append(env, "NODE_PATH="+nodePath)
		return env
	}
	log.GetLogger().Warnf("NODE_PATH is not set")
	return env
}

func getDcCallStackEnv(version string, rmConfig *rmconfig.Config) []string {
	log.GetLogger().Debugf("start to get DcCallStack env for %s", version)
	cfg, ok := rmConfig.RuntimeConfigs[version]
	if !ok {
		log.GetLogger().Warnf("no RuntimeConfigs for %s", version)
	}

	if ok && cfg.Addition != nil {
		return []string{"EnableDcCallStack=" + cfg.Addition["enableDcCallStack"],
			"DcCallStackLayer=" + cfg.Addition["dcCallStackLayer"]}
	}
	return nil
}

// getNodePath sets NODE_PATH for Third-Party Dependency
func getNodePath(version string) string {
	nodePath := os.Getenv("NODE_PATH")
	absNodePath, err := filepath.Abs(nodePath)
	if err != nil {
		log.GetLogger().Errorf("get absolute node path error: %s", err.Error())
		return ""
	}
	nodePaths := strings.Split(absNodePath, ":")
	for _, path := range nodePaths {
		if ok := strings.Contains(path, strings.TrimPrefix(version, "nodejs")); ok {
			return path
		}
	}
	return ""
}

// GetExecPath -
func GetExecPath(version string) (string, error) {
	if strings.Contains(version, NetCore31) {
		version = Dotnet31
	}
	if strings.Contains(version, NetCore60) {
		version = Dotnet60
	}
	if strings.Contains(version, NetCore80) {
		version = Dotnet80
	}
	if strings.Contains(version, NetCore21) {
		return Dotnet21, nil
	}
	if strings.Contains(version, CangJie10) {
		return "/usr/bin/bash", nil
	}
	if strings.Contains(version, CangJie0523) {
		return "/usr/bin/bash", nil
	}
	if strings.Contains(version, CangJie060x) {
		return "/usr/bin/bash", nil
	}
	if strings.Contains(version, CustomRuntimeRPC) {
		return "/usr/bin/bash", nil
	}
	if strings.Contains(version, Javax) {
		version = Java8
	}
	exePath, err := exec.LookPath(version)
	if err != nil {
		log.GetLogger().Errorf("failed to get execution file path with language: %s, error: %s", version, err.Error())
		return "", err
	}
	return exePath, nil
}

func isCheckpointRestoreMode(runningMode string) bool {
	return runningMode == constants.RunningModeCheckpoint ||
		runningMode == constants.RunningModeRestore
}

// GetSocketType -
func GetSocketType(conf *workerconfig.RuntimeConfig, version string) string {
	if conf == nil || conf.RMConfig.RuntimeConfigs == nil || isCheckpointRestoreMode(conf.RunningMode) {
		return tcpSocketType
	}
	if runtimeConfig, ok := conf.RMConfig.RuntimeConfigs[version]; ok && runtimeConfig.SocketType ==
		unixSocketType {
		return unixSocketType
	}
	return tcpSocketType
}

// AppendDomainNameToHostFile append domain name to host file
func AppendDomainNameToHostFile(dnsDomainNames map[string][]string) error {
	utilities.HostFile.Mutex.Lock()
	defer utilities.HostFile.Mutex.Unlock()
	file, err := os.OpenFile("/etc/hosts", os.O_APPEND|os.O_WRONLY|os.O_CREATE, rwPermFileMode)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for domainName, domainIPs := range dnsDomainNames {
		for _, domainIP := range domainIPs {
			content := "\n" + domainIP + " " + domainName + "\n"
			if _, err = writer.WriteString(content); err != nil {
				return err
			}
		}
	}
	if err := utilities.HostFile.SaveHostFileInfo(); err != nil {
		return err
	}
	return writer.Flush()
}

// PrintContainerID print runtime-manager container id
func PrintContainerID() error {
	file, err := os.Open("/proc/self/cgroup")
	if err != nil {
		return errors.New("failed to open cgroup file")
	}
	defer file.Close()
	// 13:files:/kubepods/burstable/pod55ff84d3-9093-44ba-9e78-c6caddccc5d4/2b70991c1e4aa0a0ba9d68d6695470cdba20f9521
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return errors.New("failed to scan cgroup file: " + err.Error())
		}
		elements := strings.Split(scanner.Text(), "/")
		if len(elements) < cgroupElementsLen {
			continue
		}
		idPart := elements[cgroupElementsLen-1]
		if len(idPart) >= containerIDLen {
			log.GetLogger().Infof("container id is: %s", idPart[:containerIDLen])
			return nil
		}
	}
	return errors.New("no valid container id pattern found")
}

// GetPreservePath -
func GetPreservePath(logPaths []string) string {
	var preservePath string
	if utilities.IsCAEFunc(os.Getenv(constants.BusinessType)) {
		preservePath = constants.FastFreezeCAEPreservePath
		if len(logPaths) > 0 {
			preservePath += ":" + strings.Join(getLogPathsDir(logPaths), ":")
		}
	} else {
		preservePath = constants.FastFreezePreservePath
	}
	return preservePath
}

func getLogPathsDir(logPaths []string) []string {
	logPathsDirMap := make(map[string]struct{})
	for _, logPath := range logPaths {
		logPathsDirMap[filepath.Dir(logPath)] = struct{}{}
	}

	var logPathsDir []string
	for logPath := range logPathsDirMap {
		logPathsDir = append(logPathsDir, logPath)
	}
	return logPathsDir
}
