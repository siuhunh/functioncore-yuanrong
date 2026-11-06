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

// Package process is runtime manager process
package process

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"functioncore/pkg/cli/constant"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/oom"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/urnutils"
	commonutil "functioncore/pkg/common/utils"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/pkg/runtime-manager/extends/debug"
	"functioncore/pkg/runtime-manager/extends/usergroup"
	"functioncore/pkg/runtime-manager/functionlog"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker"
	workerconfig "functioncore/pkg/worker/config"
	rm "functioncore/pkg/worker/runtime"
	workerutils "functioncore/pkg/worker/utils"
)

var (
	// Manager manage processes started by runtime-manager
	Manager *processMgr

	// ExitOnce - to make sure ExitSignal can be closed only once
	ExitOnce sync.Once
	// ExitSignal closed when runtime manager should exit
	ExitSignal = make(chan struct{})

	// ExitSignalOnce -
	ExitSignalOnce sync.Once

	// ExitSignalOnNoRequest receive exit signal from worker to end graceful shut down
	// it means no request in worker
	ExitSignalOnNoRequest = make(chan struct{})

	inspectProcessChannel = make(chan struct{})

	readMemoryInternal = 2 * time.Millisecond

	readCPUInterval = parseReadCPUInterval()

	runtimeError types.AtomicErrorCode

	// ClearChan -
	ClearChan = make(chan bool, 1)

	killProcessFailedTimes = 0

	// runtimes startup when generalizing pod
	generalizingStartupRuntimes = []string{worker.CustomRuntimeLanguage,
		worker.WebRuntimeLanguage, worker.GOV1RuntimeLanguage}

	logReportEnable             = true
	runtimeProcessExitedReason  = "(core dumped)"
	nodejsOOMCoreDumpString     = "FatalProcessOutOfMemory"
	csharpRuntimeExitedOutput   = ""
	runtimeManagerMemoryWarning = false
	runtimeMemoryWarning        = false

	pageSize        = uint64(os.Getpagesize())
	cgroupCPUParser *monitor.Parser
	systemCPUParser *monitor.Parser
	latestKillPid   int
)

const (
	startRuntimeRetryTimes           = 10
	maxKillProcessFailedTimes        = 10
	startRuntimeRetryInterval        = 100 * time.Millisecond
	keepsCrashingGracePeriod         = 1 * time.Second
	memoryGap                        = 1  // MB
	runtimeManagerMemOverride        = 40 // MB
	runtimeManagerMemSuicidePercent  = 0.8
	readRuntimeManagerMemoryInternal = 5 * time.Second
	bytesToMB                        = 1024 * 1024
	runtimeMemWarningPercent         = 0.8

	nodejsPathFormat       = "/opt/function/runtime/%s/rtsp/nodejs/bin/node"
	pythonPathFormat       = "/opt/function/runtime/%s/rtsp/python/bin/%s"
	javaPathFormat         = "/opt/function/runtime/%s/rtsp/jre/bin/java"
	phpPathFormat          = "/opt/function/runtime/%s/rtsp/php/bin/php"
	csharpPathFormat       = "/opt/function/runtime/%s/rtsp/dotnet/dotnet"
	binPath                = "/usr/bin"
	defaultReadCPUInterval = 2 * time.Millisecond

	defaultGetOOMTimeout = 20 * time.Second

	hostFilePath = "/etc/hosts"
	hostFilePerm = 0644

	optRuntimeDir    = "/opt/function/runtime"
	runtimeSnuserDir = "/home/snuser"

	customRuntimeRPCParamsLen = 2
	customRuntimeRPCCmdLen    = 2

	readNetWorkInternal = 100 * time.Millisecond
)

type mode byte

const (
	// RouterEtcdConnection alarm type
	pool mode = iota
	restore
	restoreDead
	normal
)

type processMgr struct {
	ConnectionWithWorker *tcpmsg.SocketConn
	CurrentRuntime       *RuntimeProcess
	CurrentExtension     []*ExtensionProcess
	conf                 *workerconfig.Configuration
	RuntimeCreator       runtimeCreateStrategy
	ExtensionCreator     runtimeCreateStrategy
	// key: version string,  value: is process pointer
	Processes          sync.Map
	currentVersion     string
	currentMode        mode
	EnableCPUWatchLoop bool
}

func init() {
	Manager = &processMgr{
		conf:             &workerconfig.Configuration{},
		RuntimeCreator:   &fusionRuntime{},
		CurrentRuntime:   &RuntimeProcess{},
		CurrentExtension: []*ExtensionProcess{},
		currentMode:      pool,
	}
	initParser()
}

func initParser() {
	var err error
	cgroupCPUParser, err = monitor.NewCGroupCPUParser()
	if err != nil {
		log.GetLogger().Warnf("failed to create cgroup cpu parser, err: %s", err.Error())
	}
	systemCPUParser, err = monitor.NewSystemCPUParser()
	if err != nil {
		log.GetLogger().Warnf("failed to create system cpu parser, err: %s", err.Error())
	}
}

func getLogPolicy() []string {
	if Manager != nil && Manager.conf != nil {
		return Manager.conf.LogPolicy.LogPaths
	}
	return nil
}

func parseReadCPUInterval() time.Duration {
	if val, err := strconv.Atoi(os.Getenv("READ_CPU_INTERVAL")); err == nil {
		fmt.Println("read cpu interval is set to ", val)
		return time.Duration(val) * time.Millisecond
	}
	return defaultReadCPUInterval
}

func startRuntimeProcess(uid int, gid int, reqBody *runtimemanagerrequest.InitRequestBody) (int, error) {
	isNeedRestart := false
	var debugEnv []string
	var err error
	debugger, envMap := debug.ExtractDebugTag(Manager.conf, Manager.currentVersion)
	if debugger != "" {
		isNeedRestart = true
		debugEnv, err = debug.GetDebugPara(debugger, envMap)
		if err != nil {
			log.GetLogger().Errorf("failed to get get debug para, error: %s", err.Error())
			return 0, err
		}
	}
	if uid != constants.DefaultRuntimeUID || gid != constants.DefaultRuntimeGID ||
		Manager.conf.RunningMode == constants.RunningModeRestore {
		isNeedRestart = true
		if err = usergroup.ChownRuntimeLogFile(uid, gid); err != nil {
			return 0, err
		}
	}

	if isNeedRestart {
		return restartRuntimeProcess(uid, gid, debugEnv, reqBody)
	}

	return getOrStartDefaultRuntimeProcess(reqBody)
}

// if RuntimeEnvs contain func-JAVA_TOOL_OPTIONS and the value contain agent,
// then replace CustomArgs with value of func-JAVA_TOOL_OPTIONS
func appendCustomArgs(reqBody *workerconfig.RequestBody) (bool, error) {
	debugger, _ := debug.ExtractDebugTag(Manager.conf, Manager.currentVersion)
	if debugger != "" {
		return false, nil
	}
	containedENV := func() (string, bool) {
		for _, env := range reqBody.Envs.RuntimeEnvs {
			if strings.Contains(env, "func-JAVA_TOOL_OPTIONS") ||
				strings.Contains(env, "func-CANGJIE_CUSTOM_ARGS") {
				return env, true
			}
		}
		return "", false
	}

	switch {
	case strings.Contains(reqBody.Conf.Language, constant.Java):
		env, ok := containedENV()
		if !ok {
			return false, nil
		}
		if runtimeConf, ok := Manager.conf.RuntimeConfig.RMConfig.RuntimeConfigs[reqBody.Version]; ok {
			customArgs := strings.Fields(env[strings.Index(env, "=")+1:])
			if !javaToolOptionsValid(customArgs) {
				return false, errors.New("java_tool_options parameter error")
			}
			runtimeConf.CustomArgs = customArgs
			Manager.conf.RuntimeConfig.RMConfig.RuntimeConfigs[reqBody.Version] = runtimeConf
			return true, nil
		}
	case strings.Contains(reqBody.Conf.Language, utils.CangJie):
		env, ok := containedENV()
		if !ok {
			return false, nil
		}
		if runtimeConf, ok := Manager.conf.RuntimeConfig.RMConfig.RuntimeConfigs[reqBody.Version]; ok {
			customArgs := strings.Split(env[strings.Index(env, "=")+1:], ";")
			runtimeConf.CustomArgs = customArgs
			Manager.conf.RuntimeConfig.RMConfig.RuntimeConfigs[reqBody.Version] = runtimeConf
			return true, nil
		}
	default:
		return false, nil
	}
	return false, nil
}

func javaToolOptionsValid(customArgs []string) bool {
	hasJavaagent := false
	if len(customArgs) == 0 {
		return true
	}
	for _, envArg := range customArgs {
		if strings.HasPrefix(envArg, "-javaagent") {
			if !strings.Contains(envArg, "skywalking-agent.jar") {
				return false
			}
			hasJavaagent = true
		}
		if !strings.HasPrefix(envArg, "-javaagent") && !strings.HasPrefix(envArg, "-Dskywalking") {
			return false
		}
	}
	return hasJavaagent
}

type prepareRestartPars struct {
	uid      int
	gid      int
	debugEnv []string
}

func prepareRestartWhenStopRuntimeProcess() (*prepareRestartPars, error) {
	uid, gid, err := usergroup.CheckID(Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.UserID,
		Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.GroupID)
	if err != nil {
		return nil, err
	}
	if uid != constants.DefaultRuntimeUID || gid != constants.DefaultRuntimeGID ||
		Manager.conf.RunningMode == constants.RunningModeRestore {
		if err = usergroup.ChownRuntimeLogFile(uid, gid); err != nil {
			return nil, err
		}
	}
	return &prepareRestartPars{
		uid: uid,
		gid: gid,
	}, nil
}

// StopRuntimeProcess -
func StopRuntimeProcess(msg *tcpmsg.Message) (int, error) {
	reqBody, err := getRequestBody(msg)
	if err != nil {
		log.GetLogger().Errorf("failed to get request body")
		return 0, err
	}
	Manager.currentVersion = reqBody.Version
	Manager.conf = reqBody.Conf
	Manager.EnableCPUWatchLoop = Manager.conf.RMConfig.EnableCPUWatchLoop
	buildRuntimeCreator(reqBody.Version)
	urnutils.SetSeparator(Manager.conf.FunctionNameSeparator)
	enableGCLog := Manager.conf.RuntimeConfig.EnableGCLog
	containOptions, err := appendCustomArgs(reqBody)
	if err != nil {
		log.GetLogger().Errorf("error when append custom args: %s", err.Error())
		return snerror.FunctionExceptionCode, err
	}
	// stop all runtime processes except the received version
	if isCheckPointRestore() {
		// checkpoint/restore mode needs to stop all runtime
		log.GetLogger().Infof("checkpoint/restore mode stop all runtime processes")
		err = stopRedundancyProcesses(constants.NotExistVersion)
	} else {
		go func() {
			if err := processSoftLink(Manager.currentVersion); err != nil {
				log.GetLogger().Warnf(err.Error())
			}
		}()
		err = stopRedundancyProcesses(Manager.currentVersion)
		if containOptions || (enableGCLog && strings.Contains(Manager.conf.Language, constant.Java)) {
			par, err := prepareRestartWhenStopRuntimeProcess()
			if err != nil {
				return 0, err
			}
			_, err = restartRuntimeProcess(par.uid, par.gid, par.debugEnv, &runtimemanagerrequest.InitRequestBody{})
		}
	}

	if err != nil {
		log.GetLogger().Errorf("error when stop redundancy runtime processes: %s", err.Error())
		return 0, err
	}

	if len(reqBody.Conf.DNSDomainNames) == 0 {
		return 0, nil
	}
	// append dns ip list to /etc/hosts
	if err = utils.AppendDomainNameToHostFile(reqBody.Conf.DNSDomainNames); err != nil {
		log.GetLogger().Errorf("failed to append domain name to /etc/hosts file with error: %s", err.Error())
		return 0, err
	}
	go watchHostFileLoop()
	log.GetLogger().Infof("succeed to append domain names:%+v to /etc/hosts file", reqBody.Conf.DNSDomainNames)

	return 0, nil
}

func isCheckPointRestore() bool {
	return Manager.conf.RunningMode == constants.RunningModeCheckpoint ||
		Manager.conf.RunningMode == constants.RunningModeRestore
}

// CheckpointRuntimeByFastFreeze -
func CheckpointRuntimeByFastFreeze() (int, error) {
	log.GetLogger().Info("start to CheckpointRuntimeByFastFreeze")
	defer backupFastfreezeLogs()
	createOptions := types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, Manager.conf)
	checkpointArgs := []string{constants.FastFreezeCheckpointScriptPath}
	criuOpts := os.Getenv(constants.CheckpointRestoreOpts)
	if criuOpts != "" {
		log.GetLogger().Infof("checkpoint configured criuOpts is %s", criuOpts)
		checkpointArgs = append(checkpointArgs, criuOpts)
	}
	log.GetLogger().Infof("execute checkpoint script")
	runCmd := exec.Command("sh", checkpointArgs...)
	if err := workerutils.SetCmdCredByID(runCmd, createOptions.UID, createOptions.GID); err != nil {
		log.GetLogger().Errorf("failed to set uid and gid with error: %s", err.Error())
		return 1, err
	}
	runCmd.Stdout = os.Stdout
	runCmd.Stderr = os.Stderr
	if err := runCmd.Run(); err != nil {
		log.GetLogger().Errorf("start checkpoint error: %s", err.Error())
		printFastfreezeLog()
		return 1, fmt.Errorf("start checkpoint error: %s", err)
	}
	log.GetLogger().Infof("fastfreezeExec command checkpoint finished")
	return 0, nil
}

func buildRuntimeCreator(version string) {
	switch version {
	case worker.GOV1RuntimeLanguage:
		Manager.RuntimeCreator = &goV1Runtime{}
	case worker.WebRuntimeLanguage:
		Manager.RuntimeCreator = &webRuntime{}
	case worker.CustomRuntimeLanguage:
		Manager.RuntimeCreator = &customRuntime{}
	default:
		Manager.RuntimeCreator = &fusionRuntime{}
	}
}

func restartRuntimeProcess(uid, gid int, debugEnv []string, reqBody *runtimemanagerrequest.
	InitRequestBody) (int, error) {
	// stop current runtime process
	if err := stopSpecRuntimeProcess(Manager.currentVersion); err != nil {
		log.GetLogger().Errorf("error when stop the specific runtime process %s: %s",
			Manager.currentVersion, err.Error())
		return 0, err
	}
	log.GetLogger().Infof("begin to restart runtime, uid: %d, gid: %d, debugEnv: %v, version: %s",
		uid, gid, debugEnv, Manager.currentVersion)
	err := CreateAndSetProcess(types.NewCreateOptions(uid, gid,
		Manager.conf, types.WithVersion(Manager.currentVersion), types.WithDebugEnv(debugEnv)).
		WithIsMcpFunction(reqBody.IsMcpFunction).WithNPMConfigRegistry(reqBody.NPMConfigRegistry).
		WithUVIndexURL(reqBody.UVIndexURL))
	if err != nil {
		if snErr, ok := err.(snerror.SNError); ok {
			return snErr.Code(), snErr
		}
		return 0, err
	}
	if !isGeneralizingStartupRuntime(Manager.currentVersion) {
		return tcpmsg.RuntimeRestarted, nil
	}
	return tcpmsg.MessageWithSuccessResponse, nil
}

// CreateAndSetProcess -
func CreateAndSetProcess(options *types.CreateOptions) error {
	process, err := create(options)
	if err != nil {
		return err
	}
	Manager.CurrentRuntime = &RuntimeProcess{
		Process:  process,
		UID:      options.UID,
		Gid:      options.GID,
		Version:  Manager.currentVersion,
		Status:   tcpmsg.RuntimeProcessStatusRunning,
		DebugEnv: options.DebugEnv,
	}
	log.GetLogger().Infof("restartRuntime | pid %d", Manager.CurrentRuntime.Process.Pid)
	return nil
}

// InitRuntimeProcess -
func InitRuntimeProcess(reqBody *runtimemanagerrequest.InitRequestBody) (int, error) {
	uid, gid, err := usergroup.CheckID(Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.UserID,
		Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.GroupID)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err == nil {
			go watchingRuntimeMemoryUsedLoop(Manager.conf.ResourceMetaData.Mem, Manager.currentVersion)
			go watchingRuntimeManagerMemoryUsedLoop(Manager.conf.ResourceMetaData.Mem)
			go watchingRuntimeCaeMetricsLoop()
			if Manager.EnableCPUWatchLoop {
				go watchingRuntimeCPUUsedLoop()
			}
			oom.StartOOMDetection()
			runtime.GC()
		}
	}()
	if err = monitorRuntimeDirMonitor(); err != nil {
		log.GetLogger().Errorf("failed to mount root, error: %s", err.Error())
		return 0, err
	}

	if Manager.conf.RunningMode == constants.RunningModeRestore {
		code, err := restoreRuntimeProcess(uid, gid)
		if err != nil {
			log.GetLogger().Errorf("failed to restore runtime process: %s", err.Error())
			return code, err
		}
		return code, nil
	}
	return startRuntimeProcess(uid, gid, reqBody)
}

func monitorRuntimeDirMonitor() error {
	monitorDir := []string{
		runtimeSnuserDir,
		optRuntimeDir,
	}
	stopCh := signals.WaitForSignal()
	if _, err := diskusage.NewRuntimeMonitor(monitorDir, stopCh, diskusage.AddProcessHooks(func(event fsnotify.Event) {
		if event.Op&fsnotify.Chmod > 0 {
			log.GetLogger().Infof("modified %+v", event)
			runtimeError.Override(snerror.RuntimeDirForbidden)
			if err := killCurrentProcess(); err != nil {
				log.GetLogger().Warnf("failed to kill process %d, the process may have exited",
					Manager.CurrentRuntime.Process.Pid)
			}
		}
	})); err != nil {
		log.GetLogger().Errorf("failed to new runtime dir monitor: %s", err.Error())
		return err
	}
	return nil
}

func getOrStartDefaultRuntimeProcess(reqBody *runtimemanagerrequest.InitRequestBody) (int, error) {
	var process *os.Process
	var err error
	options := types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, Manager.conf,
		types.WithVersion(Manager.currentVersion)).WithIsMcpFunction(reqBody.IsMcpFunction).
		WithNPMConfigRegistry(reqBody.NPMConfigRegistry).WithUVIndexURL(reqBody.UVIndexURL)
	rspCode := tcpmsg.MessageWithSuccessResponse
	runningMode := Manager.conf.RunningMode
	proc, exist := Manager.Processes.Load(Manager.currentVersion)
	if !exist || proc == nil {
		for i := 0; i < startRuntimeRetryTimes; i++ {
			process, err = create(options)
			// due to docker namespace limits, Fastfreeze can only run once in the same docker, there is no need to retry
			if err == nil || runningMode == constants.RunningModeCheckpoint {
				break
			}
			time.Sleep(startRuntimeRetryInterval)
			log.GetLogger().Infof("begin retry for: %d times", i)
		}
		if err != nil {
			log.GetLogger().Warnf("failed to start runtime, version: %s, error: %s", Manager.currentVersion, err)
			if snErr, ok := err.(snerror.SNError); ok {
				return snErr.Code(), snErr
			}
			return 0, err
		}
		Manager.Processes.Store(Manager.currentVersion, process)
		// checkpoint pods don't use pool pod, no need to restart
		if !isGeneralizingStartupRuntime(Manager.currentVersion) && runningMode != constants.RunningModeCheckpoint {
			rspCode = tcpmsg.RuntimeRestarted
		}
	} else {
		var ok bool
		process, ok = proc.(*os.Process)
		if !ok {
			return 0, errors.New("process in Processes is invalid: " + Manager.currentVersion)
		}
	}

	Manager.CurrentRuntime = &RuntimeProcess{
		Process:  process,
		UID:      constants.DefaultRuntimeUID,
		Gid:      constants.DefaultRuntimeGID,
		Version:  Manager.currentVersion,
		Status:   tcpmsg.RuntimeProcessStatusRunning,
		DebugEnv: []string{},
	}
	return rspCode, nil
}

func isGeneralizingStartupRuntime(version string) bool {
	for _, generalizingStartupRuntime := range generalizingStartupRuntimes {
		if generalizingStartupRuntime == version {
			return true
		}
	}
	return false
}

func getRequestBody(msg *tcpmsg.Message) (*workerconfig.RequestBody, error) {
	reqType := &workerconfig.RequestBody{}
	err := json.Unmarshal(msg.Body, reqType)
	if err != nil {
		return nil, err
	}
	return reqType, nil
}

// PreStartRuntimeProcesses pre starts runtime processes
func PreStartRuntimeProcesses(runtimeManagerConf *rmconfig.Config) {
	// only the fusion runtime has pre-boot
	// the default RuntimeCreator is fusion runtime
	for version, runtimeConfig := range runtimeManagerConf.RuntimeConfigs {
		if !workerutils.IsRuntimeEnable(&runtimeConfig) ||
			isGeneralizingStartupRuntime(version) || strings.Contains(version, utils.CSharp) {
			continue
		}
		config := &workerconfig.Configuration{}
		config.RuntimeConfig.RMConfig = *runtimeManagerConf
		_, err := create(types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, config,
			types.WithVersion(version)))
		if err != nil {
			continue
		}
		log.GetLogger().Infof("startRuntime | %s | %d", version, runtimeConfig.Port)
	}
}

func create(options *types.CreateOptions) (*os.Process, error) {
	if Manager.conf != nil && Manager.conf.RunningMode == constants.RunningModeRestore && Manager.currentMode == pool {
		log.GetLogger().Infof("start runtime by restore and change mode to restore")
		Manager.currentMode = restore
	}
	if Manager.conf.RunningMode == constants.RunningModeCheckpoint {
		// backup the fastfreezelog to runtime log path which will be stored on the host machine
		defer backupFastfreezeLogs()
	}
	process, stdBuff, err := Manager.RuntimeCreator.Create(options)
	if err != nil {
		log.GetLogger().Warnf("failed to start runtime with version: %s, error: %s", options.Version, err)
		return nil, err
	}
	Manager.Processes.Store(options.Version, process)
	if err = utils.AdjustRuntimeProcess(process); err != nil {
		log.GetLogger().Errorf("failed to adjust runtime process, %s", err.Error())
		return nil, err
	}
	go inspectProcess(process, options.Version, stdBuff)
	return process, nil
}

func checkRuntimeMemory(lastMemoryUsed *float64, limit float64, version string, parser *monitor.Parser) {
	tmpMemoryUsedBytes, err := parser.Read()
	if err != nil {
		log.GetLogger().Warnf("failed to check runtime memory")
	}
	tmpMemoryUsedVal, ok := tmpMemoryUsedBytes.(uint64)
	if !ok {
		log.GetLogger().Warnf("failed to assert runtime memory, %+v", tmpMemoryUsedVal)
	}
	tmpMemoryUsed := float64(tmpMemoryUsedVal) / bytesToMB
	if !runtimeMemoryWarning && tmpMemoryUsed > runtimeMemWarningPercent*limit {
		log.GetLogger().Warnf("runtime %s has reached 80 percent of the memory limit %f > %f",
			version, tmpMemoryUsed, runtimeMemWarningPercent*limit)
		runtimeMemoryWarning = true
	} else if runtimeMemoryWarning && tmpMemoryUsed < runtimeMemWarningPercent*limit {
		log.GetLogger().Infof("runtime %s is under 80 percent of the memory limit %f < %f",
			version, tmpMemoryUsed, runtimeMemWarningPercent*limit)
		runtimeMemoryWarning = false
	}

	if tmpMemoryUsed > limit {
		log.GetLogger().Infof("runtime %s has reached memory limit %f > %f, exiting",
			version, tmpMemoryUsed, limit)
		runtimeError.Override(snerror.RuntimeMemoryExceedLimit)
		if err := killCurrentProcess(); err != nil {
			log.GetLogger().Warnf("failed to kill process %d, the process may have exited",
				Manager.CurrentRuntime.Process.Pid)
			if Manager.CurrentRuntime.Process.Pid != latestKillPid {
				latestKillPid = Manager.CurrentRuntime.Process.Pid
				killProcessFailedTimes = 0
			} else {
				log.GetLogger().Warnf("failed to kill the same process: %d repeatedly", latestKillPid)
				killProcessFailedTimes++
			}
		} else {
			killProcessFailedTimes = 0
		}
	}
	if tmpMemoryUsed != *lastMemoryUsed {
		notifyMessageWithNoResp(tcpmsg.ReportMemory, commonutil.Float64ToByte(tmpMemoryUsed))
		*lastMemoryUsed = tmpMemoryUsed
	}
	if killProcessFailedTimes == maxKillProcessFailedTimes {
		log.GetLogger().Errorf("The number of consecutive process killing failures reaches the upper limit")
		exit()
	}
}

func checkRuntimeManagerMemory(suicideLimit float64, parser *monitor.Parser) {
	memoryPages, err := parser.Read()
	if err != nil {
		log.GetLogger().Warnf("failed to parse memory pages, err: %s", err.Error())
		return
	}
	memoryValue, ok := memoryPages.(uint64)
	if !ok {
		log.GetLogger().Warnf("failed to assert memory pages, %+v", memoryPages)
		return
	}
	tmpMemoryUsed := float64(memoryValue*pageSize) / bytesToMB
	if tmpMemoryUsed > suicideLimit {
		log.GetLogger().Warnf("runtimeManager has reached memory suicide threshold %f > %f ",
			tmpMemoryUsed, suicideLimit)
		log.GetLogger().Sync()
		os.Exit(1)
	}

	if tmpMemoryUsed > runtimeManagerMemOverride && !runtimeManagerMemoryWarning {
		log.GetLogger().Warnf("runtimeManager has reached memory danger threshold %f > %d ",
			tmpMemoryUsed, runtimeManagerMemOverride)
		runtimeManagerMemoryWarning = true
	} else if tmpMemoryUsed < runtimeManagerMemOverride && runtimeManagerMemoryWarning {
		log.GetLogger().Infof("runtimeManager is under memory danger threshold %f < %d ",
			tmpMemoryUsed, runtimeManagerMemOverride)
		runtimeManagerMemoryWarning = false
	}
}

// CollectRuntimeCPU collecting cpu used
func CollectRuntimeCPU(isStartInvoke bool) {
	if cgroupCPUParser == nil || systemCPUParser == nil {
		return
	}
	tmpCPUUsedNsVal, err := cgroupCPUParser.Read()
	if err != nil {
		log.GetLogger().Warnf("failed to parse cgroup cpu %s", err.Error())
		return
	}

	tmpCPUUsedNs, ok := tmpCPUUsedNsVal.(uint64)
	if !ok {
		log.GetLogger().Warnf("failed to parse cgroup cpu: %+v", tmpCPUUsedNsVal)
		return
	}
	totalTimestampNsVal, err := systemCPUParser.Read()
	if err != nil {
		log.GetLogger().Warnf("failed to parse system cpu %s", err.Error())
		return
	}

	totalTimestampNs, ok := totalTimestampNsVal.(uint64)
	if !ok {
		log.GetLogger().Warnf("failed to parse system cpu: %+v", totalTimestampNsVal)
		return
	}
	tmpCPUUsed := &rm.CPUUsed{
		CPUUsedNs:        tmpCPUUsedNs,
		TotalTimestampNs: totalTimestampNs,
	}

	notifyMessageWithNoResp(tcpmsg.ReportCPU, tmpCPUUsed.Encode())
}

func watchingRuntimeMemoryUsedLoop(hardLimit int64, version string) {
	lastMemoryUsed := 0.0
	limit := float64(hardLimit - memoryGap)
	ticker := time.NewTicker(readMemoryInternal)
	defer ticker.Stop()
	parser, err := monitor.NewCGroupMemoryParser()
	if err != nil {
		log.GetLogger().Warnf("failed to create cgroup memory parser %s", err.Error())
		return
	}
	defer parser.Close()
	for {
		select {
		case <-ticker.C:
			checkRuntimeMemory(&lastMemoryUsed, limit, version, parser)
		case _, ok := <-ExitSignal:
			if !ok {
				log.GetLogger().Warnf("process exit signal is closed")
			}
			return
		case _, ok := <-ExitSignalOnNoRequest:
			if !ok {
				log.GetLogger().Warnf("runtime-manager received termination signal, shutdown")
			}
			return
		}
	}
}

func watchingRuntimeManagerMemoryUsedLoop(hardLimit int64) {
	ticker := time.NewTicker(readRuntimeManagerMemoryInternal)
	defer ticker.Stop()
	suicideLimit := float64(hardLimit) * runtimeManagerMemSuicidePercent
	parser, err := monitor.NewProcMemoryParser("self")
	if err != nil {
		log.GetLogger().Errorf("failed to create proc memory parser %s", err.Error())
		return
	}
	defer parser.Close()
	for {
		select {
		case <-ticker.C:
			checkRuntimeManagerMemory(suicideLimit, parser)
		case _, ok := <-ExitSignal:
			if !ok {
				log.GetLogger().Warnf("process exit signal is closed")
			}
			return
		case _, ok := <-ExitSignalOnNoRequest:
			if !ok {
				log.GetLogger().Warnf("runtime-manager received termination signal, shutdown")
			}
			return
		}
	}
}

// watchingRuntimeCPUUsedLoop timing task for collecting cpu used
func watchingRuntimeCPUUsedLoop() {
	ticker := time.NewTicker(readCPUInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			CollectRuntimeCPU(false)
		case _, ok := <-ExitSignal:
			if !ok {
				log.GetLogger().Warnf("process exit signal is closed")
			}
			return
		case _, ok := <-ExitSignalOnNoRequest:
			if !ok {
				log.GetLogger().Warnf("runtime-manager received termination signal, shutdown")
			}
			return
		}
	}
}

func getLangFromVersion(version string) string {
	for _, lang := range []string{utils.Nodejs, utils.Python27, utils.Python36, utils.Python39,
		utils.Python310, utils.Python311, utils.Python312,
		utils.Java8, utils.Javax, utils.Java11, utils.Java17, utils.Java21, utils.Php, utils.NetCore31, utils.NetCore21,
		utils.CangJie0523, utils.CangJie10, utils.CangJie060x, utils.NetCore60, utils.NetCore80, utils.CustomRuntimeRPC} {
		if strings.Contains(version, lang) {
			return lang
		}
	}
	return ""
}

func reportLog(logs []byte) {
	if !logReportEnable {
		if checkRuntimeProcessAlive() {
			logReportEnable = true
		} else {
			return
		}
	}
	notifyMessageWithNoResp(tcpmsg.ReportLog, logs)
}

func inspectProcess(process *os.Process, version string, buf *functionlog.FixSizeWriter) {
	if process == nil {
		log.GetLogger().Warnf("process is nil with version: %s", version)
		return
	}
	log.GetLogger().Debugf("wait runtime process exit with version: %s, pid: %d", version, process.Pid)
	state, err := process.Wait()
	if buf != nil {
		log.GetLogger().Warnf("runtime %s exit, console print:%s", version, buf.Read())
	}
	if err != nil {
		log.GetLogger().Warnf("inspect runtime process error: %s", err.Error())
		return
	}
	if state != nil {
		if strings.Contains(Manager.currentVersion, utils.Nodejs) {
			handleNodeJsOOMCoreDump(buf, state)
		}
		log.GetLogger().Debugf("runtime process status: %s, exit code: %d with version: %s, pid: %d",
			state.String(), state.ExitCode(), version, process.Pid)
		if getProcess(version) == nil {
			log.GetLogger().Infof("no need to restart runtime process, version:%s", version)
			return
		}
		Manager.Processes.Delete(version)
	}
	// current process broken, try to restart it
	if version == Manager.currentVersion {
		if Manager.currentMode == restore {
			printFastfreezeLog()
			log.GetLogger().Errorf("failed to restore process for version: %s", version)
			// backoff to previous code start
			Manager.conf.RunningMode = ""
			Manager.currentMode = restoreDead
			log.GetLogger().Errorf("Restore is dead, go to normal start")
		}
		runtimeError.Set(snerror.RuntimeProcessExit)
		if strings.Contains(version, utils.CSharp) && buf != nil {
			csharpRuntimeExitedOutput = string(buf.Read())
		}
		if Manager.currentMode == restoreDead {
			close(inspectProcessChannel)
			notifyMessageWithNoResp(tcpmsg.RestoreDead, nil)
			logReportEnable = false
			Manager.currentMode = normal
		} else {
			notifyMessageWithNoResp(tcpmsg.RuntimeProcessExit, nil)
			logReportEnable = false
		}
	}
}

func handleNodeJsOOMCoreDump(buf *functionlog.FixSizeWriter, state *os.ProcessState) {
	nodejsOOMCoreDump, err := getNodeJsCmdStdOutOOMCoreDump(buf, state)
	log.GetLogger().Debugf("nodejsOOMCoreDump: %v", nodejsOOMCoreDump)
	if err != nil {
		log.GetLogger().Error("get cmd stdOut oom coredump error.")
	}
	if nodejsOOMCoreDump {
		runtimeError.Override(snerror.RuntimeMemoryExceedLimit)
	}
}

func printFastfreezeLog() {
	cmd := exec.Command("/bin/bash", "-c", "tail -n 5 /var/tmp/fastfreeze/logs/*.log")
	out, err := cmd.Output()
	if err != nil {
		log.GetLogger().Errorf("failed to tail fastfreeze log error: %s", err.Error())
		return
	}
	log.GetLogger().Errorf("fastfreeze log: %s", out)
}

// PrintInputLog prints health log
func PrintInputLog() {
	if Manager.CurrentRuntime.Process != nil {
		log.GetLogger().Infof("runtimeManagerAlive | %s", Manager.CurrentRuntime.Version)
	} else {
		log.GetLogger().Infof("runtime-manager is alive. The runtime is not specialized")
	}
}

func stopRedundancyProcesses(exceptVersion string) error {
	log.GetLogger().Debugf("start to kill other runtime processes, but not include version: %s", exceptVersion)
	Manager.Processes.Range(func(k, v interface{}) bool {
		nowTime := time.Now()
		version, versionFlag := k.(string)
		if !versionFlag {
			log.GetLogger().Errorf("version in wrapper process type error")
			return false
		}
		// no need to kill
		if version == exceptVersion {
			log.GetLogger().Debugf("version %s in wrapper process needn't be killed", version)
			return true
		}
		if v == nil {
			log.GetLogger().Warnf("version %s in wrapper process is nil", version)
			return true
		}
		process, processFlag := v.(*os.Process)
		if !processFlag || process == nil {
			log.GetLogger().Errorf("version %s in wrapper process type error or process is nil", version)
			return false
		}

		log.GetLogger().Debugf("begin to stop runtime process with language: %s, time: %s", version,
			nowTime.String())
		if err := process.Kill(); err != nil {
			log.GetLogger().Warnf("failed to stop runtime process with language: %s, error: %s", version, err.Error())
		} else {
			log.GetLogger().Debugf("succeed to stop runtime process with language: %s", version)
		}
		return true
	})
	return nil
}

func processSoftLink(exceptVersion string) error {
	if err := os.Chmod(binPath, 0557); err != nil {
		log.GetLogger().Warnf("failed to chmod %s, error:%s", binPath, err.Error())
	}
	defer func() {
		if err := os.Chmod(binPath, 0555); err != nil {
			log.GetLogger().Warnf("failed to chmod %s, error:%s", binPath, err.Error())
		}
		log.GetLogger().Infof("finish create soft-link, runtime version: %s", exceptVersion)
	}()

	var cmd *exec.Cmd
	switch exceptVersion {
	case utils.Nodejs610, utils.Nodejs810, utils.Nodejs1016, utils.Nodejs1213, utils.Nodejs1418, utils.Nodejs1617,
		utils.Nodejs1815, utils.Nodejs2015, utils.Nodejs2218, utils.Nodejs2223, utils.Nodejs2422:
		cmd = exec.Command("ln", fmt.Sprintf(nodejsPathFormat, exceptVersion), "/usr/bin/node", "-sf")
	case utils.Python27, utils.Python36, utils.Python39, utils.Python310, utils.Python311, utils.Python312:
		cmd = exec.Command("ln", fmt.Sprintf(pythonPathFormat, exceptVersion, exceptVersion), "/usr/bin/python", "-sf")
	case utils.Java8, utils.Java11, utils.Java17, utils.Java21, utils.Javax:
		cmd = exec.Command("ln", fmt.Sprintf(javaPathFormat, exceptVersion), "/usr/bin/java", "-sf")
	case utils.Php73, utils.Php83:
		cmd = exec.Command("ln", fmt.Sprintf(phpPathFormat, exceptVersion), "/usr/bin/php", "-sf")
	case utils.NetCore21:
		cmd = exec.Command("ln", fmt.Sprintf(csharpPathFormat, "dotnet2.1"), "/usr/bin/dotnet", "-sf")
	case utils.NetCore31:
		cmd = exec.Command("ln", fmt.Sprintf(csharpPathFormat, "dotnet3.1"), "/usr/bin/dotnet", "-sf")
	default:
	}
	if cmd == nil {
		return errors.New("failed to create soft-link, error: cmd is nil")
	}
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to create soft-link, error: %s", err.Error())
	}
	return nil
}

func getProcess(version string) *os.Process {
	processItf, exist := Manager.Processes.Load(version)
	if !exist {
		return nil
	}
	process, flag := processItf.(*os.Process)
	if !flag {
		log.GetLogger().Warnf("process in Processes is invalid: %s", version)
		return nil
	}
	return process
}

func stopSpecRuntimeProcess(version string) error {
	process := getProcess(version)
	if process == nil {
		return nil
	}
	Manager.Processes.Delete(version)
	if err := process.Kill(); err != nil {
		return err
	}
	log.GetLogger().Infof("succeed to stop %s runtime", version)
	return nil
}

// AttemptToRestart tries to restart the runtime
func AttemptToRestart() error {
	err := Manager.CurrentRuntime.restartRuntimeProcessWithRetry()
	if err != nil {
		return fmt.Errorf("cannot restart runtime in %d times: %s", retryTimes, err)
	}
	log.GetLogger().Debugf("runtime has been restarted")
	return nil
}

// GotRuntimeException worker gets exception through runtime-manager
func GotRuntimeException() (int, error) {
	runtimeErr := runtimeError.Get()
	runtimeError.Reset()
	log.GetLogger().Infof("worker got runtime exception: %d", runtimeErr)

	if runtimeErr == 0 || runtimeErr == snerror.RuntimeProcessExit {
		if strings.Contains(Manager.currentVersion, utils.CSharp) && csharpRuntimeExitedOutput != "" {
			return runtimeErr, errors.New(csharpRuntimeExitedOutput)
		}
		done := make(chan bool, 1)
		go func() {
			done <- oom.IsRuntimeOOMKilled()
		}()

		select {
		case flag := <-done:
			if flag {
				log.GetLogger().Infof("runtime has been killed with OOM")
				return snerror.RuntimeMemoryExceedLimit, nil
			}
			return runtimeErr, nil
		case <-time.After(defaultGetOOMTimeout):
			log.GetLogger().Warn("runtime OOM judged timeout")
			return runtimeErr, nil
		}
	}
	return runtimeErr, nil
}

func checkRuntimeProcessAlive() bool {
	proc, exist := Manager.Processes.Load(Manager.currentVersion)
	if !exist || proc == nil {
		log.GetLogger().Debugf("%s process is dead!", Manager.currentVersion)
		return false
	}

	process, flag := proc.(*os.Process)
	if !flag {
		log.GetLogger().Warnf("process in Processes is invalid: %s", Manager.currentVersion)
		return false
	}
	err := process.Signal(syscall.Signal(0))
	if err != nil {
		log.GetLogger().Debugf("%s process is dead!", Manager.currentVersion)
		return false
	}
	log.GetLogger().Debugf("%s process is alive!", Manager.currentVersion)
	return true
}

func gracefulShutdown(stopCh <-chan struct{}) {
	for {
		select {
		case _, ok := <-ClearChan:
			if ok {
				utils.ClearCAPs()
			}
		case _, ok := <-ExitSignal:
			if !ok {
				log.GetLogger().Warnf("process exit signal is closed")
			}
			return
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("runtime-manager received termination signal, shutdown")
			}
			return
		}
	}
}

// GracefulShutdown -
func GracefulShutdown(stopCh <-chan struct{}) {
	gracefulShutdown(stopCh)
	notifyMessageWithNoResp(tcpmsg.ReportRequest, nil)
	connectionErr := make(chan struct{})
	if Manager.ConnectionWithWorker != nil {
		connectionErr = Manager.ConnectionWithWorker.ConnectionErr
	} else {
		ExitSignalOnce.Do(func() { close(ExitSignalOnNoRequest) })
		log.GetLogger().Info("failed to connect worker, ConnectionWithWorker is nil, " +
			"will close ExitSignalOnNoRequest")
	}

	select {
	case _, ok := <-connectionErr:
		if !ok {
			log.GetLogger().Warnf("failed to connect worker, runtime-manager graceful shutdown")
		}
	case _, ok := <-ExitSignalOnNoRequest:
		if !ok {
			log.GetLogger().Warnf("process exit signal on no request is closed")
		}
	}
	log.GetLogger().Infof("runtime-manager shutdown after processing graceful exit")
	log.GetLogger().Sync()
	os.Exit(0)
}

func checkSubProcessCMDArgs(runtimeType string, params ...string) error {
	for _, param := range params {
		if !commonutil.IsInputParameterValid(param) {
			return fmt.Errorf("input parameter is invalid: %s", param)
		}
	}
	if runtimeType == utils.CustomRuntimeRPC {
		if len(params) != customRuntimeRPCParamsLen {
			return errors.New("custom-runtime-rpc params len error")
		}
		seg := strings.Split(params[1], " ")
		if len(seg) != customRuntimeRPCCmdLen {
			return errors.New("custom-runtime-rpc cmd len error")
		}
		if !commonutil.ExistPath(seg[0]) {
			return snerror.NewSNError(snerror.FunctionEntryNotFound, "handler not exist")
		}
	}
	return nil
}

func notifyMessageWithNoResp(messageType tcpmsg.MessageType, msg []byte) {
	if Manager.ConnectionWithWorker != nil {
		nErr := Manager.ConnectionWithWorker.SendMessageWithNoResp(
			tcpmsg.BuildMessage(messageType, msg))
		if nErr != nil {
			log.GetLogger().Warnf("failed to notify message, type: %c, err %s", messageType, nErr.Error())
		}
	}
}

// KillRuntimeProcess -
func KillRuntimeProcess() (int, error) {
	// WARN: Don't print logs before killing process, because sync logs may be blocked
	if err := killCurrentProcess(); err != nil {
		log.GetLogger().Warnf("failed to kill process %d, the process may have exited",
			Manager.CurrentRuntime.Process.Pid)
	}
	log.GetLogger().Infof("killRuntime | pid %d", Manager.CurrentRuntime.Process.Pid)
	return tcpmsg.MessageWithNoResponse, nil
}

func killCurrentProcess() error {
	if Manager.conf.RunningMode == constants.RunningModeRestore {
		if err := syscall.Kill(constants.FastFreezeRuntimePid, syscall.SIGKILL); err != nil {
			log.GetLogger().Warnf("failed to kill fast freeze runtime: %s", err.Error())
		}
	}
	pgid, err := syscall.Getpgid(Manager.CurrentRuntime.Process.Pid)
	if err == nil {
		return syscall.Kill(-pgid, syscall.SIGKILL)
	}
	return syscall.Kill(Manager.CurrentRuntime.Process.Pid, syscall.SIGKILL)
}

func getNodeJsCmdStdOutOOMCoreDump(buf *functionlog.FixSizeWriter, state *os.ProcessState) (bool, error) {
	if buf == nil {
		log.GetLogger().Debug("cmd out buffer in nil")
		return false, nil
	}
	if strings.Contains(string(buf.Read()), nodejsOOMCoreDumpString) {
		return true, nil
	}

	return false, nil
}

func buildExtensionCreator() {
	Manager.ExtensionCreator = &extensionProcess{}
}

// InitExtensionProcess -
func InitExtensionProcess() error {
	names, flag := rm.CheckExtension()
	if !flag {
		log.GetLogger().Errorf("extension invalid: %v", names)
		err := errors.New("extension invalid")
		return err
	}
	buildExtensionCreator()
	for _, name := range names {
		err := startExtensionProcess(name)
		if err != nil {
			Manager.CurrentExtension = Manager.CurrentExtension[:0]
			Manager.CurrentRuntime.extensionPids.Range(func(key, value interface{}) bool {
				Manager.CurrentRuntime.extensionPids.Delete(key)
				return true
			})
			return err
		}
	}
	log.GetLogger().Infof("start extension, extension length is %d, extension names : %v", len(names), names)
	return nil
}

func startExtensionProcess(name string) error {
	var process *os.Process
	var err error
	log.GetLogger().Infof("start extension process: %s", name)
	options := types.NewCreateOptions(constants.DefaultRuntimeUID, constants.DefaultRuntimeGID, Manager.conf,
		types.WithVersion(Manager.currentVersion), types.WithName(name))
	process, err = createExtension(options)
	if err != nil {
		return err
	}
	Manager.CurrentExtension = append(Manager.CurrentExtension, &ExtensionProcess{
		Process: process,
		Name:    name,
		UID:     constants.DefaultRuntimeUID,
		GID:     constants.DefaultRuntimeGID,
		Status:  tcpmsg.RuntimeProcessStatusRunning,
	})
	Manager.CurrentRuntime.extensionPids.Store(process.Pid, true)
	return nil
}

func createExtension(options *types.CreateOptions) (*os.Process, error) {
	process, _, err := Manager.ExtensionCreator.Create(options)
	if err != nil {
		log.GetLogger().Warnf("failed to start extension, error: %s", err.Error())
		return nil, err
	}
	go inspectExtensionProcess(process)
	log.GetLogger().Infof("succeed to create extension process, pid: %d, name: %s", process.Pid, options.Name)
	return process, nil
}

func inspectExtensionProcess(process *os.Process) {
	state, err := process.Wait()
	if err != nil {
		log.GetLogger().Warnf("inspect runtime process error: %s", err.Error())
		return
	}
	if state != nil {
		log.GetLogger().Errorf("extension process status: %s, exit code: %d, pid: %d",
			state.String(), state.ExitCode(), process.Pid)
	}
	notifyMessageWithNoResp(tcpmsg.ExtensionProcessExit, nil)
}

func watchHostFileLoop() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.GetLogger().Errorf("failed to create host file watcher")
		exit()
		return
	}
	defer watcher.Close()
	if err = watcher.Add(hostFilePath); err != nil {
		log.GetLogger().Errorf("failed to watch file: %s, error: %s", hostFilePath, err.Error())
		exit()
		return
	}
	if err = commonutil.HostFile.SaveHostFileInfo(); err != nil {
		log.GetLogger().Errorf("failed to save /etc/hosts info, error: %s", err.Error())
		exit()
		return
	}
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				log.GetLogger().Errorf("failed to receive the watcher event")
				exit()
				return
			}
			resetHostsFile(event)
		case err, _ = <-watcher.Errors:
			if err != nil {
				log.GetLogger().Errorf("receive a watcher error: ", err.Error())
			}
		case <-ExitSignal:
			return
		case <-ExitSignalOnNoRequest:
			return
		}
	}
}

func resetHostsFile(event fsnotify.Event) {
	log.GetLogger().Debugf("receive a watcher event: %s", event.String())
	_, sha, err := commonutil.GetFileHashInfo(hostFilePath)
	if err != nil {
		log.GetLogger().Errorf("failed to stat /etc/host, error: %s", err.Error())
		exit()
		return
	}
	commonutil.HostFile.Mutex.Lock()
	defer commonutil.HostFile.Mutex.Unlock()
	if sha != commonutil.HostFile.Sha256 {
		if err := ioutil.WriteFile(hostFilePath, commonutil.HostFile.Content, hostFilePerm); err != nil {
			log.GetLogger().Errorf("abnormal file modification was detected, but failed to reset, "+
				"event: %s", event.String())
			exit()
			return
		}
		log.GetLogger().Infof("abnormal file modification was detected and /etc/hosts was reset")
	}
}

// ExecPreStopCommand -
func ExecPreStopCommand() (string, error) {
	preStopCommand := Manager.conf.PreStop.Handler
	if len(preStopCommand) == 0 {
		log.GetLogger().Errorf("the command of preStop is empty")
		return "", errors.New("the command of preStop is empty")
	}
	var command []string
	if err := json.Unmarshal([]byte(preStopCommand), &command); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the command of preStop, err: %s", err.Error())
		return "", err
	}
	if len(command) == 0 {
		log.GetLogger().Errorf("the command of preStop is empty")
		return "", errors.New("the command of preStop is empty")
	}
	runCmd := exec.Command(command[0])
	if len(command) > 1 {
		runCmd = exec.Command(command[0], command[1:]...)
	}
	uid, gid, err := usergroup.CheckID(Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.UserID,
		Manager.conf.RuntimeConfig.FuncMountConfig.FuncMountUser.GroupID)
	if err != nil {
		return "", err
	}
	if err := workerutils.SetCmdCredByID(runCmd, uid, gid); err != nil {
		log.GetLogger().Errorf("failed to set uid and gid for preStop cmd with error: %s", err.Error())
		return "", err
	}
	output, err := runCmd.Output()
	if err != nil {
		log.GetLogger().Errorf("failed to exec command of preStop with error: %s", err.Error())
		return "", err
	}
	return string(output), nil
}

func watchingRuntimeCaeMetricsLoop() {
	ticker := time.NewTicker(readNetWorkInternal)
	defer ticker.Stop()

	netParser, err := monitor.NewNetParser()
	if err != nil {
		log.GetLogger().Warnf("failed to create net parser %s", err.Error())
		return
	}
	defer netParser.Close()

	fileSystemParser, err := monitor.NewFileSystemParser()
	if err != nil {
		log.GetLogger().Warnf("failed to create fileSystem parser %s", err.Error())
		return
	}
	defer fileSystemParser.Close()

	for {
		select {
		case <-ticker.C:
			CollectCaeMetrics(netParser, fileSystemParser)
		case _, ok := <-ExitSignal:
			if !ok {
				log.GetLogger().Warnf("process exit signal is closed when cae metrics check")
			}
			return
		case _, ok := <-ExitSignalOnNoRequest:
			if !ok {
				log.GetLogger().Warnf("runtime-manager received termination signal, shutdown when cae metrics check")
			}
			return
		}
	}
}

// CollectCaeMetrics get metrics of cae function
func CollectCaeMetrics(netParser, fileSystemParser *monitor.Parser) {
	if netParser == nil || fileSystemParser == nil {
		log.GetLogger().Errorf("failed to collect cae metrics because parse is nil")
		return
	}

	netDataVal, err := netParser.Read()
	if err != nil {
		log.GetLogger().Warnf("failed to get net data,err: %s", err.Error())
		return
	}

	netData, ok := netDataVal.(*monitor.NetData)
	if !ok {
		log.GetLogger().Warnf("failed to parse net data,netDataVal: %+v", netDataVal)
		return
	}

	fileSystemDataVal, err := fileSystemParser.Read()
	if err != nil {
		log.GetLogger().Warnf("failed to get fileSystem data,err: %s", err.Error())
		return
	}

	fileSystemData, ok := fileSystemDataVal.(monitor.BlkioStatEntrys)
	if !ok {
		log.GetLogger().Warnf("failed to parse fileSystem data,fileSystemDataVal: %+v", fileSystemDataVal)
		return
	}

	readBytes, writeBytes := fileSystemData.GetReadAndWriteBytes()

	caeMetrics := monitor.CaeMetrics{
		ReceiveBytes:         netData.ReceiveBytes,
		ReceivePackage:       netData.ReceivePackage,
		TransmitBytes:        netData.TransmitBytes,
		TransmitPackage:      netData.TransmitPackage,
		FileSystemReadBytes:  readBytes,
		FileSystemWriteBytes: writeBytes,
		TimestampMs:          uint64(time.Now().UnixMilli()),
	}

	notifyMessageWithNoResp(tcpmsg.ReportCaeMetrics, caeMetrics.Encode())
}
