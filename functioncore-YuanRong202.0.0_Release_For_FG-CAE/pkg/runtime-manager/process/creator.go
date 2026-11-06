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

package process

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/runtime-manager/extends/runtimeargs"
	"functioncore/pkg/runtime-manager/functionlog"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/runtime-manager/utils"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/custom"
	golang "functioncore/pkg/worker/runtime/golang/v1"
	"functioncore/pkg/worker/runtime/web"
	workerutils "functioncore/pkg/worker/utils"
)

type runtimeCreateStrategy interface {
	Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error)
}

type goV1Runtime struct{}

// Create -
func (g *goV1Runtime) Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
	process, err := golang.RunProcess(createOptions.UID, createOptions.GID, createOptions.Conf, reportLog)
	if err != nil {
		return nil, nil, err
	}
	log.GetLogger().Infof("succeeded to create a go1.x runtime process")
	return process, nil, nil
}

type webRuntime struct{}

// Create -
func (w *webRuntime) Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
	runProcessParams := &runtimemanagerrequest.RunProcessParams{
		UID:               createOptions.UID,
		GID:               createOptions.GID,
		IsMcpFunction:     createOptions.IsMcpFunction,
		NPMConfigRegistry: createOptions.NPMConfigRegistry,
		UVIndexURL:        createOptions.UVIndexURL,
	}

	process, err := web.RunProcess(runProcessParams, createOptions.Conf, reportLog)
	if err != nil {
		log.GetLogger().Errorf("failed to create a web runtime process, %s", err.Error())
		return nil, nil, err
	}
	log.GetLogger().Infof("succeeded to create a web runtime process")
	return process, nil, nil
}

type customRuntime struct{}

// Create -
func (c *customRuntime) Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
	process, err := custom.RunProcess(createOptions.UID, createOptions.GID, createOptions.Conf, reportLog)

	if err != nil {
		log.GetLogger().Errorf("failed to create custom runtime process, %s", err.Error())
		return nil, nil, err
	}
	log.GetLogger().Infof("succeeded to create a custom runtime process")
	return process, nil, nil
}

type fusionRuntime struct{}

// Create -
func (f *fusionRuntime) Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
	runningMode := Manager.conf.RunningMode
	port := createOptions.Conf.RuntimeConfig.RMConfig.RuntimeConfigs[createOptions.Version].Port
	lang := getLangFromVersion(createOptions.Version)
	if lang == "" {
		return nil, nil, fmt.Errorf("unknown language and version: %s", createOptions.Version)
	}
	exePath, err := utils.GetExecPath(createOptions.Version)
	if err != nil {
		return nil, nil, err
	}
	sockType := utils.GetSocketType(&createOptions.Conf.RuntimeConfig, createOptions.Version)
	buildArgs, err := runtimeargs.GetBuildArgs(
		runtimeargs.BuildArgsPars{Lang: lang, Port: port, SockType: sockType, RunningMode: runningMode},
		&createOptions.Conf.RuntimeConfig)
	if err != nil {
		log.GetLogger().Errorf("customJvmArgs invalid error: %s", err.Error())
		return nil, nil, err
	}

	if len(buildArgs) == 0 {
		return nil, nil, fmt.Errorf("failed to get runtime args: %s", createOptions.Version)
	}
	log.GetLogger().Infof("exePath %s | buildArgs %v", exePath, buildArgs)
	absPath, err := filepath.Abs(exePath)
	if err != nil {
		log.GetLogger().Errorf("get absolute file path error: %s", err.Error())
		return nil, nil, err
	}
	if err = checkSubProcessCMDArgs(createOptions.Version, buildArgs...); err != nil {
		log.GetLogger().Errorf("check sub process cmd args error: %s", err.Error())
		return nil, nil, err
	}
	startUpArgs := fmt.Sprintf("%s %s", absPath, strings.Join(buildArgs, " "))
	snapshotCriuOpts := Manager.conf.SnapshotCriuOpts
	cmd := startProcess(runningMode, startUpArgs, absPath, buildArgs, snapshotCriuOpts)
	cmd.Env = utils.ExtractEnv(createOptions.Version, createOptions.DebugEnv,
		&createOptions.Conf.RuntimeConfig.RMConfig)
	if err = workerutils.SetCmdCredByID(cmd, createOptions.UID, createOptions.GID); err != nil {
		log.GetLogger().Errorf("failed to set uid and gid with error: %s", err.Error())
		return nil, nil, err
	}

	var buf *functionlog.FixSizeWriter
	if strings.Contains(createOptions.Version, utils.Nodejs) || strings.Contains(createOptions.Version, utils.CSharp) ||
		strings.Contains(createOptions.Version, utils.CustomRuntimeRPC) ||
		strings.Contains(createOptions.Version, "cangjie") ||
		strings.Contains(createOptions.Version, utils.Java8) {
		buf = functionlog.NewFixSizeWriter()
		w := io.MultiWriter(os.Stdout, buf)
		cmd.Stdout, cmd.Stderr = w, w
	} else {
		cmd.Stdout, cmd.Stderr = os.Stdout, os.Stdout
	}
	cmd.Dir = createOptions.Conf.RuntimeConfig.RMConfig.RuntimeConfigs[createOptions.Version].WorkerDir
	if err := cmd.Start(); err != nil {
		log.GetLogger().Errorf("failed to start runtime with language: %s, error: %s", lang, err.Error())
		return nil, nil, err
	}
	return cmd.Process, buf, nil
}

type extensionProcess struct{}

// Create -
func (e *extensionProcess) Create(createOptions *types.CreateOptions) (*os.Process, *functionlog.FixSizeWriter, error) {
	process, err := runtime.RunExtensionProcess(createOptions, reportLog)
	if err != nil {
		log.GetLogger().Errorf("failed to create custom runtime process, %s", err.Error())
		return nil, nil, err
	}
	log.GetLogger().Infof("succeeded to create a extension process, name: %s", createOptions.Name)
	return process, nil, nil
}

func startProcess(runningMode string, startUpArgs string, absPath string,
	buildArgs []string, criuOpts string) *exec.Cmd {
	var cmd *exec.Cmd
	// if cae function has log policy configuration, checkpoint operation needs to add log policy path to snapshot
	preservePath := utils.GetPreservePath(getLogPolicy())
	checkpointRunArgs := []string{constants.FastFreezeRunScriptPath, startUpArgs, constants.SnapshotImagePath,
		preservePath}
	restoreRunArgs := []string{constants.FastFreezeRestoreScriptPath, constants.SnapshotImagePath,
		fmt.Sprintf("touch %s/notify", constants.FastFreezeRestoreNotifyPath)}
	if criuOpts != "" {
		log.GetLogger().Infof("startProcess configured criuOpts is %s", criuOpts)
		checkpointRunArgs = append(checkpointRunArgs, criuOpts)
		restoreRunArgs = append(restoreRunArgs, criuOpts)
	}
	switch runningMode {
	case constants.RunningModeCheckpoint:
		log.GetLogger().Infof("execute checkpoint run script with %v", checkpointRunArgs)
		cmd = exec.Command("sh", checkpointRunArgs...)
	case constants.RunningModeRestore:
		log.GetLogger().Infof("execute restore run script with %v", restoreRunArgs)
		cmd = exec.Command("sh", restoreRunArgs...)
	default:
		log.GetLogger().Infof("exeNormalRunScript")
		cmd = exec.Command(absPath, buildArgs...)
	}
	return cmd
}
