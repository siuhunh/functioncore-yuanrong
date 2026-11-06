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

// Package server is to handle tcp server
package server

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/runtime-manager/extends/domain"
	"functioncore/pkg/runtime-manager/process"
)

var (
	initOnceFlag = false
)

var (
	currentTenantCommandsTaskID = ""
	tenantCommandExecuteLock    sync.Mutex
)

// StartListening create socket server and start to listen
func StartListening(address string) error {
	newConnFunc := func(conn *tcpmsg.SocketConn) {
		if conn == nil {
			exit()
			return
		}
		process.Manager.ConnectionWithWorker = conn
		if _, ok := <-process.Manager.ConnectionWithWorker.ConnectionErr; !ok {
			log.GetLogger().Warnf("connection error chan is closed")
		}
		exit()
	}
	if err := os.Remove(tcpmsg.RuntimeUDS); err != nil && !os.IsNotExist(err) {
		log.GetLogger().Errorf("failed to remove uds, err: %s", err.Error())
	}

	if err := tcpmsg.ListenSocketServer(tcpmsg.RuntimeUDS, requestHandler, newConnFunc); err != nil {
		return err
	}
	return nil
}

// requestHandler handle request by message type
func requestHandler(msg *tcpmsg.Message) (int, tcpmsg.RuntimeRSP, error) {
	var dnsMutex sync.Mutex
	var rsp = tcpmsg.RuntimeRSP{}
	var err error
	var code int
	switch msg.GetMsgType() {
	case tcpmsg.StopProcess:
		code, err = process.StopRuntimeProcess(msg)
	case tcpmsg.InitProcess:
		defer func() {
			if err == nil && code != tcpmsg.RuntimeRestarted {
				initOnceFlag = true
			}
		}()
		if !initOnceFlag {
			return initProcessOnce(code, err, rsp, msg)
		}
		err, code = fmt.Errorf("init process can be executed only once"), 0
	case tcpmsg.ResetDNSConfig:
		code, err = handleResetDNSConfig(msg)
	case tcpmsg.ExecuteTenantCommands:
		code, err = handleTenantCommands(msg)
	case tcpmsg.RequestFinished:
		// last log happened before RequestFinished message from worker, send LogFinished on connection 'socketClient'
		process.Manager.ConnectionWithWorker.SendMessageWithNoResp(
			tcpmsg.BuildMessage(tcpmsg.LogFinished, msg.Body))
		err, code = nil, tcpmsg.MessageWithNoResponse
	case tcpmsg.GotRuntimeException:
		code, err = process.GotRuntimeException()
	case tcpmsg.StopRuntimeManager:
		process.ExitSignalOnce.Do(func() { close(process.ExitSignalOnNoRequest) })
		err, code = nil, tcpmsg.MessageWithNoResponse
	case tcpmsg.ResetDomainName:
		dnsMutex.Lock()
		err, code = domain.ResetDomainName(msg.Body), tcpmsg.MessageWithSuccessResponse
		dnsMutex.Unlock()
	case tcpmsg.PauseRuntimeProcess:
		code, err = 0, pauseRuntimeProcess()
	case tcpmsg.RecoverRuntimeProcess:
		code, err = 0, recoverRuntimeProcess()
	case tcpmsg.KillRuntimeProcess:
		code, err = process.KillRuntimeProcess()
	case tcpmsg.RestartRuntimeProcess:
		code, err = 0, process.AttemptToRestart()
	case tcpmsg.FastFreezeCheckpoint:
		code, err = process.CheckpointRuntimeByFastFreeze()
	case tcpmsg.InitExtensionProcess:
		code, err = 0, process.InitExtensionProcess()
	case tcpmsg.ExecPreStopCommand:
		return execPreStopCommand()
	case tcpmsg.TermCAEProcess:
		code, err = process.TerminateCAECurrentProcess()
	default:
		err, code = errors.New("unknown request type"), tcpmsg.MessageWithNoResponse
	}
	return code, rsp, err
}

func handleTenantCommands(msg *tcpmsg.Message) (int, error) {
	var tenantCommandStruct types.TenantCommandStruct
	err := json.Unmarshal(msg.Body, &tenantCommandStruct)
	if err != nil {
		return tcpmsg.MessageWithError, err
	}
	taskID := tenantCommandStruct.TaskID
	tenantCommandList := tenantCommandStruct.Command
	tenantCommandListStr := strings.Join(tenantCommandList, ", ")
	log.GetLogger().Infof("begin to handle tenant Commands: %s, traceId: %s", tenantCommandListStr, taskID)
	tenantCommandExecuteLock.Lock()
	defer tenantCommandExecuteLock.Unlock()
	currentTenantCommandsTaskID = taskID

	var variableMap = tenantCommandStruct.TenantCommandValuesMap

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(tenantCommandStruct.Timeout)*time.Second)
	defer cancel()
	err = executeTenantCommands(ctx, tenantCommandList, variableMap)
	if err != nil {
		log.GetLogger().Errorf("failed to handle tenant Commands, traceId: %s, error: %s", taskID, err.Error())
		return tcpmsg.MessageWithError, err
	}

	log.GetLogger().Infof("success to handle tenant Commands, traceId: %s", taskID)
	return tcpmsg.MessageWithSuccessResponse, nil
}

func executeTenantCommands(ctx context.Context, tenantCommandList []string, variableMap map[string]string) error {
	translatedCommandList, err := translateCommands(ctx, tenantCommandList, variableMap)
	if err != nil {
		return err
	}
	translatedCommandListStr := strings.Join(translatedCommandList, ", ")
	log.GetLogger().Infof("finish translate taskIDTenant Commands: %s, traceId: %s", translatedCommandListStr,
		currentTenantCommandsTaskID)
	for _, translatedCommand := range translatedCommandList {
		log.GetLogger().Infof("Executing command: %s, traceId: %s", translatedCommand,
			currentTenantCommandsTaskID)
		if err = executeSingleCommand(ctx, translatedCommand); err != nil {
			log.GetLogger().Warnf("Command execution failed: %s, traceId: %s", err.Error(),
				currentTenantCommandsTaskID)
			errorMsg := fmt.Sprintf("execute command: %s fail, err: %s", translatedCommand, err.Error())
			return errors.New(errorMsg)
		}
	}
	return nil
}

func executeSingleCommand(ctx context.Context, translatedCommand string) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", translatedCommand)
	cmd.Dir = "/tmp"

	cred := &syscall.Credential{
		Uid:    uint32(constants.DefaultRuntimeUID),
		Gid:    uint32(constants.DefaultRuntimeGID),
		Groups: []uint32{constants.DefaultWorkerGID, constants.DefaultRuntimeGID},
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.GetLogger().Warnf("failed to get stderr pipe: %s, taskID: %s", err.Error(),
			currentTenantCommandsTaskID)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func(stderr io.ReadCloser) {
		defer wg.Done()
		if stderr == nil {
			return
		}
		// print error output
		scanner := bufio.NewScanner(stderr)
		totalOutputLength := 0
		for scanner.Scan() {
			line := scanner.Text()
			if totalOutputLength >= constants.TenantCommandErrOutputLimit {
				break
			}
			lastOutputLength := constants.TenantCommandErrOutputLimit - totalOutputLength
			if len(line) > lastOutputLength {
				line = line[:lastOutputLength] + "..."
			}
			totalOutputLength += len(line)
			log.GetLogger().Infof("error output of command: %s is %s, traceId: %s", translatedCommand, line,
				currentTenantCommandsTaskID)
		}
		if err = scanner.Err(); err != nil {
			log.GetLogger().Warnf("error reading stderr: %s, taskID: %s", err.Error(),
				currentTenantCommandsTaskID)
		}
	}(stderr)

	if err = cmd.Start(); err != nil {
		return err
	}

	// wait for reading of stderr
	wg.Wait()

	return cmd.Wait()
}

func translateCommands(ctx context.Context, tenantCommandList []string,
	variableMap map[string]string) ([]string, error) {
	translatedCommands := make([]string, 0, len(tenantCommandList))

	for _, command := range tenantCommandList {
		translatedCommand, err := translateSingleCommand(ctx, command, variableMap)
		if err != nil {
			log.GetLogger().Errorf("Failed to translate tenant command: %s, err: %s, traceId: %s", command,
				err.Error(), currentTenantCommandsTaskID)
			return translatedCommands, err
		}
		translatedCommands = append(translatedCommands, translatedCommand)
	}
	return translatedCommands, nil
}

func translateSingleCommand(ctx context.Context, command string, variableMap map[string]string) (string, error) {
	for strings.Contains(command, "${") {
		start := strings.Index(command, "${")
		end := strings.Index(command, "}")
		if start == -1 || end == -1 || end < start {
			return "", fmt.Errorf("invalid variable format in command: %s", command)
		}
		variableIntervals := 2
		variable := command[start+variableIntervals : end]
		var replacement string
		switch variable {
		case "dependencyPath":
			replacement = constants.PublicDependenciesPath
		case "runtimePath":
			replacement = constants.RuntimeBinPathPrefix
		default:
			replacementQueryCommand, exists := variableMap[variable]
			if !exists {
				return "", fmt.Errorf("variable %s not found in map", variable)
			}
			// max translate value length limit is 100
			replacementQueryCommand = replacementQueryCommand + " | cut -c 1-100"
			log.GetLogger().Infof("translate variable by command: %s", replacementQueryCommand)
			cmd := exec.CommandContext(ctx, "sh", "-c", replacementQueryCommand)
			cmd.Dir = "/tmp"
			output, err := cmd.Output()
			if err != nil {
				return "", fmt.Errorf("failed to get value for variable %s: %v", variable, err)
			}
			replacement = strings.TrimSpace(string(output))
		}
		command = strings.Replace(command, fmt.Sprintf("${%s}", variable), replacement, 1)
	}
	return command, nil
}

func handleResetDNSConfig(msg *tcpmsg.Message) (int, error) {
	var dnsConfig types.DNSConfig
	err := json.Unmarshal(msg.Body, &dnsConfig)
	if err != nil {
		return tcpmsg.MessageWithNoResponse, err
	}

	err = utils.ResetDNSConfig(dnsConfig)
	if err != nil {
		return tcpmsg.MessageWithNoResponse, err
	}
	return tcpmsg.MessageWithSuccessResponse, nil
}

func execPreStopCommand() (int, tcpmsg.RuntimeRSP, error) {
	output, err := process.ExecPreStopCommand()
	if err != nil {
		return 0, tcpmsg.RuntimeRSP{}, err
	}
	rsp := tcpmsg.RuntimeRSP{Result: output}
	return 0, rsp, nil
}

func initProcessOnce(code int, err error, rsp tcpmsg.RuntimeRSP, msg *tcpmsg.Message) (int, tcpmsg.RuntimeRSP, error) {
	initReqBody, err := getInitRequestBody(msg)

	// if can't parse, we think is_mcp_function is false
	if err != nil {
		log.GetLogger().Errorf("failed to get init req body, err: %s", err.Error())
	}

	log.GetLogger().Infof("initReqBody is %t when init process", initReqBody)

	code, err = process.InitRuntimeProcess(initReqBody)
	if err != nil || code == tcpmsg.RuntimeRestarted {
		return code, rsp, err
	}
	process.ClearChan <- true
	return code, rsp, err
}

func getInitRequestBody(msg *tcpmsg.Message) (*runtimemanagerrequest.InitRequestBody, error) {
	reqType := &runtimemanagerrequest.InitRequestBody{}
	err := json.Unmarshal(msg.Body, reqType)
	if err != nil {
		return nil, err
	}
	return reqType, nil
}

func pauseRuntimeProcess() error {
	if process.Manager.CurrentRuntime.Status == tcpmsg.RuntimeProcessStatusPaused {
		return nil
	}
	_, err := process.Manager.CurrentRuntime.ProcessSignal(syscall.SIGSTOP)
	return err
}

func recoverRuntimeProcess() error {
	if process.Manager.CurrentRuntime.Status == tcpmsg.RuntimeProcessStatusRecover {
		return nil
	}
	_, err := process.Manager.CurrentRuntime.ProcessSignal(syscall.SIGCONT)
	return err
}

func exit() {
	process.ExitOnce.Do(func() {
		close(process.ExitSignal)
	})
}
