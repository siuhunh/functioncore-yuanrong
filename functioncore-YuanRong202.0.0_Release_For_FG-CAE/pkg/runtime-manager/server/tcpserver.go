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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"

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
	default:
		err, code = errors.New("unknown request type"), tcpmsg.MessageWithNoResponse
	}
	return code, rsp, err
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
