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

// Package fusion is fusion runtime
package fusion

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/tcpmsg"
	commonutil "functioncore/pkg/common/utils"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/functionenv"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/custom"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/telemetry"
	"functioncore/pkg/worker/utils"
)

const (
	// retry at most 3 second
	reconnectRuntimeManagerTimes = 3000
	retryConnectingInterval      = 10 * time.Millisecond

	minIDValueAndDefault = -1

	requestNumCheckPeriod  = 1 * time.Second
	userExtensionLogPrefix = "[Extension]"
)

var (
	checkpointStarted     = make(chan struct{})
	checkpointStartedOnce sync.Once
	preStopped            = false
)

type creator struct {
	configuration *config.Configuration
	rt            *fusionRuntime
}

// RegisterRuntime -
func RegisterRuntime() {
	runtime.Registry.Register("fusion", &creator{})
}

// NewRuntime -
func (c *creator) NewRuntime(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
	c.configuration = conf
	log.GetLogger().Infof("new runtime language: %s", conf.RuntimeConfig.Language)
	conf.RuntimeConfig.RPCPort = conf.RMConfig.RuntimeConfigs[conf.Language].Port

	var fn newRuntimeFn
	if conf.Language == worker.CustomRuntimeLanguage {
		fn = custom.CreateFusionCustomRuntime
	} else {
		fn = runtime.Registry.GetCreator(conf.Language).NewRuntime
	}

	rt, err := newFusionRuntime(conf, handlers, fn)
	if err != nil {
		log.GetLogger().Errorf("failed to new restartable runtime: %s", err.Error())
		return nil, err
	}
	c.rt = rt

	if conf.Language != worker.CustomRuntimeLanguage {
		if err := custom.StopFusionCustomHTTPServer(); err != nil {
			log.GetLogger().Errorf("error when stops the custom http server: %s", err)
			return nil, err
		}
	}

	return c.rt, nil
}

func isDefaultUIDGID(uid, gid int) bool {
	if uid != 0 && uid != minIDValueAndDefault && uid != constants.DefaultRuntimeUID {
		return false
	}
	if gid != 0 && gid != minIDValueAndDefault && gid != constants.DefaultRuntimeGID {
		return false
	}
	return true
}

// Prestart connect runtime server
func (c *creator) Prestart(configuration *config.RuntimeConfig) error {
	var err error
	for i := 0; i < reconnectRuntimeManagerTimes; i++ {
		if err = c.newClientForManager(configuration.RMConfig.ServerPort); err == nil {
			break
		}
		time.Sleep(retryConnectingInterval)
	}
	if err != nil {
		log.GetLogger().Errorf("new runtime manager client error: %s", err)
		return err
	}

	// start custom runtime http server
	if err = custom.StartFusionCustomHTTPServer(configuration); err != nil {
		log.GetLogger().Warnf("failed to start custom http server with error: %s", err)
	}
	for runtimeName, config := range configuration.RMConfig.RuntimeConfigs {
		if !utils.IsRuntimeEnable(&config) {
			continue
		}
		configuration.RPCPort = config.Port
		configuration.Language = runtimeName
		if err = runtime.Registry.Prestart(runtimeName, configuration); err != nil {
			log.GetLogger().Warnf("prestartRuntimeFail | %s | %s", runtimeName, err.Error())
			continue
		}
		log.GetLogger().Debugf("succeed to prestart runtime: %s", runtimeName)
	}
	return nil
}

func (c *creator) newClientForManager(port int32) error {
	var err error
	runtime.RuntimeManagerClient, err = tcpmsg.CreateSocketClient(tcpmsg.RuntimeUDS, c.requestHandler)
	if err != nil {
		runtime.RuntimeManagerClient = nil
		return err
	}
	log.GetLogger().Infof("succeed to new client for runtime manager with port: %d", port)
	return nil
}

func (c *creator) requestHandler(msg *tcpmsg.Message) (int, tcpmsg.RuntimeRSP, error) {
	switch msg.GetMsgType() {
	case tcpmsg.ReportLog:
		if logger, err := functionlog.GetFunctionLogger(c.configuration); err == nil && logger != nil {
			buildTelemetryExtensionEvent(msg)
			logger.WriteStdLog(string(msg.Body), time.Now().UTC().Format(functionlog.FuncLogTimeFormat),
				runtime.PodApplier == constants.WorkerManagerApplier,
				time.Now().UTC().Format(functionlog.NanoLogLayout))
		}
	case tcpmsg.LogFinished:
		if logger, err := functionlog.GetFunctionLogger(c.configuration); err == nil && logger != nil {
			recorder, ok := logger.GetLogRecorderFromInvokeID(msg.Body)
			if ok {
				log.GetLogger().Debugf("log collection finished")
				recorder.FinishSync()
			} else {
				log.GetLogger().Warnf("failed to handle LogFinished message, unknown invoke id %v", msg.Body)
			}
		}
	case tcpmsg.ReportMemory:
		runtime.RunMetrics.UpdateMemoryUsed(commonutil.ByteToFloat64(msg.Body))
	case tcpmsg.ReportCPU:
		cpuUsed := runtime.CPUUsed{}
		cpuUsed.DecodeFrom(msg.Body)
		runtime.RunMetrics.UpdateCPUUsed(cpuUsed)
	case tcpmsg.ReportCaeMetrics:
		caeMetrics := monitor.CaeMetrics{}
		caeMetrics.DecodeFrom(msg.Body)
		runtime.RunMetrics.UpdateCaeMetrics(caeMetrics)
	case tcpmsg.ReportRequest:
		go stopRuntimeManagerProcess()
	case tcpmsg.RuntimeProcessExit:
		go c.rt.Reconnect(c.configuration)
	case tcpmsg.RestoreDead:
		go c.rt.HandlerRestoreDead()
	case tcpmsg.ExtensionProcessExit:
		go runtime.GetExtensionManager(c.configuration).GetExtensionState()
	default:
		log.GetLogger().Warnf("unknown request type: %d", msg.GetMsgType())
		return tcpmsg.MessageWithNoResponse, tcpmsg.RuntimeRSP{}, errors.New("unknown request type")
	}
	return tcpmsg.MessageWithNoResponse, tcpmsg.RuntimeRSP{}, nil
}

// StopRuntimeProcess stop other runtime
func StopRuntimeProcess(conf *config.Configuration) tcpmsg.RuntimeManagerRSP {
	envs, err := functionenv.FetchEnvFromConfigWithHandlerErr(conf)
	if err != nil {
		log.GetLogger().Warnf("failed to fetch environment from the configuration, error: %s", err.Error())
	}
	reqBody := &config.RequestBody{
		Version: conf.RuntimeConfig.Language,
		Conf:    conf,
		Envs:    envs,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return tcpmsg.RuntimeManagerRSP{
			Err: err.Error(),
		}
	}
	msg := tcpmsg.BuildMessage(tcpmsg.StopProcess, bodyBytes)
	return runtime.RuntimeManagerClient.SendMessage(msg)
}

func initRuntimeProcess() tcpmsg.RuntimeManagerRSP {
	npmConfigRegistry, uvIndexURL := utils.GetProxyURL()
	reqBody := &runtimemanagerrequest.InitRequestBody{
		IsMcpFunction:     utils.GetIsMcpFunction(),
		NPMConfigRegistry: npmConfigRegistry,
		UVIndexURL:        uvIndexURL,
	}
	reqMsg := []byte("")

	reqMsg, err := json.Marshal(reqBody)
	if err != nil {
		reqMsg = []byte("")
		log.GetLogger().Warnf("failed to marshal req, isMcpFunction: %t", utils.GetIsMcpFunction())
	}

	msg := tcpmsg.BuildMessage(tcpmsg.InitProcess, reqMsg)
	return runtime.RuntimeManagerClient.SendMessage(msg)
}

// SendCheckpointCommand send checkpoint command to runtime-manager
func SendCheckpointCommand() error {
	checkpointStartedOnce.Do(func() {
		close(checkpointStarted)
	})

	msg := tcpmsg.BuildMessage(tcpmsg.FastFreezeCheckpoint, nil)
	resp := runtime.RuntimeManagerClient.SendMessage(msg)
	if resp.Code != 0 {
		log.GetLogger().Errorf("start checkpoint runtime process response error, code: %d, msg: %s",
			resp.Code, resp.Err)
		return fmt.Errorf("code: %d, msg: %s", resp.Code, resp.Err)
	}
	return nil
}

func stopRuntimeManagerProcess() {
	requestNumCheckTicker := time.NewTicker(requestNumCheckPeriod)
	defer requestNumCheckTicker.Stop()

	for {
		<-requestNumCheckTicker.C
		requestCount := runtime.RunMetrics.GetRequestCount()
		if requestCount == 0 && preStopped && status.Get() != status.Initializing {
			runtime.RuntimeManagerClient.SendMessageWithNoResp(tcpmsg.BuildMessage(tcpmsg.StopRuntimeManager, nil))
			break
		}
	}
}

func buildTelemetryExtensionEvent(msg *tcpmsg.Message) {
	if !slice.ContainsString(telemetry.Config.Types, constants.TelemetryExtensionType, nil) {
		return
	}
	logMsg := string(msg.Body)
	if strings.Contains(logMsg, userExtensionLogPrefix) {
		logMsg = strings.TrimRight(logMsg, "\n")
		split := strings.Split(logMsg, userExtensionLogPrefix)
		event := telemetry.Event{
			StartTime: time.Now().UTC().Format(functionlog.LogTimeFormat),
			Type:      constants.TelemetryExtensionType,
			Name:      constants.TelemetryExtensionType,
			Logs:      strings.Join(split, ""),
		}
		telemetry.SendEvent(event)
	}
}
