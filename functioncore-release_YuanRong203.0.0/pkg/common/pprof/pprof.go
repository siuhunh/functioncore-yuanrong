/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

// Package pprof Package options define utils
package pprof

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"time"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

const (
	lookupGoroutine LookupType = iota
	lookupThreadcreate
	lookupHeap
	lookupAllocs
	lookupBlock
	lookupMutex

	defaultPprofFilePath      = "/home/sn/pprof"
	defaultCollectPeriod      = 600
	pprofLogConfigTAG         = "PPROF_LOG_CONFIG"
	fileMode                  = 0750
	ki                        = 1024
	halfKi                    = 512
	pprofCollectDebugModeZero = 0
	pprofCollectDebugModeTwo  = 2
)

var (
	collectPprofFileSwitch = false
	switchLock             sync.Mutex
	pprofList              = []string{"goroutine", "thread-create", "heap", "allocs", "block", "mutex"}
	pprofFilePath          = defaultPprofFilePath
	collectPeriod          = defaultCollectPeriod
	debugMode              = pprofCollectDebugModeZero
)

// LookupType collect pprof type
type LookupType int8

// LogConfig CoreInfo contains the core info
type LogConfig struct {
	FilePath      string `json:"filepath" valid:",optional"`
	CollectPeriod int    `json:"collectPeriod" valid:",optional"`
	Debug         int    `json:"debug_mode" valid:",optional"`
}

func startCollectMemory(timeStamp string) {
	for i := lookupGoroutine; i <= lookupMutex; i++ {
		collectPprofFilePath := filepath.Join(pprofFilePath, timeStamp, pprofList[i]+".prof."+timeStamp)
		err := lookupPprof(i, collectPprofFilePath)
		if err != nil {
			log.GetLogger().Warnf("collect heap pprof collect file failed! err: %v\n", err)
		}
	}
}

func lookupPprof(lookupType LookupType, filePath string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	err = file.Chmod(fileMode)
	if err != nil {
		return err
	}
	defer file.Close()
	p := pprof.Lookup(pprofList[lookupType])
	if p != nil {
		err = p.WriteTo(file, debugMode)
	}
	return err
}

func initPprofCollectConfig() {
	var pprofLogConfig LogConfig
	logConfig := os.Getenv(pprofLogConfigTAG)
	if logConfig == "" {
		log.GetLogger().Infof("logConfig is nil, use defaultPprofConfig")
		return
	}
	err := json.Unmarshal([]byte(logConfig), &pprofLogConfig)
	if err != nil {
		log.GetLogger().Infof("Unmarshal logConfig fail, use defaultPprofConfig, err: %s", err.Error())
		return
	}

	if _, err = govalidator.ValidateStruct(pprofLogConfig); err != nil {
		log.GetLogger().Infof("ValidateStruct pprofLogConfig fail, use defaultPprofConfig,"+
			"err: %s", err.Error())
		return
	}

	configPprofFilePath(pprofLogConfig)
	configCollectPeriod(pprofLogConfig)
	configDebugMode(pprofLogConfig)
}

func configDebugMode(config LogConfig) {
	if config.Debug < pprofCollectDebugModeZero || config.Debug > pprofCollectDebugModeTwo {
		return
	}
	debugMode = config.Debug
}

func configCollectPeriod(pprofLogConfig LogConfig) {
	if pprofLogConfig.CollectPeriod < 60 { // at lease 60s to collect cpu info
		log.GetLogger().Infof("pprofLogConfig.CollectPeriod < 60s, at lease 60s to collect cpu info")
		return
	}
	collectPeriod = pprofLogConfig.CollectPeriod
}

func configPprofFilePath(pprofLogConfig LogConfig) {
	if pprofLogConfig.FilePath == "" {
		log.GetLogger().Infof("UpprofLogConfig.FilePath is nil, use defaultPprofFilePath")
		return
	}

	if err := utils.ValidateFilePath(pprofLogConfig.FilePath); err != nil {
		log.GetLogger().Infof("ValidateFilePath pprofLogConfig.FilePath fail,"+
			"use defaultPprofFilePath, err: %s", err.Error())
		return
	}
	pprofFilePath = pprofLogConfig.FilePath
}

func startCollectCPU(timeStamp string) *os.File {
	collectCPUPprofFilePath := filepath.Join(pprofFilePath, timeStamp, "cpu.prof."+timeStamp)

	file, err := os.Create(collectCPUPprofFilePath)
	if err != nil {
		log.GetLogger().Warnf("create cpu pprof collect file fail! err: %v\n", err)
		return file
	}
	err = file.Chmod(fileMode)
	if err != nil {
		log.GetLogger().Warnf("modify cpu pprof collect file operate right fail! err: %v\n", err)
		return file
	}

	err = pprof.StartCPUProfile(file)
	if err != nil {
		log.GetLogger().Debugf("StartCPUProfile fail! err: %s", err.Error())
	}
	return file
}

// NotifyStartPprof notify collectPprofInstanceList config change to pprof collect program
func NotifyStartPprof(collectPprofInstanceList []string) {
	podName := os.Getenv("POD_NAME")
	if podName == "" {
		log.GetLogger().Warnf("pprof can not get this pod name")
		return
	}

	if len(collectPprofInstanceList) == 0 {
		return
	}

	if strings.Contains(strings.Join(collectPprofInstanceList, ","), podName) {
		// set collect rate of pprof collect to 512 * 1024 per time
		runtime.MemProfileRate = halfKi * ki
		collectPprof()
	} else {
		switchLock.Lock()
		collectPprofFileSwitch = false
		switchLock.Unlock()
	}
}

func collectPprof() {
	switchLock.Lock()
	if collectPprofFileSwitch {
		switchLock.Unlock()
		return
	}
	collectPprofFileSwitch = true
	switchLock.Unlock()
	initPprofCollectConfig()
	go func() {
		for {
			switchLock.Lock()
			if !collectPprofFileSwitch {
				switchLock.Unlock()
				break
			}
			switchLock.Unlock()
			timeStamp := time.Now().Format("20060102150405")
			log.GetLogger().Infof("start collect pprof info of %s, pprofFilePath: %s, collectPeriod: %d,"+
				"debug_mode: %d", os.Getenv("POD_NAME"), pprofFilePath, collectPeriod, debugMode)
			err := os.MkdirAll(filepath.Join(pprofFilePath, timeStamp), fileMode)
			if err != nil {
				log.GetLogger().Warnf("create pprof directory %s fail! err: %s",
					filepath.Join(pprofFilePath, timeStamp), err.Error())
				break
			}
			cpuFile := startCollectCPU(timeStamp)
			startCollectMemory(timeStamp)
			time.Sleep(time.Duration(collectPeriod) * time.Second)
			stopCollectCPU(cpuFile)
			log.GetLogger().Infof("finish collect pprof info of %s", os.Getenv("POD_NAME"))
		}
	}()
}

func stopCollectCPU(cpuFile *os.File) {
	pprof.StopCPUProfile()
	if cpuFile != nil {
		err := cpuFile.Close()
		if err != nil {
			log.GetLogger().Debugf("collectCpuPprofFile already close!")
		}
	}
}
