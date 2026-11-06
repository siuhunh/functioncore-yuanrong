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

// Package process Lifecycle Management of Runtime Processes
package process

import (
	"context"
	"errors"
	"io/fs"
	"io/ioutil"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"functioncore/pkg/common/config"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/runtime-manager/types"
	"functioncore/pkg/runtime-manager/utils"
)

const (
	retryTimes            = 3
	pauseState            = 84
	retryIntervalDuration = time.Millisecond
	getDirTimeout         = 5 * time.Second
	checkInterval         = 5 * time.Second
	monitorDir            = "/home/snuser"
	defaultMaxDirSize     = 1024
	duCmdOutputLength     = 2
)

var firstDirSize int32
var startTimerOnce sync.Once

// RuntimeProcess the user's runtime process
type RuntimeProcess struct {
	Process       *os.Process
	DebugEnv      []string
	UID           int
	Gid           int
	Version       string
	Status        tcpmsg.RuntimeProcessStatus
	extensionPids sync.Map
}

// ExtensionProcess the user's extension process
type ExtensionProcess struct {
	Process *os.Process
	UID     int
	GID     int
	Status  tcpmsg.RuntimeProcessStatus
	Name    string
}

// ProcessSignal process the signal to runtime process
func (runtime *RuntimeProcess) ProcessSignal(sig syscall.Signal) ([]int, error) {
	if runtime.Process == nil {
		log.GetLogger().Warnf("the runtime process is nil, ignore the signal %s", sig.String())
		return nil, nil
	}
	runtimeProcessPid := runtime.Process.Pid
	runtimeProcessEntry, err := utils.GetSelfAndSubAndOrphanProcessEntries(runtime.UID, runtimeProcessPid)
	if err != nil {
		log.GetLogger().Errorf("failed to get sub(%d) and orphan processes, error: %s", runtimeProcessPid, err.Error())
		return nil, err
	}
	pids := make([]int, 0, 0)
	for _, processEntry := range runtimeProcessEntry.SubProcessEntries {
		log.GetLogger().Debugf("sub process entry pid %d", processEntry.Pid)
		pids = append(pids, processEntry.Pid)
	}
	for _, processEntry := range runtimeProcessEntry.OrphanProcessEntries {
		log.GetLogger().Debugf("orphan process entry pid %d", processEntry.Pid)
		if sig == syscall.SIGKILL {
			if _, ok := runtime.extensionPids.Load(processEntry.Pid); ok {
				log.GetLogger().Infof("extension process entry pid %d", processEntry.Pid)
				continue
			}
		}
		pids = append(pids, processEntry.Pid)
	}
	if len(pids) == 0 {
		// no process to be signaled
		return nil, nil
	}
	// signal process
	sigNo := getSigNo(sig)
	startTime := time.Now()
	log.GetLogger().Debugf("this pids %v will be %s", pids, sig.String())
	for _, pid := range pids {
		err = syscall.Kill(pid, sig)
		if err != nil {
			log.GetLogger().Warnf("send signal '%s' to process '%d' error: %s", sig.String(), pid, err.Error())
			runtime.Status = tcpmsg.RuntimeProcessStatusException
			continue
		}
		log.GetLogger().Debugf("send signal '%s' to process '%d' ok, cost: %+v.",
			sigNo, pid, time.Since(startTime))
	}
	runtime.Status = sigNo
	if sigNo == tcpmsg.RuntimeProcessStatusPaused {
		go runtime.checkProcessPauseState(pids)
	}
	return pids, nil
}

func getSigNo(sig os.Signal) tcpmsg.RuntimeProcessStatus {
	switch sig {
	case syscall.SIGSTOP:
		return tcpmsg.RuntimeProcessStatusPaused
	case syscall.SIGCONT:
		return tcpmsg.RuntimeProcessStatusRecover
	default:
		return tcpmsg.RuntimeProcessStatusRunning
	}
}

func (runtime *RuntimeProcess) restartRuntimeProcessWithRetry() error {
	log.GetLogger().Debugf("the runtime %s will be restarted", runtime.Version)
	var err error
	for i := 0; i < retryTimes; i++ {
		log.GetLogger().Debugf("restart runtime %s , %d times ", runtime.Version, i)
		err = runtime.restartRuntimeProcess()
		if err != nil {
			time.Sleep(retryIntervalDuration)
			continue
		}
		break
	}
	return err
}

func (runtime *RuntimeProcess) restartRuntimeProcess() error {
	if err := runtime.stopAllUserProcess(); err != nil {
		log.GetLogger().Errorf("failed to stop all user process %s", err.Error())
		return err
	}
	if err := runtime.restart(); err != nil {
		log.GetLogger().Errorf("failed to restart runtime %s", err.Error())
		return err
	}
	return nil
}

func (runtime *RuntimeProcess) restart() error {
	err := CreateAndSetProcess(types.NewCreateOptions(runtime.UID, runtime.Gid, Manager.conf,
		types.WithVersion(runtime.Version), types.WithDebugEnv(runtime.DebugEnv)))
	if err != nil {
		log.GetLogger().Errorf("failed to restart runtime %s", err.Error())
		return err
	}
	return nil
}

func (runtime *RuntimeProcess) stopAllUserProcess() error {
	pids, err := runtime.ProcessSignal(syscall.SIGKILL)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	wg.Add(len(pids))
	for _, pid := range pids {
		go runtime.waitProcess(pid, &wg)
	}
	wg.Wait()
	log.GetLogger().Infof("runtimeExit | pid %v", pids)
	return nil
}

func (runtime *RuntimeProcess) waitProcess(pid int, wg *sync.WaitGroup) {
	if pid == runtime.Process.Pid {
		// wait until the old process exits completely. Otherwise, the memory usage remains unchanged.
		_, err := runtime.Process.Wait()
		log.GetLogger().Debugf("runtime process pid %d exit", runtime.Process.Pid)
		if err != nil {
			log.GetLogger().Warnf("failed to kill pid %d, negligible Exceptions err %s",
				runtime.Process.Pid, err.Error())
		}
		wg.Done()
		return
	}
	ps, err := os.FindProcess(pid)
	if err != nil {
		wg.Done()
		return
	}
	if ps != nil {
		_, err = ps.Wait()
		if err != nil {
			log.GetLogger().Warnf("pid %d wait err %s", runtime.Process.Pid, err.Error())
		}
		wg.Done()
	}
}

func (runtime *RuntimeProcess) checkProcessPauseState(pids []int) {
	for _, pid := range pids {
		if runtime.Status != tcpmsg.RuntimeProcessStatusPaused {
			log.GetLogger().Infof("process status is not paused, status: %v", runtime.Status)
			return
		}
		fileInfos, err := ioutil.ReadDir("/proc")
		if err != nil {
			log.GetLogger().Errorf("failed to list fileInfos of the dir /proc.")
			return
		}
		runtime.checkFileInfo(fileInfos, pid)
	}
}

func (runtime *RuntimeProcess) checkFileInfo(fileInfos []fs.FileInfo, pid int) {
	for _, fileInfo := range fileInfos {
		for i := 0; i < retryTimes; i++ {
			if runtime.Status != tcpmsg.RuntimeProcessStatusPaused {
				log.GetLogger().Infof("process status is not paused")
				return
			}
			info := utils.GetFileInfo(fileInfo)
			if info.Pid == 0 && info.State == 0 {
				break
			}
			if (info.Pid == pid && info.State == rune(pauseState)) || info.Pid != pid {
				break
			}
			if info.Pid == pid && info.State != rune(pauseState) && i != retryTimes-1 {
				log.GetLogger().Warnf("failed to pause process %d", pid)
				time.Sleep(time.Second)
				continue
			}
			if info.Pid == pid && info.State != rune(pauseState) && i == retryTimes-1 {
				log.GetLogger().Errorf("failed to pause process %d for %d times", pid, retryTimes)
				exit()
				return
			}
		}
	}
}

func exit() {
	ExitOnce.Do(func() {
		close(ExitSignal)
	})
	ExitSignalOnce.Do(func() {
		close(ExitSignalOnNoRequest)
	})
}

// CheckSnuserDirSize check whether the snuser directory size exceeds the limit.
func CheckSnuserDirSize() {
	curDirSizeLimit := getSnuserDirSizeLimit()
	firstGetDirSizeWithTimeOut(curDirSizeLimit, monitorDir)
}

func checkSnuserDirSizePeriodically(stopChan <-chan struct{}) {
	log.GetLogger().Debugf("start task: CheckSnuserDirSizePeriodically.")
	startTimerOnce.Do(func() {
		curDirSizeLimit := getSnuserDirSizeLimit()
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()
		curFirstDirSize := atomic.LoadInt32(&firstDirSize)
		for {
			select {
			case <-ticker.C:
				size, err := getDirSize(monitorDir)
				if err != nil {
					continue
				}
				if int32(size)-curFirstDirSize > curDirSizeLimit {
					log.GetLogger().Errorf("the current /home/snuser directory exceeds the limit %d", size)
					exit()
					return
				}
			case _, ok := <-stopChan:
				if !ok {
					log.GetLogger().Warnf("stop channel is closed")
				}
				return
			}
		}
	})
}

func getDirSize(monitorDir string) (int, error) {
	duCommandLine := "du -sm " + monitorDir
	sizeRes, err := exec.Command("/bin/bash", "-c", duCommandLine).Output()
	if err != nil {
		log.GetLogger().Warnf("failed to du with '%s' and error %s", duCommandLine, err.Error())
		return 0, err
	}
	dirSize := strings.Split(string(sizeRes), "\t")
	if len(dirSize) != duCmdOutputLength {
		log.GetLogger().Warnf("failed to du with '%s' and output %s", duCommandLine, string(sizeRes))
		return 0, errors.New("failed to exec du")
	}
	size, err := strconv.Atoi(dirSize[0])
	if err != nil {
		log.GetLogger().Warnf("failed to atoi, err: %s", err.Error())
		return 0, errors.New("failed to atoi")
	}
	return size, nil
}

func getSnuserDirSizeLimit() int32 {
	rmConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s. use defaultMaxDirSize", err.Error())
		return defaultMaxDirSize
	}
	curDirSizeLimit := rmConfig.SnuserDirSizeLimit
	if curDirSizeLimit <= 0 {
		curDirSizeLimit = defaultMaxDirSize
		log.GetLogger().Infof("failed to get snuser dir size limit, use default limit 1024M")
	}
	return curDirSizeLimit
}

func firstGetDirSizeWithTimeOut(curDirSizeLimit int32, monitorDir string) {
	ctx, cancel := context.WithTimeout(context.Background(), getDirTimeout)
	defer cancel()
	finishCh := make(chan struct{})
	go func() {
		size, err := getDirSize(monitorDir)
		if err != nil {
			atomic.CompareAndSwapInt32(&firstDirSize, 0, curDirSizeLimit)
		} else {
			atomic.CompareAndSwapInt32(&firstDirSize, 0, int32(size))
		}
		close(finishCh)
	}()

	select {
	case <-ctx.Done():
		atomic.CompareAndSwapInt32(&firstDirSize, 0, curDirSizeLimit)
		log.GetLogger().Warnf("get /home/snuser directory size timeout, set firstSize as %dM", curDirSizeLimit)
	case _, ok := <-finishCh:
		if !ok {
			log.GetLogger().Infof("succeed to get /home/snuser directory size %dM", firstDirSize)
		}
	}
}
