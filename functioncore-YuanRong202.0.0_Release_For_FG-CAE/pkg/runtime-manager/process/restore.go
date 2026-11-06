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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/runtimemanagerrequest"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/utils"
	runtimeutils "functioncore/pkg/runtime-manager/utils"
)

const (
	snapshotTmpFilePerm = 0770
	restoreWaitTimeout  = 20 * time.Second
)

func restoreRuntimeProcess(uid, gid int) (int, error) {
	paths := strings.Split(runtimeutils.GetPreservePath(getLogPolicy()), ":")
	fns := make([]func() error, len(paths))
	for i, path := range paths {
		restoreFn, err := preparePath(path)
		if err != nil {
			log.GetLogger().Errorf("failed to prepare path: %s, %s", path, err.Error())
			return 0, err
		}
		fns[i] = restoreFn
	}

	code, err := startRestoreProcess(uid, gid)
	if err != nil {
		log.GetLogger().Errorf("failed to start restore process %s", err.Error())
		return code, err
	}

	for _, fn := range fns {
		if err := fn(); err != nil {
			return 0, err
		}
	}

	return code, nil
}

func preparePath(path string) (func() error, error) {
	info, err := os.Stat(path)
	log.GetLogger().Infof("preparePath for restore,path %s info %+v", path, info.Mode())
	if err != nil {
		return nil, fmt.Errorf("stat path: %s, %s", path, err.Error())
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("path %s is not a directory", path)
	}

	if err := removeContents(path); err != nil {
		return nil, fmt.Errorf("remove contents from path: %s, %s", path, err.Error())
	}
	if err := os.Chmod(path, snapshotTmpFilePerm); err != nil {
		return nil, fmt.Errorf("chmod path: %s, %s", path, err.Error())
	}
	return func() error {
		if err := os.Chmod(path, info.Mode()); err != nil {
			return fmt.Errorf("restore path: %s, %s", path, err.Error())
		}
		return nil
	}, nil
}

func removeContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func startRestoreProcess(uid, gid int) (int, error) {
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)
	if err := os.Mkdir(constants.FastFreezeRestoreNotifyPath, snapshotTmpFilePerm); err != nil {
		return 0, fmt.Errorf("mkdir, %s", err.Error())
	}

	// backup the fastfreezelog to runtime log path which will be stored on the host machine
	defer backupFastfreezeLogs()
	defer os.RemoveAll(constants.FastFreezeRestoreNotifyPath)

	// fastfreeze will create a file in notifyPath when restore finishes
	finishCh := make(chan struct{})
	var finishOnce sync.Once
	monitor, err := diskusage.New(constants.FastFreezeRestoreNotifyPath, diskusage.AddProcessHooks(
		func(fsnotify.Event) {
			finishOnce.Do(func() { close(finishCh) })
		}))
	if err != nil {
		return 0, fmt.Errorf("diskusage %s", err.Error())
	}
	defer monitor.Close()

	timeoutCtx, cancel := context.WithTimeout(context.Background(), restoreWaitTimeout)
	defer cancel()

	code, err := startRuntimeProcess(uid, gid, &runtimemanagerrequest.InitRequestBody{})
	if err != nil {
		log.GetLogger().Errorf("failed to start runtime process: %s", err.Error())
		return code, err
	}

	select {
	case msg := <-inspectProcessChannel:
		log.GetLogger().Errorf("failed to restore runtime process: %s, go to restart", msg)
		return 0, nil
	case <-timeoutCtx.Done():
		Manager.currentMode = restoreDead
		log.GetLogger().Errorf("Restore timeout, go to normal start")
		notifyMessageWithNoResp(tcpmsg.RestoreDead, nil)
		logReportEnable = false
		return 0, nil
	case <-finishCh:
		log.GetLogger().Infof("fastfreeze restore finished")
		return code, nil
	}
}

func backupFastfreezeLogs() {
	err := utils.CopyDir(constants.FastFreezeLogPath, "/home/sn/log")
	if err != nil {
		log.GetLogger().Errorf("failed to backup Fastfreeze Logs: %s", err.Error())
		return
	}
}
