/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

// Package mount is mount external file system
package mount

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
)

const (
	ecsListenPort    = 111
	ecsListenTimeOut = 200 * time.Millisecond
	typeECS          = "ecs"
	typeSFS3         = "sfs3.0"
	statusDisabled   = "disabled"
	timeout          = 240

	// make sure user can access all parent folders
	fileMode        = 0555
	hostAndPathSize = 2
)

// VolumesForFunction mount volumes for function
func VolumesForFunction(mountConfig types.FuncMountConfig) error {
	funcMounts := mountConfig.FuncMounts
	if len(funcMounts) <= 0 {
		return nil
	}
	mountCount := 0
	for _, mount := range funcMounts {
		if mount.Status != statusDisabled {
			mountCount++
			err := mountVolumeForFunction(mount)
			if err != nil {
				return err
			}
		} else {
			log.GetLogger().Warnf("mount resource %s not ready", mount.MountSharePath)
		}
	}
	log.GetLogger().Infof("succeed to mount %d volumes for current function", mountCount)
	return nil
}

// UmountVolumesForFunction umount volumes for function
func UmountVolumesForFunction(mountConfig types.FuncMountConfig) error {
	funcMounts := mountConfig.FuncMounts
	if len(funcMounts) <= 0 {
		return nil
	}
	mountCount := 0
	for _, mount := range funcMounts {
		if mount.Status != statusDisabled {
			mountCount++
			log.GetLogger().Infof("start to umount local dir: %s", mount.LocalMountPath)
			err := syscall.Unmount(mount.LocalMountPath, 0)
			if err != nil {
				log.GetLogger().Warnf("failed to umount localPath %s, err: %s",
					mount.LocalMountPath, err.Error())
			}
		} else {
			log.GetLogger().Warnf("umount resource %s not ready", mount.MountSharePath)
		}
	}
	log.GetLogger().Infof("succeed to umount %d volumes for current function", mountCount)
	return nil
}

func mountVolumeForFunction(mount types.FuncMount) error {
	err := mkLocalDirMountPath(mount.LocalMountPath)
	if err != nil {
		return err
	}
	if mount.MountType == typeECS {
		if err = checkECSMountStatus(mount.MountSharePath); err != nil {
			log.GetLogger().Warnf("specified path is not shared, " + err.Error())
			return err
		}
	}
	log.GetLogger().Infof("start to mount share dir: %s to local dir: %s",
		mount.MountSharePath, mount.LocalMountPath)
	err = execMountCommand(mount.MountSharePath, mount.MountType, mount.LocalMountPath)
	if err != nil {
		log.GetLogger().Errorf("failed to exec mount command: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("succeed to mount a share dir: %s", mount.MountSharePath)
	return nil
}

func mkLocalDirMountPath(localMountPath string) snerror.SNError {
	_, err := os.Stat(localMountPath)
	if err == nil || os.IsExist(err) {
		log.GetLogger().Warnf("failed to mount exist system path")
		return snerror.New(snerror.RuntimeMountErrorCode, "failed to mount exist system path")
	}
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)
	err = os.MkdirAll(localMountPath, fileMode)
	if err != nil {
		log.GetLogger().Errorf("failed to create local mount path")
		return snerror.New(snerror.RuntimeMountErrorCode, "failed to create local mount path")
	}
	return nil
}

func checkECSMountStatus(mountSharePath string) snerror.SNError {
	ipAndPath := strings.Split(mountSharePath, ":")
	ip := ipAndPath[0]
	address := ip + ":" + strconv.Itoa(ecsListenPort)
	// nfs server need to monitor 111/20048 port
	_, err := net.DialTimeout("tcp", address, ecsListenTimeOut)
	if err != nil {
		log.GetLogger().Errorf("specified ecs is unreachable, error: %s", err.Error())
		return snerror.New(snerror.RuntimeMountErrorCode, "specified path is not shared, "+err.Error())
	}
	return nil
}

func execMountCommand(mountSharePath, mountType, localMountPath string) snerror.SNError {
	hostAndPath := strings.Split(mountSharePath, ":")
	if len(hostAndPath) != hostAndPathSize {
		err := fmt.Errorf("unexpected nfs server path: %s, expect format: host:path", mountSharePath)
		log.GetLogger().Errorf("%s", err.Error())
		return snerror.New(snerror.RuntimeMountErrorCode, err.Error())
	}
	host := hostAndPath[0]
	address, err := net.LookupHost(host)
	if err != nil {
		err := fmt.Errorf("failed to lookup host: %s, error: %s", host, err)
		log.GetLogger().Errorf("%s", err.Error())
		return snerror.New(snerror.RuntimeMountErrorCode, err.Error())
	}
	for _, addr := range address {
		if mountType == typeSFS3 {
			// proto=tcp for SFS3.0
			opts := fmt.Sprintf("addr=%s,vers=3,timeo=%d,nolock,proto=tcp", addr, timeout)
			err := syscall.Mount(mountSharePath, localMountPath, "nfs",
				syscall.MS_NOSUID|syscall.MS_NODEV, opts)
			if err == nil {
				return nil
			}
			log.GetLogger().Warnf("failed to mount localPath %s to sfs 3.0 server %s at resolved address %s, error: %s",
				localMountPath, mountSharePath, addr, err)
		} else {
			// vers=3 asks nfs to use version 3
			opts := fmt.Sprintf("addr=%s,vers=3,timeo=%d,nolock", addr, timeout)
			err := syscall.Mount(mountSharePath, localMountPath, "nfs",
				syscall.MS_NOSUID|syscall.MS_NODEV, opts)
			if err == nil {
				return nil
			}
			log.GetLogger().Warnf("failed to mount localPath %s to nfs server %s at resolved address %s, error: %s",
				localMountPath, mountSharePath, addr, err)
		}
	}

	log.GetLogger().Errorf("failed to mount localPath %s to nfs server %s, all addresses were tried: %v",
		localMountPath, mountSharePath, address)
	err = fmt.Errorf("failed to mount localPath %s to nfs server %s", localMountPath, mountSharePath)
	return snerror.New(snerror.RuntimeMountErrorCode, err.Error())
}
