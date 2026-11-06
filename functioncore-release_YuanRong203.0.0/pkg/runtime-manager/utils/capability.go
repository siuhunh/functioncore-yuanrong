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

// Package utils provides utilities for manipulating POSIX capabilities.
package utils

import (
	"os"
	sysRuntime "runtime"
	"syscall"
	"unsafe"

	"functioncore/pkg/common/logger/log"
)

const (
	capDacOverride       = 1
	capFowner            = 3
	capKill              = 5
	capSetGid            = 6
	capSetUid            = 7
	dataLen              = 2
	runtimeManagerCapLen = 5
)

// userCAPHeader and userCAPData It is the data result required by the Linux bottom layer and cannot be adjusted.
type userCAPHeader struct {
	version uint32
	pid     int32
}

type userCAPData struct {
	effective   uint32
	permitted   uint32
	inheritable uint32
}

// Note that 64-bit capabilities use datap[0] and datap[1], whereas 32-bit capabilities use only datap[0].
var data [dataLen]userCAPData
var header = userCAPHeader{}

// ClearCAPs Ability to clear the current thread
// The clearing operation must be placed in the main thread, and runtime.LockOSThread()
// must be used to ensure that the main thread is the same as the current process.
func ClearCAPs() {
	err := ClearCurrentCAPs()
	sysRuntime.UnlockOSThread()
	if err != nil {
		log.GetLogger().Errorf("failed to clear caps err %s", err)
		log.GetLogger().Sync()
		os.Exit(0)
	}
}

// ClearCurrentCAPs Clears the cap of the current process.
func ClearCurrentCAPs() error {
	err := getCAP(&header, nil)
	header.pid = int32(0)
	if err != nil {
		log.GetLogger().Errorf("failed to get CAP err %s", err.Error())
		return err
	}
	runtimeManagerCap := [runtimeManagerCapLen]int32{capDacOverride, capFowner, capKill, capSetGid, capSetUid}
	for _, cap := range runtimeManagerCap {
		data[0].permitted |= uint32(1) << cap
		data[0].effective |= uint32(1) << cap
	}
	data[0].inheritable = 0
	err = setCAP(&header, &data[0])
	if err != nil {
		log.GetLogger().Errorf("failed to set CAP err %s, pid %d, version %d", err.Error(),
			header.pid, header.version)
		return err
	}
	return nil
}

func getCAP(header *userCAPHeader, data *userCAPData) error {
	_, _, e := syscall.Syscall(syscall.SYS_CAPGET, uintptr(unsafe.Pointer(header)),
		uintptr(unsafe.Pointer(data)), 0)
	if e != 0 {
		return e
	}
	return nil
}

func setCAP(header *userCAPHeader, data *userCAPData) error {
	_, _, e := syscall.Syscall(syscall.SYS_CAPSET, uintptr(unsafe.Pointer(header)),
		uintptr(unsafe.Pointer(data)), 0)
	if e != 0 {
		return e
	}
	return nil
}
