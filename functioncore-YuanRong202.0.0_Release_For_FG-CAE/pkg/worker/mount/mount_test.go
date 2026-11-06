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

package mount

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/utils"
)

func TestMount(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(net.DialTimeout, func(network, address string, timeout time.Duration) (net.Conn, error) {
			return nil, nil
		}),
		ApplyFunc(net.LookupHost, func(host string) (addrs []string, err error) {
			assert.Equal(t, "myhost", host)
			return []string{"127.0.0.1"}, nil
		}),
		ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return nil
		}),
		ApplyFunc(syscall.Mount, func(source string, target string, fstype string, flags uintptr, data string) error {
			return nil
		}),
	}
	defer func() {
		for _, p := range patches {
			p.Reset()
		}
	}()

	err := VolumesForFunction(types.FuncMountConfig{
		FuncMountUser: types.FuncMountUser{UserID: 1000, GroupID: 1000},
		FuncMounts: []types.FuncMount{
			{
				MountType:      "ecs",
				MountResource:  "abc",
				MountSharePath: "myhost:path",
				LocalMountPath: "/local-path",
				Status:         "some-status",
			},
		},
	})
	assert.NoError(t, err)
	err = VolumesForFunction(types.FuncMountConfig{})
	assert.NoError(t, err)
	patch := ApplyFunc(mountVolumeForFunction, func(mount types.FuncMount) error {
		return errors.New("failed to mount Volume for function")
	})
	err = VolumesForFunction(types.FuncMountConfig{
		FuncMountUser: types.FuncMountUser{UserID: 1000, GroupID: 1000},
		FuncMounts: []types.FuncMount{
			{
				MountType:      "ecs",
				MountResource:  "abc",
				MountSharePath: "myhost:path",
				LocalMountPath: "/local-path",
				Status:         "some-status",
			},
		},
	})
	assert.Equal(t, errors.New("failed to mount Volume for function"), err)
	patch.Reset()
}

func TestUmount(t *testing.T) {
	patch1 := ApplyFunc(syscall.Unmount, func(target string, flags int) error {
		return nil
	})
	err := UmountVolumesForFunction(types.FuncMountConfig{
		FuncMountUser: types.FuncMountUser{UserID: 1000, GroupID: 1000},
		FuncMounts: []types.FuncMount{
			{
				MountType:      "ecs",
				MountResource:  "abc",
				MountSharePath: "myhost:path",
				LocalMountPath: "/local-path",
				Status:         "some-status",
			},
		},
	})
	assert.NoError(t, err)
	err = UmountVolumesForFunction(types.FuncMountConfig{})
	assert.NoError(t, err)
	patch1.Reset()

	patch2 := ApplyFunc(syscall.Unmount, func(target string, flags int) error {
		return errors.New("umount failed")
	})
	err = UmountVolumesForFunction(types.FuncMountConfig{
		FuncMountUser: types.FuncMountUser{UserID: 1000, GroupID: 1000},
		FuncMounts: []types.FuncMount{
			{
				MountType:      "ecs",
				MountResource:  "abc",
				MountSharePath: "myhost:path",
				LocalMountPath: "/local-path",
				Status:         "some-status",
			},
		},
	})
	assert.NoError(t, err)
	patch2.Reset()
}

func Test_CheckECSMountStatus(t *testing.T) {
	patch := ApplyFunc(net.DialTimeout, func(network, address string, timeout time.Duration) (net.Conn, error) {
		return nil, errors.New("failed to dial")
	})
	defer patch.Reset()
	err := checkECSMountStatus("host:path")
	assert.Equal(t, snerror.RuntimeMountErrorCode, err.Code())
}

func Test_ExecMountCommand(t *testing.T) {
	err := execMountCommand("host", "", "/local-path")
	assert.Equal(t, snerror.RuntimeMountErrorCode, err.Code())

	patch := ApplyFunc(net.LookupHost, func(host string) (addrs []string, err error) {
		return nil, errors.New("failed to lookupHost")
	})

	err = execMountCommand("host:path", "", "/local-path")
	assert.Equal(t, snerror.RuntimeMountErrorCode, err.Code())
	patch.Reset()

	patch = ApplyFunc(net.LookupHost, func(host string) (addrs []string, err error) {
		return []string{"127.0.0.1"}, nil
	})
	err = execMountCommand("host:path", "", "/local-path")
	assert.Equal(t, snerror.RuntimeMountErrorCode, err.Code())
	patch.Reset()

	patches := []*Patches{
		ApplyFunc(syscall.Mount, func(source string, target string, fstype string, flags uintptr, data string) error {
			return nil
		}),
		ApplyFunc(net.LookupHost, func(host string) (addrs []string, err error) {
			return []string{"127.0.0.1"}, nil
		}),
	}
	err = execMountCommand("host:path", "", "/local-path")
	os.Remove("/local-Path")
	assert.Equal(t, nil, err)
	err = execMountCommand("host:path", typeSFS3, "/local-path")
	os.Remove("/local-Path")
	assert.Equal(t, nil, err)
	for _, p := range patches {
		p.Reset()
	}
}

func Test_MkLocalDirMountPath(t *testing.T) {
	os.MkdirAll("/localPath", fileMode)
	err := mkLocalDirMountPath("/localPath")
	assert.Equal(t, snerror.New(snerror.RuntimeMountErrorCode, "failed to mount exist system path"), err)
	os.Remove("/localPath")

	patches := ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
		return errors.New("failed to mkdir")
	})
	err = mkLocalDirMountPath("/localPath")
	assert.Equal(t, snerror.New(snerror.RuntimeMountErrorCode, "failed to create local mount path"), err)
	patches.Reset()
}

func Test_mountVolumeForFunction(t *testing.T) {
	patches := []*Patches{
		ApplyFunc(mkLocalDirMountPath, func(localMountPath string) snerror.SNError {
			return nil
		}),
		ApplyFunc(net.DialTimeout, func(network, address string, timeout time.Duration) (net.Conn, error) {
			return nil, errors.New("dial tcp 127.0.0.1:111: connect: connection refused")
		}),
	}

	mount := types.FuncMount{
		MountType:      typeECS,
		MountSharePath: "127.0.0.1:sharedata",
	}
	err := mountVolumeForFunction(mount)
	assert.True(t, utils.IsUserError(err))
	snerr := err.(snerror.SNError)
	assert.Equal(t, snerror.RuntimeMountErrorCode, snerr.Code())
	for _, p := range patches {
		p.Reset()
	}
}
