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

package diskusage

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"

	"functioncore/pkg/common/engine"
	mockutils "functioncore/test/utils"
)

func TestNewStorageMonitor(t *testing.T) {
	type args struct {
		cli         engine.ContainerClient
		rootFs      string
		containerID string
		mountPath   []string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "success",
			args: args{
				cli:         &engine.DockerClient{},
				containerID: "123",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(NewCustomImageMonitor, func(rootDir string, opts ...Opt) (*Monitor, error) {
						return &Monitor{}, nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if monitor := NewStorageMonitor(tt.args.cli, tt.args.rootFs, tt.args.containerID, tt.args.mountPath); monitor == nil {
				t.Errorf("storage monitor is nil")
			}
			patches.ResetAll()
		})
	}
}

func TestStorageMonitor_StatsFunc(t *testing.T) {
	type args struct {
		container engine.ContainerInfo
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "succeed to exec devicemapper prepare stats 1",
			args: args{
				container: engine.ContainerInfo{
					ID:            "123",
					ImageName:     "test",
					ContainerName: "test",
					RootFsPath:    "/tmp/test",
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(NewCustomImageMonitor, func(rootDir string, opts ...Opt) (*Monitor, error) {
						return &Monitor{
							cachedSize: 200,
						}, nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "succeed to exec devicemapper prepare stats 2",
			args: args{
				container: engine.ContainerInfo{
					ID:            "456",
					ImageName:     "test",
					ContainerName: "test",
					RootFsPath:    "/tmp/test",
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(NewCustomImageMonitor, func(rootDir string, opts ...Opt) (*Monitor, error) {
						return &Monitor{
							cachedSize: 100,
						}, nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			storageMonitor := NewStorageMonitor(&engine.DockerClient{}, "/tmp", "123", []string{})
			_, exist := storageMonitor.StatsFunc(tt.args.container)
			if exist != tt.wantErr {
				t.Errorf("NewStorageMonitor(), wantErr %v", tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
