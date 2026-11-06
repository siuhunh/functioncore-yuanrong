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
	"context"
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/engine"
	mockutils "functioncore/test/utils"
)

func TestNewEvictionManager(t *testing.T) {
	type args struct {
		cli       engine.ContainerClient
		container engine.ContainerInfo
		mountPath []string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "successful case1",
			args: args{
				cli: &engine.DockerClient{},
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
					gomonkey.ApplyFunc(NewStorageMonitor,
						func(cli engine.ContainerClient, rootFs, containersID string, mountPath []string) *StorageMonitor {
							return &StorageMonitor{}
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if mgr := createEvictionManager(tt.args.cli, tt.args.container, tt.args.mountPath); mgr == nil {
				t.Errorf("EvictionManager is nil")
			}
			patches.ResetAll()
		})
	}
}

func TestMonitorDiskOfCustomImage(t *testing.T) {
	type args struct {
		containerID          string
		mountPath            []string
		isUserDatasExpansion bool
		stopCh               chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "failed case1",
			args: args{
				containerID:          "123",
				isUserDatasExpansion: false,
				stopCh:               make(chan struct{}),
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, errors.New("NewClientWithOpts failed")
						}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "failed case2",
			args: args{
				containerID:          "123",
				isUserDatasExpansion: false,
				stopCh:               make(chan struct{}),
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&engine.DockerClient{}), "Info", func(_ *engine.DockerClient, ctx context.Context,
						containerID string) (engine.ContainerInfo, error) {
						return engine.ContainerInfo{}, errors.New("test")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "successful case1 512m",
			args: args{
				containerID:          "123",
				isUserDatasExpansion: false,
				stopCh:               make(chan struct{}),
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&engine.DockerClient{}), "Info", func(_ *engine.DockerClient, ctx context.Context,
						containerID string) (engine.ContainerInfo, error) {
						return engine.ContainerInfo{}, nil
					}),
					gomonkey.ApplyFunc(NewStorageMonitor,
						func(cli engine.ContainerClient, rootFs, containersID string, mountPath []string) *StorageMonitor {
							return &StorageMonitor{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&EvictionManager{}), "Run", func(mgr *EvictionManager, container engine.ContainerInfo, stop <-chan struct{}) {
						return
					}),
				})
				return patches
			},
			wantErr: false,
		},

		{
			name: "successful case1 10g",
			args: args{
				containerID:          "123",
				isUserDatasExpansion: true,
				stopCh:               make(chan struct{}),
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(engine.NewContainerClient,
						func() (engine.ContainerClient, error) {
							return &engine.DockerClient{}, nil
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&engine.DockerClient{}), "Info", func(_ *engine.DockerClient, ctx context.Context,
						containerID string) (engine.ContainerInfo, error) {
						return engine.ContainerInfo{}, nil
					}),
					gomonkey.ApplyFunc(NewStorageMonitor,
						func(cli engine.ContainerClient, rootFs, containersID string, mountPath []string) *StorageMonitor {
							return &StorageMonitor{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&EvictionManager{}), "Run", func(mgr *EvictionManager, container engine.ContainerInfo, stop <-chan struct{}) {
						return
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
			if _, err := MonitorDiskOfCustomImage(tt.args.containerID, tt.args.mountPath, tt.args.isUserDatasExpansion, tt.args.stopCh, nil); (err != nil) != tt.wantErr {
				t.Errorf("MonitorDiskOfCustomImage() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func TestEvictionManager_RUN(t *testing.T) {
	type args struct {
		container engine.ContainerInfo
		stopChan  chan struct{}
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
	}{
		{
			name: "failed case1",
			args: args{
				container: engine.ContainerInfo{
					ID:            "123",
					ImageName:     "test",
					ContainerName: "test",
					RootFsPath:    "/tmp/test",
				},
				stopChan: nil,
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(NewStorageMonitor,
						func(cli engine.ContainerClient, rootFs, containersID string, mountPath []string) *StorageMonitor {
							return &StorageMonitor{}
						}),
				})
				return patches
			},
		},
		{
			name: "failed case2",
			args: args{
				container: engine.ContainerInfo{
					ID:            "123",
					ImageName:     "test",
					ContainerName: "test",
					RootFsPath:    "/tmp/test",
				},
				stopChan: make(chan struct{}),
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(NewStorageMonitor,
						func(cli engine.ContainerClient, rootFs, containersID string, mountPath []string) *StorageMonitor {
							return &StorageMonitor{}
						}),
				})
				return patches
			},
		},
	}
	container := engine.ContainerInfo{
		ID:            "123",
		ImageName:     "test",
		ContainerName: "test",
		RootFsPath:    "/tmp/test",
	}
	mgr := createEvictionManager(&engine.DockerClient{}, container, []string{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if tt.args.stopChan != nil {
				close(tt.args.stopChan)
			}
			mgr.Run(tt.args.container, tt.args.stopChan)
			patches.ResetAll()
		})
	}
}

func TestEvictionManager_StorageEviction(t *testing.T) {
	type args struct {
		container engine.ContainerInfo
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantFlag    bool
	}{
		{
			name: "case1",
			args: args{
				container: engine.ContainerInfo{
					ID:            "123",
					ImageName:     "test",
					ContainerName: "test",
					RootFsPath:    "/test123",
				},
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(NewCustomImageMonitor, func(rootDir string, opts ...Opt) (*Monitor, error) {
						return &Monitor{}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&Monitor{}), "FirstSize", func(_ *Monitor) int64 {
						return 1024
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&Monitor{}), "Size", func(_ *Monitor) int64 {
						return 512 << 22
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&engine.DockerClient{}), "Kill", func(_ *engine.DockerClient, ctx context.Context, containerID string) error {
						return nil
					}),
				})
				return patches
			},
			wantFlag: true,
		},
	}

	dir, err := ioutil.TempDir("", "*")
	if err != nil {
		t.Errorf("failed to TempDir, err: %s", err.Error())
	}
	err = os.Mkdir(filepath.Join(dir, "123"), os.ModePerm)
	if err != nil {
		t.Errorf("failed to Mkdir, err: %s", err.Error())
	}
	container := engine.ContainerInfo{
		ID:            "123",
		ImageName:     "test",
		ContainerName: "test",
		RootFsPath:    dir,
	}
	mgr := createEvictionManager(&engine.DockerClient{}, container, []string{})
	mgr.getDiskLimitSize(false)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if mgr.storageEviction(tt.args.container) != tt.wantFlag {
				t.Errorf("storageEviction() wantFlag %v", tt.wantFlag)
			}
			patches.ResetAll()
		})
	}
}

func TestEvictionManager_evict(t *testing.T) {
	type args struct {
		container engine.ContainerInfo
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantFlag    bool
	}{
		{
			name: "failed case1",
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
					gomonkey.ApplyMethod(reflect.TypeOf(&engine.DockerClient{}), "Kill", func(_ *engine.DockerClient, ctx context.Context,
						containerID string) error {
						return nil
					}),
				})
				return patches
			},
			wantFlag: true,
		},
		{
			name: "failed case2",
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
					gomonkey.ApplyMethod(reflect.TypeOf(&engine.DockerClient{}), "Kill", func(_ *engine.DockerClient, ctx context.Context, containerID string) error {
						return errors.New("test")
					}),
				})
				return patches
			},
			wantFlag: false,
		},
	}
	container := engine.ContainerInfo{
		ID:            "123",
		ImageName:     "test",
		ContainerName: "test",
		RootFsPath:    "/tmp/test",
	}
	mgr := createEvictionManager(&engine.DockerClient{}, container, []string{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if flag := mgr.evict(tt.args.container); flag != tt.wantFlag {
				t.Errorf("NewEvictionManager(), wantFlag %v", tt.wantFlag)
			}
			patches.ResetAll()
		})
	}
}

func TestLogContainer(t *testing.T) {
	convey.Convey("test LogContainer", t, func() {
		container := engine.ContainerInfo{
			ID:            "123",
			ImageName:     "test",
			ContainerName: "test",
			RootFsPath:    "/tmp/test",
		}
		str := logContainer(container)
		convey.So(str, convey.ShouldEqual, "[id: 123, image: test, name: test]")
	})
}
