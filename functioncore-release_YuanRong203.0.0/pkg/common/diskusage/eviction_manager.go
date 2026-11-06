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

// Package diskusage is the entry of storage eviction manager
package diskusage

import (
	"context"
	"fmt"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/logger/log"
)

const (
	syncInterval = 100 * time.Millisecond
)

// EvictionManager controls the eviction of containers
type EvictionManager struct {
	containerCli   engine.ContainerClient
	storageMonitor *StorageMonitor
	diskLimit      int64
	LimitFlag      bool
	callback       func(int64)
}

// ContainerStats contains useful informations of a container
type ContainerStats struct {
	SizeRw     *int64
	SizeRootFs *int64
}

func createEvictionManager(cli engine.ContainerClient, container engine.ContainerInfo,
	mountPath []string) *EvictionManager {
	storageMonitor := NewStorageMonitor(cli, container.RootFsPath, container.ID, mountPath)
	return &EvictionManager{
		containerCli:   cli,
		storageMonitor: storageMonitor,
	}
}

// Run starts the eviction manager
func (m *EvictionManager) Run(info engine.ContainerInfo, stop <-chan struct{}) {
	if stop == nil {
		log.GetLogger().Warnf("empty stop channel")
		return
	}
	for {
		select {
		case _, ok := <-stop:
			if !ok {
				log.GetLogger().Warnf("stop check custom image disk, stop channel is closed")
				return
			}

		default:
			evictFlag := m.storageEviction(info)
			if evictFlag {
				log.GetLogger().Error("current runtime container is killed")
				return
			}
			time.Sleep(syncInterval)
		}
	}
}

func (m *EvictionManager) storageEviction(container engine.ContainerInfo) bool {
	stats, found := m.storageMonitor.StatsFunc(container)
	if !found {
		log.GetLogger().Warnf("cannot find stats for container %s", logContainer(container))
		return false
	}
	if stats.SizeRw != nil && stats.SizeRootFs != nil {
		log.GetLogger().Debugf("container %s, stats.SizeRw: %v, stats.SizeRootFs %v",
			logContainer(container), *stats.SizeRw, *stats.SizeRootFs)
	}
	evictFlag := m.sizeRwEviction(container, stats)
	return evictFlag
}

func (m *EvictionManager) sizeRwEviction(container engine.ContainerInfo, stats ContainerStats) bool {
	if stats.SizeRw != nil && m.callback != nil {
		m.callback(*stats.SizeRw)
	}
	if stats.SizeRw != nil && *stats.SizeRw > m.diskLimit {
		if m.evict(container) {
			log.GetLogger().Errorf("current runtime container [id: %s] [image: %s] exceeds the disk threshold, "+
				"LimitFlag: %v, sizeRw %v", container.ID, container.ImageName, m.LimitFlag, *stats.SizeRw)
			return true
		}
	}
	m.LimitFlag = false
	return false
}

func (m *EvictionManager) evict(container engine.ContainerInfo) bool {
	m.LimitFlag = true
	if err := m.containerCli.Kill(context.TODO(), container.ID); err != nil {
		log.GetLogger().Errorf("failed to kill container %s", logContainer(container))
		return false
	}
	log.GetLogger().Infof("succeed to kill container %s", logContainer(container))
	return true
}

func (m *EvictionManager) getDiskLimitSize(isUserDatasExpansion bool) {
	if isUserDatasExpansion {
		m.diskLimit = constants.ExpansionUserDataMaxSize
		return
	}
	m.diskLimit = constants.DefaultUserDataMaxSize
	return
}

// MonitorDiskOfCustomImage -
func MonitorDiskOfCustomImage(containerID string, mountPath []string, isUserDatasExpansion bool,
	stopCh chan struct{}, callback func(int64)) (*EvictionManager, error) {
	log.GetLogger().Infof("succeed to start to monitor custom image disk, mountPath %v", mountPath)
	cli, err := engine.NewContainerClient()
	if err != nil {
		log.GetLogger().Errorf("failed to create docker client: %s", err.Error())
		return nil, fmt.Errorf("failed to create docker client: %s", err.Error())
	}

	containerInfo, err := cli.Info(context.TODO(), containerID)
	if err != nil {
		log.GetLogger().Errorf("failed to get container info, err: %s", err.Error())
		return nil, fmt.Errorf("failed to get container info: %s", err.Error())
	}

	e := createEvictionManager(cli, containerInfo, mountPath)
	e.callback = callback
	e.getDiskLimitSize(isUserDatasExpansion)
	go e.Run(containerInfo, stopCh)
	return e, nil
}

func logContainer(container engine.ContainerInfo) string {
	return fmt.Sprintf("[id: %s, image: %s, name: %s]", container.ID, container.ImageName, container.ContainerName)
}
