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
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/logger/log"
)

type storageWatcher struct {
	monitor *Monitor
}

// StorageMonitor is an struct which can look up the stats of containers.
type StorageMonitor struct {
	containerCli engine.ContainerClient
	watchers     map[string]*storageWatcher
	mountPath    []string
}

// NewStorageMonitor creates a StorageMonitor
func NewStorageMonitor(cli engine.ContainerClient, rootFs, containersID string,
	mountPath []string) *StorageMonitor {
	dm := &StorageMonitor{
		containerCli: cli,
		watchers:     make(map[string]*storageWatcher),
		mountPath:    mountPath,
	}
	if _, exist := dm.watchers[containersID]; !exist {
		dm.startWatcher(containersID, rootFs)
	}
	for id, watcher := range dm.watchers {
		if containersID != id {
			watcher.monitor.Close()
			delete(dm.watchers, id)
			log.GetLogger().Infof("stop to monitor container id: %s", id)
		}
	}
	return dm
}

// StatsFunc -
func (dm *StorageMonitor) StatsFunc(container engine.ContainerInfo) (ContainerStats, bool) {
	watcher, ok := dm.watchers[container.ID]
	if !ok {
		return ContainerStats{}, false
	}
	stats := ContainerStats{}
	size := watcher.monitor.Size()
	if size != 0 {
		// We consider the first non-zero 'size' to be the container's rootFs size. This way is not very
		// accurate because the writeable layer may have been changed BEFORE the first 'size' is returned.
		// However, it is much more efficient than asking docker (which rebuilds the whole rootFs layer and
		// sums every file).
		fistSize := watcher.monitor.FirstSize()

		sizeRw := size - fistSize
		stats.SizeRw = &sizeRw
		stats.SizeRootFs = &fistSize
		log.GetLogger().Debugf("StorageMonitor current sizeRw %v, sizeRootFs %v", sizeRw, fistSize)
	}
	return stats, true
}

func (dm *StorageMonitor) startWatcher(containerID, watchPath string) {
	monitor, err := NewCustomImageMonitor(watchPath, Ignore(dm.mountPath...))
	if err != nil {
		log.GetLogger().Warnf("failed to monitor container %s, path: %s, err: %s",
			containerID, watchPath, err.Error())
		return
	}
	dm.watchers[containerID] = &storageWatcher{monitor: monitor}
	log.GetLogger().Infof("start to monitor container %s, watchPath: %s, mountPath: %v",
		containerID, watchPath, dm.mountPath)
}
