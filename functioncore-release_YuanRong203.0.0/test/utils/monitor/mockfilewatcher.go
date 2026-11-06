/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

package monitor

import "functioncore/pkg/common/monitor"

type MockFileWatcher struct {
	Callbacks map[string]monitor.FileChangedCallback
	StopCh    <-chan struct{}
	EventChan chan string
}

func (watcher *MockFileWatcher) Start() {
	for {
		select {
		case event, _ := <-watcher.EventChan:
			callback, _ := watcher.Callbacks[event]
			callback(event, monitor.Write)
		case <-watcher.StopCh:
			return
		}
	}
}

func (watcher *MockFileWatcher) RegisterCallback(filename string, callback monitor.FileChangedCallback) {
	watcher.Callbacks[filename] = callback
}
