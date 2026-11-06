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

// Package monitor provide memory and file monitor
package monitor

import (
	"errors"

	"functioncore/pkg/common/logger/log"
)

// OpType describes file operation type
type OpType uint32

const (
	// Create op type
	Create OpType = 1 << iota
	// Write op type
	Write
	// Remove op type
	Remove
	// Rename op type
	Rename
	// Chmod op type
	Chmod
)

var (
	creator Creator = createDefaultFileWatcher
)

// FileChangedCallback describes callback function, when file changed, callback function will be invoked
type FileChangedCallback func(filename string, opType OpType)

// Creator describes watcher create function
type Creator func(stopCh <-chan struct{}) (FileWatcher, error)

// FileWatcher describes interface of general FileWatcher
type FileWatcher interface {
	Start()
	RegisterCallback(filename string, callback FileChangedCallback)
}

// SetCreator set file watcher creator func, if not set, use createDefaultFileWatcher
func SetCreator(newCreator Creator) {
	creator = newCreator
}

// CreateFileWatcher create a file watcher
// notice: one FileWatcher can only watcher one file
func CreateFileWatcher(stopCh <-chan struct{}) (FileWatcher, error) {
	watcher, err := creator(stopCh)
	if err != nil {
		log.GetLogger().Errorf("create watcher failed %s", err.Error())
		return nil, err
	}
	if watcher == nil {
		log.GetLogger().Errorf("watcher is nil")
		return nil, errors.New("watcher is nil")
	}
	go watcher.Start()
	return watcher, nil
}
