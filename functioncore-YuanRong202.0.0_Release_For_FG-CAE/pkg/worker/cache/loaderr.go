/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package cache

import "sync"

var loadErr = struct {
	err error
	sync.RWMutex
}{}

// SetLoadErr set load error
func SetLoadErr(err error) {
	loadErr.Lock()
	loadErr.err = err
	loadErr.Unlock()
}

// ClearLoadErr clear load error
func ClearLoadErr() {
	loadErr.Lock()
	loadErr.err = nil
	loadErr.Unlock()
}

// GetLoadErr get load error
func GetLoadErr() error {
	loadErr.RLock()
	defer loadErr.RUnlock()
	return loadErr.err
}
