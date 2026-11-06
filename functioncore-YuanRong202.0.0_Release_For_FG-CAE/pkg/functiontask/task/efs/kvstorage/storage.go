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

package kvstorage

import "functioncore/pkg/functiontask/task/common"

// Storage represents a key-value storage.
type Storage interface {
	Set(key string, value string, expireTime int) common.TaskError
	Get(key string) (string, common.TaskError)
	Delete(key string) common.TaskError
	Exists(key string) (bool, common.TaskError)
	Scan(pattern string) (map[string]string, common.TaskError)
	GetNumsByServiceID(pattern string) (int, common.TaskError)
}
