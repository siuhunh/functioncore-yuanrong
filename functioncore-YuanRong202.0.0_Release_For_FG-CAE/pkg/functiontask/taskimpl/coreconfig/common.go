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

package coreconfig

import (
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
)

// GetRedisPassword returns password for redis.
func GetRedisPassword(taskConfig *Config) string {
	redisPwd, err := localauth.Decrypt(taskConfig.StateServerPassword)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt StateServerPassword, err: %s", err.Error())
		return ""
	}
	return string(redisPwd)
}
