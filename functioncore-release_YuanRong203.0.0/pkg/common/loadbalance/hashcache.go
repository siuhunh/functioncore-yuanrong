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

package loadbalance

import "sync"

type hashCache struct {
	hashes sync.Map
}

func createHashCache() *hashCache {
	return &hashCache{
		hashes: sync.Map{},
	}
}

func (cache *hashCache) getHash(key string) uint32 {
	hashIf, ok := cache.hashes.Load(key)
	if ok {
		hash, ok := hashIf.(uint32)
		if ok {
			return hash
		}
		return 0
	}
	hash := getHashKeyCRC32(key)
	cache.hashes.Store(key, hash)
	return hash
}
