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

// Package client implements StateClient for functionstate
package client

// IStateClient state client interface
type IStateClient interface {
	SetKeyWithTTL(key, value string, ttl int) error
	ResetExpiredTTL(key string, ttlInt int) error
	SaveState(id string, state string, option *SaveOption) error
	DeleteState(id string) error
	RestoreState(id string) (string, error)
	Scan(pattern string) ([]string, error)
	SAdd(key string, value string) (int, error)
	SRem(key string, value string) (int, error)
	SMembers(key string) ([]string, error)
	SCard(key string) (int, error)
	SIsMember(key string, value string) (bool, error)
	HSetNX(key, filed, value string) (bool, error)
	HDel(key string, fields ...string) (int, error)
	HLen(key string) (int, error)
	HGetAll(key string) (map[string]string, error)
	HExists(key, filed string) (bool, error)
	Exists(key string) (bool, error)
	Transaction(doArgs ...DoArg) error
}
