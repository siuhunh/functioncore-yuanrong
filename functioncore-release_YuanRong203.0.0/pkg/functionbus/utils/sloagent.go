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

package utils

import (
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	defaultRequestSLOAgentTimeout = 5 * time.Second
)

var (
	sloHTTPClient *http.Client
	sloOnce       sync.Once
)

// GetSLOAgentClient -
func GetSLOAgentClient() *http.Client {
	sloOnce.Do(func() {
		tr := &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   dialTimeout,
				KeepAlive: connKeepAlive,
			}).DialContext,
			MaxIdleConns:    maxIdleConns,
			IdleConnTimeout: idleConnTimeout,
		}
		sloHTTPClient = &http.Client{
			Timeout:   defaultRequestSLOAgentTimeout,
			Transport: tr,
		}
	})
	return sloHTTPClient
}
