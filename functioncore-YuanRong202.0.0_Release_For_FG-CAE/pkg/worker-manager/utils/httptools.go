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

// Package utils gets function services from URNs
package utils

import (
	"net"
	"time"

	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/common/logger/log"
)

const (
	// MaxIdemponentCallAttempts max idemponent call times
	MaxIdemponentCallAttempts = 1
	// MaxIdleConnDuration max idle duration
	MaxIdleConnDuration = 60
	// MaxConnsPerHost max connections per host
	MaxConnsPerHost = 2048
	// MaxReadTimeout max read timeout
	MaxReadTimeout = 1200
	// MaxDialTimeout max dial timeout
	MaxDialTimeout = 10
	// KeepAlivePeriod keep alive period
	KeepAlivePeriod = 10
)

// GetFastHTTPClient get a fast http client
func GetFastHTTPClient() *fhttp.Client {
	fastHTTPClient := &fhttp.Client{}
	fastHTTPClient.MaxIdemponentCallAttempts = MaxIdemponentCallAttempts
	fastHTTPClient.MaxIdleConnDuration = MaxIdleConnDuration * time.Second
	fastHTTPClient.MaxConnsPerHost = MaxConnsPerHost
	fastHTTPClient.ReadTimeout = MaxReadTimeout * time.Second
	fastHTTPClient.Dial = fastHTTPDailFunc

	return fastHTTPClient
}

func fastHTTPDailFunc(addr string) (net.Conn, error) {
	conn, err := fhttp.DialTimeout(addr, time.Second*MaxDialTimeout)
	if err != nil {
		log.GetLogger().Errorf("failed to dial to address %v with error %s", addr, err.Error())
		return nil, err
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, nil
	}
	err = tcpConn.SetKeepAlive(true)
	if err != nil {
		log.GetLogger().Errorf("failed to set connection keepalive of address %v with error %s", addr, err.Error())
		return nil, err
	}
	err = tcpConn.SetKeepAlivePeriod(KeepAlivePeriod * time.Second)
	if err != nil {
		log.GetLogger().Errorf("failed to set connection keepalive period of address %v with error %v", addr, err)
		return nil, err
	}
	log.GetLogger().Infof("succeed to set connection to address %v", addr)
	return tcpConn, nil
}
