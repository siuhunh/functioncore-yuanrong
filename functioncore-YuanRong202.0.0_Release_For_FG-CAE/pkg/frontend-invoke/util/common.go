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

// Package util Get Function Info
package util

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/frontend-invoke/constant"
)

const (
	// NormalWorkerIns is worker instance type
	NormalWorkerIns = "NormalIns"
	// ProxyIns is proxy instance type
	ProxyIns = "ProxyIns"
	// StateIns is state route for stateful function
	StateIns  = "StateIns"
	megabytes = 1024 * 1024
	// AppID name of module
	AppID = "frontendinvoke"
	// DialTimeOut set timeout limit
	DialTimeOut = 10 * time.Second
	// HeartbeatDialTimeOut heartbeat dial timeout
	HeartbeatDialTimeOut = 2 * time.Second
	defaultWriteTimeout  = time.Duration(30) * time.Second
	// MaxConnsPerWorker limit the max connection
	MaxConnsPerWorker = 10240
	// MaxClientConcurrency is the max concurrency of fast http client
	MaxClientConcurrency = 10000

	// WorkerInstanceMaxIdleConnDuration define max connction time
	WorkerInstanceMaxIdleConnDuration = 10 * time.Second
	// IPPortLen is the IP Port convert to Ip and Port Valid Len
	ipPortConvLen = 2
	// instanceKeyValidLEn is the Instance key valid len
	instanceKeyValidLEn = 2
	// WorkerInstanceAZSep -
	WorkerInstanceAZSep = "-#-"

	// wantedLenOfSplit the wanted length of the split of the instanceID
	wantedLenOfSplit       = 2
	funcKeyWithResSliceNum = 4
)

var (
	tcpDialer = fasthttp.TCPDialer{Concurrency: MaxClientConcurrency}
)

// InitETCDClient -
func InitETCDClient(stopCh <-chan struct{}) error {
	if constant.FConfig.CAEMetaETCD != nil && len(constant.FConfig.CAEMetaETCD.ServerList) == 0 {
		log.GetLogger().Infof("empty cae meta etcd server list, skip connect")
		constant.FConfig.CAEMetaETCD = nil
	}
	if constant.FConfig.MetaEtcdProxy != nil && len(constant.FConfig.MetaEtcdProxy.ServerList) == 0 {
		log.GetLogger().Infof("empty meta etcd proxy server list, skip connect")
		constant.FConfig.MetaEtcdProxy = nil
	}
	if constant.FConfig.CAEMetaEtcdProxy != nil && len(constant.FConfig.CAEMetaEtcdProxy.ServerList) == 0 {
		log.GetLogger().Infof("empty cae meta etcd proxy server list, skip connect")
		constant.FConfig.CAEMetaEtcdProxy = nil
	}
	if os.Getenv("MICROSERVICE_CLUSTER_ENABLE") == "true" {
		return etcd3.InitParam().
			WithStopCh(stopCh).
			WithAlarmSwitch().
			WithMetaEtcdConfig(constant.FConfig.Etcd).
			WithRouterEtcdConfig(constant.FConfig.RouterETCD).
			WithCAEMetaEtcdConfig(constant.FConfig.CAEMetaETCD).
			WithCaeRouterEtcdConfig(constant.FConfig.StateETCD).
			InitClient()
	}
	return etcd3.InitParam().
		WithStopCh(stopCh).
		WithAlarmSwitch().
		WithMetaEtcdConfig(constant.FConfig.Etcd).
		WithRouterEtcdConfig(constant.FConfig.RouterETCD).
		WithCAEMetaEtcdConfig(constant.FConfig.CAEMetaETCD).
		WithMetaEtcdProxyConfig(constant.FConfig.MetaEtcdProxy).
		WithCAEMetaEtcdProxyConfig(constant.FConfig.CAEMetaEtcdProxy).
		InitClient()
}

// WorkerInstance  define   worker instance
type WorkerInstance struct {
	IP         string
	Port       string
	InstanceID string
}

// IPPortConv convert string to ip and port
func IPPortConv(ipPort string) (*WorkerInstance, error) {
	strS := strings.Split(ipPort, ":")
	if len(strS) != ipPortConvLen {
		err := fmt.Errorf("wrong ip port : %s", ipPort)
		return nil, err
	}
	infos := &WorkerInstance{
		IP:   strS[0],
		Port: strS[1],
	}
	return infos, nil
}

// BuildWorkerInstanceKey format: NodeIP:NodePort@InstanceID
func BuildWorkerInstanceKey(nodeIP, nodePort, instanceID string) string {
	return fmt.Sprintf("%s:%s@%s", nodeIP, nodePort, instanceID)
}

// WorkerInstanceKeyConv -
func WorkerInstanceKeyConv(key string) (*WorkerInstance, error) {
	// format: NodeIP:NodePort@InstanceID
	// expected elements of strS: NodeIP:NodePort, InstanceID (two elements only)
	strS := strings.Split(key, "@")
	if len(strS) != instanceKeyValidLEn {
		err := fmt.Errorf("wrong key : %s", key)
		return nil, err
	}

	infos, err := IPPortConv(strS[0])
	if err == nil {
		infos.InstanceID = strS[1]
	}
	return infos, err
}

// GetProxyInsKey extracts a BusProxy instance key from a URN
// "sn:cn:yrk:0:function:FunctionA:$latest"
func GetProxyInsKey(urn string) (string, error) {
	parsedURN, err := urnutils.GetFuncInfoWithVersion(urn)
	if err != nil {
		return "", err
	}
	key := parsedURN.Name + urnutils.URNSep + parsedURN.Version
	return key, nil
}

var maxRequestBodySizeOnce sync.Once

// PermittedRequest is function check whether the body of the function is greater than the configured value.
func PermittedRequest(r *http.Request) snerror.SNError {
	maxRequestBodySizeOnce.Do(func() {
		constant.FConfig.HTTPConfig.MaxRequestBodySize *= megabytes
	})
	if r.ContentLength > int64(constant.FConfig.HTTPConfig.MaxRequestBodySize) {
		log.GetLogger().Errorf("the size of request body is beyond maximum:%d, body size:%d",
			constant.FConfig.HTTPConfig.MaxRequestBodySize, r.ContentLength)

		err := snerror.NewWithFmtMsg(snerror.MaxRequestBodySizeErr, snerror.ErrText(snerror.MaxRequestBodySizeErr),
			constant.FConfig.HTTPConfig.MaxRequestBodySize, r.ContentLength)
		return err
	}
	return nil
}

var (
	globalClient = struct {
		// common fast http client
		c *fasthttp.Client
		sync.Once
	}{}
	heartbeatClient = struct {
		// heartbeat fast http client
		hc *fasthttp.Client
		sync.Once
	}{}
)

// GetGlobalClient returns global client of fasthttp
func GetGlobalClient() *fasthttp.Client {
	globalClient.Do(func() {
		globalClient.c = NewFastHTTPClient(DialTimeOut)
	})
	return globalClient.c
}

// GetHeartbeatClient return heart beat client
func GetHeartbeatClient() *fasthttp.Client {
	heartbeatClient.Do(func() {
		heartbeatClient.hc = NewFastHTTPClient(HeartbeatDialTimeOut)
	})
	return heartbeatClient.hc
}

// ExtractAz InstanceID: AZ-#-intanceId
func ExtractAz(instanceID string) string {
	strs := strings.Split(instanceID, WorkerInstanceAZSep)

	if len(strs) != wantedLenOfSplit {
		return ""
	}

	return strs[0]
}

// GetReadBufferSize get the default read buffer size for server and client
func GetReadBufferSize() int {
	return constant.DefaultGraphReadBufferSize
}

// NewFastHTTPClient create fasthttp client
func NewFastHTTPClient(dialTimeout time.Duration) *fasthttp.Client {
	var tlsConfig *tls.Config
	if constant.FConfig.HTTPSConfig.HTTPSEnable {
		newCfg := *commontls.GetClientTLSConfig()
		newCfg.NextProtos = []string{"http/1.1"}
		tlsConfig = &newCfg
	}
	return &fasthttp.Client{
		MaxIdemponentCallAttempts: 1,
		MaxIdleConnDuration:       WorkerInstanceMaxIdleConnDuration,
		MaxConnsPerHost:           MaxConnsPerWorker,
		ReadTimeout:               time.Duration(constant.FConfig.HTTPConfig.WorkerInstanceReadTimeOut) * time.Second,
		WriteTimeout:              defaultWriteTimeout,
		ReadBufferSize:            GetReadBufferSize(),
		TLSConfig:                 tlsConfig,
		Dial: func(addr string) (net.Conn, error) {
			return tcpDialer.DialTimeout(addr, dialTimeout)
		},
	}
}

// ParseFuncKeyWithRes parse tenantID and funcKey from funcKeyWithRes
func ParseFuncKeyWithRes(funcKeyWithRes string) (string, string) {
	funcKeyWithResSlice := strings.Split(funcKeyWithRes, "/")
	var tenantID, funcKey string
	if len(funcKeyWithResSlice) == funcKeyWithResSliceNum {
		tenantID = funcKeyWithResSlice[0]
		funcKey = fmt.Sprintf("%s/%s/%s", funcKeyWithResSlice[0], funcKeyWithResSlice[1], funcKeyWithResSlice[2])
	}
	return tenantID, funcKey
}
