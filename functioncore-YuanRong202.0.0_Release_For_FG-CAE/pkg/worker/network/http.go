/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package network

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/worker/config"
)

const (
	maxIdleConnDuration  = 10 * time.Second
	maxConnsPerWorker    = 1024
	readBufferSize       = 8 * 1024
	maxClientConcurrency = 10000
	dftTimeout           = defaultTimeout * time.Second
)

var (
	tcpDialer  = fasthttp.TCPDialer{Concurrency: maxClientConcurrency}
	httpClient = struct {
		sync.Once
		httpClient *fasthttp.Client
	}{}
)

// HTTPHealthChecker provides the ping method.
type HTTPHealthChecker struct {
	endpoint      string
	subMetaDigest string
	httpClient    *fasthttp.Client
}

// NewHTTPHealthChecker Create a ping processor
func NewHTTPHealthChecker(ip string, subMetaDigest string, ops ...Option) *HTTPHealthChecker {
	p := &HTTPHealthChecker{
		endpoint:      ip + ":28920",
		subMetaDigest: subMetaDigest,
		httpClient:    getFastHTTPClient(),
	}
	for _, op := range ops {
		op(p)
	}
	return p
}

func (p *HTTPHealthChecker) doCheckWithRetry(retryTimes int) error {
	configuration, _ := config.GetConfig()
	if configuration == nil {
		return errors.New("get worker config failed while http do health check")
	}
	var url string
	if configuration.HTTPSEnable {
		url = fmt.Sprintf("https://%s/v2/fgs/healthcheck", p.endpoint)
	} else {
		url = fmt.Sprintf("http://%s/v2/fgs/healthcheck", p.endpoint)
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	p.addAuthorizationHeader(req)
	var err error
	for i := 0; i < retryTimes; i++ {
		start := time.Now()
		err = p.httpClient.DoTimeout(req, resp, dftTimeout)
		if err == nil && resp.StatusCode() == http.StatusOK {
			log.GetLogger().Debugf("http check connectivity %s cost： %v", p.endpoint, time.Since(start))
			break
		}
		if err != nil {
			log.GetLogger().Warnf("failed do http health check to %s for the %d time caused by %s, cost: %v",
				p.endpoint, i+1, err.Error(), time.Since(start))
		} else {
			log.GetLogger().Warnf("failed do http health check to %s for the %d time, status code %d, cost: %v",
				p.endpoint, i+1, resp.StatusCode(), time.Since(start))
		}
		// last retry failure will not sleep
		if i < defaultHealthCheckRetry-1 {
			time.Sleep(defaultRetryDelay * time.Second)
		}
		err = errors.New("failed do http health check to " + p.endpoint)
	}
	return err
}

func (p *HTTPHealthChecker) addAuthorizationHeader(proxyReq *fasthttp.Request) {
	proxyReq.Header.Set(constants.HeaderSubMetaDigest, p.subMetaDigest)
	proxyReq.Header.SetMethod(fasthttp.MethodGet)
}

// DoHealthCheck do health check
func (p *HTTPHealthChecker) DoHealthCheck(ctx context.Context) error {
	ticker := time.NewTicker(defaultRetryTime)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.GetLogger().Warnf("context is canceled while do http health check %s", p.endpoint)
			return nil
		case <-ticker.C:
			err := p.doCheckWithRetry(defaultHealthCheckRetry)
			if err != nil {
				log.GetLogger().Errorf("failed do http health check %s", p.endpoint)
				return err
			}
		}
	}
}

// getFastHTTPClient create fast http client
func getFastHTTPClient() *fasthttp.Client {
	httpClient.Do(func() {
		configuration, _ := config.GetConfig()
		var tlsConfig *tls.Config
		if configuration.ServerConfig.HTTPSConfig.HTTPSEnable {
			newCfg := *commontls.GetClientTLSConfig()
			newCfg.NextProtos = []string{"http/1.1"}
			tlsConfig = &newCfg
		}
		httpClient.httpClient = &fasthttp.Client{
			MaxIdemponentCallAttempts: 1,
			MaxIdleConnDuration:       maxIdleConnDuration,
			MaxConnsPerHost:           maxConnsPerWorker,
			ReadTimeout:               dftTimeout,
			WriteTimeout:              dftTimeout,
			ReadBufferSize:            readBufferSize,
			TLSConfig:                 tlsConfig,
			Dial: func(addr string) (net.Conn, error) {
				return tcpDialer.DialTimeout(addr, dftTimeout)
			},
		}
	})
	return httpClient.httpClient
}
