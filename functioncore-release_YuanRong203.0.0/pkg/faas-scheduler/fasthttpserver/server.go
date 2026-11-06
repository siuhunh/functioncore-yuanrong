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

// Package fasthttpserver contains fast http server logic
package fasthttpserver

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/logger/log"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

const (
	defaultReadBufferSize     = 1 * 1024
	defaultServerTimeout      = 5 * time.Second
	defaultMaxRequestBodySize = 1 * 1024 * 1024
)

// Config network server config
type Config struct {
	Service schedulerapi.Scheduler
	IP      string
	Port    string
}

// Server -
type Server struct {
	fastServer *fasthttp.Server
	config     Config
}

// NewServer -
func NewServer(config Config) (*Server, error) {
	adapter := createServerAdapter(config.Service)
	fastServer := &fasthttp.Server{
		Handler:            adapter.route,
		TLSConfig:          getTLSConfig(),
		ReadBufferSize:     defaultReadBufferSize,
		ReadTimeout:        defaultServerTimeout,
		WriteTimeout:       defaultServerTimeout,
		MaxRequestBodySize: defaultMaxRequestBodySize,
	}
	return &Server{
		fastServer: fastServer,
		config:     config,
	}, nil
}

// Start -
func (s *Server) Start(errChan chan<- error) error {
	if errChan == nil {
		return errors.New("errChan is nil")
	}
	go func() {
		err := s.start()
		if err != nil {
			log.GetLogger().Errorf("failed to start server, err %s", err.Error())
		}
		errChan <- err
	}()
	return nil
}

// Stop -
func (s *Server) Stop() {
	err := s.fastServer.Shutdown()
	if err != nil {
		log.GetLogger().Errorf("failed to shutdown http server")
		return
	}
	log.GetLogger().Infof("success to shutdown http server")
}

func (s *Server) start() error {
	addr := fmt.Sprintf("%s:%s", s.config.IP, s.config.Port)
	log.GetLogger().Infof("start to listen the http request on addr: %s", addr)
	err := s.fastServer.ListenAndServe(addr)
	if err != nil {
		log.GetLogger().Errorf("failed to listen the http request on addr %s: %s", addr, err.Error())
		return err
	}
	return nil
}

func getTLSConfig() *tls.Config {
	if config.FConfig.HTTPSConfig.HTTPSEnable == false {
		return nil
	}
	config := *commontls.GetClientTLSConfig()
	config.NextProtos = []string{"http/1.1"}
	return &config
}
