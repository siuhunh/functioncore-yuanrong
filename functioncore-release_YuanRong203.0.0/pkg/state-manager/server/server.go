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

// Package server implement http server and http router
package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/state-manager/config"
)

const (
	readTimeout  = 10 * time.Second
	writeTimeout = 10 * time.Second
	waitTimeout  = 10 * time.Second
	serviceName  = "state-manager"
)

var (
	router     = gin.New()
	httpServer *http.Server
	// initial map size is 4
	handle = struct {
		sync.RWMutex
		m map[string]*HandleParam
	}{m: make(map[string]*HandleParam, 4)}
)

// HandleParam define router m info and handle function
type HandleParam struct {
	HandleFunc func(*gin.Context)
	Path       string
	Method     string
}

var errNoRouter = errors.New("no router to register in http server")

// StartHTTPServer start http server
func StartHTTPServer(conf *config.FConfig, stopCh <-chan struct{},
	startWatch func(stopper <-chan struct{}) error) error {
	handle.RLock()
	length := len(handle.m)
	handle.RUnlock()
	if length == 0 {
		log.GetLogger().Errorf("error: %v", errNoRouter)
		return errNoRouter
	}

	handle.RLock()
	for _, v := range handle.m {
		router.Handle(v.Method, v.Path, v.HandleFunc)
	}
	handle.RUnlock()

	handler, wg, err := getHandler(conf, stopCh, startWatch)
	if err != nil {
		log.GetLogger().Errorf("getHandler error: %s", err.Error())
		return err
	}
	acquiredPodIP := os.Getenv(constants.PodIPEnvKey)
	podIP := net.ParseIP(acquiredPodIP)
	if podIP == nil {
		log.GetLogger().Errorf("failed to get pod ip address: %s", acquiredPodIP)
		return errors.New("get pod ip address error")
	}
	httpServer = &http.Server{
		Handler: handler,
		Addr:    podIP.String() + conf.Address,
		// Good practice: enforce timeouts for servers you create!
		WriteTimeout: writeTimeout,
		ReadTimeout:  readTimeout,
		IdleTimeout:  readTimeout,
	}
	go waitUtilInterrupt(stopCh, wg, httpServer)

	if !conf.HTTPSConfig.HTTPSEnable {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.GetLogger().Errorf("http server error: %s", err.Error())
			return err
		}
		return errors.New("http server error")
	}

	err = tls.HTTPListenAndServeTLS(httpServer.Addr, httpServer.Handler)
	if err != nil {
		log.GetLogger().Errorf("error when executes FastHTTPListenAndServeTLS: %s", err.Error())
		return err
	}
	return errors.New("https server error")
}

// getHandler get handler
func getHandler(conf *config.FConfig, stopCh <-chan struct{},
	startWatch func(stopper <-chan struct{}) error) (*Handler, *sync.WaitGroup, error) {
	var wg sync.WaitGroup
	clusterDiscovery, err := createClusterDiscovery(conf, stopCh, &wg)
	if err != nil || clusterDiscovery == nil {
		log.GetLogger().Errorf("failed to create cluster discovery, error: %v", err)
		return nil, &wg, err
	}
	clusterDiscovery.SetCallback(startWatch)
	err = clusterDiscovery.Discovery()
	if err != nil {
		log.GetLogger().Errorf("failed to run api server cluster discovery run, error: %s", err.Error())
		return nil, &wg, err
	}
	handler, err := NewServerHandler()
	if err != nil {
		log.GetLogger().Errorf("run api server new api handler error: NewServerHandler fail...")
		return nil, &wg, err
	}
	handler.Discovery = clusterDiscovery
	log.GetLogger().Infof("authenticationEnable: %v", handler.AuthCheck)
	handler.ForwardRequest = conf.Cluster.ForwardRequest
	handler.Conf = conf

	handler.Registry = router

	handler.Registry.GET("/role", func(c *gin.Context) {
		handler.GetRole(c.Writer, c.Request)
	})
	handler.Registry.GET("/leader", func(c *gin.Context) {
		handler.GetLeader(c.Writer, c.Request)
	})
	return handler, &wg, nil
}

// RegisterRouter register router
func RegisterRouter(param *HandleParam, name string) {
	handle.Lock()
	handle.m[name] = param
	handle.Unlock()
	log.GetLogger().Infof("register http router successfully: %s", name)
}

func waitUtilInterrupt(stopCh <-chan struct{}, wg *sync.WaitGroup, server *http.Server) {
	if stopCh == nil {
		log.GetLogger().Errorf("waitUtilInterrupt's stopCh is nil")
		return
	}
	select {
	case _, ok := <-stopCh:
		if !ok {
			log.GetLogger().Warnf("stopCh closed")
		}
		// wait all groutine graceful quit
		wg.Wait()

		ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
		defer cancel()
		// We received an interrupt signal, shutdown main goroutine.
		if err := server.Shutdown(ctx); err != nil {
			// Error from closing listeners, or context timeout:
			log.GetLogger().Errorf("failed to shutdown http-server, error: %s", err.Error())
		}
	}
}
