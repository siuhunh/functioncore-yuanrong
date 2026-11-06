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

// Package healthcheck implement health check
package healthcheck

import (
	"errors"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

const (
	readTimeout  = 180 * time.Second
	writeTimeout = 180 * time.Second
)

// StartHealthCheck -
func StartHealthCheck(errChan chan error) error {
	if errChan == nil {
		return errors.New("errChan is nil")
	}

	router := createRouter()

	podIP := os.Getenv("POD_IP")
	if net.ParseIP(podIP) == nil {
		log.GetLogger().Error("failed to get pod ip")
		return errors.New("failed to get pod ip")
	}
	addr := podIP + ":" + config.FConfig.Runtime.HealthCheckPort

	httpServer := &http.Server{
		Handler:      router,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		Addr:         addr,
	}

	go func() {
		err := startServer(httpServer)
		if err != nil {
			log.GetLogger().Errorf("failed to startServer, err %s", err.Error())
		}
		errChan <- err
	}()

	return nil
}

func startServer(httpServer *http.Server) error {
	if !config.FConfig.HTTPSConfig.HTTPSEnable {
		err := httpServer.ListenAndServe()
		if err != nil {
			log.GetLogger().Errorf("failed to http ListenAndServe: %s", err.Error())
		}
		return err
	}
	httpServer.TLSConfig = tls.GetClientTLSConfig()
	err := httpServer.ListenAndServeTLS("", "")
	if err != nil {
		log.GetLogger().Errorf("failed to HTTPListenAndServeTLS: %s", err.Error())
		return err
	}
	return nil
}

func createRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthcheck", func(c *gin.Context) {
		healthCheck(c.Writer, c.Request)
	})
	return router
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	return
}
