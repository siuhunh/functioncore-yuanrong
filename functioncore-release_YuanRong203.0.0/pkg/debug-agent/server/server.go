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

// Package server is http server of debug agent
package server

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/debug-agent/config"
)

const (
	podIPKey = "POD_IP"

	defaultMaxHeaderBytes = 1048576
	defaultReadTimeout    = 333
	defaultWriteTimeout   = 333
)

// CreateHTTPServer creates and runs http server of debug agent
func CreateHTTPServer(conf *config.DebugAgentConfig) error {
	podIP, ok := os.LookupEnv(podIPKey)
	if !ok {
		errMsg := "podIP does not exist"
		log.GetLogger().Errorf(errMsg)
		return errors.New(errMsg)
	}

	address := fmt.Sprintf("%s:%d", podIP, conf.Port)
	server := &http.Server{
		Addr:           address,
		Handler:        buildHandler(),
		MaxHeaderBytes: getMaxHeaderBytes(conf.MaxHeaderBytes),
		ReadTimeout:    time.Duration(getReadTimeout(conf.ReadTimeout)) * time.Second,
		WriteTimeout:   time.Duration(getWriteTimeout(conf.WriteTimeout)) * time.Second,
	}
	log.GetLogger().Infof("server is running at address: %s", address)

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.GetLogger().Errorf("failed to start http server, err: %s", err.Error())
		return err
	}
	return nil
}

func buildHandler() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		c.Writer.WriteHeader(http.StatusOK)
	})
	return router
}

func getMaxHeaderBytes(value int) int {
	if value <= 0 {
		return defaultMaxHeaderBytes
	}
	return value
}

func getReadTimeout(value int64) int64 {
	if value <= 0 {
		return defaultReadTimeout
	}
	return value
}

func getWriteTimeout(value int64) int64 {
	if value <= 0 {
		return defaultWriteTimeout
	}
	return value
}
