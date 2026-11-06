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

package server

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/logger/log"
	commontls "functioncore/pkg/common/tls"
	"functioncore/pkg/common/websocket"
	"functioncore/pkg/web-terminal/config"
	"functioncore/pkg/web-terminal/options"
	"functioncore/pkg/web-terminal/webshell"
)

// Run run api server and setup apis
func Run(options *options.WebTerminalOptions, stopCh <-chan struct{}) error {
	start := time.Now()
	// WatchConfig failed do not return, just config hot load not enable
	if err := config.WatchConfig(stopCh, updateConfig); err != nil {
		log.GetLogger().Warnf("WatchConfig failed, err %s", err.Error())
	}
	addr := options.ListenAddress
	router := createRouter()
	httpServer := &http.Server{
		Handler: router,
		Addr:    addr,
	}
	// ban HTTP/2, use HTTP/1.1
	httpServer.TLSNextProto = make(map[string]func(*http.Server, *tls.Conn, http.Handler))
	go websocket.WaitUtilInterrupt(stopCh, httpServer)
	err := startServer(httpServer)
	if err != nil {
		fmt.Printf("failed to run server: %s\n", err.Error())
		return err
	}
	log.GetLogger().Infof("finished to start api server with cost time %s", time.Since(start).String())
	return nil
}

func startServer(httpServer *http.Server) error {
	if config.EnableHTTPS() {
		httpServer.TLSConfig = commontls.GetClientTLSConfig()
		log.GetLogger().Infof("https server listening on %s", httpServer.Addr)
		if err := httpServer.ListenAndServeTLS("", ""); err != nil {
			log.GetLogger().Errorf("https server has exited: %s", err.Error())
			return err
		}
		return nil
	}
	if err := httpServer.ListenAndServe(); err != nil {
		log.GetLogger().Errorf("http server has exited: %s", err.Error())
		return err
	}
	return nil
}

func createRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		healthz(c.Writer, c.Request)
	})
	router.GET("/api/k8s/clusters/:cluster_id/api/v1/namespaces/:namespace_id/pods/:pod_name/exec",
		webshell.HandleWebShell)
	return router
}

func updateConfig() {
	log.GetLogger().Infof("web-terminal updateConfig success")
}

func healthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	return
}
