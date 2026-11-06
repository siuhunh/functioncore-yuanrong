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

// Package frontend start router
package frontend

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/aliasroute"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/event/stateinsevent"
	"functioncore/pkg/frontend-invoke/functionlb"
	"functioncore/pkg/frontend-invoke/loadbalancer/etcdadapter"
	"functioncore/pkg/frontend-invoke/metadata"
	"functioncore/pkg/frontend-invoke/stream"
	"functioncore/pkg/frontend-invoke/streamclient"
	"functioncore/pkg/frontend-invoke/util"
)

var (
	registerOnce sync.Once
	serverWG     sync.WaitGroup
)

// StartHTTPServer Start http server
func StartHTTPServer(stopCh <-chan struct{}) error {
	if stopCh == nil {
		return errors.New("stop channel is nil")
	}
	if err := alarm.NewClient(constant.FConfig.AlarmServerAddress, constants.FunctionAccessor, stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth(constant.FConfig.LocalAuth.AKey,
			constant.FConfig.LocalAuth.SKey)); err != nil {
		log.GetLogger().Errorf("failed to init alarm client, error: %s", err.Error())
		return err
	}
	if err := util.InitETCDClient(stopCh); err != nil {
		fmt.Printf("failed to init etcd client: %s\n", err.Error())
		return err
	}
	alarm.GetClient().RegisterAlarmType(alarm.StreamClient)
	alarm.GetClient().RegisterAlarmType(alarm.FaaSSchedulerConnection)
	metadata.SetFuncMetaUpdateCallBack(functionlb.FunctionLB.UpdateFuncMeta)
	functionlb.FunctionLB.Start(stopCh)
	stream.InitDiscovery(stopCh)
	startWatch(stopCh)

	podIP := os.Getenv("POD_IP")
	if net.ParseIP(podIP) == nil {
		log.GetLogger().Error("failed to get pod ip")
		return errors.New("failed to get pod ip")
	}

	var err error
	serverWG.Add(1)
	go func() {
		if err = statHTTPServer(podIP, stopCh); err != nil {
			log.GetLogger().Errorf("failed to start http server: %s", err.Error())
		}
		serverWG.Done()
	}()
	serverWG.Add(1)
	go func() {
		if err = statWebsocketServer(podIP, stopCh); err != nil {
			log.GetLogger().Errorf("failed to start websocket server: %s", err.Error())
		}
		serverWG.Done()
	}()
	serverWG.Wait()
	log.GetLogger().Warnf("all server is shutdown")
	return err
}

func statHTTPServer(podIP string, stopCh <-chan struct{}) error {
	router := createRouter()
	addr := podIP + ":" + constant.FConfig.Runtime.Port
	httpServer := &http.Server{
		Handler:      router,
		ReadTimeout:  constant.DefaultServerReadTimeOut,
		WriteTimeout: constant.DefaultServerWriteTimeOut,
		Addr:         addr,
	}
	log.GetLogger().Infof("start the websocket http addr: %s", addr)
	errChan := make(chan error, 1)
	go func() {
		errChan <- startServer(httpServer)
	}()
	go streamclient.CleanExpiredClient(stopCh)
	return waitShutdown(httpServer, stopCh, errChan)
}

func statWebsocketServer(podIP string, stopCh <-chan struct{}) error {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/serverless/v1/functions/:podName/:requestId/:projectId/:function-name/:version/websocket",
		WebSocketWorkerConnect)
	webSocketAddr := podIP + ":" + constant.FConfig.Runtime.WebsocketPort
	httpServer := &http.Server{
		Handler:      router,
		ReadTimeout:  constant.DefaultServerReadTimeOut,
		WriteTimeout: constant.DefaultServerWriteTimeOut,
		Addr:         webSocketAddr,
	}
	log.GetLogger().Infof("start the websocket server addr: %s", webSocketAddr)
	httpServer.RegisterOnShutdown(ShutDownAllWebsocketConnection)
	errChan := make(chan error, 1)
	go func() {
		errChan <- startServer(httpServer)
	}()
	return waitShutdown(httpServer, stopCh, errChan)
}

func createRouter() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/serverless/v1/componentshealth", func(c *gin.Context) {
		ClusterHealth(c.Writer, c.Request)
	})
	router.GET("/healthz", func(c *gin.Context) {
		healthz(c.Writer, c.Request)
	})
	router.POST("/serverless/v1/functions/:function-name/invocations", tracer.WrapGinHandler(InvokeHandler))
	// WebSocket router used to create or delete connections
	router.POST("/serverless/v1/functions/:function-name/websocket", WebSocketInstanceCreateHandler)
	router.DELETE("/serverless/v1/functions/:function-name/websocket", WebSocketInstanceDeleteHandler)
	router.GET("/serverless/v1/stream/instance", getStreamInstanceHandler)
	router.POST("/serverless/v1/functions/:function-name/stream-invoke", streamInvokeHandler)
	router.POST("/serverless/v1/functions/instances/recover", instancesRecoverHandler)
	router.GET("/serverless/v1/functions/:podName/:requestId/:projectId/:function-name/:version/websocket",
		WebSocketWorkerConnect)
	return router
}

func waitShutdown(server *http.Server, stopCh <-chan struct{}, errChan <-chan error) error {
	if stopCh == nil || errChan == nil {
		return errors.New("input channel is nil")
	}
	select {
	case <-stopCh:
		log.GetLogger().Infof("received termination signal")
		graceExit = true
		ctx, cancel := context.WithTimeout(context.Background(), constant.DefaultServerWriteTimeOut)
		defer cancel()
		return server.Shutdown(ctx)
	case err := <-errChan:
		return err
	}
}

func startServer(httpServer *http.Server) error {
	if !constant.FConfig.HTTPSConfig.HTTPSEnable {
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
	}
	return err
}

func startWatch(stopCh <-chan struct{}) {
	processWatchEventByRouterETCD(stopCh)
	processWatchEventByMetaETCD(stopCh)
	processWatchEventByCAEMetaETCD(stopCh)
}

func processWatchEventByRouterETCD(stopCh <-chan struct{}) {
	// start worker instance watcher
	successCh := make(chan string)
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Router,
		Path:          constant.WorkerInstancePrefix,
		Filter:        isWorkerInstance,
		Handler:       processInstanceEvent,
		StopCh:        stopCh,
		SuccessCh:     successCh,
		RecordRev:     true,
	})
	<-successCh
	// start node watcher
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Router,
		Path:          NodeEtcdPrefix,
		Filter:        IsTaskNode,
		Handler:       processNodeEvent,
		StopCh:        stopCh,
		RecordRev:     true,
	})
	// start faas-scheduler watcher
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Router,
		Path:          etcdadapter.SchedulerPrefix,
		Filter:        etcdadapter.IsFaaSScheduler,
		Handler:       etcdadapter.ProcessSchedulerEvent,
		StopCh:        stopCh,
		RecordRev:     true,
	})
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Router,
		Path:          stream.InstanceKey,
		Filter:        stream.IsStreamEvent,
		Handler:       stream.ProcessStreamEvent,
		StopCh:        stopCh,
		RecordRev:     true,
	})
}

func processWatchEventByMetaETCD(stopCh <-chan struct{}) {
	// start alias watcher
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Meta,
		Path:          AliasEtcdPrefix,
		Filter:        aliasroute.IsAlias,
		Handler:       processAliasEvent,
		StopCh:        stopCh,
		RecordRev:     true,
		CellEnable:    true,
	})
	// start stateWorkers watcher
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Meta,
		Path:          etcdkey.StateRoutePrefix,
		Filter:        stateinsevent.IsStateInstance,
		Handler:       stateinsevent.ProcessStateInsEvent,
		StopCh:        stopCh,
		RecordRev:     true,
	})
	// start functions watcher
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Meta,
		Path:          functionsPrefix,
		Filter:        isFunction,
		Handler:       processFunctionEvent,
		StopCh:        stopCh,
		RecordRev:     true,
		CellEnable:    true,
	})
	// start tenant QOS watcher
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.Meta,
		Path:          QOSEtcdPrefix,
		Filter:        IsTenantQOS,
		Handler:       processTenantEvent,
		StopCh:        stopCh,
		RecordRev:     true,
		CellEnable:    true,
	})
}

func processWatchEventByCAEMetaETCD(stopCh <-chan struct{}) {
	if constant.FConfig.CAEMetaETCD == nil || len(constant.FConfig.CAEMetaETCD.ServerList) == 0 {
		log.GetLogger().Warnf("empty config of cae meta etcd, skip")
		return
	}
	go processWatcher(&watchOption{
		ETCDSelection: etcd3.CAEMeta,
		Path:          functionsPrefix,
		Filter:        isFunction,
		Handler:       processFunctionEvent,
		StopCh:        stopCh,
		RecordRev:     true,
		CellEnable:    true,
	})
}

func healthz(w http.ResponseWriter, r *http.Request) {
	registerOnce.Do(func() {
		registerInstance(signals.WaitForSignal())
	})
	w.WriteHeader(http.StatusOK)
	return
}
