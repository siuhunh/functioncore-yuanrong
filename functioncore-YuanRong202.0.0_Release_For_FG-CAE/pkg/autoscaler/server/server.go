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

// Package server define function to run autoscaler
package server

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"functioncore/pkg/autoscaler/cache"
	"functioncore/pkg/autoscaler/clustermgr"
	"functioncore/pkg/autoscaler/config"
	"functioncore/pkg/autoscaler/instance"
	"functioncore/pkg/autoscaler/metrics"
	"functioncore/pkg/autoscaler/options"
	"functioncore/pkg/autoscaler/reactor"
	"functioncore/pkg/autoscaler/resourceusage"
	"functioncore/pkg/autoscaler/scaler"
	"functioncore/pkg/autoscaler/status"
	"functioncore/pkg/autoscaler/utils"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/urnutils"
)

const (
	defaultTickerInternal = 30
	// MaxTraceIDLength is the max length of traceID
	MaxTraceIDLength = 128
)

// HandleParam define router m info and handle function
type HandleParam struct {
	HandleFunc func(*gin.Context)
	Path       string
	Method     string
}

// InvokeErrorResponse invoke error response
type InvokeErrorResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// SwitchFlowRequest -
type SwitchFlowRequest struct {
	SwitchType   string                    `json:"switch_type"`
	ClusterID    string                    `json:"cluster_id"`
	FunctionType string                    `json:"function_type"`
	UrnList      map[string]map[string]int `json:"urn_list"`
}

// Run run api server and setup apis
func Run(options *options.AutoScalerOptions, stopCh <-chan struct{}) error {
	start := time.Now()
	if _, err := govalidator.ValidateStruct(*options); err != nil {
		log.GetLogger().Errorf("failed to validate option: %s", err.Error())
		return err
	}

	config.InitConfigPath(options.ConfigFile)
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get autoscaler config, error: %s", err.Error())
		return err
	}

	// WatchConfig failed do not return, just config hot load not enable
	if err := config.WatchConfig(stopCh, updateConfig); err != nil {
		log.GetLogger().Warnf("WatchConfig failed, err %s", err.Error())
	}

	if err := alarm.NewClient(conf.AlarmServerAddress, constants.Autoscaler, stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()),
		alarm.WithAuth(conf.LocalAuth.AKey, conf.LocalAuth.SKey)); err != nil {
		log.GetLogger().Errorf("failed to init alarm client, error: %s", err.Error())
		return err
	}
	alarm.GetClient().RegisterAlarmType(alarm.AutoScalerCheckWorkManagerUnHealthy)

	err = metrics.CreateMonitoringClient(conf.MetricServerConfig, stopCh)
	if err != nil {
		fmt.Printf("failed to create metrics client, error: %s", err.Error())
		return err
	}
	// init cae meta etcd
	if err := utils.InitETCDClient(stopCh); err != nil {
		fmt.Printf("init etcd client error, msg: %s\n", err.Error())
		return err
	}

	go startSelecterMasterNode(stopCh)

	log.GetLogger().Infof("begin to run health server with listen address %s", options.ListenAddress)
	startServer()

	log.GetLogger().Infof("finished to start api server with cost time %s", time.Since(start).String())
	return nil
}

func startMasterNodeJob(stopCh <-chan struct{}, conf *config.Config) error {
	clustermgr.CreateClusterMgr(stopCh, conf)
	metaReactor, err := reactor.NewEventDrive()
	if err != nil {
		log.GetLogger().Errorf("failed to new event drive for meta etcd error: %s", err.Error())
		return err
	}
	metaReactor.Run(stopCh)

	log.GetLogger().Infof("finished to run metaReactor")

	resourceusage.GetHandler().
		WithStopCh(stopCh).
		WithClusters(conf.Clusters).
		WithTickerTime(conf.TickerTime).
		WithMonitoringClient(metrics.GetMonitoringClient()).
		RegisterRefreshCacheFunc(cache.GetInstance().RefreshResourceMetricCache).
		RegisterCheckFunctionMetaExist(cache.GetInstance().CheckFunctionMetaExist).
		RegisterRefreshClusterFunc(clustermgr.GetClusterMgr().RefreshClusterMetricCache).
		HandlerResourceUsage()

	if err := scaler.CreateScaleMgr(stopCh); err != nil {
		log.GetLogger().Errorf("failed to create  scaleMgr with error %s", err.Error())
		return err
	}
	log.GetLogger().Infof("finished to start scaleMgr")

	cache.RecoverPersistence()
	go startInsCacheTicker(stopCh)
	go instance.CheckSpecialReBalanceKey(stopCh)

	return nil
}

func startServer() {
	podIP := os.Getenv(constants.PodIPEnvKey)
	if net.ParseIP(podIP) == nil {
		log.GetLogger().Errorf("failed to get pod ip of autoscaler from environment")
		return
	}
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		return
	})
	rg := router.Group("/serverless/v1")
	RegisterHandlers(rg.Group(""))
	server := &http.Server{
		Addr:    podIP + ":58877",
		Handler: router,
	}
	if config.EnableHTTPS() {
		server.TLSConfig = tls.GetClientTLSConfig()
		log.GetLogger().Infof("health check https server listening on %s", server.Addr)
		if err := server.ListenAndServeTLS("", ""); err != nil {
			log.GetLogger().Errorf("https server has exited: %s", err.Error())
		}
		return
	}

	log.GetLogger().Infof("health check http server listening on %s", server.Addr)
	if err := server.ListenAndServe(); err != nil {
		log.GetLogger().Errorf("http server has exited: %s", err.Error())
		return
	}
}

// RegisterHandlers sets up the routing of the HTTP handlers.
func RegisterHandlers(r *gin.RouterGroup) {
	r.POST("/switchflow", switchFlow)
}

func switchFlow(c *gin.Context) {
	traceID := initTraceID(c.Request)
	if requestAuthCheck(c.Request, c.Writer, traceID) {
		return
	}
	defer c.Request.Body.Close()
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		sendResponseAndWriteLog(http.StatusInternalServerError, utils.InternalError, err.Error(), traceID, c.Writer)
		log.GetLogger().Errorf("failed to read body from request, traceID %s: %s", traceID, err.Error())
		return
	}

	var switchFlowRequest SwitchFlowRequest
	if err := json.Unmarshal(body, &switchFlowRequest); err != nil {
		sendResponseAndWriteLog(http.StatusInternalServerError, utils.InternalError, err.Error(), traceID, c.Writer)
		log.GetLogger().Errorf("failed to unmarshal the switch flow request, traceID %s: %s", traceID, err.Error())
		return
	}
	switch switchFlowRequest.SwitchType {
	case utils.FlowOut:
		clustermgr.GetClusterMgr().ClusterFlowOut(switchFlowRequest.ClusterID)
		status.Set(status.FlowOut)
		handleFlowOutRequest(switchFlowRequest, traceID)
	case utils.FlowIn:
		clustermgr.GetClusterMgr().ClusterFlowIn(switchFlowRequest.ClusterID)
		status.Set(status.FlowIn)
		handleFlowInRequest(switchFlowRequest, traceID)
	default:
		log.GetLogger().Warnf("unknown switch type: %s", switchFlowRequest.SwitchType)
		sendResponseAndWriteLog(http.StatusInternalServerError, utils.InternalError,
			"unknown switch type", traceID, c.Writer)
		return
	}
	data, err := json.Marshal(InvokeErrorResponse{
		Code:    200,
		Message: "AutoScaler success handle switch flow request",
	})
	if err != nil {
		c.Writer.WriteHeader(http.StatusBadRequest)
		return
	}
	c.Writer.WriteHeader(http.StatusOK)
	if _, err := c.Writer.Write(data); err != nil {
		return
	}
}

// initTraceID init trace ID
func initTraceID(r *http.Request) string {
	traceID := r.Header.Get(constants.HeaderRequestID)
	if traceID == "" {
		traceID = r.Header.Get(constants.HeaderTraceID)
	}
	switch {
	case len(traceID) > MaxTraceIDLength:
		traceID = traceID[:MaxTraceIDLength]
	case traceID == "":
		traceID = uuid.New().String()
	default:
	}
	r.Header.Set(constants.HeaderTraceID, traceID)
	return traceID
}

// requestAuthCheck check auth for request
func requestAuthCheck(r *http.Request, w http.ResponseWriter, traceID string) bool {
	err := authCheck(r)
	if err != nil {
		sendResponseAndWriteLog(http.StatusUnauthorized, utils.UnAuthorized, err.Error(), traceID, w)
		log.GetLogger().Errorf("failed to authenticate request, traceID %s: %s", traceID, err.Error())
		return true
	}
	return false
}

func authCheck(r *http.Request) error {
	conf, err := config.GetConfig()
	if err != nil {
		return err
	}
	if !conf.AuthenticationEnable {
		return nil
	}
	requestSign := r.Header.Get(constants.HeaderAuthorization)
	timestamp := r.Header.Get(constants.HeaderAuthTimestamp)
	err = localauth.AuthCheckLocally(conf.LocalAuth.AKey, conf.LocalAuth.SKey,
		requestSign, timestamp, conf.LocalAuth.Duration)
	if err != nil {
		log.GetLogger().Errorf("failed to check authorization of URL locally, error: %s", err.Error())
		return err
	}
	return nil
}

func sendResponseAndWriteLog(code, iCode int, message, traceID string,
	w http.ResponseWriter) {
	data, err := json.Marshal(InvokeErrorResponse{
		Code:    iCode,
		Message: message,
	})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal response data: %s", err.Error())
		return
	}
	w.WriteHeader(code)
	_, err = w.Write(data)
	if err != nil {
		log.GetLogger().Warnf("failed to write response body: %s", err.Error())
	}
	log.GetLogger().Infof("request end in autoScaler, traceID: %s", traceID)
}

// handleFlowOutRequest -
func handleFlowOutRequest(switchFlowRequest SwitchFlowRequest, traceID string) {
	for funcURN, clustersInfo := range switchFlowRequest.UrnList {
		functionInfo, err := urnutils.GetFunctionInfo(funcURN)
		if err != nil {
			log.GetLogger().Warnf("failed to get function info from urn: %s, traceID: %s", funcURN, traceID)
			continue
		}
		functionSvcID := utils.CrName(functionInfo.BusinessID, functionInfo.TenantID,
			functionInfo.Name, functionInfo.Version)
		log.GetLogger().Infof("extract functionSvcID: %s from flow out request, traceID: %s", functionSvcID, traceID)
		for clusterID, switchNum := range clustersInfo {
			if switchNum <= 0 {
				log.GetLogger().Warnf("functionSvcID: %s flow out request cluster: %s switch number is 0, "+
					"ignore execute, traceID: %s", functionSvcID, clusterID, traceID)
				continue
			}
			executeFlowOutScale(clusterID, traceID, functionSvcID, switchNum, switchFlowRequest)
		}
	}
	log.GetLogger().Infof("success handle flow out request and finish all scale request, traceID: %s", traceID)
}

// executeFlowOutScale -
func executeFlowOutScale(clusterID, traceID, functionSvcID string, switchNum int,
	switchFlowRequest SwitchFlowRequest) {
	if clusterID == switchFlowRequest.ClusterID {
		log.GetLogger().Infof("cluster: %s flow out, prepare scale down %d instance", clusterID, switchNum)
		info := clustermgr.GetClusterMgr().GetClusterInfo(clusterID)
		if info.ActiveNodes.IP == "" {
			log.GetLogger().Warnf("failed to get cluster: %s active node ip, traceID: %s", clusterID, traceID)
			return
		}
		go scaler.ScaleDown(functionSvcID, int64(switchNum), 0, info.ActiveNodes.IP)
		time.Sleep(time.Second)
		log.GetLogger().Infof("finish send scale down request to worker-manager: %s, functionSvcID: %s",
			info.ActiveNodes.IP, functionSvcID)
		return
	}
	log.GetLogger().Infof("cluster: %s flow out, prepare scale up cluster: %s for %d instance",
		switchFlowRequest.ClusterID, clusterID, switchNum)
	info := clustermgr.GetClusterMgr().GetClusterInfo(clusterID)
	if info.ActiveNodes.IP == "" {
		log.GetLogger().Warnf("failed to get cluster: %s active node ip, traceID: %s", clusterID, traceID)
		return
	}
	count := cache.GetInstance().GetFuncInstanceCurrentCountInCluster(functionSvcID, clusterID)
	go scaler.ScaleUp(functionSvcID, int64(switchNum), int64(switchNum)+count, info.ActiveNodes.IP)
	time.Sleep(time.Second)
	log.GetLogger().Infof("finish send scale up request to worker-manager: %s, functionSvcID: %s, traceID:%s",
		info.ActiveNodes.IP, functionSvcID, traceID)
	return
}

// handleFlowInRequest -
func handleFlowInRequest(switchFlowRequest SwitchFlowRequest, traceID string) {
	for funcURN, clustersInfo := range switchFlowRequest.UrnList {
		functionInfo, err := urnutils.GetFunctionInfo(funcURN)
		if err != nil {
			log.GetLogger().Warnf("failed to get function info from urn: %s, traceID: %s", funcURN, traceID)
			continue
		}
		functionSvcID := utils.CrName(functionInfo.BusinessID, functionInfo.TenantID,
			functionInfo.Name, functionInfo.Version)
		log.GetLogger().Infof("extract functionSvcID: %s from flow in request, traceID: %s", functionSvcID, traceID)
		for clusterID, switchNum := range clustersInfo {
			if switchNum <= 0 {
				log.GetLogger().Warnf("functionSvcID: %s flow in request cluster: %s switch number is 0, "+
					"ignore execute, traceID: %s", functionSvcID, clusterID, traceID)
				continue
			}
			executeFlowInScale(clusterID, traceID, functionSvcID, switchNum, switchFlowRequest)
		}
	}
	log.GetLogger().Infof("success handle flow in request and finish all scale request, traceID: %s", traceID)
}

// executeFlowInScale -
func executeFlowInScale(clusterID, traceID, functionSvcID string, switchNum int,
	switchFlowRequest SwitchFlowRequest) {
	if clusterID == switchFlowRequest.ClusterID {
		log.GetLogger().Infof("cluster: %s flow in, prepare scale up %d instance", clusterID, switchNum)
		info := clustermgr.GetClusterMgr().GetClusterInfo(clusterID)
		if info.ActiveNodes.IP == "" {
			log.GetLogger().Warnf("failed to get cluster: %s active node ip, traceID: %s", clusterID, traceID)
			return
		}
		count := cache.GetInstance().GetFuncInstanceCurrentCountInCluster(functionSvcID, clusterID)
		go scaler.ScaleUp(functionSvcID, int64(switchNum), int64(switchNum)+count, info.ActiveNodes.IP)
		time.Sleep(time.Second)
		log.GetLogger().Infof("finish send scale up request to worker-manager: %s, functionSvcID: %s, "+
			"traceID: %s", info.ActiveNodes.IP, functionSvcID, traceID)
		return
	}
	log.GetLogger().Infof("cluster: %s flow in, prepare scale down cluster: %s for %d instance",
		switchFlowRequest.ClusterID, clusterID, switchNum)
	info := clustermgr.GetClusterMgr().GetClusterInfo(clusterID)
	if info.ActiveNodes.IP == "" {
		log.GetLogger().Warnf("failed to get cluster: %s active node ip, traceID: %s", clusterID, traceID)
		return
	}
	count := cache.GetInstance().GetFuncInstanceCurrentCountInCluster(functionSvcID, clusterID)
	expectedNum := count - int64(switchNum)
	if expectedNum < 0 {
		expectedNum = 0
	}
	go scaler.ScaleDown(functionSvcID, int64(switchNum), expectedNum, info.ActiveNodes.IP)
	time.Sleep(time.Second)
	log.GetLogger().Infof("finish send scale down request to worker-manager: %s, functionSvcID: %s",
		info.ActiveNodes.IP, functionSvcID)
	return
}

func updateConfig() {
	log.GetLogger().Infof("autoscaler updateConfig success")
}

func initAllModules() error {
	if err := cache.InitAutoScalerCache(); err != nil {
		log.GetLogger().Errorf("failed to init worker cache: %s", err.Error())
		return err
	}
	return nil
}

func startInsCacheTicker(stopCh <-chan struct{}) {
	var tickerInternal int64
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config, error: %s", err.Error())
		tickerInternal = defaultTickerInternal
	} else {
		tickerInternal = conf.SyncPeriod
	}
	if tickerInternal <= 0 {
		log.GetLogger().Errorf("failed to startInsCacheTicker, tickerInternal is non-positive")
		return
	}
	alarm.GetClient().RegisterAlarmType(alarm.RedisConnection)
	ticker := time.NewTicker(time.Duration(tickerInternal) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cache.SaveInsCacheToRedis()
		case <-stopCh:
			log.GetLogger().Infof("stop channel is closed")
			return
		}
	}
}
