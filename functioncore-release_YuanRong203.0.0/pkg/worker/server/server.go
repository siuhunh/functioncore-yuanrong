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

// Package server is almost main process of worker
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/fsnotify/fsnotify"
	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/diskusage"
	"functioncore/pkg/common/engine"
	"functioncore/pkg/common/kms"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/obs"
	"functioncore/pkg/common/redisclient/podevent"
	"functioncore/pkg/common/signals"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/storage"
	"functioncore/pkg/common/tcpmsg"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/tracer"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/cache"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/deploy"
	"functioncore/pkg/worker/functionlog"
	"functioncore/pkg/worker/invoke"
	"functioncore/pkg/worker/mount"
	"functioncore/pkg/worker/network"
	"functioncore/pkg/worker/runtime"
	"functioncore/pkg/worker/runtime/fusion"
	"functioncore/pkg/worker/snapshot"
	"functioncore/pkg/worker/status"
	"functioncore/pkg/worker/utils"
	"functioncore/pkg/worker/websocket"
)

var (
	loadedOnce   sync.Once
	loadedResult bool

	needUploadSnapshot = os.Getenv("need_upload_snapshot") == "true"
	needDeleteSnapshot = os.Getenv(constants.NeedDeleteSnapshot) == "true"

	streamHandlerHostCidr    = os.Getenv(constants.StreamHandlerHostCidr)
	streamHandlerServicePort = os.Getenv(constants.StreamHandlerServicePort)

	customUsedPort         = [4]int{8081, 22668, 32569, 28920}
	closeDeployChanOnce    sync.Once
	websocketFinalizerOnce sync.Once
	codeDeployChan         = make(chan interface{})
)

const (
	mountFileLimit                     = 3 * 1024 * 1024 // 3MB
	deployCodeTimeout                  = 10
	uint64Base                         = 10
	preStopTimeout                     = 72 * time.Hour
	defaultTenantCommandRequestTimeout = 300
	updateModifyTimeInterval           = 5 * time.Minute
	defaultUploadDumpFileTimeout       = 5 * 60 // 5 minute
	waitSecondsBeforeChangeNetwork     = 30 * time.Second
)

// Server represents server of worker
// It has management and invoke, 2 types interface for app
// It based on net/http
type Server struct {
	conf *config.Configuration
	// httpServer is internal server implementation
	httpServer *http.Server

	preStopServer *http.Server
	preStopCh     chan struct{}

	// invokeServer holds a type of runtime
	// And communicate and manage the child process of language runtime.
	invokeServer *invoke.Server

	net *network.NetWork

	pingCtx        context.Context
	pingCancelFunc context.CancelFunc
	pingCtxMutex   sync.Mutex
	pingHash       string
	ping6Hash      string

	deployer       deploy.Deployer
	customDeployer deploy.Deployer

	finalizers []func() error

	terminationChan          chan struct{}
	terminationChanCloseOnce sync.Once

	customDiskMgr *diskusage.EvictionManager

	customContainerPreError    error
	customContainerPreOnce     sync.Once
	skipWaitRuntimeContainerID bool
	customContainerPreFinished bool
	runtimeContainerID         string

	currentTenantCommandsTaskID string
	tenantCommandExecuteLock    sync.RWMutex
	wgDumpUpload                sync.WaitGroup
}

func (s *Server) cancelAndCreateNewContext() context.Context {
	s.pingCtxMutex.Lock()
	if s.pingCtx != nil {
		// cancel old context and create new context
		s.pingCancelFunc()
		s.pingCtx = nil
	}
	if s.pingCtx == nil {
		s.pingCtx, s.pingCancelFunc = context.WithCancel(context.Background())
	}
	ctx := s.pingCtx
	s.pingCtxMutex.Unlock()
	return ctx
}

func (s *Server) initPreStopRouter() {
	r := gin.New()
	r.Use(gin.Recovery())
	r.GET(constants.PreStopPath, func(c *gin.Context) {
		log.GetLogger().Infof("start to handle prestop")
		<-s.preStopCh
		log.GetLogger().Infof("finished to handle prestop")
		c.Status(http.StatusOK)
	})

	s.preStopServer.Handler = r
}

func (s *Server) initRouter(r *gin.Engine) {
	log.GetLogger().Infof("starting to init the router")

	r.GET("/healthz", func(c *gin.Context) {
		s.checkHealth(c.Writer, c.Request)
	})

	authorized := r.Group("/")
	authorized.Use(func(c *gin.Context) {
		if !s.checkAuth(c.Request) {
			writeResponse(c.Writer, errors.New("failed to check auth locally"))
			c.Abort()
		}
	})
	authorized.POST(constants.LoadFunctionPath, tracer.WrapGinHandler(func(c *gin.Context) {
		s.load(c)
	}))
	authorized.POST(constants.NotifyFunctionPath, func(c *gin.Context) {
		s.notifyFunction(c.Writer, c.Request)
	})
	authorized.POST(constants.ChangePatPodIPPath, func(c *gin.Context) {
		s.changePatPodIP(c.Writer, c.Request)
	})
	authorized.POST(constants.ShutdownPath, func(c *gin.Context) {
		s.handleShutdown(c.Writer, c.Request)
	})
	authorized.POST(constants.UpdateAgencyInfo, func(c *gin.Context) {
		s.handleUpdateAgencyInfo(c.Writer, c.Request)
	})
	authorized.POST(constants.NotifyRouterReady, s.handleNotifyRouterReady)
	if s.conf.RunningMode == constants.RunningModeCheckpoint {
		authorized.GET(constants.CheckpointPath, func(c *gin.Context) {
			s.checkpointFunction(c.Writer, c.Request)
		})
	}
	authorized.GET(constants.UploadSnapshotToDS, func(c *gin.Context) {
		s.uploadSnapshotToDS(c.Writer, c.Request)
	})
	authorized.GET(constants.DeleteSnapshotData, func(c *gin.Context) {
		s.deleteSnapshotData(c.Writer, c.Request)
	})
	authorized.POST(constants.WssConnectPath, func(c *gin.Context) {
		s.handleWssConnect(c.Writer, c.Request)
	})
	authorized.POST(constants.WssCountPath, func(c *gin.Context) {
		s.handleWssConnCount(c.Writer, c.Request)
	})
	authorized.POST(constants.ExecuteTenantCommands, func(c *gin.Context) {
		s.handleTenantCommands(c.Writer, c.Request)
	})
}

func (s *Server) load(c *gin.Context) {
	if needUploadSnapshot || needDeleteSnapshot || s.checkNeedDeleteSnapshotFromHeader(c.Request) {
		log.GetLogger().Info("need to upload or delete snapshot to data system and no need to load function")
		s.processUploadDeleteLoad(c)
		return
	}
	start := time.Now()
	s.loadFunction(c.Writer, c.Request)
	s.conf.DurationInfo.SpecifyPodDuration = time.Since(start)
}

func (s *Server) checkNeedDeleteSnapshotFromHeader(r *http.Request) bool {
	return r.Header.Get(constants.NeedDeleteSnapshot) == "true"
}

func (s *Server) processUploadDeleteLoad(c *gin.Context) {
	if err := s.setConfigS3AkSk(c.Request); err != nil {
		log.GetLogger().Errorf("setConfigS3AkSk failed, err: %s.", err.Error())
		c.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	if err := s.setStorageConfig(c.Request); err != nil {
		c.Writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	c.Writer.WriteHeader(http.StatusOK)
}

func newHTTPServer(conf *config.Configuration, router *gin.Engine, podIP string) (*http.Server, error) {
	ctx, cancel := context.WithCancel(context.Background())
	httpServer := &http.Server{
		Handler:        router,
		ReadTimeout:    time.Duration(conf.ServerConfig.ReadTimeout) * time.Second,
		WriteTimeout:   time.Duration(conf.ServerConfig.WriteTimeout) * time.Second,
		MaxHeaderBytes: conf.ServerConfig.MaxHeaderBytes,
		Addr:           fmt.Sprintf("%s:%d", podIP, conf.ServerConfig.Port),
		BaseContext:    func(_ net.Listener) context.Context { return ctx },
	}
	httpServer.RegisterOnShutdown(cancel)

	if conf.HTTPSConfig.HTTPSEnable {
		log.GetLogger().Infof("https enabled")
		if err := tls.InitTLSConfig(conf.HTTPSConfig.TLSProtocol, conf.HTTPSConfig.TLSCiphers); err != nil {
			log.GetLogger().Errorf("failed to init tls config, error: %s", err.Error())
			return nil, err
		}
		httpServer.TLSConfig = tls.GetClientTLSConfig()
	}
	return httpServer, nil
}

// NewServer configures and returns a Server
func NewServer(conf *config.Configuration) (*Server, error) {
	log.GetLogger().Infof("starting to create the app server")
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())

	podIP := os.Getenv("POD_IP")
	if net.ParseIP(podIP) == nil {
		log.GetLogger().Error("failed to get pod ip")
		return nil, errors.New("failed to get pod ip")
	}

	httpServer, err := newHTTPServer(conf, router, podIP)
	if err != nil {
		return nil, err
	}

	preStopServer := &http.Server{
		WriteTimeout: preStopTimeout,
		Addr:         fmt.Sprintf("%s:%d", podIP, conf.ServerConfig.Port+1),
	}

	serverAddr := fmt.Sprintf("%s:%d", podIP, conf.InvokeServer.Port)
	invokeServer, err := invoke.InitServer(conf, serverAddr)
	if err != nil {
		log.GetLogger().Errorf("failed to new invoker, error: %s", err.Error())
		return nil, err
	}

	deployer, err := deploy.NewDeployer(conf)
	if err != nil {
		log.GetLogger().Errorf("failed to new deployer: %s", err.Error())
		return nil, err
	}

	log.GetLogger().Infof("conf runningmode: %s", conf.RunningMode)
	ser := &Server{
		conf:            conf,
		deployer:        deployer,
		httpServer:      httpServer,
		preStopServer:   preStopServer,
		preStopCh:       make(chan struct{}),
		invokeServer:    invokeServer,
		terminationChan: make(chan struct{}),
		pingHash:        utils.GetHash(utils.PingPath),
		ping6Hash:       utils.GetHash(utils.Ping6Path),
	}
	ser.initRouter(router)
	ser.initPreStopRouter()
	return ser, nil
}

// GetTerminationChan returns the channel that the worker exits gracefully
func (s *Server) GetTerminationChan() chan struct{} {
	return s.terminationChan
}

// IsLoaded -
func (s *Server) IsLoaded() bool {
	if s.conf != nil && (s.conf.IsLoaded || s.conf.Language == worker.CustomContainerLanguage) {
		return true
	}
	return false
}

func (s *Server) recordLoaded() error {
	filename := path.Join(s.conf.DeploymentConfig.DeployDir, worker.LoadedFilename)
	file, err := os.Create(filename)
	if err != nil {
		log.GetLogger().Errorf("record loaded error: %s", err.Error())
		return err
	}
	err = file.Close()
	if err != nil {
		log.GetLogger().Errorf("record loaded file close error: %s", err.Error())
	}
	return nil
}

func (s *Server) isLoaded() bool {
	loadedOnce.Do(func() {
		filename := path.Join(s.conf.DeploymentConfig.DeployDir, worker.LoadedFilename)
		nonexist := false
		_, err := os.Stat(filename)
		if err != nil {
			nonexist = os.IsNotExist(err)
		}
		log.GetLogger().Infof("function is loaded, nonexist: %t", nonexist)
		loadedResult = !nonexist
	})
	return loadedResult
}

func checkNATConfigures(changedNATConfigures []types.NATConfigure) (bool, error) {
	isDeletedNum, isNewCreatedNum := 0, 0
	for i := range changedNATConfigures {
		if changedNATConfigures[i].IsDeleted && !changedNATConfigures[i].IsNewCreated {
			isDeletedNum++
		} else if !changedNATConfigures[i].IsDeleted && changedNATConfigures[i].IsNewCreated {
			isNewCreatedNum++
		}
	}
	if isDeletedNum > 0 {
		return false, nil
	}
	if isNewCreatedNum > 0 {
		return true, nil
	}
	return false, fmt.Errorf("unkown nat configures type with isDeletedNum %d, isNewCreatedNum %d",
		isDeletedNum, isNewCreatedNum)
}

func (s *Server) updateNATConfigures(changedNet *network.NetWork, traceID string) (bool, bool) {
	needReplaced, err := checkNATConfigures(changedNet.NATConfigures)
	if err != nil {
		log.GetLogger().Errorf("failed to check nat update scenario with traceID: %s, err: %s",
			traceID, err.Error())
		return false, false
	}
	// replace scenario - all nat configures are new created
	if needReplaced {
		s.net.NATConfigures = make([]types.NATConfigure, 0, len(changedNet.NATConfigures))
		for i := range changedNet.NATConfigures {
			s.net.NATConfigures = append(s.net.NATConfigures, changedNet.NATConfigures[i])
			log.GetLogger().Infof("succeed to replace nat by new pat pod name: %s and pat pod ip: %s "+
				"with traceID: %s",
				changedNet.NATConfigures[i].PatPodName, changedNet.NATConfigures[i].PatContainerIP, traceID)
		}
		return true, false
	}
	// update scenario - one nat configure is new created, and the other is deleted
	isUpdated := false
	deletedNATPatPodName := ""
	var updatedNATConfigure types.NATConfigure
	// currently, one-to-one nat configure is supported
	// it means one deleted nat pat service pod and one updated pat service pod
	for i := range changedNet.NATConfigures {
		if changedNet.NATConfigures[i].IsDeleted {
			deletedNATPatPodName = changedNet.NATConfigures[i].PatPodName
			log.GetLogger().Infof("deleted nat pat pod name: %s and pat pod ip: %s with traceID: %s",
				deletedNATPatPodName, changedNet.NATConfigures[i].PatContainerIP, traceID)
		} else {
			updatedNATConfigure = changedNet.NATConfigures[i]
			log.GetLogger().Infof("updated nat pat pod name: %s and pat pod ip: %s with traceID: %s",
				changedNet.NATConfigures[i].PatPodName, changedNet.NATConfigures[i].PatContainerIP, traceID)
		}
	}
	if deletedNATPatPodName != "" {
		for i := range s.net.NATConfigures {
			if s.net.NATConfigures[i].PatPodName == deletedNATPatPodName {
				log.GetLogger().Infof("succeed to update pat pod ip from %s to %s with traceID: %s",
					s.net.NATConfigures[i].PatContainerIP, updatedNATConfigure.PatContainerIP, traceID)
				s.net.NATConfigures[i] = updatedNATConfigure
				isUpdated = true
				// it will only update one nat configure at the same time
				break
			}
		}
	}
	return false, isUpdated
}

func (s *Server) updatePublicNATConfigures(changedNet *network.NetWork, traceID string) (bool, bool) {
	needReplaced, err := checkNATConfigures(changedNet.PublicVpcNATConfigure)
	if err != nil {
		log.GetLogger().Errorf("failed to check public nat update scenario with traceID: %s, err: %s",
			traceID, err.Error())
		return false, false
	}
	// replace scenario - all nat configures are new created
	if needReplaced {
		s.net.PublicVpcNATConfigure = make([]types.NATConfigure, 0, len(changedNet.PublicVpcNATConfigure))
		for i := range changedNet.PublicVpcNATConfigure {
			s.net.PublicVpcNATConfigure = append(s.net.PublicVpcNATConfigure, changedNet.PublicVpcNATConfigure[i])
			log.GetLogger().Infof("succeed to replace nat by new pat pod name: %s and pat pod ip: %s "+
				"with traceID: %s", changedNet.PublicVpcNATConfigure[i].PatPodName,
				changedNet.PublicVpcNATConfigure[i].PatContainerIP, traceID)
		}
		return true, false
	}
	// update scenario - one nat configure is new created, and the other is deleted
	isUpdated := false
	deletedPublicNATPatPodName := ""
	var updatedNATConfigure types.NATConfigure
	// currently, one-to-one nat configure is supported
	// it means one deleted nat pat service pod and one updated pat service pod
	for i := range changedNet.PublicVpcNATConfigure {
		if changedNet.PublicVpcNATConfigure[i].IsDeleted {
			deletedPublicNATPatPodName = changedNet.PublicVpcNATConfigure[i].PatPodName
			log.GetLogger().Infof("deleted nat public pat pod name: %s and pat pod ip: %s with traceID: %s",
				deletedPublicNATPatPodName, changedNet.PublicVpcNATConfigure[i].PatContainerIP, traceID)
		} else {
			updatedNATConfigure = changedNet.PublicVpcNATConfigure[i]
			log.GetLogger().Infof("updated nat public pat pod name: %s and pat pod ip: %s with traceID: %s",
				changedNet.PublicVpcNATConfigure[i].PatPodName, changedNet.PublicVpcNATConfigure[i].PatContainerIP, traceID)
		}
	}
	if deletedPublicNATPatPodName != "" {
		for i := range s.net.PublicVpcNATConfigure {
			if s.net.PublicVpcNATConfigure[i].PatPodName == deletedPublicNATPatPodName {
				log.GetLogger().Infof("succeed to update puiblic pat pod ip from %s to %s with traceID: %s",
					s.net.PublicVpcNATConfigure[i].PatContainerIP, updatedNATConfigure.PatContainerIP, traceID)
				s.net.PublicVpcNATConfigure[i] = updatedNATConfigure
				isUpdated = true
				// it will only update one public nat configure at the same time
				break
			}
		}
	}
	return false, isUpdated
}

func (s *Server) updateEndpointNATConfigures(changedNet *network.NetWork, traceID string) (bool, bool) {
	needReplaced, err := checkNATConfigures(changedNet.EndpointVpcNATConfigure)
	if err != nil {
		log.GetLogger().Errorf("failed to check endpoint nat update scenario with traceID: %s, err: %s",
			traceID, err.Error())
		return false, false
	}
	// replace scenario - all nat configures are new created
	if needReplaced {
		s.net.EndpointVpcNATConfigure = make([]types.NATConfigure, len(changedNet.EndpointVpcNATConfigure))
		for i := range changedNet.EndpointVpcNATConfigure {
			s.net.EndpointVpcNATConfigure = append(s.net.EndpointVpcNATConfigure, changedNet.EndpointVpcNATConfigure[i])
			log.GetLogger().Infof("succeed to replace endpoint nat by new pat pod name: %s and pat pod ip: %s "+
				"with traceID: %s", changedNet.EndpointVpcNATConfigure[i].PatPodName,
				changedNet.EndpointVpcNATConfigure[i].PatContainerIP, traceID)
		}
		return true, false
	}
	// update scenario - one nat configure is new created, and the other is deleted
	isUpdated := false
	deletedEndpointPatPodName := ""
	var updatedEndpointConfigure types.NATConfigure
	// currently, one-to-one endpoint nat configure is supported
	// it means one deleted endpoint nat pat service pod and one updated endpoint pat service pod
	for i := range changedNet.EndpointVpcNATConfigure {
		if changedNet.EndpointVpcNATConfigure[i].IsDeleted {
			deletedEndpointPatPodName = changedNet.EndpointVpcNATConfigure[i].PatPodName
			log.GetLogger().Infof("deleted endpoint pat pod name: %s and pat pod ip: %s with traceID: %s",
				deletedEndpointPatPodName, changedNet.EndpointVpcNATConfigure[i].PatContainerIP, traceID)
		} else {
			updatedEndpointConfigure = changedNet.EndpointVpcNATConfigure[i]
			log.GetLogger().Infof("updated endpoint pat pod name: %s and pat pod ip: %s with traceID: %s",
				changedNet.EndpointVpcNATConfigure[i].PatPodName,
				changedNet.EndpointVpcNATConfigure[i].PatContainerIP, traceID)
		}
	}
	if deletedEndpointPatPodName != "" {
		for i := range s.net.EndpointVpcNATConfigure {
			if s.net.EndpointVpcNATConfigure[i].PatPodName == deletedEndpointPatPodName {
				log.GetLogger().Infof("succeed to update endpoint pat pod ip from %s to %s with traceID: %s",
					s.net.EndpointVpcNATConfigure[i].PatContainerIP, updatedEndpointConfigure.PatContainerIP, traceID)
				s.net.EndpointVpcNATConfigure[i] = updatedEndpointConfigure
				isUpdated = true
				// it will only update one endpoint vpc nat configure at the same time
				break
			}
		}
	}
	return false, isUpdated
}

func (s *Server) updateNATConfigure(changedNet *network.NetWork, traceID string) (bool, error) {
	isNATReplaced, isNATUpdated, isEndpointNATReplaced, isEndpointNATUpdated := false, false, false, false
	if len(changedNet.NATConfigures) == multiplePatNumber && len(s.net.NATConfigures) > 0 {
		log.GetLogger().Infof("receive 2 pats of func vpc are %v, current is %v with traceID %s, old is %v",
			changedNet.NATConfigures, s.net.CurrentNATConfigure, traceID, s.net.NATConfigures)
		isNATReplaced, isNATUpdated = s.updateNATConfigures(changedNet, traceID)
	}
	if len(changedNet.EndpointVpcNATConfigure) == multiplePatNumber && len(s.net.EndpointVpcNATConfigure) > 0 {
		log.GetLogger().Infof("receive 2 pats of endpoint are %v, current is %v with traceID %s, old is %v",
			changedNet.EndpointVpcNATConfigure, s.net.CurrentNATConfigure, traceID, s.net.EndpointVpcNATConfigure)
		isEndpointNATReplaced, isEndpointNATUpdated = s.updateEndpointNATConfigures(changedNet, traceID)
	}
	isPublicNATReplaced, isPublicNATUpdated := false, false
	if len(changedNet.PublicVpcNATConfigure) == multiplePatNumber && len(s.net.PublicVpcNATConfigure) > 0 {
		log.GetLogger().Infof("receive 2 pats of public are %v, current is %v with traceID %s, old is %v",
			changedNet.PublicVpcNATConfigure, s.net.CurrentNATConfigure, traceID, s.net.PublicVpcNATConfigure)
		isPublicNATReplaced, isPublicNATUpdated = s.updatePublicNATConfigures(changedNet, traceID)
	}
	if isNATReplaced || isEndpointNATReplaced || isPublicNATReplaced {
		ctx := s.cancelAndCreateNewContext()
		prePatName := s.net.CurrentNATConfigure.PatPodName
		if err := changeNetWork(ctx, s.net, nil); err != nil {
			return true, fmt.Errorf("failed to change network, err:%s", err.Error())
		}
		curPatName := s.net.CurrentNATConfigure.PatPodName
		log.GetLogger().Infof("succeed to replace vpc pat pod, current patName: %s, "+
			"previous patName: %s, traceID: %s", curPatName, prePatName, traceID)
	}
	if s.conf.IsSafety {
		s.resetWorkerDNSRoute(s.net.CurrentNATConfigure.PatContainerIP)
	}
	log.GetLogger().Debugf("succeed to update nat configure, isNATReplaced: %t, isNATUpdated: %t, "+
		"isEndpointNATReplaced: %t, isEndpointNATUpdated: %t, isPublicNATReplaced: %t, isPublicNATUpdated: %t,"+
		" with traceID: %s", isNATReplaced, isNATUpdated, isEndpointNATReplaced, isEndpointNATUpdated,
		isPublicNATReplaced, isPublicNATUpdated, traceID)
	return isNATReplaced || isNATUpdated || isEndpointNATReplaced || isEndpointNATUpdated || isPublicNATReplaced ||
		isPublicNATUpdated, nil
}

func parseNetWork(r *http.Request) (*network.NetWork, error) {
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to parse network with error: %s", err.Error())
		return nil, err
	}

	var msg types.FunctionSpecRequest
	if err := json.Unmarshal(b, &msg); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the json, error: %s", err.Error())
		return nil, err
	}
	log.GetLogger().Infof("NATConfigure %v EndpointVpcNATConfigure %v, PublicVpcNATConfigure %v,"+
		" NetworkController %v", msg.Message.NATConfigure, msg.Message.EndpointVpcNATConfigure,
		msg.Message.PublicVpcNATConfigure, msg.Message.Function.NetworkController)
	return &network.NetWork{
		NATConfigures:           msg.Message.NATConfigure,
		EndpointVpcNATConfigure: msg.Message.EndpointVpcNATConfigure,
		PublicVpcNATConfigure:   msg.Message.PublicVpcNATConfigure,
		RouteInfo:               msg.Message.Function.RounteInfo,
	}, nil
}

func (s *Server) changePatPodIP(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	changedNet, err := parseNetWork(r)
	if err != nil {
		log.GetLogger().Errorf("failed to parse network from request with traceID: %s and error: %s",
			traceID, err.Error())
		writeResponse(w, err)
		return
	}
	// update server net
	isUpdated, err := s.updateNATConfigure(changedNet, traceID)
	if err != nil {
		log.GetLogger().Warnf("failed to update network from request with traceID: %s and error: %s",
			traceID, err.Error())
		writeResponse(w, err)
		return
	}
	log.GetLogger().Infof("succeed to update network from request with isUpdated: %t traceID: %s", isUpdated, traceID)
	writeResponse(w, nil)
}

func (s *Server) notifyFunction(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	idleMode := r.Header.Get(constants.HeaderIdleMode)
	billingSwitch := r.Header.Get(constants.HeaderBillingSwitch)
	if idleMode != "" {
		runtime.SetIdleMode(r.Header.Get(constants.HeaderIdleMode) == "true")
		log.GetLogger().Infof("succeed to load idle mode: %t with traceID: %s", runtime.GetIdleMode(), traceID)
	}
	if billingSwitch != "" {
		if billingSwitch == "true" {
			// periodStartTime set time.Now() when billingSwitch is true
			invoke.StartReservedInstanceFee()
		} else if billingSwitch == "false" {
			// periodStartTime set 0 when billingSwitch is false
			invoke.StopReservedInstanceFee()
		} else {
			log.GetLogger().Warnf("unknown billing switch %s with %s", billingSwitch, traceID)
		}
	}
	writeResponse(w, nil)
}

func fixContext(r *http.Request) (*http.Request, func()) {
	rctx := r.Context()
	ctx, cancel := context.WithCancel(context.Background())
	finishCh := make(chan struct{})
	finish := func() { close(finishCh) }
	go func() {
		select {
		case <-rctx.Done():
			cancel()
			return
		case <-finishCh:
			// We no longer need to cancel the context after finish.
			return
		}
	}()
	return r.WithContext(ctx), finish
}

func (s *Server) loadFunction(w http.ResponseWriter, r *http.Request) {
	cache.ClearLoadErr()
	var errRsp error
	timeInfo := fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
		constants.ErrorDuration, constants.ErrorDuration)
	r, finish := fixContext(r)
	defer func() {
		if runtime.PodApplier == constants.WorkerManagerApplier {
			invoke.SetReservedInstanceStartTime()
		}
		w.Header().Set(constants.HeaderLoadDuration, timeInfo)
		w.Header().Set(constants.HeaderDirectRuntime, strconv.FormatBool(utils.IsDirectRuntime))
		deploy.SetPreloadResult(w)

		if errRsp != nil {
			writeLoadFunctionResponse(w, errRsp)
		} else {
			writeLoadFunctionResponse(w, cache.GetLoadErr())
		}

		finish()
		runtime.LogMetrics(r.Header.Get(constants.HeaderMetricsTick))

		if commonutils.IsCAEFunc(utils.GetBusinessType()) {
			// report resource usage to bus, like cpu usage rate and memory usage rate
			runtime.RegisterResourceUsageCallBack(s.invokeServer.ReportResourceUsage)
			runtime.ReportResourceUsage(r.Header.Get(constants.HeaderResourceUsageTick))

			runtime.RegisterReportCaeMetricsCallBack(s.invokeServer.ReportCaeMetrics)
			runtime.ReportCaeMetrics(r.Header.Get(constants.HeaderCaeMetricsTick),
				s.conf.RuntimeConfig.ResourceMetaData.EnableTmpExpansion)
		}
		if r.Header.Get(constants.HeaderBridgeFuncFlag) == "true" {
			runtime.ReportBridgeFuncMetrics(s.conf.RuntimeConfig, s.invokeServer.ReportBridgeFuncMetrics)
		}
	}()

	deploy.SetPreloadEnable(r.Header)
	traceID := r.Header.Get(constants.HeaderTraceID)
	runtime.PodApplier = r.Header.Get(constants.HeaderPodApplier)
	if commonutils.IsCAEFunc(utils.GetBusinessType()) {
		runtime.CAEPodApplier = runtime.PodApplier
	}
	runtime.SetIdleMode(r.Header.Get(constants.HeaderIdleMode) == "true")
	functionType := r.Header.Get(constants.FunctionLifecycleType)
	// Stateful functions and CAE functions are configured as reserved instances, but not real reserved instances.
	if functionType == constants.PersistenceFunction {
		log.GetLogger().Infof("Set the stateful and cae function to the reserved mode.")
		runtime.PodApplier = constants.WorkerManagerApplier
		runtime.SetIdleMode(false)
	}
	if runtime.PodApplier == constants.WorkerManagerApplier {
		runtime.MetricsSourceName = constants.ReservedInstanceMetricsTimeWindow
		runtime.SetReservedInstanceModeInCurrentTimeWindow()
	}

	suicideTime, err := strconv.Atoi(r.Header.Get(constants.XSuicideTimeoutConfig))
	if err == nil {
		runtime.SetSuicideTimeout(suicideTime)
	}
	hasCompareAndSet := status.CompareAndSet(status.Waiting, status.Initializing)
	if hasCompareAndSet && s.conf.IsLoaded != true {
		log.GetLogger().Infof("load function with idle mode: %t, runningMode: %s, traceID: %s", runtime.GetIdleMode(),
			s.conf.RunningMode, traceID)
		if timeInfo, errRsp = s.deployAndLoad(r); errRsp != nil {
			status.Set(status.Waiting)
			return
		}
		if !(s.conf.Language == worker.CustomContainerLanguage) {
			if errRsp = s.recordLoaded(); errRsp != nil {
				status.Set(status.Waiting)
				log.GetLogger().Errorf("failed to record Loaded, error: %s", errRsp.Error())
				return
			}
		}
		status.Set(status.Ready)
		s.conf.IsLoaded = true
	} else {
		if hasCompareAndSet {
			status.Set(status.Waiting)
		}
		log.GetLogger().Warnf("repeated loading the function, traceID: %s", traceID)
		errRsp = fmt.Errorf("repeated loading")
		return
	}
	go func() {
		if err := persistConfig(s.conf); err != nil {
			log.GetLogger().Errorf("failed to update the config, error: %s", err.Error())
		}
	}()
	runtime.RunMetrics.IdleCheckSuicide()
}

func writeLoadFunctionResponse(w http.ResponseWriter, errRsp error) {
	if fusion.GetRestoreDead() {
		writeRestoreDeadResponse(w, errRsp)
	} else {
		writeResponse(w, errRsp)
	}
}

func writeRestoreDeadResponse(w http.ResponseWriter, errRsp error) {
	badResponse := snerror.BadResponse{}
	if errRsp == nil {
		w.WriteHeader(http.StatusOK)
		badResponse.Code = snerror.RestoreDeadErrorCode
		badResponse.Message = snerror.ErrText(snerror.RestoreDeadErrorCode)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		badResponse.Code = snerror.RestoreDeadErrorCode
		badResponse.Message = errRsp.Error()
	}
	rsp, err := json.Marshal(badResponse)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal badResponse, error: %s", err.Error())
		return
	}
	_, err = w.Write(rsp)
	if err != nil {
		log.GetLogger().Errorf("failed to write the response, error: %s", err.Error())
	}
	return
}

func (s *Server) checkpointFunction(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	if status.CompareAndSet(status.Ready, status.Checkpointing) {
		// Checkpointing
		log.GetLogger().Infof("%s | start to checkpoint runtime process", traceID)
		s.net.AddExcludeSubnetRouteToUploadSnapshot()
		go s.invokeServer.CheckpointRuntimeProcess(s.deployer, traceID)
	}
	curStatus := status.Get()
	if curStatus == status.CheckpointFailed {
		w.WriteHeader(http.StatusInternalServerError)
		errRes := snerror.New(snerror.WorkerInternalErrorCode, snapshot.GetCheckpointError())
		_, err := w.Write([]byte(errRes.Error()))
		if err != nil {
			log.GetLogger().Errorf("%s | failed to write the snapshot response, error: %s", traceID, err.Error())
		}
		return
	}
	if curStatus != status.CheckpointReady {
		log.GetLogger().Infof("%s | checkpoint is not finished, please wait", traceID)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("checkpoint is not finished"))
		if err != nil {
			log.GetLogger().Errorf("%s | failed to write the snapshot response, error: %s", traceID, err.Error())
		}
		return
	}
	log.GetLogger().Infof("%s | checkpoint is ready, return snapshot info", traceID)
	snapshotInfo := snapshot.GetSnapshotInfo()
	resByte, err := json.Marshal(snapshotInfo)
	if err != nil {
		writeResponse(w, err)
		return
	}
	log.GetLogger().Infof("%s | upload snapshot info: %s", traceID, resByte)

	w.WriteHeader(http.StatusOK)
	w.Header().Add(constants.ContentType, constants.ApplicationJSON)
	_, err = w.Write(resByte)
	if err != nil {
		log.GetLogger().Errorf("%s | failed to write the snapshot response,error: %s", traceID, err.Error())
	}
}

func (s *Server) checkAuth(r *http.Request) bool {
	requestSign := r.Header.Get(constants.HeaderAuthorization)
	timestamp := r.Header.Get(constants.HeaderAuthTimestamp)
	err := localauth.AuthCheckLocally(s.conf.AKey, s.conf.SKey, requestSign, timestamp, s.conf.Duration)
	if err != nil {
		log.GetLogger().Errorf("failed to check authorization locally, error: %s", err.Error())
		return false
	}
	return true
}

func (s *Server) changeNetWorkEx(ctx context.Context, net *network.NetWork,
	netWorkCh chan error, getCodeFinishCh chan struct{}) {
	s.waitBeforeChangeNetwork(net)
	defer s.SendToNatInitialChan()
	err := changeNetWork(ctx, net, getCodeFinishCh)
	if netWorkCh != nil {
		if err != nil {
			netWorkCh <- fmt.Errorf("change network err: %s", err.Error())
			return
		}
		netWorkCh <- nil
	}
}

func (s *Server) waitBeforeChangeNetwork(net *network.NetWork) {
	if (len(net.NATConfigures) == 0 && len(net.EndpointVpcNATConfigure) == 0 && len(net.PublicVpcNATConfigure) != 0) &&
		!s.invokeServer.NetworkController.DisablePublicNetwork &&
		!s.invokeServer.NetworkController.DisableWaitBeforeChangeNet {
		timer := time.NewTimer(waitSecondsBeforeChangeNetwork)
		defer timer.Stop()
		select {
		case <-s.invokeServer.DeployCodeFinishedChan:
		case <-timer.C:
			log.GetLogger().Warn("deploy code timeout, continue change network")
		}
	}
}

func (s *Server) prepareCaePoolPodUserMountFiles() error {
	if !s.conf.UsePoolPod || s.conf.RunningMode == constants.RunningModeCheckpoint {
		return nil
	}
	log.GetLogger().Infof("prepare cae user mount files for %s", s.conf.URN)
	mountFiles := s.conf.FileMountConfig.MountFiles
	if s.conf.FileMountConfig.S3Config != nil && len(mountFiles) > 0 {
		deployer, err := obs.NewDeployer(s.conf.FileMountConfig.S3Config)
		if err != nil {
			log.GetLogger().Errorf("failed to create obs deployer:%s", err.Error())
			return err
		}
		for _, mountFile := range mountFiles {
			if len(mountFile.FilePath) == 0 {
				continue
			}
			data, err := deployer.DownLoadFromObs(mountFile.BucketID, mountFile.ObjectID)
			if err != nil {
				log.GetLogger().Errorf("failed to download file: bucketID %s, objectId %s from obs:%s",
					mountFile.BucketID, mountFile.ObjectID, err.Error())
				return fmt.Errorf("failed to download file %s", mountFile.FilePath)
			}
			if len(data) > mountFileLimit {
				return fmt.Errorf("mount file %s size is larger than %d", mountFile.FilePath, mountFileLimit)
			}
			fullPath := path.Join(workerRuntimeShareDir, mountFile.FilePath)
			if err := commonutils.WriteFileToPathWithMode(fullPath, data, mountFilePerm, mountFilePerm); err != nil {
				log.GetLogger().Errorf("write content to %s failed, reason:%s", mountFile.FilePath, err.Error())
				return err
			}
		}
	}
	if len(s.conf.FileMountConfig.SecretFiles) > 0 {
		for _, secretFile := range s.conf.FileMountConfig.SecretFiles {
			if len(secretFile.FileMountPath) == 0 || len(secretFile.FileMountPath) == 0 {
				continue
			}
			data, err := localauth.Decrypt(secretFile.EncryptedFileContent)
			if err != nil {
				log.GetLogger().Errorf("decrypt content of %s failed:%s", secretFile.FileMountPath, err.Error())
				return err
			}
			fullPath := path.Join(workerRuntimeShareDir, secretFile.FileMountPath)
			if err := commonutils.WriteFileToPathWithMode(fullPath, data, mountFilePerm, mountFilePerm); err != nil {
				log.GetLogger().Errorf("write content to %s failed, reason:%s", secretFile.FileMountPath, err.Error())
				return err
			}
		}
	}
	return nil
}

func (s *Server) doCodeDeploy(getCodeFinishCh chan struct{}, language string) {
	_, err := s.deployer.Deploy(s.conf, getCodeFinishCh, language)
	if err != nil {
		log.GetLogger().Warnf("deploy code failed:%s", err.Error())
	} else {
		log.GetLogger().Infof("deploy code success")
	}
	closeDeployChanOnce.Do(func() {
		close(codeDeployChan)
	})
}

func (s *Server) doRestoreModeDeploy(getCodeFinishCh chan struct{}, language string) (*deploy.DurationInfo, error) {
	if s.conf.UsePoolPod {
		defer obs.ClearSecretKeyMemory()
		go s.doCodeDeploy(make(chan struct{}), language)
	}
	duration, err := s.deployer.DeploySnapshot(s.conf, getCodeFinishCh, language)
	if err != nil {
		log.GetLogger().Infof("deploy snapshot failed:%s", err.Error())
		duration = &deploy.DurationInfo{
			DeployFuncTime:  constants.ErrorDuration,
			DeployLayerTime: constants.ErrorDuration,
		}
	}
	waitCodeDeployOver(s.conf)
	return duration, err
}

func waitCodeDeployOver(conf *config.Configuration) {
	if conf.UsePoolPod {
		timer := time.NewTimer(time.Second * deployCodeTimeout)
		defer timer.Stop()
		select {
		case <-timer.C:
			log.GetLogger().Warn("wait code deploy timeout")
		case <-codeDeployChan:
			log.GetLogger().Info("code deploy finished")
		}
	}
}

func (s *Server) deploy(deployMessageChan chan *deploy.Message, getCodeFinishCh chan struct{}, language string) {
	var err error
	var d = &deploy.DurationInfo{}
	log.GetLogger().Infof("begin to deploy the function with running mode %s, business type %s, language %s",
		s.conf.RunningMode, utils.GetBusinessType(), language)
	utils.DisableOOM()
	defer utils.ResetOOMScoreAdjToDefault()
	defer s.SendToDeployCodeChan()
	if s.conf.RunningMode == constants.RunningModeRestore {
		d, err = s.doRestoreModeDeploy(getCodeFinishCh, language)
	} else if !commonutils.IsCAEFunc(utils.GetBusinessType()) || s.conf.UsePoolPod {
		d, err = s.deployer.Deploy(s.conf, getCodeFinishCh, language)
		if err != nil {
			if _, ok := err.(snerror.SNError); !ok {
				err = fmt.Errorf("deploy function error: %s", err.Error())
			}
			if deployMessageChan != nil {
				deployMessageChan <- &deploy.Message{Err: err}
			}
			return
		}
		s.conf.DurationInfo.GetCodeAndDecompressDuration = d.DeployFuncTime
	}
	s.conf.LoadTime = d.DeployFuncTime.Milliseconds()
	log.GetLogger().Infof("deploy the function successfully with running mode %s, business type %s, "+
		"language %s, cost %d ms", s.conf.RunningMode, utils.GetBusinessType(), language, s.conf.LoadTime)
	if err := utils.RestoreCPULimit(); err != nil {
		log.GetLogger().Errorf("failed to restore CPU limit with error %s", err.Error())
		if deployMessageChan != nil {
			deployMessageChan <- &deploy.Message{
				Duration: d,
				Err:      err,
			}
		}
		return
	}
	if !s.changeNetworkToStreamHandler(deployMessageChan, d) {
		return
	}
	if deployMessageChan != nil {
		deployMessageChan <- &deploy.Message{
			Duration: d,
			Err:      nil,
		}
	}
}

func (s *Server) changeNetworkToStreamHandler(deployMessageChan chan *deploy.Message, d *deploy.DurationInfo) bool {
	if s.conf.IsStreamEnable && strings.Contains(s.conf.Language, "go") {
		if err := network.ChangeNetworkToStreamHandler(streamHandlerHostCidr, streamHandlerServicePort); err != nil {
			if deployMessageChan != nil {
				deployMessageChan <- &deploy.Message{
					Duration: d,
					Err:      err,
				}
			}
			return false
		}
	}
	if s.conf.StreamResponse {
		if err := network.ChangeNetworkToDesPort(constants.StreamTransPort); err != nil {
			if deployMessageChan != nil {
				deployMessageChan <- &deploy.Message{
					Duration: d,
					Err:      err,
				}
			}
			return false
		}
	}
	return true
}

func (s *Server) deployAndLoad(r *http.Request) (string, error) {
	var err error
	s.net, err = s.readRequest(r)
	if err != nil {
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration, constants.ErrorDuration), err
	}

	if len(s.conf.LogPolicy.LogPaths) > 0 {
		log.GetLogger().Infof("start to write log config file, logPolicy: %+v", s.conf.LogPolicy)
		if err = s.writeLogConfigFile(); err != nil {
			return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
				constants.ErrorDuration, constants.ErrorDuration), err
		}
		go updateModifyTime()
	}

	s.net.Derange()
	s.invokeServer.IsVPCFunc = s.net.NeedChange(s.net.NATConfigures) || s.net.NeedChange(s.net.EndpointVpcNATConfigure)

	// In case of Custom Container, the user code has been already contained in image, so the process doesn't
	// involve code deployment.
	if s.conf.Language == worker.CustomContainerLanguage {
		return s.deployAndLoadCustomContainer(r)
	}

	ctx := s.cancelAndCreateNewContext()
	deployMes := make(chan *deploy.Message, 1)
	netWorkCh := make(chan error, 1)
	getCodeFinishCh := make(chan struct{}, 1)

	// if safety vpc function, start to change network until instance deployment and nat initialization completed
	if s.net.IsSafety {
		s.safetyVpcCfgSet(r)
		s.deployer.SetCacheManagerServiceIP(s.conf)
		go s.net.ChangeNetworkForSafeVpc()
	}
	err = s.prepareCaePoolPodUserMountFiles()
	if err != nil {
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration), fmt.Errorf("failed to prepare cae user mount files:%s", err.Error())
	}
	storage.SetConfig(utils.EnableTenantAuth(), utils.LocalFuncURN.TenantID,
		s.conf.Initializer.Extra[constants.HeaderIAMToken])
	go s.deploy(deployMes, getCodeFinishCh, s.conf.Language)
	go s.changeNetWorkEx(ctx, s.net, netWorkCh, getCodeFinishCh)

	s.conf.RuntimeConfig.StateID = r.Header.Get(constants.StateID)
	s.conf.ClusterID = r.Header.Get(constants.HeaderClusterID)
	s.conf.Initializer.Extra["StateID"] = r.Header.Get(constants.StateID)
	start := time.Now()
	duration := &deploy.DurationInfo{}
	functionlog.InitRuntimeLoggerRecoder(s.conf)
	if duration, err = s.invokeServer.LoadAndServe(r.Context(), s.conf, deployMes, netWorkCh); err != nil {
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration, constants.ErrorDuration), err
	}

	err = s.normalMountVolume()
	if err != nil {
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration, constants.ErrorDuration), err
	}
	// start to fsnotify function dump file path
	go s.dumpFileNotify()
	return fmt.Sprintf(LoadDurationFmt, duration.DeployFuncTime/time.Millisecond, duration.Decompress/time.Millisecond,
		duration.DeployLayerTime/time.Millisecond, time.Since(start)/time.Millisecond), nil
}

func (s *Server) safetyVpcCfgSet(r *http.Request) {
	cacheManagerServiceIP := r.Header.Get(constants.CacheManagerClusterID)
	dnsResolveAddr := r.Header.Get(constants.DNSResolveAddrKey)
	if net.ParseIP(cacheManagerServiceIP) == nil {
		log.GetLogger().Warnf("cache-manager service cluster ip: %s or dns resolve addr: %s is not valid",
			cacheManagerServiceIP, dnsResolveAddr)
		return
	}
	if s.conf == nil {
		log.GetLogger().Warnf("worker server conf is nil")
		return
	}
	s.conf.IsSafety = true
	s.conf.CacheManagerClusterID = cacheManagerServiceIP
	s.conf.DNSResolveAddr = dnsResolveAddr
	s.net.SafeDNSAddr = dnsResolveAddr
	s.net.CodeDeployChan = make(chan struct{}, 1)
	s.net.NatInitializeChan = make(chan struct{}, 1)

	log.GetLogger().Infof(
		"success to set safe vpc config, is_safety: %v, cache-manager cluster id: %s, dns resolve address: %s",
		s.conf.IsSafety, cacheManagerServiceIP, s.net.SafeDNSAddr)
	return
}

func (s *Server) resetWorkerDNSRoute(patPodIP string) {
	log.GetLogger().Infof("worker is safety vpc, reset worker dns route with pat pod ip: %s dns addr: %s",
		patPodIP, s.conf.DNSResolveAddr)
	dnsAddr := strings.Split(strings.Trim(s.conf.DNSResolveAddr, " "), ",")
	cmdList := make([]*exec.Cmd, 0)
	cmdList = append(cmdList, exec.Command("ip", "route", "del", defaultDestination))

	for _, addr := range dnsAddr {
		if net.ParseIP(strings.Trim(addr, " ")) == nil {
			log.GetLogger().Warnf("worker config dns server address: %s is not valid", dnsAddr)
			continue
		}
		cmdList = append(cmdList, exec.Command("ip", "route", "del", addr))
		cmdList = append(cmdList, exec.Command("ip", "route", "add", addr,
			"via", patPodIP, "dev", vpcInterfaceName, defaultOnlink))
	}
	for _, cmd := range cmdList {
		if err := cmd.Run(); err != nil {
			log.GetLogger().Warnf("execute command: %s failed, error: %s",
				strings.Join(cmd.Args, " "), err.Error())
		}
	}
	return
}

func (s *Server) writeLogConfigFile() error {
	err := os.MkdirAll(worker.LogConfigFilePath, fs.ModePerm)
	if err != nil {
		log.GetLogger().Errorf("failed to mkdir of log config file")
		return err
	}
	file, err := os.Create(fmt.Sprintf("%s/%s", worker.LogConfigFilePath, worker.LogConfigFileName))
	if err != nil {
		log.GetLogger().Errorf("failed to create log config file, err: %s", err.Error())
		return err
	}
	defer file.Close()

	logConfig := config.LogConfig{
		LogPath:            s.constructLogPath(),
		PodName:            os.Getenv("PodName"),
		EnableMultiCollect: s.conf.LogPolicy.EnableMultiCollect,
		MultiPattern:       s.conf.LogPolicy.MultiPattern,
		LogGroupID:         s.conf.LogPolicy.LogGroupID,
		LogStreamID:        s.conf.LogPolicy.LogStreamID,
		ProjectID:          utils.LocalFuncURN.TenantID,
		Properties:         s.conf.Properties,
		PodDelete:          false,
		ContainerID:        s.conf.RuntimeContainerID,
		ModifyTime:         time.Now().Unix(),
	}
	data, err := json.Marshal(logConfig)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal logConfig, err: %s", err.Error())
		return err
	}

	_, err = file.Write(data)
	if err != nil {
		log.GetLogger().Errorf("failed to write data to log config file, err: %s", err.Error())
		return err
	}

	err = file.Chmod(logConfigFileMode)
	if err != nil {
		log.GetLogger().Errorf("failed to change mode of log config file, err: %s", err.Error())
		return err
	}

	return nil
}

func updateModifyTime() {
	log.GetLogger().Infof("start to update modifyTime")
	ticker := time.NewTicker(updateModifyTimeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			updateLogConfigFile(false)
		}
	}
}

func (s *Server) constructLogPath() []string {
	var logPath []string
	for _, runtimeLogPath := range s.conf.LogPolicy.LogPaths {
		logPath = append(logPath, fmt.Sprintf("userfunc%s", runtimeLogPath))
	}
	logPath = append(logPath, fmt.Sprintf("userfunc%s/*.log", constants.CAEMountUserLogPath))
	return logPath
}

func (s *Server) deployAndLoadCustomContainer(r *http.Request) (string, error) {
	var err error
	deployMes := make(chan *deploy.Message, 1)
	err = s.addCustomContainerFinalizer()
	if err != nil {
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration, constants.ErrorDuration), err
	}
	if s.customContainerPreError != nil && s.customContainerPreError.Error() == failedGetRuntimeContainerID {
		log.GetLogger().Warnf("custom container pre error: %s", failedGetRuntimeContainerID)
		s.skipWaitRuntimeContainerID = true
		if err = s.executeMount(context.TODO()); err != nil {
			return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
				constants.ErrorDuration, constants.ErrorDuration), err
		}
		s.customContainerPreError = nil
		s.customContainerPreFinished = true
	}
	if s.customContainerPreError != nil {
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration, constants.ErrorDuration), s.customContainerPreError
	}
	if utils.NeedCustomContainerPreLoad() && !s.customContainerPreFinished {
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration), snerror.New(snerror.RuntimeMountErrorCode, "failed to exec mount command")
	}
	if err := utils.RestoreCPULimit(); err != nil {
		log.GetLogger().Errorf("failed to restore CPU limit with error %s", err.Error())
		return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
			constants.ErrorDuration, constants.ErrorDuration), err
	}
	if s.conf.StreamResponse {
		if err := network.ChangeNetworkToDesPort(constants.StreamTransPort); err != nil {
			return fmt.Sprintf(LoadDurationFmt, constants.ErrorDuration, constants.ErrorDuration,
				constants.ErrorDuration, constants.ErrorDuration), err
		}
	}
	start := time.Now()
	deployMes <- &deploy.Message{
		Duration: nil,
		Err:      nil,
	}
	if _, err = s.invokeServer.LoadAndServe(r.Context(), s.conf, deployMes, nil); err != nil {
		return fmt.Sprintf(LoadDurationFmt, 0, constants.ErrorDuration, 0, constants.ErrorDuration), err
	}
	return fmt.Sprintf(LoadDurationFmt, 0, 0, 0, time.Since(start)/time.Millisecond), nil
}

func (s *Server) prepareCustomContainer() {
	err := s.GetCustomContainerInfo()
	if err != nil {
		log.GetLogger().Warnf("failed to get custom container info: %s", err.Error())
		s.customContainerPreError = err
		return
	}
	ctx := s.cancelAndCreateNewContext()
	if err = changeNetWork(ctx, s.net, nil); err != nil {
		log.GetLogger().Warnf("failed to change network: %s", err.Error())
		s.customContainerPreError = err
		return
	}
	if len(s.conf.FuncMountConfig.FuncMounts) == 0 {
		s.customContainerPreFinished = true
		return
	}
	s.customContainerPreError = errors.New(failedGetRuntimeContainerID)
	err = s.waitRuntimeContainerID()
	if err != nil {
		return
	}
	err = s.executeMount(ctx)
	if err != nil {
		log.GetLogger().Errorf("failed to execute mount: %s", err.Error())
		s.customContainerPreError = err
		return
	}
	s.customContainerPreFinished = true
	return
}

func (s *Server) waitRuntimeContainerID() error {
	for i := 0; i < defaultRetryTimes; i++ {
		if s.skipWaitRuntimeContainerID {
			log.GetLogger().Warnf("receive skip wait runtime container ID signal")
			return errors.New(failedGetRuntimeContainerID)
		}
		if s.runtimeContainerID != "" {
			log.GetLogger().Infof("success get runtime container ID: %s by informer", s.runtimeContainerID)
			s.conf.RuntimeContainerID = s.runtimeContainerID
			s.customContainerPreError = nil
			break
		}
		if i == defaultRetryTimes-1 {
			log.GetLogger().Errorf(failedGetRuntimeContainerID)
			return errors.New(failedGetRuntimeContainerID)
		}
		time.Sleep(defaultRetryInterval)
	}
	return nil
}

func (s *Server) executeMount(ctx context.Context) error {
	cli, err := engine.NewContainerClient()
	if err != nil {
		log.GetLogger().Errorf("failed to create container client: %s", err.Error())
		return err
	}

	if err = s.mountVolume(cli); err != nil {
		log.GetLogger().Errorf("failed to mount volume: %s", err.Error())
		return err
	}
	return nil
}

func (s *Server) addCustomContainerFinalizer() error {
	cli, err := engine.NewContainerClient()
	if err != nil {
		log.GetLogger().Errorf("failed to create container client: %s", err.Error())
		return err
	}

	finalizer := func() error {
		defer cli.Close()

		runtime.BackgroundProcessLimit.RecoverRuntimeProcess(s.conf.RuntimeContainerID, true)
		log.GetLogger().Infof("begin to recover and kill runtime when worker exit")
		if err := cli.Kill(context.TODO(), s.conf.RuntimeContainerID); err != nil {
			log.GetLogger().Errorf("failed to kill container: %s, %s", s.conf.RuntimeContainerID, err.Error())
			return err
		}
		log.GetLogger().Infof("recover and kill runtime when worker exit finished")
		return nil
	}
	s.finalizers = append(s.finalizers, finalizer)
	return nil
}

func getPatPodInfoFromEnv() ([]types.NATConfigure, []types.NATConfigure, []types.NATConfigure, error) {
	var err error
	var netConfigures []types.NATConfigure
	var endpointVpcNATConfigure []types.NATConfigure
	var publicVpcNATConfigure []types.NATConfigure
	if patPodInfoStr := os.Getenv(constants.NATConfigures); patPodInfoStr != "" {
		err = json.Unmarshal([]byte(patPodInfoStr), &netConfigures)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal patPodInfoStr: %s", err.Error())
			return []types.NATConfigure{}, []types.NATConfigure{}, []types.NATConfigure{}, err
		}
	}
	if endpointTalentPatPodInfoStr := os.Getenv(constants.EndpointVpcNATConfigure); endpointTalentPatPodInfoStr != "" {
		err = json.Unmarshal([]byte(endpointTalentPatPodInfoStr), &endpointVpcNATConfigure)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal endpointTalentPatPodInfoStr: %s", err.Error())
			return []types.NATConfigure{}, []types.NATConfigure{}, []types.NATConfigure{}, err
		}
	}
	if publicVpcConfigStr := os.Getenv(constants.PublicNetworkVpcNATConfigure); publicVpcConfigStr != "" {
		err = json.Unmarshal([]byte(publicVpcConfigStr), &publicVpcNATConfigure)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal public networ pat pod info: %s", err.Error())
			return []types.NATConfigure{}, []types.NATConfigure{}, []types.NATConfigure{}, err
		}
	}
	return netConfigures, endpointVpcNATConfigure, publicVpcNATConfigure, nil
}

// GetCustomContainerInfo -
func (s *Server) GetCustomContainerInfo() error {
	netInfo := &network.NetWork{}
	netConfigures, endpointVpcNATConfigure, publicVpcNATConfigure, err := getPatPodInfoFromEnv()
	netInfo.NATConfigures = netConfigures
	netInfo.EndpointVpcNATConfigure = endpointVpcNATConfigure
	netInfo.PublicVpcNATConfigure = publicVpcNATConfigure

	RouteInfoStr := os.Getenv(constants.RouteInfo)
	var RouteInfo types.RouteInfoData
	err = json.Unmarshal([]byte(RouteInfoStr), &RouteInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal RouteInfo: %s", err.Error())
		return err
	}
	netInfo.RouteInfo = RouteInfo
	svcCidr := os.Getenv(constants.SvcCidr)
	netInfo.SvcCidr = svcCidr
	netInfo.PingHash = s.pingHash
	netInfo.Ping6Hash = s.ping6Hash
	s.net = netInfo

	if funcMountConfigStr := os.Getenv(constants.FuncMountConfig); funcMountConfigStr != "" {
		var funcMountConfig types.FuncMountConfig
		err = json.Unmarshal([]byte(funcMountConfigStr), &funcMountConfig)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal funcMountConfigStr: %s", err.Error())
			return err
		}
		s.conf.FuncMountConfig = funcMountConfig
	}
	return nil
}

func changeNetWork(ctx context.Context, net *network.NetWork, getCodeFinishCh chan struct{}) error {
	// if NATConfigures more than one,net is replaceable
	net.Replaceable = len(net.NATConfigures) > 1
	if len(net.NATConfigures) >= 1 {
		net.IPV6Enable = net.NATConfigures[0].IPV6PatGateway != ""
	}
	net.EndpointVpcReplaceable = len(net.EndpointVpcNATConfigure) > 1
	if len(net.EndpointVpcNATConfigure) >= 1 {
		net.IPV6Enable = net.EndpointVpcNATConfigure[0].IPV6PatGateway != ""
	}
	net.PublicVpcReplaceable = len(net.PublicVpcNATConfigure) > 1
	log.GetLogger().Infof("changeNetWork Replaceable %t, EndpointVpcReplaceable %t, PublicVpcReplaceable %t ",
		net.Replaceable, net.EndpointVpcReplaceable, net.PublicVpcReplaceable)
	configNumber := 0
	if err := changeNAT(net, configNumber, getCodeFinishCh); err != nil {
		return err
	}
	needChangeEndpoint := net.NeedChange(net.EndpointVpcNATConfigure)
	needFuncVpc := net.NeedChange(net.NATConfigures)
	needPublicVpc := net.NeedChange(net.PublicVpcNATConfigure)
	needPublicVpc = needPublicVpc && !needFuncVpc && !needChangeEndpoint
	go checkNAT(ctx, net, checkNATParam{
		configure:    net.NATConfigures,
		tunnelName:   funcTunnelName,
		configNumber: configNumber,
		replaceable:  net.Replaceable,
		needEndpoint: needChangeEndpoint,
	})
	go checkNAT(ctx, net, checkNATParam{
		configure:    net.EndpointVpcNATConfigure,
		tunnelName:   endpointTunnelName,
		configNumber: configNumber,
		replaceable:  net.EndpointVpcReplaceable,
		needEndpoint: needChangeEndpoint,
	})
	if needPublicVpc {
		go checkNAT(ctx, net, checkNATParam{
			configure:    net.PublicVpcNATConfigure,
			configNumber: configNumber,
			replaceable:  net.PublicVpcReplaceable,
			needPublic:   true,
		})
	}
	return nil
}

func setRuntimeMessage(rtCfg *config.Configuration, msg *types.FunctionSpecRequest) {
	rtCfg.DeploymentConfig.BucketID = msg.Message.Deployment.BucketID
	rtCfg.DeploymentConfig.ObjectID = msg.Message.Deployment.ObjectID
	rtCfg.DeploymentConfig.CodePlaintextKmsCipher = msg.Message.Deployment.CodePlaintextKmsCipher
	rtCfg.DeploymentConfig.Access = msg.Message.Deployment.Access
	rtCfg.DeploymentConfig.Secret = msg.Message.Deployment.Secret
	rtCfg.DeploymentConfig.SecurityToken = msg.Message.Deployment.SecurityToken
	rtCfg.DeploymentConfig.LayerInfo = msg.Message.Deployment.Layers
	rtCfg.DeploymentConfig.DeployDir = msg.Message.Deployment.DeployDir
	rtCfg.DeploymentConfig.CodeSize = msg.Message.Deployment.CodeSize

	rtCfg.RuntimeConfig.FunctionType = msg.Message.Function.FunctionType
	rtCfg.RuntimeConfig.EnableAuthInHeader = msg.Message.Function.EnableAuthInHeader
	rtCfg.RuntimeConfig.URN = msg.Message.Function.URN
	rtCfg.RuntimeConfig.EntryFile = msg.Message.Function.EntryFile
	rtCfg.RuntimeConfig.EnvInfo = msg.Message.Function.Environment
	rtCfg.RuntimeConfig.EnvPlaintextKmsCipher = msg.Message.Function.EnvPlaintextKmsCipher
	rtCfg.RuntimeConfig.EnvKey = msg.Message.Function.EnvKey
	rtCfg.RuntimeConfig.EncryptedUserData = msg.Message.Function.EncryptedUserData
	rtCfg.RuntimeConfig.HandlerName = msg.Message.Function.Handler
	rtCfg.RuntimeConfig.Timeout = msg.Message.Function.Timeout
	rtCfg.RuntimeConfig.Language = msg.Message.Function.Language
	rtCfg.RuntimeConfig.CodeSha256 = msg.Message.Function.CodeSha256
	rtCfg.RuntimeConfig.CodeSha512 = msg.Message.Function.CodeSha512
	rtCfg.RuntimeConfig.RuntimeContainerID = msg.Message.Function.RuntimeContainerID

	rtCfg.RuntimeConfig.ResourceMetaData.CPU = msg.Message.Function.ResourceMetaData.CPU
	rtCfg.RuntimeConfig.ResourceMetaData.Mem = msg.Message.Function.ResourceMetaData.Mem
	rtCfg.RuntimeConfig.ResourceMetaData.GpuMemory = msg.Message.Function.ResourceMetaData.GpuMemory
	rtCfg.RuntimeConfig.ResourceMetaData.EphemeralStorage = msg.Message.Function.ResourceMetaData.EphemeralStorage
	rtCfg.RuntimeConfig.ResourceMetaData.EnableTmpExpansion = msg.Message.Function.ResourceMetaData.EnableTmpExpansion
	rtCfg.RuntimeConfig.Initializer.InitializerHandler = msg.Message.Function.Initializer.Handler
	rtCfg.RuntimeConfig.Initializer.InitializerTimeout = msg.Message.Function.Initializer.Timeout
	rtCfg.RuntimeConfig.FuncMountConfig.FuncMountUser = msg.Message.Function.FuncMountConfig.FuncMountUser
	rtCfg.RuntimeConfig.FuncMountConfig.FuncMounts = msg.Message.Function.FuncMountConfig.FuncMounts
	rtCfg.RuntimeConfig.LogTankService = msg.Message.Function.LogTankService
	rtCfg.RuntimeConfig.TraceService = msg.Message.Function.TraceService
	rtCfg.RuntimeConfig.MaxStateSize = msg.Message.Function.MaxStateSize
	rtCfg.RuntimeConfig.Heartbeat.Handler = msg.Message.Function.Heartbeat.Handler
	rtCfg.RuntimeConfig.Heartbeat.Path = msg.Message.Function.Heartbeat.Path
	rtCfg.RuntimeConfig.Heartbeat.HealthEnable = msg.Message.Function.Heartbeat.HealthEnable
	rtCfg.RuntimeConfig.Heartbeat.PeriodSeconds = msg.Message.Function.Heartbeat.PeriodSeconds
	rtCfg.RuntimeConfig.Heartbeat.Timeout = msg.Message.Function.Heartbeat.Timeout
	rtCfg.RuntimeConfig.Heartbeat.FailureThreshold = msg.Message.Function.Heartbeat.FailureThreshold
	rtCfg.RuntimeConfig.Heartbeat.FirstCheckDelay = msg.Message.Function.Heartbeat.FirstCheckDelay
	rtCfg.RuntimeConfig.Heartbeat.SuccessThreshold = msg.Message.Function.Heartbeat.SuccessThreshold
	rtCfg.RuntimeConfig.StartupHeartbeat.Handler = msg.Message.Function.StartupHeartbeat.Handler
	rtCfg.RuntimeConfig.StartupHeartbeat.Path = msg.Message.Function.StartupHeartbeat.Path
	rtCfg.RuntimeConfig.StartupHeartbeat.HealthEnable = msg.Message.Function.StartupHeartbeat.HealthEnable
	rtCfg.RuntimeConfig.StartupHeartbeat.PeriodSeconds = msg.Message.Function.StartupHeartbeat.PeriodSeconds
	rtCfg.RuntimeConfig.StartupHeartbeat.Timeout = msg.Message.Function.StartupHeartbeat.Timeout
	rtCfg.RuntimeConfig.StartupHeartbeat.FailureThreshold = msg.Message.Function.StartupHeartbeat.FailureThreshold
	rtCfg.RuntimeConfig.StartupHeartbeat.FirstCheckDelay = msg.Message.Function.StartupHeartbeat.FirstCheckDelay
	rtCfg.RuntimeConfig.StartupHeartbeat.SuccessThreshold = msg.Message.Function.StartupHeartbeat.SuccessThreshold
	rtCfg.RuntimeConfig.UsePoolPod = msg.Message.Function.UsePoolPod
	rtCfg.RuntimeConfig.FileMountConfig = msg.Message.Function.FileMountConfig
	rtCfg.RuntimeConfig.CustomJavaArgs = msg.Message.Function.CustomJavaArgs
	rtCfg.RuntimeConfig.DNSDomainCfg = msg.Message.Function.DNSDomainCfg
	rtCfg.RuntimeConfig.DNSConfig = msg.Message.Function.DNSConfig
	rtCfg.RuntimeConfig.EndpointDomainCfg = msg.Message.Function.EndpointDomainCfg
	rtCfg.RuntimeConfig.DNSDomainNames = msg.Message.Function.DNSDomainNames
	rtCfg.RuntimeConfig.IsStreamEnable = msg.Message.Function.IsStreamEnable
	rtCfg.RuntimeConfig.StreamResponse = msg.Message.Function.StreamResponse
	rtCfg.RuntimeConfig.EnableClassIsolation = msg.Message.Function.EnableClassIsolation
	rtCfg.RuntimeConfig.EnableGCLog = msg.Message.Function.EnableGCLog
	rtCfg.RuntimeConfig.PreStop.Handler = msg.Message.Function.PreStop.Handler
	rtCfg.RuntimeConfig.PreStop.Timeout = msg.Message.Function.PreStop.Timeout
	rtCfg.RuntimeConfig.PreStop.Type = msg.Message.Function.PreStop.Type
	rtCfg.RuntimeConfig.CustomMetricsReport.Handler = msg.Message.Function.CustomMetricsReport.Handler
	rtCfg.RuntimeConfig.CustomMetricsReport.Path = msg.Message.Function.CustomMetricsReport.Path
	rtCfg.RuntimeConfig.CustomMetricsReport.CollectTime = msg.Message.Function.CustomMetricsReport.CollectTime
	rtCfg.RuntimeConfig.CustomMetricsReport.Timeout = msg.Message.Function.CustomMetricsReport.Timeout
	rtCfg.RuntimeConfig.MaxRequestBodySize = msg.Message.Function.MaxRequestBodySize
	rtCfg.RuntimeConfig.StreamContentType = msg.Message.Function.StreamContentType
	rtCfg.RuntimeConfig.EnableWebsocket = msg.Message.Function.EnableWebsocket

	rtCfg.RuntimeConfig.LogPolicy = msg.Message.Function.LogPolicy

	rtCfg.RuntimeConfig.Properties = msg.Message.Function.Properties
	rtCfg.RuntimeConfig.IsBridgeFunction = msg.Message.Function.IsBridgeFunction
	rtCfg.RuntimeConfig.LtsCustomTag = msg.Message.Function.LtsCustomTag
	rtCfg.RuntimeConfig.CaeLogPath = "/home/caelog"
	if rtCfg.RuntimeConfig.Language == worker.CustomContainerLanguage &&
		allowSetCustomContainerPort(msg.Message.Function.CustomContainerPort) {
		webConf := rtCfg.RMConfig.RuntimeConfigs[worker.WebRuntimeLanguage]
		webConf.Port = int32(msg.Message.Function.CustomContainerPort)
		rtCfg.RMConfig.RuntimeConfigs[worker.WebRuntimeLanguage] = webConf
		log.GetLogger().Infof("change custom-container port to %d", msg.Message.Function.CustomContainerPort)
	}

	rtCfg.FuncSignature = msg.Message.Function.FuncSignature
	setSnapshotConfig(rtCfg, msg)

	if rtCfg.SnapshotConfig.EnableSnapshot == "disable" {
		log.GetLogger().Infof("Enable snapshot is false. Do not set running mode to restore.")
		return
	}
	if rtCfg.SnapshotConfig.IsSnapshotExpire {
		log.GetLogger().Infof("Snapshot is expire. Do not set running mode to restore.")
		return
	}
	// expire or circuit_break
	if rtCfg.SnapshotConfig.SnapshotState == "circuit_breaking" {
		log.GetLogger().Infof("Snapshot is circuit breaking. Do not set running mode to restore.")
		return
	}

	if rtCfg.SnapshotConfig.SnapshotSize != 0 {
		rtCfg.RunningMode = constants.RunningModeRestore
	}
}

func allowSetCustomContainerPort(port int) bool {
	if port == 0 {
		return false
	}
	for _, value := range customUsedPort {
		if value == port {
			log.GetLogger().Warnf("failed to set already used port, port: %d", port)
			return false
		}
	}
	return true
}

func setSnapshotConfig(rtCfg *config.Configuration, msg *types.FunctionSpecRequest) {
	if msg.Message.SnapshotDeployment.FastStartEnable && commonutils.IsCAEFunc(utils.GetBusinessType()) {
		log.GetLogger().Infof("FastStartEnable is true.")
		rtCfg.SnapshotConfig.FastStartEnable = msg.Message.SnapshotDeployment.FastStartEnable
	}

	runtimeManagerTag := os.Getenv(constants.RuntimeManagerTag)
	if commonutils.IsCAEFunc(msg.Message.Function.BusinessType) && runtimeManagerTag == "" {
		log.GetLogger().Error("failed to get runtime manager tag")
		return
	}

	if !commonutils.IsCAEFunc(msg.Message.Function.BusinessType) ||
		strings.Contains(msg.Message.SnapshotDeployment.ObjectID, runtimeManagerTag) {
		setSnapshotConfBySnapshotVersion(rtCfg, msg, nil)
		return
	}

	if len(msg.Message.SnapshotDeployment.SnapshotVersions) == 0 {
		log.GetLogger().Info("the func is not configured with snapshot")
		return
	}

	matchedVersion := getMatchedSnapshotVersion(msg.Message.SnapshotDeployment, runtimeManagerTag)
	if matchedVersion.SnapshotSize == 0 {
		return
	}
	setSnapshotConfBySnapshotVersion(rtCfg, msg, &matchedVersion)
}

func getMatchedSnapshotVersion(snapshotInfo types.SnapshotDeploymentSpec,
	runtimeImageTag string) types.SnapshotVersion {
	if len(snapshotInfo.SnapshotVersions) == 0 {
		return types.SnapshotVersion{}
	}
	// prefer select the snapshot with the matching version.
	for _, version := range snapshotInfo.SnapshotVersions {
		if version.SnapshotImageTag == runtimeImageTag {
			return version
		}
	}

	javaRuntimeSize := os.Getenv(constants.RuntimeManagerSize)
	// If the snapshot does not match the version, select the snapshot with the same java runtime size.
	for _, version := range snapshotInfo.SnapshotVersions {
		if strconv.FormatInt(version.SnapshotCheckInfo.JavaRuntimeSize, uint64Base) == javaRuntimeSize {
			return version
		}
	}

	log.GetLogger().Errorf("failed to get runtime image tag %s or java runtime size %s matched snapshot version"+
		" from %v", runtimeImageTag, javaRuntimeSize, snapshotInfo)
	return types.SnapshotVersion{}
}

func setSnapshotConfBySnapshotVersion(rtCfg *config.Configuration, msg *types.FunctionSpecRequest,
	snapshotVersion *types.SnapshotVersion) {
	if snapshotVersion != nil {
		rtCfg.SnapshotConfig.SnapshotObjectID = snapshotVersion.ObjectID
		rtCfg.SnapshotConfig.SnapshotSha256 = snapshotVersion.SnapshotSha256
		rtCfg.SnapshotConfig.SnapshotSize = snapshotVersion.SnapshotSize
	} else {
		rtCfg.SnapshotConfig.SnapshotObjectID = msg.Message.SnapshotDeployment.ObjectID
		rtCfg.SnapshotConfig.SnapshotSha256 = msg.Message.SnapshotDeployment.SnapshotSha256
		rtCfg.SnapshotConfig.SnapshotSize = msg.Message.SnapshotDeployment.SnapshotSize
	}
	rtCfg.SnapshotConfig.SnapshotBucketID = msg.Message.SnapshotDeployment.BucketID
	rtCfg.SnapshotConfig.SnapshotBucketURL = msg.Message.SnapshotDeployment.BucketURL
	rtCfg.SnapshotConfig.RestoreHook.Handler = msg.Message.Function.RestoreHook.Handler
	rtCfg.SnapshotConfig.RestoreHook.Timeout = msg.Message.Function.RestoreHook.Timeout
	rtCfg.SnapshotConfig.EnableSnapshot = msg.Message.Function.EnableSnapshot
	rtCfg.SnapshotConfig.IsSnapshotExpire = msg.Message.SnapshotDeployment.IsSnapshotExpire
	rtCfg.SnapshotConfig.SnapshotState = msg.Message.SnapshotDeployment.SnapshotState
	rtCfg.SnapshotConfig.SnapshotCriuOpts = msg.Message.SnapshotDeployment.SnapshotCriuOpts
	rtCfg.SnapshotConfig.NeedDelObjectIDs = msg.Message.SnapshotDeployment.NeedDeleteObjectIDs
}

func setExtraEnv(rtCfg *config.Configuration, msg *types.FunctionSpecRequest) {
	enableTag := os.Getenv("TRACE_DISABLE")
	if msg.Message.Function.EnvKey == "" && enableTag != "DISABLE" {
		var envMap map[string]interface{}
		env, err := localauth.Decrypt(msg.Message.Function.Environment)
		if err != nil {
			log.GetLogger().Warnf("failed to decrypt EnvInfo: %s", err.Error())
			return
		}
		err = json.Unmarshal(env, &envMap)
		if err != nil {
			log.GetLogger().Warnf("failed to Unmarshal EnvInfo: %s", err.Error())
			return
		}
		rtCfg.RuntimeConfig.TraceEnv = envMap
	}
	if strings.Contains(msg.Message.Function.Language, "go") || strings.Contains(msg.Message.Function.Language,
		"custom") || strings.Contains(msg.Message.Function.Language, "http") {
		var envJSON, userData []byte
		var err error
		if msg.Message.Function.Environment != "" {
			if envJSON, err = DecryptUserEnv(msg.Message.Function.Environment, msg.Message.Function.EnvPlaintextKmsCipher); err != nil {
				log.GetLogger().Warnf("failed to decrypt Environment: %s", err.Error())
				return
			}
			rtCfg.RuntimeConfig.ExtraEnvJSON = envJSON
		}
		if msg.Message.Function.EncryptedUserData != "" {
			if userData, err = DecryptUserEnv(msg.Message.Function.EncryptedUserData, msg.Message.Function.EnvPlaintextKmsCipher); err != nil {
				log.GetLogger().Warnf("failed to decrypt EncryptedUserData: %s", err.Error())
				return
			}
			rtCfg.RuntimeConfig.ExtraUserData = userData
		}
	}
}

// DecryptUserEnv -
func DecryptUserEnv(env, envPlaintextKmsCipher string) ([]byte, error) {
	if envPlaintextKmsCipher != "" {
		return kms.DecryptDataWithAESByPlainCipherToByte(env, envPlaintextKmsCipher)
	}
	return localauth.Decrypt(env)
}

func (s *Server) setConfigS3AkSk(r *http.Request) error {
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read the loading request, error: %s", err.Error())
		return err
	}
	var msg types.FunctionSpecRequest
	if err := json.Unmarshal(b, &msg); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the json, error: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("set config ak, sk and token. ak: %s.", msg.Message.Deployment.Access)
	s.conf.DeploymentConfig.BucketID = msg.Message.Deployment.BucketID
	s.conf.DeploymentConfig.ObjectID = msg.Message.Deployment.ObjectID
	s.conf.DeploymentConfig.Access = msg.Message.Deployment.Access
	s.conf.DeploymentConfig.Secret = msg.Message.Deployment.Secret
	s.conf.DeploymentConfig.SecurityToken = msg.Message.Deployment.SecurityToken
	s.conf.URN = msg.Message.Function.URN
	return nil
}

// readRequest read config when loadFunction
func (s *Server) readRequest(r *http.Request) (*network.NetWork, error) {
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		status.Set(status.Waiting)
		log.GetLogger().Errorf("failed to read the loading request, error: %s", err.Error())
		return nil, err
	}

	var msg types.FunctionSpecRequest
	if err := json.Unmarshal(b, &msg); err != nil {
		status.Set(status.Waiting)
		log.GetLogger().Errorf("failed to unmarshal the json, error: %s", err.Error())
		return nil, err
	}

	if _, err := govalidator.ValidateStruct(&msg); err != nil {
		status.Set(status.Waiting)
		log.GetLogger().Errorf("failed to validate message, error: %s", err.Error())
		return nil, err
	}

	setRuntimeMessage(s.conf, &msg)
	setExtraEnv(s.conf, &msg)
	extra := defineExtraHeader(r)
	s.conf.RuntimeConfig.Initializer.Extra = extra
	net := &network.NetWork{NATConfigures: msg.Message.NATConfigure,
		EndpointVpcNATConfigure: msg.Message.EndpointVpcNATConfigure,
		PublicVpcNATConfigure:   msg.Message.PublicVpcNATConfigure, RouteInfo: msg.Message.Function.RounteInfo,
		SvcCidr: msg.Message.SvcCidr, VPCReservedCidr: s.conf.VPCConfig.ReservedCidr,
		PingHash: s.pingHash, Ping6Hash: s.ping6Hash, IsSafety: msg.Message.Function.IsSafety,
		DNSConfig:    s.conf.DNSConfig,
		ExtendedInfo: utils.GetExtendedInfo(msg.Message.Function.FuncMountConfig.FuncMounts),
	}
	if commonutils.IsCAEFunc(msg.Message.Function.BusinessType) &&
		msg.Message.Function.CAECustomS3ConfigInfo.Endpoint != "" {
		s.caeDumpFileUploadConfig(msg)
	} else if msg.Message.Function.CustomUploadConfig.Endpoint != "" {
		s.DumpFileUploadConfig(msg)
	}

	s.invokeServer.NetworkController.DisablePublicNetwork = msg.Message.Function.NetworkController.DisablePublicNetwork
	s.invokeServer.NetworkController.DisableWaitBeforeChangeNet = msg.Message.Function.DisableWaitBeforeChangeNet
	tcpbus.InitTimeout = msg.Message.Function.Initializer.Timeout
	tcpbus.BusinessType = msg.Message.Function.BusinessType
	runtime.SetBusinessType(msg.Message.Function.BusinessType)
	runtime.SetProperties(msg.Message.Function.Properties)
	utils.SetIsMcpFunction(msg.Message.Function.IsMcpFunction &&
		msg.Message.Function.Language == worker.WebRuntimeLanguage)
	utils.SetProxyURL(msg.Message.Function.NpmConfigRegistry, msg.Message.Function.UVIndexURL)
	utils.SetVpaResource(msg.Message.Function.ResourceMetaData.CPU, msg.Message.Function.ResourceMetaData.Mem)
	runtime.SetWebSocketEnable(msg.Message.Function.EnableWebsocket)
	return net, utils.LocalFuncURN.ParseFrom(s.conf.URN)
}

// DumpFileUploadConfig -
func (s *Server) DumpFileUploadConfig(msg types.FunctionSpecRequest) {
	s.conf.CustomUploadConfig = msg.Message.Function.CustomUploadConfig
	deployer, err := deploy.NewDumpDeployer(s.conf, s.conf.CustomUploadConfig)
	if err != nil {
		log.GetLogger().Errorf("failed to new dump file deployer: %s", err.Error())
		return
	}
	s.customDeployer = deployer
	s.rebuildOBSFilePath()
}

func (s *Server) caeDumpFileUploadConfig(msg types.FunctionSpecRequest) {
	s.conf.CAECustomS3ConfigInfo = msg.Message.Function.CAECustomS3ConfigInfo
	s.conf.IsCAECustomS3Config = true
	s.conf.BusinessType = constants.BusinessTypeCAE
	deployer, err := deploy.NewDumpDeployer(s.conf,
		types.CustomUploadConfig{CAECustomS3ConfigInfo: s.conf.CAECustomS3ConfigInfo})
	if err != nil {
		s.conf.IsCAECustomS3Config = false
		log.GetLogger().Warnf("failed to new cae custom deployer: %s", err.Error())
		return
	}
	s.customDeployer = deployer
	if msg.Message.Function.Properties != nil {
		s.saveCAEFuncProperty(msg.Message.Function.Properties)
	}
	s.rebuildCAEOBSFilePath()
}

func defineExtraHeader(r *http.Request) map[string]string {
	extra := make(map[string]string, extraHeaderMapSize)
	decryptHeaderToExtra(r, extra, constants.HeaderAccessKey)
	decryptHeaderToExtra(r, extra, constants.HeaderSecretKey)
	decryptHeaderToExtra(r, extra, constants.HeaderAuthToken)
	decryptHeaderToExtra(r, extra, constants.HeaderSecurityAccessKey)
	decryptHeaderToExtra(r, extra, constants.HeaderSecuritySecretKey)
	decryptHeaderToExtra(r, extra, constants.HeaderSecurityToken)
	decryptHeaderToExtra(r, extra, constants.HeaderIAMToken)
	return extra
}

func decryptHeaderToExtra(r *http.Request, extra map[string]string, headerName string) {
	if extra == nil {
		return
	}
	if r.Header.Get(headerName) != "" {
		decryptHeaderValue, err := localauth.Decrypt(r.Header.Get(headerName))
		if err != nil {
			log.GetLogger().Warnf("failed to decrypt %", headerName)
			extra[headerName] = ""
		} else {
			extra[headerName] = string(decryptHeaderValue)
		}
		commonutils.ClearByteMemory(decryptHeaderValue)
	} else {
		extra[headerName] = ""
	}
}

func (s *Server) checkHealth(w http.ResponseWriter, r *http.Request) {
	if status.Get() == status.Waiting && s.isLoaded() {
		log.GetLogger().Errorf("unhealthy: already loaded")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if runtimeContainerID := r.Header.Get("RUNTIME_CONTAINER_ID"); runtimeContainerID != "" {
		s.runtimeContainerID = runtimeContainerID
	}
	writeResponse(w, nil)
}

func (s *Server) handleWssConnect(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	log.GetLogger().Infof("start to create websocket client with rdispatcher, traceID: %s", traceID)
	connReq, err := parseWebsocket(r)
	if err != nil {
		log.GetLogger().Errorf("parseWebsocket err: %s", err.Error())
		writeResponse(w, err)
		return
	}
	concurrency := connReq.WebSocketConfig.Concurrency
	if concurrency == 0 {
		log.GetLogger().Warnf("invalid websocket concurrency, traceID: %s", traceID)
		concurrency = 1
	}
	log.GetLogger().Debugf("handleWssConnect concurrency %d, traceID: %s", concurrency, traceID)
	if err := cache.GetMessageQueue().WaitUnblock(concurrency, false); err != nil {
		writeResponse(w, err)
		return
	}
	// websocket functions used only
	// add route when network model is vpc or tunnel
	err = addRouteOfDispatcher(connReq.WebSocketConfig.TargetIP)
	if err != nil {
		writeResponse(w, err)
		return
	}

	wssmanager := websocket.Instance()
	err = wssmanager.ChangeWebSocketIptables(connReq)
	if err != nil {
		writeResponse(w, err)
		return
	}

	connPair, err := wssmanager.CreateWssConnPair(traceID, websocket.BuildStreamOption(traceID, connReq, s.conf))
	if err != nil {
		log.GetLogger().Errorf("CreateWssConnPair err: %s", err.Error())
		writeResponse(w, err)
		return
	}
	connPair.HandleMessage()
	log.GetLogger().Infof("succeed to handle create websocket client with rdispatcher"+
		"with traceID: %s", traceID)
	websocketFinalizerOnce.Do(func() {
		finalizer := func() error {
			wssmanager.GracefulShutdown()
			return nil
		}
		s.finalizers = append(s.finalizers, finalizer)
	})
	writeResponse(w, nil)
}

func addRouteOfDispatcher(dispatcherIP string) error {
	funcContainerIP := os.ExpandEnv("${POD_IP}")
	intfName, containerGateway, err := network.GetIntfnameAndGatewayByIPAddr(funcContainerIP)
	if err != nil {
		log.GetLogger().Errorf("GetIntfnameAndGatewayByIPAddr err: %s", err.Error())
		return err
	}
	cmdLine := fmt.Sprintf("ip route add %s via %s dev %s", dispatcherIP, containerGateway, intfName)
	startTime := time.Now()
	ret, result, err := network.ExecuteShellCommandLineWithCombinedOutput(cmdLine)
	if err != nil {
		log.GetLogger().Errorf("failed to execute command line '%s' error %s.", cmdLine, err.Error())
		return err
	}
	if ret == utils.IptablesCmdSucceed ||
		(ret == utils.IptablesCmdFailed && strings.Contains(result, utils.IptablesExists)) {
		log.GetLogger().Infof("execute command '%s' ok, cost: %+v, ret: %d, result=%s.",
			cmdLine, time.Since(startTime), ret, result)
		return nil
	}
	log.GetLogger().Errorf("execute command '%s' not ok, cost: %+v, ret: %d, result=%s.",
		cmdLine, time.Since(startTime), ret, result)
	return errors.New("unexpected returned value")
}

func (s *Server) handleWssConnCount(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	log.GetLogger().Infof("start to get websocket connections and http connections count, traceID: %s", traceID)
	allCount := runtime.RunMetrics.GetRequestCount()
	wssCount := websocket.Instance().GetWssConnPairsCount()
	countInfo := schedulerapi.WorkerWssCountResponse{
		TotalConnNum: uint32(allCount),
		WssConnNum:   uint32(wssCount),
	}
	resByte, err := json.Marshal(countInfo)
	if err != nil {
		writeResponse(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Header().Add(constants.ContentType, constants.ApplicationJSON)
	_, err = w.Write(resByte)
	if err != nil {
		log.GetLogger().Errorf("%s | failed to write the wss count response,error: %s",
			traceID, err.Error())
	}
	log.GetLogger().Infof("finish to get websocket connections: %s and total connections count: %s, traceID: %s",
		countInfo.WssConnNum, countInfo.TotalConnNum, traceID)
}

func (s *Server) handleTenantCommands(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	log.GetLogger().Infof("start to handle tenant commands traceID: %s", traceID)

	if runtime.RuntimeManagerClient == nil {
		log.GetLogger().Errorf("runtime manager is nil, traceID: %s", traceID)
		writeErrorMsg(w, http.StatusInternalServerError, "runtime manager is nil", traceID)
		return
	}

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read the tenant commands from request, error: %s, traceID: %s",
			err.Error(), traceID)
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}
	tenantCommandStruct := &types.TenantCommandStruct{}
	err = json.Unmarshal(b, tenantCommandStruct)
	if err != nil {
		log.GetLogger().Errorf("failed to Unmarshal tenant commands struct, error: %s, traceID: %s",
			err.Error(), traceID)
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}
	if tenantCommandStruct.Timeout <= 0 {
		tenantCommandStruct.Timeout = defaultTenantCommandRequestTimeout
	}

	s.tenantCommandExecuteLock.Lock()
	defer s.tenantCommandExecuteLock.Unlock()
	s.currentTenantCommandsTaskID = traceID
	resp := runtime.RuntimeManagerClient.SendMessageWithTimeout(tcpmsg.BuildMessage(tcpmsg.ExecuteTenantCommands, b),
		time.Duration(tenantCommandStruct.Timeout)*time.Second)
	if resp.Err != "" {
		log.GetLogger().Warnf("tenant command execute fail, err: %s, traceID: %s", resp.Err, traceID)
		writeErrorMsg(w, http.StatusInternalServerError, resp.Err, traceID)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add(constants.ContentType, constants.ApplicationJSON)
	log.GetLogger().Infof("succeed to handle tenant commands to runtime manager, traceID: %s", traceID)
}

func parseWebsocket(r *http.Request) (*schedulerapi.CreateWebSocketConnectionReq, error) {
	req := schedulerapi.CreateWebSocketConnectionReq{}
	res, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read request body")
		return nil, err
	}
	err = json.Unmarshal(res, &req)
	if err != nil {
		log.GetLogger().Errorf("failed to unmarshal websocket request info")
		return nil, err
	}
	return &req, nil
}

func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	log.GetLogger().Infof("start to handle shut down")
	// avoid replicate termination request
	if s.terminationChan != nil {
		s.terminationChanCloseOnce.Do(func() {
			close(s.terminationChan)
		})
	}
	traceID := r.Header.Get(constants.HeaderTraceID)
	log.GetLogger().Infof("succeed to receive termination request from worker-manager with traceID: %s", traceID)

	writeResponse(w, nil)
}

func (s *Server) handleUpdateAgencyInfo(w http.ResponseWriter, r *http.Request) {
	log.GetLogger().Infof("start to handle update update agency info request")
	podName := r.Header.Get(constants.HeaderPodName)
	if podName != "" && podName != os.Getenv("PodName") {
		log.GetLogger().Warnf("pod name mismatched when handle agency update %s", podName)
		writeResponse(w, nil)
		return
	}

	traceID := r.Header.Get(constants.HeaderTraceID)
	s.conf.RuntimeConfig.IsClusterUpgrading = r.Header.Get(constants.HeaderClusterUpgrading)
	utils.SetUserAgency(constants.HeaderAccessKey, r.Header.Get(constants.HeaderAccessKey))
	utils.SetUserAgency(constants.HeaderSecretKey, r.Header.Get(constants.HeaderSecretKey))
	utils.SetUserAgency(constants.HeaderAuthToken, r.Header.Get(constants.HeaderAuthToken))
	utils.SetUserAgency(constants.HeaderSecurityAccessKey, r.Header.Get(constants.HeaderSecurityAccessKey))
	utils.SetUserAgency(constants.HeaderSecuritySecretKey, r.Header.Get(constants.HeaderSecuritySecretKey))
	utils.SetUserAgency(constants.HeaderSecurityToken, r.Header.Get(constants.HeaderSecurityToken))
	if r.Header.Get(constants.HeaderIAMToken) != "" {
		utils.SetUserAgency(constants.HeaderIAMToken, r.Header.Get(constants.HeaderIAMToken))
	}
	if s.invokeServer != nil && (s.isJavaFunction() || r.Header.Get(constants.HeaderIAMToken) != "") {
		s.invokeServer.HandleUpdateAgencyToRuntime()
	}
	log.GetLogger().Infof("succeed to handle update update agency info request to worker from worker-manager "+
		"with traceID: %s, isUpgrading: %s", traceID, s.conf.RuntimeConfig.IsClusterUpgrading)

	writeResponse(w, nil)
}

func (s *Server) handleNotifyRouterReady(c *gin.Context) {
	utils.SetCAERouterReady()
	log.GetLogger().Info("start to handle notify router ready request")
	if s.invokeServer != nil {
		s.invokeServer.HandleRouterReady()
	}
	writeResponse(c.Writer, nil)
}

func writeResponse(w http.ResponseWriter, err error) {
	badResponse := snerror.BadResponse{}
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
	w.Header().Add(constants.ContentType, constants.ApplicationJSON)

	if snErr, ok := err.(snerror.SNError); ok {
		badResponse.Code = snErr.Code()
		badResponse.Message = snErr.Error()
	} else {
		badResponse.Code = snerror.WorkerInternalErrorCode
		badResponse.Message = err.Error()
	}
	rsp, err := json.Marshal(badResponse)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal badResponse, error: %s", err.Error())
		return
	}
	_, err = w.Write(rsp)
	if err != nil {
		log.GetLogger().Errorf("failed to write the response, error: %s", err.Error())
	}
}

// Start stats a new goroutine to start Http Server,
// So it's not blocking method.
func (s *Server) Start() error {
	// Warm start
	if status.Get() != status.Waiting {
		return fmt.Errorf("status isn't WAITING")
	}
	log.GetLogger().Infof("warm starting the server")

	go func() {
		// ListenAndServeTLS and ListenAndServe always returns a non-nil error.
		if s.httpServer.TLSConfig != nil {
			if err := s.httpServer.ListenAndServeTLS("", ""); err != http.ErrServerClosed {
				log.GetLogger().Errorf("failed to start the https server, error: %s", err.Error())
				log.GetLogger().Sync()
				os.Exit(loadErrorExitCode)
			}
			return
		}
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.GetLogger().Errorf("failed to start the http server, error: %s", err.Error())
			log.GetLogger().Sync()
			os.Exit(loadErrorExitCode)
		}
	}()
	go func() {
		if err := s.preStopServer.ListenAndServe(); err != http.ErrServerClosed {
			log.GetLogger().Warnf("failed listen and serve preStopServer, error: %s", err.Error())
		}
	}()

	// If container restart in pod, reload function
	if s.conf.IsLoaded {
		log.GetLogger().Infof("reloaded the function after warmly started the server")
		s.reloadFunction()
	}
	if utils.NeedCustomContainerPreLoad() {
		log.GetLogger().Infof("custom container need pre load vpc or func mount")
		s.customContainerPreOnce.Do(func() {
			s.prepareCustomContainer()
		})
	}
	return nil
}

// GracefulShutdown -
func (s *Server) GracefulShutdown() {
	defer updateLogConfigFile(true)

	// Wait for all requests to finish before closing the HTTP liveness probe.
	if err := s.invokeServer.GracefulShutdown(); err != nil {
		log.GetLogger().Warnf("failed to graceful shutdown invoke server: %s", err.Error())
	} else {
		log.GetLogger().Infof("succeed to graceful shutdown invoke server")
	}

	log.GetLogger().Infof("shutdown the http server")
	if err := s.httpServer.Shutdown(context.TODO()); err != nil {
		log.GetLogger().Warnf("failed to graceful shutdown http server: %s", err.Error())
	} else {
		log.GetLogger().Infof("succeed to graceful shutdown http server")
	}

	close(s.preStopCh)
	log.GetLogger().Infof("shutdown the preStop server")
	if err := s.preStopServer.Shutdown(context.TODO()); err != nil {
		log.GetLogger().Warnf("failed to graceful shutdown preStop server: %s", err.Error())
	} else {
		log.GetLogger().Infof("succeed to graceful shutdown preStop server")
	}

	for _, finalizer := range s.finalizers {
		if err := finalizer(); err != nil {
			log.GetLogger().Warnf("failed to finalize: %s", err.Error())
		} else {
			log.GetLogger().Infof("succeed to do finalize")
		}
	}
	s.waitForUploadDumpFile()
}

func updateLogConfigFile(podDelete bool) {
	filePath := fmt.Sprintf("%s/%s", worker.LogConfigFilePath, worker.LogConfigFileName)
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.GetLogger().Warnf("failed to read file %s, err: %s", filePath, err.Error())
		return
	}

	var logConfig config.LogConfig
	if err = json.Unmarshal(data, &logConfig); err != nil {
		log.GetLogger().Warnf("failed to unmarshal data of file %s, err: %s", filePath, err.Error())
		return
	}

	if podDelete {
		logConfig.PodDelete = podDelete
	}
	logConfig.ModifyTime = time.Now().Unix()

	newData, err := json.Marshal(logConfig)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal logConfig, err: %s", err.Error())
		return
	}

	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC, logConfigFileMode)
	if err != nil {
		log.GetLogger().Warnf("failed to open file %s", filePath)
		return
	}
	defer file.Close()

	_, err = file.Write(newData)
	if err != nil {
		log.GetLogger().Warnf("failed to write data to file %s", filePath)
		return
	}
	log.GetLogger().Debugf("succeed to update file %s", filePath)
}

func persistConfig(cfg *config.Configuration) error {
	conf, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	oldMask := syscall.Umask(0)
	defer syscall.Umask(oldMask)
	err = ioutil.WriteFile(worker.RestartConfigPath, conf, worker.PersistFileMode)
	if err != nil {
		return err
	}
	return nil
}

func (s *Server) reloadFunction() {
	if status.CompareAndSet(status.Waiting, status.Initializing) {
		log.GetLogger().Infof("starting to reload the function.")

		if !(s.conf.Language == worker.CustomContainerLanguage) {
			_, err := s.deployer.Deploy(s.conf, nil, s.conf.Language)
			if err != nil {
				log.GetLogger().Errorf("failed to deploy the function, error: %s", err.Error())
				log.GetLogger().Sync()
				os.Exit(loadErrorExitCode)
			}
		}
		deployMes := make(chan *deploy.Message, 1)
		deployMes <- &deploy.Message{
			Duration: nil,
			Err:      nil,
		}
		if _, err := s.invokeServer.LoadAndServe(context.Background(), s.conf, deployMes, nil); err != nil {
			log.GetLogger().Sync()
			os.Exit(loadErrorExitCode)
		}
		if err := s.Start(); err != nil {
			log.GetLogger().Sync()
			os.Exit(loadErrorExitCode)
		}
		log.GetLogger().Infof("succeed to created the invoke server and load function")
		status.Set(status.Ready)
	} else {
		log.GetLogger().Errorf("status is not WAITING, repeated load function")
	}
}

func changeNAT(n *network.NetWork, configNumber int, getCodeFinishCh chan struct{}) error {
	isNeedFuncVpc := n.NeedChange(n.NATConfigures)
	isNeedEndPointVpc := n.NeedChange(n.EndpointVpcNATConfigure)
	isNeedTwoVpcChange := isNeedEndPointVpc && isNeedFuncVpc
	isNeedPublicVpc := n.NeedChange(n.PublicVpcNATConfigure)
	isNeedPublicVpc = isNeedPublicVpc && !isNeedFuncVpc && !isNeedEndPointVpc

	// if only need change endpoint vpc
	if isNeedEndPointVpc {
		if err := changeVpcNAT(n, checkNATParam{
			configure:    n.EndpointVpcNATConfigure,
			configNumber: configNumber,
			replaceable:  n.EndpointVpcReplaceable,
			tunnelName:   endpointTunnelName,
			needEndpoint: true,
		}, getCodeFinishCh); err != nil {
			return err
		}
	}
	// if need change func vpc
	if isNeedFuncVpc {
		if err := changeVpcNAT(n, checkNATParam{
			configure:    n.NATConfigures,
			configNumber: configNumber,
			replaceable:  n.Replaceable,
			tunnelName:   funcTunnelName,
			needEndpoint: isNeedTwoVpcChange,
		}, getCodeFinishCh); err != nil {
			return err
		}
	}
	// if need change public vpc
	if isNeedPublicVpc {
		if err := changeVpcNAT(n, checkNATParam{
			configure:    n.PublicVpcNATConfigure,
			configNumber: configNumber,
			replaceable:  n.PublicVpcReplaceable,
			needPublic:   true,
		}, getCodeFinishCh); err != nil {
			return err
		}
	}
	return nil
}

func changeVpcNAT(n *network.NetWork, param checkNATParam, getCodeFinishCh chan struct{}) error {
	if param.replaceable {
		for i := 0; i <= network.MaxRetryTimes; i++ {
			if i == network.MaxRetryTimes {
				return errors.New("failed to change network")
			}
			param.configNumber = i % multiplePatNumber
			if len(param.configure) == multiplePatNumber &&
				(n.CurrentNATConfigure.PatPodName == param.configure[0].PatPodName ||
					n.CurrentNATConfigure.PatPodName == param.configure[1].PatPodName) {
				// no need to change network when current pat container ip is in configures
				log.GetLogger().Infof("no need to change network and current pat info is %+v", n.CurrentNATConfigure)
				return nil
			}
			if err := changePat(n, param); err != nil {
				log.GetLogger().Warnf("failed to change network err :%s, retry times: %d",
					err.Error(), i)
				time.Sleep(time.Second)
				continue
			}

			// record current pat info used by network
			n.CurrentNATConfigure = param.configure[param.configNumber]
			fmt.Printf("network configures len is %d and succeed to change network to %+v\n",
				len(param.configure), n.CurrentNATConfigure)
			log.GetLogger().Infof("network configures len is %d and succeed to change network to %+v",
				len(param.configure), n.CurrentNATConfigure)
			return nil
		}
	}

	if err := changePat(n, param); err != nil {
		log.GetLogger().Warnf("failed to change network err :%s", err.Error())
		return err
	}
	// record current pat info used by network
	n.CurrentNATConfigure = param.configure[0]
	fmt.Printf("network configures len is %d and succeed to change network to %+v\n",
		len(param.configure), n.CurrentNATConfigure)
	log.GetLogger().Infof("network configures len is %d and succeed to change network to %+v",
		len(param.configure), n.CurrentNATConfigure)

	return nil
}

type checkNATParam struct {
	configure    []types.NATConfigure
	configNumber int
	replaceable  bool
	tunnelName   string
	needEndpoint bool
	needPublic   bool
}

func checkNAT(ctx context.Context, n *network.NetWork, param checkNATParam) {
	if param.replaceable {
		handleCheckReplaceable(ctx, n, param)
		return
	}

	if len(param.configure) <= param.configNumber {
		return
	}
	natConfig := param.configure[param.configNumber]
	err := n.CheckNATHealth(ctx, natConfig.PatContainerIP, natConfig.PatGateway, natConfig.SubMetaDigest)
	if err != nil {
		fusion.SendWorkerErr(runtime.Error{
			Code:    snerror.WorkerPingVpcGatewayError,
			Message: snerror.ErrText(snerror.WorkerPingVpcGatewayError),
		})
	}
}

func changePat(n *network.NetWork, param checkNATParam) error {
	if !param.needPublic {
		err := n.Change(param.configNumber, param.configure, param.tunnelName, param.needEndpoint, nil)
		if err != nil {
			return err
		}
		return nil
	}
	err := n.ChangePublicVpc(param.configNumber, param.configure, nil)
	if err != nil {
		return err
	}
	return nil
}

func handleCheckReplaceable(ctx context.Context, n *network.NetWork, param checkNATParam) {
	if len(param.configure) <= param.configNumber {
		return
	}
	for i := 0; i <= network.MaxRetryTimes; {
		natConfig := param.configure[param.configNumber]
		err := n.CheckNATHealth(ctx, natConfig.PatContainerIP, natConfig.PatGateway, natConfig.SubMetaDigest)
		if err == nil {
			i++
			continue
		}

		param.configNumber = i % multiplePatNumber

		err = changePat(n, param)
		if err != nil && i != network.MaxRetryTimes {
			time.Sleep(time.Second)
			i++
			continue
		}
		if err != nil && i == network.MaxRetryTimes {
			log.GetLogger().Warnf("failed to change network err :%s, retry times: %d", err.Error(), i)
			fusion.SendWorkerErr(runtime.Error{
				Code:    snerror.WorkerPingVpcGatewayError,
				Message: snerror.ErrText(snerror.WorkerPingVpcGatewayError),
			})
			break
		}
		i = param.configNumber
	}
}

func (s *Server) mountVolume(cli engine.ContainerClient) error {
	if len(s.conf.FuncMountConfig.FuncMounts) == 0 {
		return nil
	}
	containerInfo, err := cli.Info(context.TODO(), s.conf.RuntimeContainerID)
	if err != nil {
		return fmt.Errorf("failed to get container info, err: %s", err.Error())
	}
	config := s.conf.FuncMountConfig
	for i, m := range config.FuncMounts {
		config.FuncMounts[i].LocalMountPath = filepath.Join(containerInfo.RootFsPath, m.LocalMountPath)
	}
	if err = mount.VolumesForFunction(config); err != nil {
		return fmt.Errorf("failed to mount volume for function: %s", err.Error())
	}
	finalizer := func() error {
		if err := mount.UmountVolumesForFunction(config); err != nil {
			return fmt.Errorf("failed to umount volume for function: %s", err.Error())
		}
		return nil
	}
	s.finalizers = append(s.finalizers, finalizer)
	return nil
}

func (s *Server) normalMountVolume() error {
	if len(s.conf.FuncMountConfig.FuncMounts) == 0 {
		log.GetLogger().Infof("skip mount volume without mount config")
		return nil
	}
	cli, err := engine.NewContainerClient()
	if err != nil {
		log.GetLogger().Errorf("failed to create container client: %s", err.Error())
		return err
	}
	if err := s.mountVolume(cli); err != nil {
		log.GetLogger().Errorf("failed to mount volume: %s", err.Error())
		return err
	}
	return nil
}

func (s *Server) isJavaFunction() bool {
	return commonutils.CheckJavaVersion(s.conf.RuntimeConfig.Language)
}

// SendToDeployCodeChan -
func (s *Server) SendToDeployCodeChan() {
	if s.conf.IsSafety && s.net.CodeDeployChan != nil {
		s.net.CodeDeployChan <- struct{}{}
	}
}

// SendToNatInitialChan -
func (s *Server) SendToNatInitialChan() {
	if s.conf.IsSafety && s.net.NatInitializeChan != nil {
		s.net.NatInitializeChan <- struct{}{}
	}
}

func (s *Server) uploadSnapshotToDS(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	log.GetLogger().Infof("start to download and upload snapshot to ds, traceID: %s", traceID)

	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read the upload snapshot to ds request, error: %s, traceID: %s",
			err.Error(), traceID)
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}

	snapshotDeployment := &types.SnapshotDeploymentSpec{}
	if err = json.Unmarshal(b, snapshotDeployment); err != nil {
		log.GetLogger().Errorf("failed to read the upload snapshot to ds request, error: %s, traceID: %s",
			err.Error(), traceID)
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}

	bucketID := snapshotDeployment.BucketID
	objectID := snapshotDeployment.ObjectID
	snapshotConfig := config.SnapshotConfig{SnapshotBucketID: bucketID, SnapshotObjectID: objectID,
		SnapshotSha256: snapshotDeployment.SnapshotSha256}
	language := r.Header.Get(constants.Language)

	err = s.deployer.DeleteSnapshot(s.conf, bucketID, objectID, snapshotDeployment.NeedDeleteObjectIDs)
	if err != nil {
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}

	needReturn, err := s.deployer.DownloadSnapshotWithRetry(s.conf, snapshotConfig, language, traceID)
	if needReturn && err != nil {
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}

	needReturn, err = uploadSnapshotWithRetry(bucketID, objectID, traceID)
	if needReturn && err != nil {
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add(constants.ContentType, constants.ApplicationJSON)
	log.GetLogger().Infof("succeed to download and upload snapshot to ds, traceID: %s", traceID)
}

func uploadSnapshotWithRetry(bucketID, objectID, traceID string) (bool, error) {
	var err error
	for i := 0; i < retryTime; i++ {
		_, _, err = deploy.UploadSnapshotToDS(bucketID, objectID)
		if err != nil {
			log.GetLogger().Errorf("err: %s, traceID: %s", err.Error(), traceID)
		} else {
			break
		}

		if i != retryTime-1 {
			time.Sleep(retryInterval)
		}

		if i >= retryTime-1 {
			return true, err
		}
	}
	return false, err
}

func writeErrorMsg(w http.ResponseWriter, errCode int, errMsg string, traceID string) {
	w.WriteHeader(errCode)
	if _, err := w.Write([]byte(errMsg)); err != nil {
		log.GetLogger().Errorf("failed to write the response, error: %s, traceID: %s", err.Error(), traceID)
	}
}

func (s *Server) saveCAEFuncProperty(property map[string]string) {
	log.GetLogger().Infof("start to filter cae property: %+v", property)
	propertyJSON, err := json.Marshal(property)
	if err != nil {
		log.GetLogger().Errorf("marshal cae instance property error: %s", err.Error())
		return
	}
	propertyStruct := types.CAEAppProperty{}
	if err = json.Unmarshal(propertyJSON, &propertyStruct); err != nil {
		log.GetLogger().Errorf("unmarshal cae instance property byte to struct error: %s", err.Error())
		return
	}
	s.conf.CAEAppProperty = propertyStruct
}

func (s *Server) dumpFileNotify() {
	stopChan := signals.WaitForSignal()
	if stopChan == nil {
		return
	}
	if s.customDeployer == nil {
		log.GetLogger().Warnf("function custom deployer is nil")
		return
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.GetLogger().Errorf("failed to new file watcher for dump file path, err: %s", err.Error())
		return
	}
	defer watcher.Close()
	watchPath := []string{watchHeapDumpPath}
	if !s.conf.IsCAECustomS3Config {
		watchPath = append(watchPath, watchCoreDumpPath)
	}
	for _, p := range watchPath {
		if err = watcher.Add(p); err != nil {
			log.GetLogger().Errorf("failed to add path: %s for watching dump file path, err: %s",
				p, err.Error())
			return
		}
	}
	log.GetLogger().Infof("start to watch dump file in path: %v", watchPath)
	extraWatchPath := make(map[string]struct{})
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Create > 0 {
				s.processCreatedDumpFile(event, watcher, extraWatchPath)
			}
			if event.Op&(fsnotify.Remove|fsnotify.Rename) > 0 {
				removeWatchPath(extraWatchPath, event, watcher)
			}
		case err, _ = <-watcher.Errors:
			if err != nil {
				log.GetLogger().Errorf("custom dump file watcher receive a watcher error: %s", err.Error())
			}
		case _, ok := <-stopChan:
			if !ok {
				return
			}
		}
	}
}

func (s *Server) processCreatedDumpFile(event fsnotify.Event, watcher *fsnotify.Watcher,
	extraWatchPath map[string]struct{}) {
	if extraWatchPath == nil {
		return
	}
	if strings.HasSuffix(event.Name, ".swp") || strings.HasSuffix(event.Name, ".swx") {
		return
	}
	log.GetLogger().Infof("watch the dump file event: %s, file path: %s", event.String(), event.Name)
	if commonutils.IsDir(event.Name) {
		if _, ok := extraWatchPath[event.Name]; !ok {
			if err := watcher.Add(event.Name); err != nil {
				log.GetLogger().Warnf("failed to add path: %s for watching dump file path, err: %s",
					event.Name, err.Error())
			}
			extraWatchPath[event.Name] = struct{}{}
		}
		return
	}
	go s.uploadDumpFile(event.Name)
	if s.conf.IsCAECustomS3Config {
		if err := s.reportOOMEvent(event.Name); err != nil {
			log.GetLogger().Warnf("failed to report oom event to bus, err: %s", err.Error())
		}
	}
}

func removeWatchPath(extraWatchPath map[string]struct{}, event fsnotify.Event, watcher *fsnotify.Watcher) {
	var shouldRemoveWatch bool
	_, shouldRemoveWatch = extraWatchPath[event.Name]
	if commonutils.IsFile(event.Name) {
		parentPath := filepath.Dir(event.Name)
		if parentPath != watchCoreDumpPath && parentPath != watchHeapDumpPath {
			if readDir, readErr := os.ReadDir(parentPath); readErr == nil && len(readDir) == 0 {
				shouldRemoveWatch = true
			}
		}
	}
	if shouldRemoveWatch {
		if removeErr := watcher.Remove(event.Name); removeErr != nil {
			log.GetLogger().Warnf("failed to remove path:%s, err:%s", event.Name, removeErr.Error())
		}
		delete(extraWatchPath, event.Name)
		log.GetLogger().Warnf("remove watch path :%s", event.Name)
	}
}

func (s *Server) reportOOMEvent(fileName string) error {
	if !strings.HasSuffix(fileName, ".hprof") {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), worker.DefaultReportErrMsgContextTimeout)
	defer cancel()
	if s.invokeServer != nil {
		functionURN := utils.LocalFuncURN.String()
		errMsg := snerror.ErrEventMessageText(snerror.RuntimeMemoryExceedLimit)
		err := s.invokeServer.ReportErrMsgToBus(ctx, functionURN, errMsg, podevent.EventErrorType, runtime.GetProperties())
		if err != nil {
			log.GetLogger().Warnf("failed to report event to bus, err: %s", err.Error())
			return err
		}
	}
	return nil
}

func (s *Server) uploadDumpFile(dumpFile string) {
	if s.customDeployer == nil {
		log.GetLogger().Warnf("custom deployer is nil, upload dump file: %s failed", dumpFile)
		return
	}
	s.wgDumpUpload.Add(1)
	defer s.wgDumpUpload.Done()
	s.tenantCommandExecuteLock.RLock()
	taskID := s.currentTenantCommandsTaskID
	s.tenantCommandExecuteLock.RUnlock()
	s.conf.CustomUploadConfig.ObsKey = filepath.Join(
		s.conf.CustomUploadConfig.FilePath, time.Now().UTC().Format("20060102150405"),
		os.Getenv("HOSTNAME"), taskID)
	errWaitForFile := waitForFileWriteToComplete(dumpFile)
	if errWaitForFile != nil {
		log.GetLogger().Errorf("wait for file %s write to complete failed, error: %s", dumpFile, errWaitForFile.Error())
		return
	}

	log.GetLogger().Infof("start to upload dump file: %s", dumpFile)
	if err := s.customDeployer.UploadDumpFile(s.conf, dumpFile); err != nil {
		log.GetLogger().Warnf("upload dump file: %s failed, error: %s", dumpFile, err.Error())
	} else {
		log.GetLogger().Infof("succeed to upload dump file: %s", dumpFile)
	}

	// delete dump file to release disk usage anyway
	if err := os.Remove(dumpFile); err != nil {
		log.GetLogger().Warnf("failed to delete dump file %s, error: %s", dumpFile, err.Error())
		return
	}
	log.GetLogger().Infof("succeed to delete dump file: %s", dumpFile)
}

func waitForFileWriteToComplete(filePath string) error {
	const pollInterval = 5 * time.Second
	var prevSize int64

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for range ticker.C {
		fileInfo, err := os.Stat(filePath)
		if err != nil {
			return err
		}
		currSize := fileInfo.Size()
		if currSize == prevSize {
			log.GetLogger().Infof("Finish writing file %s", filePath)
			return nil
		}
		prevSize = currSize
	}
	return nil
}
func (s *Server) rebuildCAEOBSFilePath() {
	finalPath := s.conf.CAECustomS3ConfigInfo.FilePath
	finalPath = buildFullPath(finalPath, s.conf.CAEAppProperty.EnvironmentName)
	finalPath = buildFullPath(finalPath, s.conf.CAEAppProperty.ApplicationName)
	finalPath = buildFullPath(finalPath, s.conf.CAEAppProperty.ComponentName)
	finalPath = buildFullPath(finalPath, os.Getenv("HOSTNAME"))
	log.GetLogger().Infof("reshape upload obs path success, path: %s", finalPath)
	s.conf.CAECustomS3ConfigInfo.FilePath = finalPath
}

func (s *Server) rebuildOBSFilePath() {
	var functionName, functionVersion string
	baseUrn := urnutils.BaseURN{}
	if err := baseUrn.ParseFrom(s.conf.RuntimeConfig.URN); err != nil {
		log.GetLogger().Errorf("rebuild obs file path failed, err:%s", err.Error())
	}
	functionName, functionVersion = baseUrn.Name, baseUrn.Version
	s.conf.CustomUploadConfig.FilePath = fmt.Sprintf("%s/commandTask/%s-%s", s.conf.CustomUploadConfig.FilePath,
		functionName, functionVersion)
	return
}

func buildFullPath(prePartition, newPartition string) string {
	if newPartition == "" {
		return prePartition
	}
	if prePartition == "" {
		return newPartition
	}
	return prePartition + "/" + newPartition
}

func (s *Server) deleteSnapshotData(w http.ResponseWriter, r *http.Request) {
	traceID := r.Header.Get(constants.HeaderTraceID)
	log.GetLogger().Infof("start to delete snapshot data, traceID: %s", traceID)

	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to read the delete snapshot data request, error: %s, traceID: %s",
			err.Error(), traceID)
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}

	snapshotDeployment := &types.SnapshotDeploymentSpec{}
	if err = json.Unmarshal(b, snapshotDeployment); err != nil {
		log.GetLogger().Errorf("failed to read the delete snapshot data request, error: %s, traceID: %s",
			err.Error(), traceID)
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}
	var needRemoveObjectIDs []string

	for _, v := range snapshotDeployment.SnapshotVersions {
		if v.ObjectID != "" && !commonutils.IsInSlice(v.ObjectID, needRemoveObjectIDs) {
			needRemoveObjectIDs = append(needRemoveObjectIDs, v.ObjectID)
		}
	}

	bucketID := snapshotDeployment.BucketID
	objectID := snapshotDeployment.ObjectID
	err = s.deployer.DeleteSnapshot(s.conf, bucketID, objectID, needRemoveObjectIDs)
	if err != nil {
		log.GetLogger().Errorf("failed to delete snapshot data, error: %s, traceID: %s", err.Error(), traceID)
		writeErrorMsg(w, http.StatusInternalServerError, err.Error(), traceID)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Add(constants.ContentType, constants.ApplicationJSON)
	log.GetLogger().Infof("succeed to delete snapshot data, traceID: %s", traceID)
}

func (s *Server) setStorageConfig(r *http.Request) error {
	if !utils.EnableTenantAuth() {
		return nil
	}
	extra := defineExtraHeader(r)
	s.conf.RuntimeConfig.Initializer.Extra = extra
	if err := utils.LocalFuncURN.ParseFrom(s.conf.URN); err != nil {
		log.GetLogger().Errorf("failed to parse urn %s, err: %s", s.conf.URN, err.Error())
		return err
	}
	storage.SetConfig(utils.EnableTenantAuth(), utils.LocalFuncURN.TenantID,
		s.conf.Initializer.Extra[constants.HeaderIAMToken])
	log.GetLogger().Infof("storage config is set, tenantID: %s", utils.LocalFuncURN.TenantID)
	return nil
}

func (s *Server) waitForUploadDumpFile() {
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(s.getWaitForUploadDumpFileTimeout())*time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		log.GetLogger().Infof("start to wait all dump file upload finish")
		s.wgDumpUpload.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.GetLogger().Infof("all dump file upload finish, exit...")
	case <-ctx.Done():
		log.GetLogger().Warnf("not all dump file upload finish, timeout exit...")
	}
}

func (s *Server) getWaitForUploadDumpFileTimeout() int {
	if s.conf != nil && s.conf.RuntimeConfig.EnvInfo != "" {
		envJSON, err := localauth.Decrypt(s.conf.RuntimeConfig.EnvInfo)
		if err != nil || envJSON == nil {
			return defaultUploadDumpFileTimeout
		}
		var envMap map[string]interface{}
		err = json.Unmarshal(envJSON, &envMap)
		if err != nil {
			return defaultUploadDumpFileTimeout
		}
		if len(envMap) == 0 {
			log.GetLogger().Debug("getWaitForUploadDumpFileTimeout, envMap has not meta")
			return defaultUploadDumpFileTimeout
		}

		for key, value := range envMap {
			if key != constants.UploadDumpFileTimeOutEnvKey {
				continue
			}
			timeout, errParse := toInt(value)
			if errParse == nil && timeout > 0 {
				log.GetLogger().Infof("use env config, timeout is %d", timeout)
				return timeout
			}
		}
	}
	return defaultUploadDumpFileTimeout
}

func toInt(val interface{}) (int, error) {
	switch v := val.(type) {
	case int:
		return v, nil
	case float64:
		return int(v), nil
	case string:
		return strconv.Atoi(v)
	default:
		return 0, fmt.Errorf("unsupported type: %T", val)
	}
}
