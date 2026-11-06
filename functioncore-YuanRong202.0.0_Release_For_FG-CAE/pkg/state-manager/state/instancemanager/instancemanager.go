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

// Package instancemanager implement stateID to nodeID binding, write the relationship to etcd, and ensure consistency
package instancemanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/clusterstatusutil"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/etcdkey"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	utils2 "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/utils"
	"functioncore/pkg/state-manager/constant"
	"functioncore/pkg/state-manager/resourcemanager"
	"functioncore/pkg/state-manager/scheduler"
	"functioncore/pkg/state-manager/scheduler/metadata"
	"functioncore/pkg/state-manager/server"
	"functioncore/pkg/state-manager/state/counter"
	"functioncore/pkg/state-manager/utils"
)

const (
	stateIDEtcdKeyLen         = 13
	stateIDEtcdPathTwoIndex   = 2
	stateIDEtcdPathFiveIndex  = 5
	stateIDEtcdPathSevenIndex = 7
	stateIDEtcdPathNineIndex  = 9
	stateIDEtcdPathIDIndex    = 12
	stateIDsCount             = 20
	keepConnAliveTTL          = 3 * time.Second
	functionNameLength        = 3
	serviceIndex              = 0
	prefixIndex               = 1
	functionNameIndex         = 2
	funcSeparator             = "@"
	base                      = 10
	stateRoutePrefix          = "/sn/stateworkers/business/yrk/tenant/0/function/" +
		"function-task/version/$latest/defaultaz/"
	redisContextTimeout            = 6 * time.Second
	etcdCompetitionTimeout         = 2 * time.Second
	etcdCompetitionRetrial         = 3
	defaultStateCompetitionTimeout = 60
	failedCountThreshold           = 3
	failedCountRedisKeyTTL         = 24 * time.Hour
)

// StateHandler define a state handler
type StateHandler struct {
	// StateEtcd
	StateEtcd *utils.StateEtcd
	// StateCacheMap key type is etcd path(string), value type is stateID to nodeID binding relationship(map[string]string)
	StateCacheMap sync.Map
	StateLockMap  sync.Map
}

var (
	stateHandler             *StateHandler
	once                     sync.Once
	errHandlerNil            = errors.New("state handler is nil")
	stateIDChannels          = make(map[string]chan bool)
	stateIDChLockMap         sync.Map
	stateCompetitionTimeout  = defaultStateCompetitionTimeout * time.Second
	stateIDWithoutNewNodeMap sync.Map
)

// GetStateHandler get a state handler
func GetStateHandler() (*StateHandler, error) {
	var err error
	once.Do(func() {
		if stateHandler == nil {
			etcdClient, etcdClienterr := utils.NewStateEtcd()
			if etcdClienterr != nil {
				err = etcdClienterr
				return
			}

			handler := &StateHandler{
				StateEtcd: etcdClient,
			}
			stateHandler = handler
		}
	})
	if err != nil {
		log.GetLogger().Errorf("in GetStateHandler error: %s", err.Error())
		return nil, err
	}
	return stateHandler, nil
}

func (s *StateHandler) initStateCacheMap() error {
	stateClient := etcd3.GetStateEtcdClient()
	if stateClient == nil || stateClient.Client == nil {
		return nil
	}
	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	getStateResp, err := stateClient.GetResponse(defaultEtcdCtx, "/sn/stateworkers/business/",
		clientv3.WithPrefix())
	if err != nil {
		return err
	}
	// Load stateID-to-nodeID info from etcd
	for _, kv := range getStateResp.Kvs {
		key := string(kv.Key)
		if !isStateIDKey(key) {
			continue
		}
		var stateMap map[string]string
		err = json.Unmarshal(kv.Value, &stateMap)
		if err != nil {
			log.GetLogger().Errorf("init state map cache, Unmarshal err %s", err.Error())
			continue
		}
		stateWorkersKey := &etcdkey.StateWorkersKey{}
		if err := stateWorkersKey.ParseFrom(key); err != nil {
			log.GetLogger().Errorf("failed to parse from key, error: %s", err.Error())
			return err
		}
		stateID := stateWorkersKey.StateID
		nodeID, ok := stateMap[stateID]
		if !ok || nodeID == "" {
			log.GetLogger().Warnf("nodeID is empty in stateMap, stateID: %s", stateID)
			continue
		}
		// if nodeID is not available, it will delete this key in etcd
		if !scheduler.IsAvailableNode(nodeID) {
			defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
			if err := stateClient.Delete(defaultEtcdCtx, key, clientv3.WithPrefix()); err != nil {
				log.GetLogger().Errorf("failed to delete key in etcd, error: %s", err.Error())
				return err
			}
			log.GetLogger().Infof("succeed to delete %v in etcd", stateMap)
			continue
		}
		s.StateLockMap.Store(key, &sync.Mutex{})
		s.StateCacheMap.Store(key, stateMap)
		log.GetLogger().Debugf("succeed to restore state (%s, %v)", key, stateMap)
	}
	log.GetLogger().Infof("succeed to init stateCacheMap which size is : %d", len(getStateResp.Kvs))

	return nil
}

func (s *StateHandler) updateCacheAndEtcd(request *utils.Request, nodeID, key string) error {
	m := generateStateWorkersValue(request, nodeID)
	value, err := json.Marshal(m)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal map to json, error: %s", err.Error())
		return err
	}
	err = utils.PutStateWorkersToETCDWithRetry(key, value)
	if err != nil {
		log.GetLogger().Errorf("failed to put in etcd, key: %s, traceID: %s, error: %s", key, request.TraceID, err.Error())
		return err
	}
	s.StateCacheMap.Store(key, m)
	return nil
}

func generateStateWorkersValue(request *utils.Request, nodeID string) map[string]string {
	m := make(map[string]string, 11)
	m[request.StateID] = nodeID
	m[constants.TenantIDKey+request.StateID] = request.TenantID
	m[constants.BusinessIDKey+request.StateID] = request.BusinessID
	m[constants.ServiceIDKey+request.StateID] = request.ServiceID
	m[constants.FuncNameKey+request.StateID] = request.FuncName
	m[constants.VersionKey+request.StateID] = request.Version
	m[constants.CreatorKey+request.StateID] = request.Creator
	m[constants.ClusterKey] = utils.GetClusterID()
	m[constants.ApplierKey] = constants.StateManagerAdd
	if request.CreateOptions != nil {
		m[constants.CPUKey+request.StateID] = strconv.FormatInt(request.CreateOptions.CPU, base)
		m[constants.MemoryKey+request.StateID] = strconv.FormatInt(request.CreateOptions.Memory, base)
	}
	return m
}

// Initialize init instance manager
func Initialize(stopChan <-chan struct{}) error {
	handler, err := GetStateHandler()
	if err != nil {
		return err
	}
	err = handler.initStateCacheMap()
	if err != nil {
		return err
	}
	err = handler.watchStateCacheMap(stopChan)
	if err != nil {
		return err
	}
	metadata.Register(GetNodeStateID)

	return resourcemanager.RegisterNodeEvent(resourcemanager.NodeOffline, DeleteStateMappingByNode)
}

// RegisterRouter register router
func RegisterRouter() {
	handleParam := &server.HandleParam{
		HandleFunc: func(g *gin.Context) {
			createStateIDHandler(g.Writer, g.Request)
		},
		Path:   "/createstate",
		Method: "POST",
	}
	server.RegisterRouter(handleParam, "instancemanager")

	handleParamDelete := &server.HandleParam{
		HandleFunc: func(g *gin.Context) {
			deleteStateIDHandler(g.Writer, g.Request)
		},
		Path:   "/deletestate",
		Method: "POST",
	}

	server.RegisterRouter(handleParamDelete, "instancemanagerDelete")
}

func parseRequest(r *http.Request) (*utils.Request, error) {
	contentType, ok := r.Header["Content-Type"]
	if !ok {
		return nil, errors.New("request header not set Content-Type")
	}
	if contentType[0] != "application/json" {
		return nil, errors.New("request header Content-Type should be application/json")
	}
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	result := &utils.Request{}
	if err = json.Unmarshal(body, result); err != nil {
		return nil, err
	}
	if _, err = govalidator.ValidateStruct(result); err != nil {
		return nil, err
	}

	return result, nil
}

func createStateIDHandler(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	err := utils.AuthCheck(r)
	if err != nil {
		utils.ErrorResponse(w, snerror.RequestAuthCheckFail, fmt.Sprintf("failed to check auth, err:%s",
			err.Error()))
		return
	}

	request, err := parseRequest(r)
	if err != nil {
		log.GetLogger().Errorf("failed to parse request, error: %s", err.Error())
		errs := utils.ErrorResponse(w, constant.RequestParamInvalid, constant.CodeText[constant.RequestParamInvalid])
		if errs != nil {
			log.GetLogger().Errorf("errorResponse error: %s", errs.Error())
		}
		return
	}
	createStateLogger := log.GetLogger().With(
		zap.String("traceID", request.TraceID),
		zap.String("function", request.FunctionName))
	createStateLogger.Infof("begin to create stateID with request: %+v", request)
	if !utils.IsStateful(request.FunctionName) {
		createStateLogger.Errorf("the function name(%s) don't have sn service id", request.FunctionName)
		createStateErrorResponse(w, snerror.FunctionNotStateful, "", createStateLogger)
		return
	}
	if err := counter.Incr(request.TenantID); err != nil {
		createStateLogger.Errorf("failed to incr state count, error: %s", err.Error())
		if snErr, ok := err.(snerror.SNError); ok {
			createStateErrorResponse(w, snErr.Code(), snErr.Error(), createStateLogger)
			return
		}
		createStateErrorResponse(w, constant.InternalServerError, "", createStateLogger)
		return
	}
	stateID, nodeID, err := bindStateIDToNodeID(request)
	if err != nil {
		createStateLogger.Errorf("failed to bind stateID to nodeID, error: %s", err.Error())
		createStateErrorResponse(w, constant.InternalServerError, "", createStateLogger)
		counter.Decr(request.TenantID)
		return
	}
	if err := successResponse(w, stateID, nodeID); err != nil {
		createStateLogger.Errorf("successResponse error:%s", err.Error())
	}
	createStateLogger.Infof("create stateID successfully with stateID: %s and nodeID: %s, cost time: %v",
		stateID, nodeID, time.Now().Sub(startTime))
}

func createStateErrorResponse(w http.ResponseWriter, code int, message string, createStateLogger log.FormatLogger) {
	if message == "" {
		message = constant.CodeText[code]
	}
	err := utils.ErrorResponse(w, code, message)
	if err != nil {
		createStateLogger.Errorf("errorResponse error: %s", err.Error())
	}
}

// filterStateID returns stateID and nodeID if there is no error
func filterStateID(handler *StateHandler, request *utils.Request) (string, string, error) {
	var etcdPath string

	// stateID is empty, so that it needs to be generated
	if request.StateID == "0" {
		request.StateID = uuid.New().String()
		createStateLogger := log.GetLogger().With(zap.String("traceID", request.TraceID),
			zap.String("stateID", request.StateID),
			zap.String("function", request.FunctionName))
		createStateLogger.Debugf("Start to update cache and etcd when filter stateID")
		etcdPath = utils.GetETCDPath(request.TenantID, request.ProxyFunction, request.StateID)
		handler.StateLockMap.Store(etcdPath, &sync.Mutex{})
		return updateCacheAndEtcd(handler, request, etcdPath, createStateLogger)
	}

	createStateLogger := log.GetLogger().With(zap.String("traceID", request.TraceID),
		zap.String("stateID", request.StateID),
		zap.String("function", request.FunctionName))

	// stateID is not empty
	etcdPath = utils.GetETCDPath(request.TenantID, request.ProxyFunction, request.StateID)

	stateLock, _ := handler.StateLockMap.LoadOrStore(etcdPath, &sync.Mutex{})
	lock, ok := stateLock.(*sync.Mutex)
	if !ok || lock == nil {
		return "", "", errors.New("failed to convert an interface to a lock")
	}

	lock.Lock()
	defer lock.Unlock()
	for {
		etcdValue, ok := handler.StateCacheMap.LoadOrStore(etcdPath, make(map[string]string))
		if !ok {
			stateID, nodeID, err := updateCacheAndEtcd(handler, request, etcdPath, createStateLogger)
			if err != nil {
				handler.StateCacheMap.Delete(etcdPath)
			}
			return stateID, nodeID, err
		}

		valueMap, ok := etcdValue.(map[string]string)
		if !ok || valueMap == nil {
			handler.StateCacheMap.Delete(etcdPath)
			return "", "", errors.New("failed to convert an interface to a map")
		}
		nodeID, ok := valueMap[request.StateID]
		// last request is processing and need to wait for valid value
		if !ok {
			createStateLogger.Debugf("Conflict occurred. Just wait for valid value with traceID %s, stateID: %s",
				request.TraceID, request.StateID)
			runtime.Gosched()
			continue
		}
		return request.StateID, nodeID, nil
	}
}

func updateCacheAndEtcd(handler *StateHandler, request *utils.Request, etcdPath string,
	createStateLogger log.FormatLogger) (string, string, error) {
	nodeID, err := scheduler.GetPriorityNode(request.FunctionName)
	if err != nil {
		return "", "", err
	}
	startTime := time.Now()
	if err = handler.updateCacheAndEtcd(request, nodeID, etcdPath); err != nil {
		return "", "", err
	}
	createStateLogger.Debugf("update state cache and etcd cost time: %v, stateID: %s, nodeID: %s",
		time.Now().Sub(startTime), request.StateID, nodeID)
	return request.StateID, nodeID, nil
}

func bindStateIDToNodeID(request *utils.Request) (string, string, error) {
	handler, err := GetStateHandler()
	if err != nil {
		return "", "", err
	}

	return filterStateID(handler, request)
}

func successResponse(w http.ResponseWriter, stateID string, nodeID string) error {
	var response utils.Response
	w.Header().Set(utils.HTTPContentType, utils.ContentTypeJSON)
	response.Code = http.StatusOK
	response.StateID = stateID
	response.NodeID = nodeID
	response.Error = ""
	responseJSONBytes, err := json.Marshal(response)
	if err != nil {
		return err
	}
	if _, err = w.Write(responseJSONBytes); err != nil {
		return err
	}
	return nil
}

// GetNodeStateID get all stateID on a node
func GetNodeStateID(nodeID string) (map[string]struct{}, error) {
	stateIDs := make(map[string]struct{}, stateIDsCount)

	stateHandler.StateCacheMap.Range(func(key, value interface{}) bool {
		stateIDMap, ok := value.(map[string]string)
		if !ok {
			log.GetLogger().Errorf("state cache map value reflect to map error")
			return false
		}
		for stateID, nodeIDs := range stateIDMap {
			if nodeIDs == nodeID {
				stateIDs[stateID] = struct{}{}
			}
		}
		return true
	})

	return stateIDs, nil
}

// key: /sn/stateworkers/business/<businessID>/tenant/<tenantID>/function/<functiontask>/version/<version>/<az>/<stateID>
func isStateIDKey(key string) bool {
	strs := strings.Split(key, "/")

	if len(strs) != stateIDEtcdKeyLen {
		return false
	}

	if strs[stateIDEtcdPathTwoIndex] != "stateworkers" || strs[stateIDEtcdPathFiveIndex] != "tenant" ||
		strs[stateIDEtcdPathSevenIndex] != "function" || strs[stateIDEtcdPathNineIndex] != "version" {
		return false
	}

	return true
}

func getStateIDFromETCDKey(key string) string {
	if !isStateIDKey(key) {
		return ""
	}
	strs := strings.Split(key, "/")
	return strs[stateIDEtcdPathIDIndex]
}

func deleteStateIDHandler(w http.ResponseWriter, r *http.Request) {
	log.GetLogger().Debugf("starting delete state")
	startTime := time.Now()
	parsedReq, err := parseRequest(r)
	if err != nil {
		log.GetLogger().Errorf("delete state]parse request error: %s", err.Error())
		errs := utils.ErrorResponse(w, constant.RequestParamInvalid, constant.CodeText[constant.RequestParamInvalid])
		if errs != nil {
			log.GetLogger().Errorf("errorResponse error: %s", errs.Error())
		}
		return
	}
	err = utils.AuthCheck(r)
	if err != nil {
		log.GetLogger().Errorf("failed to check auth: %s", err.Error())
		errs := utils.ErrorResponse(w, snerror.RequestAuthCheckFail, snerror.ErrText(snerror.RequestAuthCheckFail))
		if errs != nil {
			log.GetLogger().Errorf("errorResponse error: %s", errs.Error())
		}
		return
	}
	if err := deleteStateID(parsedReq); err != nil {
		log.GetLogger().Errorf("delete state]delete stateID(%s) error: %s", parsedReq.StateID, err.Error())
		errs := utils.ErrorResponse(w, constant.InternalServerError, constant.CodeText[constant.InternalServerError])
		if errs != nil {
			log.GetLogger().Errorf("errorResponse error: %s", errs.Error())
		}
		return
	}
	counter.Decr(parsedReq.TenantID)
	if err := successResponse(w, "", ""); err != nil {
		log.GetLogger().Errorf("delete stateID(%s) successfully, response error: %s",
			parsedReq.StateID, err.Error())
		return
	}
	log.GetLogger().Debugf("delete stateID(%s) successfully and cost time %v",
		parsedReq.StateID, time.Now().Sub(startTime))
}

// deleteStateID perform delete stateID process for deleteStateIDHandler
func deleteStateID(req *utils.Request) error {
	deleteStateKey := utils.GetETCDPath(req.TenantID, req.ProxyFunction, req.StateID)
	stateDeleteSum := 0
	if stateHandler == nil {
		return errHandlerNil
	}
	stateHandler.StateCacheMap.Range(func(key, value interface{}) bool {
		if key == deleteStateKey {
			stateDeleteSum++
		}
		return true
	})
	log.GetLogger().Debugf("stateID is %v, delete num is %v", req.StateID, stateDeleteSum)
	if stateDeleteSum == 0 {
		log.GetLogger().Infof("stateID is not exist in mapCache, stateID is %v", req.StateID)
		return nil
	}

	notifyCancelTimer(deleteStateKey)

	if err := deleteStateInEtcdAndCache(deleteStateKey); err != nil {
		log.GetLogger().Errorf("delete state in ETCD and cache error, stateIDKey is %s, error is %s",
			deleteStateKey, err.Error())
		return err
	}

	fields := strings.Split(req.FunctionName, funcSeparator)
	if len(fields) == functionNameLength && req.ServiceID == "" {
		req.ServiceID = fields[serviceIndex] + funcSeparator + fields[prefixIndex]
		req.FunctionName = fields[functionNameIndex]
	}

	log.GetLogger().Infof("delete state in ETCD and cache succeed, stateIDKey is %s, stateID is %s, funcName: %s",
		deleteStateKey, req.StateID, req.FunctionName)

	if req.FunctionDelete {
		utils2.CleanStateInRedis(req.TenantID, req.ServiceID, req.FunctionName, req.StateID)
	}

	return nil
}

// deleteStateInEtcdAndCache delete state in etcd and cache
func deleteStateInEtcdAndCache(deleteStateIDKey string) error {
	if deleteStateIDKey == "" {
		return errors.New("deleteStateIDKey is nil")
	}
	handler, err := GetStateHandler()
	if err != nil {
		return err
	}

	if stateLock, ok := handler.StateLockMap.Load(deleteStateIDKey); ok {
		lock, ok := stateLock.(*sync.Mutex)
		if !ok || lock == nil {
			return errors.New("failed to convert an interface to a lock")
		}

		lock.Lock()
		stateClient := etcd3.GetStateEtcdClient()
		if stateClient == nil || stateClient.Client == nil {
			log.GetLogger().Warnf("failed to init state ETCD client")
			lock.Unlock()
			return errors.New("failed to init state ETCD client")
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		err = stateClient.Delete(defaultEtcdCtx, deleteStateIDKey, clientv3.WithPrefix())
		if err != nil {
			lock.Unlock()
			return err
		}

		handler.StateLockMap.Delete(deleteStateIDKey)
		handler.StateCacheMap.Delete(deleteStateIDKey)
		lock.Unlock()
	} else {
		log.GetLogger().Warnf("delete State in ETCD and cache error, cannot find stateIDKey %s", deleteStateIDKey)
	}

	log.GetLogger().Infof("delete stateIDKey(%s) successfully", deleteStateIDKey)
	return nil
}

// DeleteStateMappingByNode register NodeDown event and delete state
func DeleteStateMappingByNode(params interface{}) {
	deletedNodeID, ok := params.(string)
	if !ok {
		log.GetLogger().Errorf("nodeID params to string error")
		return
	}
	log.GetLogger().Infof("node event is %s", deletedNodeID)
	var deleteNodeIDList []string
	stateHandler.StateCacheMap.Range(func(key, value interface{}) bool {
		stateIDKey, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("state cache map key reflect to string error")
			return false
		}
		stateIDMap, ok := value.(map[string]string)
		if !ok {
			log.GetLogger().Errorf("state cache map value reflect to map error")
			return false
		}
		for _, nodeID := range stateIDMap {
			if nodeID == deletedNodeID {
				deleteNodeIDList = append(deleteNodeIDList, stateIDKey)
			}
		}
		return true
	})
	for _, deleteStateIDKey := range deleteNodeIDList {
		if err := deleteStateInEtcdAndCache(deleteStateIDKey); err != nil {
			log.GetLogger().Errorf("delete State in ETCD and cache error, node is %s, stateIDKey is %s, error is %s",
				deletedNodeID, deleteStateIDKey, err.Error())
			return
		}
	}
	log.GetLogger().Infof("delete NodeDown event successfully")
}

func (s *StateHandler) watchStateCacheMap(stopChan <-chan struct{}) error {
	go s.RunWatcher(stopChan)
	return nil
}

// RunWatcher start a etcd watcher
func (s *StateHandler) RunWatcher(stopCh <-chan struct{}) {
	watchChan := createWatcher(s.StateEtcd.EtcdClt)
	if stopCh == nil {
		log.GetLogger().Error("state-manager stopCh is nil")
		return
	}
	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if ok {
				s.parseStateWorkersEvent(event)
				continue
			}
			watchChan = s.StateEtcd.EtcdClt.Watch(context.Background(), "/sn/stateworkers/business/", 0, true)
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Warnf("stop channel is closed, return")
				return
			}
			log.GetLogger().Infof("received signal to quit")
			return
		}
	}
}

func createWatcher(stateClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if stateClient.Client == nil {
				log.GetLogger().Warnf("failed to init state etcd client")
				continue
			}
			watchChan := stateClient.Watch(context.Background(), "/sn/stateworkers/business/", 0, true)
			ticker.Stop()
			return watchChan
		}
	}
}

func (s *StateHandler) parseStateWorkersEvent(event *etcd3.Event) {
	switch event.Type {
	case etcd3.PUT:
		s.handlePutEvent(event)
	case etcd3.DELETE:
		s.handleDeleteEvent(event)
	default:
		log.GetLogger().Infof("unknown event type: %d", event.Type)
	}
}

func (s *StateHandler) handlePutEvent(event *etcd3.Event) {
	key := event.Key
	if !isStateIDKey(key) {
		return
	}
	var stateMap map[string]string
	err := json.Unmarshal(event.Value, &stateMap)
	if err != nil {
		log.GetLogger().Errorf("handle put event, Unmarshal err %s ", err.Error())
		return
	}
	stateWorkersKey := &etcdkey.StateWorkersKey{}
	if err := stateWorkersKey.ParseFrom(key); err != nil {
		log.GetLogger().Errorf("failed to parse put event from key, error: %s", err.Error())
		return
	}
	stateID := stateWorkersKey.StateID
	nodeID, ok := stateMap[stateID]
	if !ok || nodeID == "" {
		log.GetLogger().Warnf("nodeID is empty for put event in stateMap, stateID: %s", stateID)
		return
	}

	notifyCancelTimer(stateID)

	clusterID := stateMap[constants.ClusterKey]
	if clusterID == "" && clusterstatusutil.IsCurrentClustersStatusRunning() {
		log.GetLogger().Infof("handle etcd put event, try to compete this state. key: %v, stateMap: %v", key, stateMap)
		competeErr := CompeteStateAndTryToUpdate(stateID, nodeID)
		if competeErr != nil {
			log.GetLogger().Warnf("CompeteStateAndTryToUpdate failed. stateID: %s, error: %s", stateID, competeErr.Error())
		}
		return
	}
	_, ok = s.StateLockMap.LoadOrStore(key, &sync.Mutex{})
	if !ok {
		log.GetLogger().Warnf("StateLockMap stored a new key: %s", key)
	}
	s.StateCacheMap.Store(key, stateMap)
	log.GetLogger().Infof("handle etcd put event, key: %v, stateMap: %v", key, stateMap)
}

func notifyCancelTimer(stateID string) {
	if stateLock, exist := stateIDChLockMap.Load(stateID); exist {
		lock, ok := stateLock.(*sync.Mutex)
		if !ok || lock == nil {
			// Theoretically, this won't happen.
			log.GetLogger().Errorf("failed to convert an interface to a lock when notifyCancelTimer, stateID: %s", stateID)
			return
		}
		lock.Lock()
		defer lock.Unlock()
		channel, channelExist := stateIDChannels[stateID]
		if channelExist {
			log.GetLogger().Infof("notifyCancelTimer, stateID %s", stateID)
			channel <- true
		}
	}
}

func (s *StateHandler) handleDeleteEvent(event *etcd3.Event) {
	deleteStateIDKey := event.Key
	stateID := utils2.GetStateIDByPath(deleteStateIDKey)
	notifyCancelTimer(stateID)
	if stateLock, ok := s.StateLockMap.Load(deleteStateIDKey); ok {
		lock, ok := stateLock.(*sync.Mutex)
		if !ok || lock == nil {
			return
		}
		lock.Lock()
		s.StateLockMap.Delete(deleteStateIDKey)
		s.StateCacheMap.Delete(deleteStateIDKey)
		lock.Unlock()
	} else {
		log.GetLogger().Warnf("delete event error, cannot find stateIDKey %s", deleteStateIDKey)
	}
}

// GetStateCacheMapNum returns the number of stateID
func GetStateCacheMapNum() int {
	mapNum := 0
	if stateHandler == nil {
		return -1
	}
	stateHandler.StateCacheMap.Range(func(k, v interface{}) bool {
		mapNum++
		return true
	})
	return mapNum
}

// DeleteStateIDHandlerByManager delete state for bridgeManager
func DeleteStateIDHandlerByManager(req *utils.Request) error {
	log.GetLogger().Debugf("starting delete state called by BridgeManager")
	startTime := time.Now()
	if err := deleteStateID(req); err != nil {
		log.GetLogger().Errorf("delete state]delete stateID(%s) error: %s", req.StateID, err.Error())
		return err
	}
	counter.Decr(req.TenantID)
	log.GetLogger().Debugf("delete stateID(%s) successfully and cost time %v",
		req.StateID, time.Now().Sub(startTime))
	return nil
}

// GetStateIDListOfCluster returns the stateID in the cluster
func GetStateIDListOfCluster() []string {
	var stateIDList []string
	if stateHandler == nil {
		return []string{}
	}
	stateHandler.StateCacheMap.Range(func(key, value interface{}) bool {
		stateIDKey, ok := key.(string)
		if !ok {
			log.GetLogger().Errorf("state cache map key reflect to string error")
			return false
		}
		stateID := getStateIDFromETCDKey(stateIDKey)
		if stateID == "" {
			log.GetLogger().Errorf("state cache map key is not corresponding to state id format, stateIDKey %s", stateIDKey)
			return false
		}

		stateIDMap, ok := value.(map[string]string)

		if !ok {
			log.GetLogger().Errorf("GetStateCacheMapOfCluster: state cache map value reflect to map error")
			return false
		}
		// key maybe stateID or cluster/cpu/creator/serviceId...
		for keys, values := range stateIDMap {
			if keys == constants.ClusterKey && values == utils.GetClusterID() {
				stateIDList = append(stateIDList, stateID)
			}
		}
		return true
	})
	return stateIDList
}

// MigrateStateIDToOtherCluster set cluster empty
func MigrateStateIDToOtherCluster(stateID string) bool {
	if stateHandler == nil {
		return false
	}
	stateIDKey := utils.GetSNStateWorkerPath(stateID)
	if val, exist := stateHandler.StateCacheMap.Load(stateIDKey); exist {
		stateIDMap, ok := val.(map[string]string)
		if !ok {
			log.GetLogger().Errorf("MigrateStateIDToOtherCluster: "+
				"state cache map value reflect to map error, stateID: %s", stateID)
			return false
		}
		stateLock, _ := stateHandler.StateLockMap.LoadOrStore(stateIDKey, &sync.Mutex{})
		lock, ok := stateLock.(*sync.Mutex)
		if !ok || lock == nil {
			log.GetLogger().Warnf("failed to convert an interface to a lock, stateID %s", stateID)
			return false
		}

		// Avoid concurrent delete/modify state request
		lock.Lock()
		defer lock.Unlock()

		// other cluster's statemanager will compete the stateID which cluster is empty
		stateIDMap[constants.ClusterKey] = ""
		value, err := json.Marshal(stateIDMap)
		if err != nil {
			log.GetLogger().Errorf("json marshal value map error: %s, stateID %s", err.Error(), stateID)
			return false
		}
		stateHandler.StateCacheMap.Store(stateIDKey, stateIDMap)
		err = utils.PutStateWorkersToETCDWithRetry(stateIDKey, value)
		if err != nil {
			log.GetLogger().Errorf("Put stateIDMap to etcd error, stateID: %s, key: %s, error: %s",
				stateID, stateIDKey, err.Error())
			return false
		}
		return true
	}
	log.GetLogger().Warnf("This stateID %s does not exist in StateCacheMap", stateID)
	return false
}

// UpdateEtcdWithStateAndNode -
func UpdateEtcdWithStateAndNode(newNode string, state string) error {
	log.GetLogger().Infof("new node is: %s, stateID: %s", newNode, state)
	for i := 0; i < etcdCompetitionRetrial; i++ {
		stateRouteKey := fmt.Sprintf("%s%s", stateRoutePrefix, state)
		stateClient := etcd3.GetStateEtcdClient()
		if stateClient == nil || stateClient.Client == nil {
			log.GetLogger().Warnf("failed to init state ETCD client")
			return errors.New("failed to init state ETCD client")
		}
		defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
		getResp, err := stateClient.GetResponse(defaultEtcdCtx, stateRouteKey)
		if err != nil {
			log.GetLogger().Warnf("failed to get route info from etcd: %s", err.Error())
			continue
		}
		if len(getResp.Kvs) == 0 {
			log.GetLogger().Warnf("state route key is not exist, key: %s", stateRouteKey)
			continue
		}
		stateRouteData := map[string]string{}
		if err = json.Unmarshal(getResp.Kvs[0].Value, &stateRouteData); err != nil {
			log.GetLogger().Errorf("failed to unmarshal state route, key: %s, err: %s", stateRouteKey, err.Error())
			return err
		}
		stateRouteData[state] = newNode
		stateRouteData[constants.ClusterKey] = utils.GetClusterID()
		stateRouteData[constants.ApplierKey] = constants.StateManagerUpdate
		value, err := json.Marshal(stateRouteData)
		if err != nil {
			log.GetLogger().Errorf("failed to marshal new state route: %s", err.Error())
			return err
		}
		putEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcdCompetitionTimeout)
		putErr := stateClient.Put(putEtcdCtx, stateRouteKey, string(value))
		if putErr != nil {
			log.GetLogger().Warnf("failed to rewrite node info to etcd: %s", putErr.Error())
			continue
		}
		log.GetLogger().Infof("succeeded to put etcd: %s, state: %s, node: %s", stateRouteKey, state,
			newNode)
		break
	}
	return nil
}

// CompeteStateAndTryToUpdate -
func CompeteStateAndTryToUpdate(stateID string, oldNodeID string) error {
	oldKey := fmt.Sprintf("%s-%s", stateID, oldNodeID)
	log.GetLogger().Infof("start to process key %s", oldKey)
	if utils.GetRedisCmd() == nil {
		log.GetLogger().Error("redis is not ready when compete state and try to update")
		return errors.New("redis is not ready when compete state and try to update")
	}
	ok, lockErr := utils.GetRedisCmd().SetNX(context.TODO(), oldKey, "state possession",
		redisContextTimeout).Result()
	if lockErr == nil && ok {
		log.GetLogger().Infof("succeeded to compete redis lock for %s", oldKey)
		newNode, err := scheduler.GetPriorityNodeWithoutFuncName()
		if err != nil {
			sendAlarm := sendAlarmWhenFailedSeveralTimes(stateID, oldKey)
			log.GetLogger().Errorf("failed to get a priority node without funcName, stateID: %s, error: %s",
				stateID, err.Error())
			_, delErr := utils.GetRedisCmd().Del(context.TODO(), oldKey).Result()
			if delErr != nil {
				log.GetLogger().Warnf("failed to release distributed lock, key:%s, error: %s", oldKey, err.Error())
				// If we release the lock failed, other clusters cannot acquire the distributed lock
				// so we should sleep for redisContextTimeout
				time.Sleep(redisContextTimeout)
			}
			if !sendAlarm {
				return tryToMigrateStateIDToOtherCluster(stateID, err)
			}
			log.GetLogger().Infof("compete state and try to update several times, no need to put %s to ETCD", stateID)
			return nil
		}
		clearAlarmWhenUpdateStateIDNodeSucceed(stateID, oldKey)
		err = UpdateEtcdWithStateAndNode(newNode, stateID)
		if err != nil {
			return err
		}
	} else {
		log.GetLogger().Warnf("failed to compete redis lock for %s", oldKey)
	}
	return nil
}

func tryToMigrateStateIDToOtherCluster(stateID string, err error) error {
	result := MigrateStateIDToOtherCluster(stateID)
	log.GetLogger().Infof("migrate stateID %s to other cluster, result %t", stateID, result)
	if result {
		// Successful migration, equivalent to no errors
		return nil
	}
	return err
}

func clearAlarmWhenUpdateStateIDNodeSucceed(stateID string, oldKey string) {
	stateIDWithoutNewNodeMap.Delete(stateID)
	ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
	defer cancel()
	delCount, delErr := utils.GetRedisCmd().Del(ctx, fmt.Sprintf("failed-%s", oldKey)).Result()
	if delErr != nil {
		log.GetLogger().Warnf("failed to del %s from redis, %s", fmt.Sprintf("failed-%s", oldKey), delErr.Error())
		return
	}
	if delCount != 0 {
		log.GetLogger().Infof("clear alarm when update stateID %s node succeed", stateID)
		go alarm.GetClient().ClearAlarm(alarm.MigrateStateIDFailed)
	}
}

func sendAlarmWhenFailedSeveralTimes(stateID string, oldKey string) bool {
	failedCountVal, ok := stateIDWithoutNewNodeMap.Load(stateID)
	var failedCount = 1
	if ok {
		failedCount = failedCountVal.(int) + 1
	}
	stateIDWithoutNewNodeMap.Store(stateID, failedCount)
	if failedCount >= failedCountThreshold {
		ctx, cancel := context.WithTimeout(context.Background(), redisContextTimeout)
		defer cancel()
		// failed-stateID-nodeID：cluster001
		result, err := utils.GetRedisCmd().SetNX(ctx, fmt.Sprintf("failed-%s", oldKey),
			utils.GetClusterID(), failedCountRedisKeyTTL).Result()
		if err != nil {
			log.GetLogger().Warnf("failed to set %s to redis, %s", fmt.Sprintf("failed-%s", oldKey), err.Error())
		}
		if result {
			go alarm.GetClient().ReportAlarm(alarm.MigrateStateIDFailed,
				fmt.Sprintf("failed to get a priority node for %s", stateID))
		}
		// When an exception occurs in redis, the stateID is no longer written to the ETCD,
		// assuming an alert has been reported
		return true
	}
	return false
}

// ConstructTimerForCompetitionOfStateID -
func ConstructTimerForCompetitionOfStateID(stateID string, nodeID string) {
	stateLock, _ := stateIDChLockMap.LoadOrStore(stateID, &sync.Mutex{})
	lock, ok := stateLock.(*sync.Mutex)
	if !ok || lock == nil {
		log.GetLogger().Errorf("failed to convert an interface to a lock when ConstructTimerForCompetitionOfStateID,"+
			"stateID: %s", stateID)
		return
	}
	lock.Lock()
	if stateIDChannels[stateID] != nil {
		log.GetLogger().Infof("The channel of stateID %s has been existed, "+
			"this means that the timer has already been started", stateID)
		lock.Unlock()
		return
	}

	timer := time.NewTimer(stateCompetitionTimeout)
	channel := make(chan bool, 1)
	stateIDChannels[stateID] = channel
	lock.Unlock()
	select {
	case <-timer.C:
		log.GetLogger().Infof("Try to CompeteStateAndTryToUpdate, stateID: %s", stateID)
		err := CompeteStateAndTryToUpdate(stateID, nodeID)
		if err != nil {
			log.GetLogger().Errorf("Failed to CompeteStateAndTryToUpdate for stateID %s, error: %s", stateID, err.Error())
		}
	case <-channel:
		log.GetLogger().Infof("Stop timer for stateID %s", stateID)
		timer.Stop()
	}
	lock.Lock()
	delete(stateIDChannels, stateID)
	lock.Unlock()
	stateIDChLockMap.Delete(stateID)
}
