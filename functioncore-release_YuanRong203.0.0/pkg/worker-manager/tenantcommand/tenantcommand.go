/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package tenantcommand define tenant command manager function
package tenantcommand

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/valyala/fasthttp"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/urnutils"
	"functioncore/pkg/worker-manager/apis/faas/v1alpha1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/iam"
	"functioncore/pkg/worker-manager/utils"
)

const (
	watchPathPrefix              = "/sn/tenantcommand/"
	keepConnAliveTTL             = 3 * time.Second
	keyElementsLength            = 6
	readEventChannelTaskLastTime = 600 * time.Second
	tenantCommandExecuteTimeout  = 300
	workerPort                   = "8081"
	maxTaskNumberPerFunction     = 100
)

var (
	tenantCommandManager *manager
)

type manager struct {
	workerClient           *fasthttp.Client
	cfg                    *config.FaasConfig
	functionThreadMap      *sync.Map
	functionTreadCrateLock *sync.Mutex
	stopCh                 <-chan struct{}
}

// watchOption is the option for watcher
type watchOption struct {
	Path          string
	StopCh        <-chan struct{}
	ETCDSelection etcd3.EtcdType
}

type tenantCommandEtcdKey struct {
	projectID        string
	funcURN          string
	taskID           string
	businessID       string
	tenantID         string
	funcName         string
	funcVersion      string
	fullFunctionName string
}

type tenantCommandExecuteResult struct {
	result  string
	podName string
}

// parseFromKey: key example as follows: /sn/tenantcommand/{projectID}/{funcURN}/{taskID}
func parseFromKey(key string) tenantCommandEtcdKey {
	elements := strings.Split(key, "/")
	if len(elements) != keyElementsLength {
		log.GetLogger().Errorf("failed to parse etcd key %s", key)
		return tenantCommandEtcdKey{}
	}
	// for example, sn:cn:yrk:5cb29fa73b564c90b9878a6bf8e10d78:function:0@default@test_java_log:latest
	funcURN := elements[4]
	items := strings.Split(funcURN, ":")
	if len(items) < urnutils.URNLenWithVersion {
		log.GetLogger().Errorf("failed to parse funcURN %s", funcURN)
		return tenantCommandEtcdKey{}
	}
	factors := strings.Split(items[utils.URNIndexFive], "@")
	if len(factors) <= utils.URNIndexTwo {
		log.GetLogger().Errorf("failed to parse Urn with funcURN: %s", funcURN)
		return tenantCommandEtcdKey{}
	}

	return tenantCommandEtcdKey{
		projectID:        elements[utils.URNIndexThree],
		funcURN:          funcURN,
		taskID:           elements[utils.URNIndexFive],
		businessID:       items[utils.URNIndexTwo],
		tenantID:         items[utils.URNIndexThree],
		funcName:         factors[utils.URNIndexTwo],
		funcVersion:      items[utils.URNIndexSix],
		fullFunctionName: items[utils.URNIndexFive],
	}
}

// Event tenant commands event
type Event struct {
	tenantCommandEtcdKey tenantCommandEtcdKey
	tenantCommandResult  *types.TenantCommandDeploymentSpec
	resultKey            string
	etcdType             etcd3.EtcdType
}

// InitManager init manager and enable checkpoint function
func InitManager(workerClient *fasthttp.Client, stopCh <-chan struct{}) error {
	cfg, err := config.GetConfig()
	if err != nil {
		return err
	}
	if stopCh == nil {
		return errors.New("stopCh should not be nil")
	}
	tenantCommandManager = &manager{
		workerClient:           workerClient,
		cfg:                    cfg,
		functionThreadMap:      &sync.Map{},
		functionTreadCrateLock: &sync.Mutex{},
		stopCh:                 stopCh,
	}
	tenantCommandManager.runWatcher(stopCh)
	log.GetLogger().Infof("init tenantCommand manager success")
	return nil
}

func (m *manager) runWatcher(stopCh <-chan struct{}) {
	go m.processTenantCommandWatcher(&watchOption{
		ETCDSelection: etcd3.Meta,
		Path:          watchPathPrefix,
		StopCh:        stopCh,
	})
}

func getWatchChan(metaClient *etcd3.EtcdClient) *etcd3.EtcdWatchChan {
	ticker := time.NewTicker(keepConnAliveTTL)
	for {
		select {
		case <-ticker.C:
			if metaClient.Client == nil {
				log.GetLogger().Warnf("failed to init meta etcd client")
				continue
			}
			ctx := context.WithValue(context.Background(), etcd3.CellEnable, true)
			watchChan := metaClient.Watch(ctx, watchPathPrefix, 0, true)
			log.GetLogger().Infof("success to init meta etcd watch channel")
			ticker.Stop()
			return watchChan
		}
	}
}

func (m *manager) handleTenantCommandEtcdEvent(tenantCommandEvent *Event) {
	if tenantCommandEvent == nil {
		return
	}
	tenantCommandKey := tenantCommandEvent.tenantCommandEtcdKey
	funcSvcID := utils.CrName(tenantCommandKey.businessID, tenantCommandKey.tenantID,
		tenantCommandKey.fullFunctionName, tenantCommandKey.funcVersion)
	m.functionTreadCrateLock.Lock()
	defer m.functionTreadCrateLock.Unlock()
	value, exists := m.functionThreadMap.Load(funcSvcID)
	if !exists {
		eventChannel := make(chan *Event, maxTaskNumberPerFunction)
		go m.readEventChannelTask(funcSvcID, m.stopCh, eventChannel)
		m.functionThreadMap.Store(funcSvcID, eventChannel)
		eventChannel <- tenantCommandEvent
	} else {
		eventChannel, ok := value.(chan *Event)
		if !ok {
			return
		}
		eventChannel <- tenantCommandEvent
	}
	log.GetLogger().Infof("success to set tenant command meta for func %s with key %s", funcSvcID,
		tenantCommandEvent.resultKey)
}

func (m *manager) readEventChannelTask(funcSvcID string, stopCh <-chan struct{}, eventChannel chan *Event) {
	startTime := time.Now()
	for {
		select {
		case _, ok := <-stopCh:
			if !ok {
				log.GetLogger().Infof("received system signal to quit")
				return
			}
		case event, ok := <-eventChannel:
			if !ok {
				log.GetLogger().Warnf("eventChannel has been close")
				return
			}
			// receive event, refresh startTime
			startTime = time.Now()

			m.handleTenantCommand(funcSvcID, event)
		default:
			if time.Now().Sub(startTime) < readEventChannelTaskLastTime {
				continue
			}
			log.GetLogger().Infof("long time no receive signal, task need to end")
			m.functionTreadCrateLock.Lock()
			m.functionThreadMap.Delete(funcSvcID)
			close(eventChannel)
			m.functionTreadCrateLock.Unlock()
			return
		}
	}
}

func (m *manager) handleTenantCommand(funcSvcID string, event *Event) {
	tenantCommandResult := event.tenantCommandResult
	taskID := tenantCommandResult.TaskID
	log.GetLogger().Infof("begin to execute tenant command, taskID: %s", tenantCommandResult.TaskID)
	executePodList, err := generateExecutePodList(taskID, funcSvcID, tenantCommandResult.Mode,
		tenantCommandResult.PodName)
	if err != nil {
		log.GetLogger().Errorf("failed to execute tenant command, taskID: %s, error: %s",
			tenantCommandResult.TaskID, err.Error())
		return
	}
	if len(executePodList) == 0 {
		log.GetLogger().Infof("task %s has no pod satisfy requirement to execute tenant command", taskID)
		return
	}
	executeTime := event.tenantCommandResult.Timeout
	if executeTime <= 0 {
		executeTime = tenantCommandExecuteTimeout
		event.tenantCommandResult.Timeout = tenantCommandExecuteTimeout
	}

	var wg sync.WaitGroup
	executePodNumber := len(executePodList)
	resultChan := make(chan *tenantCommandExecuteResult, executePodNumber)

	for _, executePod := range executePodList {
		wg.Add(1)
		go m.executeTenantCommandInPod(executePod, resultChan, taskID, event, &wg)
	}

	go func(wg *sync.WaitGroup) {
		defer close(resultChan)
		wg.Wait()
	}(&wg)

	for result := range resultChan {
		log.GetLogger().Infof("task %s execute result in pod %s is: %s", taskID, result.podName, result.result)
	}
}

func (m *manager) executeTenantCommandInPod(executePod v1alpha1.WorkerInstance,
	resultChan chan<- *tenantCommandExecuteResult, taskID string, event *Event, wg *sync.WaitGroup) {
	defer wg.Done()
	req, response := fasthttp.AcquireRequest(), fasthttp.AcquireResponse()
	scheme := "https"
	if m.cfg.HTTPSConfig == nil || !m.cfg.HTTPSConfig.HTTPSEnable {
		scheme = "http"
	}

	req.SetRequestURI(fmt.Sprintf("%s//%s:%s%s", scheme, executePod.IP, workerPort,
		constants.ExecuteTenantCommands))
	req.Header.SetMethod(fasthttp.MethodPost)
	req.Header.Set(constants.HeaderTraceID, taskID)
	iam.SetIAMTokenRequestHeader(req, event.tenantCommandEtcdKey.tenantID, event.tenantCommandEtcdKey.funcURN)

	executeResult := &tenantCommandExecuteResult{podName: executePod.PodName, result: "success"}

	tenantCommandDeploymentSpec := event.tenantCommandResult
	tenantCommandStruct := types.TenantCommandStruct{
		TaskID:                 tenantCommandDeploymentSpec.TaskID,
		CreateTime:             tenantCommandDeploymentSpec.CreateTime,
		Path:                   tenantCommandDeploymentSpec.Path,
		Command:                tenantCommandDeploymentSpec.Command,
		TenantCommandValuesMap: m.cfg.TenantCommandValuesMap,
		Timeout:                tenantCommandDeploymentSpec.Timeout,
	}
	bodyValue, err := json.Marshal(tenantCommandStruct)
	if err != nil {
		executeResult.result = "failed to marshal tenantCommandDeploymentSpec to byte"
		resultChan <- executeResult
		return
	}
	req.SetBody(bodyValue)
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(response)
	req.Header.ResetConnectionClose()
	authorization, timestamp := localauth.SignLocally(m.cfg.LocalAuth.AKey, m.cfg.LocalAuth.SKey,
		utils.AppID, m.cfg.LocalAuth.Duration)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.Authorization, authorization)

	requestTimout := time.Duration(event.tenantCommandResult.Timeout) * time.Second
	log.GetLogger().Infof("send %s execute_tenant_commands request to worker %s", taskID, executePod.PodName)
	if err = m.workerClient.DoTimeout(req, response, requestTimout); err != nil {
		executeResult.result = fmt.Sprintf("failed to send execute_tenant_commands request to worker %s, err: %s",
			executePod.IP, err.Error())
		resultChan <- executeResult
		return
	}

	handleTenantCommandResponse(response, resultChan, executeResult)
}

func handleTenantCommandResponse(response *fasthttp.Response, resultChan chan<- *tenantCommandExecuteResult,
	executeResult *tenantCommandExecuteResult) {
	if response.Header.StatusCode() == fasthttp.StatusOK {
		resultChan <- executeResult
		return
	}
	if response.Header.StatusCode() == fasthttp.StatusNotFound {
		executeResult.result = "404 not found, worker unsupported /execute_tenant_commands request"
		resultChan <- executeResult
		return
	}
	if response.Body() != nil {
		executeResult.result = fmt.Sprintf("failed to execute tenant command, response: %s",
			string(response.Body()))
	} else {
		executeResult.result = "failed to execute tenant command"
	}

	resultChan <- executeResult
}

func generateExecutePodList(taskID string, funcSvcID string, mode string,
	podNameList []string) ([]v1alpha1.WorkerInstance, error) {
	if mode != "function" && mode != "pod" {
		return nil, errors.New("unknown task mode type")
	}
	podNameListStr := ""
	if mode == "pod" {
		if podNameList == nil || len(podNameList) == 0 {
			return nil, errors.New("mode is pod, but podName list len is 0")
		}
		podNameListStr = strings.Join(podNameList, ",")
	}

	funcionInstanceList := cache.WorkerCache.GetInstanceList(funcSvcID)
	resultInstanceList := make([]v1alpha1.WorkerInstance, 0, len(funcionInstanceList))
	var resultPodNameList []string

	for _, instance := range funcionInstanceList {
		if !instance.IsRunning {
			log.GetLogger().Infof("pod %s is not ready, can not execute tenant command", instance.PodName)
			continue
		}
		if mode == "pod" && !strings.Contains(podNameListStr, instance.PodName) {
			log.GetLogger().Infof("pod %s is not in podName list, throw it", instance.PodName)
			continue
		}

		resultInstanceList = append(resultInstanceList, instance)
		resultPodNameList = append(resultPodNameList, instance.PodName)
	}
	log.GetLogger().Infof("task %s meets the requirements pod list: %s", taskID,
		strings.Join(resultPodNameList, ", "))

	return resultInstanceList, nil
}

func (m *manager) processTenantCommandWatcher(opt *watchOption) {
	etcdClient := etcd3.GetETCDClient(opt.ETCDSelection)
	if etcdClient == nil {
		log.GetLogger().Warnf("ETCDClient is nil, ETCDSelection is %s", opt.ETCDSelection.String())
		return
	}
	watchChannel := getWatchChan(etcdClient)
	for {
		select {
		case _, ok := <-opt.StopCh:
			if !ok {
				log.GetLogger().Infof("received signal to quit")
				return
			}
		case event, ok := <-watchChannel.ResultChan:
			if !ok {
				log.GetLogger().Warnf("the channel received the result may be closed and try to reconnect")
				watchChannel.Stop()
				ctx := context.WithValue(context.Background(), etcd3.CellEnable, true)
				watchChannel = etcdClient.Watch(ctx, opt.Path, 0, true)
				continue
			}

			tenantCommandEvent, err := parseTenantCommandEvent(event, opt.ETCDSelection)
			if err != nil {
				log.GetLogger().Errorf("parse tenant command etcd event fail: %s", err.Error())
				continue
			}

			m.handleTenantCommandEtcdEvent(tenantCommandEvent)
		}
	}
}

func parseTenantCommandEvent(event *etcd3.Event, etcdType etcd3.EtcdType) (*Event, error) {
	switch event.Type {
	case etcd3.PUT:
		log.GetLogger().Infof("receive tenantCommand put event, key: %s", event.Key)
		eventObj := &Event{}
		eventObj.resultKey = event.Key
		eventObj.etcdType = etcdType
		keyObj := parseFromKey(event.Key)
		if keyObj.funcURN == "" {
			errorMsg := fmt.Sprintf("failed to parse from etcd event key: %s", event.Key)
			return eventObj, errors.New(errorMsg)
		}
		eventObj.tenantCommandEtcdKey = keyObj
		tenantCommandDeployment := &types.TenantCommandDeploymentSpec{}
		if err := json.Unmarshal(event.Value, tenantCommandDeployment); err != nil {
			errorMsg := fmt.Sprintf("failed to unmarshal the json, error: %s", err.Error())
			return eventObj, errors.New(errorMsg)
		}
		eventObj.tenantCommandResult = tenantCommandDeployment
		return eventObj, nil
	default:
		log.GetLogger().Infof("unconcerned event type: %d", event.Type)
		return nil, nil
	}
}
