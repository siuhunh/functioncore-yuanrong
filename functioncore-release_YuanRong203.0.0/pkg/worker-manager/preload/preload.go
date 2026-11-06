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

// Package preload -
package preload

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/panjf2000/ants/v2"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/cffclient"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/utils"
)

const (
	cffAgentPost = "28930"
	// /v2/{project_id}/fgs/i/cold-start-preload/{file_type}
	preloadPOSTAPIFormat = "/v2/%s/fgs/i/cold-start-preload/%s"
	// /v2/{proejct_id}/fgs/i/cold-start-preload/{file_type}/{file_name}
	preloadDELETEAPIFormat = "/v2/%s/fgs/i/cold-start-preload/%s/%s"
	batchSize              = 10

	addPreloadTimeOut                      = 30 * time.Second
	maxBlockingTasks                       = 20000
	maxNumAddPreloadTaskRun                = 10
	maxNumDeletePreloadTaskRun             = 100
	checkIsExistsRequestFailedNodeInterval = 30 * time.Second
	checkCffAgentReadyCountLimit           = 30
	checkCffAgentReadySleepTime            = 6 * time.Second
	preloadToAllNodesTaskTimeout           = 30 * time.Minute

	reachPreloadDiskLimitCode       = "FSS_100401"
	reachTenantPreloadDiskLimitCode = "FSS_100402"
)

var (
	client        *Client
	preloadEnable = os.Getenv("PRELOAD_ENABLE")
)

// CreateClient -
func CreateClient(stopCh <-chan struct{}) error {
	if preloadEnable != "true" {
		log.GetLogger().Warn("preload is not enable")
		return nil
	}
	alarm.GetClient().RegisterAlarmType(alarm.PreloadRequest)
	alarm.GetClient().RegisterAlarmType(alarm.PreloadWorkerPanic)
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config with error: %s", err.Error())
		return err
	}
	cffClient, err := cffclient.NewClient(addPreloadTimeOut, cffclient.WithTLSConfig(tls.GetClientTLSConfig()),
		cffclient.WithAuth(cfg.LocalAuth.AKey, cfg.LocalAuth.SKey))
	if err != nil {
		log.GetLogger().Errorf("failed to new cff client, error: %s", err.Error())
		return err
	}
	addTaskPool, err := ants.NewPool(maxNumAddPreloadTaskRun, ants.WithPreAlloc(true),
		ants.WithMaxBlockingTasks(maxBlockingTasks))
	if err != nil {
		log.GetLogger().Errorf("failed to create add preload task pool, error: %s", err.Error())
		return err
	}
	deleteTaskPool, err := ants.NewPool(maxNumDeletePreloadTaskRun,
		ants.WithMaxBlockingTasks(maxBlockingTasks))
	if err != nil {
		log.GetLogger().Errorf("failed to create delete preload task pool, error: %s", err.Error())
		return err
	}
	client = &Client{
		cffClient:      cffClient,
		addTaskPool:    addTaskPool,
		deleteTaskPool: deleteTaskPool,
	}
	go periodCheckIsExistsFailedNode(stopCh)
	return nil
}

// IsEnable --
func IsEnable() bool {
	if preloadEnable == "true" {
		return true
	}
	return false
}

// GetClient -
func GetClient() *Client {
	return client
}

func validNodeRoleSelector() labels.Selector {
	req, err := labels.NewRequirement(constants.NodeRoleLabelKey, selection.In,
		constants.GetValidNodeRoleValuesWithGPU())
	if err != nil {
		log.GetLogger().Errorf("failed to requirement, err: %s", err.Error())
		return labels.Nothing()
	}
	s := labels.NewSelector()
	// NOTE: selector.Add() is similar to append(), we should use the return value
	return s.Add(*req)
}

// StartAddPreloadToAllNodes -
func (c *Client) StartAddPreloadToAllNodes(tenantID, typeStr string, preloadInfo config.PreloadInfo) {
	preloadTaskKey := getPreloadTaskKey(tenantID, typeStr, preloadInfo.VersionUrn)
	v, exists := c.addToAllNodeTaskMap.Load(preloadTaskKey)
	if exists {
		previousTask, ok := v.(*AddToAllNodesTask)
		if ok {
			log.GetLogger().Warnf("cancel the previous task %s of adding preload %s to all nodes",
				previousTask.id, preloadTaskKey)
			previousTask.cancelFunc()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), preloadToAllNodesTaskTimeout)
	addTask := &AddToAllNodesTask{
		id:         uuid.New().String(),
		ctx:        ctx,
		cancelFunc: cancel,
		client:     c,
	}
	c.addToAllNodeTaskMap.Store(preloadTaskKey, addTask)
	go func() {
		log.GetLogger().Infof("start to run the task %s of adding preload %s to all nodes", addTask.id, preloadTaskKey)
		if err := addTask.Run(tenantID, typeStr, preloadInfo); err != nil {
			log.GetLogger().Warnf("failed to run the task %s of adding preload %s to all nodes, err:%s", addTask.id,
				preloadTaskKey, err.Error())
		}
		log.GetLogger().Infof("finish to run the task %s of adding preload %s to all nodes", addTask.id, preloadTaskKey)
	}()
}

func getPreloadTaskKey(tenantID, typeStr, versionName string) string {
	return tenantID + "-" + typeStr + "-" + versionName
}

// Run -
func (a *AddToAllNodesTask) Run(tenantID, typeStr string, preloadInfo config.PreloadInfo) error {
	addPreloadTaskKey := getPreloadTaskKey(tenantID, typeStr, preloadInfo.VersionUrn)
	defer func() {
		a.cancelFunc()
		a.client.addToAllNodeTaskMap.Delete(addPreloadTaskKey)
	}()
	nodesList, err := utils.GetNodeInformer().Lister().List(validNodeRoleSelector())
	if err != nil {
		log.GetLogger().Errorf("failed to get k8s node, err %s", err.Error())
		return err
	}
	payload, err := json.Marshal([]config.PreloadInfo{preloadInfo})
	if err != nil {
		log.GetLogger().Errorf("failed to marshal preloadInfo, err %s", err.Error())
		return err
	}
	addID := a.id
	versionName := preloadInfo.VersionUrn
	apiPath := fmt.Sprintf(preloadPOSTAPIFormat, tenantID, typeStr)
	for _, node := range nodesList {
		select {
		case <-a.ctx.Done():
			log.GetLogger().Warnf("the task %s of add preload %s to all nodes canceled by %s", addID,
				versionName, a.ctx.Err().Error())
			return nil
		default:
			nodeIP := node.Status.Addresses[0].Address
			task := func() {
				if err := checkAddPreloadIsValid(tenantID, typeStr, versionName, preloadInfo); err != nil {
					log.GetLogger().Warnf("%s | add preload %s to node %s is invalid, err %s",
						addID, versionName, nodeIP, err.Error())
					return
				}
				if err := a.client.addPreloadRequest(nodeIP, apiPath, payload, tenantID); err != nil {
					log.GetLogger().Errorf("%s | failed to add preload %s to node %s, err %s",
						addID, versionName, nodeIP, err.Error())
					return
				}
				log.GetLogger().Infof("%s | succeed add preload %s to node %s", addID, versionName, nodeIP)
			}
			if err := a.client.addTaskPool.Submit(task); err != nil {
				log.GetLogger().Warnf("%s | failed to submit add preload %s to node %s task, err %s",
					addID, versionName, nodeIP, err.Error())
				continue
			}
		}
	}
	return nil
}

func checkAddPreloadIsValid(tenantID, typeStr, versionName string, preloadInfo config.PreloadInfo) error {
	currentPreloadInfo := cache.GetPreloadCache().GelPreloadInfo(tenantID, typeStr, versionName)
	if currentPreloadInfo == nil {
		return errors.New("not exists in preload cache")
	}
	if typeStr == constants.PreloadFunctionType && currentPreloadInfo.Sha512 != preloadInfo.Sha512 {
		return errors.New("sha512 is not equal with current preload")
	}
	return nil
}

// StartDelPreloadOfALLNodes -
func (c *Client) StartDelPreloadOfALLNodes(tenantID, typeStr, versionName string) {
	preloadTaskKey := getPreloadTaskKey(tenantID, typeStr, versionName)
	v, exists := c.delOfAllNodeTaskMap.Load(preloadTaskKey)
	if exists {
		previousTask, ok := v.(*DelOfAllNodesTask)
		if ok {
			log.GetLogger().Warnf("cancel the previous task %s of del preload %s to all nodes",
				previousTask.id, preloadTaskKey)
			previousTask.cancelFunc()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), preloadToAllNodesTaskTimeout)
	delTask := &DelOfAllNodesTask{
		id:         uuid.New().String(),
		ctx:        ctx,
		cancelFunc: cancel,
		client:     c,
	}
	c.delOfAllNodeTaskMap.Store(preloadTaskKey, delTask)
	go func() {
		log.GetLogger().Infof("start to run the task %s of del preload %s of all nodes", delTask.id, preloadTaskKey)
		if err := delTask.Run(tenantID, typeStr, versionName); err != nil {
			log.GetLogger().Warnf("failed to run the task %s of del preload %s of all nodes, err:%s",
				delTask.id, preloadTaskKey, err.Error())
		}
		log.GetLogger().Infof("finish to run the task %s of del preload %s of all nodes", delTask.id, preloadTaskKey)
	}()
}

// Run -
func (t *DelOfAllNodesTask) Run(tenantID, typeStr, versionName string) error {
	defer func() {
		preloadTaskKey := getPreloadTaskKey(tenantID, typeStr, versionName)
		t.cancelFunc()
		t.client.delOfAllNodeTaskMap.Delete(preloadTaskKey)
	}()
	nodesList, err := utils.GetNodeInformer().Lister().List(validNodeRoleSelector())
	if err != nil {
		log.GetLogger().Errorf("failed to get k8s node, err %s", err.Error())
		return err
	}
	delID := t.id
	apiPath := fmt.Sprintf(preloadDELETEAPIFormat, tenantID, typeStr, versionName)
	for _, node := range nodesList {
		select {
		case <-t.ctx.Done():
			log.GetLogger().Warnf("the task %s of del preload %s of all nodes canceled by %s", delID,
				versionName, t.ctx.Err().Error())
			return nil
		default:
			nodeIP := node.Status.Addresses[0].Address
			task := func() {
				currentPreloadInfo := cache.GetPreloadCache().GelPreloadInfo(tenantID, typeStr, versionName)
				if currentPreloadInfo != nil {
					log.GetLogger().Warnf("%s | no need to del preload %s of node %s "+
						"due to the preload cache existed", delID, versionName, nodeIP)
					return
				}
				address := nodeIP + ":" + cffAgentPost
				resp, err := t.client.cffClient.DELETE(context.Background(), address, apiPath)
				if err != nil {
					log.GetLogger().Errorf("%s | failed to request del preload %s of node %s, err %s",
						delID, versionName, nodeIP, err.Error())
					return
				}
				closeResponse(resp)
				if resp.StatusCode != http.StatusNoContent {
					log.GetLogger().Errorf("%s | del preload %s of node %s resp not ok, statusCode: %d",
						delID, versionName, nodeIP, resp.StatusCode)
					return
				}
				log.GetLogger().Infof("%s | succeed del preload %s of node %s", delID, versionName, nodeIP)
			}
			if err := t.client.deleteTaskPool.Submit(task); err != nil {
				log.GetLogger().Warnf("%s | failed to submit del preload %s of node %s task, err %s",
					delID, versionName, nodeIP, err.Error())
				continue
			}
		}
	}
	return nil
}

// AddAllPreloadToOneNode -
func (c *Client) AddAllPreloadToOneNode(node *v1.Node) error {
	if node == nil {
		log.GetLogger().Warnf("node is nil")
		return errors.New("node is nil")
	}
	if len(node.Status.Addresses) == 0 {
		log.GetLogger().Warnf("node %s has no address", node.Name)
		return fmt.Errorf("node %s has no address", node.Name)
	}
	if !checkCffAgentIsReady(node.Name) {
		log.GetLogger().Warnf("cff-agent of node %s is not ready", node.Name)
		return fmt.Errorf("cff-agent of node %s is not ready", node.Name)
	}
	nodeIP := node.Status.Addresses[0].Address
	functionPreloadMap, layerPreloadMap := cache.GetPreloadCache().GelALLPreloadInfo()
	for tenantID, preloads := range functionPreloadMap {
		apiPath := fmt.Sprintf(preloadPOSTAPIFormat, tenantID, "function")
		c.batchPostPreload(nodeIP, apiPath, preloads, tenantID)
	}
	for tenantID, preloads := range layerPreloadMap {
		apiPath := fmt.Sprintf(preloadPOSTAPIFormat, tenantID, "layer")
		c.batchPostPreload(nodeIP, apiPath, preloads, tenantID)
	}
	return nil
}

func checkCffAgentIsReady(nodeName string) bool {
	podName := ""
	for i := 0; i < checkCffAgentReadyCountLimit; i++ {
		pod, err := getCffAgentPod(nodeName, podName)
		if err != nil {
			log.GetLogger().Warnf("failed to get cff agent pod of node %s, err %s", nodeName, err.Error())
			time.Sleep(checkCffAgentReadySleepTime)
			continue
		}
		if pod.Status.Phase != "Running" {
			podName = pod.Name
			log.GetLogger().Warnf("cff-agent of node %s is not ready, retry count %d", nodeName, i)
			time.Sleep(checkCffAgentReadySleepTime)
			continue
		}
		return true
	}
	return false
}

func cffAgentSelector() labels.Selector {
	req, err := labels.NewRequirement("app", selection.Equals, []string{"cffagent"})
	if err != nil {
		log.GetLogger().Errorf("failed to requirement, err: %s", err.Error())
		return labels.Nothing()
	}
	s := labels.NewSelector()
	// NOTE: selector.Add() is similar to append(), we should use the return value
	return s.Add(*req)
}

func getCffAgentPod(nodeName, podName string) (*v1.Pod, error) {
	if podName != "" {
		pod, err := utils.GetPodInformer().Lister().Pods(utils.FunctionNameSpace()).Get(podName)
		if err != nil {
			log.GetLogger().Errorf("failed to get cff agent pod, err %s", err.Error())
			return nil, err
		}
		return pod, nil
	}
	pods, err := utils.GetPodInformer().Lister().Pods(utils.FunctionNameSpace()).List(cffAgentSelector())
	if err != nil {
		log.GetLogger().Errorf("failed to get cff agent pod, err %s", err.Error())
		return nil, err
	}
	for _, pod := range pods {
		if pod.Spec.NodeName == nodeName {
			return pod, nil
		}
	}
	return nil, fmt.Errorf("cffagent of node %s not exists", nodeName)
}

// ReAddPreloadToOneNode -
func (c *Client) ReAddPreloadToOneNode(tenantID, versionName, nodeIP, typeStr string) error {
	apiPath := fmt.Sprintf(preloadPOSTAPIFormat, tenantID, typeStr)
	preloadInfo := cache.GetPreloadCache().GelPreloadInfo(tenantID, typeStr, versionName)
	if preloadInfo == nil {
		log.GetLogger().Errorf("reAdd preload %s not exists", versionName)
		return errors.New("preload info not exists")
	}
	reAddRepeatedCheckKey := strings.Join([]string{tenantID, typeStr, versionName, nodeIP}, "-")
	_, exists := c.reAddRepeatedCheckMap.LoadOrStore(reAddRepeatedCheckKey, "")
	if exists {
		// check is same preload reAdd on same node
		log.GetLogger().Warnf("no need to repeat reAdd preload %s to node %s", versionName, nodeIP)
		return errors.New("repeat add preload on same node")
	}
	defer c.reAddRepeatedCheckMap.Delete(reAddRepeatedCheckKey)
	payload, err := json.Marshal([]config.PreloadInfo{*preloadInfo})
	if err != nil {
		log.GetLogger().Warnf("failed to marshal preload post body, err %s", err.Error())
		return err
	}
	if err := c.addPreloadRequest(nodeIP, apiPath, payload, tenantID); err != nil {
		log.GetLogger().Errorf("failed to reAdd preload %s to node %s, err %s", versionName, nodeIP, err.Error())
		return err
	}
	log.GetLogger().Infof("succeed reAdd preload %s to node %s", versionName, nodeIP)
	return nil
}

func (c *Client) batchPostPreload(nodeIP, apiPath string, preloads []config.PreloadInfo, tenantID string) {
	preloadNum := len(preloads)
	numBatches := (preloadNum + batchSize - 1) / batchSize
	log.GetLogger().Infof("start to batch add preload to node %s batch num %s", nodeIP, numBatches)
	for i := 0; i < numBatches; i++ {
		start := i * batchSize
		end := start + batchSize
		if end > preloadNum {
			end = preloadNum
		}
		batch := preloads[start:end]
		payload, err := json.Marshal(batch)
		if err != nil {
			log.GetLogger().Warnf("failed to marshal preload post body, err %s", err.Error())
			continue
		}
		if err := c.addPreloadRequest(nodeIP, apiPath, payload, tenantID); err != nil {
			log.GetLogger().Errorf("failed to add batch %d preload request to node %s, err %s", i, nodeIP, err.Error())
			continue
		}
		log.GetLogger().Infof("succeed add batch %d preload to node %s", i, nodeIP)
	}
}

func (c *Client) addPreloadRequest(nodeIP, apiPath string, payload []byte, tenantID string) error {
	address := nodeIP + ":" + cffAgentPost
	resp, err := c.cffClient.POST(context.Background(), address, apiPath, payload)
	if err != nil {
		log.GetLogger().Errorf("failed to add preload request to node %s, err %s", nodeIP, err.Error())
		recordOtherProblem(tenantID, nodeIP)
		addRequestFailedNode(nodeIP)
		return err
	}
	defer closeResponse(resp)
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("the response of add preload request is not 200, node: %s statusCode: %d",
			nodeIP, resp.StatusCode)
		errMsg := processAddPreloadErrorResponse(resp, nodeIP, tenantID)
		addRequestFailedNode(nodeIP)
		return errors.New(errMsg)
	}
	removeRequestFailedNodeWhenOK(nodeIP, tenantID)
	return nil
}

func processAddPreloadErrorResponse(resp *http.Response, nodeIP string, tenantID string) string {
	errMsg := fmt.Sprintf("response statusCode %d", resp.StatusCode)
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		recordOtherProblem(tenantID, nodeIP)
		log.GetLogger().Warnf("failed to read preload resp body, err %s", err.Error())
		return errMsg
	}
	preloadErr := &AddPreloadError{}
	if err := json.Unmarshal(respBody, preloadErr); err != nil {
		recordOtherProblem(tenantID, nodeIP)
		log.GetLogger().Warnf("failed to unmarshal preload resp body, err %s", err.Error())
		return errMsg
	}
	switch preloadErr.ErrCode {
	case reachPreloadDiskLimitCode:
		recordExceedTenantPreloadDiskLimit(exceedPreloadDiskLimitTenant, nodeIP)
		errMsg = fmt.Sprintf("disk usage of preload on node %s exceeds the limit", nodeIP)
	case reachTenantPreloadDiskLimitCode:
		recordExceedTenantPreloadDiskLimit(tenantID, nodeIP)
		errMsg = fmt.Sprintf("disk usage of the tenant %s preload on node %s exceeds the limit", tenantID, nodeIP)
	default:
		recordOtherProblem(tenantID, nodeIP)
		errMsg = preloadErr.ErrMessage
	}
	return errMsg
}

func closeResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil {
		log.GetLogger().Errorf("failed to close response body, err: %s", err.Error())
	}
}

// CheckAndReportWarn -
func CheckAndReportWarn(pod *v1.Pod) error {
	if preloadEnable != "true" {
		return nil
	}
	podLabels := pod.GetLabels()
	isPreload := podLabels[utils.LabelIsPreload]
	if isPreload == "true" {
		alarm.GetClient().ReportAlarm(alarm.PreloadWorkerPanic,
			fmt.Sprintf("a panic occurs on the worker of the prelad pod %s uid %s of node %s. "+
				"check and clear the preload mount bind.", pod.Name, pod.UID, pod.Spec.NodeName))
	}
	return nil
}
