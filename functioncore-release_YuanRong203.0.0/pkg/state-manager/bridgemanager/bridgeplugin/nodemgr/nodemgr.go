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

// Package nodemgr is manager of worker-manager cluster info
package nodemgr

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/common/workermanagerutils"
	bridgeConfig "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/config"
	"functioncore/pkg/state-manager/config"
)

const (
	defaultScaleTimeout = 15 * time.Minute
	scaleDelay          = 46

	workerManagerDeployURL = "/worker-manager/v1/functions/sn:cn:yrk:%s:function:%s:%s/worker"
	workerManagerDeleteURL = "/worker-manager/v1/functions/worker/delete"
	defaultBusinessID      = "yrk"
	workerIDPrefix         = "defaultaz-#-"

	scaleUpTraceIDPrefix   = "bridgeScaleUp"
	scaleDownTraceIDPrefix = "bridgeScaleDown"
)

const (
	// URNIndexZero URN index 0
	URNIndexZero = iota
	// URNIndexOne URN index 1
	URNIndexOne
	// URNIndexTwo URN index 2
	URNIndexTwo
	// URNIndexThree URN index 3
	URNIndexThree
	// URNIndexFour URN index 4
	URNIndexFour
	// URNIndexFive URN index 5
	URNIndexFive
	// URNIndexSix URN index 6
	URNIndexSix
	// URNIndexSeven URN index 7
	URNIndexSeven
)

var (
	mgrInstance *Manager
	once        = sync.Once{}
	selfNodeIP  = os.Getenv("NODE_IP")
)

// Manager define struct of node manager
type Manager struct {
	lock   sync.RWMutex
	nodeIP string
}

// Instance get node manager singleton instance
func Instance() *Manager {
	once.Do(func() {
		mgrInstance = &Manager{
			lock: sync.RWMutex{},
		}
	})
	return mgrInstance
}

// RemoveNode remove node
func (c *Manager) RemoveNode() {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.nodeIP = ""
}

// AddNode add node
func (c *Manager) AddNode(nodeIP string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.nodeIP = nodeIP
}

// GetNode get node
func (c *Manager) GetNode() string {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.nodeIP
}

// GetFuncInstances -
func (c *Manager) GetFuncInstances(funcSvc string) (*types.QueryInstance, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	nodeIP := c.GetNode()
	queryInsResp, err := workermanagerutils.QueryInstancesFromWmByIP(funcSvc, nodeIP, cfg.HTTPSConfig.HTTPSEnable,
		&cfg.LocalAuth)
	if err != nil {
		if strings.Contains(err.Error(), "failed to get function cache") {
			log.GetLogger().Warnf("no instance of function from worker-manager, node: %s funcSvc: %s", nodeIP, funcSvc)
			return &types.QueryInstance{}, nil
		}
		log.GetLogger().Errorf("failed to query function instances from worker-manager, node: %s, funcSvcID: %s, err: %s",
			nodeIP, funcSvc, err.Error())
		return nil, err
	}
	if queryInsResp.Code != http.StatusOK {
		log.GetLogger().Warnf("code of queryInsResp is not OK, node: %s, funcSvcID: %s, code: %d, message: %s",
			nodeIP, funcSvc, queryInsResp.Code, queryInsResp.Message)
		return &types.QueryInstance{}, nil
	}
	return &queryInsResp.Instances, nil
}

// ScaleUpInstance -
func (c *Manager) ScaleUpInstance(functionSvcID string, functionMetaInfo *bridgeConfig.FunctionMetaInfo) (
	*types.Instance, error) {

	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}

	traceID := scaleUpTraceIDPrefix + uuid.New().String()

	ctx, cancel := context.WithTimeout(context.Background(), getScaleTimeOut(functionMetaInfo))
	defer cancel()
	request, err := c.makeScaleUpReq(ctx, functionMetaInfo, traceID, cfg)
	if err != nil {
		log.GetLogger().Errorf("failed to makeScaleUpReq by functionSvcID: %s, err: %s", functionSvcID, err.Error())
		return nil, err
	}

	log.GetLogger().Debugf("sign the authorization of functionSvcID: %s, traceID: %s", functionSvcID, traceID)
	resp, err := workermanagerutils.GetHTTPClient(cfg.HTTPSConfig.HTTPSEnable).Do(request)
	if err != nil {
		log.GetLogger().Errorf("send request to worker manager error: %s, functionSvcID: %s, traceID: %s",
			err.Error(), functionSvcID, traceID)
		return nil, err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("failed to get response body: %s, functionSvcID: %s, traceID: %s", err.Error(),
			functionSvcID, traceID)
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Errorf("failed to unmarshal scaleUp error response: %s, functionSvcID: %s, traceID: %s",
				err, functionSvcID, traceID)
			return nil, err
		}
		return nil, fmt.Errorf(snErr.Message)
	}

	scaleUpResp, instance, err := workermanagerutils.HandleResp(functionSvcID, respBody, traceID)
	if err != nil {
		return instance, err
	}

	log.GetLogger().Infof("deploy functionSvcID %s instance: %v traceID: %s successfully",
		functionSvcID, scaleUpResp.Instance, traceID)
	return scaleUpResp.Instance, nil
}

func (c *Manager) makeScaleUpReq(ctx context.Context, functionMetaInfo *bridgeConfig.FunctionMetaInfo,
	traceID string, cfg *config.FConfig) (*http.Request, error) {

	scaleUpParam := workermanagerutils.DeployParam{
		Applier:    constants.BridgeManagerApplier,
		DeployNode: selfNodeIP,
		Business:   defaultBusinessID,
		TraceID:    traceID,
	}

	data, err := json.Marshal(scaleUpParam)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal scaleUp request: %s", err.Error())
		return nil, err
	}
	functionInfo := GetFunctionInfoByURN(functionMetaInfo.FuncMetaData.FunctionVersionURN)

	u, err := url.Parse(fmt.Sprintf("%s%s",
		workermanagerutils.GetWorkerManagerBaseURLByIP(cfg.HTTPSConfig.HTTPSEnable, c.GetNode()),
		fmt.Sprintf(workerManagerDeployURL, functionInfo.Tenant, functionInfo.FuncName, functionInfo.Version)))

	if err != nil {
		return nil, fmt.Errorf("failed to parse url, %s", err.Error())
	}

	request, err := http.NewRequestWithContext(ctx, "POST", u.String(), bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("action failed when make scaleUp request, traceID: %s, err %s",
			scaleUpParam.TraceID, err.Error())
		return nil, err
	}

	workermanagerutils.FillInWorkerManagerRequestHeaders(request, &cfg.LocalAuth)
	request.Header.Set(constants.HeaderTraceID, scaleUpParam.TraceID)
	request.Header.Set(constants.HeaderForceDeploy, "false")
	return request, nil
}

// ScaleDownInstance -
func (c *Manager) ScaleDownInstance(functionSvcID, workerID string, functionMetaInfo *bridgeConfig.
	FunctionMetaInfo) error {
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get config when ScaleDownInstance")
		return err
	}

	traceID := scaleDownTraceIDPrefix + uuid.New().String()
	log.GetLogger().Infof("start to scale down by functionSvcID: %s, traceID: %s", functionSvcID, traceID)
	ctx, cancel := context.WithTimeout(context.Background(), workermanagerutils.ScaleDownTimeout)
	defer cancel()

	request, err := c.makeScaleDownReq(ctx, cfg, workerID, functionMetaInfo)
	if err != nil {
		log.GetLogger().Errorf("failed to make scaleDown request to worker manager error: %s, functionSvcID: %s",
			err.Error(), functionSvcID)
		return err
	}
	workermanagerutils.FillInWorkerManagerRequestHeaders(request, &cfg.LocalAuth)
	resp, err := workermanagerutils.GetHTTPClient(cfg.HTTPSConfig.HTTPSEnable).Do(request)
	if err != nil {
		log.GetLogger().Errorf("failed to send scaleDown request error: %s, functionSvcID: %s", err.Error(), functionSvcID)
		return err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snErr := &snerror.BadResponse{}
		err = json.Unmarshal(respBody, snErr)
		if err != nil {
			log.GetLogger().Warnf("failed to unmarshal scaleDownResponse: %s, functionSvcID: %s", err, functionSvcID)
			return err
		}
		return fmt.Errorf(snErr.Message)
	}

	scaleDownResp := workermanagerutils.DeleteWorkerResponse{}
	if err = json.Unmarshal(respBody, &scaleDownResp); err != nil {
		log.GetLogger().Errorf("failed to unmarshal scaleDownResponse: %s, functionSvcID: %s", err, functionSvcID)
		return err
	}

	if scaleDownResp.Code != http.StatusOK {
		log.GetLogger().Warnf("scale down functionSvcID %s response code %d, response message %s",
			functionSvcID, scaleDownResp.Code, scaleDownResp.Message)
		if scaleDownResp.Code == 0 {
			return nil
		}
		return fmt.Errorf(scaleDownResp.Message)
	}
	log.GetLogger().Infof("scale down %s and reservedResult is %t", functionSvcID, scaleDownResp.Reserved)
	return nil
}

func (c *Manager) makeScaleDownReq(ctx context.Context, cfg *config.FConfig, workerID string,
	functionMetaInfo *bridgeConfig.FunctionMetaInfo) (*http.Request, error) {
	u, err := url.Parse(fmt.Sprintf("%s%s",
		workermanagerutils.GetWorkerManagerBaseURLByIP(cfg.HTTPSConfig.HTTPSEnable, c.GetNode()), workerManagerDeleteURL))

	if err != nil {
		return nil, fmt.Errorf("failed to parse url, %s", err.Error())
	}

	functionInfo := GetFunctionInfoByURN(functionMetaInfo.FuncMetaData.FunctionVersionURN)

	param := workermanagerutils.DeleteParam{
		InstanceID:  workerIDPrefix + workerID,
		FuncName:    functionInfo.FuncName,
		FuncVersion: functionInfo.Version,
		BusinessID:  defaultBusinessID,
		TenantID:    functionInfo.Tenant,
		Applier:     constants.BridgeManagerApplier,
	}

	data, err := json.Marshal(param)
	if err != nil {
		log.GetLogger().Errorf("marshal error when make scaleUp request")
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("error when make scaleDown request: %s", err.Error())
		return nil, err
	}
	return request, nil
}

// GetFunctionInfoByURN sn:cn:yrk:xxxxxxx:function:weatherforecast:1
func GetFunctionInfoByURN(urn string) FunctionInfo {
	var functionInfo FunctionInfo
	urnFields := strings.Split(urn, ":")

	switch len(urnFields) {
	case URNIndexSeven:
		functionInfo.Version = urnFields[URNIndexSix]
		fallthrough
	case URNIndexSix:
		functionInfo.FuncName = urnFields[URNIndexFive]
		functionInfo.Tenant = urnFields[URNIndexThree]
		functionInfo.Business = urnFields[URNIndexTwo]
		return functionInfo
	default:
		return functionInfo
	}
}

// FunctionInfo defines Function Info
type FunctionInfo struct {
	Business string
	Tenant   string
	FuncName string
	Version  string
}

func getScaleTimeOut(functionEtcdInfo *bridgeConfig.FunctionMetaInfo) time.Duration {
	var timeOut = defaultScaleTimeout
	if functionEtcdInfo.FuncMetaData.Timeout == 0 {
		timeOut = defaultScaleTimeout
	} else {
		timeOut = time.Duration(scaleDelay+functionEtcdInfo.FuncMetaData.Timeout+
			functionEtcdInfo.ExtendedMetaData.Initializer.Timeout) * time.Second
	}
	return timeOut
}
