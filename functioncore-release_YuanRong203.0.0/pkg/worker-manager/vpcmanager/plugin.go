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

// Package vpcmanager is vpc manager module
package vpcmanager

import (
	"encoding/json"
	"errors"
	"plugin"
	"strings"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/types"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

// PluginVPC -
type PluginVPC struct {
	*plugin.Plugin
	becomeLeader   plugin.Symbol
	createResource plugin.Symbol
	deleteResource plugin.Symbol
	notifyExit     plugin.Symbol
}

func (p *PluginVPC) initVpcPlugin() error {
	err := p.initController()
	if err != nil {
		log.GetLogger().Errorf("failed to exec initController, error: %s", err.Error())
		return err
	}
	err = p.checkResource(vpcHandler.VpcChannel)
	if err != nil {
		log.GetLogger().Errorf("failed to exec checkResource, error: %s", err.Error())
		return err
	}
	return nil
}

func (p *PluginVPC) initController() error {
	if p.Plugin == nil {
		log.GetLogger().Error("pluginVpc is nil")
		return errors.New("pluginVpc is nil")
	}
	targetFunc, err := p.Plugin.Lookup("InitController")
	if err != nil {
		log.GetLogger().Errorf("failed to look up InitController, error: %s", err.Error())
		return err
	}
	if initController, ok := targetFunc.(func() error); ok {
		err = initController()
		if err != nil {
			log.GetLogger().Errorf("failed to init Controller, error: %s", err.Error())
			return err
		}
	}
	becomeLeader, err := p.Plugin.Lookup("BecomeLeader")
	if err != nil {
		log.GetLogger().Errorf("failed to lookup function of BecomeLeader: %s", err.Error())
		return err
	}
	p.becomeLeader = becomeLeader
	createResource, err := p.Plugin.Lookup("CreateResource")
	if err != nil {
		log.GetLogger().Errorf("failed to lookup function of CreateResource: %s", err.Error())
		return err
	}
	p.createResource = createResource
	deleteResource, err := p.Plugin.Lookup("DeleteResource")
	if err != nil {
		log.GetLogger().Errorf("failed to lookup function of DeleteResource: %s", err.Error())
		return err
	}
	p.deleteResource = deleteResource
	notifyExit, err := p.Plugin.Lookup("NotifyExit")
	if err != nil {
		log.GetLogger().Errorf("failed to lookup function of NotifyExit: %s", err.Error())
		return err
	}
	p.notifyExit = notifyExit
	return nil
}

func (p *PluginVPC) checkResource(ch chan string) error {
	if p.Plugin == nil {
		log.GetLogger().Error("pluginVpc is nil")
		return errors.New("pluginVpc is nil")
	}
	targetFunc, err := p.Plugin.Lookup("CheckResource")
	if err != nil {
		log.GetLogger().Errorf("failed to look up CheckResource, error: %s", err.Error())
		return err
	}
	if checkResource, ok := targetFunc.(func(ch chan string) error); ok {
		err = checkResource(ch)
		if err != nil {
			log.GetLogger().Errorf("failed to check Resource, error: %s", err.Error())
			return err
		}
	}
	return nil
}

func (p *PluginVPC) becomeVpcLeader() error {
	if p.Plugin == nil {
		log.GetLogger().Error("pluginVpc is nil")
		return errors.New("pluginVpc is nil")
	}
	start := time.Now()
	if becomeLeader, ok := p.becomeLeader.(func() error); ok {
		err := becomeLeader()
		if err != nil {
			log.GetLogger().Errorf("failed to become vpc leader: %s", err.Error())
			return err
		}
	} else {
		log.GetLogger().Error("failed to assert becomeLeader")
		return errors.New("failed to assert becomeLeader")
	}
	log.GetLogger().Infof("succeed to call become leader with cost %s", time.Since(start).String())
	return nil
}

func (p *PluginVPC) createVpcResource(service *faasfunction.FunctionService,
	traceID string, vpcType int, needInit, needNewVersion bool) ([]types.NATConfigure, error) {
	if p.Plugin == nil {
		log.GetLogger().Error("pluginVpc is nil")
		return nil, errors.New("pluginVpc is nil")
	}
	patPod := make([]types.NATConfigure, 1)
	if createResource, ok := p.createResource.(func(request []byte) ([]byte, error)); ok {
		request := describeRequest(traceID, service, vpcType, needInit, needNewVersion)
		reqInfo, err := json.Marshal(request)
		if err != nil {
			log.GetLogger().Errorf("HandleVpcFunctionInfo Marshal error: %s", err.Error())
			return nil, err
		}
		resp, err := createResource(reqInfo)
		if err != nil {
			log.GetLogger().Errorf("HandleVpcFunctionInfo createResource error: %s", err.Error())
			return nil, err
		}
		err = json.Unmarshal(resp, &patPod)
		if err != nil {
			log.GetLogger().Errorf("HandleVpcFunctionInfo Unmarshal error: %s", err.Error())
			return nil, err
		}
	} else {
		log.GetLogger().Error("failed to assert createResource")
		return nil, errors.New("failed to assert createResource")
	}
	return patPod, nil
}

func (p *PluginVPC) deleteVpcResource(patKey string) error {
	if p.Plugin == nil {
		log.GetLogger().Error("pluginVpc is nil")
		return errors.New("pluginVpc is nil")
	}
	splits := strings.Split(patKey, "/")
	if len(splits) != defaultKeyLen {
		log.GetLogger().Errorf("unsupported type of pat service key: %s", patKey)
		return errors.New("unsupported type of pat service key")
	}
	podName := splits[patPodIndex]
	if deleteResource, ok := p.deleteResource.(func(string2 string) error); ok {
		err := deleteResource(podName)
		if err != nil {
			log.GetLogger().Errorf("failed to delete vpc resource: %s", err.Error())
			return err
		}
	} else {
		log.GetLogger().Error("failed to assert deleteResource")
		return errors.New("failed to assert deleteResource")
	}
	return nil
}

func (p *PluginVPC) notifyVpcControllerExit() error {
	if p.Plugin == nil {
		log.GetLogger().Error("pluginVpc is nil")
		return errors.New("pluginVpc is nil")
	}
	if notifyExit, ok := p.notifyExit.(func()); ok && notifyExit != nil {
		notifyExit()
		return nil
	}
	log.GetLogger().Error("failed to assert notifyExit")
	return errors.New("failed to assert notifyExit")
}
