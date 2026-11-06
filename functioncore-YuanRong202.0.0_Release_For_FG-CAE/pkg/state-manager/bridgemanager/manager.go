/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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

// Package bridgemanager is bridge manager module
package bridgemanager

import (
	"fmt"
	"plugin"
	"strings"
	"sync"

	"github.com/pkg/errors"

	"functioncore/pkg/common/logger/log"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
)

// BridgeHandler define struct of bridge manager
type BridgeHandler struct {
	PluginBridge  *plugin.Plugin
	BridgeChannel chan string
	BgMgrEnable   bool
}

const (
	// PluginFile is the path of bridgeplugin.so
	PluginFile      = "/home/sn/bridgeplugin.so"
	bridgeInitError = "realpath failed"
)

var mgrInstance *BridgeHandler

var once = sync.Once{}

// Instance get BridgeHandler singleton instance
func Instance() (*BridgeHandler, error) {
	var err error
	once.Do(func() {
		mgrInstance, err = initBridgeManager()
	})
	return mgrInstance, err
}

func initBridgeManager() (*BridgeHandler, error) {
	log.GetLogger().Infof("call initBridgeManager.")
	bgMgrEnable := true
	handler := &BridgeHandler{}
	pluginFile, err := plugin.Open(PluginFile)
	if err != nil {
		if strings.Contains(err.Error(), bridgeInitError) {
			bgMgrEnable = false
		} else {
			fmt.Printf("failed to open bridge plugin file: %s\n", err.Error())
			return nil, err
		}
	}
	handler.PluginBridge = pluginFile
	handler.BgMgrEnable = bgMgrEnable
	if bgMgrEnable {
		err = initPlugin(pluginFile)
		if err != nil {
			return nil, err
		}
	}
	return handler, nil
}

func initPlugin(pluginFile *plugin.Plugin) error {
	targetFunc, err := pluginFile.Lookup("InitBridgeManager")
	if err != nil {
		return err
	}
	if initBridgeManagerPlugin, ok := targetFunc.(func() error); ok {
		err = initBridgeManagerPlugin()
		if err != nil {
			return err
		}
	}
	return nil
}

// ApplyStateKey method called by functiontask to get stateKey
func (b *BridgeHandler) ApplyStateKey(clientInstance *bgtypes.ClientInstance, traceID string) (string, bool, error) {
	if clientInstance == nil {
		log.GetLogger().Errorf("ApplyStateKey input is nil clientInstance")
		return "", false, errors.New("clientInstance is nil when apply state key")
	}
	log.GetLogger().Infof("call ApplyStateKey.")
	var stateKey string
	isNewStateKeyScenario := false
	targetFunc, err := b.PluginBridge.Lookup("ApplyStateKey")
	if err != nil {
		return "", false, err
	}
	if applyStateKeyInPlugin, ok :=
		targetFunc.(func(clientInstance *bgtypes.ClientInstance, traceID string) (string, bool, error)); ok {
		stateKey, isNewStateKeyScenario, err = applyStateKeyInPlugin(clientInstance, traceID)
		if err != nil {
			return "", false, err
		}
	}
	return stateKey, isNewStateKeyScenario, nil
}

// ReleaseBridge method called by functiontask to release bridge instance
func (b *BridgeHandler) ReleaseBridge(clientInstance *bgtypes.ClientInstance, traceID string) (bool, error) {
	if clientInstance == nil {
		log.GetLogger().Errorf("ReleaseBridge input is nil clientInstance")
		return false, errors.New("clientInstance is nil when release bridge")
	}
	log.GetLogger().Infof("call ReleaseBridge for stateID: %s", clientInstance.AssignStateID)
	var isNeedRelease bool
	targetFunc, err := b.PluginBridge.Lookup("ReleaseBridge")
	if err != nil {
		return false, err
	}
	if releaseBridgeInPlugin, ok :=
		targetFunc.(func(clientInstance *bgtypes.ClientInstance, traceID string) (bool, error)); ok {
		isNeedRelease, err = releaseBridgeInPlugin(clientInstance, traceID)
		if err != nil {
			return false, err
		}
	}
	return isNeedRelease, nil
}
