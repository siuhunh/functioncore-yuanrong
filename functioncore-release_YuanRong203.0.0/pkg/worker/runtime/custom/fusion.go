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

package custom

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker"
	"functioncore/pkg/worker/config"
	"functioncore/pkg/worker/runtime"
)

var (
	fusionCustomRuntime *fusionCustom

	clientReadyOnce sync.Once
	clientReadyChan = make(chan struct{})
)

type fusionCustom struct {
	*Custom
	hotLoadOnce sync.Once
	hotLoadErr  error
	handlers    runtime.Handlers
}

// ClientReady -
func ClientReady() {
	clientReadyOnce.Do(func() {
		close(clientReadyChan)
	})
}

// HotLoad implements runtime.Runtime
func (f *fusionCustom) HotLoad(ctx context.Context, conf *config.Configuration) error {
	f.hotLoadOnce.Do(func() {
		if f.handlers.CheckRuntimeStatusHandler != nil && f.handlers.GetExceptionHandler != nil {
			go f.WatchRuntimeProcessStatus(ctx, f.handlers)
		}
		f.hotLoadErr = f.Custom.HotLoad(ctx, conf)
	})
	return f.hotLoadErr
}

// CreateFusionCustomRuntime -
func CreateFusionCustomRuntime(conf *config.Configuration, handlers runtime.Handlers) (runtime.Runtime, error) {
	newAbstractRuntime, err := runtime.NewAbstractRuntime(conf)
	if err != nil {
		return nil, fmt.Errorf("new abstract runtime error: %s", err.Error())
	}
	rt := &fusionCustom{
		Custom: &Custom{
			AbstractRuntime: *newAbstractRuntime,
			InvokeTimeout:   time.Duration(conf.Timeout) * time.Second,
			handlers:        handlers,
			invokeChan:      make(chan *recvMsgCall, invokeReqNotifyChNum),
		},
	}
	address := getServerAddress(conf.RMConfig.RuntimeConfigs[worker.CustomRuntimeLanguage].Port)
	rt.server, err = getServer(address)
	if err != nil {
		log.GetLogger().Errorf("new custom runtime server with error %s", err.Error())
		return nil, err
	}
	if handlers.CheckRuntimeStatusHandler != nil && handlers.GetExceptionHandler != nil {
		rt.handlers = handlers
	}
	fusionCustomRuntime = rt

	return fusionCustomRuntime, nil
}

// StartFusionCustomHTTPServer -
func StartFusionCustomHTTPServer(conf *config.RuntimeConfig) error {
	log.GetLogger().Infof("preStarting the server of custom runtime")
	port := conf.RMConfig.RuntimeConfigs[worker.CustomRuntimeLanguage].Port
	server, err := getServer(getServerAddress(port))
	if err != nil {
		log.GetLogger().Errorf("preStarted the server error: %s", err.Error())
		return err
	}
	go server.Start()
	log.GetLogger().Infof("start custom runtime http server with port: %d", port)
	return nil
}

// StopFusionCustomHTTPServer -
func StopFusionCustomHTTPServer() error {
	if fusionCustomRuntime == nil || fusionCustomRuntime.server == nil ||
		fusionCustomRuntime.server.httpServer == nil {
		return nil
	}
	customServer := fusionCustomRuntime.server
	customServer.closeServerFlag = true
	if err := customServer.httpServer.Shutdown(context.TODO()); err != nil {
		return err
	}
	log.GetLogger().Infof("finished to stop custom runtime http server")
	return nil
}

// RunProcess run user custom runtime process
func RunProcess(uid, gid int, conf *config.Configuration, logReporter func([]byte)) (*os.Process, error) {
	newAbstractRuntime, err := runtime.NewAbstractRuntime(conf)
	if err != nil {
		log.GetLogger().Errorf("failed to new abstract runtime, %s", err.Error())
		return nil, err
	}
	customRuntime := &Custom{
		AbstractRuntime: *newAbstractRuntime,
	}
	// enable std log collection before custom runtime started
	customRuntime.EnableStdLogCollection(logReporter)

	address := getServerAddress(conf.RMConfig.RuntimeConfigs[worker.CustomRuntimeLanguage].Port)
	customRuntime.server, err = getServer(address)
	if err != nil {
		log.GetLogger().Errorf("failed to get custom server, %s", err.Error())
		return nil, err
	}
	// Custom runtime always new a process when load actually
	if customRuntime.Process, err = customRuntime.RunWrapperByID(uid, gid); err != nil {
		log.GetLogger().Errorf("failed to run custom runtime process, %s", err.Error())
		return nil, err
	}

	return customRuntime.Process, nil
}
