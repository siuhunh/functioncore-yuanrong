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

// Package healthcheck implement health check
package healthcheck

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/state-manager/server"
)

const (
	// initial map size is 3
	initHealthRouter = 3
)

var (
	checkMap = &healthCheckMap{m: make(map[string]*checkFunc, initHealthRouter)}
)

// healthCheckMap health check map with rw lock
type healthCheckMap struct {
	sync.RWMutex
	m map[string]*checkFunc
}

// checkFunc register health check with CallBack function
type checkFunc struct {
	CallBack func() error
}

// InitHealthCheck init health check
func InitHealthCheck() {
	param := &server.HandleParam{
		Path: "/health",
		HandleFunc: func(c *gin.Context) {
			checkHandler(c.Writer, c.Request)
		},
		Method: http.MethodGet,
	}
	server.RegisterRouter(param, "health_check")
	log.GetLogger().Infof("init health check router successfully")
}

// RegisterParam register health check with checkFunc
func RegisterParam(name string, f func() error) {
	checkMap.Lock()
	checkMap.m[name] = &checkFunc{CallBack: f}
	checkMap.Unlock()
	log.GetLogger().Infof("register health check successfully: %s", name)
}

func checkHandler(w http.ResponseWriter, r *http.Request) {
	checkMap.RLock()
	defer checkMap.RUnlock()

	if len(checkMap.m) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	for k, v := range checkMap.m {
		if err := v.CallBack(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.GetLogger().Errorf("failed to health check, checkFunc name: %s", k)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}
