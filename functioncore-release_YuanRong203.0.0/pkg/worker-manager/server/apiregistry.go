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

// Package server New Api Registry
package server

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/worker-manager/api"
)

// HandleFunc define the handle function
type HandleFunc func(w *http.ResponseWriter, r *http.Request)

// APIRegistry define api register
type APIRegistry struct {
	Router *gin.Engine
}

// newAPIRegistry New Api Registry
func newAPIRegistry() *APIRegistry {
	gin.SetMode(gin.ReleaseMode)
	registry := APIRegistry{
		Router: gin.New(),
	}

	return &registry
}

// registerAPI Register Api
func (r *APIRegistry) registerAPI(apiVersion api.Version) error {
	return apiVersion.RegisterAPI(r.Router)
}
