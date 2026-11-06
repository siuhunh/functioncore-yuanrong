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

// Package v1 New and Register Api V1
package v1

import (
	"github.com/gin-gonic/gin"

	apiServerOptions "functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/api/v1/faasfunction"
)

var versionPath = "/worker-manager/v1"
var tenantPath = "/{tenant}"

// APIV1 define the v1 api
type APIV1 struct {
	FunctionManager *faasfunction.FunctionAPI
	Version         string
}

// GetVersionPath Get Version Path
func GetVersionPath() string {
	return versionPath
}

// NewAPIV1 New Api V1
func NewAPIV1(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}) (*APIV1, error) {
	// default disable tenant, path use '/worker-manager/v1' as root path
	apiVersionPath := versionPath
	if options.EnableTenant {
		// enable tenant, path use '/worker-manager/v1/{tenant}' as root path
		apiVersionPath = versionPath + tenantPath
		log.GetLogger().Infof("enable tenant...")
	}

	functionManager, err := faasfunction.NewFaaSFunctionAPI(apiVersionPath, options, stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to create faasfunction api, error: %s", err.Error())
		return nil, err
	}
	v1 := APIV1{
		Version:         "v1",
		FunctionManager: functionManager,
	}

	return &v1, nil
}

// RegisterAPI Register Api
func (a *APIV1) RegisterAPI(router *gin.Engine) error {
	log.GetLogger().Infof("register faasFunctionAPI")
	a.FunctionManager.RegisterAPI(router)
	return nil
}
