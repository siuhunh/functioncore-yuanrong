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

// Package utils get function info
package utils

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/worker-manager/storage/meta"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
)

const (
	// DefaultNamespace define the default namespace
	DefaultNamespace = "default"
	// HeaderTypeGet define the header type
	HeaderTypeGet = "get"
	// ReqTriggerFlag request is triggered by http trigger
	ReqTriggerFlag = "trigger"
	// RequestCallType request call type is "active"
	RequestCallType = "active"
	// initFilterSize is the init size of filter
	initFilterSize = 4
)

// Tenant2Namespace transform Tenant to Namespace
func Tenant2Namespace(c *gin.Context) string {
	val := c.Param("tenant")

	if val != "" {
		return val
	}

	return DefaultNamespace
}

// GetMetaName Get Meta Name
func GetMetaName(c *gin.Context) (string, error) {
	val := c.Param("name")
	if val != "" {
		return val, nil
	}

	return "", ErrFunctionNameNotFound
}

// GetType Get Type
func GetType(r *http.Request) string {
	activeType := r.Header.Get(constants.HeaderCallType)
	if activeType == "" {
		activeType = HeaderTypeGet
	}
	return activeType
}

// GetTraceID Get Trace ID
func GetTraceID(r *http.Request) string {
	return r.Header.Get(constants.HeaderTraceID)
}

// GetTriggerFlag  When request is created by http trigger, "X-Trigger-Flag"== "trigger"
func GetTriggerFlag(r *http.Request) string {
	return r.Header.Get(constants.HeaderTriggerFlag)
}

// GetForceDeploy Function Bus decides whether force to deploy pod on a node or not
func GetForceDeploy(r *http.Request) string {
	return r.Header.Get(constants.HeaderForceDeploy)
}

// GetNodeLabel specifies which node to be chosen
func GetNodeLabel(r *http.Request) string {
	return r.Header.Get(constants.HeaderNodeLabel)
}

// GetTraceParent get traceParent
func GetTraceParent(r *http.Request) string {
	return r.Header.Get(constants.HeaderTraceParent)
}

// GetOwnerIP get owner ID
func GetOwnerIP(r *http.Request) string {
	return r.URL.Query().Get("ownerIp")
}

// IsAlias check if it is alias
func IsAlias(r *http.Request) bool {
	alias := r.URL.Query().Get("isAlias")
	if alias == "" {
		return false
	}
	return true
}

// GetApplier get applier
func GetApplier(r *http.Request) string {
	return r.URL.Query().Get("applier")
}

// GetDeployedNode get deployed node
func GetDeployedNode(r *http.Request) string {
	return r.URL.Query().Get("deployedNode")
}

// NewFilter new filter
func NewFilter() *meta.StorageFilter {
	filter := meta.StorageFilter{
		Filters: make(map[string]string, initFilterSize),
	}
	filter.Filters[meta.FilterKeyMetaType] = faasfunction.Type()

	return &filter
}
