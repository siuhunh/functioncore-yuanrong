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

// Package server New Api Server Handler
package server

import (
	"encoding/json"
	"net/http"

	"functioncore/pkg/common/cluster"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/worker-manager/utils"
)

// APIServerHandler define api server handler
type APIServerHandler struct {
	Registry       *APIRegistry
	LocalAuth      *localauth.AuthConfig
	Name           string
	AuthCheck      bool
	ForwardRequest bool
	Discovery      cluster.Discovery
}

// newAPIServerHandler New Api Server Handler
func newAPIServerHandler() (*APIServerHandler, error) {
	handler := APIServerHandler{
		Name:           "apiserver",
		Registry:       nil,
		AuthCheck:      true,
		ForwardRequest: false,
		Discovery:      nil,
	}

	return &handler, nil
}

// getRole Write Discovery Role
func (a *APIServerHandler) getRole(w http.ResponseWriter, r *http.Request) {
	role := a.Discovery.Role()
	_, err := w.Write([]byte(role + "\n"))
	if err != nil {
		return
	}
}

// getLeader Write Discovery Leader
func (a *APIServerHandler) getLeader(w http.ResponseWriter, r *http.Request) {
	leader := a.Discovery.Leader()
	if leader == nil {
		_, err := w.Write([]byte("Not found leader" + "\n"))
		if err != nil {
			return
		}
	} else {
		_, err := w.Write([]byte(leader.Host + "\n"))
		if err != nil {
			return
		}
	}

}

// ServeHTTP return Registry Router Serve HTTP
func (a *APIServerHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/role" || r.URL.Path == "/leader" || r.URL.Path == "/healthz" {
		// use node self api
		a.Registry.Router.ServeHTTP(w, r)
		return
	}

	if a.Discovery.Role() != cluster.ClusterRoleLeader && a.ForwardRequest {
		a.Discovery.ForwardRequest(w, r)
		return
	}

	if a.AuthCheck {
		var err error
		requestSign := r.Header.Get(constants.HeaderAuthorization)
		timestamp := r.Header.Get(constants.HeaderAuthTimestamp)
		err = localauth.AuthCheckLocally(a.LocalAuth.AKey, a.LocalAuth.SKey, requestSign, timestamp,
			a.LocalAuth.Duration)
		if err != nil {
			log.GetLogger().Warnf("failed to check authorization of api server handler with error %s",
				err.Error())
			err = errorResponse(w, utils.StatusUnauthorized, err)
			if err != nil {
				log.GetLogger().Warnf("failed to get errorResponse, error: %s", err.Error())
			}
			return
		}
	}
	a.Registry.Router.ServeHTTP(w, r)
}

func errorResponse(w http.ResponseWriter, statusCode int, errorMsg error) error {
	httpStatus, _ := utils.ErrTrans[statusCode]
	w.WriteHeader(httpStatus)
	response, err := json.Marshal(struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{Code: statusCode, Message: errorMsg.Error()})
	if err != nil {
		return err
	}
	_, err = w.Write(response)
	if err != nil {
		return err
	}
	return nil
}
