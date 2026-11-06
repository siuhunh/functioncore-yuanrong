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

// Package router start the service.
package router

import (
	"net/http"

	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/util"
)

func writeHeaders(w http.ResponseWriter, status int, msg string) error {
	w.WriteHeader(status)
	_, err := w.Write([]byte(msg))
	return err
}

func handleHealthCheck(w http.ResponseWriter, request *http.Request) {
	var connected = true
	functioncore.RegEtcdConnStatusHandler(func(c bool) {
		connected = c
	})
	if !connected {
		err := writeHeaders(w, http.StatusInternalServerError, "failed to connect to etcd")
		if err != nil {
			util.DelayExit("writeHeadersError", err)
			return
		}
	} else {
		err := writeHeaders(w, http.StatusOK, "ok")
		if err != nil {
			util.DelayExit("writeHeadersError", err)
			return
		}
	}
}

// Init the router.
func Init() {
	http.HandleFunc("/healthz", handleHealthCheck)
}
