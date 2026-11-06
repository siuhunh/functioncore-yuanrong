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

// Package webshell define function to exec webshell
package webshell

import (
	"encoding/json"
	"errors"
	"net/http"

	k8serr "k8s.io/apimachinery/pkg/api/errors"

	"functioncore/pkg/common/logger/log"
)

// APIError define web-terminal error
type APIError struct {
	Message string
	Code    int
}

// Error -
func (e *APIError) Error() string {
	return e.Message
}

// ResponseErrorMsg response with error message
func ResponseErrorMsg(w http.ResponseWriter, statusCode int, errMsg string, traceID string) {
	apiErrorImpl(w, &APIError{Code: statusCode, Message: errMsg}, traceID)
}

// ResponseError response with error object
func ResponseError(w http.ResponseWriter, err error, traceID string) {
	apiErrorImpl(w, err, traceID)
}

func apiErrorImpl(w http.ResponseWriter, err error, traceID string) {
	var v *k8serr.StatusError
	var e *APIError
	status := http.StatusInternalServerError
	code := http.StatusText(http.StatusInternalServerError)
	message := err.Error()
	if errors.As(err, &v) {
		status = int(v.ErrStatus.Code)
		code = string(v.ErrStatus.Reason)
		message = v.ErrStatus.Message
	} else if errors.As(err, &e) {
		status = e.Code
		code = http.StatusText(e.Code)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	jErr := json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  status,
		"code":    code,
		"message": message,
	})
	if jErr != nil {
		log.GetLogger().Errorf("failed to marshal response data: %s", jErr.Error())
	}
	log.GetLogger().Infof("request end in web-terminal, traceID: %s", traceID)
}
