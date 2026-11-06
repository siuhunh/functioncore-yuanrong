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

// Package fast -
package fast

import (
	"math"
	"time"

	fhttp "github.com/valyala/fasthttp"

	"functioncore/pkg/cli/config"
	"functioncore/pkg/cli/pkg/http/common"
)

// Fast -
type Fast struct {
	Client *fhttp.Client
}

func setRequestHeaders(request *fhttp.Request, headers map[string]string) {
	if len(headers) == 0 {
		return
	}
	for key, value := range headers {
		request.Header.Set(key, value)
	}
}

// Post -
func (fast *Fast) Post(path string, body []byte, headers map[string]string) (*fhttp.Response, error) {
	request := fhttp.AcquireRequest()
	response := fhttp.AcquireResponse()
	setRequestHeaders(request, headers)
	request.Header.Set(common.HeaderContentType, common.ApplicationJSONUTF8)
	request.Header.SetMethod(fhttp.MethodPost)
	request.URI().DisablePathNormalizing = true
	request.URI().SetHost(config.FConfig.CLIConfig.Host)
	request.URI().SetPath(path)
	if config.FConfig.CLIConfig.EnableHTTPS {
		request.URI().SetScheme("https")
	}
	request.SetBody(body)
	timeout := math.MaxInt32 * time.Second
	fast.Client.ReadTimeout = timeout
	if err := fast.Client.DoTimeout(request, response, timeout); err != nil {
		return nil, err
	}
	return response, nil
}
