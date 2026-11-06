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

package utils

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/api/schedulerapi"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/faas-scheduler/config"
)

func TestGetRecoverInstancesMsg(t *testing.T) {
	convey.Convey("TestGetRecoverInstancesMsg", t, func() {
		config.FConfig = &config.Config{
			LocalAuth: &localauth.AuthConfig{
				AKey:     "000",
				SKey:     "111",
				Duration: 5,
			},
			HTTPSConfig: &tls.InternalHTTPSConfig{
				HTTPSEnable: false,
				TLSProtocol: "TLSv1.2",
				TLSCiphers:  "TLS_ECDHE_RSA",
			},
		}
		convey.Convey("GetRecoverInstancesMsg internal error", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*http.Client).Do, func(c *http.Client,
					req *http.Request) (*http.Response, error) {
					return &http.Response{Body: nil, StatusCode: http.StatusInternalServerError}, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			GetRecoverInstancesMsg()

			assert.Equal(t, 0, len(GetRecoverInstances().InstancesRes))
		})

		convey.Convey("GetRecoverInstancesMsg connect error", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*http.Client).Do, func(c *http.Client,
					req *http.Request) (*http.Response, error) {
					return &http.Response{Body: nil,
						StatusCode: http.StatusInternalServerError}, errors.New("connect timeout")
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()
			GetRecoverInstancesMsg()
			assert.Equal(t, 0, len(GetRecoverInstances().InstancesRes))
		})

		convey.Convey("GetRecoverInstancesMsg Unmarshal error", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*http.Client).Do, func(c *http.Client,
					req *http.Request) (*http.Response, error) {
					return &http.Response{Body: ioutil.NopCloser(bytes.NewReader([]byte("hello world"))),
						StatusCode: http.StatusOK}, nil
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			GetRecoverInstancesMsg()

			assert.Equal(t, 0, len(GetRecoverInstances().InstancesRes))
		})

		convey.Convey("GetRecoverInstancesMsg success", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc((*http.Client).Do, func(c *http.Client,
					req *http.Request) (*http.Response, error) {
					processStatus := &schedulerapi.ProcessStatus{
						ProcessorID:  0,
						RestThread:   9,
						LeaseVersion: 1,
					}
					processStatusMap := make(map[uint32]*schedulerapi.ProcessStatus)
					processStatusMap[0] = processStatus
					instanceMap := make(map[string]*schedulerapi.InstancesStatus)
					instanceMap["instanceA"] = &schedulerapi.InstancesStatus{
						ProcessesStatusMap: processStatusMap,
					}
					data := schedulerapi.RecoverInstancesRes{InstancesRes: make(map[string]*schedulerapi.CheckInstanceReq)}
					data.InstancesRes["funcA/300-128"] = &schedulerapi.CheckInstanceReq{
						InstancesStatusMap: instanceMap,
					}
					response, err := json.Marshal(data)
					return &http.Response{Body: ioutil.NopCloser(bytes.NewReader(response)), StatusCode: http.StatusOK}, err
				}),
			}
			defer func() {
				for _, p := range patches {
					p.Reset()
				}
			}()

			config.FConfig = &config.Config{
				LocalAuth: &localauth.AuthConfig{
					AKey:     "000",
					SKey:     "111",
					Duration: 5,
				},
				HTTPSConfig: &tls.InternalHTTPSConfig{
					HTTPSEnable: false,
					TLSProtocol: "TLSv1.2",
					TLSCiphers:  "TLS_ECDHE_RSA",
				},
			}
			GetRecoverInstancesMsg()

			assert.Equal(t, 1, len(GetRecoverInstances().InstancesRes))
			assert.Equal(t, 1, len(GetRecoverInstances().InstancesRes["funcA/300-128"].InstancesStatusMap))
			assert.Equal(t, 1, len(GetRecoverInstances().
				InstancesRes["funcA/300-128"].InstancesStatusMap["instanceA"].ProcessesStatusMap))
			assert.Equal(t, 0, int(GetRecoverInstances().
				InstancesRes["funcA/300-128"].InstancesStatusMap["instanceA"].ProcessesStatusMap[0].ProcessorID))
			assert.Equal(t, 9, int(GetRecoverInstances().
				InstancesRes["funcA/300-128"].InstancesStatusMap["instanceA"].ProcessesStatusMap[0].RestThread))
			assert.Equal(t, 1, int(GetRecoverInstances().
				InstancesRes["funcA/300-128"].InstancesStatusMap["instanceA"].ProcessesStatusMap[0].LeaseVersion))

			ResetRecoverInstances()
			assert.Equal(t, 0, len(GetRecoverInstances().InstancesRes))
		})
	})

}
