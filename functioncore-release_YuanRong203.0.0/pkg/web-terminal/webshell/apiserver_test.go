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
	"crypto/tls"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gorilla/websocket"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/web-terminal/config"
)

func TestGetK8SAPIServerConn(t *testing.T) {
	convey.Convey("GetK8SAPIServerConn error", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(config.GetConfig,
				func() (*config.Config, error) {
					return &config.Config{ClusterConfig: clusterConfig}, nil
				}),
		}

		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "111"}
		k8sWssConn := &K8sWssConn{}
		conn, _ := k8sWssConn.GetWssConn(requestInfo)
		assert.NotEqual(t, nil, conn)
	},
	)
}

func TestCreateK8SAPIServerConn(t *testing.T) {
	convey.Convey("createK8SAPIServerConn error", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patches := []*gomonkey.Patches{gomonkey.ApplyFunc(getTLSConfig,
			func(requestInfo *RequestInfo) (*tls.Config, error) {
				return &tls.Config{}, nil
			}),
			gomonkey.ApplyFunc(config.GetConfig,
				func() (*config.Config, error) {
					return &config.Config{ClusterConfig: clusterConfig}, nil
				}),
		}

		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "111"}
		_, err := createK8SAPIServerConn(requestInfo)
		assert.NotEqual(t, nil, err)
	},
	)

	convey.Convey("createK8SAPIServerConn success", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patches := []*gomonkey.Patches{gomonkey.ApplyFunc(getTLSConfig,
			func(requestInfo *RequestInfo) (*tls.Config, error) {
				return &tls.Config{}, nil
			}),
			gomonkey.ApplyFunc(config.GetConfig,
				func() (*config.Config, error) {
					return &config.Config{ClusterConfig: clusterConfig}, nil
				}),
			//(d *Dialer) Dial(urlStr string, requestHeader http.Header) (*Conn, *http.Response, error)
			gomonkey.ApplyMethod(reflect.TypeOf(&websocket.Dialer{}), "Dial", func(_ *websocket.Dialer, urlStr string, requestHeader http.Header) (*websocket.Conn, *http.Response, error) {
				return &websocket.Conn{}, nil, nil
			}),
		}

		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "111"}
		_, err := createK8SAPIServerConn(requestInfo)
		assert.Equal(t, nil, err)
	},
	)

	convey.Convey("createK8SAPIServerConn error1", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patches := []*gomonkey.Patches{gomonkey.ApplyFunc(getTLSConfig,
			func(requestInfo *RequestInfo) (*tls.Config, error) {
				return &tls.Config{}, errors.New("error")
			}),
			gomonkey.ApplyFunc(config.GetConfig,
				func() (*config.Config, error) {
					return &config.Config{ClusterConfig: clusterConfig}, nil
				}),
		}

		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "111"}
		_, err := createK8SAPIServerConn(requestInfo)
		assert.NotEqual(t, nil, err)
	},
	)

	convey.Convey("createK8SAPIServerConn error2", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patches := []*gomonkey.Patches{gomonkey.ApplyFunc(getTLSConfig,
			func(requestInfo *RequestInfo) (*tls.Config, error) {
				return &tls.Config{}, nil
			}),
			gomonkey.ApplyFunc(config.GetConfig,
				func() (*config.Config, error) {
					return &config.Config{ClusterConfig: clusterConfig}, errors.New("error")
				}),
		}

		defer func() {
			for _, patch := range patches {
				patch.Reset()
			}
		}()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "111"}
		_, err := createK8SAPIServerConn(requestInfo)
		assert.NotEqual(t, nil, err)
	},
	)
}

func TestGetClusterConfig(t *testing.T) {
	convey.Convey("getClusterConfig success", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patch := gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Config, error) {
				return &config.Config{ClusterConfig: clusterConfig}, nil
			})
		defer patch.Reset()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "111"}
		cfg, _ := getClusterConfig(requestInfo)
		assert.Equal(t, "192.168.0.1", cfg.Address)
	},
	)

	convey.Convey("getClusterConfig failed test1, clusterid info not exist", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patch := gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Config, error) {
				return &config.Config{ClusterConfig: clusterConfig}, nil
			})
		defer patch.Reset()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "222"}
		_, err := getClusterConfig(requestInfo)
		assert.NotEqual(t, nil, err)
	},
	)

	convey.Convey("getClusterConfig failed test2, getConfig error", t, func() {
		clusterConfig := make(map[string]*config.ClusterConfig, 0)
		clusterConfig["111"] = &config.ClusterConfig{ClusterID: "111", Address: "192.168.0.1"}
		patch := gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Config, error) {
				return &config.Config{ClusterConfig: clusterConfig}, errors.New("error")
			})
		defer patch.Reset()
		requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "111"}
		_, err := getClusterConfig(requestInfo)
		assert.NotEqual(t, nil, err)
	},
	)
}

func TestGetTLSConfig(t *testing.T) {
	convey.Convey("cert file is not exist", t, func() {
		convey.Convey("test1", func() {
			requestInfo := &RequestInfo{TraceID: "1111111111", ClusterID: "222222"}
			_, err := getTLSConfig(requestInfo)
			assert.NotEqual(t, nil, err)
		},
		)
	})
}
