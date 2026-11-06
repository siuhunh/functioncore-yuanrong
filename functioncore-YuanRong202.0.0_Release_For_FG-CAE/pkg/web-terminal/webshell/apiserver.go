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
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"github.com/gorilla/websocket"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/web-terminal/config"
	"functioncore/pkg/web-terminal/constant"
)

// K8sWssConn -
type K8sWssConn struct {
}

// GetWssConn -
func (*K8sWssConn) GetWssConn(requestInfo *RequestInfo) (*websocket.Conn, error) {
	var err error
	var conn *websocket.Conn
	for i := 0; i < constant.MaxSendRetryTimes; i++ {
		if conn, err = createK8SAPIServerConn(requestInfo); err == nil {
			return conn, nil
		}
		log.GetLogger().Errorf("failed to create wss connection for clusterID: %s, namespace: %s, podName: %s, "+
			"err: %s", requestInfo.ClusterID, requestInfo.Namespace, requestInfo.PodName, err.Error())
		time.Sleep(constant.DialInterval)
	}

	log.GetLogger().Errorf("retry create wss connection for clusterID: %s, namespace: %s, podName: %s failed",
		requestInfo.ClusterID, requestInfo.Namespace, requestInfo.PodName)
	return nil, err
}

func createK8SAPIServerConn(requestInfo *RequestInfo) (*websocket.Conn, error) {
	tlsConfig, err := getTLSConfig(requestInfo)
	if err != nil {
		return nil, err
	}

	dialer := websocket.DefaultDialer
	dialer.TLSClientConfig = tlsConfig
	dialer.Subprotocols = []string{constant.SubProtocols}
	dialer.WriteBufferSize = constant.DefaultBufferSize
	dialer.ReadBufferSize = constant.DefaultBufferSize
	dialer.HandshakeTimeout = constant.HandshakeTimeOut

	urlParam := url.Values{}
	urlParam.Set("container", constant.ContainerRuntime)
	urlParam.Set("command", requestInfo.Command)
	urlParam.Set("stdin", "1")
	urlParam.Set("stdout", "1")
	urlParam.Set("stderr", "1")
	urlParam.Set("tty", "1")

	clusterConfig, err := getClusterConfig(requestInfo)
	if err != nil {
		return nil, err
	}

	urlPath := fmt.Sprintf(constant.APIServerExecRequestURL, requestInfo.Namespace, requestInfo.PodName)
	host := clusterConfig.Address
	if len(host) == 0 {
		return nil, fmt.Errorf("failed to get api server info of cluster %s, namespace %s, podName %s",
			requestInfo.ClusterID, requestInfo.Namespace, requestInfo.PodName)
	}
	u := url.URL{Scheme: constant.SchemeWSS, Host: clusterConfig.Address, Path: urlPath,
		RawQuery: urlParam.Encode()}
	conn, _, err := dialer.Dial(u.String(), make(http.Header))
	if err != nil {
		log.GetLogger().Errorf("create wss connection for %s failed, err: %s", u, err.Error())
		return nil, err
	}
	return conn, nil
}

func getClusterConfig(requestInfo *RequestInfo) (*config.ClusterConfig, error) {
	conf, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get web terminal config, err: %s", err.Error())
		return nil, err
	}

	clusterConfig, ok := conf.ClusterConfig[requestInfo.ClusterID]
	if !ok {
		log.GetLogger().Errorf("failed to get cluster %s config", requestInfo.ClusterID)
		return nil, fmt.Errorf("failed to get cluster %s config", requestInfo.ClusterID)
	}
	return clusterConfig, nil
}

func getTLSConfig(requestInfo *RequestInfo) (*tls.Config, error) {
	caCertPath := filepath.Join(constant.ClusterCertPathPrefix, requestInfo.ClusterID, constant.CACrt)
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		log.GetLogger().Errorf("failed to read file, err: %s", err.Error())
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	clientCertPath := filepath.Join(constant.ClusterCertPathPrefix, requestInfo.ClusterID, constant.ClientCrt)
	clientKeyPath := filepath.Join(constant.ClusterCertPathPrefix, requestInfo.ClusterID, constant.ClientKey)
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		log.GetLogger().Errorf("failed to LoadX509KeyPair: %s", err.Error())
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{clientCert},
	}
	return tlsConfig, nil
}
