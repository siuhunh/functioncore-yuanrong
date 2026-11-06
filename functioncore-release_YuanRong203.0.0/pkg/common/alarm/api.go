/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package alarm is used to notify alarm message
package alarm

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"strings"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snapshotmanager"
	commTls "functioncore/pkg/common/tls"
)

var client Client

// ClientOption client option function
type ClientOption func(*Client)

// WithTLSEnable enable TLS option
func WithTLSEnable(tlsConfig *tls.Config) ClientOption {
	if tlsConfig == nil {
		// disable tls
		return func(c *Client) {
			c.scheme = "http"
		}
	}

	// enable tls
	return func(c *Client) {
		tr, ok := http.DefaultTransport.(*http.Transport)
		if ok {
			tr.TLSClientConfig = commTls.GetClientTLSConfig()
			c.httpClient.Transport = tr
			c.scheme = "https"
		}
	}
}

// WithAuth auth option
func WithAuth(ak, sk string) ClientOption {
	return func(c *Client) {
		c.ak = ak
		c.sk = sk
	}
}

// WithSpecLocation specific location info
func WithSpecLocation(location string) ClientOption {
	return func(_ *Client) {
		locationInfo = location
	}
}

// NewClient new once
func NewClient(serverAddress string, serviceName string, stopCh <-chan struct{}, opts ...ClientOption) error {
	client = Client{
		httpClient: &http.Client{
			Timeout: requestTimeoutInterval * time.Second,
		},
		scheme:      "http",
		serviceName: serviceName,
		alarmMetas:  make(map[Type]*alarmMeta, 0),
		sendCh:      make(chan alarmReq, typeSize),
	}

	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("snapshot manager skip reportAlarmInfo")
		return nil
	}

	for _, opt := range opts {
		opt(&client)
	}
	serverAddresses := strings.Split(serverAddress, ",")
	urls := make([]string, len(serverAddresses))
	for i, address := range serverAddresses {
		urls[i] = fmt.Sprintf("%s://%s%s", client.scheme, address, uri)
	}
	client.urls = urls
	go client.reportAlarmInfo(stopCh)
	log.GetLogger().Infof("start alarm client on %s, service: %s", locationInfo, serviceName)
	return nil
}

// GetClient get singleton of alarm client
func GetClient() *Client {
	return &client
}

// RegisterAlarmType register new alarm type
func (c *Client) RegisterAlarmType(almType Type) {
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("snapshot manager skip register alarm type: %s", almType.ResourceID(c.serviceName))
		return
	}
	meta := makeAlarmMeta(almType, &AlarmInfo{
		Severity:     almType.Severity(),
		LocationInfo: locationInfo,
		Cause:        almType.Cause(),
		ResourceId:   almType.ResourceID(c.serviceName),
		AlarmName:    almType.AlarmName(),
		AlarmID:      almType.AlarmID(),
	}, c.getAdditionalPrefix(almType))
	c.Lock()
	c.alarmMetas[almType] = meta
	c.Unlock()
	log.GetLogger().Infof("register alarm type %d %s: %s", almType, c.serviceName, almType.ResourceID(c.serviceName))
}

// CheckAlarmType check alarm is registered or not
func (c *Client) CheckAlarmType(almType Type) bool {
	return c.alarmMetas[almType] != nil
}

func (c *Client) getMeta(almType Type) *alarmMeta {
	c.RLock()
	defer c.RUnlock()
	meta, ok := c.alarmMetas[almType]
	if !ok {
		log.GetLogger().Errorf("not registered alarm type: %s", almType.ResourceID(c.serviceName))
		return nil
	}
	return meta
}

// ReportAlarm report spec alarm with additional information
func (c *Client) ReportAlarm(almType Type, specAddition string) {
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Infof("snapshot manager skip rReportAlarm")
		return
	}
	if meta := c.getMeta(almType); meta != nil {
		var alarmData *alarmReq
		if almType.NeedSuppressed() {
			alarmData = meta.generateAbnormalAlarmWithSuppression(specAddition)
		} else {
			alarmData = meta.generateAbnormalAlarm(specAddition)
		}
		if alarmData != nil {
			c.sendCh <- *alarmData
		}
	}
}

// ClearAlarm clear spec alarm
func (c *Client) ClearAlarm(almType Type) {
	if snapshotmanager.GetSnapshotManagerMode() {
		log.GetLogger().Debugf("snapshot manager skip ClearAlarm")
		return
	}
	if meta := c.getMeta(almType); meta != nil {
		alarmData := meta.generateNormalAlarm()
		if alarmData != nil {
			c.sendCh <- *alarmData
		}
	}
}
