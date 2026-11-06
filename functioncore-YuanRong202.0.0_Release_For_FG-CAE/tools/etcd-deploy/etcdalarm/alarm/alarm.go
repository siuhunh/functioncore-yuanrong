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

// Package alarm -
package alarm

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	selfTls "functioncore/pkg/common/tls"
	"functioncore/tools/etcd-deploy/etcdalarm/config"
)

const (
	requestTimeoutInterval = 10

	// HeaderAlarmAuthTimestamp is the timestamp for alarm authorization
	HeaderAlarmAuthTimestamp = "X-CFF-Timestamp-Auth"
	// HeaderAlarmAuthorization is authorization
	HeaderAlarmAuthorization = "X-CFF-Authorization"
)

// Request alarm http request.
type Request struct {
	request *http.Request
	ctx     context.Context
	cancel  func()
}

// Alarm to report an alarm as a client when an error occurs
type Alarm struct {
	httpClient  *http.Client
	AlarmInfo   *Info
	lock        *sync.Mutex
	stopCh      <-chan struct{}
	cancel      func()
	time        time.Time
	baseURLPath string
	num         int64
}

var (
	regionName  string
	clusterInfo string
	nodeIP      string
)

// Info describes alarm info
type Info struct {
	Category       string `json:"category,omitempty"`
	Severity       string `json:"severity,omitempty"`
	LocationInfo   string `json:"locationinfo,omitempty"`
	Additional     string `json:"additional,omitempty"`
	Cause          string `json:"cause,omitempty"`
	ResourceID     string `json:"resourceid,omitempty"`
	AdditionalTemp string `json:"-"`
}

// NewAlarmClient init an alarm client
func NewAlarmClient(tls *tls.Config, cfg *config.Config) (*Alarm, error) {
	alarm := &Alarm{}
	if err := alarm.initClient(requestTimeoutInterval*time.Second, tls, cfg.Address); err != nil {
		return nil, err
	}
	alarm.lock = new(sync.Mutex)
	alarm.time = time.Now()
	return alarm, nil
}

// InitClient inits http client
func (a *Alarm) initClient(timeout time.Duration, tls *tls.Config, baseURLPath string) error {
	a.httpClient = &http.Client{
		Timeout: timeout,
	}
	scheme := "http"
	if tls != nil {
		tr, ok := http.DefaultTransport.(*http.Transport)
		if !ok {
			return errors.New("failed to assert http default transport")
		}
		tr.TLSClientConfig = selfTls.GetClientTLSConfig()
		a.httpClient.Transport = tr
		scheme = "https"
	}
	a.baseURLPath = fmt.Sprintf("%s://%s", scheme, baseURLPath)
	return nil
}

// InitAlarmInfo -
func InitAlarmInfo(category string, info *config.AlarmInfo, regionCfg *config.LocationInfo) *Info {
	if regionCfg == nil {
		log.GetLogger().Error("the regionConfig is not configured")
		os.Exit(1)
	}
	if info == nil {
		log.GetLogger().Error("the alarm info is not configured")
		os.Exit(1)
	}
	clusterInfo = regionCfg.Cluster
	regionName = regionCfg.Region
	nodeIP = regionCfg.NodeIP
	additionalTemp := fmt.Sprintf("CloudService=FunctionGraph,Service=FunctionGraph,"+
		"Resource=%s,NativeMeDn=0.0.0.0,msg=%s", info.Resource, info.Content)
	locationInfo := fmt.Sprintf("Region=%s;ClusterInfo=%s;HostIP=%s;",
		regionName, clusterInfo, nodeIP)
	alarmInfo := &Info{
		Category:     category,
		Severity:     info.ImportantLevel,
		LocationInfo: locationInfo,
		Additional:   additionalTemp,
		ResourceID:   info.Resource,
	}
	return alarmInfo
}

// GenAlarmRequest build a request
func (a *Alarm) GenAlarmRequest(ak, sk string) (Request, error) {
	req := Request{}
	uri := "/v2/fgs/om/alarm"
	requestURI := fmt.Sprintf("%s%s", a.baseURLPath, uri)
	data, err := json.Marshal(a.AlarmInfo)
	if err != nil {
		return Request{}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	curRequest, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		return Request{}, err
	}
	authorization, timestamp := localauth.SignOMSVC(ak, sk, requestURI, data)
	curRequest.Header.Set(HeaderAlarmAuthorization, authorization)
	curRequest.Header.Set(HeaderAlarmAuthTimestamp, timestamp)
	curRequest.Header.Set("Content-Type", "application/json")
	curRequest.Header.Set("Accept", "application/json")
	req.request = curRequest
	req.cancel = cancel
	req.ctx = ctx
	return req, nil
}

// NotifyAlarm upload alarm message
func (a *Alarm) NotifyAlarm(request Request) {
	select {
	case <-request.ctx.Done():
		return
	default:
		var resp *http.Response
		resp, err := a.httpClient.Do(request.request)
		if err != nil && errors.Is(err, context.Canceled) {
			log.GetLogger().Errorf("cancel this alarm, alarm info %s ", a.AlarmInfo.ToString())
			os.Exit(1)
		}
		if err != nil {
			log.GetLogger().Errorf("failed to request alarm, alarm type:%s, resource: %s, err: %s",
				a.AlarmInfo.Category, a.AlarmInfo.ResourceID, err.Error())
			os.Exit(1)
		}
		if resp != nil && resp.StatusCode != http.StatusOK {
			log.GetLogger().Errorf("failed to request alarm, alarm type:%s, resource:%s, status code:%d",
				a.AlarmInfo.Category, a.AlarmInfo.ResourceID, resp.StatusCode)
			os.Exit(1)
		}
		log.GetLogger().Infof("reported alarm successfully, alarm type:%s, resource: %s",
			a.AlarmInfo.Category, a.AlarmInfo.ResourceID)
	}
}

// ToString Info to string format
func (alarmInfo *Info) ToString() string {
	return fmt.Sprintf("alarm type:%s, resource:%s, severity:%s, cause: %s",
		alarmInfo.Category, alarmInfo.ResourceID, alarmInfo.Severity, alarmInfo.Cause)
}
