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
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	selfTls "functioncore/pkg/common/tls"
	"functioncore/tools/autoscaleralarm/config"
)

var (
	alarmFlag   bool
	regionName  string
	clusterInfo string
	nodeIP      string
)

const (
	requestTimeoutInterval = 10

	// HeaderAlarmAuthTimestamp is the timestamp for alarm authorization
	HeaderAlarmAuthTimestamp = "X-CFF-Timestamp-Auth"
	// HeaderAlarmAuthorization is authorization
	HeaderAlarmAuthorization = "X-CFF-Authorization"

	autoScaleMasterLockKey = "autoscalerMasterLock"

	checkInterval = 10 * time.Second

	reportAlarm = "1"

	clearAlarm = "2"

	importantLevel = "1"
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
		log.GetLogger().Errorf("init alarm client error")
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
func InitAlarmInfo(category string, info *config.AlarmInfo, regionCfg *config.LocationInfo) (*Info, error) {
	if regionCfg == nil {
		log.GetLogger().Error("the regionConfig is not configured")
		return nil, errors.New("regionCfg is nil")
	}
	if info == nil {
		log.GetLogger().Error("the alarm info is not configured")
		return nil, errors.New("AlarmInfo is nil")
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
	return alarmInfo, nil
}

// GenAlarmRequest build a request
func (a *Alarm) GenAlarmRequest(ak, sk string) (Request, error) {
	req := Request{}
	uri := "/v2/fgs/om/alarm"
	requestURI := fmt.Sprintf("%s%s", a.baseURLPath, uri)
	data, err := json.Marshal(a.AlarmInfo)
	if err != nil {
		log.GetLogger().Errorf("json marshal alarm info error")
		return Request{}, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	curRequest, err := http.NewRequestWithContext(ctx, "POST", requestURI, bytes.NewBuffer(data))
	if err != nil {
		log.GetLogger().Errorf("create alarm req error")
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
func (a *Alarm) NotifyAlarm(request Request) error {
	var resp *http.Response
	resp, err := a.httpClient.Do(request.request)
	if err != nil && errors.Is(err, context.Canceled) {
		log.GetLogger().Errorf("cancel this alarm, alarm info %s ", a.AlarmInfo.ToString())
		return err
	}
	if err != nil {
		log.GetLogger().Errorf("failed to request alarm, alarm type:%s, resource: %s, err: %s",
			a.AlarmInfo.Category, a.AlarmInfo.ResourceID, err.Error())
		return err
	}
	if resp != nil && resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("failed to request alarm, alarm type:%s, resource:%s, status code:%d",
			a.AlarmInfo.Category, a.AlarmInfo.ResourceID, resp.StatusCode)
		return err
	}
	log.GetLogger().Infof("reported alarm successfully, alarm type:%s, resource: %s",
		a.AlarmInfo.Category, a.AlarmInfo.ResourceID)
	return nil
}

// ToString Info to string format
func (alarmInfo *Info) ToString() string {
	return fmt.Sprintf("alarm type:%s, resource:%s, severity:%s, cause: %s",
		alarmInfo.Category, alarmInfo.ResourceID, alarmInfo.Severity, alarmInfo.Cause)
}

func (a *Alarm) sendAlarmMessage(cfg *config.Config) error {
	resp, err := getAutoScalerMasterLockKey()
	if err != nil {
		log.GetLogger().Warnf("query route etcd info fail! reason: %s", err.Error())
		return err
	}
	if len(resp.Kvs) == 0 && alarmFlag == true || len(resp.Kvs) != 0 && alarmFlag == false {
		return nil
	}
	info := &config.AlarmInfo{
		ImportantLevel: importantLevel,
		Content:        "",
		Resource:       fmt.Sprintf("%d", alarm.AutoScalerInstance),
	}

	if len(resp.Kvs) == 0 && alarmFlag == false {
		alarmFlag = true
		log.GetLogger().Warnf("autoscaler master node not exist, report alarm ")
		info.Content = fmt.Sprintf("failed to get autoscalerMasterLockKey key:%s, report alarm", autoScaleMasterLockKey)
		a.AlarmInfo, err = InitAlarmInfo(reportAlarm, info, cfg.LocationInfo)
		if err != nil {
			return err
		}
	}

	if len(resp.Kvs) != 0 && alarmFlag == true {
		alarmFlag = false
		log.GetLogger().Infof("autoscaler master node exist, clear alarm")
		info.Content = fmt.Sprintf("get autoscalerMasterLockKey key:%s ok, clear alarm", autoScaleMasterLockKey)
		a.AlarmInfo, err = InitAlarmInfo(clearAlarm, info, cfg.LocationInfo)
		if err != nil {
			return err
		}
	}

	req, err := a.GenAlarmRequest(cfg.AuthConfig.AKey, cfg.AuthConfig.SKey)
	if err != nil {
		log.GetLogger().Errorf("failed to gen alarm request, err: %s", err.Error())
		return errors.New("create alarm req error")
	}

	return a.NotifyAlarm(req)
}

func getAutoScalerMasterLockKey() (*clientv3.GetResponse, error) {
	routerClient := etcd3.GetRouterEtcdClient()
	if routerClient == nil || routerClient.Client == nil {
		log.GetLogger().Warnf("failed to init route ETCDclient")
		return nil, errors.New("failed to init route ETCDclient")
	}
	ctx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := routerClient.GetResponse(ctx, autoScaleMasterLockKey)
	if resp == nil || err != nil {
		log.GetLogger().Warnf("failed to get autoscalerMasterLockKey key:%s, err:%s", autoScaleMasterLockKey, err)
		return nil, errors.New("failed to get autoscalerMasterLockKey")
	}
	return resp, nil
}

// Witch  alarm witch
func (a *Alarm) Witch(cfg *config.Config, stopCh <-chan struct{}) {
	log.GetLogger().Infof("register master node circle service start!")
	checkTicker := time.NewTicker(checkInterval)
	for {
		select {
		case <-checkTicker.C:
			err := a.sendAlarmMessage(cfg)
			if err != nil {
				log.GetLogger().Errorf("send message error, error is %s", err.Error())
			}
		case <-stopCh:
			checkTicker.Stop()
			log.GetLogger().Infof("register master node circle service close!")
			return
		}
	}
}
