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

// Package alarm is used to notify alarm message to FG
package alarm

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"os"
	"sync"
	"time"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

const (
	// The maximum sleep duration is 128s.
	exponentialRetry = 7
	alarmSuppressNum = 10
	alarmInterval    = 30 * time.Minute
	uri              = "/v2/fgs/om/alarm"
)

var (
	locationInfo = fmt.Sprintf("Region=%s;ClusterInfo=%s;HostIP=%s;",
		os.Getenv("REGION_NAME"), os.Getenv("CLUSTER_ID"), os.Getenv("NODE_IP"))
	podIP   = os.Getenv("POD_IP")
	podName = os.Getenv("POD_NAME")
)

func (c *Client) getAdditionalPrefix(almType Type) string {
	if podIP != "" && podName != "" {
		return fmt.Sprintf("CloudService=FunctionGraph,Service=FunctionGraph,MicroService=%s,"+
			"NativeMeDn=0.0.0.0,containerIP=%s,PodName=%s,Resource=%s,", c.serviceName,
			podIP, podName, almType.ResourceID(c.serviceName))
	}

	return fmt.Sprintf("CloudService=FunctionGraph,Service=FunctionGraph,MicroService=%s,"+
		"NativeMeDn=0.0.0.0,Resource=%s,", c.serviceName, almType.ResourceID(c.serviceName))
}

// AlarmInfo describes alarm info
type AlarmInfo struct {
	Category     string `json:"category,omitempty"`
	Severity     string `json:"severity,omitempty"`
	LocationInfo string `json:"locationinfo,omitempty"`
	Additional   string `json:"additional,omitempty"`
	Cause        string `json:"cause,omitempty"`
	ResourceId   string `json:"resourceid,omitempty"`
	AlarmName    string `json:"alarmname,omitempty"`
	AlarmID      string `json:"alarmid,omitempty"`
}

// ToString alarm content to string format
func (alarmInfo *AlarmInfo) ToString() string {
	return fmt.Sprintf("alarm type:%s, resource:%s, severity:%s, cause: %s, additional: %s, alarmName: %s, "+
		"alarmid: %s", alarmInfo.Category, alarmInfo.ResourceId, alarmInfo.Severity, alarmInfo.Cause,
		alarmInfo.Additional, alarmInfo.AlarmName, alarmInfo.AlarmID)
}

// Summary alarm content summary
func (alarmInfo *AlarmInfo) Summary() string {
	return fmt.Sprintf("alarm type:%s, resource:%s, severity:%s",
		alarmInfo.Category, alarmInfo.ResourceId, alarmInfo.Severity)
}

func makeAlarmMeta(almType Type, info *AlarmInfo, additionalPrefix string) *alarmMeta {
	return &alarmMeta{
		almType:          almType,
		info:             info,
		isNormal:         true,
		lock:             new(sync.Mutex),
		cancel:           nil,
		time:             time.Now(),
		num:              0,
		additionalPrefix: additionalPrefix,
	}
}

type alarmMeta struct {
	info             *AlarmInfo
	lock             *sync.Mutex
	almType          Type
	isNormal         bool
	cancel           func()
	time             time.Time
	num              int64
	additionalPrefix string
}

// Client to report an alarm as a client when an error occurs
type Client struct {
	httpClient *http.Client
	alarmMetas map[Type]*alarmMeta
	sync.RWMutex
	scheme string

	// identify alarm resource name, like 'FunctionTask'
	serviceName string
	sendCh      chan alarmReq
	urls        []string

	// enable auth
	ak string
	sk string
}

type alarmReq struct {
	summary string
	ctx     context.Context
	cancel  func()
	data    []byte
}

func (a *alarmMeta) genNewAlarmDataAndCancelPrevious() (*alarmReq, error) {
	data, err := json.Marshal(a.info)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal alarmInfo, resource: %s, err: %s", a.info.ResourceId, err.Error())
		return nil, err
	}
	almData := &alarmReq{summary: a.info.Summary()}
	ctx, cancel := context.WithCancel(context.Background())
	almData.ctx = ctx
	almData.data = data

	// cancel previous alarm and update new cancel method
	if a.cancel != nil {
		a.cancel()
	}
	a.cancel = cancel
	return almData, nil
}

// notifyAlarm upload alarm message
func (c *Client) notifyAlarm(almData alarmReq) {
	var retryCount uint32 = 0
	for {
		select {
		case <-almData.ctx.Done():
			log.GetLogger().Infof("stop this alarm, resource: %s ", almData.summary)
			return
		default:
			err := c.doAlarmRequestByUrls(almData)
			if err != nil {
				log.GetLogger().Errorf("failed to request alarm, resource: %s, err: %s",
					almData.summary, err.Error())
				backOff := exponentialWithJitterBackoff(retryCount)
				time.Sleep(backOff)
				retryCount++
				continue
			}
			return
		}
	}
}

func (c *Client) doAlarmRequestByUrls(almData alarmReq) error {
	urls := utils.ShuffleOneArray(c.urls)
	var err error
	for _, url := range urls {
		err = c.doAlarmRequest(almData, url)
		if err != nil {
			log.GetLogger().Errorf("failed to request alarm url: %s, err: %s ", url, err.Error())
			// Failed to retry the next URL.
			continue
		}
		return err
	}
	return err
}

func (c *Client) doAlarmRequest(almData alarmReq, url string) error {
	req := c.newHTTPRequest(almData, url)
	if req == nil {
		return fmt.Errorf("new http request error")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			log.GetLogger().Infof("cancel this alarm, alarm resource %s ", almData.summary)
			return nil
		}
		return err
	}
	defer closeResponse(resp)
	if resp != nil && resp.StatusCode != http.StatusOK {
		if resp.Body != nil {
			if respBody, err := ioutil.ReadAll(resp.Body); err == nil {
				return fmt.Errorf("resp code: %d, resp body: %s", resp.StatusCode, respBody)
			}
		}
		return fmt.Errorf("resp code: %d", resp.StatusCode)
	}
	log.GetLogger().Infof("reported alarm successfully, alarm resource %s",
		almData.summary)
	return nil
}

func (a *alarmMeta) generateNormalAlarm() *alarmReq {
	a.lock.Lock()
	if a.isNormal {
		a.lock.Unlock()
		return nil
	}
	a.info.Category = Clear
	a.isNormal = true
	almData, err := a.genNewAlarmDataAndCancelPrevious()
	if err != nil {
		a.lock.Unlock()
		return nil
	}
	log.GetLogger().Infof("report normal alarm info, %s, isNormal: %t",
		a.info.ToString(), a.isNormal)
	a.lock.Unlock()
	return almData
}

func (a *alarmMeta) generateAbnormalAlarmInternal(specAddition string) *alarmReq {
	a.info.Category = Report
	a.isNormal = false
	a.info.Additional = a.additionalPrefix + specAddition
	almData, err := a.genNewAlarmDataAndCancelPrevious()
	if err != nil {
		return nil
	}
	log.GetLogger().Infof("report abnormal alarm info, %s, isNormal %t",
		a.info.ToString(), a.isNormal)
	return almData
}

func (a *alarmMeta) generateAbnormalAlarm(specAddition string) *alarmReq {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.isNormal {
		return nil
	}
	return a.generateAbnormalAlarmInternal(specAddition)
}

// Alarm suppression is implemented here.
func (a *alarmMeta) generateAbnormalAlarmWithSuppression(specAddition string) *alarmReq {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.isNormal {
		return nil
	}
	now := time.Now()
	if a.num == 0 || now.Sub(a.time) > alarmInterval {
		a.num = 1
		a.time = time.Now()
		return a.generateAbnormalAlarmInternal(specAddition)
	}
	if a.num%alarmSuppressNum == 0 {
		a.num++
		return a.generateAbnormalAlarmInternal(specAddition)
	}
	a.num++
	log.GetLogger().Infof("The current alarm %s is suppressed, num: %d", a.info.ResourceId, a.num)
	return nil
}

func (c *Client) reportAlarmInfo(stopCh <-chan struct{}) {
	if stopCh == nil || c.sendCh == nil {
		log.GetLogger().Errorf("invalid stop channel or send channel")
		return
	}
	for {
		select {
		case <-stopCh:
			log.GetLogger().Warnf("received termination signal to shutdown")
			return
		case curAlarmData, ok := <-c.sendCh:
			if !ok {
				log.GetLogger().Errorf("received termination signal to shutdown sendCh")
				return
			}
			go c.notifyAlarm(curAlarmData)
		}
	}
}

func exponentialWithJitterBackoff(retryCount uint32) time.Duration {
	if retryCount > exponentialRetry {
		retryCount = exponentialRetry
	}
	sleep := math.Exp2(float64(retryCount))
	return time.Duration(rand.Int63n(int64(sleep))) * time.Second
}

func (c *Client) newHTTPRequest(almData alarmReq, url string) *http.Request {
	curRequest, err := http.NewRequestWithContext(almData.ctx, "POST", url, bytes.NewReader(almData.data))
	if err != nil {
		log.GetLogger().Errorf("failed to make alarm request, resource %s, err %s",
			almData.summary, err.Error())
		return nil
	}
	authorization, timestamp := localauth.SignOMSVC(c.ak, c.sk, url, almData.data)
	curRequest.Header.Set(HeaderAlarmAuthorization, authorization)
	curRequest.Header.Set(HeaderAlarmAuthTimestamp, timestamp)
	curRequest.Header.Set("Content-Type", "application/json")
	curRequest.Header.Set("Accept", "application/json")
	return curRequest
}

func closeResponse(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	if err := resp.Body.Close(); err != nil {
		log.GetLogger().Errorf("failed to close response body, err: %s", err.Error())
	}
}
