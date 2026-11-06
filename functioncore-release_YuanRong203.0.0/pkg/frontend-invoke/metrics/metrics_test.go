/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package metrics

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/redisclient"
)

func TestMetrics(t *testing.T) {
	t.Run("TestStartReportMetrics", testStartReportMetrics)
	resetMetrics()
	t.Run("TestMetricsChan", testMetricsChan)
}
func testStartReportMetrics(t *testing.T) {
	var redisValue string
	now := time.Now()
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(src string) ([]byte, error) {
				return []byte("test"), nil
			}),
		gomonkey.ApplyFunc(redisclient.New, func(param redisclient.NewRedisClientParam, stopCh <-chan struct{},
			options ...redisclient.Option) (*redisclient.Client, error) {
			return &redisclient.Client{}, nil
		}),
		gomonkey.ApplyFunc(redisclient.ZADDMetricsToRedis,
			func(key string, metrics interface{}, limit int64, expireTime time.Duration) error {
				redisValue = string(metrics.([]byte))
				return nil
			}),
		gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
			c := make(chan time.Time, 1)
			t := &time.Ticker{
				C: c,
			}
			time.Sleep(10 * time.Millisecond)
			c <- time.Now()
			return t
		}),
		gomonkey.ApplyFunc(redisclient.CheckRedisConnectivity, func(clientRedisConfig *redisclient.NewRedisClientParam, stopCh <-chan struct{}) {
		}),
		gomonkey.ApplyFunc(time.Now, func() time.Time {
			return now
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	mockMetrics := RequestStatistics{
		TotalDelay:    0.1,
		BusDelay:      0.02,
		FrontendDelay: 0.01,
		RuntimeDelay:  0.07,
	}
	PublishMetrics(mockMetrics)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int64(0), statistics.count)

	ch := make(chan struct{})
	go StartReportMetrics(ch)
	time.Sleep(100 * time.Millisecond)
	timestamp := now.Unix() - int64(now.Second())
	expect := fmt.Sprintf(`{"timeStamp":%d,"count":1,"totalDelay":0.1,"frontendDelay":0.01,"busDelay":0.02,"runtimeDelay":0.07}`, timestamp)
	assert.Equal(t, expect, redisValue)

	mockMetrics = RequestStatistics{
		ErrorFlag:     true,
		TotalDelay:    0.1,
		BusDelay:      0.02,
		FrontendDelay: 0.05,
		RuntimeDelay:  0.03,
	}
	PublishMetrics(mockMetrics)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, statistics.count, int64(1))
	assert.Equal(t, statistics.errCount, int64(1))
	assert.Equal(t, statistics.totalDelay, 0.1)
	assert.Equal(t, statistics.runtimeDelay, 0.03)

	close(ch)
	time.Sleep(10 * time.Millisecond)
	StartReportMetrics(nil)
	subscribeMetrics(nil)
}

func testMetricsChan(t *testing.T) {
	go func() {
		for i := 0; i < 100; i++ {
			PublishMetrics(RequestStatistics{
				TotalDelay: float64(i) / 100.0,
			})
		}
	}()
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 100, len(metricsChan))

	go func() {
		for i := 0; i < bufferSize+50; i++ {
			PublishMetrics(RequestStatistics{
				TotalDelay: float64(i) / 100.0,
			})
		}
	}()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, bufferSize, len(metricsChan))
	ch := make(chan struct{})
	go subscribeMetrics(ch)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, int64(bufferSize), statistics.count)
	assert.Equal(t, 0, len(metricsChan))
	resetMetrics()
	go func() {
		for i := 0; i < bufferSize; i++ {
			PublishMetrics(RequestStatistics{
				TotalDelay: float64(i) / 100.0,
			})
		}
		time.Sleep(10 * time.Millisecond)
		for i := 0; i < bufferSize/2; i++ {
			PublishMetrics(RequestStatistics{
				TotalDelay: float64(i) / 100.0,
			})
		}
	}()
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, int64(bufferSize/2+bufferSize), statistics.count)

	go func() {
		for i := 0; i < bufferSize/2; i++ {
			PublishMetrics(RequestStatistics{
				TotalDelay: float64(i) / 100.0,
			})
		}
		time.Sleep(50 * time.Millisecond)
		for i := 0; i < bufferSize; i++ {
			PublishMetrics(RequestStatistics{
				TotalDelay: float64(i) / 100.0,
			})
		}
	}()
	time.Sleep(1 * time.Millisecond)
	b := resetMetrics()
	time.Sleep(100 * time.Millisecond)

	var request RequestInfoMetrics
	err := json.Unmarshal(b, &request)
	assert.Nil(t, err)
	assert.Equal(t, int64(3*bufferSize), statistics.count+request.Count)
}
