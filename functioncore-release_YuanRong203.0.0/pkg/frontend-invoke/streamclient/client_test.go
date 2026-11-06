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

package streamclient

import (
	"clients/common"
	"clients/streamcache"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/test/utils"
)

func TestCreateClient(t *testing.T) {
	os.Setenv("NODE_IP", "127.0.0.1")
	t.Run("succeed to create stream client", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = false
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Init",
			func(*streamcache.StreamClient, bool) common.Status {
				return common.CreateStatus(common.Ok, "")
			})
		patch.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Subscribe",
			func(*streamcache.StreamClient, string, streamcache.SubscriptionConfig, bool,
				...uint32) (*streamcache.StreamConsumer, common.Status) {
				return &streamcache.StreamConsumer{}, common.CreateStatus(common.Ok, "")
			})
		defer func() {
			patch.Reset()
		}()
		c, err := createClient("tenantID")
		assert.Nil(t, err)
		assert.NotNil(t, c)
	})
	t.Run("failed to create stream client", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = false
		patch := gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Init",
			func(*streamcache.StreamClient, bool) common.Status {
				return common.CreateStatus(common.UnexpectedError, "error")
			})
		defer func() {
			patch.Reset()
		}()
		c, err := createClient("tenantID")
		assert.NotNil(t, err)
		assert.Nil(t, c)
	})
	t.Run("succeed to create stream client with aksk", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = true
		constant.FConfig.LocalAuth = &localauth.AuthConfig{
			AKey: "ak",
			SKey: "sk",
		}
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Init",
				func(*streamcache.StreamClient, bool) common.Status {
					return common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Subscribe",
				func(*streamcache.StreamClient, string, streamcache.SubscriptionConfig, bool,
					...uint32) (*streamcache.StreamConsumer, common.Status) {
					return &streamcache.StreamConsumer{}, common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(_ string) ([]byte, error) {
				return []byte(""), nil
			}),
		})
		defer patches.ResetAll()
		c, err := createClient("tenantID")
		assert.Nil(t, err)
		assert.NotNil(t, c)
	})
	t.Run("failed to create stream client with aksk: localAuth is nil", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = true
		constant.FConfig.LocalAuth = nil
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Init",
				func(*streamcache.StreamClient, bool) common.Status {
					return common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Subscribe",
				func(*streamcache.StreamClient, string, streamcache.SubscriptionConfig, bool,
					...uint32) (*streamcache.StreamConsumer, common.Status) {
					return &streamcache.StreamConsumer{}, common.CreateStatus(common.Ok, "")
				}),
		})
		defer patches.ResetAll()
		_, err := createClient("tenantID")
		assert.NotNil(t, err)
		assert.Equal(t, "localAuth is nil", err.Error())
	})
	t.Run("failed to create stream client with aksk: failed to decrypt ak", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = true
		constant.FConfig.LocalAuth = &localauth.AuthConfig{
			AKey: "ak",
			SKey: "sk",
		}
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Init",
				func(*streamcache.StreamClient, bool) common.Status {
					return common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Subscribe",
				func(*streamcache.StreamClient, string, streamcache.SubscriptionConfig, bool,
					...uint32) (*streamcache.StreamConsumer, common.Status) {
					return &streamcache.StreamConsumer{}, common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				if src == "ak" {
					return nil, errors.New("decrypt ak error")
				}
				return []byte(""), nil
			}),
		})
		defer patches.ResetAll()
		_, err := createClient("tenantID")
		assert.NotNil(t, err)
		assert.Equal(t, "decrypt ak error", err.Error())
	})
	t.Run("failed to create stream client with aksk: failed to decrypt sk", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = true
		constant.FConfig.LocalAuth = &localauth.AuthConfig{
			AKey: "ak",
			SKey: "sk",
		}
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Init",
				func(*streamcache.StreamClient, bool) common.Status {
					return common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Subscribe",
				func(*streamcache.StreamClient, string, streamcache.SubscriptionConfig, bool,
					...uint32) (*streamcache.StreamConsumer, common.Status) {
					return &streamcache.StreamConsumer{}, common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				if src == "sk" {
					return nil, errors.New("decrypt sk error")
				}
				return []byte(""), nil
			}),
		})
		defer patches.ResetAll()
		_, err := createClient("tenantID")
		assert.NotNil(t, err)
		assert.Equal(t, "decrypt sk error", err.Error())
	})
	t.Run("succeed to create stream client, iamToken is enable", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = true
		constant.FConfig.DataSystem.IAMTokenEnable = true
		constant.FConfig.LocalAuth = &localauth.AuthConfig{
			AKey: "ak",
			SKey: "sk",
		}
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Init",
				func(*streamcache.StreamClient, bool) common.Status {
					return common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyMethod(reflect.TypeOf(&streamcache.StreamClient{}), "Subscribe",
				func(*streamcache.StreamClient, string, streamcache.SubscriptionConfig, bool,
					...uint32) (*streamcache.StreamConsumer, common.Status) {
					return &streamcache.StreamConsumer{}, common.CreateStatus(common.Ok, "")
				}),
			gomonkey.ApplyFunc(localauth.Decrypt, func(_ string) ([]byte, error) {
				return []byte(""), nil
			}),
		})
		defer patches.ResetAll()
		c, err := createClient("tenantID")
		assert.Nil(t, err)
		assert.NotNil(t, c)
	})
	t.Run("failed to create stream client: iamToken is enabled, but aksk is not enabled", func(t *testing.T) {
		constant.FConfig.DataSystem.AkSkEnable = false
		constant.FConfig.DataSystem.IAMTokenEnable = true
		_, err := createClient("tenantID")
		assert.NotNil(t, err)
		assert.Equal(t, "iamToken is enabled, but aksk is not enabled, tenantID: tenantID", err.Error())
	})
}

func TestCreateProducerByDefault(t *testing.T) {
	c := DataSystemStreamClient{client: &streamcache.StreamClient{}}
	t.Run("failed to createProducer", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "CreateProducer",
			func(cli *streamcache.StreamClient, streamName string, delayFlushTimeMs int64, pageSize int64,
				maxStreamSize uint64, autoCleanup bool) (*streamcache.StreamProducer, common.Status) {
				return nil, common.CreateStatus(common.UnexpectedError, "error")
			})
		producer, err := c.CreateProducerByDefault("stream name")
		defer patch.Reset()
		assert.NotNil(t, err)
		assert.Equal(t, "Status code: 10 Error: error", err.Error())
		assert.Nil(t, producer)
	})
	t.Run("succeed to createProducer", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "CreateProducer",
			func(cli *streamcache.StreamClient, streamName string, delayFlushTimeMs int64, pageSize int64,
				maxStreamSize uint64, autoCleanup bool) (*streamcache.StreamProducer, common.Status) {
				return &streamcache.StreamProducer{}, common.CreateStatus(common.Ok, "")
			})
		producer, err := c.CreateProducerByDefault("stream name")
		defer patch.Reset()
		assert.Nil(t, err)
		assert.NotNil(t, producer)
	})
	t.Run("retry", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "CreateProducer",
			func(cli *streamcache.StreamClient, streamName string, delayFlushTimeMs int64, pageSize int64,
				maxStreamSize uint64, autoCleanup bool) (*streamcache.StreamProducer, common.Status) {
				executionTimes++
				return nil, common.CreateStatus(streamDeleteInProgress, "error")
			})
		producer, err := c.CreateProducerByDefault("stream name")
		defer patch.Reset()
		assert.NotNil(t, err)
		assert.Equal(t, "Status code: 3007 Error: error", err.Error())
		assert.Nil(t, producer)
		assert.Equal(t, retryCountLimit+1, executionTimes)
	})
}

func TestSendDataWithRetry(t *testing.T) {
	data := []byte{0}
	producer := &streamcache.StreamProducer{}
	t.Run("send without retry", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(producer), "Send",
			func(*streamcache.StreamProducer, streamcache.Element) common.Status {
				return common.CreateStatus(common.Ok, "")
			})
		defer patch.Reset()
		err := SendDataWithRetry(producer, &data[0], 1)
		assert.Nil(t, err)
	})
	t.Run("send only retry for oom", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(producer), "Send",
			func(*streamcache.StreamProducer, streamcache.Element) common.Status {
				executionTimes++
				return common.CreateStatus(outOfMEMORY, "OOM")
			})
		defer patch.Reset()
		err := SendDataWithRetry(producer, &data[0], 1)
		assert.NotNil(t, err)
		assert.Equal(t, "Status code: 6 Error: OOM", err.Error())
		assert.Equal(t, retryCountLimit+1, executionTimes)
	})
}

func TestRecWithRetry(t *testing.T) {
	c := &streamcache.StreamConsumer{}
	t.Run("rec success", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c), "ReceiveExpectNum",
			func(c *streamcache.StreamConsumer, expectNum uint32, timeoutMs uint32) ([]streamcache.Element, common.Status) {
				return []streamcache.Element{}, common.CreateStatus(common.Ok, "")
			})
		defer patch.Reset()
		elements, err := RecWithRetry(c)
		assert.Nil(t, err)
		assert.NotNil(t, elements)
	})
	t.Run("receive retry", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c), "ReceiveExpectNum",
			func(c *streamcache.StreamConsumer, expectNum uint32, timeoutMs uint32) ([]streamcache.Element, common.Status) {
				executionTimes++
				return nil, common.CreateStatus(outOfMEMORY, "OOM")
			})
		defer patch.Reset()
		elements, err := RecWithRetry(c)
		assert.NotNil(t, err)
		assert.Nil(t, elements)
		assert.Equal(t, "Status code: 6 Error: OOM", err.Error())
		assert.Equal(t, retryCountLimit+1, executionTimes)
	})
}

func TestQueryStreamConsumerExists(t *testing.T) {
	c := DataSystemStreamClient{client: &streamcache.StreamClient{}}
	t.Run("query timeout", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "QueryGlobalConsumersNum",
			func(cli *streamcache.StreamClient, streamName string) (uint64, common.Status) {
				return 0, common.CreateStatus(common.Ok, "")
			})
		err := c.QueryStreamConsumerExists("stream name", 1*time.Second)
		defer patch.Reset()
		assert.NotNil(t, err)
		assert.Equal(t, "query customer of stream timeout", err.Error())
	})
	t.Run("succeed to query stream consumer", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "QueryGlobalConsumersNum",
			func(cli *streamcache.StreamClient, streamName string) (uint64, common.Status) {
				return 1, common.CreateStatus(common.Ok, "")
			})
		err := c.QueryStreamConsumerExists("stream name", 1*time.Second)
		defer patch.Reset()
		assert.Nil(t, err)
	})
	t.Run("retry", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "QueryGlobalConsumersNum",
			func(cli *streamcache.StreamClient, streamName string) (uint64, common.Status) {
				executionTimes++
				return 0, common.CreateStatus(outOfMEMORY, "OOM")
			})
		defer patch.Reset()
		err := c.QueryStreamConsumerExists("stream name", 1*time.Second)
		assert.NotNil(t, err)
		assert.Equal(t, "Status code: 6 Error: OOM", err.Error())
		assert.Equal(t, retryCountLimit+1, executionTimes)
	})
}

func TestSubscribe(t *testing.T) {
	c := DataSystemStreamClient{client: &streamcache.StreamClient{}}
	t.Run("failed to Subscribe", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "Subscribe",
			func(cli *streamcache.StreamClient, streamName string, config streamcache.SubscriptionConfig, autoAck bool,
				cacheCapacity ...uint32) (*streamcache.StreamConsumer, common.Status) {
				return nil, common.CreateStatus(common.UnexpectedError, "error")
			})
		consumer, err := c.Subscribe("stream name")
		defer patch.Reset()
		assert.NotNil(t, err)
		assert.Equal(t, "Status code: 10 Error: error", err.Error())
		assert.Nil(t, consumer)
	})
	t.Run("succeed to Subscribe", func(t *testing.T) {
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "Subscribe",
			func(cli *streamcache.StreamClient, streamName string, config streamcache.SubscriptionConfig, autoAck bool,
				cacheCapacity ...uint32) (*streamcache.StreamConsumer, common.Status) {
				return &streamcache.StreamConsumer{}, common.CreateStatus(common.Ok, "")
			})
		consumer, err := c.Subscribe("stream name")
		defer patch.Reset()
		assert.Nil(t, err)
		assert.NotNil(t, consumer)
	})
	t.Run("retry", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(c.client), "Subscribe",
			func(cli *streamcache.StreamClient, streamName string, config streamcache.SubscriptionConfig, autoAck bool,
				cacheCapacity ...uint32) (*streamcache.StreamConsumer, common.Status) {
				executionTimes++
				return nil, common.CreateStatus(outOfMEMORY, "OOM")
			})
		defer patch.Reset()
		consumer, err := c.Subscribe("stream name")
		assert.NotNil(t, err)
		assert.Equal(t, "Status code: 6 Error: OOM", err.Error())
		assert.Equal(t, retryCountLimit+1, executionTimes)
		assert.Nil(t, consumer)
	})
}

func TestCloseConsumer(t *testing.T) {
	consumer := &streamcache.StreamConsumer{}
	t.Run("succeed to close consumer", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(consumer), "Close",
			func(c *streamcache.StreamConsumer) common.Status {
				executionTimes++
				return common.CreateStatus(common.Ok, "")
			})
		defer patch.Reset()
		CloseConsumer(consumer)
		assert.Equal(t, 1, executionTimes)
	})
	t.Run("retry", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(consumer), "Close",
			func(c *streamcache.StreamConsumer) common.Status {
				executionTimes++
				return common.CreateStatus(streamDeleteInProgress, "error")
			})
		defer patch.Reset()
		CloseConsumer(consumer)
		assert.Equal(t, retryCountLimit+1, executionTimes)
	})
}

func TestCloseProducer(t *testing.T) {
	producer := &streamcache.StreamProducer{}
	t.Run("succeed to close producer", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(producer), "Close",
			func(c *streamcache.StreamProducer) common.Status {
				executionTimes++
				return common.CreateStatus(common.Ok, "")
			})
		defer patch.Reset()
		CloseProducer(producer)
		assert.Equal(t, 1, executionTimes)
	})
	t.Run("retry", func(t *testing.T) {
		executionTimes := 0
		patch := gomonkey.ApplyMethod(reflect.TypeOf(producer), "Close",
			func(c *streamcache.StreamProducer) common.Status {
				executionTimes++
				return common.CreateStatus(streamDeleteInProgress, "error")
			})
		defer patch.Reset()
		CloseProducer(producer)
		assert.Equal(t, retryCountLimit+1, executionTimes)
	})
}

func TestGetZeroElementSleepTime(t *testing.T) {
	os.Setenv("STREAM_ZERO_SLEEP_TIME", "")
	if result := initZeroSleepTime(); result != defaultZeroSleepTime {
		t.Errorf("Expected %d, but got %d", defaultZeroSleepTime, result)
	}

	os.Setenv("STREAM_ZERO_SLEEP_TIME", "abc")
	if result := initZeroSleepTime(); result != defaultZeroSleepTime {
		t.Errorf("Expected %d, but got %d", defaultZeroSleepTime, result)
	}

	os.Setenv("STREAM_ZERO_SLEEP_TIME", "-1")
	if result := initZeroSleepTime(); result != defaultZeroSleepTime {
		t.Errorf("Expected %d, but got %d", defaultZeroSleepTime, result)
	}

	os.Setenv("STREAM_ZERO_SLEEP_TIME", "20")
	if result := initZeroSleepTime(); result != 20*time.Millisecond {
		t.Errorf("Expected %d, but got %d", 20*time.Millisecond, result)
	}
}

func Test_GetClient(t *testing.T) {
	convey.Convey("test GetClient", t, func() {
		convey.Convey("case1: success, iamToken is not enable", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(createClient, func() (*DataSystemStreamClient, error) {
					return &DataSystemStreamClient{}, nil
				}),
			})
			defer patches.ResetAll()
			clientMap.Delete("")
			constant.FConfig.DataSystem.IAMTokenEnable = false
			client := GetClient("tenantID")
			convey.So(client, convey.ShouldNotBeNil)
			_, exist := clientMap.Load("")
			convey.So(exist, convey.ShouldBeTrue)
		})
		convey.Convey("case2: success, iamToken is enable", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(createClient, func() (*DataSystemStreamClient, error) {
					return &DataSystemStreamClient{}, nil
				}),
			})
			defer patches.ResetAll()
			clientMap.Delete("tenantID")
			constant.FConfig.DataSystem.IAMTokenEnable = true
			client := GetClient("tenantID")
			convey.So(client, convey.ShouldNotBeNil)
			_, exist := clientMap.Load("tenantID")
			convey.So(exist, convey.ShouldBeTrue)
		})
		convey.Convey("case3: failed to init data system client", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(createClient, func() (*DataSystemStreamClient, error) {
					return nil, errors.New("error")
				}),
			})
			defer patches.ResetAll()
			clientMap.Delete("")
			constant.FConfig.DataSystem.IAMTokenEnable = false
			client := GetClient("tenantID")
			convey.So(client, convey.ShouldBeNil)
			_, exist := clientMap.Load("")
			convey.So(exist, convey.ShouldBeFalse)
		})
		convey.Convey("case4: success, client exists", func() {
			clientMap.Store("", &DataSystemStreamClient{})
			constant.FConfig.DataSystem.IAMTokenEnable = false
			client := GetClient("tenantID")
			convey.So(client, convey.ShouldNotBeNil)
			_, exist := clientMap.Load("")
			convey.So(exist, convey.ShouldBeTrue)
		})
		convey.Convey("case5: failed to convert clientIf to *DataSystemStreamClient", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(createClient, func() (*DataSystemStreamClient, error) {
					return &DataSystemStreamClient{}, nil
				}),
			})
			defer patches.ResetAll()
			clientMap.Store("", "xxx")
			constant.FConfig.DataSystem.IAMTokenEnable = false
			client := GetClient("tenantID")
			convey.So(client, convey.ShouldNotBeNil)
			_, exist := clientMap.Load("")
			convey.So(exist, convey.ShouldBeTrue)
		})
	})
}

func Test_CleanExpiredClient(t *testing.T) {
	convey.Convey("test CleanExpiredClient", t, func() {
		convey.Convey("case1: stopCh is nil", func() {
			execute := 0
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
					execute = 1
					return nil
				}),
			})
			defer patches.ResetAll()
			CleanExpiredClient(nil)
			convey.So(execute, convey.ShouldEqual, 0)
		})
		convey.Convey("case2: delete timeout client", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyFunc(getCleanInterval, func() time.Duration {
					return time.Second
				}),
				gomonkey.ApplyFunc(getClientExpiration, func() time.Duration {
					return time.Second
				}),
			})
			defer patches.ResetAll()
			clientCloseCh := make(chan struct{})
			clientMap.Store("tenantID", &DataSystemStreamClient{
				lastUsed: time.Now(),
				closeCh:  clientCloseCh,
			})
			stopCh := make(chan struct{})
			defer close(stopCh)
			go CleanExpiredClient(stopCh)
			time.Sleep(3 * time.Second)
			_, exist := clientMap.Load("tenantID")
			convey.So(exist, convey.ShouldBeFalse)
		})
	})
}

func Test_getCleanInterval(t *testing.T) {
	convey.Convey("test getCleanInterval", t, func() {
		convey.Convey("case1: success", func() {
			constant.FConfig.DataSystem.CleanStreamClientMapInterval = 1
			interval := getCleanInterval()
			convey.So(interval, convey.ShouldEqual, time.Second)
		})
		convey.Convey("case2: interval is invalid", func() {
			constant.FConfig.DataSystem.CleanStreamClientMapInterval = -1
			interval := getCleanInterval()
			convey.So(interval, convey.ShouldEqual, defaultCleanInterval)
		})
	})
}

func Test_getClientExpiration(t *testing.T) {
	convey.Convey("test getClientExpiration", t, func() {
		convey.Convey("case1: success", func() {
			constant.FConfig.DataSystem.StreamClientMapExpiration = 1
			interval := getClientExpiration()
			convey.So(interval, convey.ShouldEqual, time.Second)
		})
		convey.Convey("case2: expiration is invalid", func() {
			constant.FConfig.DataSystem.StreamClientMapExpiration = -1
			interval := getClientExpiration()
			convey.So(interval, convey.ShouldEqual, defaultClientExpiration)
		})
	})
}
