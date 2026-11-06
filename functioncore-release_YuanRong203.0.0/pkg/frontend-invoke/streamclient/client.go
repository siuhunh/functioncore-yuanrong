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

// Package streamclient process data system stream
package streamclient

import (
	"clients/common"
	"clients/streamcache"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/frontend-invoke/constant"
	"functioncore/pkg/frontend-invoke/util"
)

var (
	zeroSleepTime = initZeroSleepTime()
	clientMap     sync.Map
)

const (
	// PageSize --
	PageSize int64 = 1 * 1024 * 1024

	defaultZeroSleepTime        = 10 * time.Millisecond
	maxStreamSize        uint64 = 100 * 1024 * 1024
	delayFlushTime       int64  = 5

	retryCountLimit        = 3
	retrySleepTime         = 1 * time.Second
	recTimeoutMs    uint32 = 10
	// control the maximum number of data that can be read at a time to avoid memory impact.
	recExpectNum uint32 = 10

	// data system error code
	outOfMEMORY            = 6
	tryAgain               = 19
	rpcUnavailable         = 1002
	streamDeleteInProgress = 3007

	defaultCleanInterval    = time.Duration(60*60) * time.Second
	defaultClientExpiration = time.Duration(24*60*60) * time.Second
)

var needRetryCode = []int{outOfMEMORY, tryAgain, rpcUnavailable, streamDeleteInProgress}

// DataSystemStreamClient -
type DataSystemStreamClient struct {
	client   *streamcache.StreamClient
	lastUsed time.Time
	closeCh  chan struct{}
}

func initZeroSleepTime() time.Duration {
	env := os.Getenv("STREAM_ZERO_SLEEP_TIME")
	if env == "" {
		return defaultZeroSleepTime
	}
	val, err := strconv.Atoi(env)
	if err != nil || val <= 0 {
		return defaultZeroSleepTime
	}
	return time.Duration(val) * time.Millisecond
}

// GetZeroSleepTime ~
func GetZeroSleepTime() time.Duration {
	return zeroSleepTime
}

// GetClient -
func GetClient(tenantID string) *DataSystemStreamClient {
	clientTenantID := util.GetStreamClientTenantID(tenantID)
	clientIf, exist := clientMap.Load(clientTenantID)
	if exist {
		if client, ok := clientIf.(*DataSystemStreamClient); ok {
			client.lastUsed = time.Now()
			return client
		}
		log.GetLogger().Warnf("failed to convert clientIf to *DataSystemStreamClient, type: %T, tenantID: %s",
			clientIf, tenantID)
	}
	client, err := createClient(tenantID)
	if err != nil {
		log.GetLogger().Errorf("failed to init data system client with error %s, tenantID: %s", err.Error(), tenantID)
		return nil
	}
	clientMap.Store(clientTenantID, client)
	return client
}

func createClient(tenantID string) (*DataSystemStreamClient, error) {
	arguments, err := initArgument()
	if err != nil {
		return nil, err
	}
	var ak, sk []byte
	defer func() {
		if len(ak) != 0 {
			utils.ClearByteMemory(ak)
		}
		if len(sk) != 0 {
			utils.ClearByteMemory(sk)
		}
	}()
	if constant.FConfig.DataSystem.IAMTokenEnable {
		if !constant.FConfig.DataSystem.AkSkEnable {
			errMsg := fmt.Sprintf("iamToken is enabled, but aksk is not enabled, tenantID: %s", tenantID)
			log.GetLogger().Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		arguments.TenantID = tenantID
	}
	if constant.FConfig.DataSystem.AkSkEnable {
		if constant.FConfig.LocalAuth == nil {
			errMsg := "localAuth is nil"
			log.GetLogger().Errorf(errMsg)
			return nil, errors.New(errMsg)
		}
		ak, err = localauth.Decrypt(constant.FConfig.LocalAuth.AKey)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt ak, err: %s", err.Error())
			return nil, err
		}
		sk, err = localauth.Decrypt(constant.FConfig.LocalAuth.SKey)
		if err != nil {
			log.GetLogger().Errorf("failed to decrypt sk, err: %s", err.Error())
			return nil, err
		}
		arguments.AccessKey = string(ak)
		arguments.SecretKey = sk
	}
	log.GetLogger().Infof("dataSystem worker address %s on port %d, iamTokenEnable: %t, akSkEnable: %t, tenantID: %s",
		arguments.Host, arguments.Port, constant.FConfig.DataSystem.IAMTokenEnable,
		constant.FConfig.DataSystem.AkSkEnable, tenantID)
	streamClient := streamcache.CreateClient(arguments)
	if status := streamClient.Init(false); status.IsError() {
		streamClient.DestroyClient()
		return nil, errors.New(status.ToString())
	}
	return constructStreamClient(&streamClient), nil
}

func initArgument() (common.ConnectArguments, error) {
	dataSystemPort, err := strconv.Atoi(constants.DefaultDataSystemPort)
	if err != nil {
		log.GetLogger().Errorf("failed to strconv.Atoi DataSystemPort, err %s", err.Error())
		return common.ConnectArguments{}, err
	}
	arguments := common.ConnectArguments{
		Host: os.Getenv("NODE_IP"),
		Port: dataSystemPort,
	}
	return arguments, nil
}

func constructStreamClient(streamClient *streamcache.StreamClient) *DataSystemStreamClient {
	return &DataSystemStreamClient{
		client:   streamClient,
		lastUsed: time.Now(),
		closeCh:  make(chan struct{}),
	}
}

// CreateProducerByDefault -
func (c *DataSystemStreamClient) CreateProducerByDefault(streamName string) (*streamcache.StreamProducer, error) {
	var err error
	retryCount := 0
	for {
		producer, s := c.client.CreateProducer(streamName, delayFlushTime, PageSize, maxStreamSize, true)
		if !s.IsError() {
			return producer, nil
		}
		err = errors.New(s.ToString())
		if checkCodeIsNeedRetry(s.Code) && retryCount < retryCountLimit {
			retryCount++
			retrySleep("CreateProducerByDefault", retryCount, err)
			continue
		}
		break
	}
	return nil, err
}

// QueryStreamConsumerExists -
func (c *DataSystemStreamClient) QueryStreamConsumerExists(streamName string, timeout time.Duration) error {
	queryTimeoutTimer := time.NewTimer(timeout)
	for {
		select {
		case <-queryTimeoutTimer.C:
			log.GetLogger().Errorf("query customer of stream %s timeout %d(s)", streamName,
				int(timeout.Seconds()))
			return errors.New("query customer of stream timeout")
		default:
			num, err := queryGlobalConsumersNum(c.client, streamName)
			if err != nil {
				log.GetLogger().Errorf("failed to query num of customer of stream %s, err %s", streamName,
					err.Error())
				return err
			}
			if num > 0 {
				return nil
			}
			time.Sleep(zeroSleepTime)
		}
	}
}

func queryGlobalConsumersNum(client *streamcache.StreamClient, streamName string) (uint64, error) {
	if client == nil {
		return 0, errors.New("stream client is nil")
	}
	var err error
	retryCount := 0
	for {
		num, s := client.QueryGlobalConsumersNum(streamName)
		if !s.IsError() {
			return num, nil
		}
		err = errors.New(s.ToString())
		if checkCodeIsNeedRetry(s.Code) && retryCount < retryCountLimit {
			retryCount++
			retrySleep("queryGlobalConsumersNum", retryCount, err)
			continue
		}
		break
	}
	return 0, err
}

// Subscribe -
func (c *DataSystemStreamClient) Subscribe(streamName string) (*streamcache.StreamConsumer, error) {
	var err error
	retryCount := 0
	for {
		subConfig := streamcache.SubscriptionConfig{SubName: utils.UniqueID(), SubType: streamcache.Stream}
		consumer, s := c.client.Subscribe(streamName, subConfig, true)
		if !s.IsError() {
			return consumer, nil
		}
		err = errors.New(s.ToString())
		if checkCodeIsNeedRetry(s.Code) && retryCount < retryCountLimit {
			retryCount++
			retrySleep("subscribe", retryCount, err)
			continue
		}
		break
	}
	return nil, err
}

// SendDataWithRetry -
func SendDataWithRetry(producer *streamcache.StreamProducer, ptr *uint8, size uint64) error {
	element := streamcache.Element{Ptr: ptr, Size: size}
	var err error
	retryCount := 0
	for {
		s := producer.Send(element)
		if !s.IsError() {
			return nil
		}
		err = errors.New(s.ToString())
		if checkCodeIsNeedRetry(s.Code) && retryCount < retryCountLimit {
			retryCount++
			retrySleep("producer.Send", retryCount, err)
			continue
		}
		break
	}
	return err
}

// RecWithRetry -
func RecWithRetry(consumer *streamcache.StreamConsumer) ([]streamcache.Element, error) {
	var err error
	retryCount := 0
	for {
		elements, s := consumer.ReceiveExpectNum(recExpectNum, recTimeoutMs)
		if !s.IsError() {
			return elements, nil
		}
		err = errors.New(s.ToString())
		if checkCodeIsNeedRetry(s.Code) && retryCount < retryCountLimit {
			retryCount++
			retrySleep("consumer.Receive", retryCount, err)
			continue
		}
		break
	}
	return nil, err
}

// CloseConsumer -
func CloseConsumer(consumer *streamcache.StreamConsumer) {
	if consumer == nil {
		log.GetLogger().Warn("consumer is nil")
		return
	}
	retryCount := 0
	for {
		s := consumer.Close()
		if !s.IsError() {
			return
		}
		err := errors.New(s.ToString())
		if checkCodeIsNeedRetry(s.Code) && retryCount < retryCountLimit {
			retryCount++
			retrySleep("consumer.Close", retryCount, err)
			continue
		}
		break
	}
}

// CloseProducer -
func CloseProducer(producer *streamcache.StreamProducer) {
	if producer == nil {
		log.GetLogger().Warn("producer is nil")
		return
	}
	retryCount := 0
	for {
		s := producer.Close()
		if !s.IsError() {
			return
		}
		err := errors.New(s.ToString())
		if checkCodeIsNeedRetry(s.Code) && retryCount < retryCountLimit {
			retryCount++
			retrySleep("producer.Close", retryCount, err)
			continue
		}
		break
	}
}

func retrySleep(action string, retryCount int, err error) {
	sleepTime := time.Duration(retryCount) * retrySleepTime
	time.Sleep(sleepTime)
	log.GetLogger().Warnf("the %s %d retry after %v second with err %s", action, retryCount,
		sleepTime, err.Error())
}

func checkCodeIsNeedRetry(code int) bool {
	for _, need := range needRetryCode {
		if code == need {
			return true
		}
	}
	return false
}

// CleanExpiredClient regularly clean up clients that have not been used for a long time
func CleanExpiredClient(stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	ticker := time.NewTicker(getCleanInterval())
	clientExpiration := getClientExpiration()
	for {
		select {
		case <-ticker.C:
			deleteExpiredClient(clientExpiration)
		case _, ok := <-stopCh:
			if !ok {
				ticker.Stop()
				return
			}
		}
	}
}

func getCleanInterval() time.Duration {
	if constant.FConfig.DataSystem.CleanStreamClientMapInterval <= 0 {
		log.GetLogger().Warnf("cleanStreamClientMapInterval %d is invalid",
			constant.FConfig.DataSystem.CleanStreamClientMapInterval)
		return defaultCleanInterval
	}
	log.GetLogger().Infof("cleanStreamClientMapInterval is %d", constant.FConfig.DataSystem.CleanStreamClientMapInterval)
	return time.Duration(constant.FConfig.DataSystem.CleanStreamClientMapInterval) * time.Second
}

func deleteExpiredClient(clientExpiration time.Duration) {
	var clientsToDelete []string
	clientMap.Range(func(key, value interface{}) bool {
		tenantID, ok := key.(string)
		if !ok {
			log.GetLogger().Warnf("failed to convert tenantID to string, type: %T", key)
			return true
		}
		client, ok := value.(*DataSystemStreamClient)
		if !ok {
			log.GetLogger().Warnf("failed to convert client to *DataSystemStreamClient, type: %T, tenantID: %s",
				value, tenantID)
			return true
		}
		if tenantID == "" {
			return true
		}
		if time.Since(client.lastUsed) > clientExpiration {
			log.GetLogger().Infof("client %s is unused for %v, delete it from map",
				tenantID, clientExpiration)
			close(client.closeCh)
			clientsToDelete = append(clientsToDelete, tenantID)
		}
		return true
	})

	for _, tenantID := range clientsToDelete {
		clientMap.Delete(tenantID)
	}
}

func getClientExpiration() time.Duration {
	if constant.FConfig.DataSystem.StreamClientMapExpiration <= 0 {
		log.GetLogger().Warnf("streamClientMapExpiration %d is invalid",
			constant.FConfig.DataSystem.StreamClientMapExpiration)
		return defaultClientExpiration
	}
	log.GetLogger().Infof("streamClientMapExpiration is %d", constant.FConfig.DataSystem.StreamClientMapExpiration)
	return time.Duration(constant.FConfig.DataSystem.StreamClientMapExpiration) * time.Second
}

// GetCloseCh get closeCh of stream client
func (c *DataSystemStreamClient) GetCloseCh() chan struct{} {
	return c.closeCh
}
