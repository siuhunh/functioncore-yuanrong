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

// Package redisclient new a redis client
package redisclient

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	commonTLS "functioncore/pkg/common/tls"
	"functioncore/pkg/common/utils"
)

const (
	// timeout : allow TCP reconnection for 3 times(1, 2, 4)
	dialTimeout         = 8 * time.Second
	readTimeout         = 8 * time.Second
	writeTimeout        = 8 * time.Second
	idleTimeout         = 300 * time.Second
	defaultDialTimeout  = 8
	defaultReadTimeout  = 8
	defaultWriteTimeout = 8
	defaultIdleTimeout  = 300
	defaultRedisConn    = 20
	// TTL -
	TTL           = 1 * time.Minute
	maxRetryTimes = 3
	// DefaultCAFile is the default ca file for tls client
	DefaultCAFile = "/home/sn/resource/redis-secret/ca.pem"
	// DefaultCertFile is the default cert file for tls client
	DefaultCertFile = "/home/sn/resource/redis-secret/cert.pem"
	// DefaultKeyFile is the default key file for tls client
	DefaultKeyFile = "/home/sn/resource/redis-secret/key.pem"
	// redisStringFile is the temp file to store string type data of redis
	redisStringFile = "/tmp/redis-string"
	// redisStringFile is the temp file to store slice type data of redis
	redisSliceFile = "/tmp/redis-slice"
	redisSeparator = "%WITH%"

	// the detection is performed every 5 seconds.
	healthCheckIntervalTime = 5
	// 2 * 60min * 60s / 5 second, trigger every 5 minutes
	twoHoursCount             = 2 * 60 * 60 / healthCheckIntervalTime
	success                   = 0
	fail                      = 1
	redisValueIndex           = 2
	redisReconnectionInternal = 10 * time.Second
	// DefaultRedisContextTimeout -
	DefaultRedisContextTimeout = time.Second

	redisRetryTimes    = 3
	redisRetryInterval = 100 * time.Millisecond
)

var (
	errMode = errors.New("serverMode is not single or cluster")
	// RedisClient -
	RedisClient = &Client{&redis.Client{}, redisClientOption{}, false, sync.RWMutex{}}
	// defaultTimeoutConf is the default timeout conf
	defaultTimeoutConf = TimeoutConf{
		DialTimeout:  defaultDialTimeout,
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
		IdleTimeout:  defaultIdleTimeout,
	}
)

var (
	// lock protection for restoring from persistent cache to redis
	cacheLock sync.Mutex
)

// Option -
type Option func(*redisClientOption)

type redisClientOption struct {
	tlsConfig       *tls.Config
	serverAddr      string
	dialTimeout     time.Duration
	readTimeout     time.Duration
	writeTimeout    time.Duration
	idleTimeout     time.Duration
	password        string
	serverMode      string
	enableTLS       bool
	hotloadConfFunc func() (string, TimeoutConf, error)
	enableAlarm     bool
}

// RedisCache -
type RedisCache struct {
	Key    string
	Value  string
	Method string
}

// RedisOperation -
type RedisOperation struct {
	Key    string
	Value  string
	Method string
	TTL    time.Duration
}

// Client -
type Client struct {
	client    redis.Cmdable
	option    redisClientOption
	connected bool
	sync.RWMutex
}

var (
	mu       sync.RWMutex
	redisCmd *Client
)

// TimeoutConf A variety of timeout configurations
type TimeoutConf struct {
	DialTimeout  int `json:"dialTimeout,omitempty" valid:",optional"`
	ReadTimeout  int `json:"readTimeout,omitempty" valid:",optional"`
	WriteTimeout int `json:"writeTimeout,omitempty" valid:",optional"`
	IdleTimeout  int `json:"idleTimeout,omitempty" valid:",optional"`
}

// NewRedisClientParam parameters of a new redis client
type NewRedisClientParam struct {
	ServerMode      string
	ServerAddr      string
	Password        string
	Timeout         TimeoutConf
	EnableTLS       bool `json:"enableTLS,omitempty" valid:",optional"`
	HotloadConfFunc func() (string, TimeoutConf, error)
}

// GetRedisCmd -
func GetRedisCmd() *Client {
	mu.Lock()
	client := redisCmd
	mu.Unlock()
	return client
}

// SetRedisCmd -
func SetRedisCmd(client *Client) {
	mu.Lock()
	redisCmd = client
	mu.Unlock()
}

// SetEnableTLS -
func SetEnableTLS(enableTLS bool) Option {
	return func(c *redisClientOption) {
		c.enableTLS = enableTLS
	}
}

// SetGetRealTimeServerAddrFunc hot update server address when disconnected
func SetGetRealTimeServerAddrFunc(getServerAddr func() (string, TimeoutConf, error)) Option {
	return func(c *redisClientOption) {
		c.hotloadConfFunc = getServerAddr
	}
}

// AlarmSwitchOn enable alarm
func AlarmSwitchOn() Option {
	return func(c *redisClientOption) {
		c.enableAlarm = true
	}
}

// AlarmSwitchOff disable alarm
func AlarmSwitchOff() Option {
	return func(c *redisClientOption) {
		c.enableAlarm = false
	}
}

// New create a redis client
func New(newClientParam NewRedisClientParam, stopCh <-chan struct{}, options ...Option) (*Client, error) {
	o := getNewRedisOption(newClientParam)
	for _, option := range options {
		option(&o)
	}

	var redisCMD redis.Cmdable
	switch newClientParam.ServerMode {
	case "single":
		redisCMD = newSingleClient(o)
	case "cluster":
		redisCMD = newClusterClient(o)
	default:
		utils.ClearStringMemory(o.password)
		return nil, errMode
	}

	if redisCMD == nil {
		return nil, errors.New("failed to new redis cmd")
	}
	finished := make(chan int)
	go connectRedis(redisCMD, finished, o)
	select {
	case i, ok := <-finished:
		if ok && i == fail {
			return nil, errors.New("failed to connect redis server")
		}
	case <-time.After(o.dialTimeout):
		log.GetLogger().Errorf("dialing redis server error with incorrect ip address:%s.", o.serverAddr)
		return nil, errors.New("dialing redis server timeout")
	}
	RedisClient = &Client{redisCMD, o, true, sync.RWMutex{}}
	if o.enableAlarm {
		alarm.GetClient().RegisterAlarmType(alarm.RedisConnection)
		alarm.GetClient().RegisterAlarmType(alarm.CriticalRedisConnection)
		go RedisClient.healthCheckAndReport(stopCh, twoHoursCount)
	}
	return RedisClient, nil
}

func initClient(clientRedisConfig *NewRedisClientParam, stopCh <-chan struct{}) (*Client, error) {
	c, err := New(NewRedisClientParam{
		ServerMode: clientRedisConfig.ServerMode,
		ServerAddr: clientRedisConfig.ServerAddr,
		Password:   clientRedisConfig.Password,
		Timeout:    clientRedisConfig.Timeout,
	}, stopCh, SetEnableTLS(clientRedisConfig.EnableTLS), AlarmSwitchOn(),
		SetGetRealTimeServerAddrFunc(clientRedisConfig.HotloadConfFunc))
	if err != nil {
		log.GetLogger().Errorf("failed to new a redis Client, %s", err.Error())
		return nil, err
	}
	return c, nil
}

// CheckRedisConnectivity -
func CheckRedisConnectivity(clientRedisConfig *NewRedisClientParam, client *Client, stopCh <-chan struct{}) {
	if stopCh == nil {
		log.GetLogger().Errorf("stopCh is nil")
		return
	}
	ticker := time.NewTicker(redisReconnectionInternal)
	for {
		select {
		case <-ticker.C:
			log.GetLogger().Debug("redis check redis connection start")
			if client != nil {
				_, err := client.Ping(context.TODO()).Result()
				if err == nil {
					log.GetLogger().Debug("redis periodically checks availability")
					continue
				}
			}
			newClient, err := initClient(clientRedisConfig, stopCh)
			if err != nil {
				log.GetLogger().Errorf("failed to init redis client, err:%s", err.Error())
				continue
			}
			if client != nil {
				client = newClient
			}
			SetRedisCmd(newClient)
			log.GetLogger().Infof("redis connect succeed, clear the redis alarm")
			alarm.GetClient().ClearAlarm(alarm.RedisConnection)
		case <-stopCh:
			log.GetLogger().Errorf("module process exit")
			ticker.Stop()
			return
		}
	}
}

func getNewRedisOption(param NewRedisClientParam) redisClientOption {
	o := redisClientOption{
		serverAddr: param.ServerAddr,
		password:   param.Password,
		serverMode: param.ServerMode,
	}
	if param.Timeout.DialTimeout > 0 {
		o.dialTimeout = time.Duration(param.Timeout.DialTimeout) * time.Second
		log.GetLogger().Infof("new dialTimeout: %d", param.Timeout.DialTimeout)
	} else {
		o.dialTimeout = dialTimeout
	}
	if param.Timeout.ReadTimeout > 0 {
		o.readTimeout = time.Duration(param.Timeout.ReadTimeout) * time.Second
		log.GetLogger().Infof("new readTimeout: %d", param.Timeout.ReadTimeout)
	} else {
		o.readTimeout = readTimeout
	}
	if param.Timeout.WriteTimeout > 0 {
		o.writeTimeout = time.Duration(param.Timeout.WriteTimeout) * time.Second
		log.GetLogger().Infof("new writeTimeout: %d", param.Timeout.WriteTimeout)
	} else {
		o.writeTimeout = writeTimeout
	}
	if param.Timeout.IdleTimeout > 0 {
		o.idleTimeout = time.Duration(param.Timeout.IdleTimeout) * time.Second
		log.GetLogger().Infof("new idleTimeout: %d", param.Timeout.IdleTimeout)
	} else {
		o.idleTimeout = idleTimeout
	}
	return o
}

func connectRedis(redisCmd redis.Cmdable, finished chan int, o redisClientOption) {
	if finished == nil {
		return
	}
	var err error
	for i := 0; i < maxRetryTimes; i++ {
		if redisCmd == nil {
			log.GetLogger().Errorf("redis is not ready")
			continue
		}
		_, err = redisCmd.Ping(context.Background()).Result()
		if err == nil {
			finished <- success
			return
		}
	}
	// The key relies on go's GC for memory cleanup
	log.GetLogger().Errorf("dialing redis server error: %s", err.Error())
	finished <- fail
	return
}

// ProcessReportAlarmInfo provides interface for component to report alarm info
func (c *Client) healthCheckAndReport(stopCh <-chan struct{}, criticalFailedTimes int) {
	if stopCh == nil {
		return
	}
	failedCount := 0
	for {
		select {
		case <-stopCh:
			log.GetLogger().Info("graceful exit")
			return
		default:
			time.Sleep(healthCheckIntervalTime * time.Second)
			err := c.checkNormalConnection()
			if err == nil {
				failedCount = 0
				alarm.GetClient().ClearAlarm(alarm.RedisConnection)
				alarm.GetClient().ClearAlarm(alarm.CriticalRedisConnection)
				continue
			}

			failedCount++
			if failedCount >= criticalFailedTimes {
				alarm.GetClient().ReportAlarm(alarm.CriticalRedisConnection,
					fmt.Sprintf("error msg=%s,", err.Error()))
				log.GetLogger().Warnf("report redis critical abnormalAlarm")
				failedCount = 0
			}

			// Reporting Abnormal Alarms with Suppression.
			alarm.GetClient().ReportAlarm(alarm.RedisConnection,
				fmt.Sprintf("error msg=%s,", err.Error()))
		}
	}
}

// CheckNormalConnection to check whether the Redis connection is normal
func (c *Client) checkNormalConnection() error {
	_, err := c.Ping(context.TODO()).Result()
	if err != nil {
		log.GetLogger().Errorf("dialing redis server error: %s", err.Error())
		if c.option.hotloadConfFunc == nil {
			return err
		}
		// attempt to get new server address from config
		if addr, timeoutConf, hotloadConfErr := c.option.hotloadConfFunc(); hotloadConfErr != nil {
			log.GetLogger().Errorf("failed to get real-time server conf: %s", hotloadConfErr.Error())
		} else {
			changed := c.checkTimeout(timeoutConf)
			if c.option.serverAddr == addr && !changed {
				log.GetLogger().Warnf("address is not changed")
			} else {
				log.GetLogger().Infof("update server address: %s", addr)
				c.option.serverAddr = addr
			}
		}
		var stdError error
		c.Lock()
		c.client, stdError = newClient(c.option)
		c.Unlock()
		if stdError != nil {
			log.GetLogger().Errorf("failed to renew client: %s", stdError.Error())
			return stdError
		}
		if _, err := c.Ping(context.TODO()).Result(); err == nil {
			if stdError = TakeCacheToRedis(c); stdError != nil {
				log.GetLogger().Errorf("failed to rewrite to redis: %s", stdError.Error())
			}
			return nil
		}
		return err
	}
	return nil
}

func (c *Client) checkTimeout(timeoutConf TimeoutConf) bool {
	changed := false
	if timeoutConf.DialTimeout > 0 {
		if c.option.dialTimeout != time.Duration(timeoutConf.DialTimeout)*time.Second {
			c.option.dialTimeout = time.Duration(timeoutConf.DialTimeout) * time.Second
			log.GetLogger().Infof("update dialTimeout: %d", timeoutConf.DialTimeout)
			changed = true
		}
	} else {
		c.option.dialTimeout = dialTimeout
	}
	if timeoutConf.ReadTimeout > 0 {
		if c.option.readTimeout != time.Duration(timeoutConf.ReadTimeout)*time.Second {
			c.option.readTimeout = time.Duration(timeoutConf.ReadTimeout) * time.Second
			log.GetLogger().Infof("update readTimeout: %d", timeoutConf.ReadTimeout)
			changed = true
		}
	} else {
		c.option.readTimeout = readTimeout
	}
	if timeoutConf.WriteTimeout > 0 {
		if c.option.writeTimeout != time.Duration(timeoutConf.WriteTimeout)*time.Second {
			c.option.writeTimeout = time.Duration(timeoutConf.WriteTimeout) * time.Second
			log.GetLogger().Infof("update writeTimeout: %d", timeoutConf.WriteTimeout)
			changed = true
		}
	} else {
		c.option.writeTimeout = writeTimeout
	}
	if timeoutConf.IdleTimeout > 0 {
		if c.option.idleTimeout != time.Duration(timeoutConf.IdleTimeout)*time.Second {
			c.option.idleTimeout = time.Duration(timeoutConf.IdleTimeout) * time.Second
			log.GetLogger().Infof("update idleTimeout: %d", timeoutConf.IdleTimeout)
			changed = true
		}
	} else {
		c.option.idleTimeout = idleTimeout
	}
	return changed
}

// GetRedisAddress return the redis client address
func GetRedisAddress(c *Client) string {
	if c != nil {
		return c.option.serverAddr
	}
	return ""
}

func newClient(o redisClientOption) (redis.Cmdable, error) {
	var redisCMD redis.Cmdable
	switch o.serverMode {
	case "single":
		redisCMD = newSingleClient(o)
	case "cluster":
		redisCMD = newClusterClient(o)
	default:
		return redisCMD, errMode
	}
	return redisCMD, nil
}

func newClusterClient(o redisClientOption) redis.Cmdable {
	options := &redis.ClusterOptions{
		PoolSize:     defaultRedisConn,
		Addrs:        strings.Split(o.serverAddr, ","),
		Password:     o.password,
		DialTimeout:  o.dialTimeout,
		ReadTimeout:  o.readTimeout,
		WriteTimeout: o.writeTimeout,
		IdleTimeout:  o.idleTimeout,
		MaxRetries:   maxRetryTimes,
	}
	if o.enableTLS {
		tlsConfig, err := buildCfg(DefaultCAFile, DefaultCertFile, DefaultKeyFile)
		if err != nil {
			utils.ClearStringMemory(options.Password)
			log.GetLogger().Errorf("failed to build redis ClusterClient tls config: %s", err.Error())
			return nil
		}
		options.TLSConfig = tlsConfig
	}
	return redis.NewClusterClient(options)
}

func newSingleClient(o redisClientOption) redis.Cmdable {
	options := &redis.Options{
		PoolSize:     defaultRedisConn,
		Addr:         o.serverAddr,
		Password:     o.password,
		DialTimeout:  o.dialTimeout,
		ReadTimeout:  o.readTimeout,
		WriteTimeout: o.writeTimeout,
		IdleTimeout:  o.idleTimeout,
		MaxRetries:   maxRetryTimes,
	}
	if o.enableTLS {
		tlsConfig, err := buildCfg(DefaultCAFile, DefaultCertFile, DefaultKeyFile)
		if err != nil {
			utils.ClearStringMemory(options.Password)
			log.GetLogger().Errorf("failed to build single client tls config: %s", err.Error())
			return nil
		}
		options.TLSConfig = tlsConfig
	}
	return redis.NewClient(options)
}

func buildCfg(caFile string, certFile string, keyFile string) (*tls.Config, error) {
	var pools *x509.CertPool
	var err error
	pools, err = commonTLS.GetX509CACertPool(caFile)
	if err != nil {
		log.GetLogger().Errorf("failed to get X509 CACert Pool: %s", err.Error())
		return nil, err
	}

	var certs []tls.Certificate
	if certs, err = commonTLS.LoadServerTLSCertificate(certFile, keyFile, "", false); err != nil {
		log.GetLogger().Errorf("failed to load Server TLS Certificate: %s", err.Error())
		return nil, err
	}

	clientAuth := tls.NoClientCert
	tlsConfig := &tls.Config{
		RootCAs:      pools,
		Certificates: certs,
		ClientAuth:   clientAuth,
	}
	return tlsConfig, nil
}

func isInterfaceNil(i interface{}) bool {
	vi := reflect.ValueOf(i)
	if vi.Kind() == reflect.Ptr {
		return vi.IsNil()
	}
	return false
}

func processRedisFile(c redis.Cmdable, path string) error {
	if utils.ExistPath(path) {
		fi, err := os.Open(path)
		if err != nil {
			log.GetLogger().Errorf("failed to open the file %s ,with error: %s", path, err.Error())
			return err
		}
		defer fi.Close()
		log.GetLogger().Infof("success to load temp file: %s", path)
		br := bufio.NewReader(fi)
		for {
			content, _, err := br.ReadLine()
			if err == io.EOF {
				break
			}
			if len(content) == 0 {
				continue
			}
			key, value, err := splitContent(content)
			if err != nil {
				continue
			}
			log.GetLogger().Debugf("success to load key value: %s and %s", key, value)
			ctx, cancel := context.WithTimeout(context.Background(), DefaultRedisContextTimeout)
			if path == redisStringFile {
				_, err = c.Set(ctx, key, value, TTL).Result()
			} else if path == redisSliceFile {
				_, err = c.RPush(ctx, key, value).Result()
			}
			cancel()
			if err != nil {
				log.GetLogger().Errorf("failed to write data to redis: %s", err.Error())
				return err
			}
		}
		return deleteTempCache(path)
	}
	return nil
}

// TakeCacheToRedis take temp data and write to redis
func TakeCacheToRedis(c *Client) error {
	if c == nil {
		log.GetLogger().Errorf("redis is not ready when take cache to redis")
		return nil
	}
	cli := c.GetClient()
	cacheLock.Lock()
	defer cacheLock.Unlock()
	if cli == nil || isInterfaceNil(cli) {
		log.GetLogger().Errorf("redis is not ready when take cache to redis")
		return nil
	}
	err := processRedisFile(cli, redisStringFile)
	if err != nil {
		return err
	}
	err = processRedisFile(cli, redisSliceFile)
	if err != nil {
		return err
	}
	return nil
}

func splitContent(content []byte) (string, string, error) {
	contents := strings.Split(string(content), redisSeparator)
	if len(contents) < redisValueIndex {
		return "", "", errors.New("failed to take data from file")
	}
	key, value := contents[0], contents[1]
	return key, value, nil
}

func deleteTempCache(file string) error {
	if utils.ExistPath(file) {
		err := os.Remove(file)
		if err != nil {
			log.GetLogger().Errorf("failed to delete temp file %s", file)
			return err
		}
		log.GetLogger().Infof("succeed to delete temp file %s", file)
	}
	return nil
}

// WriteToDisk write redis info to disk as temp file
func WriteToDisk(redisCache RedisCache) error {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	var f *os.File
	var err error
	var file string
	if redisCache.Method == "set" {
		file = redisStringFile
	} else if redisCache.Method == "push" {
		file = redisSliceFile
	}
	f, err = os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		log.GetLogger().Errorf("failed to open file %s", file)
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	_, err = w.WriteString(redisCache.Key + redisSeparator + redisCache.Value + "\n")
	if err != nil {
		log.GetLogger().Errorf("failed to write to disk with error: %s", err.Error())
	}
	log.GetLogger().Infof("success to write data to file %s", file)
	return w.Flush()
}

// GetDefaultTimeoutConf returns defaultTimeoutConf
func GetDefaultTimeoutConf() TimeoutConf {
	return defaultTimeoutConf
}

// ZADDMetricsToRedis -
func ZADDMetricsToRedis(key string, metrics interface{}, limit int64, expireTime time.Duration) error {
	redisCmd := GetRedisCmd()
	if redisCmd == nil {
		log.GetLogger().Errorf("redis client is nil")
		return errors.New("redis client is nil")
	}
	count, err := redisCmd.ZCard(context.TODO(), key).Result()
	if err != nil {
		log.GetLogger().Errorf("failed to ZCard metrics key %s from redis, err: %s", key, err.Error())
		return err
	}
	// if count reach limit, delete the earliest metric
	if count >= limit {
		earliestValues, err := redisCmd.ZRange(context.TODO(), key, 0, 0).Result()
		if err != nil {
			log.GetLogger().Errorf("failed to ZRange metrics key %s from redis, err: %s", key, err.Error())
			return err
		}
		_, err = redisCmd.ZRem(context.TODO(), key, earliestValues[0]).Result()
		if err != nil {
			log.GetLogger().Errorf("failed to ZRem metrics key %s to redis, err: %s", key, err.Error())
			return err
		}
	}
	// Add a new value to the sorted set, with the score being the current timestamp
	score := time.Now().Unix()
	err = redisCmd.ZAdd(context.TODO(), key, &redis.Z{Score: float64(score), Member: metrics}).Err()
	if err != nil {
		log.GetLogger().Errorf("failed to ZAdd metrics key %s to redis, err: %s", key, err.Error())
		return err
	}
	redisCmd.Expire(context.TODO(), key, expireTime)
	return nil
}

// SetWithRetry -
func SetWithRetry(redisKey string, redisValue []byte, expireTime time.Duration) bool {
	redisCmd := GetRedisCmd()
	if redisCmd == nil {
		log.GetLogger().Errorf("redis client is nil")
		return false
	}
	for i := 0; i < redisRetryTimes; i++ {
		_, err := redisCmd.Set(context.TODO(), redisKey, string(redisValue), expireTime).Result()
		if err == nil {
			log.GetLogger().Debugf("succeed to set data to redis key: %s, value:%s",
				redisKey, string(redisValue))
			return true
		}
		log.GetLogger().Warnf("failed to set key: %s, err: %s, retry time %d", redisKey, err.Error(), i)
		time.Sleep(redisRetryInterval)
	}
	return false
}

// GetClient -
func (c *Client) GetClient() redis.Cmdable {
	c.RLock()
	defer c.RUnlock()
	return c.client
}

// SetClient -
func (c *Client) SetClient(cli redis.Cmdable) {
	c.Lock()
	defer c.Unlock()
	c.client = cli
}

// HGetAll -
func (c *Client) HGetAll(ctx context.Context, key string) *redis.StringStringMapCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HGetAll(ctx, key)
}

// HDel -
func (c *Client) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HDel(ctx, key, fields...)
}

// Ping -
func (c *Client) Ping(ctx context.Context) *redis.StatusCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.Ping(ctx)
}

// Set -
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.Set(ctx, key, value, expiration)
}

// RPush -
func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.RPush(ctx, key, values...)
}

// HMSet -
func (c *Client) HMSet(ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HMSet(ctx, key, values...)
}

// Expire -
func (c *Client) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.Expire(ctx, key, expiration)
}

// Get -
func (c *Client) Get(ctx context.Context, key string) *redis.StringCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.Get(ctx, key)
}

// Del -
func (c *Client) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.Del(ctx, keys...)
}

// ZCard -
func (c *Client) ZCard(ctx context.Context, key string) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.ZCard(ctx, key)
}

// SetNX -
func (c *Client) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.SetNX(ctx, key, value, expiration)
}

// Exists -
func (c *Client) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.Exists(ctx, keys...)
}

// SAdd -
func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.SAdd(ctx, key, members...)
}

// SRem -
func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.SRem(ctx, key, members...)
}

// SMembers -
func (c *Client) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.SMembers(ctx, key)
}

// SCard -
func (c *Client) SCard(ctx context.Context, key string) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.SCard(ctx, key)
}

// HSetNX -
func (c *Client) HSetNX(ctx context.Context, key, field string, value interface{}) *redis.BoolCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HSetNX(ctx, key, field, value)
}

// HLen -
func (c *Client) HLen(ctx context.Context, key string) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HLen(ctx, key)
}

// HExists -
func (c *Client) HExists(ctx context.Context, key, field string) *redis.BoolCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HExists(ctx, key, field)
}

// TTL -
func (c *Client) TTL(ctx context.Context, key string) *redis.DurationCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.TTL(ctx, key)
}

// Scan -
func (c *Client) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.Scan(ctx, cursor, match, count)
}

// SIsMember -
func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.SIsMember(ctx, key, member)
}

// ZRange -
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.ZRange(ctx, key, start, stop)
}

// ZRem -
func (c *Client) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.ZRem(ctx, key, members...)
}

// ZAdd -
func (c *Client) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.ZAdd(ctx, key, members...)
}

// HSet -
func (c *Client) HSet(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HSet(ctx, key, values...)
}

// HGet -
func (c *Client) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.HGet(ctx, key, field)
}

// TxPipeline -
func (c *Client) TxPipeline() redis.Pipeliner {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.TxPipeline()
}

// LLen -
func (c *Client) LLen(ctx context.Context, key string) *redis.IntCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.LLen(ctx, key)
}

// LPop -
func (c *Client) LPop(ctx context.Context, key string) *redis.StringCmd {
	c.RLock()
	cli := c.client
	c.RUnlock()
	return cli.LPop(ctx, key)
}
