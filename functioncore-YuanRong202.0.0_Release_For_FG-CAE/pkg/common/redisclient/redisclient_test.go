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
package redisclient

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/go-redis/redis/v8"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/alarm"
	commonTLS "functioncore/pkg/common/tls"
	"functioncore/test/utils"
)

func TestZADDMetricsToRedis(t *testing.T) {
	err := ZADDMetricsToRedis("mockKey", 1, 3, 5*time.Second)
	assert.NotNil(t, err)
	convey.Convey("TestZADDMetricsToRedis", t, func() {
		convey.Convey("ZCard exception", func() {
			redisCmd = &Client{client: &redis.Client{}}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "ZCard",
					func(cli *redis.Client, ctx context.Context, key string) *redis.IntCmd {
						return &redis.IntCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
					func(_ *redis.IntCmd) (int64, error) {
						return 0, errors.New("mock ZCard error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err = ZADDMetricsToRedis("mockKey", 1, 3, 5*time.Second)
			assert.NotNil(t, err)
			assert.Equal(t, "mock ZCard error", err.Error())
		})

		convey.Convey("ZRange exception", func() {
			redisCmd = &Client{client: &redis.Client{}}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "ZCard",
					func(cli *redis.Client, ctx context.Context, key string) *redis.IntCmd {
						return &redis.IntCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
					func(_ *redis.IntCmd) (int64, error) {
						return 3, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "ZRange",
					func(cli *redis.Client, ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
						return &redis.StringSliceCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringSliceCmd{}), "Result",
					func(_ *redis.StringSliceCmd) ([]string, error) {
						return nil, errors.New("mock ZRange error")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err = ZADDMetricsToRedis("mockKey", 1, 3, 5*time.Second)
			assert.NotNil(t, err)
			assert.Equal(t, "mock ZRange error", err.Error())
		})

		convey.Convey("ZAdd success", func() {
			redisCmd = &Client{client: &redis.Client{}}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "ZCard",
					func(cli *redis.Client, ctx context.Context, key string) *redis.IntCmd {
						return &redis.IntCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
					func(_ *redis.IntCmd) (int64, error) {
						return 3, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "ZRange",
					func(cli *redis.Client, ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
						return &redis.StringSliceCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.StringSliceCmd{}), "Result",
					func(_ *redis.StringSliceCmd) ([]string, error) {
						return []string{"0", "1", "2"}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "ZRem",
					func(cli *redis.Client, ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
						return &redis.IntCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "ZAdd",
					func(cli *redis.Client, ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
						return &redis.IntCmd{}
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "Expire",
					func(cli *redis.Client, ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
						return &redis.BoolCmd{}
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err = ZADDMetricsToRedis("mockKey", 1, 3, 5*time.Second)
			assert.Nil(t, err)
		})
	})
}

func TestCheckNormalConnection(t *testing.T) {
	options := &redis.Options{
		Addr:         "127.0.0.1",
		Password:     "aaa",
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		DialTimeout:  dialTimeout,
		MaxRetries:   maxRetryTimes,
	}
	redisCMD := redis.NewClient(options)
	type args struct {
		c redis.Cmdable
	}
	var a args
	statusCMD := &redis.StatusCmd{}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(redisCMD), "Ping",
			func(_ *redis.Client, _ context.Context) *redis.StatusCmd {
				return statusCMD
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	a.c = redisCMD
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{client: tt.args.c}
			if err := c.checkNormalConnection(); (err != nil) != tt.wantErr {
				t.Errorf("CheckNormalConnection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNew(t *testing.T) {
	type args struct {
		serverMode string
		serverAddr string
		password   string
		options    []Option
	}
	var a args
	var b args
	option := SetEnableTLS(false)
	b.serverMode = "single"
	b.options = append(b.options, option)
	var c args
	c.serverMode = "cluster"
	c.options = append(b.options, option)
	c.options = append(b.options, SetGetRealTimeServerAddrFunc(func() (string, TimeoutConf, error) {
		return "", TimeoutConf{}, nil
	}))
	tests := []struct {
		name    string
		args    args
		want    redis.Cmdable
		wantErr bool
	}{
		{"case1", a, nil, true},
		{"case2", b, nil, true},
		{"case3", c, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := New(NewRedisClientParam{
				tt.args.serverMode,
				tt.args.serverAddr,
				tt.args.password,
				TimeoutConf{},
				false,
				nil,
			}, nil, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("New() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != nil {
				t.Errorf("New() got = %v, want %v", got, tt.want)
			}
		})
	}

	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{gomonkey.ApplyFunc((*alarm.Client).RegisterAlarmType,
		func(a *alarm.Client, almType alarm.Type) {
			return
		})})
	statusCMD := &redis.StatusCmd{}
	patches.Append(utils.PatchSlice{gomonkey.ApplyFunc((*redis.Client).Ping,
		func(_ *redis.Client, _ context.Context) *redis.StatusCmd {
			return statusCMD
		})})
	defer patches.ResetAll()
	_, err := New(NewRedisClientParam{
		"single",
		"",
		"",
		TimeoutConf{},
		false,
		nil,
	}, nil, AlarmSwitchOn())
	if err != nil {
		t.Errorf("failed to test new client with alarm switch on: %s", err.Error())
	}
}

func TestWriteToDisk(t *testing.T) {
	convey.Convey("Test Write To Disk 1", t, func() {
		r := RedisCache{
			Method: "set",
			Key:    "key",
			Value:  "value",
		}
		err := WriteToDisk(r)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test Write To Disk 2", t, func() {
		r := RedisCache{
			Method: "push",
			Key:    "key",
			Value:  "value",
		}
		err := WriteToDisk(r)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test Write To Disk error", t, func() {
		patch := gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
			return nil, errors.New("err")
		})
		defer patch.Reset()
		r := RedisCache{
			Method: "push",
			Key:    "key",
			Value:  "value",
		}
		err := WriteToDisk(r)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestTakeCacheToRedis(t *testing.T) {
	convey.Convey("Test Take Cache To Redis", t, func() {
		mc := &Client{}
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(os.Stat, func(name string) (os.FileInfo, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(bufio.NewReader, func(rd io.Reader) *bufio.Reader {
				return new(bufio.Reader)
			}),
			gomonkey.ApplyFunc((*bufio.Reader).ReadLine, func(*bufio.Reader) (line []byte, isPrefix bool, err error) {
				str := "key:value"
				line = []byte(str)
				return line, false, io.EOF
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		err := TakeCacheToRedis(mc)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestDeleteTempCache(t *testing.T) {
	path := "./test"
	os.Create(path)
	err := deleteTempCache(path)
	assert.Equal(t, err, nil)
}

func TestSplitContent(t *testing.T) {
	content := []byte("key value")
	_, _, err := splitContent(content)
	assert.NotNil(t, err)
	content = []byte("key%WITH%value")
	_, _, err = splitContent(content)
	assert.Nil(t, err)
}

func Test_buildCfg(t *testing.T) {
	type args struct {
		caFile   string
		certFile string
		keyFile  string
	}
	var a args
	tests := []struct {
		name    string
		args    args
		want    *tls.Config
		wantErr bool
	}{
		{"case1", a, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := buildCfg(tt.args.caFile, tt.args.certFile, tt.args.keyFile)
			assert.Equalf(t, tt.want, got, "buildCfg(%v, %v, %v)", tt.args.caFile, tt.args.certFile, tt.args.keyFile)
		})
	}
}

func TestNewClient(t *testing.T) {
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(commonTLS.GetX509CACertPool,
		func(caCertFilePath string) (caCertPool *x509.CertPool, err error) {
			return nil, nil
		})})
	patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(commonTLS.LoadServerTLSCertificate,
		func(certFile, keyFile string, passPhase string, isHttps bool) (tlsCert []tls.Certificate,
			err error) {
			return nil, nil
		})})
	defer patches.ResetAll()

	convey.Convey("Test Single Client", t, func() {
		o := redisClientOption{
			serverMode: "single",
			enableTLS:  true,
		}
		redisCMD, err := newClient(o)
		convey.So(redisCMD, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test cluster Client", t, func() {
		o := redisClientOption{
			serverMode: "cluster",
			enableTLS:  true,
		}
		redisCMD, err := newClient(o)
		convey.So(redisCMD, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test default Client", t, func() {
		o := redisClientOption{
			serverMode: "",
			enableTLS:  true,
		}
		redisCMD, err := newClient(o)
		convey.So(redisCMD, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestGetRedisAddress(t *testing.T) {
	c := &Client{option: redisClientOption{
		serverAddr: "123",
	}}
	addr := GetRedisAddress(c)
	assert.Equal(t, addr, "123")
	var d *Client
	addr2 := GetRedisAddress(d)
	assert.Equal(t, addr2, "")
}

func TestCheckUnNormalConnection(t *testing.T) {
	options := &redis.Options{
		Addr:         "10.0.0.1",
		Password:     "aaa",
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		DialTimeout:  dialTimeout,
		MaxRetries:   maxRetryTimes,
	}
	redisCMD := redis.NewClient(options)
	redisOption := redisClientOption{
		serverAddr: "10.0.0.1",
		hotloadConfFunc: func() (string, TimeoutConf, error) {
			return "10.0.0.2", TimeoutConf{}, nil
		},
		serverMode: "single",
	}
	ReClient := &Client{redisCMD, redisOption, true, sync.RWMutex{}}
	err := ReClient.checkNormalConnection()
	assert.Errorf(t, err, "dial tcp: address 10.0.0.1: missing port in address")
}

func TestEmptyClients(t *testing.T) {
	opt := redisClientOption{enableTLS: true}
	redisCMD := newSingleClient(opt)
	assert.Equal(t, redisCMD, nil)
	redisCMD = newClusterClient(opt)
	assert.Equal(t, redisCMD, nil)
}

func TestBuildCfg(t *testing.T) {
	patches := utils.InitPatchSlice()
	patches.Append(utils.PatchSlice{gomonkey.ApplyFunc(commonTLS.GetX509CACertPool,
		func(caCertFilePath string) (caCertPool *x509.CertPool, err error) {
			return nil, nil
		})})
	defer patches.ResetAll()
	convey.Convey("Test build cfg error", t, func() {
		tlsConfig, err := buildCfg(DefaultCAFile, DefaultCertFile, DefaultKeyFile)
		convey.So(tlsConfig, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func Test_healthCheckAndReport(t *testing.T) {
	client := &Client{}
	convey.Convey("healthCheckAndReport", t, func() {
		convey.Convey("report alarm", func() {
			stopCh := make(chan struct{})
			reportChan := make(chan int, 1)
			defer gomonkey.ApplyFunc((*Client).checkNormalConnection, func(*Client) error {
				return errors.New("")
			}).ApplyFunc(time.Sleep, func(time.Duration) {
				close(stopCh)
			}).ApplyFunc((*alarm.Client).ReportAlarm,
				func(a *alarm.Client, almType alarm.Type, specAddition string) {
					reportChan <- 1
				}).Reset()
			go client.healthCheckAndReport(stopCh, 10)
			res := <-reportChan
			assert.Equal(t, 1, res)
		})
		convey.Convey("clear alarm", func() {
			stopCh := make(chan struct{})
			reportChan := make(chan int, 2)
			defer gomonkey.ApplyFunc((*Client).checkNormalConnection, func(*Client) error {
				return nil
			}).ApplyFunc(time.Sleep, func(time.Duration) {
				close(stopCh)
			}).ApplyFunc((*alarm.Client).ClearAlarm,
				func(a *alarm.Client, almType alarm.Type) {
					reportChan <- 1
				}).Reset()
			go client.healthCheckAndReport(stopCh, 10)
			impRes := <-reportChan
			criRes := <-reportChan
			assert.Equal(t, 1, impRes)
			assert.Equal(t, 1, criRes)
		})
		convey.Convey("report critical alarm", func() {
			stopCh := make(chan struct{})
			reportChan := make(chan int, 2)
			defer gomonkey.ApplyFunc((*Client).checkNormalConnection, func(*Client) error {
				return errors.New("")
			}).ApplyFunc(time.Sleep, func(time.Duration) {
				close(stopCh)
			}).ApplyFunc((*alarm.Client).ReportAlarm,
				func(a *alarm.Client, almType alarm.Type, specAddition string) {
					reportChan <- 1
				}).Reset()
			go client.healthCheckAndReport(stopCh, 1)
			impRes := <-reportChan
			criRes := <-reportChan
			assert.Equal(t, 1, impRes)
			assert.Equal(t, 1, criRes)
		})
	})
}

func TestGetDefaultTimeoutConf(t *testing.T) {
	tests := []struct {
		name string
		want TimeoutConf
	}{
		{
			name: "case1",
			want: defaultTimeoutConf,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetDefaultTimeoutConf(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetDefaultTimeoutConf() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClient_checkTimeout(t *testing.T) {
	options := &redis.Options{
		Addr:         "10.0.0.1",
		Password:     "aaa",
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		DialTimeout:  dialTimeout,
		MaxRetries:   maxRetryTimes,
	}
	redisCMD := redis.NewClient(options)
	redisOption := redisClientOption{
		serverAddr: "10.0.0.1",
		hotloadConfFunc: func() (string, TimeoutConf, error) {
			return "10.0.0.2", TimeoutConf{}, nil
		},
		serverMode: "single",
	}
	type fields struct {
		Cmdable   redis.Cmdable
		option    redisClientOption
		connected bool
		RWMutex   sync.RWMutex
	}
	type args struct {
		timeoutConf TimeoutConf
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "case1",
			fields: fields{
				Cmdable:   redisCMD,
				option:    redisOption,
				connected: true,
				RWMutex:   sync.RWMutex{},
			},
			args: args{
				timeoutConf: TimeoutConf{
					DialTimeout:  defaultDialTimeout,
					ReadTimeout:  defaultReadTimeout,
					WriteTimeout: 3,
					IdleTimeout:  2,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				client:    tt.fields.Cmdable,
				option:    tt.fields.option,
				connected: tt.fields.connected,
				RWMutex:   tt.fields.RWMutex,
			}
			c.checkTimeout(tt.args.timeoutConf)
			if c.option.idleTimeout != 2*time.Second || c.option.writeTimeout != 3*time.Second {
				t.Error("wrong timeout")
			}
		})
	}
}

func Test_getNewRedisOption(t *testing.T) {
	type args struct {
		param NewRedisClientParam
	}
	tests := []struct {
		name string
		args args
		want redisClientOption
	}{
		{
			name: "case1",
			args: args{
				param: NewRedisClientParam{
					"single",
					"127.0.0.1",
					"aaa",
					TimeoutConf{},
					false,
					nil,
				},
			},
			want: redisClientOption{
				serverAddr:   "127.0.0.1",
				serverMode:   "single",
				password:     "aaa",
				dialTimeout:  dialTimeout,
				readTimeout:  readTimeout,
				writeTimeout: writeTimeout,
				idleTimeout:  idleTimeout,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getNewRedisOption(tt.args.param); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getNewRedisOption() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_initClient(t *testing.T) {
	convey.Convey("Test redis Client is success", t, func() {
		param := NewRedisClientParam{
			ServerMode:      "122",
			ServerAddr:      "333",
			Password:        "1222",
			Timeout:         TimeoutConf{},
			EnableTLS:       false,
			HotloadConfFunc: nil,
		}
		defer gomonkey.ApplyFunc(New, func(newClientParam NewRedisClientParam, stopCh <-chan struct{}, options ...Option) (*Client, error) {
			return &Client{}, nil
		}).Reset()
		stopCh := make(chan struct{})
		redisClient, _ := initClient(&param, stopCh)
		convey.So(redisClient, convey.ShouldNotBeNil)
	})
	convey.Convey("Test to not init redis client", t, func() {
		param := NewRedisClientParam{
			ServerMode:      "122",
			ServerAddr:      "333",
			Password:        "1222",
			Timeout:         TimeoutConf{},
			EnableTLS:       false,
			HotloadConfFunc: nil,
		}
		defer gomonkey.ApplyFunc(New, func(newClientParam NewRedisClientParam, stopCh <-chan struct{}, options ...Option) (*Client, error) {
			return &Client{}, errors.New("redis is not ready")
		}).Reset()
		stopCh := make(chan struct{})
		_, err := initClient(&param, stopCh)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestCheckRedisConnectivity(t *testing.T) {
	var isCalled = 0
	convey.Convey("Test check redis to connect in cyclist", t, func() {
		param := NewRedisClientParam{
			ServerMode:      "122",
			ServerAddr:      "333",
			Password:        "1222",
			Timeout:         TimeoutConf{},
			EnableTLS:       false,
			HotloadConfFunc: nil,
		}
		patch1 := gomonkey.ApplyFunc(New, func(newClientParam NewRedisClientParam, stopCh <-chan struct{}, options ...Option) (*Client, error) {
			isCalled++
			return &Client{}, nil
		})
		patch := gomonkey.ApplyFunc((*redis.Client).Ping,
			func(_ *redis.Client, _ context.Context) *redis.StatusCmd {
				return &redis.StatusCmd{}
			})
		patch.ApplyFunc((*alarm.Client).ClearAlarm,
			func(a *alarm.Client, almType alarm.Type) {
				return
			})
		defer patch.Reset()
		stopCh := make(chan struct{}, 0)
		tickerCh := make(chan time.Time)
		patch.ApplyFunc(time.NewTicker, func(_ time.Duration) *time.Ticker {
			return &time.Ticker{C: tickerCh}
		})

		go CheckRedisConnectivity(&param, nil, stopCh)
		tickerCh <- time.Time{}
		stopCh <- struct{}{}
		convey.So(isCalled, convey.ShouldEqual, 1)
		patch1.Reset()
		patch.ApplyFunc(New, func(newClientParam NewRedisClientParam, stopCh <-chan struct{}, options ...Option) (*Client, error) {
			isCalled++
			return &Client{}, errors.New("state is not ready")
		})
		stopCh = make(chan struct{}, 0)
		go CheckRedisConnectivity(&param, nil, stopCh)
		tickerCh <- time.Time{}
		tickerCh <- time.Time{}
		stopCh <- struct{}{}
		convey.So(isCalled, convey.ShouldEqual, 3)
	})
}

func TestSetWithRetry(t *testing.T) {
	type args struct {
		redisKey   string
		redisValue []byte
		expireTime time.Duration
	}
	tests := []struct {
		name      string
		args      args
		want      bool
		redisCmd  *Client
		patchFunc utils.PatchesFunc
	}{
		{
			"test redis cli is nil",
			args{},
			false,
			nil,
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
		},
		{
			"test failed",
			args{},
			false,
			&Client{
				client: &redis.Client{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "Set",
						func(_ *redis.Client, ctx context.Context,
							key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result", func(_ *redis.StatusCmd) (string, error) {
							return "", errors.New("result error")
						}),
				})
				return patches
			},
		},
		{
			"test success",
			args{},
			true,
			&Client{
				client: &redis.Client{},
			},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.Client{}), "Set",
						func(_ *redis.Client, ctx context.Context,
							key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
							return &redis.StatusCmd{}
						}),
					gomonkey.ApplyMethod(reflect.TypeOf(&redis.StatusCmd{}),
						"Result", func(_ *redis.StatusCmd) (string, error) {
							return "", nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		tempCmd := redisCmd
		redisCmd = tt.redisCmd
		patches := tt.patchFunc()
		assert.Equalf(t, tt.want, SetWithRetry(tt.args.redisKey, tt.args.redisValue, tt.args.expireTime),
			"SetWithRetry(%v, %v, %v)", tt.args.redisKey, tt.args.redisValue, tt.args.expireTime)
		patches.ResetAll()
		redisCmd = tempCmd
	}
}

func TestClient_Methods(t *testing.T) {
	c := Client{client: redis.NewClient(&redis.Options{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	clientRef := reflect.ValueOf(&c)
	ctxRef := reflect.ValueOf(ctx)

	// use go test -race to assume write and read concurrently will not panic;
	go func() {
		for i := 0; i < 1000; i++ {
			c.SetClient(redis.NewClient(&redis.Options{}))
		}
	}()

	tests := []struct {
		name  string
		param []reflect.Value
	}{
		{
			name:  "HGetAll",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "HDel",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "Ping",
			param: []reflect.Value{clientRef, ctxRef},
		},
		{
			name:  "Set",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf(""), reflect.ValueOf(time.Duration(0))},
		},
		{
			name:  "RPush",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "HMSet",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "Expire",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf(time.Duration(0))},
		},
		{
			name:  "Get",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "Del",
			param: []reflect.Value{clientRef, ctxRef},
		},
		{
			name:  "ZCard",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "SetNX",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf(""), reflect.ValueOf(time.Duration(0))},
		},
		{
			name:  "Exists",
			param: []reflect.Value{clientRef, ctxRef},
		},
		{
			name:  "SAdd",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "SRem",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "SMembers",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "SCard",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "HSetNX",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf(""), reflect.ValueOf(0)},
		},
		{
			name:  "HLen",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "HExists",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf("")},
		},
		{
			name:  "TTL",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "Scan",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(uint64(0)), reflect.ValueOf(""), reflect.ValueOf(int64(0))},
		},
		{
			name:  "SIsMember",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf("")},
		},
		{
			name:  "ZRange",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf(int64(0)), reflect.ValueOf(int64(0))},
		},
		{
			name:  "ZRem",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "ZAdd",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "HSet",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "HGet",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf(""), reflect.ValueOf("")},
		},
		{
			name:  "TxPipeline",
			param: []reflect.Value{clientRef},
		},
		{
			name:  "LLen",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
		{
			name:  "LPop",
			param: []reflect.Value{clientRef, ctxRef, reflect.ValueOf("")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := reflect.TypeOf(&c)
			f, ok := ref.MethodByName(tt.name)
			assert.True(t, ok, tt.name)
			f.Func.Call(tt.param)
		})
	}
}
