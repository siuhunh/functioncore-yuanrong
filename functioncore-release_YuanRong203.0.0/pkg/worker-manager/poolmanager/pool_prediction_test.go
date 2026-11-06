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

// Package poolmgr manage pool
package poolmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/redis/go-redis/v9"

	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/monitoring"
	mockUtils "functioncore/test/utils"
)

func TestPoolMgrUpdatePodUsedRecords(t *testing.T) {
	mockPool22 := standard("pool22")
	mockPool23 := standard("pool23")
	type fields struct {
		podUsedHistory      []*podUsedRecordChanElement
		receiveChanIsClosed bool
	}
	type args struct {
		stopCh chan struct{}
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		expected map[standard]int64
	}{
		{
			name:   "case 1 goroutine return when stop chan is nil",
			fields: fields{},
			args:   args{stopCh: nil},
		}, {
			name:   "case 2 goroutine return when stop chan receives msg",
			fields: fields{},
			args:   args{stopCh: make(chan struct{})},
		}, {
			name:   "case 3 goroutine return when receive chan is closed",
			fields: fields{receiveChanIsClosed: true},
			args:   args{stopCh: make(chan struct{})},
		}, {
			name: "case 4 record the max value for history records",
			fields: fields{
				podUsedHistory: []*podUsedRecordChanElement{
					{
						poolStandard: mockPool22,
						usedNumber:   10,
					}, {
						poolStandard: mockPool22,
						usedNumber:   30,
					}, {
						poolStandard: mockPool23,
						usedNumber:   50,
					},
				},
			},
			args: args{stopCh: make(chan struct{})},
			expected: map[standard]int64{
				mockPool22: 30,
				mockPool23: 50,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pr := newPodUsedRecord()
			pr.usedPodRecordVal[mockPool22] = 0
			pr.usedPodRecordVal[mockPool23] = 0
			go pr.updateUsedPodRecord(tt.args.stopCh)
			if tt.args.stopCh != nil {
				if tt.fields.receiveChanIsClosed {
					close(pr.UsedPodRecordChan)
				}
				if tt.fields.podUsedHistory != nil {
					for _, record := range tt.fields.podUsedHistory {
						pr.UsedPodRecordChan <- record
					}
				}
				timer := time.NewTimer(time.Millisecond * 30)
				go func(stopCh chan struct{}) {
					<-timer.C
					close(stopCh)
				}(tt.args.stopCh)
				time.Sleep(time.Millisecond * 100)
				if tt.fields.podUsedHistory != nil && !reflect.DeepEqual(pr.usedPodRecordVal, tt.expected) {
					t.Errorf("pod used record: actual %v, want %v", pr.usedPodRecordVal, tt.expected)
				}
			}
			time.Sleep(time.Millisecond * 100)
		})
	}
}

func TestUploadPodUsedRecordsToRedis(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				UsedPodRecordResetAndUploadInterval: 1,
			},
		}, nil
	}
	type fields struct {
		receiveChanIsClosed bool
	}
	type args struct {
		stopCh chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:   "case 1 check stopCh is nil",
			fields: fields{},
			args:   args{stopCh: nil},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 2 goroutine return when receive chan is closed",
			fields: fields{
				receiveChanIsClosed: true,
			},
			args: args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name:   "case 3 successfully reset current records and upload to Redis",
			fields: fields{},
			args:   args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					ApplyFunc((*PodUsedRecord).resetUsedPodRecord,
						func(_ *PodUsedRecord) { return },
					),
					ApplyFunc((*PodUsedRecord).uploadUsedPodRecordJSON,
						func(_ *PodUsedRecord) { return },
					),
				})
				return patches
			},
		}, {
			name:   "case 4 when can not get faas config",
			fields: fields{},
			args:   args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := newPodUsedRecord()
			go p.uploadAndResetUsedPodRecordEveryMinute(tt.args.stopCh)
			if tt.args.stopCh != nil {
				if tt.fields.receiveChanIsClosed {
					close(p.UsedPodRecordChan)
				}
				timer := time.NewTimer(50 * time.Millisecond)
				go func(stopCh chan struct{}) {
					<-timer.C
					close(stopCh)
				}(tt.args.stopCh)
			}
			time.Sleep(time.Millisecond * 100)
			patches.ResetAll()
		})
	}
}

func TestResetUsedPodRecord(t *testing.T) {
	mockStandard22 := standard("pool22")
	mockStandard23 := standard("pool23")
	type fields struct {
		usedPodRecordMap  map[standard]int64
		usedPodRecordJson string
	}
	type expected struct {
		mapVal     map[standard]int64
		jsonString string
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		expected    expected
	}{
		{
			name: "case 1 succeed to marshal use record and reset to default value",
			fields: fields{
				usedPodRecordMap: map[standard]int64{
					mockStandard22: 12,
					mockStandard23: 24,
				},
				usedPodRecordJson: "",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			expected: expected{
				mapVal: map[standard]int64{
					mockStandard22: 0,
					mockStandard23: 0,
				},
				jsonString: `{"pool22":12,"pool23":24}`,
			},
		}, {
			name: "case 2 fail to marshal used record",
			fields: fields{
				usedPodRecordMap: map[standard]int64{
					mockStandard22: 12,
					mockStandard23: 24,
				},
				usedPodRecordJson: "",
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(json.Marshal,
						func(v interface{}) ([]byte, error) {
							return nil, errors.New("mock error")
						},
					),
				})
				return patches
			},
			expected: expected{
				mapVal: map[standard]int64{
					mockStandard22: 0,
					mockStandard23: 0,
				},
				jsonString: ``,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := newPodUsedRecord()
			p.usedPodRecordJSON = tt.fields.usedPodRecordJson
			p.usedPodRecordVal = tt.fields.usedPodRecordMap
			p.resetUsedPodRecord()
			if !reflect.DeepEqual(p.usedPodRecordVal, tt.expected.mapVal) {
				t.Errorf("reset used pod record: actual = %v, want %v",
					p.usedPodRecordVal, tt.expected.mapVal)
			}
			if !reflect.DeepEqual(p.usedPodRecordJSON, tt.expected.jsonString) {
				t.Errorf("reset used pod record json string: actual = %s, want %s",
					p.usedPodRecordJSON, tt.expected.jsonString)
			}
			patches.ResetAll()
		})
	}
}

func TestUploadUsedPodRecordJson(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				UsedPodRecordExpiration: 8 * 24,
			},
			EnableUploadUsedPodRecordToRedis: true,
		}, nil
	}
	mockTime := time.Now()
	//mockClusterId := "fgs-yuanrong-az1-01"
	mockUsedPodRecordJson := `{"pool22":12,"pool23":24}`
	//mockPodUsedRecordRedisKey := "PoolPrediction:UsedRecord:fgs-yuanrong-az1-01:" +
	mockTime.Format(usedPodRecordRedisKeySuffixFormat)
	mockDefaultPodUsedRecordRedisKey := "PoolPrediction:UsedRecord:default:" +
		mockTime.Format(usedPodRecordRedisKeySuffixFormat)
	//expirationTime := 8 * 24 * time.Hour
	intCmd := &redis.IntCmd{}
	intCmd.SetVal(1)
	intCmd.SetErr(nil)
	boolCmd := &redis.BoolCmd{}
	boolCmd.SetVal(true)
	boolCmd.SetErr(nil)
	mockRedis := make(map[string]string)
	mockRedisExpiration := make(map[string]time.Duration)
	type fields struct {
		usedPodRecordJson string
		isException       bool
		redisKey          string
	}
	type expected struct {
		jsonString string
		expiration time.Duration
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		expected    expected
	}{
		//{
		//	name: "case 1 upload redis key, value and expiration time successfully",
		//	fields: fields{
		//		usedPodRecordJson: mockUsedPodRecordJson,
		//		redisKey:          mockPodUsedRecordRedisKey,
		//	},
		//	expected: expected{
		//		jsonString: mockUsedPodRecordJson,
		//		expiration: expirationTime,
		//	},
		//	patchesFunc: func() mockUtils.PatchSlice {
		//		patches := mockUtils.InitPatchSlice()
		//		patches.Append(mockUtils.PatchSlice{
		//			ApplyFunc(config.GetConfig, mockGetConfigFunc),
		//			ApplyFunc((*redisclient.Client).HSet,
		//				func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
		//					mockRedis[key] = values[1].(string)
		//					return intCmd
		//				},
		//			),
		//			ApplyFunc((*redisclient.Client).Expire,
		//				func(_ *redisclient.Client, ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
		//					mockRedisExpiration[key] = expiration
		//					return boolCmd
		//				},
		//			),
		//			ApplyFunc(time.Now,
		//				func() time.Time {
		//					return mockTime
		//				},
		//			),
		//			ApplyFunc(os.Getenv,
		//				func(str string) string {
		//					return mockClusterId
		//				},
		//			),
		//			ApplyFunc(monitoring.GetRedisClient,
		//				func() *redisclient.Client {
		//					return &redisclient.Client{}
		//				},
		//			),
		//		})
		//		return patches
		//	},
		//},
		{
			name: "case 2 skip upload when json string is nil",
			fields: fields{
				usedPodRecordJson: "",
				isException:       true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 3 failed to convert to UTC time",
			fields: fields{
				usedPodRecordJson: mockUsedPodRecordJson,
				isException:       true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(time.LoadLocation,
						func(key string) (*time.Location, error) {
							return nil, errors.New("")
						},
					),
				})
				return patches
			},
		},
		//{
		//	name: "case 4 use default cluster id if env not defined",
		//	fields: fields{
		//		usedPodRecordJson: mockUsedPodRecordJson,
		//		redisKey:          mockDefaultPodUsedRecordRedisKey,
		//	},
		//	expected: expected{
		//		jsonString: mockUsedPodRecordJson,
		//		expiration: expirationTime,
		//	},
		//	patchesFunc: func() mockUtils.PatchSlice {
		//		patches := mockUtils.InitPatchSlice()
		//		patches.Append(mockUtils.PatchSlice{
		//			ApplyFunc(config.GetConfig, mockGetConfigFunc),
		//			ApplyFunc(os.Getenv,
		//				func(str string) string {
		//					return ""
		//				},
		//			),
		//			ApplyFunc((*redisclient.Client).HSet,
		//				func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
		//					mockRedis[key] = values[1].(string)
		//					return intCmd
		//				},
		//			),
		//			ApplyFunc((*redisclient.Client).Expire,
		//				func(_ *redisclient.Client, ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
		//					mockRedisExpiration[key] = expiration
		//					return boolCmd
		//				},
		//			),
		//			ApplyFunc(time.Now,
		//				func() time.Time {
		//					return mockTime
		//				},
		//			),
		//			ApplyFunc(monitoring.GetRedisClient,
		//				func() *redisclient.Client {
		//					return &redisclient.Client{}
		//				},
		//			),
		//		})
		//		return patches
		//	},
		//},
		{
			name: "case 6 fail to upload data to Redis",
			fields: fields{
				usedPodRecordJson: mockUsedPodRecordJson,
				redisKey:          mockDefaultPodUsedRecordRedisKey,
				isException:       true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*redisclient.Client).HSet,
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							mockRedis[key] = values[1].(string)
							intCmd.SetErr(errors.New("mock error"))
							return intCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						},
					),
				})
				return patches
			},
		}, {
			name: "case 7 key and hash key has been exiting in Redis",
			fields: fields{
				usedPodRecordJson: mockUsedPodRecordJson,
				redisKey:          mockDefaultPodUsedRecordRedisKey,
				isException:       true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc((*redisclient.Client).HSet,
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							mockRedis[key] = values[1].(string)
							intCmd.SetErr(nil)
							intCmd.SetVal(0)
							return intCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						},
					),
				})
				return patches
			},
		}, {
			name: "case 8 fail to set expiration time",
			fields: fields{
				usedPodRecordJson: mockUsedPodRecordJson,
				redisKey:          mockDefaultPodUsedRecordRedisKey,
				isException:       true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).HSet,
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							mockRedis[key] = values[1].(string)
							intCmd.SetErr(nil)
							intCmd.SetVal(1)
							return intCmd
						},
					),
					ApplyFunc((*redisclient.Client).Expire,
						func(_ *redisclient.Client, ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
							mockRedisExpiration[key] = expiration
							boolCmd.SetErr(errors.New("mock error"))
							return boolCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						},
					),
				})
				return patches
			},
		}, {
			name: "case 9 fail to set expiration time",
			fields: fields{
				usedPodRecordJson: mockUsedPodRecordJson,
				redisKey:          mockDefaultPodUsedRecordRedisKey,
				isException:       true,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).HSet,
						func(_ *redisclient.Client, ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
							mockRedis[key] = values[1].(string)
							intCmd.SetErr(nil)
							intCmd.SetVal(1)
							return intCmd
						},
					),
					ApplyFunc((*redisclient.Client).Expire,
						func(_ *redisclient.Client, ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
							mockRedisExpiration[key] = expiration
							boolCmd.SetErr(nil)
							boolCmd.SetVal(false)
							return boolCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient,
						func() *redisclient.Client {
							return &redisclient.Client{}
						},
					),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := newPodUsedRecord()
			p.usedPodRecordJSON = tt.fields.usedPodRecordJson
			p.uploadUsedPodRecordJSON()
			if !tt.fields.isException && !reflect.DeepEqual(mockRedis[tt.fields.redisKey], tt.expected.jsonString) {
				t.Errorf("reset used pod record json string: actual = %s, want %s",
					p.usedPodRecordJSON, tt.expected.jsonString)
			}
			if !tt.fields.isException &&
				!reflect.DeepEqual(mockRedisExpiration[tt.fields.redisKey], tt.expected.expiration) {
				t.Errorf("reset used pod record json string: actual = %s, want %s",
					p.usedPodRecordJSON, tt.expected.jsonString)
			}
			patches.ResetAll()
		})
	}
}

func TestGenerateScalingPlanPeriodically(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				PoolPredictionResultDownloadInterval: 10,
			},
		}, nil
	}
	type fields struct {
		receiveChanIsClosed bool
	}
	type args struct {
		stopCh chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		args        args
	}{
		{
			name:   "case 1 check stopCh is nil",
			fields: fields{},
			args:   args{stopCh: nil},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 2 goroutine return when receive chan is closed",
			fields: fields{
				receiveChanIsClosed: true,
			},
			args: args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		}, {
			name:   "case 3 succeed to generate scaling plan",
			fields: fields{},
			args:   args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					ApplyFunc((*PoolMgr).getPoolPredictionResultFromRedis,
						func(_ *PoolMgr) (map[standard][]float64, error) { return nil, nil },
					),
					ApplyFunc((*PoolMgr).generateScalingPlan,
						func(_ *PoolMgr, result map[standard][]float64) error { return nil },
					),
				})
				return patches
			},
		}, {
			name:   "case 4 fail to generate scaling plan",
			fields: fields{},
			args:   args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					ApplyFunc((*PoolMgr).getPoolPredictionResultFromRedis,
						func(_ *PoolMgr) (map[standard][]float64, error) { return nil, errors.New("mock error") },
					),
					ApplyFunc((*PoolMgr).generateScalingPlan,
						func(_ *PoolMgr, result map[standard][]float64) error { return nil },
					),
				})
				return patches
			},
		}, {
			name:   "case 5 fail to generate scaling plan because no valid prediction result can be used",
			fields: fields{},
			args:   args{stopCh: make(chan struct{})},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc(time.NewTicker, func(d time.Duration) *time.Ticker {
						c := make(chan time.Time, 1)
						t := &time.Ticker{
							C: c,
						}
						c <- time.Now()
						return t
					}),
					ApplyFunc((*PoolMgr).getPoolPredictionResultFromRedis,
						func(_ *PoolMgr) (map[standard][]float64, error) { return nil, nil },
					),
					ApplyFunc((*PoolMgr).generateScalingPlan,
						func(_ *PoolMgr, result map[standard][]float64) error { return errors.New("mock error") },
					),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			poolManager := &PoolMgr{}
			go poolManager.generateScalingPlanPeriodically(tt.args.stopCh)
			if tt.args.stopCh != nil {
				if tt.fields.receiveChanIsClosed {
					close(tt.args.stopCh)
				}
			}
			time.Sleep(time.Millisecond * 100)
			patches.ResetAll()
		})
	}
}

func TestGetPoolPredictionResultFromRedis(t *testing.T) {
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				PoolPredictionResultLength: 30,
			},
		}, nil
	}
	strCmd := &redis.StringCmd{}
	redisErr := errors.New("mock get Redis error")
	marshalErr := errors.New("mock unmarshal error")
	predictionResultInvalidErr := errors.New("fail to deserialize redis json to prediction type")
	predictionResultTimeWindowErr := errors.New("prediction result can not cover current timeline")
	mockClusterId := "fgs-yuanrong-az1-01"
	mockTime := time.Now()
	mockTimeNow := func() time.Time {
		return mockTime
	}
	mockGetEnv := func(str string) string {
		return mockClusterId
	}
	mockRedisClient := func() *redisclient.Client {
		return &redisclient.Client{}
	}
	type expected struct {
		err    error
		result map[standard][]float64
	}
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		expected    expected
	}{
		{
			name: "case 1 fail to get cluster id from env and use default value",
			expected: expected{
				err: nil,
				result: map[standard][]float64{
					"pool22-300-128": {20, 21, 32},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							minuteTime := mockTime.Unix() / 60
							strCmd.SetErr(nil)
							predictionResult := `, "result":{"pool22-300-128": [20,21,32]}}`
							strCmd.SetVal(`{"start_time": ` + strconv.Itoa(int(minuteTime)) + predictionResult)
							return strCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(time.Now, mockTimeNow),
				})
				return patches
			},
		}, {
			name: "case 2 return error when can not get result from Redis",
			expected: expected{
				err: redisErr,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							strCmd.SetErr(redisErr)
							return strCmd
						},
					),
					ApplyFunc(os.Getenv, mockGetEnv),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
				})
				return patches
			},
		}, {
			name: "case 3 return error when fail to unmarshal prediction result",
			expected: expected{
				err: marshalErr,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							strCmd.SetErr(nil)
							strCmd.SetVal("")
							return strCmd
						},
					),
					ApplyFunc(os.Getenv, mockGetEnv),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return marshalErr
						},
					),
				})
				return patches
			},
		}, {
			name: "case 4 return error when prediction result is invalid",
			expected: expected{
				err: predictionResultInvalidErr,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							strCmd.SetErr(nil)
							strCmd.SetVal("{}")
							return strCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						},
					),
					ApplyFunc(os.Getenv, mockGetEnv),
				})
				return patches
			},
		}, {
			name: "case 5 return error when prediction result can not cover current time window",
			expected: expected{
				err: predictionResultTimeWindowErr,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							minuteTime := mockTime.Unix()/60 - 30
							strCmd.SetErr(nil)
							predictionResult := `, "result":{"pool22-300-128": [20,21,32]}}`
							strCmd.SetVal(`{"start_time": ` + strconv.Itoa(int(minuteTime)) + predictionResult)
							return strCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(os.Getenv, mockGetEnv),
					ApplyFunc(time.Now, mockTimeNow),
				})
				return patches
			},
		}, {
			name: "case 6 align prediction result to current time window",
			expected: expected{
				err: nil,
				result: map[standard][]float64{
					"pool22-300-128": {20, 21, 32},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							minuteTime := mockTime.Unix() / 60
							strCmd.SetErr(nil)
							predictionResult := `, "result":{"pool22-300-128": [20,21,32]}}`
							strCmd.SetVal(`{"start_time": ` + strconv.Itoa(int(minuteTime)) + predictionResult)
							return strCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(time.Now, mockTimeNow),
					ApplyFunc(os.Getenv, mockGetEnv),
				})
				return patches
			},
		}, {
			name: "case 7 delete useless key in result map if have",
			expected: expected{
				err:    nil,
				result: map[standard][]float64{"pool22-300-128": {-1}},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							minuteTime := mockTime.Unix() / 60
							strCmd.SetErr(nil)
							predictionResult := `, "result":{"pool22-300-128": [-1]}}`
							strCmd.SetVal(`{"start_time": ` + strconv.Itoa(int(minuteTime)) + predictionResult)
							return strCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(time.Now, mockTimeNow),
					ApplyFunc(os.Getenv, mockGetEnv),
				})
				return patches
			},
		}, {
			name: "case 8 fill prediction result if timestamp of system is earlier than model's output",
			expected: expected{
				err: nil,
				result: map[standard][]float64{
					"pool22-300-128": {10.2, 10.2, 10.2, 20.2},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							minuteTime := mockTime.Unix()/60 + 2
							strCmd.SetErr(nil)
							predictionResult := `, "result":{"pool22-300-128": [10.2,20.2]}}`
							strCmd.SetVal(`{"start_time": ` + strconv.Itoa(int(minuteTime)) + predictionResult)
							return strCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(time.Now, mockTimeNow),
					ApplyFunc(os.Getenv, mockGetEnv),
				})
				return patches
			},
		}, {
			name: "case 9 cut off prediction result if timestamp of model's output is earlier than system",
			expected: expected{
				err: nil,
				result: map[standard][]float64{
					"pool22-300-128": {20.2},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
					ApplyFunc((*redisclient.Client).Get,
						func(_ *redisclient.Client, ctx context.Context, key string) *redis.StringCmd {
							minuteTime := mockTime.Unix()/60 - 2
							strCmd.SetErr(nil)
							predictionResult := `, "result":{"pool22-300-128": [20.2, 10.2,20.2]}}`
							strCmd.SetVal(`{"start_time": ` + strconv.Itoa(int(minuteTime)) + predictionResult)
							return strCmd
						},
					),
					ApplyFunc(monitoring.GetRedisClient, mockRedisClient),
					ApplyFunc(time.Now, mockTimeNow),
					ApplyFunc(os.Getenv, mockGetEnv),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			poolManager := &PoolMgr{}
			predictionResult, err := poolManager.getPoolPredictionResultFromRedis()
			if !reflect.DeepEqual(err, tt.expected.err) {
				t.Errorf("actual %v, want %v", err, tt.expected.err)
			}
			if err == nil && !reflect.DeepEqual(predictionResult, tt.expected.result) {
				t.Errorf("actual %v, want %v", predictionResult, tt.expected.result)
			}
			patches.ResetAll()
		})
	}
}

func TestGenerateScalingPlan(t *testing.T) {
	mockStandard := standard("pool22-300-128")
	mockStandard23 := standard("pool23-400-256")
	mockStandard24 := standard("pool24-600-512")
	mockSSDStandard24 := standard("pool24-600-512-ssd")
	generateScalingPlanErr := errors.New("no scaling plan generated because of invalid prediction result ")
	type args struct {
		predictionResults map[standard][]float64
		poolListSc        map[standard]*ScalingConfig
	}
	type expected struct {
		err                  error
		result               map[standard][]int64
		scalingPlanAvailable map[standard]bool
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		expected    expected
	}{
		{
			name: "case 1 no valid prediction result will return directly",
			args: args{
				predictionResults: nil,
			},
			expected: expected{
				err: generateScalingPlanErr,
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 2 return directly if proactive mode is false",
			args: args{
				poolListSc: map[standard]*ScalingConfig{
					mockStandard: {
						proactiveMode: 0,
					},
				},
				predictionResults: map[standard][]float64{
					mockStandard: {12.14, 12.4},
				},
			},
			expected: expected{
				err: nil,
				result: map[standard][]int64{
					mockStandard: nil,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, func() (*config.FaasConfig, error) {
						return nil, errors.New("test")
					}),
				})
				return patches
			},
		}, {
			name: "case 3 reset scaling plan and available flag if prediction result is invalid",
			args: args{
				poolListSc: map[standard]*ScalingConfig{
					mockStandard: {
						proactiveMode: 1,
					},
				},
				predictionResults: map[standard][]float64{
					mockStandard: nil,
				},
			},
			expected: expected{
				err: nil,
				result: map[standard][]int64{
					mockStandard: nil,
				},
				scalingPlanAvailable: map[standard]bool{
					mockStandard: false,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
		}, {
			name: "case 4 succeed to generate scaling plan",
			args: args{
				poolListSc: map[standard]*ScalingConfig{
					mockStandard: {
						proactiveMode: 1,
						replicas:      20,
					},
					mockStandard23: {
						proactiveMode: 1,
						replicas:      30,
					},
					mockStandard24: {
						proactiveMode: 1,
						replicas:      100,
					},
				},
				predictionResults: map[standard][]float64{
					mockStandard:      {10.2, 12.2, 15, 19, 26, 25},
					mockStandard23:    {70, 55, 35, 49, 36, 100},
					mockStandard24:    {10.2, 12.2, 15, 19, 26, 25},
					mockSSDStandard24: {0, 0, 0, 0, 0, 0, 0},
				},
			},
			expected: expected{
				err: nil,
				result: map[standard][]int64{
					mockStandard:   {36, 36, 36, 36, 36, 34},
					mockStandard23: {60, 60, 60, 60, 60, 90},
					mockStandard24: {70, 70, 70, 70, 70, 40},
				},
				scalingPlanAvailable: map[standard]bool{
					mockStandard:   true,
					mockStandard23: true,
					mockStandard24: true,
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig,
						func() (*config.FaasConfig, error) {
							return &config.FaasConfig{
								PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
									ScalingPlanTimeWindow: 5,
									ScalingPlanFactor:     1.35,
									MaxScalingStep:        30,
								},
							}, nil
						},
					),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			poolManager := &PoolMgr{}
			poolManager.poolList = map[standard]*Pool{}
			for k := range tt.args.poolListSc {
				poolManager.poolList[k] = &Pool{}
				poolManager.poolList[k].scalingConfig = tt.args.poolListSc[k]
				poolManager.poolList[k].scalingConfig.standard = k
			}
			err := poolManager.generateScalingPlan(tt.args.predictionResults)
			if !reflect.DeepEqual(err, tt.expected.err) {
				t.Errorf("actual %v, want %v", err, tt.expected.err)
				return
			}
			for k, v := range poolManager.poolList {
				curPoolSc := v.scalingConfig
				if !reflect.DeepEqual(curPoolSc.scalingPlan, tt.expected.result[k]) {
					t.Errorf("%s actual %v, want %v",
						curPoolSc.standard, curPoolSc.scalingPlan, tt.expected.result[k])
				}
			}
			patches.ResetAll()
		})
	}
}

func TestPoolMgrProcessPoolProactiveModeUpdate(t *testing.T) {
	mockProactiveElasticScalingConfig := &config.ElasticScalingConfig{}
	mockGetConfigFunc := func() (*config.FaasConfig, error) {
		return &config.FaasConfig{
			ProactiveEnableList: map[string]bool{
				"pool22": true,
				"pool23": false,
				"pool24": true,
			},
			ProactiveElasticScalingConfig: mockProactiveElasticScalingConfig,
			PoolMgrProactiveConfig: &config.PoolManagerProactiveElasticConfig{
				EnableFusionPolicy:            1,
				ScalingUpTriggerTimeThreshold: 3,
			},
		}, nil
	}
	type fields struct {
		poolList map[standard]*Pool
	}
	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		expected    map[standard]*Pool
	}{
		{
			name: "succeed to update scaling mode for all pools",
			fields: fields{
				poolList: map[standard]*Pool{
					"pool22": {
						scalingConfig: &ScalingConfig{
							elasticScalingCfg:         config.GetElasticScalingConfig(),
							scalingPlan:               nil,
							enableFusionPolicy:        0,
							scalingUpTriggerCounter:   0,
							scalingUpTriggerThreshold: 3,
							cooldownPeriod:            15,
							proactiveMode:             0,
						},
					},
					"pool23": {
						scalingConfig: &ScalingConfig{
							elasticScalingCfg:         config.GetElasticScalingConfig(),
							scalingPlan:               nil,
							enableFusionPolicy:        0,
							scalingUpTriggerCounter:   0,
							scalingUpTriggerThreshold: 3,
							cooldownPeriod:            15,
							proactiveMode:             0,
						},
					},
					"pool24": {
						scalingConfig: &ScalingConfig{
							elasticScalingCfg:         config.GetElasticScalingConfig(),
							scalingPlan:               nil,
							enableFusionPolicy:        0,
							scalingUpTriggerCounter:   0,
							scalingUpTriggerThreshold: 3,
							cooldownPeriod:            15,
							proactiveMode:             0,
						},
					},
				},
			},
			expected: map[standard]*Pool{
				"pool22": {
					scalingConfig: &ScalingConfig{
						elasticScalingCfg:         mockProactiveElasticScalingConfig,
						scalingPlan:               nil,
						enableFusionPolicy:        1,
						scalingUpTriggerCounter:   0,
						scalingUpTriggerThreshold: 3,
						cooldownPeriod:            0,
						proactiveMode:             1,
					},
				},
				"pool23": {
					scalingConfig: &ScalingConfig{
						elasticScalingCfg:         config.GetElasticScalingConfig(),
						scalingPlan:               nil,
						enableFusionPolicy:        1,
						scalingUpTriggerCounter:   0,
						scalingUpTriggerThreshold: 3,
						cooldownPeriod:            0,
						proactiveMode:             0,
					},
				},
				"pool24": {
					scalingConfig: &ScalingConfig{
						elasticScalingCfg:         mockProactiveElasticScalingConfig,
						scalingPlan:               nil,
						enableFusionPolicy:        1,
						scalingUpTriggerCounter:   0,
						scalingUpTriggerThreshold: 3,
						cooldownPeriod:            0,
						proactiveMode:             1,
					},
				},
			},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					ApplyFunc(config.GetConfig, mockGetConfigFunc),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			p := &PoolMgr{
				poolList: tt.fields.poolList,
			}
			p.ProcessPoolProactiveModeUpdate()

			for k, v := range p.poolList {
				fmt.Printf("actual %v, want %v\n", v.scalingConfig, tt.expected[k].scalingConfig)
				if !reflect.DeepEqual(v.scalingConfig, tt.expected[k].scalingConfig) {
					t.Errorf("actual %v, want %v", v.scalingConfig, tt.expected[k].scalingConfig)
				}
			}
			patches.ResetAll()
		})
	}
}
