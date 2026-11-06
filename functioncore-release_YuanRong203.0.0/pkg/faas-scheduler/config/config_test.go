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

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/monitor"
	mockUtils "functioncore/test/utils"
)

var (
	testConfig = &Config{
		Runtime: &RuntimeConfig{
			ServicePort: "8889",
		},
		AlarmServerAddress: "127.0.0.1:28915",
	}
)

func mockReadFile(_ string) ([]byte, error) {
	data, err := json.Marshal(testConfig)
	return data, err
}

func TestInitConfig(t *testing.T) {
	convey.Convey("TestInitConfig", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(readHotLoadConfig, func(configPath string) (*HotLoadConfig, error) {
				return nil, nil
			}),
			gomonkey.ApplyFunc(setHotLoadConfig, func(config *Config, hotLoadConfig *HotLoadConfig) {
				return
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		convey.So(InitConfig(), convey.ShouldNotBeNil)

		setEnv()
		convey.So(InitConfig(), convey.ShouldBeNil)

		convey.So(FConfig.Runtime.ServicePort, convey.ShouldEqual, testConfig.Runtime.ServicePort)
		convey.So(FConfig.AlarmServerAddress, convey.ShouldEqual, testConfig.AlarmServerAddress)
	})
}

func setEnv() {
	configJSON := `{
			"etcd": {
			"servers": ["127.0.0.1:32379"],
			"user": "***",
			"password": "***",
			"cafile": "/home/sn/resource/ca/ca.pem",
			"certfile": "/home/sn/resource/ca/cert.pem",
			"keyfile": "/home/sn/resource/ca/key.pem",
			"sslEnable": false
			},
			"routerETCD": {
			"servers": ["http://127.0.0.1:32379"],
			"user": "***",
			"password": "***",
			"cafile": "/home/sn/resource/routerEtcd/ca.pem",
			"certfile": "/home/sn/resource/routerEtcd/cert.pem",
			"keyfile": "/home/sn/resource/routerEtcd/key.pem",
			"sslEnable": false
			},
			"localAuth": {
			"aKey": "***",
			"sKey": "***",
			"duration": 5
			},
			"httpsConfig": {
			"httpsEnable": false,
			"tlsProtocol": "TLSv1.2",
			"tlsCiphers": "***"
			},
			"alarmServerAddress": "127.0.0.1:28915"
		}`
	os.Setenv(configEnvKey, configJSON)
}

func Test_readHotLoadConfig(t *testing.T) {
	convey.Convey("Test_readHotLoadConfig", t, func() {
		convey.Convey("failed to read file", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return nil, errors.New("failed to read file")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			_, err := readHotLoadConfig(filePath)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to read file")
		})
		convey.Convey("failed to json Unmarshal", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return nil, nil
					}),
				gomonkey.ApplyFunc(json.Unmarshal,
					func(data []byte, v interface{}) error {
						return errors.New("failed to json unmarshal")
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			_, err := readHotLoadConfig(filePath)
			convey.So(err, convey.ShouldNotEqual, nil)
			convey.So(err.Error(), convey.ShouldEqual, "failed to json unmarshal")
		})
		convey.Convey("success", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return nil, nil
					}),
				gomonkey.ApplyFunc(json.Unmarshal,
					func(data []byte, v interface{}) error {
						return nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()

			_, err := readHotLoadConfig(filePath)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestSetHotLoadConfig(t *testing.T) {
	convey.Convey("TestSetHotLoadConfig", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		oldCfg := &Config{EtcdLeaseConfig: &EtcdLeaseConfig{
			LeaseTTL: 20,
			RenewTTL: 20,
		}}
		cfg := &HotLoadConfig{EtcdLeaseConfig: &EtcdLeaseConfig{
			LeaseTTL: 10,
			RenewTTL: 10,
		}}
		setHotLoadConfig(oldCfg, cfg)
		convey.So(cfg.EtcdLeaseConfig.LeaseTTL, convey.ShouldEqual, 10)
		convey.So(cfg.EtcdLeaseConfig.RenewTTL, convey.ShouldEqual, 10)
	})
}

func TestWatchConfig(t *testing.T) {
	type args struct {
		stopCh   <-chan struct{}
		callback ChangedCallback
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case_01 normal scenario",
			args: args{make(<-chan struct{}), func() {}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		}, {
			name: "case_02 abnormal scenario",
			args: args{make(<-chan struct{}), func() {}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(monitor.CreateFileWatcher,
						func(stopCh <-chan struct{}) (monitor.FileWatcher, error) {
							return nil, fmt.Errorf("error")
						}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := WatchConfig(tt.args.stopCh, tt.args.callback); (err != nil) != tt.wantErr {
				t.Errorf("WatchConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

func Test_hotLoadETCDConfig(t *testing.T) {
	type args struct {
		config    *Config
		newConfig *HotLoadConfig
	}
	var a args
	conf := &Config{
		Runtime:              nil,
		Etcd:                 nil,
		RouterETCD:           nil,
		EtcdLeaseConfig:      nil,
		LocalAuth:            nil,
		InstanceLeaseConfig:  nil,
		HTTPSConfig:          nil,
		AlarmServerAddress:   "",
		AuthenticationEnable: false,
	}
	etcdConf := &etcd3.ClientConfig{
		ServerList: []string{"100.100.100.100"},
		User:       "",
		Password:   "",
		CaFile:     "",
		CertFile:   "",
		KeyFile:    "",
		SslEnable:  false,
	}
	newConf := &HotLoadConfig{
		EtcdLeaseConfig: nil,
		Etcd:            nil,
	}
	conf.Etcd = etcdConf
	newConf.Etcd = etcdConf
	a.config = conf
	a.newConfig = newConf
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
		hotLoadETCDConfig(tt.args.config, tt.args.newConfig)
	}
}

func Test_hotLoadLeaseConfig(t *testing.T) {
	type args struct {
		config    *Config
		newConfig *HotLoadConfig
	}
	var a args
	conf := &Config{
		Runtime:              nil,
		Etcd:                 nil,
		RouterETCD:           nil,
		EtcdLeaseConfig:      nil,
		LocalAuth:            nil,
		InstanceLeaseConfig:  nil,
		HTTPSConfig:          nil,
		AlarmServerAddress:   "",
		AuthenticationEnable: false,
	}
	etcdLeaseConfig := &EtcdLeaseConfig{
		LeaseTTL: 1,
		RenewTTL: 1,
	}
	newConf := &HotLoadConfig{
		EtcdLeaseConfig: nil,
		Etcd:            nil,
	}
	conf.EtcdLeaseConfig = etcdLeaseConfig
	newConf.EtcdLeaseConfig = etcdLeaseConfig
	a.config = conf
	a.newConfig = newConf
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
		hotLoadLeaseConfig(tt.args.config, tt.args.newConfig)
	}
}

func Test_hotLoadConfig(t *testing.T) {
	type args struct {
		filename string
		opType   monitor.OpType
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "readHotLoadConfig error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig, func(_ string) (*HotLoadConfig, error) {
						return nil, fmt.Errorf("mocke error")
					}),
				})
				return patches
			},
		}, {
			name: "govalidator.ValidateStruct error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig, func(_ string) (*HotLoadConfig, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(_ interface{}) (bool, error) {
						return true, fmt.Errorf("mocke error")
					}),
				})
				return patches
			},
		}, {
			name: "normal path",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig, func(_ string) (*HotLoadConfig, error) {
						return nil, nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(_ interface{}) (bool, error) {
						return true, nil
					}),
					gomonkey.ApplyFunc(setHotLoadConfig, func(config *Config, hotLoadConfig *HotLoadConfig) {
						return
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			hotLoadConfig(tt.args.filename, tt.args.opType)
			patches.ResetAll()
		})
	}
}
