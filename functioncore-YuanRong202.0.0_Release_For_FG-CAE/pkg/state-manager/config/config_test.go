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

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/state-manager/constant"
	"functioncore/test/utils"
)

func TestGetConfig(t *testing.T) {
	tests := []struct {
		name        string
		fcConfig    *FConfig
		patchedFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test readFile error
		{
			"case1",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte{}, errors.New("read file error")
					}),
				})
				return patches
			},
			true,
		},
		// test json Unmarshal err
		{
			"case2",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte{}, nil
					}),
				})
				return patches
			},
			true,
		},
		// test json Unmarshal err
		{
			"case3",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("{\"address\":}"), nil
					}),
				})
				return patches
			},
			true,
		},
		// test json Unmarshal success
		{
			"case4",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("{\"address\":\"127.0.0.1\"}"), nil
					}),
				})
				return patches
			},
			true,
		},
		// get success result
		{
			"case5",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("{\"address\":\"127.0.0.1\"}"), nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
					gomonkey.ApplyGlobalVar(&kubeCfgTimeout, "10"),
				})
				return patches
			},
			false,
		},
		// get error result
		{
			"case5",
			nil,
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("{\"address\":\"127.0.0.1\"}"), nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, errors.New("validate error")
					}),
					gomonkey.ApplyGlobalVar(&kubeCfgTimeout, "10"),
				})
				return patches
			},
			true,
		},
		// get old instance
		{
			"case6",
			&FConfig{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			false,
		},
	}
	temp := FConfigs
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			FConfigs = tt.fcConfig
			patches := tt.patchedFunc()
			_, err := GetConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
	FConfigs = temp
}

func TestGetRealTimeMetricsServerAddr(t *testing.T) {
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		wantErr     bool
	}{
		// test read file error
		{
			"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte{}, errors.New("read file error")
					}),
				})
				return patches
			},
			true,
		},
		// test json Unmarshal error
		{
			"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("{qwe}"), nil
					}),
				})
				return patches
			},
			true,
		},
		// test ValidateStruct error
		{
			"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("{\"address\":\"127.0.0.1\"}"), nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("validate error")
					}),
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("unmarshal error")
					}),
				})
				return patches
			},
			true,
		},
		// test success
		{
			"case4",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("{\"address\":\"127.0.0.1\"}"), nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, nil
					}),
				})
				return patches
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			_, _, err := GetHotloadMetricsConf()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetHotloadMetricsConf() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_getEtcdConfig(t *testing.T) {
	_ = os.Setenv(constant.Etcd, "{\"user\":\"root\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        *etcd3.ClientConfig
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("etcd unmarshal error")
					}),
				})
				return patches
			},
			nil,
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("etcd validate error")
					}),
				})
				return patches
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getEtcdConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getEtcdConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getMetaEtcdProxyConfig(t *testing.T) {
	_ = os.Setenv(constant.MetaEtcdProxy, "{\"user\":\"root\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        *etcd3.ClientConfig
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("etcd unmarshal error")
					}),
				})
				return patches
			},
			nil,
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("etcd validate error")
					}),
				})
				return patches
			},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getMetaEtcdProxyConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getMetaEtcdProxyConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getLocalAuthConfig(t *testing.T) {
	_ = os.Setenv(constant.LocalAuth, "{\"sKey\":\"root\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        localauth.AuthConfig
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("localAuth unmarshal error")
					}),
				})
				return patches
			},
			localauth.AuthConfig{},
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("localAuth validate error")
					}),
				})
				return patches
			},
			localauth.AuthConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getLocalAuthConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getLocalAuthConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getLogConfig(t *testing.T) {
	_ = os.Setenv(constant.Log, "{\"log_dir\":\"root\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        LogConfig
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("log unmarshal error")
					}),
				})
				return patches
			},
			LogConfig{},
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("log validate error")
					}),
				})
				return patches
			},
			LogConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getLogConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getLogConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getClusterConfig(t *testing.T) {
	_ = os.Setenv(constant.Cluster, "{\"clusterType\":\"single\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        ClusterConfig
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("cluster unmarshal error")
					}),
				})
				return patches
			},
			ClusterConfig{},
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("cluster validate error")
					}),
				})
				return patches
			},
			ClusterConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getClusterConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getClusterConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getSchedulerConfig(t *testing.T) {
	_ = os.Setenv(constant.Scheduler, "{\"clusterType\":\"single\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        SchedulerConfig
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("scheduler unmarshal error")
					}),
				})
				return patches
			},
			SchedulerConfig{},
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("scheduler validate error")
					}),
				})
				return patches
			},
			SchedulerConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getSchedulerConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSchedulerConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getStateCounterCfg(t *testing.T) {
	_ = os.Setenv(constant.StateCounterCfg, "{\"serverAddr\":\"127.0.0.1\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        StateCounterCfg
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("StateCounter unmarshal error")
					}),
				})
				return patches
			},
			StateCounterCfg{},
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("StateCounter validate error")
					}),
				})
				return patches
			},
			StateCounterCfg{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getStateCounterCfg(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getStateCounterCfg() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_getHTTPSConfig(t *testing.T) {
	_ = os.Setenv(constant.HTTPSConfig, "{\"tlsProtocol\":\"127.0.0.1\"}")
	tests := []struct {
		name        string
		patchedFunc utils.PatchesFunc
		want        tls.InternalHTTPSConfig
	}{
		{"case1",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return errors.New("HTTPS unmarshal error")
					}),
				})
				return patches
			},
			tls.InternalHTTPSConfig{},
		},
		{"case2",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("HTTPS validate error")
					}),
				})
				return patches
			},
			tls.InternalHTTPSConfig{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := getHTTPSConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getHTTPSConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func TestWatchConfig(t *testing.T) {
	type args struct {
		stopCh   <-chan struct{}
		callback ChangedCallback
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case_01 normal scenario",
			args: args{make(<-chan struct{}), func() {}},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			wantErr: false,
		}, {
			name: "case_02 abnormal scenario",
			args: args{make(<-chan struct{}), func() {}},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
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

func TestSetHotLoadConfig(t *testing.T) {
	convey.Convey("TestSetHotLoadConfig", t, func() {
		patches := [...]*gomonkey.Patches{}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()

		oldCfg := &FConfig{EtcdCfg: &etcd3.ClientConfig{
			ServerList: []string{"100.100.100.100"},
		}}
		cfg := &HotLoadConfig{Etcd: &etcd3.ClientConfig{
			ServerList: []string{"100.100.100.100"},
		}}
		setHotLoadConfig(oldCfg, cfg)
		convey.So(cfg.Etcd.ServerList, convey.ShouldNotEqual, []string{"100.100.100.100"})
	})
}

func Test_hotLoadETCDConfig(t *testing.T) {
	type args struct {
		config    *FConfig
		newConfig *HotLoadConfig
	}
	var a args
	conf := &FConfig{
		EtcdCfg:            nil,
		RouterETCD:         etcd3.ClientConfig{},
		LogCfg:             LogConfig{},
		LocalAuth:          localauth.AuthConfig{},
		HTTPSConfig:        tls.InternalHTTPSConfig{},
		Address:            "",
		Cluster:            ClusterConfig{},
		ScheCfg:            SchedulerConfig{},
		KubeCfgTimeout:     0,
		StateCounterCfg:    StateCounterCfg{},
		AlarmServerAddress: "",
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
		Etcd: nil,
	}
	conf.EtcdCfg = etcdConf
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

			_, err := readHotLoadConfig(configPath)
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

			_, err := readHotLoadConfig(configPath)
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

			_, err := readHotLoadConfig(configPath)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}
