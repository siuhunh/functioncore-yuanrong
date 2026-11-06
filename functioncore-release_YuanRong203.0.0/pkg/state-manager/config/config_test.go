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
	"functioncore/pkg/common/types/bridgetypes"
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
		// test stateConfig siteClusters
		{"case3",
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(_ string) string {
						return `{"clusterID": "testCluster001"}`
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
				})
				return patches
			},
			StateCounterCfg{ClusterID: "testCluster001", SiteClusters: []string{"testCluster001"}},
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

func TestGetCAERouterEtcdConfig(t *testing.T) {
	convey.Convey("Test get cae router etcd config", t, func() {
		config := etcd3.ClientConfig{
			ServerList: []string{"1.2.3.4:5678"},
		}
		configBytes, _ := json.Marshal(config)
		configStr := string(configBytes)
		convey.Convey("Case1: success", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return configStr
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			getConfig := getCAERouterEtcdConfig()
			convey.So(len(getConfig.ServerList), convey.ShouldEqual, 1)
			convey.So(getConfig.ServerList[0], convey.ShouldEqual, "1.2.3.4:5678")
		})
		convey.Convey("Case2: env is not json string", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "error"
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			getConfig := getCAERouterEtcdConfig()
			convey.So(getConfig, convey.ShouldBeNil)
		})
		convey.Convey("Case3: failed to validate", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return configStr
				}),
				gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
					return false, errors.New("error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			getConfig := getCAERouterEtcdConfig()
			convey.So(getConfig, convey.ShouldBeNil)
		})
	})
}

func TestGetCAEMetaEtcdConfig(t *testing.T) {
	convey.Convey("Test get cae meta etcd config", t, func() {
		config := etcd3.ClientConfig{
			ServerList: []string{"1.2.3.4:5678"},
		}
		configBytes, _ := json.Marshal(config)
		configStr := string(configBytes)
		convey.Convey("Case1: success", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return configStr
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			getConfig := getCAEMetaEtcdConfig()
			convey.So(len(getConfig.ServerList), convey.ShouldEqual, 1)
			convey.So(getConfig.ServerList[0], convey.ShouldEqual, "1.2.3.4:5678")
		})
		convey.Convey("Case2: env is not json string", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return "error"
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			getConfig := getCAEMetaEtcdConfig()
			convey.So(getConfig, convey.ShouldBeNil)
		})
		convey.Convey("Case3: failed to validate", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return configStr
				}),
				gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
					return false, errors.New("error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			getConfig := getCAEMetaEtcdConfig()
			convey.So(getConfig, convey.ShouldBeNil)
		})
	})
}

func Test_TransToClusterTenantHashTagKey(t *testing.T) {
	tests := []struct {
		name              string
		patchesFunc       utils.PatchesFunc
		testTenantCluster []string
	}{
		{"case1", func() utils.PatchSlice {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
				gomonkey.ApplyGlobalVar(&clusterKeyByHashTag, make([]string, 0)),
			})
			return patches
		},
			[]string{"test1", "test2", "test3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			TransToClusterTenantHashTagKey(tt.testTenantCluster)
			if len(clusterKeyByHashTag) != 3 {
				t.Errorf("error, real clusterKeyByHashTag: %v", clusterKeyByHashTag)
			}
			patches.ResetAll()
		})
	}
}

func Test_hotScaleDataConfig(t *testing.T) {
	type args struct {
		config    *FConfig
		newConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"test hot load scaleData config",
			args{
				config: &FConfig{
					ScaleData: ScaleData{},
				},
				newConfig: &HotLoadConfig{ScaleData: ScaleData{
					SyncPeriod:             1,
					ScaleUpTolerance:       1,
					ScaleDownTolerance:     1,
					TickerTime:             1,
					DownscaleStabilization: 1,
					ScaleRules:             map[string]map[string]int64{},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotScaleDataConfig(tt.args.config, tt.args.newConfig)
			if tt.args.config.ScaleData.SyncPeriod != 1 || tt.args.config.ScaleData.ScaleUpTolerance != 1 {
				t.Errorf("failed to test hotScaleDataConfig")
			}
		})
	}
}

func Test_hotBridgeManagerConfig(t *testing.T) {
	type args struct {
		config    *FConfig
		newConfig *HotLoadConfig
	}
	tests := []struct {
		name string
		args args
		want bridgetypes.BridgeManagerConfig
	}{
		{
			name: "test new config is nil",
			args: args{
				config: &FConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						PullInstanceTimeSpan: 5,
					},
				},
			},
			want: bridgetypes.BridgeManagerConfig{
				PullInstanceTimeSpan: 5,
			},
		},
		{
			name: "test pullInstanceTimeSpan negative",
			args: args{
				config: &FConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						PullInstanceTimeSpan: 5,
					},
				},
				newConfig: &HotLoadConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						PullInstanceTimeSpan: -5,
					},
				},
			},
			want: bridgetypes.BridgeManagerConfig{
				PullInstanceTimeSpan: 30,
			},
		},
		{
			name: "test pullInstanceTimeSpan valid",
			args: args{
				config: &FConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						PullInstanceTimeSpan: 5,
					},
				},
				newConfig: &HotLoadConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						PullInstanceTimeSpan: 15,
					},
				},
			},
			want: bridgetypes.BridgeManagerConfig{
				PullInstanceTimeSpan: 15,
			},
		},
		{
			name: "test pullInstanceTimeSpan valid",
			args: args{
				config: &FConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						BridgeFuncWhiteList: make(map[string]bridgetypes.SpecificBridgeScaleData),
					},
				},
				newConfig: &HotLoadConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						PullInstanceTimeSpan: 15,
					},
				},
			},
			want: bridgetypes.BridgeManagerConfig{
				PullInstanceTimeSpan: 15,
			},
		},
		{
			name: "test BridgeFuncWhiteList valid",
			args: args{
				config: &FConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{},
				},
				newConfig: &HotLoadConfig{
					BridgeManagerConfig: bridgetypes.BridgeManagerConfig{
						BridgeFuncWhiteList: map[string]bridgetypes.SpecificBridgeScaleData{
							"funcSvc": {
								ScaleDownTolerance: 0.5,
								ScaleUpTolerance:   0.2,
								ScaleRules: map[string]map[string]int64{
									"cache": {
										"cpu":         30,
										"memory":      40,
										"connections": 40}},
							},
						},
					},
				},
			},
			want: bridgetypes.BridgeManagerConfig{
				BridgeFuncWhiteList: map[string]bridgetypes.SpecificBridgeScaleData{
					"funcSvc": {
						ScaleDownTolerance: 0.5,
						ScaleUpTolerance:   0.2,
						ScaleRules: map[string]map[string]int64{
							"cache": {
								"cpu":         30,
								"memory":      40,
								"connections": 40}},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotBridgeManagerConfig(tt.args.config, tt.args.newConfig)
			if tt.args.config.BridgeManagerConfig.PullInstanceTimeSpan != tt.want.PullInstanceTimeSpan {
				t.Errorf("failed to test hotBridgeManagerConfig")
			}
			if tt.args.config.BridgeManagerConfig.BridgeFuncWhiteList["funcSvc"].ScaleUpTolerance != tt.want.BridgeFuncWhiteList["funcSvc"].ScaleUpTolerance {
				t.Errorf("failed to test hotBridgeManagerConfig")
			}
			if tt.args.config.BridgeManagerConfig.BridgeFuncWhiteList["funcSvc"].ScaleDownTolerance != tt.want.BridgeFuncWhiteList["funcSvc"].ScaleDownTolerance {
				t.Errorf("failed to test hotBridgeManagerConfig")
			}
			if len(tt.args.config.BridgeManagerConfig.BridgeFuncWhiteList["funcSvc"].ScaleRules) != len(tt.want.BridgeFuncWhiteList["funcSvc"].ScaleRules) {
				t.Errorf("failed to test hotBridgeManagerConfig")
			}
		})
	}
}

func Test_hotLoadTenantConfig(t *testing.T) {
	type args struct {
		newConfig *HotLoadConfig
	}
	var normalTenantConfig args
	tenantConfig := TenantConfig{
		TenantStateLimit: 1000,
	}
	newConf := &HotLoadConfig{
		TenantConfig: tenantConfig,
	}
	normalTenantConfig.newConfig = newConf

	var nilTenantConfig args
	zeroTenantConfig := TenantConfig{}
	newZeroConf := &HotLoadConfig{
		TenantConfig: zeroTenantConfig,
	}
	nilTenantConfig.newConfig = newZeroConf

	tests := []struct {
		name string
		args args
	}{
		{"case1", normalTenantConfig},
		{"case2", nilTenantConfig},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hotLoadTenantConfig(tt.args.newConfig)
			if tt.args.newConfig.TenantConfig.TenantStateLimit == 0 {
				if reflect.DeepEqual(tt.args.newConfig.TenantConfig.TenantStateLimit, TenantStateLimit) {
					t.Errorf("TenantStateLimit shouldn't be 0 or nil")
				}
			} else if !reflect.DeepEqual(tt.args.newConfig.TenantConfig.TenantStateLimit, TenantStateLimit) {
				t.Errorf("hot load tenant config fail, new value: %v, old value: %v",
					tt.args.newConfig.TenantConfig.TenantStateLimit, TenantStateLimit)
			}
		})
	}
}
