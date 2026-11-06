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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/monitor"
	"functioncore/pkg/common/tls"
	"functioncore/test/utils"
)

func Test_newCacheConfig(t *testing.T) {
	type args struct {
		path string
	}
	//var a args
	//a.path = "../scripts/cache-manager.conf"
	//b := &CacheManagerConfig{
	//	EtcdConfig: etcd3.ClientConfig{
	//		ServerList: []string{"127.0.0.1:32379"},
	//		User:       "username",
	//		Password:   "password",
	//		CaFile:     "/home/sn/resource/ca/ca.pem",
	//		CertFile:   "/home/sn/resource/ca/cert.pem",
	//		KeyFile:    "/home/sn/resource/ca/key.pem",
	//		SslEnable:  false,
	//	},
	//	S3Config: S3Config{
	//		UseSSL:    false,
	//		AccessKey: "accessKey",
	//		SecretKey: "secretKey",
	//		Endpoint:  "local-cache-service.default.svc.cluster.local:9000",
	//		CaFile:    "",
	//	},
	//}

	var c args
	c.path = "dsa"

	tests := []struct {
		name string
		args args
		want *CacheManagerConfig
	}{
		//{"test1", a, b},
		{"test2", c, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newCacheConfig(tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newCacheConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGetConfig(t *testing.T) {
	tests := []struct {
		name string
		want *CacheManagerConfig
	}{
		{"test1", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_newCacheConfig1(t *testing.T) {
	type args struct {
		path string
	}
	var a args
	a.path = "../scripts/cache-manager.conf"
	//b := &CacheManagerConfig{
	//	EtcdConfig: etcd3.ClientConfig{
	//		ServerList: []string{"127.0.0.1:32379"},
	//		User:       "username",
	//		Password:   "password",
	//		CaFile:     "/home/sn/resource/ca/ca.pem",
	//		CertFile:   "/home/sn/resource/ca/cert.pem",
	//		KeyFile:    "/home/sn/resource/ca/key.pem",
	//		SslEnable:  false,
	//	},
	//	S3Config: S3Config{
	//		UseSSL:    false,
	//		AccessKey: "accessKey",
	//		SecretKey: "secretKey",
	//		Endpoint:  "local-cache-service.default.svc.cluster.local:9000",
	//		CaFile:    "",
	//	},
	//}

	var c args
	c.path = "dsa"

	tests := []struct {
		name  string
		args  args
		want  *CacheManagerConfig
		Setup func(t *testing.T)
	}{
		//{
		//	name: "test1",
		//	args: a,
		//	want: b,
		//},
		{
			name: "test2",
			args: c,
			want: nil,
		},
		{
			name: "test3",
			args: a,
			want: nil,
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(filepath.Abs,
					func(path string) (string, error) {
						fmt.Println("sssdsdssdsds")
						return "", errors.New("")
					})

				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
		{
			name: "test4",
			args: a,
			want: nil,
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(ioutil.ReadAll,
					func(r io.Reader) ([]byte, error) {
						return nil, errors.New("")
					})

				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
		{
			name: "test5",
			args: a,
			want: nil,
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(govalidator.ValidateStruct,
					func(s interface{}) (bool, error) {
						return true, errors.New("")
					})
				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			if got := newCacheConfig(tt.args.path); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newCacheConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_loadHTTPSConfig(t *testing.T) {
	httpsConf1 := &tls.InternalHTTPSConfig{
		HTTPSEnable: true,
	}
	confStr1, _ := json.Marshal(httpsConf1)
	tests := []struct {
		name        string
		want        *tls.InternalHTTPSConfig
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: httpsConf1,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr1)
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: &tls.InternalHTTPSConfig{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
		{
			name: "case3_NotJson",
			want: &tls.InternalHTTPSConfig{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := loadHTTPSConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadHTTPSConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_loadMetaEtcdConfig(t *testing.T) {
	etcdConf1 := &etcd3.ClientConfig{
		User: "aaa",
	}
	confStr1, _ := json.Marshal(etcdConf1)
	tests := []struct {
		name        string
		want        *etcd3.ClientConfig
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: etcdConf1,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr1)
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: nil,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
		{
			name: "case3_NotJson",
			want: nil,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := loadMetaEtcdConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadMetaEtcdConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_loadS3Config(t *testing.T) {
	s3Conf1 := S3Config{
		Endpoint: "127.0.0.1",
	}
	confStr1, _ := json.Marshal(s3Conf1)
	s3Conf2 := S3Config{
		UseSSL: false,
	}
	confStr2, _ := json.Marshal(s3Conf2)
	tests := []struct {
		name        string
		want        S3Config
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: s3Conf1,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr1)
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: S3Config{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
		{
			name: "case3_NotJson",
			want: S3Config{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
		{
			name: "case4_ValidationFailure",
			want: S3Config{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr2)
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := loadS3Config(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadS3Config() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_loadMetaEtcdProxyConfig(t *testing.T) {
	etcdConf1 := &etcd3.ClientConfig{
		User:       "aaa",
		ServerList: []string{"127.0.0.1"},
	}
	etcdConf2 := &etcd3.ClientConfig{
		User: "aaa",
	}
	confStr1, _ := json.Marshal(etcdConf1)
	confStr2, _ := json.Marshal(etcdConf2)
	tests := []struct {
		name        string
		want        *etcd3.ClientConfig
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: etcdConf1,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr1)
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: nil,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
		{
			name: "case3_NotJson",
			want: nil,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
		{
			name: "case4_empty_serverList",
			want: nil,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr2)
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := loadMetaEtcdProxyConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadMetaEtcdProxyConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_loadPersistenceConfig(t *testing.T) {
	persistence := PersistenceConfig{
		Enable: true,
	}
	confStr1, _ := json.Marshal(persistence)
	tests := []struct {
		name        string
		want        PersistenceConfig
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: persistence,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr1)
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: PersistenceConfig{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
		{
			name: "case3_NotJson",
			want: PersistenceConfig{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchedFunc()
			if got := loadPersistenceConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadPersistenceConfig() = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}

func Test_loadLimitCodeSize(t *testing.T) {
	tests := []struct {
		name        string
		want        int
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: 100,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return strconv.Itoa(100)
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: DefaultLimitCodeSize,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
		{
			name: "case3_NotInt",
			want: DefaultLimitCodeSize,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchedFunc()
		t.Run(tt.name, func(t *testing.T) {
			if got := loadLimitCodeSize(); got != tt.want {
				t.Errorf("loadLimitCodeSize() = %v, want %v", got, tt.want)
			}
		})
		patches.ResetAll()
	}
}

func Test_loadAlarmServerAddr(t *testing.T) {
	tests := []struct {
		name        string
		want        string
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: "aaa",
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: "",
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchedFunc()
		t.Run(tt.name, func(t *testing.T) {
			if got := loadAlarmServerAddr(); got != tt.want {
				t.Errorf("loadAlarmServerAddr() = %v, want %v", got, tt.want)
			}
		})
		patches.ResetAll()
	}
}

func Test_loadLocalAuthConfig(t *testing.T) {
	localAuth := &localauth.AuthConfig{
		AKey: "aaa",
	}
	confStr1, _ := json.Marshal(localAuth)
	tests := []struct {
		name        string
		want        *localauth.AuthConfig
		patchedFunc utils.PatchesFunc
	}{
		{
			name: "case1_AllPass",
			want: localAuth,
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return string(confStr1)
					}),
				})
				return patches
			},
		},
		{
			name: "case2_EmptyString",
			want: &localauth.AuthConfig{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
		},
		{
			name: "case3_NotJson",
			want: &localauth.AuthConfig{},
			patchedFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "aaa"
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchedFunc()
		t.Run(tt.name, func(t *testing.T) {
			if got := loadLocalAuthConfig(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadLocalAuthConfig() = %v, want %v", got, tt.want)
			}
		})
		patches.ResetAll()
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

func Test_hotLoadConfig(t *testing.T) {
	type args struct {
		filename string
		opType   monitor.OpType
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
	}{
		{
			name: "case_01 failed to get new hotload config file",
			args: args{"/home/sn/config/cache-manager.conf", 1},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, fmt.Errorf("error")
						}),
				})
				return patches
			},
		}, {
			name: "case_02 new hotload config file param check error",
			args: args{"/home/sn/config/cache-manager.conf", 1},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, fmt.Errorf("error")
						}),
				})
				return patches
			},
		}, {
			name: "case_03 normal scenario",
			args: args{"/home/sn/config/cache-manager.conf", 1},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(readHotLoadConfig,
						func(p string) (*HotLoadConfig, error) {
							return &HotLoadConfig{}, nil
						}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct,
						func(s interface{}) (bool, error) {
							return false, nil
						}),
					gomonkey.ApplyFunc(setHotLoadConfig,
						func(config *CacheManagerConfig, newHotLoadConfig *HotLoadConfig) {
						}),
				})
				return patches
			},
		},
	}
	configChangedCallbacks = append(configChangedCallbacks, func() {})
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			hotLoadConfig(tt.args.filename, tt.args.opType)
		})
		patches.ResetAll()
	}
}

func Test_readHotLoadConfig(t *testing.T) {
	type args struct {
		p string
	}
	tests := []struct {
		name        string
		args        args
		wantErr     bool
		patchesFunc utils.PatchesFunc
	}{
		{
			name:    "case01 failed to open file",
			args:    args{"/home/sn/config/cache-manager.conf"},
			wantErr: true,
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, fmt.Errorf("open error")
						}),
				})
				return patches
			},
		}, {
			name:    "case02 failed to read file",
			args:    args{"/home/sn/config/cache-manager.conf"},
			wantErr: true,
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return nil, fmt.Errorf("read error")
						}),
				})
				return patches
			},
		}, {
			name:    "case03 failed to unmarshal",
			args:    args{"/home/sn/config/cache-manager.conf"},
			wantErr: true,
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return []byte(""), nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return fmt.Errorf("unmarshal failed")
						}),
				})
				return patches
			},
		}, {
			name:    "case04 normal scenario",
			args:    args{"/home/sn/config/cache-manager.conf"},
			wantErr: false,
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.Open,
						func(key string) (*os.File, error) {
							return &os.File{}, nil
						}),
					gomonkey.ApplyFunc(ioutil.ReadAll,
						func(r io.Reader) ([]byte, error) {
							return []byte(""), nil
						}),
					gomonkey.ApplyFunc(json.Unmarshal,
						func(data []byte, v interface{}) error {
							return nil
						}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := readHotLoadConfig(tt.args.p)
			if (err != nil) != tt.wantErr {
				t.Errorf("readHotLoadConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_setHotLoadConfig(t *testing.T) {
	type args struct {
		config        *CacheManagerConfig
		hotLoadConfig *HotLoadConfig
	}
	var a args
	conf := &CacheManagerConfig{
		HTTPSConfig:        nil,
		LocalAuth:          nil,
		EtcdConfig:         &etcd3.ClientConfig{},
		S3Config:           S3Config{},
		Persistence:        PersistenceConfig{},
		LimitCodeSize:      0,
		SmallCodePoolSize:  0,
		LargeCodePoolSize:  0,
		AlarmServerAddress: "",
	}
	serverList := []string{"100.000.000.00"}
	hotLoadConfig := &HotLoadConfig{EtcdConfig: &etcd3.ClientConfig{
		ServerList: serverList,
		User:       "",
		Password:   "",
		CaFile:     "",
		CertFile:   "",
		KeyFile:    "",
		SslEnable:  false,
	}}
	a.config = conf
	a.hotLoadConfig = hotLoadConfig
	setHotLoadConfig(a.config, a.hotLoadConfig)
}
