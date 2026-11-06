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
	"errors"
	"io/fs"
	"io/ioutil"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"

	"functioncore/pkg/common/localauth"
	rmconfig "functioncore/pkg/runtime-manager/config"
	"functioncore/test/utils"
)

func TestCleanWorkerSecret(t *testing.T) {
	p := gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return []fs.FileInfo{}, nil
	})
	defer p.Reset()
	CleanWorkerSecret()
	p1 := gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return []fs.FileInfo{}, errors.New("")
	})
	defer p1.Reset()
	CleanWorkerSecret()
}

func Test_getWorkerKey(t *testing.T) {
	config := &Configuration{}
	data := make([]byte, 0, 10)

	tests := []struct {
		name    string
		Setup   func(t *testing.T)
		wantErr error
	}{
		{
			name:    "test getWorkerKey",
			wantErr: errors.New(""),
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return data, nil
					})
				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
		{
			name:    "test getWorkerKey",
			wantErr: nil,
			Setup: func(t *testing.T) {
				patches := gomonkey.ApplyFunc(ioutil.ReadFile,
					func(filename string) ([]byte, error) {
						return make([]byte, 0, 10), errors.New("")
					})
				t.Cleanup(func() {
					patches.Reset()
				})
			},
		},
		{
			name:    "test getWorkerKey",
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			err := getWorkerKey(config)
			if err == tt.wantErr {
				assert.EqualError(t, err, " Not as expectedg")
			}
		})
	}
}

func TestGetConfig(t *testing.T) {
	gPatch := gomonkey.ApplyMethod(reflect.TypeOf(&sync.Once{}), "Do", func(_ *sync.Once, f func()) {
		f()
	})
	defer gPatch.Reset()

	tests := []struct {
		name        string
		want        *Configuration
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			want: &Configuration{},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "", true
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
					gomonkey.ApplyFunc(rmconfig.GetConfig, func() (*rmconfig.Config, error) {
						return &rmconfig.Config{}, nil
					}),
					gomonkey.ApplyFunc(getWorkerKey, func(config *Configuration) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: false,
		},
		{
			name: "invalid config from env",
			want: &Configuration{},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "{", true
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "failed to valid",
			want: &Configuration{},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "", true
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("valid failed")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "failed to get rm config",
			want: &Configuration{},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "", true
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
					gomonkey.ApplyFunc(rmconfig.GetConfig, func() (*rmconfig.Config, error) {
						return &rmconfig.Config{}, nil
					}),
					gomonkey.ApplyFunc(getWorkerKey, func(config *Configuration) error {
						return errors.New("failed to get wk key")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "failed to get worker key config",
			want: &Configuration{},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(os.LookupEnv, func(key string) (string, bool) {
						return "", true
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
					gomonkey.ApplyFunc(rmconfig.GetConfig, func() (*rmconfig.Config, error) {
						return nil, errors.New("failed to get rm config")
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
			got, err := GetConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConfig() error = %v, wantErr %v", err, tt.wantErr)
				return

			}
			if !tt.wantErr && !assert.NotNil(t, got) {
				t.Errorf("GetConfig() got is nil")
			}
			patches.ResetAll()
		})
	}
}

func TestGetConfig1(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				return patches
			},
			wantErr: true,
		},
		{
			name: "case2",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("aaa"), nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case3",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case4",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return false, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case5",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
					gomonkey.ApplyFunc(rmconfig.GetConfig, func() (*rmconfig.Config, error) {
						return nil, errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case6",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte{}, nil
					}),
					gomonkey.ApplyFunc(yaml.Unmarshal, func(in []byte, out interface{}) error {
						return nil
					}),
					gomonkey.ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
						return true, nil
					}),
					gomonkey.ApplyFunc(rmconfig.GetConfig, func() (*rmconfig.Config, error) {
						return &rmconfig.Config{}, nil
					}),
					gomonkey.ApplyFunc(getWorkerKey, func(config *Configuration) error {
						return errors.New("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configSingleton = struct {
				sync.Once
				config *Configuration
			}{}
			patches := tt.patchesFunc()
			_, err := GetConfig()
			if (err != nil) != tt.wantErr {
				t.Errorf("GetConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}

type fakeFile struct{}

func (fakeFile) Name() string       { return "" }
func (fakeFile) Size() int64        { return 0 }
func (fakeFile) Mode() fs.FileMode  { return 0755 }
func (fakeFile) ModTime() time.Time { return time.Time{} }
func (fakeFile) IsDir() bool        { return false }
func (fakeFile) Sys() interface{}   { return nil }

func TestCleanWorkerSecret1(t *testing.T) {
	tests := []struct {
		name        string
		patchesFunc utils.PatchesFunc
	}{
		{
			name: "case1",
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
						return []fs.FileInfo{
							fakeFile{},
							fakeFile{},
						}, nil
					}),
					gomonkey.ApplyFunc(os.RemoveAll, func(path string) error {
						return errors.New("error")
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			CleanWorkerSecret()
			patches.ResetAll()
		})
	}
}

func Test_getWorkerKey1(t *testing.T) {
	type args struct {
		config *Configuration
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "case1",
			args: args{
				config: &Configuration{
					DeploymentConfig: DeploymentConfig{
						AuthConfig: localauth.AuthConfig{
							AKey:     "",
							SKey:     "",
							Duration: 1,
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFuncSeq(ioutil.ReadFile, []gomonkey.OutputCell{
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte{}, nil}},
						{Values: gomonkey.Params{[]byte{}, nil}},
						{Values: gomonkey.Params{[]byte{}, nil}},
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case2",
			args: args{
				config: &Configuration{
					DeploymentConfig: DeploymentConfig{
						AuthConfig: localauth.AuthConfig{
							AKey:     "",
							SKey:     "",
							Duration: 1,
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFuncSeq(ioutil.ReadFile, []gomonkey.OutputCell{
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte{}, nil}},
						{Values: gomonkey.Params{[]byte{}, nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
					}),
				})
				return patches
			},
			// support rotated token, and compatible with fixed token
			wantErr: false,
		},
		{
			name: "case3",
			args: args{
				config: &Configuration{
					DeploymentConfig: DeploymentConfig{
						AuthConfig: localauth.AuthConfig{
							AKey:     "",
							SKey:     "",
							Duration: 1,
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFuncSeq(ioutil.ReadFile, []gomonkey.OutputCell{
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte{}, nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
					}),
				})
				return patches
			},
			// support rotated token, and compatible with fixed token
			wantErr: false,
		},
		{
			name: "case4",
			args: args{
				config: &Configuration{
					DeploymentConfig: DeploymentConfig{
						AuthConfig: localauth.AuthConfig{
							AKey:     "",
							SKey:     "",
							Duration: 1,
						},
					},
				},
			},
			patchesFunc: func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					gomonkey.ApplyFuncSeq(ioutil.ReadFile, []gomonkey.OutputCell{
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
						{Values: gomonkey.Params{[]byte("aaa"), nil}},
					}),
				})
				return patches
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			if err := getWorkerKey(tt.args.config); (err != nil) != tt.wantErr {
				t.Errorf("getWorkerKey() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
