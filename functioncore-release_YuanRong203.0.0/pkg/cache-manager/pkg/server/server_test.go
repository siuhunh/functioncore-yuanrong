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
package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"functioncore/pkg/common/types"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sync"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/gin-gonic/gin"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/cache-manager/pkg/utils"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/tokenrotation"
)

func TestNewDeployer_3(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(commonObs.NewObsClient,
			func(opts commonObs.Option) (*commonObs.Client, error) {
				return &commonObs.Client{}, nil
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return []byte("secret"), errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		c *config.CacheManagerConfig
	}
	var a args
	configs := &config.CacheManagerConfig{
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		HTTPSConfig: nil,
	}
	a.c = configs
	a.c.S3Config.SecretKey = "SecretKey"
	a.c.S3Config.AccessKey = "AccessKey"
	a.c.S3Config.CaFile = "CaFile"
	a.c.S3Config.Endpoint = "Endpoint"
	a.c.S3Config.TrustedCA = false
	a.c.S3Config.UseSSL = false
	tests := []struct {
		name    string
		args    args
		want    Deployer
		wantErr bool
	}{
		{"case1", a, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewDeployer(tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDeployer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDeployer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewDeployer_4(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(commonObs.NewObsClient,
			func(opts commonObs.Option) (*commonObs.Client, error) {
				return &commonObs.Client{}, nil
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return []byte("secret"), nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		c *config.CacheManagerConfig
	}
	var a args
	configs := &config.CacheManagerConfig{
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		HTTPSConfig: nil,
	}
	a.c = configs
	a.c.S3Config.SecretKey = "SecretKey"
	a.c.S3Config.AccessKey = "AccessKey"
	a.c.S3Config.CaFile = "CaFile"
	a.c.S3Config.Endpoint = "Endpoint"
	a.c.S3Config.TrustedCA = false
	a.c.S3Config.UseSSL = false
	a.c.LargeCodePoolSize = 10
	a.c.SmallCodePoolSize = 10
	tests := []struct {
		name    string
		args    args
		want    Deployer
		wantErr bool
	}{
		{"case1", a, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDeployer(tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDeployer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestNewDeployer_5(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(commonObs.NewObsClient,
			func(opts commonObs.Option) (*commonObs.Client, error) {
				return &commonObs.Client{}, errors.New("test")
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return []byte("secret"), nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		c *config.CacheManagerConfig
	}
	var a args
	configs := &config.CacheManagerConfig{
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		HTTPSConfig: nil,
	}
	a.c = configs
	a.c.S3Config.SecretKey = "SecretKey"
	a.c.S3Config.AccessKey = "AccessKey"
	a.c.S3Config.CaFile = "CaFile"
	a.c.S3Config.Endpoint = "Endpoint"
	a.c.S3Config.TrustedCA = false
	a.c.S3Config.UseSSL = false
	a.c.LargeCodePoolSize = 10
	a.c.SmallCodePoolSize = 10
	tests := []struct {
		name    string
		args    args
		want    Deployer
		wantErr bool
	}{
		{"case1", a, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDeployer(tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDeployer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestNewDeployer_6(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(commonObs.NewObsClient,
			func(opts commonObs.Option) (*commonObs.Client, error) {
				return &commonObs.Client{}, nil
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return []byte("secret"), nil
			}),
		gomonkey.ApplyFunc(ants.NewPool,
			func(size int, options ...ants.Option) (*ants.Pool, error) {
				return &ants.Pool{}, errors.New("test")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		c *config.CacheManagerConfig
	}
	var a args
	configs := &config.CacheManagerConfig{
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		HTTPSConfig: nil,
	}
	a.c = configs
	a.c.S3Config.SecretKey = "SecretKey"
	a.c.S3Config.AccessKey = "AccessKey"
	a.c.S3Config.CaFile = "CaFile"
	a.c.S3Config.Endpoint = "Endpoint"
	a.c.S3Config.TrustedCA = false
	a.c.S3Config.UseSSL = false
	a.c.LargeCodePoolSize = 10
	a.c.SmallCodePoolSize = 10
	tests := []struct {
		name    string
		args    args
		want    Deployer
		wantErr bool
	}{
		{"case1", a, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewDeployer(tt.args.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDeployer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestPrintInputLog(t *testing.T) {
	LruCacheMap = utils.NewLru(&CacheMap)
	LruCacheMap.AddCache("aaa", CodeCache{
		CodeBuffer: []byte{'a'},
		Checksum:   "aaa",
	}, int64(len([]byte{'a'})))
	tests := []struct {
		name string
	}{
		{"case1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			PrintInputLog()
		})
	}
}

func TestHTTPServerCreate(t *testing.T) {
	server := &http.Server{
		Addr:           "127.0.0.1",
		Handler:        buildHandler(),
		MaxHeaderBytes: defaultHttpHeaderLimit,
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(server), "ListenAndServe",
			func(_ *http.Server) error {
				return nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		conf *config.CacheManagerConfig
	}
	var a args
	HTTPSConfig := &tls.InternalHTTPSConfig{
		HTTPSEnable: false,
		TLSProtocol: "",
		TLSCiphers:  "",
	}
	conf := &config.CacheManagerConfig{
		HTTPSConfig: HTTPSConfig,
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		Persistence: config.PersistenceConfig{},
	}
	a.conf = conf
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go HTTPServerCreate(tt.args.conf)
		})
	}
}

func TestHTTPServerCreate_2(t *testing.T) {
	type args struct {
		conf *config.CacheManagerConfig
	}
	var a args
	HTTPSConfig := &tls.InternalHTTPSConfig{
		HTTPSEnable: false,
		TLSProtocol: "",
		TLSCiphers:  "",
	}
	conf := &config.CacheManagerConfig{
		HTTPSConfig: HTTPSConfig,
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		Persistence: config.PersistenceConfig{},
	}
	a.conf = conf
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go HTTPServerCreate(tt.args.conf)
		})
	}
}

func TestHTTPServerCreate_3(t *testing.T) {
	type args struct {
		conf *config.CacheManagerConfig
	}
	var a args
	HTTPSConfig := &tls.InternalHTTPSConfig{
		HTTPSEnable: true,
		TLSProtocol: "",
		TLSCiphers:  "",
	}
	conf := &config.CacheManagerConfig{
		HTTPSConfig: HTTPSConfig,
		EtcdConfig:  &etcd3.ClientConfig{},
		S3Config:    config.S3Config{},
		Persistence: config.PersistenceConfig{},
	}
	a.conf = conf
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"case1", a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			go HTTPServerCreate(tt.args.conf)
		})
	}
}

func Test_s3Deployer_Deploy(t *testing.T) {
	type fields struct {
		cli *commonObs.Client
	}
	type args struct {
		info *config.FunctionMetaInfo
	}
	var f fields
	var a args
	cli := &commonObs.Client{}
	info := &config.FunctionMetaInfo{
		FuncMetaData: config.FuncMetaData{
			Layers: []*types.Layer{
				{
					ObjectID:       "objectid-layer5",
					Name:           "layer3",
					ETag:           "33",
					DependencyType: "public",
				},
				{
					ObjectID:       "objectid-layer6",
					Name:           "layer4",
					ETag:           "44",
					DependencyType: "private",
				},
			},
		},
		S3MetaData:       config.S3MetaData{},
		EnvMetaData:      config.EnvMetaData{},
		ResourceMetaData: config.ResourceMetaData{},
		InstanceMetaData: config.InstanceMetaData{},
		ExtendedMetaData: config.ExtendedMetaData{},
	}
	f.cli = cli
	a.info = info
	deployer := &s3Deployer{
		cli: f.cli,
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc((*ants.Pool).Submit, func(f *ants.Pool, task func()) error {
			return nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(deployer.cli), "Refresh",
			func(_ *commonObs.Client, _ string, _, _ []byte) {
			}),
		gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
			return []byte(""), nil
		}),
		gomonkey.ApplyFunc(tokenrotation.GetTempToken, func() (string, string, string) {
			return "ak", "sk", "token"
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"case1", f, a, false},
	}
	pool, err := ants.NewPool(10)
	assert.Nil(t, err)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &s3Deployer{
				cli:           tt.fields.cli,
				smallCodePool: pool,
				largeCodePool: pool,
			}
			if err := s.Deploy(tt.args.info); (err != nil) != tt.wantErr {
				t.Errorf("Deploy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Deployer_DeployFunction(t *testing.T) {
	type fields struct {
		cli *commonObs.Client
	}
	type args struct {
		bucketID string
		objectID string
		info     *config.FunctionMetaInfo
		checksum string
		isLayer  bool
	}
	var f fields
	var a args
	cli := &commonObs.Client{}
	info := &config.FunctionMetaInfo{
		FuncMetaData:     config.FuncMetaData{},
		S3MetaData:       config.S3MetaData{},
		EnvMetaData:      config.EnvMetaData{},
		ResourceMetaData: config.ResourceMetaData{},
		InstanceMetaData: config.InstanceMetaData{},
		ExtendedMetaData: config.ExtendedMetaData{},
	}
	f.cli = cli
	a.info = info
	deployer := &s3Deployer{
		cli: f.cli,
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(deployer), "DetermineFile",
			func(_ *s3Deployer, _ string, _ string, _ *config.FunctionMetaInfo, isLayer bool, checksum string) error {
				return nil
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"case1", f, a, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &s3Deployer{
				cli: tt.fields.cli,
			}
			if err := s.DownloadObject(tt.args.bucketID, tt.args.objectID, tt.args.info, tt.args.isLayer, tt.args.checksum); (err != nil) != tt.wantErr {
				t.Errorf("DeployFunction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Deployer_DeployFunction_2(t *testing.T) {
	type fields struct {
		cli *commonObs.Client
	}
	type args struct {
		bucketID string
		objectID string
		info     *config.FunctionMetaInfo
		checksum string
		isLayer  bool
	}
	var f fields
	var a args
	cli := &commonObs.Client{}
	info := &config.FunctionMetaInfo{
		FuncMetaData:     config.FuncMetaData{},
		S3MetaData:       config.S3MetaData{},
		EnvMetaData:      config.EnvMetaData{},
		ResourceMetaData: config.ResourceMetaData{},
		InstanceMetaData: config.InstanceMetaData{},
		ExtendedMetaData: config.ExtendedMetaData{},
	}
	f.cli = cli
	a.info = info
	deployer := &s3Deployer{
		cli: f.cli,
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(deployer), "DetermineFile",
			func(_ *s3Deployer, _ string, _ string, _ *config.FunctionMetaInfo, _ bool, _ string) error {
				return errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"case1", f, a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &s3Deployer{
				cli: tt.fields.cli,
			}
			if err := s.DownloadObject(tt.args.bucketID, tt.args.objectID, tt.args.info, tt.args.isLayer, tt.args.checksum); (err != nil) != tt.wantErr {
				t.Errorf("DeployFunction() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Deployer_DetermineFile(t *testing.T) {
	type fields struct {
		cli *commonObs.Client
	}
	type args struct {
		bucketID string
		objectID string
		info     *config.FunctionMetaInfo
		checksum string
		isLayer  bool
	}
	var f fields
	var a args
	cli := &commonObs.Client{}
	info := &config.FunctionMetaInfo{
		FuncMetaData:     config.FuncMetaData{},
		S3MetaData:       config.S3MetaData{},
		EnvMetaData:      config.EnvMetaData{},
		ResourceMetaData: config.ResourceMetaData{},
		InstanceMetaData: config.InstanceMetaData{},
		ExtendedMetaData: config.ExtendedMetaData{},
	}
	f.cli = cli
	a.info = info
	deployer := &s3Deployer{
		cli: f.cli,
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(deployer.cli), "GetObject",
			func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return &obs.GetObjectOutput{}, nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&ObjSizeCache{}), "IsObjInDisk",
			func(_ *ObjSizeCache, info *config.FunctionMetaInfo, checksum, key string, isLayer bool) bool {
				return false
			}),
		gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
			return []byte{}, nil
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()

	var a2 args
	a2.objectID = "aaa.zstd"
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"case1", f, a, false},
		{"case2", f, a2, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &s3Deployer{
				cli: tt.fields.cli,
			}
			if err := s.DetermineFile(tt.args.bucketID, tt.args.objectID, tt.args.info, tt.args.isLayer, tt.args.checksum); (err != nil) != tt.wantErr {
				t.Errorf("DetermineFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_s3Deployer_DetermineFile_2(t *testing.T) {
	type fields struct {
		cli *commonObs.Client
	}
	type args struct {
		bucketID string
		objectID string
		info     *config.FunctionMetaInfo
		checksum string
		isLayer  bool
	}
	var f fields
	var a args
	cli := &commonObs.Client{}
	info := &config.FunctionMetaInfo{
		FuncMetaData:     config.FuncMetaData{},
		S3MetaData:       config.S3MetaData{},
		EnvMetaData:      config.EnvMetaData{},
		ResourceMetaData: config.ResourceMetaData{},
		InstanceMetaData: config.InstanceMetaData{},
		ExtendedMetaData: config.ExtendedMetaData{},
	}
	f.cli = cli
	a.info = info
	deployer := &s3Deployer{
		cli: f.cli,
	}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(deployer), "GetObject",
			func(_ *s3Deployer, _ string, _ string, _ *config.FunctionMetaInfo, isLayer bool, checksum string) error {
				return errors.New("aaa")
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&ObjSizeCache{}), "IsObjInDisk",
			func(_ *ObjSizeCache, info *config.FunctionMetaInfo, checksum, key string, isLayer bool) bool {
				return false
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"case1", f, a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &s3Deployer{
				cli: tt.fields.cli,
			}
			if err := s.DetermineFile(tt.args.bucketID, tt.args.objectID, tt.args.info, tt.args.isLayer, tt.args.checksum); (err != nil) != tt.wantErr {
				t.Errorf("DetermineFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_persistence(t *testing.T) {
	type args struct {
		objectID string
		buffer   []byte
		checksum string
		urn      string
		isLayer  bool
	}

	tests := []struct {
		name string
		args args
	}{
		{"case1", args{urn: "persistence1"}},
		{"case1", args{urn: "persistence2", isLayer: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persistence(tt.args.objectID, tt.args.buffer, tt.args.checksum, tt.args.urn, tt.args.isLayer)
		})
	}
}

func Test_persistence_2(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(os.OpenFile,
			func(_ string, _ int, _ os.FileMode) (*os.File, error) {
				return nil, errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		objectID string
		buffer   []byte
		checksum string
		urn      string
		isLayer  bool
	}

	tests := []struct {
		name string
		args args
	}{
		{"case1", args{urn: "persistence1"}},
		{"case1", args{urn: "persistence2", isLayer: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persistence(tt.args.objectID, tt.args.buffer, tt.args.checksum, tt.args.urn, tt.args.isLayer)
		})
	}
}

func Test_persistence_3(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(json.Marshal,
			func(_ interface{}) ([]byte, error) {
				return nil, errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		objectID string
		buffer   []byte
		checksum string
		urn      string
		isLayer  bool
	}
	var a args

	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persistence(tt.args.objectID, tt.args.buffer, tt.args.checksum, tt.args.urn, tt.args.isLayer)
		})
	}
}

func Test_persistence_4(t *testing.T) {
	w := &bufio.Writer{}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(w), "Write",
			func(_ *bufio.Writer, _ []byte) (int, error) {
				return 0, errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		objectID string
		buffer   []byte
		checksum string
		urn      string
		isLayer  bool
	}
	var a args

	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persistence(tt.args.objectID, tt.args.buffer, tt.args.checksum, tt.args.urn, tt.args.isLayer)
		})
	}
}

func Test_persistence_5(t *testing.T) {
	w := &bufio.Writer{}
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyMethod(reflect.TypeOf(w), "Flush",
			func(_ *bufio.Writer) error {
				return errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		objectID string
		buffer   []byte
		checksum string
		urn      string
		isLayer  bool
	}
	var a args

	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persistence(tt.args.objectID, tt.args.buffer, tt.args.checksum, tt.args.urn, tt.args.isLayer)
		})
	}
}

func Test_persistence_6(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(localauth.Encrypt,
			func(_ string) (string, error) {
				return "", errors.New("aaa")
			}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	type args struct {
		objectID string
		buffer   []byte
		checksum string
		urn      string
		isLayer  bool
	}
	var a args

	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			persistence(tt.args.objectID, tt.args.buffer, tt.args.checksum, tt.args.urn, tt.args.isLayer)
		})
	}
}

func Test_persistent_7(t *testing.T) {
	convey.Convey("TestPersistent", t, func() {
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(os.OpenFile, func(name string, flag int, perm os.FileMode) (*os.File, error) {
				return &os.File{}, nil
			}),
			gomonkey.ApplyFunc(json.Marshal, func(v interface{}) ([]byte, error) {
				return []byte{}, nil
			}),
			gomonkey.ApplyFunc(localauth.Encrypt, func(src string) (string, error) {
				return "", nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		convey.Convey("writeErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&bufio.Writer{}), "Write", func(_ *bufio.Writer,
					p []byte) (nn int, err error) {
					return 0, errors.New("test")
				}),
			}
			persistence("", []byte{}, "", "", false)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
		convey.Convey("flushErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&bufio.Writer{}), "Write", func(_ *bufio.Writer,
					p []byte) (nn int, err error) {
					return 0, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&bufio.Writer{}), "Flush", func(_ *bufio.Writer) error {
					return errors.New("test error")
				}),
			}
			persistence("", []byte{}, "", "", false)
			for idx := range patches {
				patches[idx].Reset()
			}
		})
	})
}

func Test_loadFunction(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	rg := router.Group("/v1")
	RegisterHandlers(rg.Group(""))
	type args struct {
		c *gin.Context
	}
	var a args
	c := &gin.Context{
		Request:  nil,
		Writer:   nil,
		Params:   nil,
		Keys:     nil,
		Errors:   nil,
		Accepted: nil,
	}
	a.c = c
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = PerformRequest(router, "GET", "/v1/load", []byte{})
			_ = PerformRequest2(router, "GET", "/v1/load", []byte{})
		})
	}
}

func Test_loadFunction_2(t *testing.T) {
	LruCacheMap.AddCache("aaa", CodeCache{
		CodeBuffer: []byte{'a'},
		Checksum:   "aaa",
	}, int64(len([]byte{'a'})))
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	rg := router.Group("/v1")
	RegisterHandlers(rg.Group(""))
	type args struct {
		c *gin.Context
	}
	var a args
	c := &gin.Context{
		Request:  nil,
		Writer:   nil,
		Params:   nil,
		Keys:     nil,
		Errors:   nil,
		Accepted: nil,
	}
	a.c = c
	tests := []struct {
		name string
		args args
	}{
		{"case1", a},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = PerformRequest2(router, "GET", "/v1/load?id=aaa", []byte{})
		})
	}
}

func PerformRequest(r http.Handler, method, path string, input []byte) *httptest.ResponseRecorder {
	req := &http.Request{}
	req = nil
	if input == nil {
		req = httptest.NewRequest(method, path, nil)
	} else {
		buffer := bytes.NewReader(input)
		req = httptest.NewRequest(method, path, buffer)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func PerformRequest2(r http.Handler, method, path string, input []byte) *httptest.ResponseRecorder {
	_ = os.Setenv(constants.PodNamespaceEnvKey, "default")
	req := &http.Request{}
	req = nil
	if input == nil {
		req = httptest.NewRequest(method, path, nil)
	} else {
		buffer := bytes.NewReader(input)
		req = httptest.NewRequest(method, path, buffer)
	}
	defaultServiceID := fmt.Sprintf(constants.CacheServiceName, "default")
	req.Header.Set(constants.HeaderServiceID, defaultServiceID)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func Test_loadFunction_all(t *testing.T) {
	r := gin.New()
	r.GET("/load", loadFunction)

	objID := "exit_obj"
	checkSum := "correct_checksum"

	LruCacheMap = utils.NewLru(&sync.Map{})
	LruCacheMap.AddCache(objID, CodeCache{
		Checksum: checkSum,
	}, 0)

	LruCacheMap.AddCache("errortype", "abc", 0)
	_ = os.Setenv(constants.PodNamespaceEnvKey, "default")
	defaultServiceID := fmt.Sprintf(constants.CacheServiceName, "default")

	tests := []struct {
		name   string
		query  map[string]string
		header map[string]string

		exceptedCode int
	}{
		{
			name: "service id mismatched",
			header: map[string]string{
				constants.HeaderServiceID: "abcd",
			},
			exceptedCode: http.StatusUnauthorized,
		},
		{
			name: "pre connect objID",
			header: map[string]string{
				constants.HeaderServiceID: defaultServiceID,
			},
			query: map[string]string{
				"id": "pre",
			},
			exceptedCode: http.StatusBadRequest,
		},
		{
			name: "obj not exists",
			header: map[string]string{
				constants.HeaderServiceID: defaultServiceID,
			},
			query: map[string]string{
				"id": "notexist",
			},
			exceptedCode: http.StatusBadRequest,
		},
		{
			name: "obj type error",
			header: map[string]string{
				constants.HeaderServiceID: defaultServiceID,
			},
			query: map[string]string{
				"id": "errortype",
			},
			exceptedCode: http.StatusBadRequest,
		},
		{
			name: "check sum mismatched",
			header: map[string]string{
				constants.HeaderServiceID: defaultServiceID,
				constants.HeaderSha512:    "abcd",
			},
			query: map[string]string{
				"id": objID,
			},
			exceptedCode: http.StatusBadRequest,
		},
		{
			name: "ok",
			header: map[string]string{
				constants.HeaderServiceID: defaultServiceID,
				constants.HeaderSha512:    checkSum,
			},
			query: map[string]string{
				"id": objID,
			},
			exceptedCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest(http.MethodGet, "/load", nil)
			q := req.URL.Query()
			for k, v := range tt.query {
				q.Add(k, v)
			}
			req.URL.RawQuery = q.Encode()
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			r.ServeHTTP(w, req)
			assert.Equal(t, tt.exceptedCode, w.Code)
		})
	}
}

func Test_RefreshToken(t *testing.T) {
	convey.Convey("RefreshToken", t, func() {
		p := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
			if src == "SecretKey" {
				return nil, errors.New("mock decrypting sk error")
			} else if src == "SecurityToken" {
				return nil, errors.New("mock decrypting token error")
			} else {
				return []byte("key"), nil
			}
		})
		defer p.Reset()
		s3d := &s3Deployer{}
		err := s3d.refreshToken("ok", "SecretKey", "ok")
		convey.ShouldNotBeNil(err)
		convey.ShouldEqual(err.Error(), "mock decrypting sk error")
		err = s3d.refreshToken("ok", "ok", "SecurityToken")
		convey.ShouldNotBeNil(err)
		convey.ShouldEqual(err.Error(), "mock decrypting token error")
	})
}
