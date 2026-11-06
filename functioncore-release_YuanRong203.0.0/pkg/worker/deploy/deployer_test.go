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

package deploy

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/kms"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/storage"
	commontypes "functioncore/pkg/common/types"
	utilities "functioncore/pkg/common/utils"
	"functioncore/pkg/worker/config"
	mockUtils "functioncore/test/utils"
)

// TestNewDeployer test the exception branch of the mock NewDeployer
func TestNewDeployer(t *testing.T) {
	convey.Convey("TestNewDeployer", t, func() {
		convey.Convey("TestDecryptErr", func() {
			patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
				return nil, errors.New("test err")
			})
			deployer, err := NewDeployer(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					StorageType: "s3",
				},
			})
			convey.So(deployer, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
			patch.Reset()
		})
		convey.Convey("TestNewObsClientErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("secret"), nil
				}),
				gomonkey.ApplyFunc(commonObs.NewObsClient, func(o commonObs.Option) (*commonObs.Client, error) {
					return nil, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			deployer, err := NewDeployer(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					StorageType: "s3",
					S3Config:    config.S3Config{},
				},
			})
			convey.So(deployer, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("TestPreConnectCache_makeRequestErr", func() {
			patches := []*gomonkey.Patches{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("secret"), nil
				}),
				gomonkey.ApplyFunc(commonObs.NewObsClient, func(o commonObs.Option) (*commonObs.Client, error) {
					return &commonObs.Client{}, nil
				}),
				gomonkey.ApplyFunc(http.NewRequest, func(method, url string, body io.Reader) (*http.Request, error) {
					return nil, errors.New("test err")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			deployer, err := NewDeployer(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					StorageType: "s3",
					S3Config:    config.S3Config{},
				},
			})
			convey.So(deployer, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
		convey.Convey("TestInvalidStorageType", func() {
			deployer, err := NewDeployer(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					StorageType: "test",
				},
			})
			convey.So(deployer, convey.ShouldBeNil)
			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func TestDeploy(t *testing.T) {
	convey.Convey("Deploy", t, func() {
		convey.Convey("test zip deployDir", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),
				gomonkey.ApplyFunc(commonObs.NewObsClient,
					func(opts commonObs.Option) (*commonObs.Client, error) {
						return &commonObs.Client{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return &obs.GetObjectOutput{}, nil
				}),
				gomonkey.ApplyFunc(commonObs.DownloadFileToPathBySlice, func(bucketID string, objectID string, objPath string, objSize int64, client commonObs.Client) error {
					return nil
				}),
				gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) { return []byte{}, nil }),
				gomonkey.ApplyFunc(os.MkdirAll,
					func(_ string, _ os.FileMode) error {
						return nil
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return []byte("secret"), nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&ZipHandler{}), "Decompress", func(_ *ZipHandler, objectSize int64, downloadFilePath string, buffer []byte,
					extractPath string, isLayer bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(closeConnectionToCacheMgr, func() {
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			conf := &config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					BucketID:            "bucketID",
					ObjectID:            "objectID.zip",
					DeployDir:           "/opt/function/code",
					StorageType:         "s3",
					CodePackage:         config.CodePackage{},
					S3Config:            config.S3Config{},
					AuthConfig:          localauth.AuthConfig{},
					InternalHTTPSConfig: config.InternalHTTPSConfig{HTTPSEnable: true},
				},
				RuntimeConfig: config.RuntimeConfig{
					URN: "urn",
				},
				ServerConfig: config.ServerConfig{IOReadTimeout: 3},
			}
			deployer, err := NewDeployer(conf)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			conf.DeployDir = "/opt/function/code"
			_, err = deployer.Deploy(conf, nil, "")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test zip deployDir with pool pod", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),
				gomonkey.ApplyFunc(commonObs.NewObsClient,
					func(opts commonObs.Option) (*commonObs.Client, error) {
						return &commonObs.Client{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return &obs.GetObjectOutput{}, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "Refresh", func(c *commonObs.Client, ak string, sk, securityToken []byte) {
				}),
				gomonkey.ApplyFunc(commonObs.DownloadFileToPathBySlice, func(bucketID string, objectID string, objPath string, objSize int64, client commonObs.Client) error {
					return nil
				}),
				gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) { return []byte{}, nil }),
				gomonkey.ApplyFunc(os.MkdirAll,
					func(_ string, _ os.FileMode) error {
						return nil
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return []byte("secret"), nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&ZipHandler{}), "Decompress", func(_ *ZipHandler, objectSize int64, downloadFilePath string, buffer []byte,
					extractPath string, isLayer bool) error {
					return nil
				}),
				gomonkey.ApplyFunc(closeConnectionToCacheMgr, func() {
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			conf := &config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					BucketID:            "bucketID",
					ObjectID:            "objectID.zip",
					DeployDir:           "/opt/function/code",
					StorageType:         "s3",
					CodePackage:         config.CodePackage{},
					S3Config:            config.S3Config{},
					AuthConfig:          localauth.AuthConfig{},
					InternalHTTPSConfig: config.InternalHTTPSConfig{HTTPSEnable: true},
				},
				RuntimeConfig: config.RuntimeConfig{
					URN: "urn",
				},
				ServerConfig: config.ServerConfig{IOReadTimeout: 3},
			}
			deployer, err := NewDeployer(conf)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			conf.DeployDir = "/opt/function/code"
			conf.UsePoolPod = true
			_, err = deployer.Deploy(conf, nil, "")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test unzip deployDir", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),
				gomonkey.ApplyFunc(commonObs.NewObsClient,
					func(opts commonObs.Option) (*commonObs.Client, error) {
						return &commonObs.Client{}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
					return &obs.GetObjectOutput{}, nil
				}),
				gomonkey.ApplyFunc(commonObs.DownloadFileToPathBySlice, func(bucketID string, objectID string, objPath string, objSize int64, client commonObs.Client) error {
					return nil
				}),
				gomonkey.ApplyFunc(commonObs.DownloadFileToPath, func(downloadPath string, objectOutput *obs.GetObjectOutput, isLayer bool) (int64, error) {
					return 100, nil
				}),
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode fs.FileMode) error { return nil }),
				gomonkey.ApplyFunc(closeConnectionToCacheMgr, func() {
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			conf := &config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					BucketID:            "bucketID",
					ObjectID:            "bootstrap",
					DeployDir:           "/opt/function/code",
					StorageType:         "s3",
					CodePackage:         config.CodePackage{},
					S3Config:            config.S3Config{},
					AuthConfig:          localauth.AuthConfig{},
					InternalHTTPSConfig: config.InternalHTTPSConfig{HTTPSEnable: true},
				},
				RuntimeConfig: config.RuntimeConfig{
					URN: "urn",
				},
				ServerConfig: config.ServerConfig{IOReadTimeout: 1000},
			}
			deployer, err := NewDeployer(conf)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			conf.DeployDir = "/opt/function/code"
			_, err = deployer.Deploy(conf, nil, "")
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestDeploySnapshot(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Configuration, error) {
				return &config.Configuration{
					DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
						AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
						InternalHTTPSConfig: config.InternalHTTPSConfig{
							HTTPSEnable: false,
							TLSProtocol: "",
							TLSCiphers:  "",
						}},
				}, nil
			}),
		gomonkey.ApplyFunc(commonObs.NewObsClient,
			func(opts commonObs.Option) (*commonObs.Client, error) {
				return &commonObs.Client{}, nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
			return &obs.GetObjectOutput{}, nil
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "Refresh", func(c *commonObs.Client, ak string, sk, securityToken []byte) {
		}),
		gomonkey.ApplyFunc(commonObs.DownloadFileToPathBySlice, func(bucketID string, objectID string, objPath string, objSize int64, client commonObs.Client) error {
			return nil
		}),
		gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) { return []byte{}, nil }),
		gomonkey.ApplyFunc(os.MkdirAll,
			func(_ string, _ os.FileMode) error {
				return nil
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return []byte("secret"), nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&SnapshotZipHandler{}), "Decompress", func(_ *SnapshotZipHandler, objectSize int64, downloadFilePath string, buffer []byte,
			extractPath string, isLayer bool) error {
			return nil
		}),
		gomonkey.ApplyFunc(closeConnectionToCacheMgr, func() {
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()

	snapshotConfig := generateSnapshotConfig()
	deployer, err := NewDeployer(snapshotConfig)
	if err != nil {
		t.Errorf("new deployer failed: %s", err)
	}
	_, ok := deployer.(*s3Deployer)
	if !ok {
		t.Errorf("new deployer type error")
	}

	convey.Convey("Deploy Snapshot", t, func() {
		convey.Convey("test invalid deployDir", func() {
			snapshotConfig.DeployDir = "/opt/function/code"
			_, err = deployer.DeploySnapshot(snapshotConfig, nil, "")
			convey.So(err, convey.ShouldEqual, nil)
		})
		convey.Convey("test invalid rotated access", func() {
			snapshotConfig.DeployDir = "/opt/function/code"
			snapshotConfig.Access = "abc"
			addPatch := gomonkey.ApplyFunc(localauth.Decrypt,
				func(_ string) ([]byte, error) {
					return nil, errors.New("abc")
				})
			defer addPatch.Reset()
			_, err = deployer.DeploySnapshot(snapshotConfig, nil, "")
			assert.NotNil(t, err)
		})
		convey.Convey("test DeploySnapshot with pool pod", func() {
			snapshotConfig.DeployDir = "/opt/function/code"
			snapshotConfig.UsePoolPod = true
			snapshotConfig.Access = ""
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err = deployer.DeploySnapshot(snapshotConfig, nil, "")
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func TestUploadSnapshot(t *testing.T) {
	cli := &commonObs.Client{}
	convey.Convey("Upload Snapshot", t, func() {
		convey.Convey("test upload snapshot succeed", func() {
			testUploadFileName := "TestUploadSnapshot.txt"
			testUploadFile, err := os.Create(testUploadFileName)
			if err != nil {
				t.Errorf("create TestUploadSnapshot.txt error")
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),
				gomonkey.ApplyFunc(commonObs.NewObsClient,
					func(opts commonObs.Option) (*commonObs.Client, error) {
						return cli, nil
					}),
				gomonkey.ApplyFunc(os.MkdirAll,
					func(_ string, _ os.FileMode) error {
						return nil
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return []byte("secret"), nil
					}),
				gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
					return testUploadFile, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(cli), "UploadFile", func(_ *commonObs.Client, input *obs.PutFileInput) error {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&SnapshotZipHandler{}), "Compress", func(_ *SnapshotZipHandler, zipFile string, src string) error {
					return nil
				}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("secret"), nil
				}),
				gomonkey.ApplyFunc((*s3Deployer).RefreshToken, func(_ *s3Deployer, conf *config.Configuration) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotConfig := generateSnapshotConfig()
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			_, _, err = deployer.UploadSnapshot(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Access: "mockAccess",
				},
			}, "", "")
			convey.So(err, convey.ShouldBeNil)
			testUploadFile.Close()
			os.RemoveAll(testUploadFileName)
		})

		convey.Convey("test upload snapshot by zstd succeed", func() {
			testUploadFileName := "TestUploadSnapshot.tar.zst"
			testUploadFile, err := os.Create(testUploadFileName)
			if err != nil {
				t.Errorf("create TestUploadSnapshot.tar.zst error")
			}
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
							SnapshotConfig: config.SnapshotConfig{FastStartEnable: true},
						}, nil
					}),
				gomonkey.ApplyFunc(commonObs.NewObsClient,
					func(opts commonObs.Option) (*commonObs.Client, error) {
						return cli, nil
					}),
				gomonkey.ApplyFunc(os.MkdirAll,
					func(_ string, _ os.FileMode) error {
						return nil
					}),
				gomonkey.ApplyFunc(localauth.Decrypt,
					func(_ string) ([]byte, error) {
						return []byte("secret"), nil
					}),
				gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
					return testUploadFile, nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(cli), "UploadFile", func(_ *commonObs.Client, input *obs.PutFileInput) error {
					return nil
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&SnapshotZstdHandler{}), "Compress", func(_ *SnapshotZstdHandler, zipFile string, src string) error {
					return nil
				}),
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("secret"), nil
				}),
				gomonkey.ApplyFunc((*s3Deployer).RefreshToken, func(_ *s3Deployer, conf *config.Configuration) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotConfig := generateSnapshotConfig()
			snapshotConfig.FastStartEnable = true
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			_, _, err = deployer.UploadSnapshot(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Access: "mockAccess",
				},
				SnapshotConfig: config.SnapshotConfig{FastStartEnable: true},
			}, "", "")
			convey.So(err, convey.ShouldBeNil)
			testUploadFile.Close()
			os.RemoveAll(testUploadFileName)
		})
	})
}

func generateSnapshotConfig() *config.Configuration {
	conf := &config.Configuration{
		DeploymentConfig: config.DeploymentConfig{
			BucketID:            "bucketID",
			ObjectID:            "objectID.zip",
			LayerInfo:           "layer",
			DeployDir:           "/opt/function/code",
			StorageType:         "s3",
			CodePackage:         config.CodePackage{},
			S3Config:            config.S3Config{},
			AuthConfig:          localauth.AuthConfig{},
			InternalHTTPSConfig: config.InternalHTTPSConfig{HTTPSEnable: true},
		},
		SnapshotConfig: config.SnapshotConfig{
			SnapshotBucketID: "snapshotBucketID",
			SnapshotObjectID: "snapshotObjectID.zip",
			SnapshotSize:     120,
		},
		RuntimeConfig: config.RuntimeConfig{
			URN: "urn",
		},
		ServerConfig: config.ServerConfig{IOReadTimeout: 3},
	}
	return conf
}

type fakeHasher struct {
}

func (fakeHasher) Write(b []byte) (int, error) { return len(b), nil }
func (fakeHasher) Sum(b []byte) []byte         { return nil }
func (fakeHasher) Reset()                      {}
func (fakeHasher) Size() int                   { return 0 }
func (fakeHasher) BlockSize() int              { return 0 }

func TestDeployFromOBS(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(utilities.ValidateFilePath, func(path string) error { return nil })
	p.ApplyFunc(utilities.IsDir, func(string) bool { return true })
	p.ApplyFunc(syscall.Umask, func(mask int) (oldmask int) { return 0 })
	p.ApplyFunc(os.MkdirAll, func(path string, perm fs.FileMode) error { return nil })
	cli := &commonObs.Client{}
	p.ApplyMethod(reflect.TypeOf(cli), "GetObjectMetadata", func(_ *commonObs.Client, input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
		return &obs.GetObjectMetadataOutput{
			ContentLength: 100,
		}, nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				CodePackage: config.CodePackage{
					ZipFileSizeMaxMB: 1,
				},
			},
		}, nil
	})
	p.ApplyMethod(reflect.TypeOf(cli), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
		return &obs.GetObjectOutput{}, nil
	})
	p.ApplyMethod(reflect.TypeOf(cli), "Refresh", func(_ *commonObs.Client, _ string, _, _ []byte) {
	})
	p.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
		return []byte(""), nil
	})
	p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) { return []byte{}, nil })

	httpClient := &http.Client{}
	p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		time.Sleep(1 * time.Second)
		return &http.Response{StatusCode: http.StatusInternalServerError}, errors.New("internal error")
	})

	hasher := fakeHasher{}
	p.ApplyFunc(sha256.New, func() hash.Hash { return hasher })

	checksum := "my checksum"
	p.ApplyFunc(hex.EncodeToString, func(src []byte) string { return checksum })

	var zstdHandler *ZstdHandler
	p.ApplyMethod(reflect.TypeOf(zstdHandler), "Decompress", func(_ *ZstdHandler, objectSize int64, downloadFilePath string, buffer []byte,
		extractPath string, isLayer bool) error {
		return nil
	})
	defer p.Reset()

	deployer := &s3Deployer{
		cli:           cli,
		httpClient:    httpClient,
		ioReadTimeout: 100,
	}
	_, err := deployer.Deploy(&config.Configuration{
		DeploymentConfig: config.DeploymentConfig{
			ObjectID: "test.tar.zst",
			Access:   "abc",
		},
		RuntimeConfig: config.RuntimeConfig{
			CodeSha512: checksum,
		},
	}, nil, "")
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)
}

func TestDeployFromCacheManager(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(utilities.ValidateFilePath, func(path string) error { return nil })
	p.ApplyFunc(utilities.IsDir, func(string) bool { return true })
	p.ApplyFunc(syscall.Umask, func(mask int) (oldmask int) { return 0 })
	p.ApplyFunc(os.MkdirAll, func(path string, perm fs.FileMode) error { return nil })
	cli := &commonObs.Client{}
	p.ApplyMethod(reflect.TypeOf(cli), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
		time.Sleep(50 * time.Millisecond)
		return nil, errors.New("get object failed")
	})
	p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) { return []byte{}, nil })

	httpClient := &http.Client{}
	p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		body := []byte(`"YWJj"`) // base64
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       ioutil.NopCloser(bytes.NewBuffer(body)),
		}, nil
	})

	hasher := fakeHasher{}
	p.ApplyFunc(sha256.New, func() hash.Hash { return hasher })

	checksum := "my checksum"
	p.ApplyFunc(hex.EncodeToString, func(src []byte) string { return checksum })

	var zstdHandler *ZstdHandler
	p.ApplyMethod(reflect.TypeOf(zstdHandler), "Decompress", func(_ *ZstdHandler, objectSize int64, downloadFilePath string, buffer []byte,
		extractPath string, isLayer bool) error {
		return nil
	})
	var zipHandler *ZipHandler
	p.ApplyMethod(reflect.TypeOf(zipHandler), "Decompress", func(_ *ZipHandler, objectSize int64, downloadFilePath string, buffer []byte,
		extractPath string, isLayer bool) error {
		return nil
	})
	defer p.Reset()

	deployer := &s3Deployer{
		cli:           cli,
		httpClient:    httpClient,
		ioReadTimeout: 100,
	}
	_, err := deployer.Deploy(&config.Configuration{
		DeploymentConfig: config.DeploymentConfig{
			ObjectID: "test.tar.zst",
		},
		RuntimeConfig: config.RuntimeConfig{
			CodeSha512: checksum,
		},
	}, nil, "")
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)
}

func Test_getCodeFromCacheManager(t *testing.T) {
	convey.Convey("Test getCodeFromCacheManager", t, func() {
		convey.Convey("test failed to do http request", func() {
			p := gomonkey.NewPatches()
			httpClient := &http.Client{}
			p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
				return nil, errors.New("failed to do http request")
			})
			p.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
				return "", nil
			})
			defer p.Reset()
			deployer := &s3Deployer{
				cli:           nil,
				httpClient:    httpClient,
				ioReadTimeout: 100,
			}
			_, _, err := deployer.getCodeFromCacheManager(&deployDetail{})
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test failed to request to cache", func() {
			p := gomonkey.NewPatches()
			httpClient := &http.Client{}
			p.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
				return "", nil
			})
			defer p.Reset()
			deployer := &s3Deployer{
				cli:           nil,
				httpClient:    httpClient,
				ioReadTimeout: 100,
			}
			_, _, err := deployer.getCodeFromCacheManager(&deployDetail{})
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test failed to make request", func() {
			p := gomonkey.NewPatches()
			httpClient := &http.Client{}
			p.ApplyFunc(http.NewRequest, func(method, url string, body io.Reader) (*http.Request, error) {
				return nil, fmt.Errorf("error")
			})
			p.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
				return "", nil
			})
			defer p.Reset()
			deployer := &s3Deployer{
				cli:           nil,
				httpClient:    httpClient,
				ioReadTimeout: 100,
			}
			_, _, err := deployer.getCodeFromCacheManager(&deployDetail{})
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test failed to download file to path", func() {
			p := gomonkey.NewPatches()
			httpClient := &http.Client{}
			p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
				body := []byte(`"test"`) // base64
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(bytes.NewBuffer(body)),
				}, nil
			})
			p.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
				return "", nil
			})
			p.ApplyFunc(commonObs.DownloadFileToPath, func(downloadPath string, objectOutput *obs.GetObjectOutput, isLayer bool) (int64, error) {
				return 100, fmt.Errorf("error")
			})
			defer p.Reset()
			deployer := &s3Deployer{
				cli:           nil,
				httpClient:    httpClient,
				ioReadTimeout: 100,
			}
			_, _, err := deployer.getCodeFromCacheManager(&deployDetail{
				ObjectID: "test.zip",
				Checksum: "test",
				Path:     "/test",
				CodeSize: 4194310,
			})
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test failed to chmod", func() {
			p := gomonkey.NewPatches()
			httpClient := &http.Client{}
			p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
				body := []byte(`"test"`) // base64
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(bytes.NewBuffer(body)),
				}, nil
			})
			p.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
				return "", nil
			})
			p.ApplyFunc(commonObs.DownloadFileToPath, func(downloadPath string, objectOutput *obs.GetObjectOutput, isLayer bool) (int64, error) {
				return 100, nil
			})
			p.ApplyFunc(os.Chmod, func(name string, mode fs.FileMode) error { return fmt.Errorf("error") })
			defer p.Reset()
			deployer := &s3Deployer{
				cli:           nil,
				httpClient:    httpClient,
				ioReadTimeout: 100,
			}
			_, _, err := deployer.getCodeFromCacheManager(
				&deployDetail{
					ObjectID: "test.zip",
					Checksum: "test",
					Path:     "/test",
					CodeSize: 4194310,
				})
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test succeed to get code from cache", func() {
			p := gomonkey.NewPatches()
			httpClient := &http.Client{}
			p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
				body := []byte(`"test"`) // base64
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       ioutil.NopCloser(bytes.NewBuffer(body)),
				}, nil
			})
			p.ApplyFunc(commonObs.DownloadFileToPath, func(downloadPath string, objectOutput *obs.GetObjectOutput, isLayer bool) (int64, error) {
				return 100, nil
			})
			p.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
				return "", nil
			})
			p.ApplyFunc(os.Chmod, func(name string, mode fs.FileMode) error { return nil })
			p.ApplyFunc(ioutil.ReadAll, func(r io.Reader) ([]byte, error) {
				return []byte(""), nil
			})
			defer p.Reset()
			deployer := &s3Deployer{
				cli:           nil,
				httpClient:    httpClient,
				ioReadTimeout: 100,
			}
			_, _, err := deployer.getCodeFromCacheManager(&deployDetail{})
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_getCodeFromCacheManager2(t *testing.T) {
	p := gomonkey.NewPatches()
	httpClient := &http.Client{}
	p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return nil, errors.New("failed to do http request")
	})
	defer p.Reset()
	deployer := &s3Deployer{
		cli:           nil,
		httpClient:    httpClient,
		ioReadTimeout: 100,
	}
	_, _, err := deployer.getCodeFromCacheManager(&deployDetail{})
	assert.Equal(t, errors.New("failed to do http request"), err)
}

func TestDeployTimeout(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(utilities.ValidateFilePath, func(path string) error { return nil })
	p.ApplyFunc(utilities.IsDir, func(string) bool { return true })
	p.ApplyFunc(syscall.Umask, func(mask int) (oldmask int) { return 0 })
	p.ApplyFunc(os.MkdirAll, func(path string, perm fs.FileMode) error { return nil })
	cli := &commonObs.Client{}
	p.ApplyMethod(reflect.TypeOf(cli), "GetObjectMetadata", func(_ *commonObs.Client, input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
		return &obs.GetObjectMetadataOutput{
			ContentLength: 100,
		}, nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				CodePackage: config.CodePackage{
					ZipFileSizeMaxMB: 1,
				},
			},
		}, nil
	})
	p.ApplyMethod(reflect.TypeOf(cli), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
		time.Sleep(1 * time.Second)
		return nil, errors.New("get object error")
	})
	p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) { return []byte{}, nil })

	httpClient := &http.Client{}
	p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		time.Sleep(1 * time.Second)
		return &http.Response{StatusCode: http.StatusInternalServerError}, errors.New("internal error")
	})

	hasher := fakeHasher{}
	p.ApplyFunc(sha256.New, func() hash.Hash { return hasher })

	checksum := "my checksum"
	p.ApplyFunc(hex.EncodeToString, func(src []byte) string { return checksum })

	var zstdHandler *ZstdHandler
	p.ApplyMethod(reflect.TypeOf(zstdHandler), "Decompress", func(_ *ZstdHandler, objectSize int64, downloadFilePath string, buffer []byte,
		extractPath string, isLayer bool) error {
		return nil
	})
	p.ApplyFunc(closeConnectionToCacheMgr, func() {
	})
	defer p.Reset()

	deployer := &s3Deployer{
		cli:           cli,
		httpClient:    httpClient,
		ioReadTimeout: 0,
	}
	_, err := deployer.Deploy(&config.Configuration{
		DeploymentConfig: config.DeploymentConfig{
			ObjectID: "test.tar.zst",
		},
		RuntimeConfig: config.RuntimeConfig{
			CodeSha512: checksum,
		},
	}, nil, "")
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "timed out"))

	time.Sleep(1 * time.Second)
}

func TestDeployLayers(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(utilities.ValidateFilePath, func(path string) error { return nil })
	p.ApplyFunc(utilities.IsDir, func(string) bool { return true })
	p.ApplyFunc(syscall.Umask, func(mask int) (oldmask int) { return 0 })
	p.ApplyFunc(os.MkdirAll, func(path string, perm fs.FileMode) error { return nil })
	cli := &commonObs.Client{}
	p.ApplyMethod(reflect.TypeOf(cli), "GetObjectMetadata", func(_ *commonObs.Client, input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
		return &obs.GetObjectMetadataOutput{
			ContentLength: 100,
		}, nil
	})
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{
			DeploymentConfig: config.DeploymentConfig{
				CodePackage: config.CodePackage{
					ZipFileSizeMaxMB: 100,
				},
			},
		}, nil
	})
	p.ApplyMethod(reflect.TypeOf(cli), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
		return &obs.GetObjectOutput{
			GetObjectMetadataOutput: obs.GetObjectMetadataOutput{
				ContentLength: readAllLimitSize + 1, // larger than readAllLimitSize
			},
		}, nil
	})
	p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) { return []byte{}, nil })

	httpClient := &http.Client{}
	p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		time.Sleep(1 * time.Second)
		return &http.Response{StatusCode: http.StatusInternalServerError}, errors.New("internal error")
	})

	hasher := fakeHasher{}
	p.ApplyFunc(sha256.New, func() hash.Hash { return hasher })

	checksum := "my checksum"
	p.ApplyFunc(hex.EncodeToString, func(src []byte) string { return checksum })

	var zstdHandler *ZstdHandler
	p.ApplyMethod(reflect.TypeOf(zstdHandler), "Decompress", func(_ *ZstdHandler, objectSize int64, downloadFilePath string, buffer []byte,
		extractPath string, isLayer bool) error {
		return nil
	})
	var zipHandler *ZipHandler
	p.ApplyMethod(reflect.TypeOf(zipHandler), "Decompress", func(_ *ZipHandler, objectSize int64, downloadFilePath string, buffer []byte,
		extractPath string, isLayer bool) error {
		return nil
	})
	p.ApplyFunc(closeConnectionToCacheMgr, func() {
	})
	p.ApplyFunc(commonObs.DownloadFileToPathBySlice, func(bucketID string, objectID string, objPath string, objSize int64, client commonObs.Client) error {
		return nil
	})
	gomonkey.ApplyFunc(commonObs.DownloadFileToPath, func(downloadPath string, objectOutput *obs.GetObjectOutput, isLayer bool) (int64, error) {
		return 100, nil
	})
	p.ApplyFunc(os.Chmod, func(name string, mode fs.FileMode) error { return nil })
	f := &os.File{}
	p.ApplyFunc(os.Open, func(name string) (*os.File, error) { return f, nil })
	p.ApplyMethod(reflect.TypeOf(f), "Close", func(_ *os.File) error { return nil })
	p.ApplyFunc(io.Copy, func(dst io.Writer, src io.Reader) (written int64, err error) { return 0, nil })
	p.ApplyFunc((*exec.Cmd).Run, func(_ *exec.Cmd) error {
		return errors.New("error")
	})
	defer p.Reset()

	layers := []*commontypes.Layer{
		{
			ObjectID:       "test_layer.txt",
			DependencyType: "public",
		},
	}
	layersEnvPlain, err := json.Marshal(layers)
	if err != nil {
		t.Fatal(err)
	}
	layersEnv := base64.StdEncoding.EncodeToString(layersEnvPlain)

	deployer := &s3Deployer{
		cli:           cli,
		httpClient:    httpClient,
		ioReadTimeout: 100,
	}
	_, err = deployer.Deploy(&config.Configuration{
		DeploymentConfig: config.DeploymentConfig{
			ObjectID:  "test.tar.zst",
			LayerInfo: layersEnv,
		},
		RuntimeConfig: config.RuntimeConfig{
			CodeSha512: checksum,
		},
	}, nil, "")
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)
}

func TestDeployLayersDownloadError(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
		return &config.Configuration{}, nil
	})
	p.ApplyFunc(utilities.ValidateFilePath, func(path string) error { return nil })
	p.ApplyFunc(utilities.IsDir, func(string) bool { return true })
	p.ApplyFunc(syscall.Umask, func(mask int) (oldmask int) { return 0 })
	p.ApplyFunc(os.MkdirAll, func(path string, perm fs.FileMode) error { return nil })
	p.ApplyFunc(closeConnectionToCacheMgr, func() {
	})
	cli := &commonObs.Client{}
	p.ApplyMethod(reflect.TypeOf(cli), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
		time.Sleep(100 * time.Millisecond)
		return nil, errors.New("get object error")
	})

	httpClient := &http.Client{}
	p.ApplyMethod(reflect.TypeOf(httpClient), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusInternalServerError}, errors.New("internal error")
	})
	defer p.Reset()

	layers := []*commontypes.Layer{
		{
			ObjectID:       "test_layer.txt",
			DependencyType: "public",
		},
	}
	layersEnvPlain, err := json.Marshal(layers)
	if err != nil {
		t.Fatal(err)
	}
	layersEnv := base64.StdEncoding.EncodeToString(layersEnvPlain)

	deployer := &s3Deployer{
		cli:           cli,
		httpClient:    httpClient,
		ioReadTimeout: 200,
	}
	_, err = deployer.Deploy(&config.Configuration{
		DeploymentConfig: config.DeploymentConfig{
			ObjectID:  "test.tar.zst",
			LayerInfo: layersEnv,
		},
		RuntimeConfig: config.RuntimeConfig{
			CodeSha512: "abc",
		},
	}, nil, "")
	assert.NotNil(t, err)
	snerr, ok := err.(snerror.SNError)
	assert.True(t, ok)
	assert.Equal(t, snerror.DownLoadCodeErrorCode, snerr.Code())

	time.Sleep(2 * time.Second)
}

func Test_initLogger(t *testing.T) {
	convey.Convey("initLogger", t, func() {
		convey.Convey("test 1", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(log.GetLogger,
					func() log.FormatLogger {
						return nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			var err error = nil
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

type fakeFile struct{}

func (fakeFile) Name() string       { return "" }
func (fakeFile) Size() int64        { return 0 }
func (fakeFile) Mode() fs.FileMode  { return 0755 }
func (fakeFile) ModTime() time.Time { return time.Time{} }
func (fakeFile) IsDir() bool        { return false }
func (fakeFile) Sys() interface{}   { return nil }

func Test_linkPublicDependencies(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	p.ApplyFunc(os.Chown, func(name string, uid, gid int) error { return nil })
	p.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return []fs.FileInfo{fakeFile{}}, nil
	})
	p.ApplyFunc(os.Symlink, func(oldname, newname string) error { return nil })
	p.ApplyFuncSeq(os.Stat, []gomonkey.OutputCell{
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{nil, os.ErrNotExist}},
		{Values: gomonkey.Params{nil, os.ErrNotExist}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
	})
	defer p.Reset()

	layer := &commontypes.Layer{
		Name: "dependency1",
	}
	ok, err := linkPublicDependencies("nodejs12.13", layer, "/opt/function/code")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, ok)

	ok, err = linkPublicDependencies("nodejs12.13", layer, "/opt/function/code")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, ok)

	ok, err = linkPublicDependencies("java8", layer, "/opt/function/code")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, ok)

	ok, err = linkPublicDependencies("python3.6", layer, "/opt/function/code")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, ok)

	ok, err = linkPublicDependencies("php7.3", layer, "/opt/function/code")
	assert.Equal(t, nil, err)
	assert.Equal(t, false, ok)

	ok, err = linkPublicDependencies("cangjie1.0", layer, "/opt/function/code")
	assert.Equal(t, nil, err)
	assert.Equal(t, true, ok)
}

func Test_linkPublicDependencies2(t *testing.T) {
	p := gomonkey.NewPatches()
	mkdirErr := errors.New("mkdir err")
	p.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return mkdirErr })
	p.ApplyFunc(exists, func(dir string) bool { return true })
	defer p.Reset()

	layer := &commontypes.Layer{
		Name: "dependency1",
	}
	_, err := linkPublicDependencies("nodejs12.13", layer, "/opt/function/code")
	assert.Equal(t, mkdirErr, err)
}

func Test_linkPublicDependencies3(t *testing.T) {
	p := gomonkey.NewPatches()
	chownErr := errors.New("chown err")
	p.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	p.ApplyFunc(os.Chown, func(name string, uid, gid int) error { return chownErr })
	p.ApplyFunc(exists, func(dir string) bool { return true })
	defer p.Reset()

	layer := &commontypes.Layer{
		Name: "dependency1",
	}
	_, err := linkPublicDependencies("nodejs12.13", layer, "/opt/function/code")
	assert.Equal(t, chownErr, err)
}

func Test_linkPublicDependencies4(t *testing.T) {
	p := gomonkey.NewPatches()
	readdirErr := errors.New("readdir err")
	p.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	p.ApplyFunc(os.Chown, func(name string, uid, gid int) error { return nil })
	p.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return nil, readdirErr
	})
	p.ApplyFunc(exists, func(dir string) bool { return true })
	defer p.Reset()

	layer := &commontypes.Layer{
		Name: "dependency1",
	}
	_, err := linkPublicDependencies("nodejs12.13", layer, "/opt/function/code")
	assert.Equal(t, readdirErr, err)
}

func Test_linkPublicDependencies5(t *testing.T) {
	p := gomonkey.NewPatches()
	symlinkErr := errors.New("symlink err")
	p.ApplyFunc(os.Mkdir, func(name string, perm os.FileMode) error { return nil })
	p.ApplyFunc(os.Chown, func(name string, uid, gid int) error { return nil })
	p.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
		return []fs.FileInfo{fakeFile{}}, nil
	})
	p.ApplyFunc(os.Symlink, func(oldname, newname string) error { return symlinkErr })
	p.ApplyFuncSeq(os.Stat, []gomonkey.OutputCell{
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{fakeFile{}, nil}},
		{Values: gomonkey.Params{nil, os.ErrNotExist}},
	})
	defer p.Reset()

	layer := &commontypes.Layer{
		Name: "dependency1",
	}
	_, err := linkPublicDependencies("nodejs12.13", layer, "/opt/function/code")
	assert.Equal(t, symlinkErr, err)
}

func Test_checkBufferHash(t *testing.T) {
	p := gomonkey.NewPatches()
	hasher := fakeHasher{}
	p.ApplyFunc(sha256.New, func() hash.Hash { return hasher })
	p.ApplyMethod(reflect.TypeOf(hasher), "Write", func(_ fakeHasher, p []byte) (n int, err error) {
		return 0, errors.New("my error")
	})
	defer p.Reset()

	err := checkBufferHash(nil, "")
	assert.NotNil(t, err)
}

func Test_checkBufferHash2(t *testing.T) {
	p := gomonkey.NewPatches()
	hasher := fakeHasher{}
	p.ApplyFunc(sha256.New, func() hash.Hash { return hasher })
	p.ApplyFunc(hex.EncodeToString, func(src []byte) string { return "my checksum" })
	defer p.Reset()

	err := checkBufferHash(nil, "wrong checksum")
	assert.NotNil(t, err)
}

func Test_checkHash(t *testing.T) {
	err := checkHash(0, "", nil, "")
	assert.Nil(t, err)
}

func Test_checkFileHash(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(os.Open, func(name string) (*os.File, error) { return nil, errors.New("my error") })
	defer p.Reset()

	err := checkFileHash("", "")
	assert.NotNil(t, err)
}

func Test_checkFileHash2(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(os.Open, func(name string) (*os.File, error) { return &os.File{}, nil })
	p.ApplyFunc(io.Copy, func(dst io.Writer, src io.Reader) (written int64, err error) {
		return 0, errors.New("my error")
	})
	defer p.Reset()

	err := checkFileHash("", "")
	assert.NotNil(t, err)
}

func Test_checkFileHash3(t *testing.T) {
	p := gomonkey.NewPatches()
	p.ApplyFunc(os.Open, func(name string) (*os.File, error) { return &os.File{}, nil })
	p.ApplyFunc(io.Copy, func(dst io.Writer, src io.Reader) (written int64, err error) {
		return 0, nil
	})
	p.ApplyFunc(hex.EncodeToString, func(src []byte) string { return "my checksum" })
	defer p.Reset()

	err := checkFileHash("", "wrong checksum")
	assert.NotNil(t, err)
}

func Test_s3Deployer_getCodeFromObs(t *testing.T) {
	type fields struct {
		cli              *commonObs.Client
		httpClient       *http.Client
		ioReadTimeout    int
		cacheHTTPSEnable bool
	}
	type args struct {
		bucketID  string
		objectID  string
		path      string
		layerName string
		language  string
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantBuffer    []byte
		wantObjSize   int64
		wantErr       bool
		patchesFunc   mockUtils.PatchesFunc
		wantErrString string
	}{
		{"case1 GetObjectMetadata error", fields{cli: &commonObs.Client{}}, args{
			bucketID:  "bucket",
			objectID:  "object",
			path:      "/opt",
			layerName: "jalor-test",
			language:  "java8",
		}, nil, 0, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*commonObs.Client).GetObject,
					func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return nil, errors.New("failed to GetObject")
					}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						DeploymentConfig: config.DeploymentConfig{
							CodePackage: config.CodePackage{
								ZipFileSizeMaxMB: 100,
							},
						},
					}, nil
				}),
				gomonkey.ApplyFunc((*commonObs.Client).GetObjectMetadata,
					func(_ *commonObs.Client,
						input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
						return &obs.GetObjectMetadataOutput{ContentLength: 4000}, nil
					}),
			})
			return patches
		}, "download code from obs error: failed to GetObject, bucket: bucket, object: object, layer: jalor-test"},
		{"case2 DownloadFileToPathBySlice fail", fields{cli: &commonObs.Client{}}, args{
			bucketID:  "bucket",
			objectID:  "object",
			path:      "/opt",
			layerName: "jalor-test",
			language:  "java8",
		}, nil, 0, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(commonObs.DownloadFileToPathBySlice, func(bucketID string,
					objectID string, objPath string, objSize int64, client commonObs.Client) error {
					return nil
				}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						DeploymentConfig: config.DeploymentConfig{
							CodePackage: config.CodePackage{
								ZipFileSizeMaxMB: 100,
							},
						},
					}, nil
				}),
				gomonkey.ApplyFunc((*commonObs.Client).GetObjectMetadata,
					func(_ *commonObs.Client,
						input *obs.GetObjectMetadataInput) (output *obs.GetObjectMetadataOutput, err error) {
						return &obs.GetObjectMetadataOutput{ContentLength: commonObs.PartSize + 1}, nil
					}),
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return errors.New("failed to Chmod")
				}),
			})
			return patches
		}, "failed to Chmod"},
		{"case3 Chmod fail", fields{cli: &commonObs.Client{}}, args{
			bucketID:  "bucket",
			objectID:  "object",
			path:      "/opt",
			layerName: "",
			language:  "",
		}, nil, 4194305, true, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc((*commonObs.Client).GetObject,
					func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						output = &obs.GetObjectOutput{}
						output.ContentLength = readAllLimitSize + 1
						return output, nil
					}),
				gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
					return &config.Configuration{
						DeploymentConfig: config.DeploymentConfig{
							CodePackage: config.CodePackage{
								ZipFileSizeMaxMB: 100,
							},
						},
					}, nil
				}),
				gomonkey.ApplyFunc(commonObs.DownloadFileToPath,
					func(downloadPath string, objectOutput *obs.GetObjectOutput, isLayer bool) (int64, error) {
						return readAllLimitSize + 1, nil
					}),
				gomonkey.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
					return errors.New("failed to Chmod")
				}),
			})
			return patches
		}, "failed to Chmod"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			d := &s3Deployer{
				cli:              tt.fields.cli,
				httpClient:       tt.fields.httpClient,
				ioReadTimeout:    tt.fields.ioReadTimeout,
				cacheHTTPSEnable: tt.fields.cacheHTTPSEnable,
			}
			gotBuffer, gotObjSize, err := d.getCodeFromObs(tt.args.bucketID,
				tt.args.objectID, tt.args.path, tt.args.layerName, tt.args.language)
			if (err != nil) != tt.wantErr {
				t.Errorf("getCodeFromObs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				if err.Error() != tt.wantErrString {
					t.Errorf("getCodeFromObs() error = %v, wantErr %v", err, tt.wantErrString)
					return
				}
			}
			if !reflect.DeepEqual(gotBuffer, tt.wantBuffer) {
				t.Errorf("getCodeFromObs() gotBuffer = %v, want %v", gotBuffer, tt.wantBuffer)
			}
			if gotObjSize != tt.wantObjSize {
				t.Errorf("getCodeFromObs() gotObjSize = %v, want %v", gotObjSize, tt.wantObjSize)
			}
			patches.ResetAll()
		})
	}
}

type fakeReaderCloser struct {
	readError  bool
	closeError bool
}

func (f *fakeReaderCloser) Read(p []byte) (n int, err error) {
	if f.readError {
		return 0, errors.New("")
	}
	return len(p), io.EOF
}

func (f *fakeReaderCloser) Close() error {
	if f.closeError {
		return errors.New("")
	}
	return nil
}

func Test_preconnectCache(t *testing.T) {
	cfg := &config.Configuration{}
	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name: "discard error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{Body: &fakeReaderCloser{readError: true}}, nil
					}),
					gomonkey.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
						return "", nil
					}),
				})
				return patches
			},
		},
		{
			name: "body close error",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{Body: &fakeReaderCloser{closeError: true}}, nil
					}),
					gomonkey.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
						return "", nil
					}),
				})
				return patches
			},
		},
		{
			name: "request accept",
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{Body: &fakeReaderCloser{}}, nil
					}),
					gomonkey.ApplyFunc((*s3Deployer).determineCacheManagerSafetyVPC, func(_ *s3Deployer, objectID string) (string, error) {
						return "", nil
					}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			c, err := preconnectCache(cfg)
			assert.NotNil(t, c)
			assert.Nil(t, err)
			time.Sleep(100 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func Test_refreshToken_Error(t *testing.T) {
	patch := gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
		if src == "error" {
			return nil, errors.New("failed to decrypt")
		}
		return []byte(src), nil
	})
	defer patch.Reset()

	tests := []struct {
		name   string
		sKey   string
		sToken string
	}{
		{
			name: "secret key error",
			sKey: "error",
		},
		{
			name:   "security token error",
			sToken: "error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := &config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Secret:        tt.sKey,
					SecurityToken: tt.sToken,
				},
			}
			err := (&s3Deployer{}).RefreshToken(conf)
			assert.Error(t, err)
		})
	}
}

func Test_DeploySnapshot(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Configuration, error) {
				return &config.Configuration{
					DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
						AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
						InternalHTTPSConfig: config.InternalHTTPSConfig{
							HTTPSEnable: false,
							TLSProtocol: "",
							TLSCiphers:  "",
						}},
				}, nil
			}),
		gomonkey.ApplyFunc(commonObs.NewObsClient,
			func(opts commonObs.Option) (*commonObs.Client, error) {
				return &commonObs.Client{}, nil
			}),
		gomonkey.ApplyFunc(os.MkdirAll,
			func(_ string, _ os.FileMode) error {
				return nil
			}),
		gomonkey.ApplyFunc(localauth.Decrypt,
			func(_ string) ([]byte, error) {
				return []byte("secret"), nil
			}),
		gomonkey.ApplyFunc(utilities.IsDir, func(_ string) bool {
			return true
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&SnapshotZipHandler{}), "Decompress", func(_ *SnapshotZipHandler, objectSize int64, downloadFilePath string, buffer []byte,
			extractPath string, isLayer bool) error {
			return nil
		}),
	}
	defer func() {
		for idx := range patches {
			patches[idx].Reset()
		}
	}()

	tests := []struct {
		name        string
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		{
			name: "deploy failed",
			patchesFunc: func() mockUtils.PatchSlice {
				p := mockUtils.InitPatchSlice()
				p.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{Body: &fakeReaderCloser{readError: true}}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return nil, errors.New("failed to get object")
					}),
				})
				return p
			},
			wantErr: true,
		},
		{
			name: "deploy success",
			patchesFunc: func() mockUtils.PatchSlice {
				p := mockUtils.InitPatchSlice()
				p.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{Body: &fakeReaderCloser{}}, nil
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return &obs.GetObjectOutput{Body: &fakeReaderCloser{}}, nil
					}),
				})
				return p
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localPatches := tt.patchesFunc()
			snapshotConfig := generateSnapshotConfig()
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			snapshotConfig.DeployDir = "/opt/function/code"
			_, err = deployer.DeploySnapshot(snapshotConfig, nil, "")
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
			}
			localPatches.ResetAll()
		})
	}
}

func Test_parseLimitCodeSize(t *testing.T) {
	tests := []struct {
		name    string
		patches mockUtils.PatchesFunc

		excepted int64
	}{
		{
			name: "env not exist",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return ""
					}),
				})
				return patches
			},
			excepted: cacheLimitSize,
		},
		{
			name: "env exist, but cannot parse int",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "abcd"
					}),
				})
				return patches
			},
			excepted: cacheLimitSize,
		},
		{
			name: "env exist, can parse int",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "1024"
					}),
				})
				return patches
			},
			excepted: 1024,
		},
		{
			name: "env exist, over read all limit size",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(os.Getenv, func(key string) string {
						return "5000000"
					}),
				})
				return patches
			},
			excepted: readAllLimitSize,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patches()
			res := parseLimitCodeSize()
			assert.Equal(t, tt.excepted, res)
			patches.ResetAll()
		})
	}
}

func Test_deploy_large_code(t *testing.T) {
	gPatches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Configuration, error) {
				cfg := &config.Configuration{}
				cfg.DeploymentConfig.CodePackage.ZipFileSizeMaxMB = 300
				return cfg, nil
			}),
		gomonkey.ApplyFunc(bufio.NewReaderSize, func(io.Reader, int) *bufio.Reader {
			return &bufio.Reader{}
		}),
		gomonkey.ApplyFunc((*bufio.Reader).Read, func(*bufio.Reader, []byte) (n int, err error) {
			return 0, io.EOF
		}),
	}
	defer func() {
		for idx := range gPatches {
			gPatches[idx].Reset()
		}
	}()
	s := s3Deployer{
		cli:        &commonObs.Client{},
		httpClient: &http.Client{},
	}

	tests := []struct {
		name    string
		patches mockUtils.PatchesFunc

		exceptedErr    bool
		exceptedErrStr string
	}{
		{
			name: "get from obs success",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return &obs.GetObjectOutput{Body: &fakeReaderCloser{}}, nil
					}),
				})
				return patches
			},
			exceptedErr: false,
		},
		{
			name: "get from obs failed, then get from cache",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return nil, errors.New("failed to get from obs")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return &http.Response{StatusCode: http.StatusOK, Body: &fakeReaderCloser{}}, nil
					}),
				})
				return patches
			},
			exceptedErr: false,
		},
		{
			name: "all failed",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return nil, errors.New("failed to get from obs")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("failed to get from cache")
					}),
				})
				return patches
			},
			exceptedErr:    true,
			exceptedErrStr: "failed to download code from OBS and cache-manager",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patches()
			_, _, err := s.generateLargeCodeDeployFunc(&deployDetail{}, s.getCodeFromObs, "", nil)()
			if tt.exceptedErr {
				assert.Error(t, err)
				assert.Equal(t, err.Error(), tt.exceptedErrStr)
			} else {
				assert.Nil(t, err)
			}
			patches.ResetAll()
		})
	}
}

func Test_deploy_failed_return_obs_error(t *testing.T) {
	gPatches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Configuration, error) {
				cfg := &config.Configuration{}
				cfg.DeploymentConfig.CodePackage.ZipFileSizeMaxMB = 300
				return cfg, nil
			}),
	}
	defer func() {
		for idx := range gPatches {
			gPatches[idx].Reset()
		}
	}()
	s := s3Deployer{
		cli:           &commonObs.Client{},
		httpClient:    &http.Client{},
		ioReadTimeout: 1000,
	}

	tests := []struct {
		name    string
		patches mockUtils.PatchesFunc
	}{
		{
			name: "obs error first",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return nil, errors.New("failed to get from obs")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						time.Sleep(100 * time.Millisecond)
						return nil, errors.New("failed to get from cache")
					}),
				})
				return patches
			},
		},
		{
			name: "cache error first",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						time.Sleep(100 * time.Millisecond)
						return nil, errors.New("failed to get from obs")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("failed to get from cache")
					}),
				})
				return patches
			},
		},
		{
			name: "random",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
						return nil, errors.New("failed to get from obs")
					}),
					gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
						return nil, errors.New("failed to get from cache")
					}),
				})
				return patches
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patches()
			err := s.deploy(&DurationInfo{}, &deployDetail{}, "", nil, false)
			snErr, ok := err.(snerror.SNError)
			assert.True(t, ok, "should return sn error")
			assert.Equal(t, snerror.DownLoadCodeErrorCode, snErr.Code())
			patches.ResetAll()
		})
	}
}

func Test_deploy_timeout_return_error(t *testing.T) {
	patches := [...]*gomonkey.Patches{
		gomonkey.ApplyFunc(config.GetConfig,
			func() (*config.Configuration, error) {
				cfg := &config.Configuration{}
				cfg.DeploymentConfig.CodePackage.ZipFileSizeMaxMB = 300
				return cfg, nil
			}),
		gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
			time.Sleep(100 * time.Millisecond)
			return nil, errors.New("failed to get from obs")
		}),
		gomonkey.ApplyMethod(reflect.TypeOf(&http.Client{}), "Do", func(_ *http.Client, req *http.Request) (*http.Response, error) {
			time.Sleep(100 * time.Millisecond)
			return nil, errors.New("failed to get from cache")
		}),
		gomonkey.ApplyFunc(time.After, func(_ time.Duration) <-chan time.Time {
			c := make(chan time.Time, 1)
			c <- time.Time{}
			return c
		}),
	}
	defer func() {
		// assert background goroutine has called
		time.Sleep(100 * time.Millisecond)
		for idx := range patches {
			patches[idx].Reset()
		}
	}()
	s := s3Deployer{
		cli:           &commonObs.Client{},
		httpClient:    &http.Client{},
		ioReadTimeout: 1000,
	}
	err := s.deploy(&DurationInfo{}, &deployDetail{}, "", nil, false)
	snErr, ok := err.(snerror.SNError)
	assert.True(t, ok, "should return sn error")
	assert.Equal(t, snerror.WorkerInternalErrorCode, snErr.Code())
	assert.Equal(t, "deploy code timed out", snErr.Error())
}

func Test_deployUnzippedObject_and_getCodeFromObs(t *testing.T) {
	s := s3Deployer{
		cli:                    &commonObs.Client{},
		httpClient:             &http.Client{},
		ioReadTimeout:          1000,
		codePlaintextKmsCipher: "test",
	}
	t.Run("test GetOutputObjectBuffer failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
			cfg := &config.Configuration{}
			cfg.DeploymentConfig.CodePackage.ZipFileSizeMaxMB = 300
			return cfg, nil
		})
		defer p.Reset()
		p.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
			time.Sleep(100 * time.Millisecond)
			return &obs.GetObjectOutput{}, nil
		})
		p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
			return []byte{}, errors.New("test")
		})
		_, _, err := s.deployUnzippedObject("test", "test", "test", "", "")
		assert.Error(t, err)
		_, _, err = s.getCodeFromObs("test", "test", "test", "", "")
		assert.Error(t, err)
	})

	t.Run("test DecryptDataWithAESByPlainCipherToByte failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
			cfg := &config.Configuration{}
			cfg.DeploymentConfig.CodePackage.ZipFileSizeMaxMB = 300
			return cfg, nil
		})
		defer p.Reset()
		p.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
			time.Sleep(100 * time.Millisecond)
			return &obs.GetObjectOutput{}, nil
		})
		p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
			return []byte{}, nil
		})
		p.ApplyFunc(kms.DecryptDataWithAESByPlainCipherToByte, func(data, plainCipher string) ([]byte, error) {
			return []byte{}, errors.New("test")
		})
		_, _, err := s.deployUnzippedObject("test", "test", "test", "", "")
		assert.Error(t, err)
		_, _, err = s.getCodeFromObs("test", "test", "test", "", "")
		assert.Error(t, err)
	})

	t.Run("test DownloadAllFileToPath failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
			cfg := &config.Configuration{}
			cfg.DeploymentConfig.CodePackage.ZipFileSizeMaxMB = 300
			return cfg, nil
		})
		defer p.Reset()
		p.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
			time.Sleep(100 * time.Millisecond)
			return &obs.GetObjectOutput{
				GetObjectMetadataOutput: obs.GetObjectMetadataOutput{
					ContentLength: 4194310,
				},
			}, nil
		})
		p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
			return []byte{}, nil
		})
		p.ApplyFunc(kms.DecryptDataWithAESByPlainCipherToByte, func(data, plainCipher string) ([]byte, error) {
			return []byte{}, nil
		})
		p.ApplyFunc(commonObs.DownloadAllFileToPath, func(downloadPath string, body []byte) error {
			return errors.New("test")
		})
		_, _, err := s.deployUnzippedObject("test", "test", "test", "", "")
		assert.Error(t, err)
		_, _, err = s.getCodeFromObs("test", "test", "test", "", "")
		assert.Error(t, err)
	})

	t.Run("test chmod failed", func(t *testing.T) {
		p := gomonkey.ApplyFunc(config.GetConfig, func() (*config.Configuration, error) {
			cfg := &config.Configuration{}
			cfg.DeploymentConfig.CodePackage.ZipFileSizeMaxMB = 300
			return cfg, nil
		})
		defer p.Reset()
		p.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "GetObject", func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
			time.Sleep(100 * time.Millisecond)
			return &obs.GetObjectOutput{}, nil
		})
		p.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
			return []byte{}, nil
		})
		p.ApplyFunc(kms.DecryptDataWithAESByPlainCipherToByte, func(data, plainCipher string) ([]byte, error) {
			return []byte{}, nil
		})
		p.ApplyFunc(commonObs.DownloadAllFileToPath, func(downloadPath string, body []byte) error {
			return nil
		})
		p.ApplyFunc(os.Chmod, func(name string, mode os.FileMode) error {
			return errors.New("test")
		})
		_, _, err := s.deployUnzippedObject("test", "test", "test", "", "")
		assert.Error(t, err)
	})
}

func Test_determineCacheManagerSafetyVPC(t *testing.T) {
	tests := []struct {
		name      string
		patches   mockUtils.PatchesFunc
		s         s3Deployer
		ExpectErr bool
		result    string
	}{
		{
			name: "test isSafety false",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			s: s3Deployer{
				isSafety:         false,
				cacheHTTPSEnable: true,
			},
			ExpectErr: false,
			result:    "cache-manager",
		},
		{
			name: "test isSafety true, cacheManagerServiceIP nil",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			s: s3Deployer{
				isSafety:         true,
				cacheHTTPSEnable: true,
			},
			ExpectErr: true,
			result:    "127.0.0.1",
		},
		{
			name: "test isSafety true, cacheManagerServiceIP nil",
			patches: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			},
			s: s3Deployer{
				isSafety:              true,
				cacheHTTPSEnable:      true,
				cacheManagerServiceIP: "127.0.0.1",
			},
			ExpectErr: false,
			result:    "127.0.0.1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patches()
			defer patches.ResetAll()
			url, err := tt.s.determineCacheManagerSafetyVPC("test_object")
			if err == nil == tt.ExpectErr || (!strings.Contains(url, tt.result) && url != "") {
				t.Errorf("error")
			}
		})
	}
}

func Test_SetCacheManagerServiceIP(t *testing.T) {
	tests := []struct {
		name string
		s    s3Deployer
		c    *config.Configuration
	}{
		{
			name: "test conf is nil",
			s: s3Deployer{
				isSafety:         false,
				cacheHTTPSEnable: true,
			},
			c: nil,
		},
		{
			name: "test clear",
			s: s3Deployer{
				isSafety:         false,
				cacheHTTPSEnable: true,
			},
			c: &config.Configuration{CacheManagerClusterID: "127.0.0.1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.SetCacheManagerServiceIP(tt.c)
		})
	}
}

func TestDownloadSnapshotWithRetry(t *testing.T) {
	convey.Convey("download Snapshot", t, func() {
		convey.Convey("test download snapshot error", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&SnapshotZipHandler{}), "Compress", func(_ *SnapshotZipHandler, zipFile string, src string) error {
					return nil
				}),
				gomonkey.ApplyFunc((*s3Deployer).RefreshToken, func(_ *s3Deployer, conf *config.Configuration) error {
					return nil
				}),
				gomonkey.ApplyFunc((*s3Deployer).getCodeFromObs, func(_ *s3Deployer, bucketID string, objectID string, path string,
					layerName string, language string) (buffer []byte, objSize int64, err error) {
					return nil, 0, errors.New("error")
				}),
				gomonkey.ApplyFunc(commonObs.NewObsClient,
					func(opts commonObs.Option) (*commonObs.Client, error) {
						return &commonObs.Client{}, nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotConfig := generateSnapshotConfig()
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			_, err = deployer.DownloadSnapshotWithRetry(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Access: "mockAccess",
				},
			}, config.SnapshotConfig{}, "", "")
			convey.So(err, convey.ShouldNotBeNil)
		})
	})

	convey.Convey("download Snapshot", t, func() {
		convey.Convey("test download snapshot succeed", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),

				gomonkey.ApplyMethod(reflect.TypeOf(&SnapshotZipHandler{}), "Compress", func(_ *SnapshotZipHandler, zipFile string, src string) error {
					return nil
				}),
				gomonkey.ApplyFunc((*s3Deployer).RefreshToken, func(_ *s3Deployer, conf *config.Configuration) error {
					return nil
				}),
				gomonkey.ApplyFunc((*s3Deployer).getCodeFromObs, func(_ *s3Deployer, bucketID string, objectID string, path string,
					layerName string, language string) (buffer []byte, objSize int64, err error) {
					return nil, 0, nil
				}),
				gomonkey.ApplyFunc(commonObs.NewObsClient,
					func(opts commonObs.Option) (*commonObs.Client, error) {
						return &commonObs.Client{}, nil
					}),
				gomonkey.ApplyFunc((*s3Deployer).decompress, func(_ *s3Deployer, duration *DurationInfo, handler Decompressor, objSize int64, dep *deployDetail, buffer []byte) error {
					return nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotConfig := generateSnapshotConfig()
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			_, err = deployer.DownloadSnapshotWithRetry(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Access: "mockAccess",
				},
			}, config.SnapshotConfig{}, "", "")
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_UploadDumpFile(t *testing.T) {
	cli := &commonObs.Client{}
	tests := []struct {
		name        string
		expectErr   bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:      "test1, utilities.GetFileHashInfo error",
			expectErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utilities.GetFileSize, func(path string) (int64, error) { return 0, errors.New("error") }),
				})
				return patches
			},
		},
		{
			name:      "test2, d.InitMultipart error",
			expectErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utilities.GetFileSize, func(path string) (int64, error) { return 0, nil }),
					gomonkey.ApplyMethod(reflect.TypeOf(cli), "InitMultipart", func(_ *commonObs.Client, input *obs.InitiateMultipartUploadInput) (string, error) {
						return "", errors.New("test error")
					}),
				})
				return patches
			},
		},
		{
			name:      "test3, d.completeMultipartUpload error",
			expectErr: true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utilities.GetFileSize, func(path string) (int64, error) { return 0, nil }),
					gomonkey.ApplyMethod(reflect.TypeOf(cli), "InitMultipart", func(_ *commonObs.Client, input *obs.InitiateMultipartUploadInput) (string, error) {
						return "", nil
					}),
					gomonkey.ApplyFunc((*s3Deployer).completeMultipartUpload,
						func(_ *s3Deployer, fileSize int64, partResults []PartResult, fileInput *obs.PutFileInput, uploadID string) error {
							return errors.New("test error")
						}),
				})
				return patches
			},
		},
		{
			name:      "test4, ok",
			expectErr: false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(utilities.GetFileSize, func(path string) (int64, error) { return 100, nil }),
					gomonkey.ApplyMethod(reflect.TypeOf(cli), "InitMultipart", func(_ *commonObs.Client, input *obs.InitiateMultipartUploadInput) (string, error) {
						return "", nil
					}),
					gomonkey.ApplyFunc((*s3Deployer).completeMultipartUpload,
						func(_ *s3Deployer, fileSize int64, partResults []PartResult, fileInput *obs.PutFileInput, uploadID string) error {
							return nil
						}),
					gomonkey.ApplyFunc((*s3Deployer).uploadMultipart,
						func(_ *s3Deployer, fileSize int64, dumpFileDir string, fileInput *obs.PutFileInput, uploadID string) []PartResult {
							return nil
						}),
				})
				return patches
			},
		},
	}
	s := &s3Deployer{
		cli: cli,
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			res := s.UploadDumpFile(&config.Configuration{}, "")
			if (res != nil) != tt.expectErr {
				t.Errorf("error, res: %v, expect: %v", res != nil, tt.expectErr)
			}
		})
	}
	res := renameKeyByReplacingSlash("", "name")
	if !strings.Contains(res, "name") {
		t.Errorf("this is error")
	}
	res = renameKeyByReplacingSlash("/tmp", "name")
	if !strings.Contains(res, "tmp/") || !strings.Contains(res, "name") {
		t.Errorf("this is error")
	}
	res = renameKeyByReplacingSlash("//tmp", "/name//abcdefg")
	if res != "tmp/abcdefg" {
		t.Errorf("this is error")
	}
}

func Test_NewCaeDeployer(t *testing.T) {
	tests := []struct {
		name        string
		c           *config.Configuration
		dumpConfig  commontypes.CustomUploadConfig
		expect      bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{
			name:       "test1, Decrypt error",
			c:          &config.Configuration{CAECustomS3ConfigInfo: commontypes.CAECustomS3ConfigInfo{CustomSK: "123"}},
			dumpConfig: commontypes.CustomUploadConfig{CAECustomS3ConfigInfo: commontypes.CAECustomS3ConfigInfo{CustomSK: "123"}},
			expect:     false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return nil, errors.New("error") }),
				})
				return patches
			},
		},
		{
			name:       "test2, commonObs.NewObsClient error",
			c:          &config.Configuration{},
			dumpConfig: commontypes.CustomUploadConfig{},
			expect:     false,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return nil, nil }),
					gomonkey.ApplyFunc(commonObs.NewObsClient, func(o commonObs.Option) (*commonObs.Client, error) { return nil, errors.New("error") }),
				})
				return patches
			},
		},
		{
			name:       "test3, clear",
			c:          &config.Configuration{},
			dumpConfig: commontypes.CustomUploadConfig{},
			expect:     true,
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) { return nil, nil }),
					gomonkey.ApplyFunc(commonObs.NewObsClient, func(o commonObs.Option) (*commonObs.Client, error) { return &commonObs.Client{}, nil }),
					gomonkey.ApplyFunc(preconnectCache, func(c *config.Configuration) (*http.Client, error) { return &http.Client{}, nil }),
					gomonkey.ApplyFunc(commonObs.ClearSecretKeyMemory, func() {}),
				})
				return patches
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			_, err := NewDumpDeployer(tt.c, tt.dumpConfig)
			if (err == nil) != tt.expect {
				t.Errorf("result: %v, expect: %v", err == nil, tt.expect)
			}
			patches.ResetAll()
		})
	}
}
func TestS3Deployer_DeleteSnapshot(t *testing.T) {
	convey.Convey("S3Deployer delete snapshot", t, func() {
		convey.Convey("need delete snapshot image tags is nil", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),
				gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "DeleteObject", func(_ *commonObs.Client,
					input *obs.DeleteObjectInput) (*obs.DeleteObjectOutput, error) {
					return &obs.DeleteObjectOutput{}, nil
				}),

				gomonkey.ApplyFunc(utilities.Domain2IP, func(endpoint string) (string, error) {
					return "1.2.11.11", nil
				}),
				gomonkey.ApplyFunc(preconnectCache, func(c *config.Configuration) (*http.Client, error) {
					return &http.Client{}, nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotConfig := generateSnapshotConfig()
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			err = deployer.DeleteSnapshot(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Access: "mockAccess",
				},
			}, "", "", []string{})

			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test refresh token error", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),

				gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "DeleteObject", func(_ *commonObs.Client,
					input *obs.DeleteObjectInput) (*obs.DeleteObjectOutput, error) {
					return &obs.DeleteObjectOutput{}, nil
				}),

				gomonkey.ApplyFunc(utilities.Domain2IP, func(endpoint string) (string, error) {
					return "1.2.11.11", nil
				}),

				gomonkey.ApplyFunc(preconnectCache, func(c *config.Configuration) (*http.Client, error) {
					return &http.Client{}, nil
				}),

				gomonkey.ApplyFunc((*s3Deployer).RefreshToken, func(_ *s3Deployer, conf *config.Configuration) error {
					return errors.New("refresh token error")
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotConfig := generateSnapshotConfig()
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			err = deployer.DeleteSnapshot(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Access: "mockAccess",
				},
			}, "", "", []string{"test.zip.VERSION1.101"})

			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test delete snapshot from data-system error", func() {
			patches := [...]*gomonkey.Patches{
				gomonkey.ApplyFunc(config.GetConfig,
					func() (*config.Configuration, error) {
						return &config.Configuration{
							DeploymentConfig: config.DeploymentConfig{S3Config: config.S3Config{
								AccessKey: "C2", SecretKey: "FYq7", Endpoint: "1.2.11.11:30"},
								InternalHTTPSConfig: config.InternalHTTPSConfig{
									HTTPSEnable: false,
									TLSProtocol: "",
									TLSCiphers:  "",
								}},
						}, nil
					}),

				gomonkey.ApplyMethod(reflect.TypeOf(&commonObs.Client{}), "DeleteObject", func(_ *commonObs.Client,
					input *obs.DeleteObjectInput) (*obs.DeleteObjectOutput, error) {
					return &obs.DeleteObjectOutput{}, nil
				}),

				gomonkey.ApplyFunc(utilities.Domain2IP, func(endpoint string) (string, error) {
					return "1.2.11.11", nil
				}),

				gomonkey.ApplyFunc(preconnectCache, func(c *config.Configuration) (*http.Client, error) {
					return &http.Client{}, nil
				}),

				gomonkey.ApplyFunc((*s3Deployer).RefreshToken, func(_ *s3Deployer, conf *config.Configuration) error {
					return nil
				}),
				gomonkey.ApplyFunc(storage.GetStorageType, func() string {
					return "data-system"
				}),

				gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
					return &MockDelErrStorage{}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			snapshotConfig := generateSnapshotConfig()
			deployer, err := NewDeployer(snapshotConfig)
			if err != nil {
				t.Errorf("new deployer failed: %s", err)
			}
			_, ok := deployer.(*s3Deployer)
			if !ok {
				t.Errorf("new deployer type error")
			}
			err = deployer.DeleteSnapshot(&config.Configuration{
				DeploymentConfig: config.DeploymentConfig{
					Access: "mockAccess",
				},
			}, "", "", []string{"test.zip.VERSION1.101"})

			convey.So(err, convey.ShouldNotBeNil)
		})
	})
}

func Test_s3Deployer_deleteSnapshotFromObs(t *testing.T) {
	var (
		patch = gomonkey.NewPatches()
		d     = &s3Deployer{
			cli:        &commonObs.Client{},
			httpClient: &http.Client{},
		}

		bucketID            string
		needRemoveObjectIDs []string
	)

	convey.Convey("test deleteSnapshotFromObs", t, func() {
		convey.Convey("test need delete image tags is nil", func() {
			err := d.deleteSnapshotFromObs(bucketID, needRemoveObjectIDs)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}
func TestCompleteMultipartUpload(t *testing.T) {
	cli := &commonObs.Client{}
	s := &s3Deployer{
		cli: cli,
	}
	fileSize := int64(100)
	partResults := []PartResult{
		{
			PartNumber: 1,
			ETag:       "etag1",
		},
	}
	fileInput := &obs.PutFileInput{}
	fileInput.Bucket = "bucket"
	fileInput.Key = "testKey"
	uploadID := "uploadID"
	t.Run("Success", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "CompleteMultipartUpload", func(_ *commonObs.Client, input *obs.CompleteMultipartUploadInput) error {
				return nil
			}),
		})
		defer patches.ResetAll()

		err := s.completeMultipartUpload(fileSize, partResults, fileInput, uploadID)
		assert.NoError(t, err)
	})

	t.Run("Failure", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "CompleteMultipartUpload", func(_ *commonObs.Client, input *obs.CompleteMultipartUploadInput) error {
				return errors.New("test error")
			}),
		})
		defer patches.ResetAll()

		err := s.completeMultipartUpload(fileSize, partResults, fileInput, uploadID)
		assert.Error(t, err)
		assert.Equal(t, "failed to complete multipart upload error: test error", err.Error())
	})
}

func TestUploadMultipart(t *testing.T) {
	cli := &commonObs.Client{}
	d := &s3Deployer{
		cli: cli,
	}

	t.Run("Test file size equals to partSize, upload ok", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(utilities.GetFileContent, func(path string, start int64, end int64) ([]byte, error) {
				return nil, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "UploadMultipart", func(_ *commonObs.Client, input *obs.UploadPartInput) (output *obs.UploadPartOutput, err error) {
				return &obs.UploadPartOutput{
					ETag: "11",
				}, nil
			}),
		})
		defer patches.ResetAll()

		partResults := d.uploadMultipart(int64(partSize), "testDir", &obs.PutFileInput{}, "uploadID")
		if len(partResults) != 1 {
			t.Errorf("Expected 1 part, but got %d", len(partResults))
		}
		if partResults[0].ETag != "11" {
			t.Errorf("Expected ETag '11', but got %v", partResults[0].ETag)
		}
	})
	t.Run("Test file size larger than partSize, upload ok", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(utilities.GetFileContent, func(path string, start int64, end int64) ([]byte, error) {
				return nil, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "UploadMultipart", func(_ *commonObs.Client, input *obs.UploadPartInput) (output *obs.UploadPartOutput, err error) {
				return &obs.UploadPartOutput{
					ETag: "11",
				}, nil
			}),
		})
		defer patches.ResetAll()

		partResults := d.uploadMultipart(int64(partSize)+1, "testDir", &obs.PutFileInput{}, "uploadID")
		if len(partResults) != 2 {
			t.Errorf("Expected 2 part, but got %d", len(partResults))
		}
		if partResults[0].ETag != "11" {
			t.Errorf("Expected ETag '11', but got %v", partResults[0].ETag)
		}
		if partResults[1].ETag != "11" {
			t.Errorf("Expected ETag '11', but got %v", partResults[0].ETag)
		}
	})
	t.Run("Test file size smaller than partSize, upload ok", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(utilities.GetFileContent, func(path string, start int64, end int64) ([]byte, error) {
				return nil, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "UploadMultipart", func(_ *commonObs.Client, input *obs.UploadPartInput) (output *obs.UploadPartOutput, err error) {
				return &obs.UploadPartOutput{
					ETag: "11",
				}, nil
			}),
		})
		defer patches.ResetAll()

		partResults := d.uploadMultipart(int64(partSize)-1, "testDir", &obs.PutFileInput{}, "uploadID")
		if len(partResults) != 1 {
			t.Errorf("Expected 1 part, but got %d", len(partResults))
		}
		if partResults[0].ETag != "11" {
			t.Errorf("Expected ETag '11', but got %v", partResults[0].ETag)
		}
	})
	t.Run("Test file size larger than partSize, get the second part content failed", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "UploadMultipart", func(_ *commonObs.Client, input *obs.UploadPartInput) (output *obs.UploadPartOutput, err error) {
				return &obs.UploadPartOutput{
					ETag: "11",
				}, nil
			}),
			gomonkey.ApplyFunc(utilities.GetFileContent, func(path string, start int64, end int64) ([]byte, error) {
				if start != 0 {
					return nil, errors.New("test error")
				}
				return nil, nil
			}),
		})
		defer patches.ResetAll()

		partResults := d.uploadMultipart(int64(partSize)+1, "testDir", &obs.PutFileInput{}, "uploadID")
		if len(partResults) != 2 {
			t.Errorf("Expected 2 part, but got %d", len(partResults))
		}
		if partResults[0].ETag != "11" {
			t.Errorf("Expected ETag '11', but got %v", partResults[0].ETag)
		}
		if partResults[1].Err.Error() != "test error" {
			t.Errorf("Expected Err 'test error', but got %v", partResults[1].Err.Error())
		}
	})
	t.Run("Test upload failed", func(t *testing.T) {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(utilities.GetFileContent, func(path string, start int64, end int64) ([]byte, error) {
				return nil, nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(cli), "UploadMultipart", func(_ *commonObs.Client, input *obs.UploadPartInput) (output *obs.UploadPartOutput, err error) {
				return nil, errors.New("test error")
			}),
		})
		defer patches.ResetAll()

		partResults := d.uploadMultipart(int64(partSize)+1, "testDir", &obs.PutFileInput{}, "uploadID")
		if len(partResults) != 2 {
			t.Errorf("Expected 2 part, but got %d", len(partResults))
		}
		if partResults[0].Err.Error() != "test error" {
			t.Errorf("Expected Err 'test error', but got %v", partResults[0].Err.Error())
		}
		if partResults[1].Err.Error() != "test error" {
			t.Errorf("Expected Err 'test error', but got %v", partResults[1].Err.Error())
		}
	})
}
