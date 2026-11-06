/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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

package obs

import (
	"fmt"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
	mockUtils "functioncore/test/utils"
)

func Test_NewDeployer(t *testing.T) {
	type fields struct {
		config types.S3Config
	}

	tests := []struct {
		name        string
		fields      fields
		patchesFunc mockUtils.PatchesFunc
		wantErr     bool
	}{
		// test failed to get config
		{
			name: "case_01 create obs client error",
			fields: fields{types.S3Config{
				UseSSL:    false,
				AccessKey: "xxxxxx",
				SecretKey: "",
				Endpoint:  "1.0.0.0:9000",
				CaFile:    "",
				TrustedCA: false,
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(NewObsClient, func(o Option) (*Client, error) {
						return nil, fmt.Errorf("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case_02 create obs client error",
			fields: fields{types.S3Config{
				UseSSL:    false,
				AccessKey: "xxxxxx",
				SecretKey: "xxxxxxx",
				Endpoint:  "1.0.0.0:9000",
				CaFile:    "",
				TrustedCA: false,
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return nil, fmt.Errorf("decrypt error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case_03 create obs client ok",
			fields: fields{types.S3Config{
				UseSSL:    false,
				AccessKey: "xxxxxx",
				SecretKey: "xxxxx",
				Endpoint:  "1.0.0.0:9000",
				CaFile:    "",
				TrustedCA: false,
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte("test"), nil
					}),
					gomonkey.ApplyFunc(NewObsClient, func(o Option) (*Client, error) {
						return nil, fmt.Errorf("create client error")
					}),
				})

				return patches
			},
			wantErr: true,
		},
		{
			name: "case_04 create obs client ok",
			fields: fields{types.S3Config{
				UseSSL:    false,
				AccessKey: "xxxxxx",
				SecretKey: "xxxxx",
				Endpoint:  "1.0.0.0:9000",
				CaFile:    "",
				TrustedCA: false,
			}},
			patchesFunc: func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(NewObsClient, func(o Option) (*Client, error) {
						return &Client{}, nil
					}),
					gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
						return []byte("test"), nil
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
			_, err := NewDeployer(&tt.fields.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewDeployer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			patches.ResetAll()
		})
	}
}

func Test_DownLoadFromObs(t *testing.T) {
	convey.Convey("01 test DownLoadFromObs error", t, func() {
		convey.Convey("case_01 DownLoadFromObs error", func() {
			p := gomonkey.ApplyFunc((*Client).GetObject, func(_ *Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return nil, fmt.Errorf("download error")
			})
			p2 := gomonkey.ApplyFunc(GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return nil, fmt.Errorf("download error")
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("02 test DownLoadFromObs error,data is nil", func() {
			p := gomonkey.ApplyFunc((*Client).GetObject, func(_ *Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return nil, fmt.Errorf("download error")
			})
			p2 := gomonkey.ApplyFunc(GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return nil, fmt.Errorf("download error")
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("03 test DownLoadFromObs error", func() {
			p := gomonkey.ApplyFunc((*Client).GetObject, func(_ *Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return &obs.GetObjectOutput{}, nil
			})
			p2 := gomonkey.ApplyFunc(GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return nil, fmt.Errorf("download error")
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("04 test DownLoadFromObs error", func() {
			p := gomonkey.ApplyFunc((*Client).GetObject, func(_ *Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return &obs.GetObjectOutput{}, nil
			})
			p2 := gomonkey.ApplyFunc(GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return []byte{1, 2, 3, 4, 5, 6}, nil
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &Client{},
			}
			_, err := deployer.DownLoadFromObs("", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("05 test DownLoadFromObs ok", func() {
			p := gomonkey.ApplyFunc((*Client).GetObject, func(_ *Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return &obs.GetObjectOutput{}, nil
			})
			p2 := gomonkey.ApplyFunc(GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return []byte{1, 2, 3, 4, 5, 6}, nil
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldBeNil)
		})

	})
}
