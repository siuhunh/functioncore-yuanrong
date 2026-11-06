package utils

import (
	"fmt"
	"testing"

	mockUtils "functioncore/test/utils"
	"github.com/agiledragon/gomonkey/v2"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/localauth"
	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/worker-manager/config"
)

func Test_NewDeployer(t *testing.T) {
	type fields struct {
		config config.S3Config
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
			fields: fields{config.S3Config{
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
					gomonkey.ApplyFunc(commonObs.NewObsClient, func(o commonObs.Option) (*commonObs.Client, error) {
						return nil, fmt.Errorf("error")
					}),
				})
				return patches
			},
			wantErr: true,
		},
		{
			name: "case_02 create obs client error",
			fields: fields{config.S3Config{
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
			fields: fields{config.S3Config{
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
					gomonkey.ApplyFunc(commonObs.NewObsClient, func(o commonObs.Option) (*commonObs.Client, error) {
						return nil, fmt.Errorf("create client error")
					}),
				})

				return patches
			},
			wantErr: true,
		},
		{
			name: "case_04 create obs client ok",
			fields: fields{config.S3Config{
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
					gomonkey.ApplyFunc(commonObs.NewObsClient, func(o commonObs.Option) (*commonObs.Client, error) {
						return &commonObs.Client{}, nil
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
			p := gomonkey.ApplyFunc((*commonObs.Client).GetObject, func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return nil, fmt.Errorf("download error")
			})
			p2 := gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return nil, fmt.Errorf("download error")
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &commonObs.Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("02 test DownLoadFromObs error,data is nil", func() {
			p := gomonkey.ApplyFunc((*commonObs.Client).GetObject, func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return nil, fmt.Errorf("download error")
			})
			p2 := gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return nil, fmt.Errorf("download error")
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &commonObs.Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("03 test DownLoadFromObs error", func() {
			p := gomonkey.ApplyFunc((*commonObs.Client).GetObject, func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return &obs.GetObjectOutput{}, nil
			})
			p2 := gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return nil, fmt.Errorf("download error")
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &commonObs.Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("04 test DownLoadFromObs error", func() {
			p := gomonkey.ApplyFunc((*commonObs.Client).GetObject, func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return &obs.GetObjectOutput{}, nil
			})
			p2 := gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return []byte{1, 2, 3, 4, 5, 6}, nil
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &commonObs.Client{},
			}
			_, err := deployer.DownLoadFromObs("", "bbbb")
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("05 test DownLoadFromObs ok", func() {
			p := gomonkey.ApplyFunc((*commonObs.Client).GetObject, func(_ *commonObs.Client, input *obs.GetObjectInput) (output *obs.GetObjectOutput, err error) {
				return &obs.GetObjectOutput{}, nil
			})
			p2 := gomonkey.ApplyFunc(commonObs.GetOutputObjectBuffer, func(objectOutput *obs.GetObjectOutput) ([]byte, error) {
				return []byte{1, 2, 3, 4, 5, 6}, nil
			})
			defer p.Reset()
			defer p2.Reset()
			deployer := S3Deployer{
				cli: &commonObs.Client{},
			}
			_, err := deployer.DownLoadFromObs("aaa", "bbbb")
			convey.So(err, convey.ShouldBeNil)
		})

	})
}
