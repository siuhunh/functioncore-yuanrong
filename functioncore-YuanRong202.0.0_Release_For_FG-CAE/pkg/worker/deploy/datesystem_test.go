package deploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/stretchr/testify/assert"

	commonObs "functioncore/pkg/common/obs"
	"functioncore/pkg/common/storage"
	mockutils "functioncore/test/utils"
)

type MockStorage struct {
}

func (m *MockStorage) Get(key string) (string, error) {
	fileList := []string{"1.ffs", "2.ffs", "3.ffs", "4.ffs", "manifest.txt"}
	fileListBytes, _ := json.Marshal(fileList)
	return string(fileListBytes), nil
}

func (m *MockStorage) Set(key, value string) error {
	return nil
}

type MockGetErrStorage struct {
}

func (m *MockGetErrStorage) Get(key string) (string, error) {
	return "", errors.New("mock storage get error")
}

func (m *MockGetErrStorage) Set(key, value string) error {
	return nil
}

type MockSetErrStorage struct {
}

func (m *MockSetErrStorage) Get(key string) (string, error) {
	fileList := []string{"1.ffs", "2.ffs", "3.ffs", "4.ffs", "manifest.txt"}
	fileListBytes, _ := json.Marshal(fileList)
	return string(fileListBytes), nil
}

func (m *MockSetErrStorage) Set(key, value string) error {
	return errors.New("mock storage set error")
}

func Test_s3Deployer_DownloadSnapshotFromDs(t *testing.T) {
	type fields struct {
		storageCli       storage.Storage
		cli              *commonObs.Client
		httpClient       *http.Client
		ioReadTimeout    int
		cacheHttpsEnable bool
	}
	type args struct {
		bucketID string
		ObjectID string
	}
	mockStorage := &MockStorage{}
	mockGetErrStorage := &MockGetErrStorage{}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test success",
			fields: fields{
				storageCli: mockStorage,
			},
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(os.WriteFile, func(name string, data []byte, perm os.FileMode) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		}, // test success
		{
			name: "test os write file error",
			fields: fields{
				storageCli: mockStorage,
			},
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(os.WriteFile, func(name string, data []byte, perm os.FileMode) error {
						return errors.New("mock write file error")
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		}, // test os write file error
		{
			name: "test storage cli get error",
			fields: fields{
				storageCli: mockGetErrStorage,
			},
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		}, // test storage cli get error
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &s3Deployer{
				storageCli:       tt.fields.storageCli,
				cli:              tt.fields.cli,
				httpClient:       tt.fields.httpClient,
				ioReadTimeout:    tt.fields.ioReadTimeout,
				cacheHTTPSEnable: tt.fields.cacheHttpsEnable,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			tt.wantErr(t, d.DownloadSnapshotFromDs(tt.args.bucketID, tt.args.ObjectID), fmt.Sprintf("DownloadSnapshotFromDs(%v, %v)", tt.args.bucketID, tt.args.ObjectID))
		})
	}
}

type MockFileInfo struct {
}

func (m MockFileInfo) Name() string {
	return "mockName"
}

func (m MockFileInfo) Size() int64 {
	return 1
}

func (m MockFileInfo) Mode() fs.FileMode {
	return fs.FileMode(1)
}

func (m MockFileInfo) ModTime() time.Time {
	return time.Now()
}

func (m MockFileInfo) IsDir() bool {
	return false
}

func (m MockFileInfo) Sys() any {
	return "mockSys"
}

func Test_s3Deployer_NoCompressANDUploadToDS(t *testing.T) {
	type fields struct {
		storageCli       storage.Storage
		cli              *commonObs.Client
		httpClient       *http.Client
		ioReadTimeout    int
		cacheHttpsEnable bool
	}
	type args struct {
		bucketID string
		ObjectID string
	}
	mockStorage := &MockStorage{}
	mockSetErrStorage := &MockSetErrStorage{}
	mockFileInfo := &MockFileInfo{}
	var mockFileInfos []fs.FileInfo
	mockFileInfos = append(mockFileInfos, mockFileInfo)
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test success",
			fields: fields{
				storageCli: mockStorage,
			},
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
						return mockFileInfos, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("mockReadFileReturn"), nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		}, // test success
		{
			name: "test storage set error",
			fields: fields{
				storageCli: mockSetErrStorage,
			},
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
						return mockFileInfos, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("mockReadFileReturn"), nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		}, // test storage set error
		{
			name: "test read file error",
			fields: fields{
				storageCli: mockStorage,
			},
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
						return mockFileInfos, nil
					}),
					gomonkey.ApplyFunc(ioutil.ReadFile, func(filename string) ([]byte, error) {
						return []byte("mockReadFileReturn"), errors.New("mock read file error")
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		}, // test read file error
		{
			name: "test read dir error",
			fields: fields{
				storageCli: mockStorage,
			},
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(ioutil.ReadDir, func(dirname string) ([]fs.FileInfo, error) {
						return mockFileInfos, errors.New("mock read dir error")
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		}, // test read dir error
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &s3Deployer{
				storageCli:       tt.fields.storageCli,
				cli:              tt.fields.cli,
				httpClient:       tt.fields.httpClient,
				ioReadTimeout:    tt.fields.ioReadTimeout,
				cacheHTTPSEnable: tt.fields.cacheHttpsEnable,
			}
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			_, _, err := d.NoCompressANDUploadToDS(tt.args.bucketID, tt.args.ObjectID)
			if !tt.wantErr(t, err, fmt.Sprintf("NoCompressANDUploadToDS(%v, %v)", tt.args.bucketID, tt.args.ObjectID)) {
				return
			}
		})
	}
}
