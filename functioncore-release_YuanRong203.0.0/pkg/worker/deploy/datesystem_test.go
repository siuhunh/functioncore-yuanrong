package deploy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/storage"
	mockutils "functioncore/test/utils"
)

type MockStorage struct {
}

func (m *MockStorage) Get(key string) (string, error) {
	fileID := strings.TrimRight(key, snapshotDSListKeySplit)
	if !strings.Contains(fileID, snapshotDSKeySeparator) {
		return "", errors.New("invalid fileID")
	}

	fileList := []string{"1.ffs", "2.ffs", "3.ffs", "4.ffs", "manifest.txt"}
	fileListBytes, _ := json.Marshal(fileList)
	return string(fileListBytes), nil
}

func (m *MockStorage) Set(key, value string) error {
	return nil
}

func (m *MockStorage) Delete(key string) error {
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

func (m *MockGetErrStorage) Delete(key string) error {
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

func (m *MockSetErrStorage) Delete(key string) error {
	return nil
}

type MockDelErrStorage struct {
}

func (m *MockDelErrStorage) Get(key string) (string, error) {
	fileList := []string{"1.ffs", "2.ffs", "3.ffs", "4.ffs", "manifest.txt"}
	fileListBytes, _ := json.Marshal(fileList)
	return string(fileListBytes), nil
}

func (m *MockDelErrStorage) Set(key, value string) error {
	return nil
}

func (m *MockDelErrStorage) Delete(key string) error {
	return errors.New("mock storage del error")
}

func Test_s3Deployer_DownloadSnapshotFromDs(t *testing.T) {
	type args struct {
		bucketID string
		ObjectID string
	}
	mockStorage := &MockStorage{}
	mockGetErrStorage := &MockGetErrStorage{}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test success",
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockStorage
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockStorage
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
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockGetErrStorage
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		}, // test storage cli get error
		{
			name: "test storage client is nil",
			args: args{
				bucketID: "mockBucketID",
				ObjectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		}, // test storage client is nil
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			tt.wantErr(t, DownloadSnapshotFromDS(tt.args.bucketID, tt.args.ObjectID), fmt.Sprintf("DownloadSnapshotFromDS(%v, %v)", tt.args.bucketID, tt.args.ObjectID))
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
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test success",
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockStorage
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockSetErrStorage
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockStorage
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockStorage
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		}, // test read dir error
		{
			name: "test storage client is nil",
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		}, // test storage client is nil
		{
			name: "test io read error",
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockStorage
					}),
					gomonkey.ApplyFunc(deleteOldDSFileIfExists, func(storageClient storage.Storage,
						fileID string) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
		{
			name: "test delete old ds file error",
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
					gomonkey.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
						return mockStorage
					}),
					gomonkey.ApplyFunc(deleteOldDSFileIfExists, func(storageClient storage.Storage,
						fileID string) error {
						return errors.New("delete old ds file error")
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err != nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			_, _, err := noCompressANDUploadToDS(tt.args.bucketID, tt.args.ObjectID)
			if !tt.wantErr(t, err, fmt.Sprintf("noCompressANDUploadToDS(%v, %v)", tt.args.bucketID, tt.args.ObjectID)) {
				return
			}
		})
	}
}

func TestDownloadSnapshotFromDS(t *testing.T) {
	type args struct {
		bucketID string
		objectID string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test download snapshot from data system successfully",
			args: args{
				bucketID: "mockBucketID",
				objectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(downloadSnapshotFromDS, func(bucketID, objectID string) error {
						return nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		},
	}
	for _, tt := range tests {
		patches := tt.patchesFunc()
		t.Run(tt.name, func(t *testing.T) {
			tt.wantErr(t, DownloadSnapshotFromDS(tt.args.bucketID, tt.args.objectID), fmt.Sprintf("DownloadSnapshotFromDS(%v, %v)", tt.args.bucketID, tt.args.objectID))
		})
		patches.ResetAll()
	}
}

func TestUploadSnapshotToDS(t *testing.T) {
	type args struct {
		bucketID string
		objectID string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc mockutils.PatchesFunc
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "test upload snapshot to data system successfully",
			args: args{
				bucketID: "mockBucketID",
				objectID: "mockObjectID",
			},
			patchesFunc: func() mockutils.PatchSlice {
				patches := mockutils.InitPatchSlice()
				patches.Append(mockutils.PatchSlice{
					gomonkey.ApplyFunc(noCompressANDUploadToDS, func(bucketID, ObjectID string) (int64, string, error) {
						return 0, "", nil
					}),
				})
				return patches
			},
			wantErr: func(t assert.TestingT, err error, i ...interface{}) bool {
				return err == nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			defer patches.ResetAll()
			_, _, err := UploadSnapshotToDS(tt.args.bucketID, tt.args.objectID)
			if !tt.wantErr(t, err, fmt.Sprintf("UploadSnapshotToDS(%v, %v)", tt.args.bucketID, tt.args.objectID)) {
				return
			}
		})
	}
}

func TestDeleteSnapshotFromDS(t *testing.T) {
	var (
		patch               = gomonkey.NewPatches()
		bucketID            string
		objectID            string
		needRemoveObjectIDs []string
	)

	convey.Convey("test DeleteSnapshotFromDS", t, func() {
		convey.Convey("test delete snapshot tag is empty", func() {
			err := DeleteSnapshotFromDS(bucketID, objectID, needRemoveObjectIDs)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test delete snapshot ds with system error", func() {
			needRemoveObjectIDs = []string{"test.zip.VERSION1.101"}
			mockExecuteDeleteSnapshotFromDS(patch, errors.New("system error"))
			err := DeleteSnapshotFromDS(bucketID, objectID, needRemoveObjectIDs)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "system error")
		})

		convey.Convey("test delete snapshot ds with timeout", func() {
			needRemoveObjectIDs = []string{"test.zip.VERSION1.101"}
			mockExecuteDeleteSnapshotFromDS(patch, errors.New("delete snapshot to data system timeout"))
			err := DeleteSnapshotFromDS(bucketID, objectID, needRemoveObjectIDs)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "timeout")
		})

		convey.Convey("test delete snapshot ds success", func() {
			needRemoveObjectIDs = []string{"test.zip.VERSION1.101"}
			mockExecuteDeleteSnapshotFromDS(patch, nil)
			err := DeleteSnapshotFromDS(bucketID, objectID, needRemoveObjectIDs)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_deleteOldDSFileIfExists(t *testing.T) {
	var (
		patch  = gomonkey.NewPatches()
		fileID string
	)

	convey.Convey("test deleteSnapshotToDS", t, func() {
		convey.Convey("test get key error by invalid fileID", func() {
			fileID = ""
			err := deleteOldDSFileIfExists(&MockStorage{}, fileID)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test get key error should ignore", func() {
			fileID = "test-bucket@objectID"
			err := deleteOldDSFileIfExists(&MockGetErrStorage{}, fileID)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Convey("test json unmarshal error", func() {
			fileID = "test-bucket@objectID"
			mockJsonUnmarshal(patch, errors.New("json unmarshal error"))
			err := deleteOldDSFileIfExists(&MockStorage{}, fileID)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test delete snapshot failed", func() {
			fileID = "test-bucket@objectID"
			err := deleteOldDSFileIfExists(&MockDelErrStorage{}, fileID)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "mock storage del error")
		})

		convey.Convey("test del snapshot success", func() {
			fileID = "test-bucket@objectID"
			err := deleteOldDSFileIfExists(&MockStorage{}, fileID)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func Test_deleteSnapshotFromDS(t *testing.T) {
	var (
		patch               = gomonkey.NewPatches()
		bucketID            string
		needDeleteImageTags []string
	)

	convey.Convey("test deleteSnapshotFromDS", t, func() {
		convey.Convey("test get storage client is nil", func() {
			mockGetStorageClient(patch, nil)
			err := deleteSnapshotFromDS(bucketID, needDeleteImageTags)
			convey.So(err, convey.ShouldNotBeNil)
		})

		convey.Convey("test delete snapshot failed", func() {
			needDeleteImageTags = []string{"test.zip.VERSION1.101"}
			mockGetStorageClient(patch, &MockDelErrStorage{})
			err := deleteSnapshotFromDS(bucketID, needDeleteImageTags)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldContainSubstring, "mock storage del error")
		})

		convey.Convey("test del snapshot success", func() {
			needDeleteImageTags = []string{"test.zip.VERSION1.101"}
			mockGetStorageClient(patch, &MockStorage{})
			err := deleteSnapshotFromDS(bucketID, needDeleteImageTags)
			convey.So(err, convey.ShouldBeNil)
		})

		convey.Reset(func() {
			patch.Reset()
		})
	})
}

func mockExecuteDeleteSnapshotFromDS(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc(deleteSnapshotFromDS, func(bucketID string, needDeleteImageTag []string) error {
		if err != nil && strings.Contains(err.Error(), "mock timeout") {
			time.Sleep(2001 * time.Millisecond)
		}
		return err
	})
}

func mockGetStorageClient(patch *gomonkey.Patches, store storage.Storage) {
	patch.ApplyFunc(storage.GetStorageClient, func() storage.Storage {
		return store
	})
}

func mockJsonUnmarshal(patch *gomonkey.Patches, err error) {
	patch.ApplyFunc(json.Unmarshal, func(data []byte, v any) error {
		return err
	})
}
