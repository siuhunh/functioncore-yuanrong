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

package obs

import (
	"errors"
	"os"
	"testing"
	"unsafe"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/utils"
	mockutils "functioncore/test/utils"
)

func TestNewObsClient(t *testing.T) {
	convey.Convey("Test New Obs Client error 1", t, func() {
		opt := Option{SecretKey: "sk"}
		s3Client, err := NewObsClient(opt)
		convey.So(s3Client, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Obs Client 1", t, func() {
		patches := mockutils.InitPatchSlice()
		patches.Append(mockutils.PatchSlice{gomonkey.ApplyFunc(utils.Domain2IP, func(endpoint string) (string, error) {
			return "", nil
		})})
		defer patches.ResetAll()
		opt := Option{SecretKey: "sk"}
		s3Client, err := NewObsClient(opt)
		convey.So(s3Client, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
	convey.Convey("Test New Obs Client 2", t, func() {
		opt := Option{SecretKey: "sk", Secure: true}
		s3Client, err := NewObsClient(opt)
		convey.So(s3Client, convey.ShouldNotBeNil)
		convey.So(err, convey.ShouldBeNil)
	})
}

func TestGetTLSConfig(t *testing.T) {
	config := getTLSConfig("ca", true)
	assert.NotEmpty(t, config)
}

func TestGetEndpoint(t *testing.T) {
	endpoint, urlPrefix := getEndpoint("http://")
	assert.Equal(t, endpoint, "")
	assert.Equal(t, urlPrefix, "http://")

	endpoint, urlPrefix = getEndpoint("https://")
	assert.Equal(t, endpoint, "")
	assert.Equal(t, urlPrefix, "https://")
}

func (ReadCloserImp) Read(_ []byte) (n int, err error) {
	return '0', errors.New("f")
}

func (ReadCloserImp) Close() error {
	return nil
}

type ReadCloserImp struct{}

func TestDownloadFileToPath(t *testing.T) {
	convey.Convey("Test New Obs Client error 1", t, func() {
		filePath := "./test"
		objectOutput := &obs.GetObjectOutput{
			DeleteMarker:       true,
			CacheControl:       "string",
			ContentDisposition: "string",
			ContentEncoding:    "string",
			ContentLanguage:    "string",
			Expires:            "string",
			Body:               ReadCloserImp{},
		}
		_, err := DownloadFileToPath(filePath, objectOutput, false)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Obs Client error 2", t, func() {
		filePath := " "
		objectOutput := &obs.GetObjectOutput{
			DeleteMarker:       true,
			CacheControl:       "string",
			ContentDisposition: "string",
			ContentEncoding:    "string",
			ContentLanguage:    "string",
			Expires:            "string",
			Body:               ReadCloserImp{},
		}
		_, err := DownloadFileToPath(filePath, objectOutput, false)
		convey.So(err, convey.ShouldNotBeNil)
	})
	convey.Convey("Test New Obs Client error 3", t, func() {
		patch := gomonkey.ApplyFunc(os.MkdirAll, func(path string, perm os.FileMode) error {
			return errors.New("err")
		})
		defer patch.Reset()
		filePath := "./test"
		objectOutput := &obs.GetObjectOutput{
			DeleteMarker:       true,
			CacheControl:       "string",
			ContentDisposition: "string",
			ContentEncoding:    "string",
			ContentLanguage:    "string",
			Expires:            "string",
			Body:               ReadCloserImp{},
		}
		_, err := DownloadFileToPath(filePath, objectOutput, false)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestDownloadFileToPathBySlice(t *testing.T) {

	convey.Convey("Test DownloadFileToPathBySlice success", t, func() {
		patch := gomonkey.ApplyFunc(DownloadFile, func(input *obs.DownloadFileInput, client Client) error {
			return nil
		})
		defer patch.Reset()
		bucketID := "string"
		objectID := "string"
		objPath := "string"
		objSize := 100001
		client := Client{cli: &obs.ObsClient{}}
		err := DownloadFileToPathBySlice(bucketID, objectID, objPath, int64(objSize), client)
		convey.So(err, convey.ShouldBeNil)
	})

	convey.Convey("Test DownloadFileToPathBySlice success error 1", t, func() {
		patch := gomonkey.ApplyFunc(DownloadFile, func(input *obs.DownloadFileInput, client Client) error {
			return errors.New("Test DownloadFileToPathBySlice  fail")
		})
		defer patch.Reset()
		bucketID := "string"
		objectID := "string"
		objPath := "string"
		objSize := 100001
		client := Client{cli: &obs.ObsClient{}}
		err := DownloadFileToPathBySlice(bucketID, objectID, objPath, int64(objSize), client)
		convey.So(err, convey.ShouldNotBeNil)
	})

	convey.Convey("Test DownloadFileToPathBySlice success error obs", t, func() {
		patch := gomonkey.ApplyFunc(DownloadFile, func(input *obs.DownloadFileInput, client Client) error {
			return obs.ObsError{Code: "500", Message: "aaa"}
		})
		defer patch.Reset()
		bucketID := "string"
		objectID := "string"
		objPath := "string"
		objSize := 100001
		client := Client{cli: &obs.ObsClient{}}
		err := DownloadFileToPathBySlice(bucketID, objectID, objPath, int64(objSize), client)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestClient(t *testing.T) {
	client := Client{cli: &obs.ObsClient{}}
	_, err := client.GetObject(nil)
	assert.NotEmpty(t, err)
	_, err = client.GetObjectMetadata(nil)
	assert.NotEmpty(t, err)
}

func TestGetOutputObjectBuffer(t *testing.T) {
	objectOutput := &obs.GetObjectOutput{
		DeleteMarker:       true,
		CacheControl:       "string",
		ContentDisposition: "string",
		ContentEncoding:    "string",
		ContentLanguage:    "string",
		Expires:            "string",
		Body:               ReadCloserImp{},
	}
	convey.Convey("Test Get Output Object Buffer", t, func() {
		buffer, err := GetOutputObjectBuffer(objectOutput)
		convey.So(buffer, convey.ShouldBeNil)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestClearSecretKeyMemory(t *testing.T) {
	secretKey := []byte("")
	secretKeyRecord = *(*string)(unsafe.Pointer(&secretKey))
	{
		// assert no panic
		ClearSecretKeyMemory()
	}
	{
		skRecord = []byte("b")
		securityTokenRecord = []byte("a")
		ClearSecretKeyMemory()
		if string(skRecord) == "b" || string(securityTokenRecord) == "a" {
			t.Errorf("failed to clear secret record")
		}
	}
	{
		// assert no panic
		ClearCustomSecretKeyMemory()
	}
	{
		customSecretKeyRecord = string([]byte{'a', 'b', 'c'})
		ClearCustomSecretKeyMemory()
		if customSecretKeyRecord == "abc" {
			t.Errorf("failed to clear custom secret record")
		}
	}
}
