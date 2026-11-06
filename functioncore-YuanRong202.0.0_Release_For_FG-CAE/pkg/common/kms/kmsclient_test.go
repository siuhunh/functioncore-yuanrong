/*
 * Copyright (c) 2025 Huawei Technologies Co., Ltd
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

// Package kms
package kms

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	kms "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/kms/v2"
	kmsModel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/kms/v2/model"
	. "github.com/smartystreets/goconvey/convey"
)

func TestKmsClient(t *testing.T) {
	client := NewKmsClient("abc", "ak", "sk", "sts-token")
	keyId := strings.Repeat("a", 32)
	Convey("Test CreateDefaultKey", t, func() {
		Convey("error", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "CreateKey", func(_ *kms.KmsClient, request *kmsModel.CreateKeyRequest) (*kmsModel.CreateKeyResponse, error) {
				return nil, errors.New("error alias")
			}).Reset()
			_, err := client.CreateDefaultKey()
			So(err.Error(), ShouldEqual, "error alias")
		})
		Convey("ok", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "CreateKey", func(_ *kms.KmsClient, request *kmsModel.CreateKeyRequest) (*kmsModel.CreateKeyResponse, error) {
				return &kmsModel.CreateKeyResponse{KeyInfo: &kmsModel.KeKInfo{KeyId: &keyId}}, nil
			}).Reset()
			key, err := client.CreateDefaultKey()
			So(err, ShouldBeNil)
			So(key, ShouldEqual, keyId)
		})
	})
	Convey("Test GetKeyByAlias", t, func() {
		keyAlias := "functiongraph/default"
		domainId := "abc"
		Convey("error", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "ListKeys", func(_ *kms.KmsClient, request *kmsModel.ListKeysRequest) (*kmsModel.ListKeysResponse, error) {
				return nil, errors.New("error")
			}).Reset()
			_, err := client.GetKeyByAlias(keyAlias, "abc")
			So(err.Error(), ShouldEqual, "error")
		})
		Convey("key not found", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "ListKeys", func(_ *kms.KmsClient, request *kmsModel.ListKeysRequest) (*kmsModel.ListKeysResponse, error) {
				return nil, nil
			}).Reset()
			key, err := client.GetKeyByAlias(keyAlias, "abc")
			So(err, ShouldBeNil)
			So(key, ShouldBeNil)
		})
		Convey("ok", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "ListKeys", func(_ *kms.KmsClient, request *kmsModel.ListKeysRequest) (*kmsModel.ListKeysResponse, error) {
				truncated := kmsModel.GetListKeysResponseTruncatedEnum().TRUE
				return &kmsModel.ListKeysResponse{KeyDetails: &[]kmsModel.KeyDetails{{KeyId: &keyId, KeyAlias: &keyAlias, DomainId: &domainId}}, Truncated: &truncated}, nil
			}).Reset()
			key, err := client.GetKeyByAlias("functiongraph/default", "abc")
			So(err, ShouldBeNil)
			So(*key.KeyId, ShouldEqual, keyId)
		})
	})
	Convey("Test GetKeyDetails", t, func() {
		Convey("error", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "ListKeyDetail", func(_ *kms.KmsClient, request *kmsModel.ListKeyDetailRequest) (*kmsModel.ListKeyDetailResponse, error) {
				return nil, errors.New("error")
			}).Reset()
			_, err := client.GetKeyDetails(keyId)
			So(err.Error(), ShouldEqual, "error")
		})
		Convey("ok", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "ListKeyDetail", func(_ *kms.KmsClient, request *kmsModel.ListKeyDetailRequest) (*kmsModel.ListKeyDetailResponse, error) {
				return &kmsModel.ListKeyDetailResponse{KeyInfo: &kmsModel.KeyDetails{KeyId: &keyId}}, nil
			}).Reset()
			key, err := client.GetKeyDetails(keyId)
			So(err, ShouldBeNil)
			So(*key.KeyId, ShouldEqual, keyId)
		})
	})
	Convey("Test CreateDataKey", t, func() {
		Convey("error", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "CreateDatakey", func(_ *kms.KmsClient, request *kmsModel.CreateDatakeyRequest) (*kmsModel.CreateDatakeyResponse, error) {
				return nil, errors.New("error")
			}).Reset()
			_, err := client.CreateDataKey(keyId)
			So(err.Error(), ShouldEqual, "error")
		})
		Convey("ok", func() {
			plainText := "xxx"
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "CreateDatakey", func(_ *kms.KmsClient, request *kmsModel.CreateDatakeyRequest) (*kmsModel.CreateDatakeyResponse, error) {
				return &kmsModel.CreateDatakeyResponse{KeyId: &keyId, PlainText: &plainText}, nil
			}).Reset()
			key, err := client.CreateDataKey(keyId)
			So(err, ShouldBeNil)
			So(*key.PlainText, ShouldEqual, plainText)
		})
	})
	Convey("Test DecryptDataKey", t, func() {
		Convey("error", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "DecryptDatakey", func(_ *kms.KmsClient, request *kmsModel.DecryptDatakeyRequest) (*kmsModel.DecryptDatakeyResponse, error) {
				return nil, errors.New("error")
			}).Reset()
			key, err := client.DecryptDataKey(keyId, "xxx")
			So(err.Error(), ShouldEqual, "error")
			So(key, ShouldBeNil)
		})
		Convey("ok", func() {
			plainText := "xxx"
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "DecryptDatakey", func(_ *kms.KmsClient, request *kmsModel.DecryptDatakeyRequest) (*kmsModel.DecryptDatakeyResponse, error) {
				return &kmsModel.DecryptDatakeyResponse{DataKey: &plainText}, nil
			}).Reset()
			key, err := client.DecryptDataKey(keyId, "yyy")
			So(err, ShouldBeNil)
			So(*key.DataKey, ShouldEqual, plainText)
		})
	})
	Convey("Test EncryptDataWithAES", t, func() {
		Convey("create datakey error", func() {
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "CreateDatakey", func(_ *kms.KmsClient, request *kmsModel.CreateDatakeyRequest) (*kmsModel.CreateDatakeyResponse, error) {
				return nil, errors.New("error")
			}).Reset()
			_, _, err := client.EncryptDataWithAES(keyId, "abc")
			So(err.Error(), ShouldEqual, "error")
		})
		Convey("aes Encrypt failed", func() {
			plainText := "abc"
			defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "CreateDatakey", func(_ *kms.KmsClient, request *kmsModel.CreateDatakeyRequest) (*kmsModel.CreateDatakeyResponse, error) {
				return &kmsModel.CreateDatakeyResponse{KeyId: &keyId, PlainText: &plainText}, nil
			}).Reset()
			_, _, err := client.EncryptDataWithAES("abc", "hello")
			So(err, ShouldNotBeNil)
		})
	})
	Convey("Test DecryptDataWithAES error", t, func() {
		defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "DecryptDatakey", func(_ *kms.KmsClient, request *kmsModel.DecryptDatakeyRequest) (*kmsModel.DecryptDatakeyResponse, error) {
			return nil, errors.New("error")
		}).Reset()
		_, err := client.DecryptDataWithAES(keyId, "xxx", "abc")
		So(err.Error(), ShouldEqual, "error")
	})
	Convey("Test Encrypt and Decrypt DataWithAES", t, func() {
		data := "hello world"
		plainText := strings.Repeat("a", 32)
		defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "CreateDatakey", func(_ *kms.KmsClient, request *kmsModel.CreateDatakeyRequest) (*kmsModel.CreateDatakeyResponse, error) {
			return &kmsModel.CreateDatakeyResponse{KeyId: &keyId, PlainText: &plainText, CipherText: &data}, nil
		}).Reset()
		defer gomonkey.ApplyMethod(reflect.TypeOf(&kms.KmsClient{}), "DecryptDatakey", func(_ *kms.KmsClient, request *kmsModel.DecryptDatakeyRequest) (*kmsModel.DecryptDatakeyResponse, error) {
			return &kmsModel.DecryptDatakeyResponse{DataKey: &plainText}, nil
		}).Reset()

		enData, cipherText, err := client.EncryptDataWithAES(keyId, data)
		So(err, ShouldBeNil)
		deData, err := client.DecryptDataWithAES(keyId, cipherText, enData)
		So(err, ShouldBeNil)
		So(deData, ShouldEqual, data)
	})
}
