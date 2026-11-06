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

// Package kms -
package kms

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/basic"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/region"
	kms "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/kms/v2"
	kmsModel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/kms/v2/model"

	"functioncore/pkg/common/logger/log"
)

const (
	httpTimeout      = 8 * time.Second // second
	httpRetries      = 5
	httpRetriesLimit = 3

	// FgDefaultKeyAlias FG the default KMS master key is exclusively for use by this service
	// can only be created once, and cannot be modified or deleted
	FgDefaultKeyAlias = "functiongraph/default"
)

var (
	regionID = os.Getenv("REGION_ID")
	endpoint = fmt.Sprintf("kms.%s.myhuaweicloud.com", regionID)

	aesKeyLength = "256"
)

func init() {
	if os.Getenv("kms_endpoint") != "" {
		endpoint = os.Getenv("kms_endpoint")
	}
}

// Client -
type Client struct {
	client *kms.KmsClient
}

// NewKmsClient -
func NewKmsClient(projectID string, ak, sk, securityToken string) *Client {
	authBuilder := basic.NewCredentialsBuilder().WithProjectId(projectID).WithAk(ak).WithSk(sk)
	if securityToken != "" {
		authBuilder = authBuilder.WithSecurityToken(securityToken)
	}
	auth := authBuilder.Build()
	httpConfig := config.DefaultHttpConfig()
	httpConfig.WithIgnoreSSLVerification(true).WithTimeout(httpTimeout).WithRetries(httpRetries)
	client := kms.NewKmsClient(
		kms.KmsClientBuilder().
			WithRegion(region.NewRegion(regionID, endpoint)).
			WithHttpConfig(httpConfig).
			WithCredential(auth).
			Build())
	return &Client{client: client}
}

// CreateDefaultKey Create the default secret key functiongraph/default.
// KMS has a whitelist control, so an application is required before creation
func (c *Client) CreateDefaultKey() (string, error) {
	keySpec := kmsModel.GetCreateKeyRequestBodyKeySpecEnum().AES_256
	keyUsage := kmsModel.GetCreateKeyRequestBodyKeyUsageEnum().ENCRYPT_DECRYPT
	origin := kmsModel.GetCreateKeyRequestBodyOriginEnum().KMS
	req := kmsModel.CreateKeyRequest{Body: &kmsModel.CreateKeyRequestBody{
		KeyAlias: FgDefaultKeyAlias,
		KeySpec:  &keySpec,
		KeyUsage: &keyUsage,
		Origin:   &origin,
	}}
	key, err := c.client.CreateKey(&req)
	if err != nil {
		return "", err
	}
	return *key.KeyInfo.KeyId, nil
}

// GetKeyByAlias get key id by alias
// The key may be shared across accounts and can be distinguished for this account through the domainID
func (c *Client) GetKeyByAlias(alias string, domainID string) (*kmsModel.KeyDetails, error) {
	marker := "0"
	for {
		req := kmsModel.ListKeysRequest{Body: &kmsModel.ListKeysRequestBody{Marker: &marker}}
		keys, err := c.client.ListKeys(&req)
		if err != nil {
			return nil, err
		}
		if keys == nil || keys.Truncated.Value() == "false" {
			break
		}
		for _, detail := range *keys.KeyDetails {
			if *detail.KeyAlias == alias && *detail.DomainId == domainID {
				return &detail, nil
			}
		}
		marker = *keys.NextMarker
	}
	return nil, nil
}

// GetKeyDetails get key details
func (c *Client) GetKeyDetails(keyID string) (*kmsModel.KeyDetails, error) {
	req := kmsModel.ListKeyDetailRequest{Body: &kmsModel.OperateKeyRequestBody{KeyId: keyID}}
	detail, err := c.client.ListKeyDetail(&req)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Infof("get kms key detail: %v", *detail.KeyInfo)
	return detail.KeyInfo, nil
}

// CreateDataKey create a data encryption key
func (c *Client) CreateDataKey(keyID string) (*kmsModel.CreateDatakeyResponse, error) {
	req := kmsModel.CreateDatakeyRequest{Body: &kmsModel.CreateDatakeyRequestBody{
		KeyId:         keyID,
		DatakeyLength: &aesKeyLength,
	}}
	return c.client.CreateDatakey(&req)
}

// DecryptDataKey -
func (c *Client) DecryptDataKey(keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
	req := kmsModel.DecryptDatakeyRequest{Body: &kmsModel.DecryptDatakeyRequestBody{
		KeyId:      keyID,
		CipherText: cipherText,
	}}
	return c.client.DecryptDatakey(&req)
}

// DecryptDataKeyWithRetry -
func (c *Client) DecryptDataKeyWithRetry(keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
	req := kmsModel.DecryptDatakeyRequest{Body: &kmsModel.DecryptDatakeyRequestBody{
		KeyId:      keyID,
		CipherText: cipherText,
	}}
	var resp *kmsModel.DecryptDatakeyResponse
	var err error
	for retryTimes := 1; retryTimes < httpRetriesLimit; retryTimes++ {
		resp, err = c.client.DecryptDatakey(&req)
		if err != nil && strings.Contains(err.Error(), strconv.Itoa(http.StatusTooManyRequests)) {
			log.GetLogger().Errorf("kms decrypt request reach api limit,"+
				" will retry. times: %d, err: %s", retryTimes, err.Error())
			time.Sleep(time.Duration(retryTimes) * time.Second)
			continue
		}
		break
	}
	return resp, err
}

// EncryptDataWithAES encrypt data using the AES encryption algorithm + KMS key
// return the encrypted data, encryption key, and error message
func (c *Client) EncryptDataWithAES(keyID, data string) (string, string, error) {
	key, err := c.CreateDataKey(keyID)
	if err != nil {
		return "", "", err
	}
	log.GetLogger().Infof("create data key: %v", *key)
	planKeyByte, err := hex.DecodeString(*key.PlainText)
	if err != nil {
		return "", "", err
	}
	enData, err := Encrypt(planKeyByte, data)
	if err != nil {
		return "", "", err
	}
	return enData, *key.CipherText, nil
}

// DecryptDataWithAES first, decrypt the KMS encryption key, then use the AES decryption algorithm to decrypt
func (c *Client) DecryptDataWithAES(keyID, cipherText, data string) (string, error) {
	key, err := c.DecryptDataKey(keyID, cipherText)
	if err != nil {
		return "", err
	}
	log.GetLogger().Infof("Decrypt data key: %v", *key)
	planKeyByte, err := hex.DecodeString(*key.DataKey)
	if err != nil {
		return "", err
	}
	return Decrypt(planKeyByte, data)
}

// DecryptDataWithAESByPlainCipherToByte  -
func DecryptDataWithAESByPlainCipherToByte(data, plainCipher string) ([]byte, error) {
	planKeyByte, err := hex.DecodeString(plainCipher)
	if err != nil {
		return []byte{}, err
	}
	return decryptToByte(planKeyByte, data)
}
