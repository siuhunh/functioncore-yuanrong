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
	"os"
	"time"

	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/auth/basic"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/config"
	"github.com/huaweicloud/huaweicloud-sdk-go-v3/core/region"
	kms "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/kms/v2"
	kmsModel "github.com/huaweicloud/huaweicloud-sdk-go-v3/services/kms/v2/model"

	"functioncore/pkg/common/logger/log"
)

const (
	httpTimeout = 8 * time.Second // second
	httpRetries = 5

	// FgDefaultKeyAlias -
	FgDefaultKeyAlias = "functiongraph/default" // FG 默认kms主秘钥，只给本服务使用，只能创建一次且不能更改和删除
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

// CreateDefaultKey 创建默认秘钥 functiongraph/default, KMS有白名单控制，创建前需要申请
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

// GetKeyByAlias 根据别名获取key id
// 秘钥可能会账号间共享，可通过domainID区分本账号的秘钥
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

// GetKeyDetails 获取key详细信息
func (c *Client) GetKeyDetails(keyID string) (*kmsModel.KeyDetails, error) {
	req := kmsModel.ListKeyDetailRequest{Body: &kmsModel.OperateKeyRequestBody{KeyId: keyID}}
	detail, err := c.client.ListKeyDetail(&req)
	if err != nil {
		return nil, err
	}
	log.GetLogger().Infof("get kms key detail: %v", *detail.KeyInfo)
	return detail.KeyInfo, nil
}

// CreateDataKey 创建数据加密秘钥
func (c *Client) CreateDataKey(keyID string) (*kmsModel.CreateDatakeyResponse, error) {
	req := kmsModel.CreateDatakeyRequest{Body: &kmsModel.CreateDatakeyRequestBody{
		KeyId:         keyID,
		DatakeyLength: &aesKeyLength,
	}}
	return c.client.CreateDatakey(&req)
}

// DecryptDataKey 解密秘钥
func (c *Client) DecryptDataKey(keyID, cipherText string) (*kmsModel.DecryptDatakeyResponse, error) {
	req := kmsModel.DecryptDatakeyRequest{Body: &kmsModel.DecryptDatakeyRequestBody{
		KeyId:      keyID,
		CipherText: cipherText,
	}}
	return c.client.DecryptDatakey(&req)
}

// EncryptDataWithAES 使用AES加密算法+KMS秘钥加密数据
// 返回加密后的数据，加密秘钥，错误信息
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

// DecryptDataWithAES 先解密KMS加密秘钥，再使用AES解密算法解密
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
