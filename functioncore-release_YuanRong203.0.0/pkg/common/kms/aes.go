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
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
)

// Encrypt 使用AES加密数据, key length must be 16, 24, or 32 bytes to AES-128, AES-192, or AES-256.
// 返回加密数据的base64编码
func Encrypt(key []byte, plaintext string) (string, error) {
	gcm, err := getGCM(key)
	if err != nil {
		return "", err
	}
	nonce := key[:gcm.NonceSize()]
	// 加密数据
	ciphertext := gcm.Seal([]byte(nonce), []byte(nonce), []byte(plaintext), nil)
	// 返回IV和加密后的数据
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt 使用AES解密数据
// 输入为使用base64编码的加密数据
func Decrypt(key []byte, ciphertext string) (string, error) {
	cipherBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}
	gcm, err := getGCM(key)
	if err != nil {
		return "", err
	}
	// 分离IV和加密后的数据
	nonceSize := gcm.NonceSize()
	iv := cipherBytes[:nonceSize]
	cipherData := cipherBytes[nonceSize:]
	// 解密数据
	plaintext, err := gcm.Open(nil, iv, cipherData, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// 使用AES解密数据
// 输入为使用base64编码的加密数据
func decryptToByte(key []byte, ciphertext string) ([]byte, error) {
	cipherBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return []byte{}, err
	}
	gcm, err := getGCM(key)
	if err != nil {
		return []byte{}, err
	}
	// 分离IV和加密后的数据
	nonceSize := gcm.NonceSize()
	iv := cipherBytes[:nonceSize]
	cipherData := cipherBytes[nonceSize:]
	// 解密数据
	plaintext, err := gcm.Open(nil, iv, cipherData, nil)
	if err != nil {
		return []byte{}, err
	}

	return plaintext, nil
}

func getGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	// 创建一个GCM模式的AEAD
	return cipher.NewGCM(block)
}
