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

// Encrypt encrypt data with AES, key length must be 16, 24, or 32 bytes to AES-128, AES-192, or AES-256.
// return the base64 encoding of the encrypted data
func Encrypt(key []byte, plaintext string) (string, error) {
	gcm, err := getGCM(key)
	if err != nil {
		return "", err
	}
	nonce := key[:gcm.NonceSize()]
	// encrypt data
	ciphertext := gcm.Seal([]byte(nonce), []byte(nonce), []byte(plaintext), nil)
	// return IV and encrypt data
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// Decrypt decrypt data with AES
// the input is encrypted data encoded in base64
func Decrypt(key []byte, ciphertext string) (string, error) {
	cipherBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return "", err
	}
	gcm, err := getGCM(key)
	if err != nil {
		return "", err
	}
	// separate IV and encrypted data
	nonceSize := gcm.NonceSize()
	iv := cipherBytes[:nonceSize]
	cipherData := cipherBytes[nonceSize:]
	// decrypt data
	plaintext, err := gcm.Open(nil, iv, cipherData, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// decrypt data with AES
// the input is encrypted data encoded in base64
func decryptToByte(key []byte, ciphertext string) ([]byte, error) {
	cipherBytes, err := base64.StdEncoding.DecodeString(ciphertext)
	if err != nil {
		return []byte{}, err
	}
	gcm, err := getGCM(key)
	if err != nil {
		return []byte{}, err
	}
	// separate IV and encrypted data
	nonceSize := gcm.NonceSize()
	iv := cipherBytes[:nonceSize]
	cipherData := cipherBytes[nonceSize:]
	// decrypt data
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
	// create an AEAD in GCM mode
	return cipher.NewGCM(block)
}
