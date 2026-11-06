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

package localauth

import (
	"errors"
	"sync"

	_ "huaweicloud.com/containers/security/cbb_adapt/src/go/aeswithkey"
	"huaweicloud.com/containers/security/cbb_adapt/src/go/gcrypto"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

var (
	gcryptoEngine gcrypto.Engine
	algorithm     = "aeswithkey"
	once          sync.Once
)

func initOnce() error {
	engine, err := gcrypto.New(algorithm)
	if err != nil {
		log.GetLogger().Errorf("failed to initialize the crypto engine")
		return err
	}
	gcryptoEngine = engine
	return nil
}

// Decrypt decrypts a cypher text using a certain algorithm
func Decrypt(src string) ([]byte, error) {
	var err error
	once.Do(func() {
		err = initOnce()
	})
	if gcryptoEngine == nil {
		return nil, err
	}

	plaintext, err := gcryptoEngine.Decrypt(0, src)
	if err != nil {
		return nil, errors.New("failed to decrypt the ciphertext")
	}
	text := []byte(plaintext)
	utils.ClearStringMemory(plaintext)
	return text, nil
}

// Encrypt encrypts a cypher text using a certain algorithm
func Encrypt(src string) (string, error) {
	var err error
	once.Do(func() {
		err = initOnce()
	})
	if gcryptoEngine == nil {
		return "", errors.New("gcrypto engine is null")
	}

	ciperText, err := gcryptoEngine.Encrypt(0, src)
	if err != nil {
		return "", errors.New("failed to encrypt the data")
	}
	return ciperText, nil
}

// DecryptKeys decrypts a set of aKey and sKey
func DecryptKeys(inputAKey string, inputSKey string) ([]byte, []byte, error) {
	aKey, err := Decrypt(inputAKey)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt AKey, error: %s", err.Error())
		return nil, nil, err
	}
	sKey, err := Decrypt(inputSKey)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt SKey, error: %s", err.Error())
		return nil, nil, err
	}
	return aKey, sKey, nil
}
