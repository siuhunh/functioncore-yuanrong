/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package render encoding/decoding code cache
package render

import (
	"encoding/json"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/cache-manager/pb"
)

var (
	marshalFunc = jsonMarshal
)

func init() {
	if os.Getenv("EVS_ENABLE") == "true" {
		marshalFunc = pbMarshal
	}
}

// Marshal marshals function code
func Marshal(objectID string, buffer []byte, checksum string) ([]byte, error) {
	return marshalFunc(objectID, buffer, checksum)
}

func pbMarshal(objectID string, buffer []byte, checksum string) ([]byte, error) {
	return proto.Marshal(&pb.CacheCode{
		ObjectID: objectID,
		Buffer:   buffer,
		Checksum: checksum,
	})
}

func jsonMarshal(objectID string, buffer []byte, checksum string) ([]byte, error) {
	return json.Marshal(config.CacheCode{
		ObjectID:  objectID,
		Buffer:    buffer,
		Checksum:  checksum,
		TimeStamp: time.Now(),
	})
}
