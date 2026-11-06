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

package config

import (
	"errors"
	"os"
)

var (
	swrAPI string
)

// InitEnvs if SWR_API exists use SWR_REGION_ID, else use REGION_ID
func InitEnvs() error {
	swrAPI = os.Getenv("SWR_API")
	if swrAPI == "" {
		return errors.New("failed to get SWR_API from env")
	}
	return nil
}

// GetSWRAPI get swrRegionID
func GetSWRAPI() string {
	return swrAPI
}
