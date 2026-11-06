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

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/asaskevich/govalidator"
	"github.com/stretchr/testify/assert"
)

func Test_GetConfig(t *testing.T) {
	patches := ApplyFunc(os.LookupEnv, func(string) (string, bool) { return "", false })
	_, err := GetConfig()
	assert.Equal(t, fmt.Errorf("config key %s does not exist", configEnvKey), err)
	patches.Reset()
	StablePatches := ApplyFunc(os.LookupEnv, func(string) (string, bool) { return "", true })

	defer StablePatches.Reset()
	patches = ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error { return fmt.Errorf("failed") })
	_, err = GetConfig()
	assert.Equal(t, errors.New("failed to unmarshal runtime manager config with error: "+"failed"), err)
	patches.Reset()
	StablePatches = ApplyFunc(json.Unmarshal, func(data []byte, v interface{}) error { return nil })

	patches = ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
		return true, fmt.Errorf("failed")
	})
	_, err = GetConfig()
	assert.Equal(t, errors.New("runtime manager config file checking error: "+"failed"), err)
	patches.Reset()

	StablePatches = ApplyFunc(govalidator.ValidateStruct, func(s interface{}) (bool, error) {
		return true, nil
	})
	rmConfig, _ := GetConfig()
	assert.Equal(t, &Config{}, rmConfig)
}
