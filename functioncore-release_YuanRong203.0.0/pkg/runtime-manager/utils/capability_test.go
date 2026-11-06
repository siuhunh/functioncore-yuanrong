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

package utils

import (
	"errors"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
)

func TestClearCurrentCaps(t *testing.T) {
	convey.Convey("test clear", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(getCAP,
				func(header *userCAPHeader, data *userCAPData) (err error) {
					return nil
				}),
			gomonkey.ApplyFunc(setCAP,
				func(header *userCAPHeader, data *userCAPData) (err error) {
					return nil
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := ClearCurrentCAPs()
		assert.Equal(t, nil, err)
	})
	convey.Convey("test clear", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(getCAP,
				func(header *userCAPHeader, data *userCAPData) (err error) {
					return errors.New("failed to getCAP")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := ClearCurrentCAPs()
		assert.Equal(t, errors.New("failed to getCAP"), err)
	})
	convey.Convey("test clear", t, func() {
		patches := [...]*gomonkey.Patches{
			gomonkey.ApplyFunc(getCAP,
				func(header *userCAPHeader, data *userCAPData) (err error) {
					return nil
				}),
			gomonkey.ApplyFunc(setCAP,
				func(header *userCAPHeader, data *userCAPData) (err error) {
					return errors.New("failed to setCAP")
				}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		err := ClearCurrentCAPs()
		assert.Equal(t, errors.New("failed to setCAP"), err)
	})
}

func Test_ClearCAPs(t *testing.T) {
	convey.Convey("Test_ClearCAPs", t, func() {
		convey.Convey("Test_ClearCAPs_success", func() {
			ClearCAPs()
		})
	})
}
