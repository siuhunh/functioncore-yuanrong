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

// Package utils for common functions
package utils

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
)

func TestGetResourcePath(t *testing.T) {
	Convey("TestInitConfig", t, func() {
		err := os.Unsetenv("ResourcePath")
		if err != nil {
			return
		}
		// exception test: abs should fail
		patch := ApplyFunc(filepath.Abs, func(string) (string, error) {
			return "", errors.New("abs error")
		})
		So(GetResourcePath(), ShouldEqual, "resource")
		patch.Reset()

		err = os.Setenv("ResourcePath", "...")
		if err == nil {
			pwd, err := os.Getwd()
			if err != nil {
				return
			}
			So(GetResourcePath(), ShouldEqual, pwd+"/...")
		}
		// normal test
		path := "/home/sn/resource"
		err = os.Setenv("ResourcePath", path)
		if err == nil {
			So(GetResourcePath(), ShouldEqual, path)
		}
		return
	})
}
