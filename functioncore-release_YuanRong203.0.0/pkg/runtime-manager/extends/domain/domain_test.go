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

package domain

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/utils"
)

func TestResetDomainName(t *testing.T) {
	ResetDomainName([]byte("body"))
	convey.Convey("TestResetDomainNameJsonUnmarshalError", t, func() {
		isMocked := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
				isMocked = true
				return &os.File{}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		ResetDomainName([]byte(""))
		convey.So(isMocked, convey.ShouldBeFalse)
	})
	convey.Convey("TestResetDomainNameJsonUnmarshalError2", t, func() {
		isMocked := false
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
				isMocked = true
				return &os.File{}, nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		bodyMap := map[string]string{
			constants.HeaderDomainName: "",
		}
		body, err := json.Marshal(bodyMap)
		convey.So(err, convey.ShouldBeNil)
		ResetDomainName(body)
		convey.So(isMocked, convey.ShouldBeFalse)
	})
	convey.Convey("TestResetDomainName", t, func() {
		isMocked := false
		bodyDomainNames := map[string]string{
			"www.baidu.com": "192.168.0.1",
			"www.zhihu.com": "192.168.0.3",
			"www.qq.com":    "192.168.0.5",
		}
		mockScanner := &bufio.Scanner{}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(os.Open, func(name string) (*os.File, error) {
				isMocked = true
				return &os.File{}, nil
			}),
			gomonkey.ApplyFunc(os.Create, func(name string) (*os.File, error) {
				isMocked = true
				return nil, errors.New("mock error")
			}),
			gomonkey.ApplyFunc(bufio.NewScanner, func(r io.Reader) *bufio.Scanner {
				return nil
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockScanner), "Scan", func(_ *bufio.Scanner) bool {
				return false
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(mockScanner), "Err", func(_ *bufio.Scanner) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		domainNamesBytes, err := json.Marshal(bodyDomainNames)
		convey.So(err, convey.ShouldBeNil)
		bodyMap := map[string]string{
			constants.HeaderDomainName: string(domainNamesBytes),
		}
		body, err := json.Marshal(bodyMap)
		convey.So(err, convey.ShouldBeNil)
		ResetDomainName(body)
		convey.So(isMocked, convey.ShouldBeTrue)
	})
	convey.Convey("TestResetDomainNameSuccess", t, func() {
		bodyDomainNames := map[string]string{
			"www.baidu.com": "192.168.0.1",
			"www.zhihu.com": "192.168.0.3",
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.ReplaceByDNS, func(filePath string, domainNames map[string]string) error {
				return nil
			}),
		}
		defer func() {
			for idx := range patches {
				patches[idx].Reset()
			}
		}()
		domainNamesBytes, err := json.Marshal(bodyDomainNames)
		convey.So(err, convey.ShouldBeNil)
		bodyMap := map[string]string{
			constants.HeaderDomainName: string(domainNamesBytes),
		}
		body, err := json.Marshal(bodyMap)
		err = ResetDomainName(body)
		convey.So(err, convey.ShouldBeNil)
	})
}
