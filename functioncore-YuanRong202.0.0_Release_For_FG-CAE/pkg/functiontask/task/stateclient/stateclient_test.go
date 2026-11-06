/*
 * Copyright (c) 2024 Huawei Technologies Co., Ltd
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
package stateclient

import (
	"errors"
	"reflect"
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/core-sdk/common"
	sc "functioncore/pkg/functionstate/client"
)

func TestHSetNX_HDel_HGetAll_HExists_HLen(t *testing.T) {
	Convey("TestHSetNX_HDel_HGetAll_HExists_HLen", t, func() {
		Convey("success", func() {
			StateCli = &sc.StateClient{}
			patch := ApplyGlobalVar(&StateCli, new(sc.StateClient))
			defer patch.Reset()
			patches := []*Patches{
				ApplyMethod(reflect.TypeOf(StateCli), "HSetNX",
					func(_ *sc.StateClient, key string, filed string, value string) (bool, error) {
						return true, nil
					}),
				ApplyMethod(reflect.TypeOf(StateCli), "HDel",
					func(_ *sc.StateClient, key string, fields ...string) (int, error) {
						return 1, nil
					}),
				ApplyMethod(reflect.TypeOf(StateCli), "HExists",
					func(_ *sc.StateClient, key string, filed string) (bool, error) {
						return true, nil
					}),
			}
			defer func() {
				for _, patch := range patches {
					patch.Reset()
				}
			}()
			{
				n, err := HSetNX("key", "filed", "value")
				So(n, ShouldEqual, true)
				So(err, ShouldEqual, nil)
			}
			{
				values, err := HDel("key", "fields")
				So(values, ShouldEqual, 1)
				So(err, ShouldEqual, nil)
			}
			{
				n, err := HExists("key", "filed")
				So(n, ShouldEqual, true)
				So(err, ShouldEqual, nil)
			}
			Convey("state client is nil", func() {
				StateCli = nil
				error := errors.New(common.StateClientErrMsg)
				_, err := HSetNX("key", "filed", "value")
				So(err.Error(), ShouldEqual, error.Error())
				_, err = HDel("key", "fields")
				So(err.Error(), ShouldEqual, error.Error())
				_, err = HExists("key", "filed")
				So(err.Error(), ShouldEqual, error.Error())
			})
			Convey("error", func() {
				patch := ApplyGlobalVar(&StateCli, new(sc.StateClient))
				defer patch.Reset()
				patches := []*Patches{
					ApplyMethod(reflect.TypeOf(StateCli), "HSetNX",
						func(_ *sc.StateClient, key string, filed string, value string) (bool, error) {
							return false, errors.New("test HSetNX error")
						}),
					ApplyMethod(reflect.TypeOf(StateCli), "HDel",
						func(_ *sc.StateClient, key string, fields ...string) (int, error) {
							return 0, errors.New("test HDel error")
						}),
					ApplyMethod(reflect.TypeOf(StateCli), "HExists",
						func(_ *sc.StateClient, key string, filed string) (bool, error) {
							return false, errors.New("test HSetNX error")
						}),
				}
				defer func() {
					for _, patch := range patches {
						patch.Reset()
					}
				}()
				_, err := HSetNX("key", "filed", "value")
				So(err, ShouldNotEqual, nil)
				_, err = HDel("key", "fields")
				So(err, ShouldNotEqual, nil)
				_, err = HExists("key", "filed")
				So(err, ShouldNotEqual, nil)
			})
		})
	})
}
