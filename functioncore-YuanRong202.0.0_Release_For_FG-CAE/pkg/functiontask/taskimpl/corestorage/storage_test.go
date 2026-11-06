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

package corestorage

import (
	"testing"

	. "github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	functioncore "functioncore/pkg/core-sdk/sdk"
	"functioncore/pkg/functiontask/task/common"
	"functioncore/test/utils"
)

func TestStorage_Set(t *testing.T) {
	Convey("TestStorage_Set", t, func() {
		s := NewStorage()
		patches := []*Patches{
			ApplyFunc(functioncore.SetKeyWithTTL, func(key, value string, ttl int) snerror.SNError {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		Convey("test: set failed", func() {
			patch := ApplyFunc(functioncore.SetKeyWithTTL, func(key, value string, ttl int) snerror.SNError {
				return snerror.New(common.EFunctionCore, "")
			})
			defer patch.Reset()
			err := s.Set("xx", "v", 0)
			So(err, ShouldNotBeNil)
			So(err.Code(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: set success", func() {
			err := s.Set("xx", "v", 0)
			So(err, ShouldBeNil)
		})
	})
}
func TestStorage_Get(t *testing.T) {
	Convey("TestStorage_Get", t, func() {
		s := NewStorage()
		patches := []*Patches{
			ApplyFunc(functioncore.GetState, func(stateID string) (string, snerror.SNError) {
				return "xxx", nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		Convey("test: get failed", func() {
			patch := ApplyFunc(functioncore.GetState, func(stateID string) (string, snerror.SNError) {
				return "", snerror.New(common.EFunctionCore, "")
			})
			defer patch.Reset()
			_, err := s.Get("xx")
			So(err, ShouldNotBeNil)
			So(err.Code(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: get success", func() {
			state, err := s.Get("xx")
			So(err, ShouldBeNil)
			So(state, ShouldEqual, "xxx")
		})
	})
}

func TestStorage_Delete(t *testing.T) {
	Convey("TestStorage_Delete", t, func() {
		s := NewStorage()
		patches := []*Patches{
			ApplyFunc(functioncore.DeleteState, func(stateID string) snerror.SNError {
				return nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		Convey("test: delete failed", func() {
			patch := ApplyFunc(functioncore.DeleteState, func(stateID string) snerror.SNError {
				return snerror.New(common.EFunctionCore, "")
			})
			defer patch.Reset()
			err := s.Delete("xxx")
			So(err, ShouldNotBeNil)
			So(err.Code(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: delete success", func() {
			err := s.Delete("xxx")
			So(err, ShouldBeNil)
		})
	})
}
func TestStorage_Scan(t *testing.T) {
	Convey("TestStorage_Scan", t, func() {
		s := NewStorage()
		patches := []*Patches{
			ApplyFunc(functioncore.ScanState, func(pattern string) (map[string]string, snerror.SNError) {
				return map[string]string{"s1": "1", "s2": "2"}, nil
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		Convey("test: scan failed", func() {
			patch := ApplyFunc(functioncore.ScanState, func(pattern string) (map[string]string, snerror.SNError) {
				return nil, snerror.New(common.EFunctionCore, "")
			})
			defer patch.Reset()
			_, err := s.Scan("s:*")
			So(err, ShouldNotBeNil)
			So(err.Code(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: scan success", func() {
			states, err := s.Scan("s:*")
			So(err, ShouldBeNil)
			So(states, ShouldContainKey, "s1")
			So(states, ShouldContainKey, "s2")
		})
	})
}
func TestStorage_Exists(t *testing.T) {
	Convey("TestStorage_Exists", t, func() {
		s := NewStorage()
		patches := []*Patches{
			ApplyFunc(functioncore.Exists, func(key string) (bool, snerror.SNError) {
				switch key {
				case "existState":
					return true, nil
				default:
					return false, nil
				}
			}),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		Convey("test: call failed", func() {
			patch := ApplyFunc(functioncore.Exists, func(key string) (bool, snerror.SNError) {
				return true, snerror.New(common.EFunctionCore, "")
			})
			defer patch.Reset()
			_, err := s.Exists("s:1")
			So(err, ShouldNotBeNil)
			So(err.Code(), ShouldEqual, common.EFunctionCore)
		})
		Convey("test: call success", func() {
			exists, err := s.Exists("existState")
			So(err, ShouldBeNil)
			So(exists, ShouldBeTrue)
			exists, err = s.Exists("NotExistState")
			So(err, ShouldBeNil)
			So(exists, ShouldBeFalse)
		})
	})
}

func TestStorage_GetNumsByServiceID(t *testing.T) {
	type args struct {
		pattern string
	}
	tests := []struct {
		name        string
		args        args
		patchesFunc utils.PatchesFunc
		want        int
	}{
		// test error
		{
			"case1",
			args{},
			func() utils.PatchSlice {
				return utils.InitPatchSlice()
			},
			0,
		},
		// test success
		{
			"case2",
			args{},
			func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{
					ApplyFunc(functioncore.HLen, func(key string) (int, snerror.SNError) {
						return 10, nil
					}),
				})
				return patches
			},
			10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Storage{}
			patches := tt.patchesFunc()
			got, _ := s.GetNumsByServiceID(tt.args.pattern)
			if got != tt.want {
				t.Errorf("GetNumsByServiceID() got = %v, want %v", got, tt.want)
			}
			patches.ResetAll()
		})
	}
}
