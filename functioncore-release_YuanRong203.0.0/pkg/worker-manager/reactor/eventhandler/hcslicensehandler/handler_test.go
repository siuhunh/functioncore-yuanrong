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

package hcslicensehandler

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
	mockUtils "functioncore/test/utils"
)

func TestLicenseHandler_HandleEvent(t *testing.T) {
	convey.Convey("test HandleEvent", t, func() {
		convey.Convey("case1: invalid event type", func() {
			event := eventsource.Event{
				SourceType: "",
				EventType:  eventsource.Added,
				Kind:       utils.FunctionRepoEventKind,
			}
			handler := &LicenseHandler{}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "event type does not match HCSLicense invalid event")
		})
		convey.Convey("case2: invalid event object", func() {
			event := eventsource.Event{
				SourceType: "",
				EventType:  eventsource.Added,
				Kind:       utils.HCSLicenseEventKind,
				Object:     "error",
			}
			handler := &LicenseHandler{}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "invalid event object")
		})
		convey.Convey("case3: failed to decrypt data", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("aaa"), errors.New("error")
				}),
			})
			defer patches.ResetAll()

			event := eventsource.Event{
				SourceType: "",
				EventType:  eventsource.Added,
				Kind:       utils.HCSLicenseEventKind,
				Object: &etcd3.Event{
					Type:      0,
					Key:       "",
					Value:     []byte("aaa"),
					PrevValue: nil,
					Rev:       0,
				},
			}
			handler := &LicenseHandler{}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "error")
		})
		convey.Convey("case4: failed to unmarshal data", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("aaa"), nil
				}),
			})
			defer patches.ResetAll()

			event := eventsource.Event{
				SourceType: "",
				EventType:  eventsource.Added,
				Kind:       utils.HCSLicenseEventKind,
				Object: &etcd3.Event{
					Type:      0,
					Key:       "",
					Value:     []byte("aaa"),
					PrevValue: nil,
					Rev:       0,
				},
			}
			handler := &LicenseHandler{}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(strings.Contains(err.Error(), "invalid character"), convey.ShouldBeTrue)
		})
		convey.Convey("case5: success", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Decrypt, func(src string) ([]byte, error) {
					return []byte("{}"), nil
				}),
				gomonkey.ApplyFunc(handleLicenseEvent, func(event eventsource.Event, licenseMetaData config.HCSLicenseMetaData) error {
					return nil
				}),
			})
			defer patches.ResetAll()

			event := eventsource.Event{
				SourceType: "",
				EventType:  eventsource.Added,
				Kind:       utils.HCSLicenseEventKind,
				Object: &etcd3.Event{
					Type:      0,
					Key:       "",
					Value:     []byte("aaa"),
					PrevValue: nil,
					Rev:       0,
				},
			}
			handler := &LicenseHandler{}
			err := handler.HandleEvent(event)
			convey.So(err, convey.ShouldBeNil)
		})
	})
}

func Test_handleLicenseEvent(t *testing.T) {
	convey.Convey("test handleLicenseEvent", t, func() {
		convey.Convey("case1: add success", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(reportCPUUsageRegularly, func(stopCh <-chan struct{}) {}),
			})
			defer patches.ResetAll()

			event := eventsource.Event{
				EventType: eventsource.Added,
			}
			data := config.HCSLicenseMetaData{
				CPU: 1,
			}
			err := handleLicenseEvent(event, data)
			convey.So(err, convey.ShouldBeNil)
			convey.So(config.CPUQuota, convey.ShouldEqual, data.CPU)
		})
		convey.Convey("case2: update success", func() {
			event := eventsource.Event{
				EventType: eventsource.Updated,
			}
			data := config.HCSLicenseMetaData{
				CPU: 2,
			}
			err := handleLicenseEvent(event, data)
			convey.So(err, convey.ShouldBeNil)
			convey.So(config.CPUQuota, convey.ShouldEqual, data.CPU)
		})
		convey.Convey("case3: delete success", func() {
			event := eventsource.Event{
				EventType: eventsource.Deleted,
			}
			data := config.HCSLicenseMetaData{
				CPU: 1,
			}
			err := handleLicenseEvent(event, data)
			convey.So(err, convey.ShouldBeNil)
			convey.So(config.CPUQuota, convey.ShouldEqual, 0)
		})
	})
}

func Test_reportClusterLicenseUsed(t *testing.T) {
	convey.Convey("test reportClusterLicenseUsed", t, func() {
		convey.Convey("case1: not enable hcs license", func() {
			config.EnableHCSLicense = false
			err := reportClusterLicenseUsed()
			convey.So(err, convey.ShouldBeNil)
		})
		convey.Convey("case2: failed to marshal data", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(json.Marshal, func(v any) ([]byte, error) {
					return nil, errors.New("error")
				}),
			})
			defer patches.ResetAll()

			config.EnableHCSLicense = true
			err := reportClusterLicenseUsed()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "error")
		})
		convey.Convey("case3: failed to encrypt data", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Encrypt, func(src string) (string, error) {
					return "", errors.New("error")
				}),
			})
			defer patches.ResetAll()

			config.EnableHCSLicense = true
			err := reportClusterLicenseUsed()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "error")
		})
		convey.Convey("case4: failed to put data to etcd", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Encrypt, func(src string) (string, error) {
					return "aaa", nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
						return errors.New("error")
					}),
			})
			defer patches.ResetAll()

			config.EnableHCSLicense = true
			err := reportClusterLicenseUsed()
			convey.So(err, convey.ShouldNotBeNil)
			convey.So(err.Error(), convey.ShouldEqual, "error")
		})
		convey.Convey("case5: success", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(localauth.Encrypt, func(src string) (string, error) {
					return "aaa", nil
				}),
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
					func(_ *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, value string, opts ...clientv3.OpOption) error {
						return nil
					}),
			})
			defer patches.ResetAll()

			config.EnableHCSLicense = true
			err := reportClusterLicenseUsed()
			convey.So(err, convey.ShouldBeNil)
		})
	})
}
