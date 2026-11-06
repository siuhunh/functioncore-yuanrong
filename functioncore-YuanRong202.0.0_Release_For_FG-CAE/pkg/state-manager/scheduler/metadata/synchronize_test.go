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
package metadata

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
)

func TestInitSync(t *testing.T) {
	convey.Convey("InitSync", t, func() {
		convey.Convey("test 1", func() {
			patches := [2]*gomonkey.Patches{
				gomonkey.ApplyFunc(get,
					func(_ string) (map[string]map[string]struct{}, error) {
						return map[string]map[string]struct{}{}, nil
					}),
				gomonkey.ApplyFunc(put,
					func(_ string, _ map[string]map[string]struct{}) error {
						return nil
					}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitSync(nil)
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func Test_get(t *testing.T) {
	convey.Convey("get", t, func() {
		convey.Convey("test 1", func() {
			patches := [3]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse",
					func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, _ string,
						_ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						res := &clientv3.GetResponse{
							Kvs: []*mvccpb.KeyValue{},
						}
						res.Kvs = append(res.Kvs, &mvccpb.KeyValue{
							Key:   []byte("hello"),
							Value: []byte("world"),
						})
						return res, nil
					}),
				gomonkey.ApplyFunc(json.Unmarshal,
					func(data []byte, v interface{}) error {
						return nil
					}),
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := get("keyPath")
			convey.So(err, convey.ShouldEqual, nil)
		})

		convey.Convey("test 3", func() {
			patches := [2]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "GetResponse",
					func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, _ string,
						_ ...clientv3.OpOption) (*clientv3.GetResponse, error) {
						res := &clientv3.GetResponse{
							Kvs: []*mvccpb.KeyValue{},
						}
						res.Kvs = append(res.Kvs, &mvccpb.KeyValue{
							Key:   []byte("hello"),
							Value: []byte("world"),
						})
						return res, nil
					}),
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := get("keyPath")
			convey.So(err, convey.ShouldNotEqual, nil)
		})
		// state client is nil
		convey.Convey("test 4", func() {
			patches := [1]*gomonkey.Patches{
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			_, err := get("keyPath")
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}

func Test_put(t *testing.T) {
	convey.Convey("put", t, func() {
		convey.Convey("test 1", func() {
			patches := [3]*gomonkey.Patches{
				gomonkey.ApplyMethod(reflect.TypeOf(&etcd3.EtcdClient{}), "Put",
					func(_ *etcd3.EtcdClient, _ etcd3.EtcdCtxInfo, _, _ string, _ ...clientv3.OpOption) error {
						return nil
					}),
				gomonkey.ApplyFunc(json.Marshal,
					func(v interface{}) ([]byte, error) {
						return nil, nil
					}),
				gomonkey.ApplyFunc(etcd3.GetStateEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := put("keyPath", map[string]map[string]struct{}{})
			convey.So(err, convey.ShouldEqual, nil)
		})
	})
}
