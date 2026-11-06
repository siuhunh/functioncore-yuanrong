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
package server

import (
	"functioncore/pkg/cache-manager/config"
	"functioncore/pkg/common/types"
	"github.com/smartystreets/goconvey/convey"
	"testing"
)

func Test_objSizeCache_persistObj(t *testing.T) {
	convey.Convey("persistObj test", t, func() {
		convey.Convey("Update_persistObj", func() {
			GetObjSizeCacheInstance().evsEnable = true
			defer func() {
				GetObjSizeCacheInstance().evsEnable = true
			}()
			key1 := "test1"
			GetObjSizeCacheInstance().UpdatePersistObj(key1, FileInfo{100, "test1", "test1"})
			key2 := "test2"
			GetObjSizeCacheInstance().UpdatePersistObj(key2, FileInfo{200, "test2", "test2"})
			convey.So(len(GetObjSizeCacheInstance().persistObj), convey.ShouldEqual, 2)
			convey.So(GetObjSizeCacheInstance().persistObj[key1].ObjSize, convey.ShouldEqual, 100)
			convey.So(GetObjSizeCacheInstance().persistObj[key2].ObjSize, convey.ShouldEqual, 200)
			convey.So(GetObjSizeCacheInstance().persistObj[key1].Name, convey.ShouldEqual, "test1")
			convey.So(GetObjSizeCacheInstance().persistObj[key2].Name, convey.ShouldEqual, "test2")
			GetObjSizeCacheInstance().UpdatePersistObj(key1, FileInfo{100, "test11", "test11"})
			convey.So(GetObjSizeCacheInstance().persistObj[key1].Name, convey.ShouldEqual, "test11")

			GetObjSizeCacheInstance().UpdatePersistObjSize(key1, 400)
			GetObjSizeCacheInstance().UpdatePersistObjSize(key2, 500)
			key3 := "test3"
			GetObjSizeCacheInstance().UpdatePersistObjSize(key3, 300)
			convey.So(GetObjSizeCacheInstance().persistObj[key1].ObjSize, convey.ShouldEqual, 400)
			convey.So(GetObjSizeCacheInstance().persistObj[key2].ObjSize, convey.ShouldEqual, 500)
			convey.So(GetObjSizeCacheInstance().persistObj[key3].ObjSize, convey.ShouldEqual, 300)
			key4 := "test4"
			GetObjSizeCacheInstance().UpdateObjPersistName(key3, "test33")
			GetObjSizeCacheInstance().UpdateObjPersistName(key4, "test44")
			convey.So(GetObjSizeCacheInstance().persistObj[key3].Name, convey.ShouldEqual, "test33")
			convey.So(len(GetObjSizeCacheInstance().persistObj), convey.ShouldEqual, 3)

			GetObjSizeCacheInstance().UpdateObjSizeAndCheckSum(key3, 333, "test333")
			GetObjSizeCacheInstance().UpdateObjSizeAndCheckSum(key4, 444, "test444")
			convey.So(GetObjSizeCacheInstance().persistObj[key3].ObjSize, convey.ShouldEqual, 333)
			convey.So(GetObjSizeCacheInstance().persistObj[key4].ObjSize, convey.ShouldEqual, 444)
			convey.So(GetObjSizeCacheInstance().persistObj[key3].Checksum, convey.ShouldEqual, "test333")
			convey.So(GetObjSizeCacheInstance().persistObj[key4].Checksum, convey.ShouldEqual, "test444")
			GetObjSizeCacheInstance().DelPersistObj(key4)
			convey.So(len(GetObjSizeCacheInstance().persistObj), convey.ShouldEqual, 3)
			convey.So(GetObjSizeCacheInstance().CheckObjInMemThreshold("test5"), convey.ShouldBeFalse)
			GetObjSizeCacheInstance().UpdateObjSizeAndCheckSum(key3, 100*1024+1, "test333")
			convey.So(GetObjSizeCacheInstance().CheckObjInMemThreshold(key3), convey.ShouldBeFalse)
			convey.So(GetObjSizeCacheInstance().CheckObjInMemThreshold(key2), convey.ShouldBeTrue)
			metaInfo := &config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					FunctionVersionURN: "test1@default",
					CodeSha512:         "test11",
					CodeSize:           400,
					Layers: []*types.Layer{
						{
							ObjectID: "objectid-layer1",
							Name:     "layer1",
							ETag:     "1",
						},
						{
							ObjectID: "objectid-layer2",
							Name:     "layer2",
							ETag:     "2",
						},
						{
							ObjectID: "objectid-layer3",
							Name:     "layer3",
							ETag:     "3",
						},
					},
				},
			}
			inDisk := GetObjSizeCacheInstance().IsObjInDisk(metaInfo, "test11", key1, false)
			convey.So(inDisk, convey.ShouldBeTrue)
			inDisk = GetObjSizeCacheInstance().IsObjInDisk(metaInfo, "test11", "key6", false)
			convey.So(inDisk, convey.ShouldBeFalse)
			GetObjSizeCacheInstance().UpdateObjSizeAndCheckSum("layer1", 100, "test11")
			GetObjSizeCacheInstance().UpdateObjPersistName("layer1", "layer1")
			inDisk = GetObjSizeCacheInstance().IsObjInDisk(metaInfo, "test11", "layer1", true)
			convey.So(inDisk, convey.ShouldBeTrue)
			inDisk = GetObjSizeCacheInstance().IsObjInDisk(metaInfo, "test12", "layer1", false)
			convey.So(inDisk, convey.ShouldBeFalse)
		})
	})
}

func Test_objSizeCache_layerCache(t *testing.T) {
	convey.Convey("layerCache test", t, func() {
		convey.Convey("Update_layerCache", func() {
			GetObjSizeCacheInstance().evsEnable = true
			defer func() {
				GetObjSizeCacheInstance().evsEnable = true
			}()
			metaInfo := &config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					FunctionVersionURN: "test1@default",
					Layers: []*types.Layer{
						{
							ObjectID: "objectid-layer1",
							Name:     "layer1",
							ETag:     "1",
						},
						{
							ObjectID: "objectid-layer2",
							Name:     "layer2",
							ETag:     "2",
						},
						{
							ObjectID: "objectid-layer3",
							Name:     "layer3",
							ETag:     "3",
						},
					},
				},
			}
			deletedLayers := GetObjSizeCacheInstance().GetNeedDeletedLayers(metaInfo)
			convey.So(len(deletedLayers), convey.ShouldEqual, 0)
			metaInfo2 := &config.FunctionMetaInfo{
				FuncMetaData: config.FuncMetaData{
					FunctionVersionURN: "test1@default",
					Layers: []*types.Layer{
						{
							ObjectID: "objectid-layer3",
							Name:     "layer3",
							ETag:     "33",
						},
						{
							ObjectID: "objectid-layer4",
							Name:     "layer4",
							ETag:     "44",
						},
					},
				},
			}
			deletedLayers2 := GetObjSizeCacheInstance().GetNeedDeletedLayers(metaInfo2)
			convey.So(len(deletedLayers2), convey.ShouldEqual, 3)
			convey.So(deletedLayers2["objectid-layer1"], convey.ShouldEqual, "1")
			convey.So(deletedLayers2["objectid-layer2"], convey.ShouldEqual, "2")
			convey.So(deletedLayers2["objectid-layer3"], convey.ShouldEqual, "3")

			inDisk := GetObjSizeCacheInstance().IsObjInDisk(metaInfo, "1", "2", false)
			convey.So(inDisk, convey.ShouldBeFalse)
			inDisk = GetObjSizeCacheInstance().IsObjInDisk(metaInfo, "1", "2", true)
			convey.So(inDisk, convey.ShouldBeFalse)
			convey.So(len(GetObjSizeCacheInstance().layerCache), convey.ShouldEqual, 1)
			GetObjSizeCacheInstance().DeleteLayerCache(metaInfo)
			convey.So(len(GetObjSizeCacheInstance().layerCache), convey.ShouldEqual, 0)
		})
	})
}
