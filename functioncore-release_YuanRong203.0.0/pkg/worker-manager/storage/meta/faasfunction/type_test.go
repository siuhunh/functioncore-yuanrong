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

package faasfunction

import (
	commontypes "functioncore/pkg/common/types"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

// TestFunctionService_Type Test Function Service_Type
func TestFunctionService_Type(t *testing.T) {
	var service FunctionService
	ftype, err := service.Type()

	if ftype != "functionservice" || err != nil {
		t.Errorf("Expected type is functionservice, return is %s", ftype)
	}
}

// TestFunctionService_AppendEnvVariables Test Function Service_Append Env Variables
func TestFunctionService_AppendEnvVariables(t *testing.T) {
	var service FunctionService
	env := make(map[string]string)
	env["key1"] = "value1"
	env["key2"] = "value2"

	for key, value := range env {
		service.AppendEnvVariables(key, value)
	}

	// Test basic input is ok
	for _, c := range service.Spec.Configuration.Env {
		value, ok := env[c.Name]
		if ok != true {
			t.Fatalf("Env '%s' is not in Service", c.Name)
		} else if value != c.Value {
			t.Fatalf("Failed to set env '%s' value to '%s'", c.Name, value)
		}
	}
}

// TestFunctionService_Update test Update
func TestFunctionService_Update(t *testing.T) {
	convey.Convey("test all Update", t, func() {
		f := FunctionService{}
		f.UpdateBucketID("bucketID_test")
		f.UpdateObjectID("objectID_test")
		f.UpdateAppID("appID_test")
		f.UpdateBucketURL("bucketURL_test")
		data, _ := f.Data()
		convey.So(data, convey.ShouldNotBeNil)
		convey.So(f.Spec.Code.Location.Nsp.BucketID, convey.ShouldEqual, "bucketID_test")
		convey.So(f.Spec.Code.Location.Nsp.ObjectID, convey.ShouldEqual, "objectID_test")
		convey.So(f.Spec.Code.Location.Nsp.AppID, convey.ShouldEqual, "appID_test")
		convey.So(f.Spec.Code.Location.Nsp.BucketURL, convey.ShouldEqual, "bucketURL_test")
	})
	convey.Convey("test all add layer", t, func() {
		f := FunctionService{
			Spec: FunctionServiceSpec{
				Configuration: ConfigurationSpec{
					Layers: []*commontypes.Layer{},
				},
			},
		}
		layer := &commontypes.Layer{
			AppID: "123",
		}
		f.AppendLayer(layer)
		convey.So(f.Spec.Configuration.Layers, convey.ShouldNotBeNil)
	})
}
