package bridgefunc

import (
	"testing"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/snerror"
	"functioncore/pkg/common/types/bridgetypes"
	"functioncore/pkg/core-sdk/common"
	"functioncore/pkg/core-sdk/communication"
)

func TestApplyStateKey(t *testing.T) {
	Convey("TestDeleteState", t, func() {
		Convey("failed: stateClient is nil", func() {
			expectedErr := snerror.New(common.BridgeFuncParamErrCode, common.BridgeFuncParamErrMsg)
			_, isNewStateKeyScenario, err := ApplyStateKey(bridgetypes.ClientInstance{}, "123")
			So(err.Error(), ShouldEqual, expectedErr.Error())
			So(isNewStateKeyScenario, ShouldEqual, false)
		})
		Convey("failed: clientInstance is nil", func() {
			expectedErr := snerror.New(common.BridgeFuncParamErrCode, common.BridgeFuncParamErrMsg)
			_, isNewStateKeyScenario, err := ApplyStateKey(bridgetypes.FuncRequestInfo{}.ClientInstance, "123")
			So(err.Error(), ShouldEqual, expectedErr.Error())
			So(isNewStateKeyScenario, ShouldEqual, false)
		})
		Convey("succeed", func() {
			patch := gomonkey.ApplyFunc(communication.ApplyStateKeyByHTTP, func(funcName string, clientInstance bridgetypes.ClientInstance, traceID string) (
				communication.CreateStateResponse, snerror.SNError) {
				return communication.CreateStateResponse{}, nil
			})
			defer patch.Reset()
			_, isNewStateKeyScenario, err := ApplyStateKey(bridgetypes.ClientInstance{PodName: "111", FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID: "111", ServiceID: "0@base", BridgeFuncName: "test123"}}, "123")
			So(err, ShouldEqual, nil)
			So(isNewStateKeyScenario, ShouldEqual, false)
		})
	})
}

func TestReleaseBridge(t *testing.T) {
	Convey("TestGetState", t, func() {
		Convey("failed: stateClient is nil", func() {
			err := snerror.New(common.BridgeFuncParamErrCode, common.BridgeFuncParamErrMsg)
			_, resErr := Release(bridgetypes.ClientInstance{}, "123")
			So(resErr.Error(), ShouldEqual, err.Error())
		})
		Convey("failed: clientInstance is nil", func() {
			err := snerror.New(common.BridgeFuncParamErrCode, common.BridgeFuncParamErrMsg)
			_, resErr := Release(bridgetypes.FuncRequestInfo{}.ClientInstance, "123")
			So(resErr.Error(), ShouldEqual, err.Error())
		})
		Convey("succeed", func() {
			patch := gomonkey.ApplyFunc(communication.ReleaseByHTTP, func(funcName string, clientInstance bridgetypes.ClientInstance, traceID string) (
				communication.InvokeFuncResponse, snerror.SNError) {
				return communication.InvokeFuncResponse{}, nil
			})
			defer patch.Reset()
			_, resErr := Release(bridgetypes.ClientInstance{PodName: "111", FuncInfo: &bridgetypes.BridgeFuncInfo{
				TenantID: "111", ServiceID: "0@base", BridgeFuncName: "test123"}}, "123")
			So(resErr, ShouldEqual, nil)
		})
	})
}
