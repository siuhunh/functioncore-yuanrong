package adaptor

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/functiontask/task/api/invoke"
)

func TestToData(t *testing.T) {
	Convey("TestToData", t, func() {
		data := &invoke.TaskRequestPb{}
		result := ConvertInterToData(data)
		So(result, ShouldNotBeNil)
	})
}
