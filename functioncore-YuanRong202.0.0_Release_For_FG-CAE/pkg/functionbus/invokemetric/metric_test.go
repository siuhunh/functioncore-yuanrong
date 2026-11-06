package invokemetric

import (
	"context"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/smartystreets/goconvey/convey"

	"functioncore/pkg/common/redisclient"
)

type redisCmd struct {
	m map[string][]Metric
	redis.Client
}

func (cmd *redisCmd) ZAdd(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	for _, v := range members {
		cmd.m[key] = append(cmd.m[key], v.Member.(Metric))
	}
	return &redis.IntCmd{}
}

func TestReportInvokeResultMetric(t *testing.T) {
	convey.Convey("TestReportInvokeResultMetric", t, func() {
		convey.Convey("succeed to record metric", func() {
			saveInternal = 10 * time.Millisecond
			var cmd = &redisCmd{m: map[string][]Metric{}}
			stopCh := make(chan struct{})
			c := &redisclient.Client{}
			c.SetClient(cmd)
			ReportInvokeResultMetric(c, stopCh)
			SaveMetric("namespace/0@default@funcName/latest", 10, 4021)
			SaveMetric("namespace/0@default@funcName/latest", 5, 0)
			time.Sleep(100 * time.Millisecond)
			close(stopCh)
			met := cmd.m[fssMetricKey][0]
			convey.So(met.Count, convey.ShouldEqual, 2)
			convey.So(met.FunctionErrorCount, convey.ShouldEqual, 1)
			convey.So(met.MaxDuration, convey.ShouldEqual, 10)
			convey.So(met.MinDuration, convey.ShouldEqual, 5)
			convey.So(met.Duration, convey.ShouldEqual, 15)
		})
		convey.Convey("failed to record metric with invalid key", func() {
			saveInternal = 10 * time.Millisecond
			var cmd = &redisCmd{m: map[string][]Metric{}}
			stopCh := make(chan struct{})
			c := &redisclient.Client{}
			c.SetClient(cmd)
			ReportInvokeResultMetric(c, stopCh)
			SaveMetric("namespace/default@funcName/latest", 10, 4021)
			time.Sleep(30 * time.Millisecond)
			close(stopCh)
			convey.So(len(cmd.m), convey.ShouldEqual, 0)
		})
	})
}
