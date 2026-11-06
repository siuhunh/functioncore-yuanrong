package invokemetric

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/types"
	"functioncore/pkg/functionbus/proxyerrors"
)

type redisCmd struct {
	m map[string][]Metric
	redis.Client
}

func (cmd *redisCmd) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	for _, v := range members {
		cmd.m[key] = append(cmd.m[key], v.Member.(Metric))
	}
	return &redis.IntCmd{}
}

func TestReportInvokeResultMetric(t *testing.T) {
	convey.Convey("TestReportInvokeResultMetric", t, func() {
		convey.Convey("succeed to record metric", func() {
			saveInternal = 10 * time.Millisecond
			concurrencyStatisticsWindow = 5 * time.Millisecond
			var cmd = &redisCmd{m: map[string][]Metric{}}
			stopCh := make(chan struct{})
			c := &redisclient.Client{}
			c.SetClient(cmd)
			ReportInvokeResultMetric(c, stopCh)
			response1 := &types.TCPResponse{
				ErrorCode:       proxyerrors.TrafficLimitEffective,
				ErrorMessage:    proxyerrors.InternalErr,
				MetricsErrSaved: true,
				Summary:         "{\"memoryUsed\":31.14,\"podName\":\"calleePod\",\"nodeIP\":\"calleeNodeIP\",\"clusterID\":\"calleeClusterID\"}",
			}
			response2 := &types.TCPResponse{
				ErrorCode:       0,
				ErrorMessage:    "",
				MetricsErrSaved: true,
				Summary:         "{\"memoryUsed\":60.88,\"podName\":\"calleePod\",\"nodeIP\":\"calleeNodeIP\",\"clusterID\":\"calleeClusterID\"}",
			}
			SaveMetric("namespace/0@default@funcName/latest", 10, response1)
			SaveMetric("namespace/0@default@funcName/latest", 5, response2)
			SaveRejectCount("namespace/0@default@funcName/latest", false)
			SaveRejectCount("namespace/0@default@funcName/latest", false)
			time.Sleep(100 * time.Millisecond)
			close(stopCh)
			met := cmd.m[fssMetricKey][0]
			convey.So(met.Count, convey.ShouldEqual, 2)
			convey.So(met.FunctionErrorCount, convey.ShouldEqual, 1)
			convey.So(met.MaxDuration, convey.ShouldEqual, 10)
			convey.So(met.MinDuration, convey.ShouldEqual, 5)
			convey.So(met.Duration, convey.ShouldEqual, 15)
			convey.So(met.MaxMemory, convey.ShouldEqual, 60.88)
			convey.So(met.MinMemory, convey.ShouldEqual, 31.14)
			convey.So(met.MemoryUsed, convey.ShouldEqual, 31.14+60.88)
			convey.So(met.RejectCount, convey.ShouldEqual, 2)
		})
		convey.Convey("failed to record metric with invalid key", func() {
			saveInternal = 10 * time.Millisecond
			concurrencyStatisticsWindow = 5 * time.Millisecond
			var cmd = &redisCmd{m: map[string][]Metric{}}
			stopCh := make(chan struct{})
			c := &redisclient.Client{}
			c.SetClient(cmd)
			ReportInvokeResultMetric(c, stopCh)
			response1 := &types.TCPResponse{
				ErrorCode:       proxyerrors.TrafficLimitEffective,
				ErrorMessage:    proxyerrors.InternalErr,
				MetricsErrSaved: true,
			}
			SaveMetric("namespace/default@funcName/latest", 10, response1)
			time.Sleep(30 * time.Millisecond)
			close(stopCh)
			convey.So(len(cmd.m), convey.ShouldEqual, 0)
		})
	})
}

func TestUpdateConcurrencySaveWindows(t *testing.T) {
	stopCh := make(chan struct{})
	saveInternal = 1000 * time.Millisecond
	concurrencyStatisticsWindow = 100 * time.Millisecond
	go updateConcurrencySaveWindows(stopCh)
	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, concurrencySaveWindows >= 4, true)
	close(stopCh)
}
