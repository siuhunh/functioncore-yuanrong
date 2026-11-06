package utils

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	. "github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
	"gotest.tools/assert"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/redisclient"
	bgtypes "functioncore/pkg/common/types/bridgetypes"
	stateutils "functioncore/pkg/state-manager/utils"
	mockUtils "functioncore/test/utils"
)

func TestBridgeDeleteStateInRedis(t *testing.T) {
	mockUtils.FakeRedisInit()
	Convey("Test delete state in redis", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(redisclient.GetRedisCmd, func() *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "HDel", func(_ *redisclient.Client, ctx context.Context, key string, fields ...string) *redis.IntCmd {
				return &redis.IntCmd{}
			}),
		})
		Convey("case1 HDel no error", func() {
			patch := gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					return 1, nil
				})
			changeCount, err := DeleteStateInRedis("", "", "", "")
			So(changeCount, ShouldEqual, 1)
			So(err, ShouldBeNil)
			patch.Reset()
		})
		Convey("case1 HDel error", func() {
			patch1 := gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result",
				func(_ *redis.IntCmd) (int64, error) {
					return 0, errors.New("failed to get result")
				})
			patch2 := gomonkey.ApplyFunc(time.Sleep, func(d time.Duration) {
				return
			})
			changeCount, err := DeleteStateInRedis("", "", "", "")
			So(changeCount, ShouldEqual, 0)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "failed to get result")
			patch1.Reset()
			patch2.Reset()
		})
		patches.ResetAll()
	})
}

func Test_IsClientInsFuncUseInfoEmpty(t *testing.T) {
	clientInstance := &bgtypes.ClientInstance{}
	result := IsClientInsFuncUseInfoEmpty(clientInstance)
	assert.Equal(t, result, true)
}

func Test_CleanStateInRedis(t *testing.T) {
	Convey("Test CleanStateInRedis, all error", t, func() {
		patch := gomonkey.ApplyFunc(delOptInRedis, func(key, stateID string) (int, error) {
			return 0, errors.New("error")
		})
		defer patch.Reset()
		CleanStateInRedis("tenantID", "serviceID", "functionName", "stateID")
	})

	Convey("Test CleanStateInRedis, all clear", t, func() {
		patch := gomonkey.ApplyFunc(delOptInRedis, func(key, stateID string) (int, error) {
			return 1, nil
		})
		defer patch.Reset()
		CleanStateInRedis("tenantID", "serviceID", "functionName", "stateID")
	})
}

func Test_delOptInRedis(t *testing.T) {
	Convey("Test delOptInRedis, client is nil", t, func() {
		patch := gomonkey.ApplyFunc(stateutils.GetRedisCmd, func() *redisclient.Client {
			return nil
		})
		defer patch.Reset()
		if _, err := delOptInRedis("key", "stateID"); err == nil {
			t.Errorf("this is error")
		}
	})

	Convey("Test delOptInRedis, Result error", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(stateutils.GetRedisCmd, func() *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del", func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.IntCmd {
				return &redis.IntCmd{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result", func(_ *redis.IntCmd) (int64, error) {
				return 0, errors.New("error")
			}),
		})

		defer patches.ResetAll()
		if _, err := delOptInRedis("key", "stateID"); err == nil {
			t.Errorf("this is error")
		}
	})

	Convey("Test delOptInRedis, all clear", t, func() {
		patches := mockUtils.InitPatchSlice()
		patches.Append(mockUtils.PatchSlice{
			gomonkey.ApplyFunc(stateutils.GetRedisCmd, func() *redisclient.Client {
				return &redisclient.Client{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redisclient.Client{}), "Del", func(_ *redisclient.Client, ctx context.Context, key ...string) *redis.IntCmd {
				return &redis.IntCmd{}
			}),
			gomonkey.ApplyMethod(reflect.TypeOf(&redis.IntCmd{}), "Result", func(_ *redis.IntCmd) (int64, error) {
				return 1, nil
			}),
		})

		defer patches.ResetAll()
		if _, err := delOptInRedis("key", "stateID"); err != nil {
			t.Errorf("this is error")
		}
	})
}

func TestGetValueFromMetaEtcd(t *testing.T) {
	Convey("TestGetValueFromMetaEtcd", t, func() {
		Convey("client is nil", func() {
			patch := gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{}
			})
			resp, err := GetValueFromMetaEtcd("testkey")
			So(resp, ShouldBeNil)
			So(err, ShouldNotBeNil)
			So(strings.Contains(err.Error(), "failed to init meta ETCD client"), ShouldBeTrue)
			patch.Reset()
		})
		Convey("query etcd timeout", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues,
					func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						return nil, context.DeadlineExceeded
					}),
			})
			defer patches.ResetAll()
			resp, err := GetValueFromMetaEtcd("testkey")
			So(resp, ShouldBeNil)
			So(err, ShouldNotBeNil)
		})
		Convey("query etcd succeed", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
					return &etcd3.EtcdClient{Client: &clientv3.Client{}}
				}),
				gomonkey.ApplyFunc((*etcd3.EtcdClient).GetValues,
					func(e *etcd3.EtcdClient, ctxInfo etcd3.EtcdCtxInfo, key string, opts ...clientv3.OpOption) ([]string, error) {
						return []string{"value"}, nil
					}),
			})
			defer patches.ResetAll()
			resp, err := GetValueFromMetaEtcd("testkey")
			So(resp, ShouldNotBeNil)
			So(string(resp), ShouldEqual, "value")
			So(err, ShouldBeNil)
		})
	})

}

func TestParseWorkerManagerClusterInfoFromKey(t *testing.T) {
	Convey("Test parse worker-manager cluster info", t, func() {
		Convey("Case1: succeed", func() {
			key := "/sn/worker-manager/instances/cluster001/1.2.3.4/worker-manager-xxx"
			clusterID, nodeIP, err := ParseWorkerManagerClusterInfoFromKey(key)
			So(err, ShouldBeNil)
			So(clusterID, ShouldEqual, "cluster001")
			So(nodeIP, ShouldEqual, "1.2.3.4")
		})
		Convey("Case2: failed, key is illegal", func() {
			key := "error"
			_, _, err := ParseWorkerManagerClusterInfoFromKey(key)
			So(err, ShouldNotBeNil)
			ShouldBeTrue(strings.Contains(err.Error(), "illegal"))
		})
		Convey("Case3: failed, clusterID is empty", func() {
			key := "/sn/worker-manager/instances//1.2.3.4/worker-manager-xxx"
			_, _, err := ParseWorkerManagerClusterInfoFromKey(key)
			So(err, ShouldNotBeNil)
			ShouldBeTrue(strings.Contains(err.Error(), "clusterID is empty"))
		})
		Convey("Case4: failed, nodeIP is empty", func() {
			key := "/sn/worker-manager/instances/cluster001//worker-manager-xxx"
			_, _, err := ParseWorkerManagerClusterInfoFromKey(key)
			So(err, ShouldNotBeNil)
			ShouldBeTrue(strings.Contains(err.Error(), "nodeIP is empty"))
		})
	})
}

func TestGetCheckInstancesInterval(t *testing.T) {
	Convey("Test check instance interval", t, func() {
		Convey("Case1: succeed to get value from env", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					if key == checkInstancesIntervalKey {
						return "10"
					}
					return ""
				}),
			})
			defer patches.ResetAll()
			interval := GetCheckInstancesInterval()
			So(interval, ShouldEqual, 10)
		})
		Convey("Case1: env value is empty", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					return ""
				}),
			})
			defer patches.ResetAll()
			interval := GetCheckInstancesInterval()
			So(interval, ShouldEqual, defaultCheckInstancesInterval)
		})
		Convey("Case1: env value is invalid", func() {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{
				gomonkey.ApplyFunc(os.Getenv, func(key string) string {
					if key == checkInstancesIntervalKey {
						return "error"
					}
					return ""
				}),
			})
			defer patches.ResetAll()
			interval := GetCheckInstancesInterval()
			So(interval, ShouldEqual, defaultCheckInstancesInterval)
		})
	})
}
