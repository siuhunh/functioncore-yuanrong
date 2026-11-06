package etcdevents

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	commonutils "functioncore/pkg/common/utils"
	mockUtils "functioncore/test/utils"
)

func TestNewProvisioner(t *testing.T) {
	convey.Convey("New Provisioner test", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			// 5*time.Millisecond and 1000*time.Second define baseDelay and maxDelay
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
			// Limiter that allows events up to rate 200 and permits, bursts of at most 2000 tokens.
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(200), 2000)},
		))
		patches := gomonkey.ApplyFunc(etcd3.GetETCDClient,
			func(_ etcd3.EtcdType) *etcd3.EtcdClient { return &etcd3.EtcdClient{} })
		defer patches.Reset()
		imp := NewProvisioner(etcd3.Meta, "CronTrigger", "/sn/triggers/triggerType/CRON/business/", queue)
		convey.So(imp, convey.ShouldNotBeNil)
	})
}

func TestProcessWatchEvent(t *testing.T) {
	convey.Convey("etcd Process Watch Event", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(20), 30)},
		))
		imp := Provisioner{
			Namespace:  " ",
			SinkQueue:  queue,
			SourceType: etcd3.Meta,
			Kind:       "InstanceMetadata",
		}
		convey.Convey("is  function event", func() {
			event := &etcd3.Event{
				Type:  etcd3.ERROR,
				Key:   "/sn/functions/busi/<busID>/tenant/<tena>/function/<functionName>/version/<vn>",
				Value: []byte(" "),
			}
			imp.processEvent(event)
			convey.So(imp.SinkQueue.Len(), convey.ShouldEqual, 0)
			event = &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   "/sn/functions/busi/<busID>/tenant/<tena>/function/<functionName>/version/<vn>",
				Value: []byte(" "),
			}
			imp.processEvent(event)
			convey.So(imp.SinkQueue.Len(), convey.ShouldEqual, 1)
			event = &etcd3.Event{
				Type:  etcd3.DELETE,
				Key:   "/sn/functions/busi/<busID>/tenant/<tena>/function/<functionName>/version/<vn>",
				Value: []byte(" "),
			}
			imp.processEvent(event)
			convey.So(imp.SinkQueue.Len(), convey.ShouldEqual, 2)
		})
	})
}

func TestGenerateEvents(t *testing.T) {
	convey.Convey("test generate events", t, func() {
		imp := Provisioner{
			Namespace:  " ",
			SinkQueue:  nil,
			SourceType: etcd3.Meta,
			Kind:       "InstanceMetadata",
		}
		event := &etcd3.Event{
			Key:   "1/2/3/4/5/6/7/8/9/0",
			Value: []byte(" "),
		}
		eventItem := imp.generateEvents(event.Key, "Added", event)
		convey.So(eventItem, convey.ShouldNotBeNil)

	})
}

func TestEventKey(t *testing.T) {
	convey.Convey("etcd event key test", t, func() {
		imp := Provisioner{
			Namespace:  " ",
			SinkQueue:  nil,
			SourceType: etcd3.Meta,
			Kind:       "InstanceMetadata",
		}
		convey.Convey("is key FunctionRepo", func() {
			key := "/sn/functions/business/<businessID>/tenant/<tenantID>/function/<functionName>/version/<version>"
			convey.So(imp.isFunctionRepo(key), convey.ShouldBeTrue)
			key = "/sn/functions/business/<businessID>/tenant/<tenantID>/function/<functionName>"
			convey.So(imp.isFunctionRepo(key), convey.ShouldBeFalse)
			key = "/sn/func/business/<businessID>/tenant/<tenantID>/11/<functionName>/version/<version>"
			convey.So(imp.isFunctionRepo(key), convey.ShouldBeFalse)
		})
		convey.Convey("is key NativeInstance", func() {
			patch := gomonkey.ApplyFunc(commonutils.AzEnv, func() string { return "functest" })
			defer patch.Reset()
			key := "/sn/workers/business/iot/tenant/j0f441085/function/functest/version/functest/functest/functest"
			convey.So(imp.isNativeInstance(key), convey.ShouldBeTrue)
			key = " /sn/workers/business/iot/tenant/j0f4413f7b4b4c33be576d432f7ee085/function"
			convey.So(imp.isNativeInstance(key), convey.ShouldBeFalse)
			key = " /sn/workers/business/iot/tenant/j0f4413f7b4b4c33be576d432f7ee085/function/functest/111/$latest"
			convey.So(imp.isNativeInstance(key), convey.ShouldBeFalse)
			key = " /sn/workers/business/iot/tenant/j0f4413f7b4b4c33be576d432f7ee085/function/functest/111/$latest/1/2"
			convey.So(imp.isNativeInstance(key), convey.ShouldBeFalse)
		})
		convey.Convey("is key InstanceMetadata", func() {
			key := "sn/triggers/triggerType/CRON/business/yrk/tenant/function/function/version/version/12"
			convey.So(imp.isInstanceMetadata(key), convey.ShouldBeTrue)
			key = "sn/triggers/triggerType/CRON/business/yrk/tenant/function"
			convey.So(imp.isInstanceMetadata(key), convey.ShouldBeFalse)
			key = "sn/triggers/triggerType/CRON/business/yrk/tenant/8/9/version/11/12"
			convey.So(imp.isInstanceMetadata(key), convey.ShouldBeFalse)
		})
		convey.Convey("is key prediction", func() {
			key := "/prediction/cluster/clusterID/"
			convey.So(imp.isInstancePrediction(key), convey.ShouldBeTrue)
			key = "/prediction/cluster/clusterID"
			convey.So(imp.isInstancePrediction(key), convey.ShouldBeFalse)
			key = "/prediction/cxx/clusterID"
			convey.So(imp.isInstancePrediction(key), convey.ShouldBeFalse)
		})
	})
}

func TestProvisioner_Start(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		Namespace  string
		SinkQueue  workqueue.RateLimitingInterface
		SourceType etcd3.EtcdType
		Kind       string
	}
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 watchChan nil", fields{}, args{make(chan struct{}, 1)}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetETCDClient,
						func(_ etcd3.EtcdType) *etcd3.EtcdClient {
							return &etcd3.EtcdClient{Client: &clientv3.Client{}}
						}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch,
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return nil
						}),
				})
				return patches
			}},
		{"case2 succeed tp start", fields{}, args{make(chan struct{}, 1)}, false,
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetETCDClient,
						func(_ etcd3.EtcdType) *etcd3.EtcdClient {
							return &etcd3.EtcdClient{Client: &clientv3.Client{}}
						}),
					gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch,
						func(_ *etcd3.EtcdClient, ctx context.Context,
							watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
							return &etcd3.EtcdWatchChan{}
						}),
					gomonkey.ApplyFunc((*Provisioner).StartWatch,
						func(_ *Provisioner, stop <-chan struct{}) {
							return
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			a := &Provisioner{
				Namespace:  tt.fields.Namespace,
				SinkQueue:  tt.fields.SinkQueue,
				SourceType: tt.fields.SourceType,
				Kind:       tt.fields.Kind,
			}
			if err := a.Start(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
			time.Sleep(10 * time.Millisecond)
			patches.ResetAll()
		})
	}
}

func TestProvisioner_StartWatch(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		Namespace  string
		SinkQueue  workqueue.RateLimitingInterface
		SourceType etcd3.EtcdType
		Kind       string
	}
	type args struct {
		watchChan *etcd3.EtcdWatchChan
		stop      chan struct{}
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 stop nil", fields{},
			args{&etcd3.EtcdWatchChan{}, nil}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			}},
		{"case2 stop chan close", fields{},
			args{&etcd3.EtcdWatchChan{}, make(chan struct{}, 1)}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			}},
		{"case3 stop chan received", fields{},
			args{&etcd3.EtcdWatchChan{}, make(chan struct{}, 1)}, func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc(etcd3.GetETCDClient, func(_ etcd3.EtcdType) *etcd3.EtcdClient {
						return &etcd3.EtcdClient{Client: &clientv3.Client{}}
					}),
				})
				return patches
			}},
	}
	i := 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			a := &Provisioner{
				Namespace:  tt.fields.Namespace,
				SinkQueue:  tt.fields.SinkQueue,
				SourceType: tt.fields.SourceType,
				Kind:       tt.fields.Kind,
			}
			if i == 1 {
				close(tt.args.stop)
			} else if i == 2 {
				tt.args.stop <- struct{}{}
			}
			a.StartWatch(tt.args.stop)
			patches.ResetAll()
			i += 1
		})
	}
}

func TestProvisioner_processWatchEvent(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		Namespace  string
		SinkQueue  workqueue.RateLimitingInterface
		SourceType etcd3.EtcdType
		Kind       string
	}
	type args struct {
		event *etcd3.Event
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1 isFunction, processFunctionEvent error, succeed to processEvent", fields{}, args{
			&etcd3.Event{Key: "/sn/functions/business/<businessID>/tenant/<tenantID>/function/<functionName>/version/<version>",
				Type: 5}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				return patches
			}},
		{"case2 isFunction, processFunctionEvent error, processEvent error", fields{}, args{
			&etcd3.Event{Key: "/sn/functions/business/<businessID>/tenant/<tenantID>/function/<functionName>/version/<version>"}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*Provisioner).processEvent,
						func(_ *Provisioner, event *etcd3.Event) {
							return
						}),
				})
				return patches
			}},
		{"case2 skipped the event", fields{}, args{
			&etcd3.Event{Key: "test"}},
			func() mockUtils.PatchSlice {
				patches := mockUtils.InitPatchSlice()
				patches.Append(mockUtils.PatchSlice{
					gomonkey.ApplyFunc((*Provisioner).processEvent,
						func(_ *Provisioner, event *etcd3.Event) {
							return
						}),
				})
				return patches
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			a := &Provisioner{
				Namespace:  tt.fields.Namespace,
				SinkQueue:  tt.fields.SinkQueue,
				SourceType: tt.fields.SourceType,
				Kind:       tt.fields.Kind,
			}
			a.processWatchEvent(tt.args.event)
			patches.ResetAll()
		})
	}
}

func TestProvisioner_isTenantMetadata(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		SinkQueue  workqueue.RateLimitingInterface
	}
	type args struct {
		etcdRef string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "case_01 invalid tenant metadata key",
			fields: fields{},
			args: args{
				etcdRef: "/sn/q/cluster/<clusterID>/tenant/<tenantID>/instancemetadata",
			},
			want: false,
		}, {
			name:   "case_02 normal tenant metadata key",
			fields: fields{},
			args: args{
				etcdRef: "/sn/quota/cluster/<clusterID>/tenant/<tenantID>/instancemetadata",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Provisioner{
				SinkQueue: tt.fields.SinkQueue,
			}
			if got := a.isTenantMetadata(tt.args.etcdRef); got != tt.want {
				t.Errorf("isTenantMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProvisioner_isDefaultTenantMetadata(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		SinkQueue  workqueue.RateLimitingInterface
	}
	type args struct {
		etcdRef string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name:   "case_01 invalid default tenant metadata key",
			fields: fields{},
			args: args{
				etcdRef: "/sn/q/cluster/<clusterID>/default/instancemetadata",
			},
			want: false,
		}, {
			name:   "case_02 normal default tenant metadata key",
			fields: fields{},
			args: args{
				etcdRef: "/sn/quota/cluster/<clusterID>/default/instancemetadata",
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Provisioner{
				SinkQueue: tt.fields.SinkQueue,
			}
			if got := a.isDefaultTenantMetadata(tt.args.etcdRef); got != tt.want {
				t.Errorf("isDefaultTenantMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProvisioner_isClusterInfo(t *testing.T) {
	a := Provisioner{}
	assert.Equal(t, a.isClusterInfo("fss_cluster_flow_state"), true)
	assert.Equal(t, a.isClusterInfo("a"), false)
}

func TestProvisioner_isStateRouter(t *testing.T) {
	type fields struct {
		Namespace  string
		SinkQueue  workqueue.RateLimitingInterface
		SourceType etcd3.EtcdType
		Kind       string
		lastRev    int64
	}
	type args struct {
		key string
	}
	var f fields
	var a args
	a.key = "/sn/stateworkers/business/yrk/tenant/0/function/function-task/version/aaa"
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{"test1", f, a, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &Provisioner{
				Namespace:  tt.fields.Namespace,
				SinkQueue:  tt.fields.SinkQueue,
				SourceType: tt.fields.SourceType,
				Kind:       tt.fields.Kind,
				lastRev:    tt.fields.lastRev,
			}
			assert.Equalf(t, tt.want, a.isStateRouter(tt.args.key), "isStateRouter(%v)", tt.args.key)
		})
	}
}
