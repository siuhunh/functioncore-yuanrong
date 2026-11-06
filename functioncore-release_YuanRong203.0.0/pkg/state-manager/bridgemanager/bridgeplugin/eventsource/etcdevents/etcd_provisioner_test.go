package etcdevents

import (
	"context"
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	. "github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	"functioncore/test/utils"
)

func TestNewProvisioner(t *testing.T) {
	Convey("Test new provisioner", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(200), 2000)},
		))
		Convey("Test new provisioner with meta client", func() {
			patch := gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
				func() *etcd3.EtcdClient { return &etcd3.EtcdClient{} })
			defer patch.Reset()
			imp := NewProvisioner("Etcd", "FunctionRepo", "/sn/functions/business", queue)
			So(imp, ShouldNotBeNil)
		})
		Convey("Test new provisioner with route client", func() {
			patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
				func() *etcd3.EtcdClient { return &etcd3.EtcdClient{} })
			defer patch.Reset()
			imp := NewProvisioner("Etcd", "ClientInstance", "/sn/workers/business", queue)
			So(imp, ShouldNotBeNil)
		})
		Convey("Test new provisioner with worker-manager instance", func() {
			patch := gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient,
				func() *etcd3.EtcdClient { return &etcd3.EtcdClient{} })
			defer patch.Reset()
			imp := NewProvisioner("Etcd", "WorkerManagerInstance", "/sn/worker-manager/instances", queue)
			So(imp, ShouldNotBeNil)
		})
	})
}

func TestProvisioner_Start(t *testing.T) {
	Convey("Test provisioner start", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(200), 2000)},
		))
		patch := gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient,
			func() *etcd3.EtcdClient { return &etcd3.EtcdClient{Client: &clientv3.Client{}} })
		provisioner := NewProvisioner("Etcd", "FunctionRepo", "/sn/functions/business", queue)
		defer patch.Reset()
		Convey("case1 watchChan nil", func() {
			patch1 := gomonkey.ApplyFunc((*etcd3.EtcdClient).Watch, func(_ *etcd3.EtcdClient, ctx context.Context,
				watchPath string, rev int64, recursive bool) *etcd3.EtcdWatchChan {
				return nil
			})
			err := provisioner.Start(make(chan struct{}, 1))
			So(err, ShouldBeNil)
			patch1.Reset()
		})
		Convey("case2 succeed starting", func() {
			patches := utils.InitPatchSlice()
			patches.Append(utils.PatchSlice{
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
			err := provisioner.Start(make(chan struct{}, 1))
			So(err, ShouldBeNil)
			patches.ResetAll()
		})
	})
}

func TestProvisioner_StartWatch(t *testing.T) {
	type fields struct {
		EtcdClient *etcd3.EtcdClient
		Path       string
		SinkQueue  workqueue.RateLimitingInterface
		SourceType string
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
		patchesFunc utils.PatchesFunc
	}{
		{"case1 stop nil", fields{},
			args{&etcd3.EtcdWatchChan{}, nil}, func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{})
				return patches
			}},
		{"case2 stop chan close", fields{},
			args{&etcd3.EtcdWatchChan{}, make(chan struct{}, 1)}, func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{})
				return patches
			}},
		{"case3 stop chan received", fields{},
			args{&etcd3.EtcdWatchChan{}, make(chan struct{}, 1)}, func() utils.PatchSlice {
				patches := utils.InitPatchSlice()
				patches.Append(utils.PatchSlice{})
				return patches
			}},
	}
	i := 0
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()
			a := &Provisioner{
				EtcdClient: &etcd3.EtcdClient{Client: &clientv3.Client{}},
				Path:       tt.fields.Path,
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

func TestProvisioner_generateEvents(t *testing.T) {
	Convey("test generate events", t, func() {
		imp := Provisioner{
			Path:       "/sn/functions/business",
			SinkQueue:  nil,
			EtcdClient: nil,
			SourceType: "FunctionRepo",
			Kind:       "Etcd",
		}
		event := &etcd3.Event{
			Key:   "1/2/3/4/5/6/7/8/9/0",
			Value: []byte(" "),
		}
		eventItem := imp.generateEvents(event.Key, "Added", event)
		So(eventItem, ShouldNotBeNil)
	})
}

func TestProvisioner_isNeedWatchPath(t *testing.T) {
	Convey("Test judge isNeedWatchPath", t, func() {
		imp := Provisioner{
			Path:       "",
			SinkQueue:  nil,
			EtcdClient: nil,
			SourceType: "FunctionRepo",
			Kind:       "Etcd",
		}
		Convey("Case1 match function meta successfully", func() {
			path := "/sn/functions/business"
			result := imp.isNeedWatchPath(path)
			So(result, ShouldBeTrue)
		})
		Convey("Case2 match instance meta successfully", func() {
			path := "/sn/workers/business"
			result := imp.isNeedWatchPath(path)
			So(result, ShouldBeTrue)
		})
		Convey("Case3 match bridgemanager cache successfully", func() {
			path := "/sn/bridgemanager/business"
			result := imp.isNeedWatchPath(path)
			So(result, ShouldBeTrue)
		})
		Convey("Case4 match function or instance meta failed", func() {
			path := "otherPath"
			result := imp.isNeedWatchPath(path)
			So(result, ShouldBeFalse)
		})
		Convey("Case5 match worker-manager instance successfully", func() {
			path := "/sn/worker-manager/instances"
			result := imp.isNeedWatchPath(path)
			So(result, ShouldBeTrue)
		})
	})
}

func TestProvisioner_processWatchEvent(t *testing.T) {
	Convey("Test process watch event", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(20), 30)},
		))
		imp := Provisioner{
			Path:       "",
			SinkQueue:  queue,
			EtcdClient: nil,
			SourceType: "FunctionRepo",
			Kind:       "Etcd",
		}
		Convey("Case1 not function or instance event", func() {
			event := &etcd3.Event{
				Type:  etcd3.ERROR,
				Key:   "/sn/xxxx",
				Value: []byte(" "),
			}
			imp.processEvent(event)
			So(imp.SinkQueue.Len(), ShouldEqual, 0)
		})
		Convey("Case2 is function or instance event", func() {
			event := &etcd3.Event{
				Type:  etcd3.ERROR,
				Key:   "/sn/functions/busi/<busID>/tenant/<tena>/function/<functionName>/version/<vn>",
				Value: []byte(" "),
			}
			imp.processEvent(event)
			So(imp.SinkQueue.Len(), ShouldEqual, 0)
			event = &etcd3.Event{
				Type:  etcd3.PUT,
				Key:   "/sn/functions/business/<busID>/tenant/<tena>/function/<functionName>/version/<vn>",
				Value: []byte(" "),
			}
			imp.processWatchEvent(event)
			So(imp.SinkQueue.Len(), ShouldEqual, 1)
			event = &etcd3.Event{
				Type:  etcd3.DELETE,
				Key:   "/sn/functions/busi/<busID>/tenant/<tena>/function/<functionName>/version/<vn>",
				Value: []byte(" "),
			}
			imp.processEvent(event)
			So(imp.SinkQueue.Len(), ShouldEqual, 2)
		})
	})
}
