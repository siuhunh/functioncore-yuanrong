package nodeevents

import (
	"testing"
	"time"

	"github.com/smartystreets/goconvey/convey"
	"golang.org/x/time/rate"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	mockUtils "functioncore/test/utils"
)

func TestNewProvisioner(t *testing.T) {
	convey.Convey("New provisioner test", t, func() {
		imp := NewProvisioner(" ", " ", nil,
			nil)
		convey.So(imp, convey.ShouldNotBeNil)
	})
}

func TestGenerateEvents(t *testing.T) {
	convey.Convey(" test generate events", t, func() {
		{
			imp := Provisioner{
				SinkQueue:    nil,
				NodeInformer: nil,
				SourceType:   "string",
				Kind:         "string",
			}
			obj := 'a'
			convey.So(imp.generateEvents("a", "b", obj), convey.ShouldNotBeNil)
		}
	})
}

func TestAddUpdateDeleteFunc(t *testing.T) {
	convey.Convey("TestAddUpdateDeleteFuc", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter())
		provisioner := &Provisioner{
			SinkQueue: queue,
		}
		provisioner.addFunc(cache.ExplicitKey("1"))
		convey.So(provisioner.SinkQueue.Len(), convey.ShouldEqual, 1)
		provisioner.updateFunc(cache.ExplicitKey("1"), cache.ExplicitKey("2"))
		convey.So(provisioner.SinkQueue.Len(), convey.ShouldEqual, 2)
		provisioner.deleteFunc(cache.ExplicitKey("2"))
		convey.So(provisioner.SinkQueue.Len(), convey.ShouldEqual, 3)
	})
}

func TestProvisioner_Start(t *testing.T) {
	queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		// 5*time.Millisecond and 1000*time.Second define baseDelay and maxDelay
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		// Limiter that allows events up to rate 200 and permits, bursts of at most 2000 tokens.
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(200), 2000)},
	))
	nodeInformer := mockUtils.InformerFakeFactory.Core().V1().Nodes()
	type args struct {
		stopCh <-chan struct{}
	}
	tests := []struct {
		name        string
		p           *Provisioner
		args        args
		wantErr     bool
		patchesFunc mockUtils.PatchesFunc
	}{
		{"case1", NewProvisioner("string", "string", queue, nodeInformer), args{}, false, func() mockUtils.PatchSlice {
			patches := mockUtils.InitPatchSlice()
			patches.Append(mockUtils.PatchSlice{})
			return patches
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			patches := tt.patchesFunc()

			if err := tt.p.Start(tt.args.stopCh); (err != nil) != tt.wantErr {
				t.Errorf("Start() error = %v, wantErr %v", err, tt.wantErr)
			}
			patches.ResetAll()
		})
	}
}
