package podevents

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"golang.org/x/time/rate"
	"gotest.tools/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/worker-manager/eventsource"
	"functioncore/pkg/worker-manager/utils"
)

func TestNewProvisioner(t *testing.T) {
	convey.Convey("New Provisioner test", t, func() {
		patch := gomonkey.ApplyFunc(utils.FunctionNameSpace, func() string { return " " })
		defer patch.Reset()
		imp := NewProvisioner(" ", " ", nil,
			nil)
		convey.So(imp, convey.ShouldNotBeNil)
	})
}

func TestGenerateEvents(t *testing.T) {
	convey.Convey(" test Generate Events", t, func() {
		{
			imp := Provisioner{
				SinkQueue:   nil,
				PodInformer: nil,
				SourceType:  "string",
				Kind:        "string",
			}
			obj := 'a'
			convey.So(imp.generateEvents("a", "b", obj), convey.ShouldNotBeNil)
		}
	})
}

func TestGenerateResourceEventHandler(t *testing.T) {
	queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 20)},
	))
	imp := Provisioner{
		SinkQueue:   queue,
		PodInformer: nil,
		SourceType:  "string",
		Kind:        "string",
	}
	handler := imp.generateResourceEventHandler()

	handler.OnAdd(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-fusion-xxx"}})
	assert.Equal(t, 1, queue.Len())
	item, _ := queue.Get()
	assert.Equal(t, eventsource.Added, item.(eventsource.Event).EventType)
	queue.Done(item)

	handler.OnUpdate(nil, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pool22-300-128-fusion-xxx"}})
	assert.Equal(t, 1, queue.Len())
	item, _ = queue.Get()
	assert.Equal(t, eventsource.Updated, item.(eventsource.Event).EventType)
	queue.Done(item)

	convey.Convey(" test resource event handler method", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 20)},
		))
		imp := Provisioner{
			SinkQueue:   queue,
			PodInformer: nil,
			SourceType:  "string",
			Kind:        "string",
		}
		patch := gomonkey.ApplyFunc(cache.MetaNamespaceKeyFunc, func(obj interface{}) (string, error) { return " ", nil })
		defer patch.Reset()
		handler := imp.generateResourceEventHandler()
		handler.AddFunc("anything")
		handler.UpdateFunc("old", "new")
		handler.DeleteFunc("anything")
	})
}

func TestGenerateFilteringResourceEventHandler(t *testing.T) {
	convey.Convey("TestGenerateFilteringResourceEventHandler filter string", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 20)},
		))
		imp := Provisioner{
			SinkQueue:   queue,
			PodInformer: nil,
			SourceType:  "string",
			Kind:        "string",
		}
		patch := gomonkey.ApplyFunc(cache.MetaNamespaceKeyFunc, func(obj interface{}) (string, error) { return " ", nil })
		defer patch.Reset()
		handler := imp.generateFilteringResourceEventHandler()
		res := handler.FilterFunc("anything")
		convey.So(res, convey.ShouldBeFalse)
	})
	convey.Convey("TestGenerateFilteringResourceEventHandler filter IsWorkerPod", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 20)},
		))
		imp := Provisioner{
			SinkQueue:   queue,
			PodInformer: nil,
			SourceType:  "string",
			Kind:        "string",
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.IsWorkerPod, func(pod *corev1.Pod) bool { return true }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		patch := gomonkey.ApplyFunc(cache.MetaNamespaceKeyFunc, func(obj interface{}) (string, error) { return " ", nil })
		defer patch.Reset()
		handler := imp.generateFilteringResourceEventHandler()
		res := handler.FilterFunc(&corev1.Pod{})
		convey.So(res, convey.ShouldBeTrue)
	})
	convey.Convey("TestGenerateFilteringResourceEventHandler filter IsVPAPod", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 20)},
		))
		imp := Provisioner{
			SinkQueue:   queue,
			PodInformer: nil,
			SourceType:  "string",
			Kind:        "string",
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.IsWorkerPod, func(pod *corev1.Pod) bool { return false }),
			gomonkey.ApplyFunc(utils.IsDsWorker, func(pod *corev1.Pod) bool { return true }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		patch := gomonkey.ApplyFunc(cache.MetaNamespaceKeyFunc, func(obj interface{}) (string, error) { return " ", nil })
		defer patch.Reset()
		handler := imp.generateFilteringResourceEventHandler()
		res := handler.FilterFunc(&corev1.Pod{})
		convey.So(res, convey.ShouldBeTrue)
	})
	convey.Convey("TestGenerateFilteringResourceEventHandler filter IsFunctionTask", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 20)},
		))
		imp := Provisioner{
			SinkQueue:   queue,
			PodInformer: nil,
			SourceType:  "string",
			Kind:        "string",
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.IsWorkerPod, func(pod *corev1.Pod) bool { return false }),
			gomonkey.ApplyFunc(utils.IsDsWorker, func(pod *corev1.Pod) bool { return true }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		patch := gomonkey.ApplyFunc(cache.MetaNamespaceKeyFunc, func(obj interface{}) (string, error) { return " ", nil })
		defer patch.Reset()
		handler := imp.generateFilteringResourceEventHandler()
		res := handler.FilterFunc(&corev1.Pod{})
		convey.So(res, convey.ShouldBeTrue)
	})
	convey.Convey("TestGenerateFilteringResourceEventHandler filter default", t, func() {
		queue := workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 10*time.Second),
			&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 20)},
		))
		imp := Provisioner{
			SinkQueue:   queue,
			PodInformer: nil,
			SourceType:  "string",
			Kind:        "string",
		}
		patches := []*gomonkey.Patches{
			gomonkey.ApplyFunc(utils.IsWorkerPod, func(pod *corev1.Pod) bool { return false }),
			gomonkey.ApplyFunc(utils.IsDsWorker, func(pod *corev1.Pod) bool { return false }),
		}
		defer func() {
			for _, p := range patches {
				p.Reset()
			}
		}()
		patch := gomonkey.ApplyFunc(cache.MetaNamespaceKeyFunc, func(obj interface{}) (string, error) { return " ", nil })
		defer patch.Reset()
		handler := imp.generateFilteringResourceEventHandler()
		res := handler.FilterFunc(&corev1.Pod{})
		convey.So(res, convey.ShouldBeFalse)
	})
}
