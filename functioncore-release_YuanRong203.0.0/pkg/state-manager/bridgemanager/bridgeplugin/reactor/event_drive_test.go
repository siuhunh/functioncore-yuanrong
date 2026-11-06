package reactor

import (
	"testing"
	"time"

	"github.com/agiledragon/gomonkey/v2"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"

	"functioncore/pkg/common/etcd3"
	evs "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/eventsource"
	evh "functioncore/pkg/state-manager/bridgemanager/bridgeplugin/reactor/eventhandler"
	"functioncore/test/utils"
)

var (
	queue = workqueue.NewRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(baseDelay*time.Millisecond, maxDelay*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(rateLimit), tokensBurst)},
	))
	reactor = &Reactor{
		EventSources:    map[string]evs.EventProvisioner{},
		queue:           queue,
		EventHandlerMap: map[string]evh.Handler{},
		APIClients: &APIClients{
			MetadataEtcdClient:  &etcd3.EtcdClient{},
			RoutedataEtcdClient: &etcd3.EtcdClient{},
			CAEMetaEtcdClient:   &etcd3.EtcdClient{},
		},
	}
	rspChan = make(chan int, 1)
)

type adapter struct{}

func (*adapter) Start(_ <-chan struct{}) error {
	event := evs.Event{
		Kind: "BridgeInstance",
	}
	queue.Add(event)
	return nil
}

type handler struct{}

func (*handler) HandleEvent(_ evs.Event) error {
	rspChan <- 1
	return nil
}

type errHandler struct{}

func (*errHandler) HandleEvent(_ evs.Event) error {
	return errors.New("handle failed")
}

func TestRun(t *testing.T) {
	Convey("TestRun", t, func() {
		stopCh := make(chan struct{}, 1)
		reactor.registerEventHandler(&handler{}, "BridgeInstance")
		reactor.registerEventSource(&adapter{}, "BridgeInstance")
		setWorkerNum(0)
		_ = reactor.Run(stopCh)
		reactor.processNextItem()
		rsp := <-rspChan
		So(rsp, ShouldEqual, 1)
		close(stopCh)
		close(rspChan)
	})
}

func TestNewEventDrive(t *testing.T) {
	Convey("Test new event driver", t, func() {
		patch := gomonkey.ApplyFunc(makeReactor, func(*APIClients) *Reactor {
			return &Reactor{}
		})
		reactor := NewEventDrive()
		So(reactor, ShouldNotBeNil)
		patch.Reset()
	})
}

func TestReactor_makeFunctionRepoReactor(t *testing.T) {
	Convey("Test make function repo reactor", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
		})
		reactor.makeFunctionRepoReactor()
		So(reactor.EventHandlerMap["FunctionRepo"], ShouldNotBeNil)
		patches.ResetAll()
	})
}

func TestReactor_makeClientInstanceReactor(t *testing.T) {
	Convey("Test make client instance reactor", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
		})
		reactor.makeClientInstanceReactor()
		So(reactor.EventHandlerMap["ClientInstance"], ShouldNotBeNil)
		patches.ResetAll()
	})
}

func TestReactor_makeBridgeInstanceReactor(t *testing.T) {
	Convey("Test make bridge instance reactor", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
		})
		reactor.makeBridgeInstanceReactor()
		So(reactor.EventHandlerMap["BridgeInstance"], ShouldNotBeNil)
		patches.ResetAll()
	})
}

func Test_makeReactor(t *testing.T) {
	Convey("Test make bridge instance reactor", t, func() {
		patches := utils.InitPatchSlice()
		patches.Append(utils.PatchSlice{
			gomonkey.ApplyFunc(etcd3.GetRouterEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
			gomonkey.ApplyFunc(etcd3.GetMetaEtcdClient, func() *etcd3.EtcdClient {
				return &etcd3.EtcdClient{Client: &clientv3.Client{}}
			}),
		})
		apiClients := &APIClients{
			MetadataEtcdClient:  &etcd3.EtcdClient{},
			RoutedataEtcdClient: &etcd3.EtcdClient{},
		}
		reactor := makeReactor(apiClients)
		So(reactor.EventHandlerMap["FunctionRepo"], ShouldNotBeNil)
		So(reactor.EventHandlerMap["ClientInstance"], ShouldNotBeNil)
		So(reactor.EventHandlerMap["BridgeInstance"], ShouldNotBeNil)
		patches.ResetAll()
	})
}

func TestReactor_processItem(t *testing.T) {
	Convey("Test reactor processItem with error", t, func() {
		Convey("Case1 unknown event type", func() {
			event := evs.Event{
				Kind:      "Not exist",
				EventType: "",
				KeyRef:    "",
			}
			err := reactor.processItem(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "unKnowKind event")
		})
		Convey("Case2 handler error", func() {
			event := evs.Event{
				Kind:      "handleError",
				EventType: "test",
				KeyRef:    "test",
			}
			reactor.registerEventHandler(&errHandler{}, "handleError")
			err := reactor.processItem(event)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "handle failed")
		})
	})
}
