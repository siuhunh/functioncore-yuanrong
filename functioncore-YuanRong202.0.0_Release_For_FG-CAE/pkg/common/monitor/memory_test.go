package monitor

import (
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/agiledragon/gomonkey/v2"
	"github.com/smartystreets/goconvey/convey"
	"github.com/stretchr/testify/assert"

	"functioncore/pkg/common/types"
)

func TestInitMemMonitor(t *testing.T) {
	convey.Convey("TestInitMemMonitor", t, func() {
		convey.Convey("success", func() {
			patches := [...]*Patches{
				ApplyFunc(os.Getenv, func(key string) string {
					return "1G"
				}),
				ApplyMethod(reflect.TypeOf(new(Parser)), "Read", func(_ *Parser) (interface{}, error) {
					return uint64(100), nil
				}),
			}
			defer func() {
				for idx := range patches {
					patches[idx].Reset()
				}
			}()
			err := InitMemMonitor()
			assert.Nil(t, err)
			assert.Equal(t, uint64(0x0), memory.monitor.used)

			time.Sleep(2 * time.Second)
			assert.NotEqual(t, uint64(0x0), memory.monitor.used)
			if curMemUsageReachThreshold != 0 {
				t.Error("this is error")
			}
		})
	})

}

func TestMemMonitor_Allow(t *testing.T) {
	memMonitor := &memMonitor{enable: true, threshold: 1024, used: 10}
	result := memMonitor.Allow(1000)
	assert.Equal(t, true, result)
	result = memMonitor.Allow(15)
	assert.Equal(t, false, result)
}

func TestMemMonitor_AllowWithCondition(t *testing.T) {
	memMonitor1 := &memMonitor{enable: true, threshold: 1024, lowerThreshold: 500, used: 520}
	result := memMonitor1.AllowWithCondition(func() bool {
		return true
	})
	assert.Equal(t, true, result)
	memMonitor2 := &memMonitor{enable: true, threshold: 1024, lowerThreshold: 500, used: 1110}
	result = memMonitor2.AllowWithCondition(func() bool {
		return true
	})
	assert.Equal(t, false, result)
	memMonitor3 := &memMonitor{enable: true, threshold: 1024, lowerThreshold: 500, used: 10}
	result = memMonitor3.AllowWithCondition(func() bool {
		return true
	})
	assert.Equal(t, true, result)
}

func TestAllowByLowerThreshold(t *testing.T) {
	memMonitor := &memMonitor{
		enable:         true,
		threshold:      200000,
		lowerThreshold: 140000,
		memMapMutex:    sync.Mutex{},
		functionMemMap: map[string]uint64{},
	}

	allow := memMonitor.Allow(100000)
	assert.Equal(t, true, allow)
	allow = memMonitor.AllowByLowerThreshold("1", "1", 100000)
	assert.Equal(t, true, allow)

	allow = memMonitor.Allow(20000)
	assert.Equal(t, true, allow)
	allow = memMonitor.AllowByLowerThreshold("2", "2", 20000)
	assert.Equal(t, true, allow)

	allow = memMonitor.Allow(30000)
	assert.Equal(t, true, allow)
	allow = memMonitor.AllowByLowerThreshold("1", "1", 30000)
	assert.Equal(t, false, allow)

	memMonitor.ReleaseFunctionMem("1", 100000)
	memMonitor.used = memMonitor.used - 100000

	allow = memMonitor.Allow(100000)
	assert.Equal(t, true, allow)
	allow = memMonitor.AllowByLowerThreshold("1", "1", 100000)
	assert.Equal(t, true, allow)
}

func TestAllowByStatefulFuncInvoke(t *testing.T) {
	memMonitor := &memMonitor{
		enable:            true,
		threshold:         200000,
		statefulThreshold: 250000,
		lowerThreshold:    140000,
		memMapMutex:       sync.Mutex{},
		functionMemMap:    map[string]uint64{},
		isStateful:        true,
	}

	allow := memMonitor.Allow(210000)
	assert.Equal(t, true, allow)
	memMonitor.used = 0

	allow = memMonitor.Allow(260000)
	assert.Equal(t, false, allow)

	allow = memMonitor.Allow(150000)
	assert.Equal(t, true, allow)

	allow = memMonitor.Allow(200001)
	assert.Equal(t, false, allow)

	memMonitor.resetMemStateful()
	allow = memMonitor.Allow(50000)
	assert.Equal(t, true, allow)

	allow = memMonitor.AllowByLowerThreshold("1", "1", 50000)
	assert.Equal(t, true, allow)

	allow = memMonitor.AllowByLowerThreshold("1", "1", 50000)
	assert.Equal(t, false, allow)
}

func TestSetMemoryControlConfig(t *testing.T) {
	convey.Convey("TestSetMemoryControlConfig", t, func() {
		convey.Convey("nil config", func() {
			SetMemoryControlConfig(nil)
		})
		convey.Convey("SetMemoryControlConfig", func() {
			cfg := &types.MemoryControlConfig{
				LowerMemoryPercent:     0.5,
				BodyThreshold:          1024,
				MemDetectIntervalMs:    3,
				HighMemoryPercent:      0.5,
				StatefulHighMemPercent: 0.9,
			}
			SetMemoryControlConfig(cfg)
			convey.So(*config == *cfg, convey.ShouldEqual, true)
		})
	})
}

func Test_parseValue(t *testing.T) {
	convey.Convey("parseValue", t, func() {
		v, err := parseValue("100", 10, 64)
		convey.So(v, convey.ShouldEqual, 100)
		convey.So(err, convey.ShouldBeNil)
		v, err = parseValue("-100", 10, 64)
		convey.So(v, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldBeNil)
		v, err = parseValue("1.01", 10, 64)
		convey.So(v, convey.ShouldEqual, 0)
		convey.So(err, convey.ShouldNotBeNil)
	})
}

func TestIsAllowByMemory(t *testing.T) {
	convey.Convey("TestIsAllowByMemory", t, func() {
		memory.monitor = &memMonitor{
			enable:         true,
			threshold:      200000,
			lowerThreshold: 140000,
			memMapMutex:    sync.Mutex{},
			functionMemMap: map[string]uint64{},
		}
		memory.monitor.decreaseMemCnt(100)

		allow := IsAllowByMemory("1", 200001, "")
		convey.So(allow, convey.ShouldBeFalse)

		allow = IsAllowByMemory("1", 100000, "")
		convey.So(allow, convey.ShouldBeTrue)

		allow = IsAllowByMemory("1", 50000, "")
		convey.So(allow, convey.ShouldBeFalse)
	})
}

func TestDistinguishMemAllowByStateful(t *testing.T) {
	convey.Convey("TestDistinguishMemAllowByStateful", t, func() {
		memory.monitor = &memMonitor{
			enable:            true,
			threshold:         200000,
			lowerThreshold:    140000,
			statefulThreshold: 260000,
			memMapMutex:       sync.Mutex{},
			functionMemMap:    map[string]uint64{},
		}
		memory.monitor.decreaseMemCnt(100)

		allow := DistinguishMemAllowByStateful("1", 200001, "", true)
		convey.So(allow, convey.ShouldBeTrue)
		GetMemInstance().ReleaseFunctionMem("1", 200001)

		allow = DistinguishMemAllowByStateful("2", 200001, "", false)
		convey.So(allow, convey.ShouldBeFalse)

		allow = DistinguishMemAllowByStateful("1", 50000, "", true)
		convey.So(allow, convey.ShouldBeTrue)
		GetMemInstance().ReleaseFunctionMem("1", 200000)
		memory.monitor.used = 50001

		allow = DistinguishMemAllowByStateful("2", 50000, "", false)
		convey.So(allow, convey.ShouldBeTrue)

		allow = DistinguishMemAllowByStateful("1", 50000, "", true)
		convey.So(allow, convey.ShouldBeTrue)
		GetMemInstance().ReleaseFunctionMem("1", 50000)
		memory.monitor.used = 100001

		allow = DistinguishMemAllowByStateful("2", 50000, "", false)
		convey.So(allow, convey.ShouldBeFalse)
	})
}

func TestStatefulFunctionReqMemSeparator(t *testing.T) {
	convey.Convey("TestDistinguishMemAllowByStateful", t, func() {
		memory.monitor = &memMonitor{
			enable:            true,
			threshold:         200000,
			lowerThreshold:    140000,
			statefulThreshold: 260000,
			memMapMutex:       sync.Mutex{},
			functionMemMap:    map[string]uint64{},
		}
		memory.monitor.decreaseMemCnt(100)

		memory.monitor.used = 0

		allow := StatefulFunctionReqMemSeparator(false, 200001)
		convey.So(allow, convey.ShouldBeFalse)

		allow = StatefulFunctionReqMemSeparator(true, 200001)
		convey.So(allow, convey.ShouldBeTrue)

		allow = StatefulFunctionReqMemSeparator(true, 50000)
		convey.So(allow, convey.ShouldBeTrue)
		memory.monitor.used = 50001

		allow = StatefulFunctionReqMemSeparator(false, 50000)
		convey.So(allow, convey.ShouldBeTrue)

		allow = StatefulFunctionReqMemSeparator(true, 50000)
		convey.So(allow, convey.ShouldBeTrue)
		memory.monitor.used = 100001

		allow = StatefulFunctionReqMemSeparator(false, 50000)
		convey.So(allow, convey.ShouldBeTrue)
	})
}

func TestReachMemoryThreshold(t *testing.T) {
	if ReachMemoryThreshold() {
		t.Error("this should false")
	}
	atomic.StoreUint32(&curMemUsageReachThreshold, 1)
	defer func() { atomic.StoreUint32(&curMemUsageReachThreshold, 0) }()
	if !ReachMemoryThreshold() {
		t.Error("this should true")
	}
}
