[TOC]

# Worker在init阶段支持函数互调

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述
为支撑 ServiceBridge 需求场景，需要在 worker init 阶段支持函数互调。

### 1.2特性场景分析
![1681890078117](C:\Users\Z30024~1\AppData\Local\Temp\1681890078117.png)

当前 worker init 阶段无法进行函数互调的原因：

1. 以前bus与worker通信是由worker起服务端，bus纳管worker时与之建链。
2. 但worker初始化阶段尚未被bus纳管，因此无法与bus通信。


## 2. 特性/功能实现原理

### 2.1总体方案
![1681890307264](C:\Users\Z30024~1\AppData\Local\Temp\1681890307264.png)

**bus模块**

1. bus 初始化阶段新起一个 server，监听 pod ip+32569端口(新增端口)

2. 通过pod ip映射到节点ip

3. 对请求的处理仍然复用worker的处理逻辑

**worker模块**

1. worker init阶段收到函数互调请求则向自己所在节点ip + 32569端口建链
2. 将请求发往bus

### 2.2模块详细设计

#### 2.2.1 bus 子模块设计

##### 2.2.1.1模块实现
``` golang
type BusWorkerServer struct {
	conf           *common.BusWorkerServerConfig
	requestHandler internal.HandleReq
}

// InitServer initializes a new server connected to busProxy
func InitServer(requestHandler internal.HandleReq) (*BusWorkerServer, error) {
	server := &BusWorkerServer{
		conf: common.FConfig.BusWorkerServerConfig,
	}
	serverOpt := tcpbus.ServerOption{
		Receiver:   server,
		ListenAddr: internal.SelfPodIP + ":" + constants.BusWorkerServerTCPort,
	}
	server.requestHandler = requestHandler

	if err := startBusServer(server.conf, serverOpt); err != nil {
		log.GetLogger().Errorf("failed to start bus server, error: %s", err.Error())
		return nil, err
	}
	return server, nil
}

func (b *BusWorkerServer) HandleRequest(payload []byte, clientIP string) types.Result{}

func (b *BusWorkerServer) RegisterService(client *tcpbus.Connection) error {}
```

#### 2.2.2 worker 子模块设计

##### 2.2.2.1模块实现
``` goalng
{
	if s.bus == nil && status.Get() == status.Initializing {
		busRsp, err = s.processInitCall(ctx, target, req, false)
	} else {
		busRsp, err = s.bus.Call(ctx, target, req, false)
	}
}

// process initialize stage call for TCPBus
func (s *Server) processInitCall(ctx context.Context, dst *buscommon.FunctionProperty,
	req *types.TCPRequest, isSendingRemoteProxy bool) (*types.TCPResponse, error) {
	config.GetConfig()
	traceID := busutils.ContextValueString(ctx, constants.HeaderTraceID)
	log.GetLogger().Infof("called the function by bus in initialize stage, traceID: %s", traceID)
	if s.initBus != nil {
		return s.initBus.Call(ctx, dst, req, isSendingRemoteProxy)
	}
	configuration, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	serverAddr := fmt.Sprintf("%s:%d", os.Getenv("HOST_IP"), busServerPort)
	opt := tcpbus.ClientOption{
		ServerAddr: serverAddr,
	}
	if configuration.HTTPSConfig.HTTPSEnable {
		opt.TLSEnable = true
		opt.Config = tls.GetClientTLSConfig()
	}
	client, err := tcpbus.BuildClientConnection(opt)
	if err != nil {
		return nil, err
	}
	tcpBusOpt := &tcpbus.Option{CreateClient: false, Client: client}
	tcpBus, err := tcpbus.NewTCPBus(buscommon.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		return nil, err
	}
	s.initBus = tcpBus
	return s.initBus.Call(ctx, dst, req, isSendingRemoteProxy)
}
```

### 2.3 约束

#### 2.3.1上下游依赖需求

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

	bus起服务端监听32569端口，worker开发接收32569端口。由init阶段互调则多一条通信链路。

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

1. 本需求只保证java函数init阶段互调正常
2. 当前需求worker与bus建链后未断开
3. init阶段互调成功后，若runtime进程被kill，worker此时已不在init阶段，会走之前的调用逻辑，存在问题。

以上问题均在下个AR进行可靠性完善。


### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

### 3.12升级兼容性

以前功能均不受影响。

## 4. 用例

| **序号** | **用例名** | 用例类型 | **测试步骤** | **预期结果** |
| -------- | ---------- | -------- | ------------ | ------------ |
| 1 | 拉起Bus server | LLT | 拉起Bus server | server能正常提供服务 |
| 2 | worker与服务端建链 | LLT | worker与服务端建链 | worker能正常与server交互 |
| 3 | java有状态函数init阶段支持函数互调 | 冒烟、全量用例 | java函数init阶段进行函数互调 | 调用成功 |
| 3 | java无状态函数init阶段支持函数互调 | 冒烟、全量用例 | java函数init阶段进行函数互调 | 调用成功 |