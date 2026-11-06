# SR.IREQ02793119.008-新增scheduler服务，用于小并发场景下的均衡调度

# 场景

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

需求来源于问题单DTS2022072106307转需求 测试发现在1:1并发场景下，多扩现象会比较严重，11并发请求最终使用到35个实例，存在较大的资源浪费。针对这种场景，需要通过中心化调度方案来解决。

### 1.2特性场景分析

**有worker时的调度**

[有worker存在的调度方案](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=d613ac3669704537ad58509772aa9603)![有worker存在的调度方案](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Nld?t=1659403833693)

1.现有调度方案中，每个frontend独立调度，通过随机算法挑选请求数最小的worker进行调度

2.调度时把请求发到worker所在节点的functiontask上，有functiontask进一步进行负载均衡调度

由于frontent独立调度，在1比1并发的情况下，多个frontend可能将请求同时调度到一个worker上，导致functiontask出现多扩的现象

**无worker时的调度**

[无worker时的调度方案](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=f7656ce096a948aeb0fc6bab62bdc2aa)![无worker时的调度方案](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Nle?t=1659404049643)

1.无worker时，frontend通过一致性hash选择functiontask

2.functiontask触发扩容，扩容成功后将请求发送到扩容出来的worker上执行

## 2. 特性/功能实现原理

### 2.1总体方案

#### 2.1.1 设计目标和原理

##### 2.1.1.1 设计目标

1.降低成本：解决低并发场景下，负载不均，worker实际数量是预期数量的3倍的多扩问题

2.高可靠性：在各类故障场景中，在用户可接受的时间范围内，函数均可正常执行完成

3.高性能：每次调度平均耗时在2ms以内

4.可演进：考虑后续scheduler作为弹性中心的设计

5.可测试：针对scheduler接口可进行场景化测试

##### 2.1.1.2设计原理

设计的基本原理是scheduler统一管理所有worker实例，frontend需要实例时，向scheduler请求实例，使用完成后归还，请求处理过程中，frontend定期向scheduler续租实例



对于单个worker实例，scheduler将1并发度认为是一个thread，多个thread组成一个Processor、一个实例最多分成10个Processor。Processor是scheduler调度的最小单元。例如当函数的并发度为1时，一个Processor就包含一个thread；当函数并发度为1000时，一个Processor就包含100个thread。

```
type Processor struct {
	InstanceID string // 实例ID
	ProcessorID  int // ProcessorID，例如并发度100的实例，分成10个processor，ProcessorID就是0,1,2...9
	ThreadNum    int // processor所持有的线程数，例如并发度为11，每个processor的ThreadNum为2,1,1,1,1,1,1,1,1,1,
	LeaseVersion uint64 // 租约版本，每次租约结束之后，+1，防止processor租借错乱
}
```

#### 2.1.2 functioncore逻辑架构

[FunctionCore逻辑模型-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/8e77cb2bca3148e2a26bcb20970567e3?projectDiagramUUID=f43bd1bfaa1e4f1ebb5d357de61ddcd2&versionUUID=6cec6db8aef24edeb150e26655e9b523)![FunctionCore逻辑模型](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2PLy?t=1659937692834)

新增FaaS-Scheduler服务，（后面简称scheduler）作为worker负载均衡调度中心，Frontend使用SchedulerAPI向scheduler发起worker路由请求，scheduler进行worker负载均衡，向Frontend分配worker实例

#### 2.1.3 功能时序设计

##### 2.1.3.1 scheduler进程启动时序

[scheduler进程启动时序](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=6bb9a9ac7fbc42798227d530477b6d0f)![scheduler进程启动时序](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Pyw?t=1660096023739)

scheduler需要获取函数元数据以及worker路由信息，作为负载均衡调度的依据

##### 2.1.3.2 frontend进程启动时序

[frontend进程启动时序](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=7cd0baea1e6b4dd6947302b29fa0552c)![frontend进程启动时序](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2PzV?t=1660096404571)

frontend需要根据并发度来判断是否是向scheduler请求调度还是直接调度，需要增加元数据etcd监听

##### 2.1.3.3 基本调度时序

frontend有3种调度算法：

**1.本地全量调度**：从所有实例中，选择请求数最小的实例，进行调度，这是frontend的兜底调度算法，和原先的随机调度类似

**2.本地Processor调度**：从租借的Processor中选择一个有空闲的Processor进行调度

**3.向scheduler申请实例调度**：向scheduler发送获取实例请求，scheduler返回Processor，将请求调度到Processor所在的实例上

[向scheduler申请实例调度](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=cd08ca48a3d34a45b4300c88e3a7770b)![向scheduler申请实例调度](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2mAm?t=1665402124941)

1.当函数并发度小于10时，才会走scheduler调度，否则直接采用本地全量调度

2.当函数等待调度的时延大于250ms时，直接走本地全量调度

3.当条件1、2都满足时，frontend先判断自己的缓存中是否有可用的Processor，如果有，随机选择一个进行调度

4.如果没有可用的Processor，向scheduler请求Processor，scheduler随机选择一个空闲的Processor，将Processor返回给frontend

5.frontend将收到Processor响应，将请求调度到Processor所在的实例上

6.当请求结束时，frontend归还Processor，scheduler将Processor租约版本+1

##### **异常场景**

1.frontend向scheduler请求的超时时间是250ms，如果超时，则直接采用本地全量调度兜底

2.scheduler返回错误，则直接采用本地全量调度兜底

3.scheduler返回了frontend 没有监听到的实例，frontend不会使用此实例，直接丢弃，Processor需要等待租约到期回收（后续会优化成直接归还scheduler）

##### 2.1.3.4 实例续租

续租是为了保证Processor在被一个frontend占用时，不会被分配给另一个frontend。以及在frontend故障时，Processor可以自动被回收。目前租约是6s，

1.针对每个函数，scheduler会每隔2s检查函数所有实例的Processor的时间戳，如果时间戳超过了6s，则直接回收Processor，Processor租约版本+1

2.针对每个函数，frontend会每隔2s向scheduler进行所有Processor的续租，如果续租失败，标记Processor不可用

3.frontend每次进行Processor调度前，会检查Processor状态，如果Processor不可用，会删除Processor

#### 2.1.4 调度场景分析

由于scheduler没有扩容能力，worker是由functiontask扩容出来的，scheduler感知到实例时，上面实际上已经是有请求的，但是scheduler认为没有，这种场景会导致多扩

**多扩场景：Functiontask扩容场景**

当scheduler无法返回实例时，frontend采用原有方案进行调度，这些调度没有被scheduler进行管理，导致出现多个调度中心，出现多扩现象

[0-1扩容场景](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=5fcc855c6c604aa6b190efb34a98176b)![0-1扩容场景](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2QKQ?t=1660197015262)

**多扩场景3：frontend实例归还延迟**

scheduler与frontend是通过网络进行连接，网络传输存在延迟，如果frontend归还延迟较高，会出现worker空闲，但是无法被使用的场景

这种情况，网络延迟不可避免，只能尽量优化frontend归还效率以及scheduler回收效率

[frontend归还延迟](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=7b036d9dc1664fc9bfc905d15a87d1bc)![frontend归还延迟](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2QPc?t=1660201217388)

### 2.2 Scheduler详细设计

#### 2.2.1scheduler逻辑架构

[FaasScheduler-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/8e77cb2bca3148e2a26bcb20970567e3?projectDiagramUUID=1f49b21765e248ca8690b1e54c5dc06f&versionUUID=6cec6db8aef24edeb150e26655e9b523)![FaasScheduler](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Q3W?t=1665403504018)

#### 2.2.2Scheduler核心领域设计

scheduler核心领域通过SchedulerAPI接收调度请求、通过FuncMetaAPI接收函数变更消息，通过FuncInsAPI接收实例变更消息

scheduler内部分为4个子模块

FuncMetaManager负责函数元数据信息管理

FuncInsManager负责实例管理

InsScheduler负责Processor的调度以及管理

schedulerManager负责所有函数的InsScheduler的管理

#### 2.2.3scheduler性能设计

1.scheduler采用Lazy方式加载，当函数请求到来时，才创建scheduler。函数删除时删除scheduler。

#### 2.2.4 scheduler可靠性设计

1.scheduler支持告警能力，复用ETCD断连告警

2.scheduler支持k8s健康检查，健康检查失败时重启进程

#### 2.2.5FaaSSchedulerAPI协议定义

```
service SchedulerRPC {
  rpc AcquireInstance (AcquireInstanceReq) returns (AcquireInstanceRsp) {}
  rpc RetainInstance (RetainInstanceReq) returns (RetainInstanceRsp) {}
  rpc ReleaseInstance (ReleaseInstanceReq) returns (ReleaseInstanceRsp) {}
  rpc Heartbeat (HeartbeatReq) returns (HeartbeatRsp) {}
}

message Error {
    uint32 ErrorCode = 1;
    string ErrorMessage = 2;
}

message AcquireInstanceReq {
    string TraceID = 1;
    string FuncKeyWithRes = 2;
}

message AcquireInstanceRsp {
    string InstanceKey = 1;
    uint32 ProcessorID = 2;
    uint32 ThreadNum = 3;
    uint64 LeaseVersion = 4;
    Error Error = 5;
}

message RetainInstanceReq {
    string TraceID = 1;
    string FuncKeyWithRes = 2;
    string InstanceKey = 3;
    uint32 ProcessorID = 4;
    uint64 LeaseVersion = 5;
}

message RetainInstanceRsp {
    Error Error = 1;
}

message ReleaseInstanceReq {
    string TraceID = 1;
    string FuncKeyWithRes = 2;
    string InstanceKey = 3;
    uint32 ProcessorID = 4;
    uint64 LeaseVersion = 5;
}

message ReleaseInstanceRsp {}

message HeartbeatReq {}

message HeartbeatRsp {}
```

#### 2.2.5scheduler路由信息API

scheduler路由信息注册到路由etcd，key:/sn/faas-scheduler/instances/cluster001/${NODE_IP}

value:

{

   "podname":

​    "pod_ip": "XXX",
​    "port": "XXX",
​    "nodeIP": "XXX",

}

#### 2.2.6scheduler安装部署设计

1.scheduler采用独立pod部署，基础功能版本无负载均衡功能，只有一个scheduler

2.scheduler容器规格暂定2U2G

3.scheduer服务端口：8889

### 2.3 Frontend详细设计

#### 2.3.1Frontend逻辑模型

[FunctionAccessor-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/8e77cb2bca3148e2a26bcb20970567e3?projectDiagramUUID=1ec4413c20494359966aa711d8a35fdd&versionUUID=6cec6db8aef24edeb150e26655e9b523)![FunctionAccessor](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Q3U?t=1665404621298)

新增LoadBalancer模块，模块通过LoadBalanceAPI与原有Frontend模块进行交互，避免scheduler逻辑分散在各处；通过SchedulerRouterAPI获取scheduler变更信息，通过RPCAPI发送rpc请求

Service模块负责整体调度的串联

LocalScheduler实现本地实例级别的调度

RemoteScheduler实现faas-scheduler实例负载均衡管理

Instance负载单个实例的Processor管理

#### 2.3.2frontend负载均衡设计

对于同一个函数，frontend需要把调度请求发到同一个scheduler，避免由多个scheduler决策带来的多中心问题

frontend通过通过一致性Hash环解决这个问题

[一致性Hash](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=d37d22f98a0a467aa5016da3dab2d7b9)![一致性Hash](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2O96?t=1659494750161)

具体流程

1.scheduler与路由etcd建立租约，通过路由etcd发布服务

2.frontend通过etcd监听scheduler路由信息，当有scheduler加入时，将scheduler加入hash环

3.scheduler通过etcd监听scheduler路由信息，当有scheduler加入时，将scheduler加入hash环

4.frontend收到函数请求时，根据函数的Key计算函数的在Hash环中的位置，将函数与scheduler绑定，这里每个frontend构建的hash环是完全一致的

5.scheduler的添加和删除只依赖于etcd事件

#### 2.3.3frontend心跳设计

[心跳检测](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=814bfbe611a14d9b9213e91654bf5b26)![心跳检测](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2NPj?t=1665970616797)

frontend维持与scheduler的心跳

当心跳alive时，标记scheduler可用

当心跳dead时，将scheduler标记为不可用，上报紧急告警，采用兜底调度方案

#### 2.3.4frontend性能设计

1.frontend向scheduler请求实例超时时间250ms，一次调度最大时延不超过500ms

2.frontend异步归还实例，不在函数请求的链路之上

3.frontend异步续租实例，只在请求调度时，检查一下是否有续租失败的实例，对性能影响很小

### 2.4 约束

#### 2.4.1相关场景约束

1.动态内存场景：已考虑，函数实例的管理和调度都是按照funcKeyWithRes来实现的，

2.静默函数场景：已考虑，scheduler从/silent/sn/functions下读取函数，对于单个函数，frontend目前请求实例是串行的，一个集群是3-5个frontend，不存在缓存击穿问题。 当前scheduler未做singleflight防止缓存击穿，后续需要可以加上

3.函数并发度更新场景：已考虑，函数并发度更新时，Processor重新生成，LeaseVersion重置为1，有极低概率LeaseVersion重复，即使重复影响也很小，可接受

4.函数多版本：已考虑，funcKeyWithRes中有函数版本

5.伏羲：配置还没有到环境

#### 2.4.2上下游依赖需求

AR.SR.IREQ02793119.008.003 支持scheduler统一调度可靠性 10月第二个迭代转测试

AR.SR.IREQ02793119.008.002 支持scheduler统一调度性能：代码实现和设计时已经尽可能考虑性能，具体性能还需摸底后再做优化

AR.SR.IREQ02793119.008.004 scheduler支持0-1扩容，暂未规划

#### 2.4.3其他约束

1.本迭代只交付功能，可靠性需求在下一个迭代

2.scheduler负载均衡功能在下一个迭代，当前只支持部署1个scheduler

2.由于frontend没有请求队列，请求实例是依赖锁来串行的，对性能有比较大的影响，已经通过调度超时时间对调度时延进行兜底。且代码实现和设计时已经尽可能考虑性能，具体性能还需摸底后再做优化，由AR.SR.IREQ02793119.008.002承载。

目标：20个函数共1万QPS，函数执行10ms， 增加的平均时延在2ms以内（测试摸底）

3.由于scheduler没有弹性扩容能力，因此在Functiontask触发扩容后会多扩，如果流量持续，当前的调度算法无法快速缩容多余的实例。这个依赖后续scheduler以及frontend的调度算法的优化。

## 3. 可信软件

### 3.1安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*

scheduler与frontend通信采用TLS协议

scheduler服务端口写入通信矩阵

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*

#### 3.2.1.scheduler与元数据ETCD断连

1.当scheduler与元数据ETCD断连时，scheduler自身会上报紧急告警；

2.当frontend向scheduler请求实例时，如果已经有函数缓存，调度不受影响；如果没有函数缓存，会返回170001错误，frontend降级成兜底调度算法

#### 3.2.2.scheduler与路由ETCD断连

1.当scheduler与路由ETCD断连时，scheduler自身会上报紧急告警；

2.scheduler只能感知已经缓存的实例信息，可能给出错误的调度结果，frontend会对调度结果进行兜底，如果scheduler给出的实例不是frontend已经从etcd里缓存的实例，frontend会忽略这个实例，降级成兜底调度算法

### 3.3隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

scheduler不打印租户ID等用户相关信息

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

#### 3.4.1.scheduler与frontend网络故障

1.frontend检测到scheduler心跳失联，上报紧急告警，并标记scheduler不可用，降级成兜底调度算法

2.网络故障恢复后，重新走scheduler调度

3.发生故障期间以及故障恢复后的一小段时间内有多扩现象

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

frontend有是否启用scheduler进行调度的开关，支持配置热加载，默认开启，可以关闭，当现网scheduler出问题时，可以关闭，快速恢复业务

"loadBalanceConfig":{
"concurrencyThreshold":-1
}

大于concurrencyThreshold，走本地直接调度，否则走scheduler调度

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

scheduler的核心领域可基于接口进行LLT场景化测试

frontend的FaaSScheduler可基于接口进行LLT场景化测试

目前scheduler覆盖率83%+

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

见性能设计

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

scheduler：每个经过scheduler调度的函数需要一个go协程，函数删除时释放

frontend：每个经过frontend调度的函数需要2个go协程，一个异步归还实例，一个异步续租。无实例时释放

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

内部模块，客户不感知

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

frontend可独立升级，即使不部署scheduler，也无影响

## 4. 用例

### 4.1LLT用例

![1665540177019](C:\Users\c00464709\AppData\Roaming\Typora\typora-user-images\1665540177019.png)

![1665407039024](C:\Users\c00464709\AppData\Roaming\Typora\typora-user-images\1665407039024.png)

![1665407057832](C:\Users\c00464709\AppData\Roaming\Typora\typora-user-images\1665407057832.png)

### 4.2调度场景实测

持续调用3分钟之后，多扩不超过10%

| 函数并发度 | 函数执行时间 | 请求并发数 | frontend个数 | 预留实例个数 | （scheduler）瞬时、稳态实例数 | (无scheduler）瞬时、稳态实例数 |
| ---------- | ------------ | ---------- | ------------ | ------------ | ----------------------------- | ------------------------------ |
| 1          | 5s           | 20         | 3            | 0            | 24、21                        | 59、59                         |
| 1          | 5s           | 20         | 3            | 10           | 23、23                        | 60、60                         |
| 1          | 5s           | 20         | 3            | 20           | 21、20                        | 65、65                         |
| 1          | 100ms        | 20         | 3            | 0            | 23、23                        | 96、94                         |
| 1          | 100ms        | 20         | 3            | 10           | 22、22                        | 82、80                         |
| 1          | 100ms        | 20         | 3            | 20           | 22、22                        | 95、93                         |
| 1          | 10ms         | 20         | 3            | 0            | 21、21                        | 45、45                         |
| 1          | 10ms         | 20         | 3            | 10           | 24、24                        | 40、40                         |
| 1          | 10ms         | 20         | 3            | 20           | 21、21                        | 36、36                         |
| 10         | 5s           | 200        | 3            | 0            | 31、22                        | 39、39                         |
| 10         | 5s           | 200        | 3            | 10           | 30、21                        | 28、28                         |
| 10         | 5s           | 200        | 3            | 20           | 21、21                        | 26、26                         |

## 5遗留问题

1.scheduler针对不活跃函数，删除scheduler，释放资源