[TOC]

# functioncore实例调度及管理优化Story设计说明书

关联AR:

- AR.20210623113005.001: 【functioncore】busproxy worker 请求分发和Eviction策略
- AR.20210623113213.001: Frontend和Busproxy 负载均衡策略需要匹配
- AR.20210623113821.001: Busproxy 对远端worker的转发策略
- AR.20210623114222.001: workermanger 在纳管资源性能优化

## 1. Story 整体设计

### 1.1 功能描述

**需求**：当前functioncore存在请求调度不均衡，最小实例处理请求存在时延等问题，导致长尾出现多个用例失败。需要对frontend负载均衡，bus proxy请求转发，最小实例纳管等系统功能进行优化，使得请求调度更加合理，从而提升整体处理效率

**作用**：根据proxy实际对接的实例分布来进行请求调度，同时优化frontend轮询算法，通过租约来控制proxy对worker的管理，预留实例通过租约的续约来实现，worker manager仅纳管缩容实例用于加速扩容

**约束**：1. 该方案采用远程worker处理方式，调用链较普通worker会长一些，暂时通过降低权重的方式来减少请求在远程worker上的调度  2. functiontask故障场景下最小实例worker目前不会再次向proxy注册，会出现该worker无法被调度和管理的问题，待proxy注册流程修改后可修复  3. 函数互调场景无法保证被调函数的请求均匀性

 

### 1.2  场景分析

**frontend调度不均衡**

在有worker实例的情况下，frontend会对worker实例进行加权轮询，将请求发给worker所在的proxy上。但是，frontend目前采用的轮询算法，会以实例加入先后，以固定顺序进行轮询。同时：

1. frontend通过watch etcd来感知实例信息；当前算法下会导致所有的frontend的轮询顺序都是完全一致的
2. 如果有新的实例被拉起，那么frontend的负载均衡会被重置，又重头开始轮询，但是新实例的位置在轮询的末尾。

这样就导致了，越早被拉起的实例，越容易收到请求，且frontend实例数量越多，负载不均衡就越明显。

![frontend负载均衡问题](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/t82?t=1625141672212)

**远端worker调度问题**

当前busproxy在拉起一个远端worker后，会立即将worker并发度数量的请求转发到对应的proxy上；同时对于这个远端worker来说，其他所有节点的proxy都能向它转发请求，而其他节点的proxy实际上是不知道这个worker的负载情况的。这样容易导致盲目转发、相互转发。

**最小实例调度问题**

proxy-1和proxy-2各有一个最小实例被worker manager纳管，frontend首先将请求发给Proxy-1，Proxy-1从worker manager处获得了worker-1，开始调度请求。此时请求也发送到了proxy-2，此时proxy-2通过etcd发现proxy-1存在可用实例，将请求转发给了proxy-1所持有的worker-1，worker-2只有到Proxy的请求量需要扩容时，才会被取出，此时已出现调度不均衡。

![最小实例调度](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/suy)

### 1.3方案详细设计

- **负载均衡及调度优化 **

2. 在已存在worker的时候，frontend将按照各个proxy实际对接的worker进行加权随机，将请求分发给各个proxy节点。
3. proxy对接两种类型的worker，本地worker以及远程worker，所有worker都保存在一个优先级队列中，通过请求处理情况和权重来调整worker在队列中的位置，队首worker被认为是当前最空闲的，将被优先调度请求。

- **扩缩容及实例管理优化**

1. 在某个proxy发起扩容请求时，worker manager会尽量选择该proxy所在节点的worker返回，当worker池分布不允许时（例如节点资源不足），worker manager将返回其它节点的worker。
2. 扩容时worker特化后会向本节点proxy注册，proxy会先在broker缓存中创建worker broker，此时该broker不会被调度，待worker manager返回一个worker id之后，proxy会去cache中找到该broker并进行调度。远程worker场景下，worker的远端broker会发激活请求来连接近端broker来转发请求。
3. 缩容时worker会被worker manager纳管，待一定时间后被永久删除，假如期间收到扩容请求会直接返回该worker来加速扩容

![调度优化](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/t22)

#### 1.3.1 Frontend调度优化

##### frontend负载均衡

需要修改负载均衡策略，避免每个frontend采用一致的轮询顺序，在轮询时改为随机顺序。

##### 远端worker纳管
workermanager拉起实例时，将拉起方的IP写入实例信息的OwnerIP中。其中，最小实例的OwnerIP是实例所在节点的NodeIP。
为了保证远端worker在被纳管之前，该worker不被纳入本节点的proxy调度，proxy收到worker的注册请求之后，会将该请求暂时缓存起来，等收到通知这个worker是本地worker或是远端worker之后，再做相应处理。
之后改成由proxy向worker注册后，可以去除缓存操作。

拉起实例分三种情况讨论：
workermanager拉起的实例在本节点（本地worker，非远端worker）
1. broker缓存worker的注册信息
2. proxy收到workermanager的返回消息，发现拉起的worker在本节点，从缓存中取出worker注册信息，创建对应的broker，纳入本地调度

实例由workermanager直接拉起（最小实例的情况）
1. 实例所在节点的proxy缓存worker的注册信息
2. 该节点的proxy收到etcd事件，发现这个worker由本节点纳管，且为本节点的worker，从缓存中取出worker注册信息，创建对应的broker，纳入本地调度

workermanager拉起的实例在其他节点（远端worker），假设proxy A在proxy B上拉起了一个worker C
1. proxy B缓存worker C的注册信息
2. proxy A收到workermanager的返回，发现是远端worker，向proxy B发送通知
3. proxy B收到通知后，从缓存中取出worker C的注册信息，创建对应的workerbroker，但不纳入本地调度。
4. proxy A创建一个remote broker，并将其纳入调度。
5. worker C退出后，proxy B清理对应的资源，并通知proxy A； proxy A收到通知后，删除对应的remote broker和调度。

#### 1.3.2 Function Bus调度优化

**1、优先级队列请求调度**

每个busproxy的woker实例分为本地预留实例、本地扩容实例和远程实例，本地预留实例无法被缩容。本地预留实例、本地扩容实例和远程实例维护在一个优先级队列中，优先级高的woker实例位于队头，请求调度时将请求直接发给队头worker实例。

优先级值计算如下：

```mathematica
value = PriorityRate * (requestQueueLen / concurrency)
```

其中本地实例的PriorityRate >> 远程实例的PriorityRate。

请求调度时，优先将请求调度到本地实例、其次调度到远程实例。

**2、缩容**

计算请求qps，当有请求可以缩容时，从优先级队列尾部缩容实例。当实例请求数为0可以缩容时，向workermanager发送缩容请求，workermanager判断当前节点实例数量是不是大于节点最小实例数量，如果是，返回可缩容；否则，返回不可缩容。

[busproxy调度策略优化](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=6afc9819d3b44e8f958db10f66cc2bd3)![busproxy调度策略优化](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/v9X?t=1626319602943)



#### 1.3.3 优化函数实例管理

##### FunctionService优化

当前在functionservice的status字段挂载了该函数特化的所有Instance信息，但是在大并发场景中扩缩容量大的情况下，status中的Instance会频繁更新，造成worker manager函数更新时冲突增多，进而造成函数更新不及时等一系列问题。

##### 最小实例管理

最小实例是用户付费资源，由平台纳管存在一定争议，同时当前节点存在转发问题，导致最小实例的请求调度存在问题，最小实例应该直接归属给bus proxy，从而直接处理请求。考虑到最小实例故障补充可能造成过度扩容，采用最小实例普通实例不与区分的方案，通过worker manager缩容返回值来判断当前实例数是否到达最小实例从而不进行缩容。

![minInstance_lease](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/t7H)

##### 缩容实例纳管

考虑到缩容worker是已特化worker可以延迟删除，利用缩容实例已加载函数的特点来加速冷启。worker manager将缩容worker存放在一个池中，在proxy再次向worker manager申请worker的时候，直接挑选一个池中之前缩容的worker返回。考虑到远程worker调用链过长应尽量避免，再次分配worker时会根据请求节点来尽量返回对应节点的worker。缩容worker池中的worker在到达阈值时间之后就会被永久删除。

![Instance_manage_optimize](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/t36)

## 2.可信软件

#### 安全性 Security

*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*  

#### 韧性 Resilience

*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  

#### 隐私性 Privacy

*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  

#### 可靠性 Reliability

*在给定的条件、给定的时间区间能无失效地执行要求的能力*  

#### 可用性 Availability

*处于按要求执行状态的能力*  

#### 安全 Safety

*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  

## 3.HLT 用例

| 序号 | 测试用例 |                 测试步骤                 |             预期结果             |
| :--: | :------: | :--------------------------------------: | :------------------------------: |
|  1   | 并发测试 | 创建函数并在无预留实例情况下进行并发测试 | 每个实例处理请求数均匀性优于当前 |
|  2   | 预留实例 |      创建5个预留实例，并发请求数>5       |       每个预留实例都被调度       |