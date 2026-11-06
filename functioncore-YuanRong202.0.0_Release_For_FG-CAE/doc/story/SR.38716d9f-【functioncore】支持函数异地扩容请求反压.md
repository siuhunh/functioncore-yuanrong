[TOC]

# 【functioncore】支持函数异地扩容请求反压

关联AR:
+ AR.SR.38716d9f.002: 【functionCore】frontend支持根据反压请求更新路由重均衡
+ AR.SR.38716d9f.001: 【functionCore】busProxy支持请求扩容反压

## Story 整体设计

### 功能描述
请求调度链路上：
frontend： 订阅路由 etcd 的函数实例事件，将请求均衡的路由给 busProxy ，根据实例数的权重做请求并发度的实时均衡。
busProxy：做请求的实际调度和扩缩容决策。

[bus-backpressure](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=88c251ca70db4ada89f7f293c6130980)![bus-backpressure](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2CnU?t=1657073780375)

当 busProxy 扩容的实例不在本节点时，busProxy 会将请求转发管理该实例的 busProxy （即实例所在节点的 busProxy ） 去处理，
因此请求实际在 proxyB 上执行，而 frontend 以为请求在 proxyA 上。
导致：
    1. frontend 认知的实例并发有偏差，无法进行负载均衡的纠正。
    2. 在未感知到新实例前，请求仍然发给 proxyA ， 可能再次触发扩容，引发多扩的问题。


### 场景分析
#### 场景一：

​    DTS2022062917018 【功能-SDV-proxy扩容】黄区融合测试环境，创建python3.6无状态函数，设置最大实例数1，单实例并发数10，函数10并发调用，四个请求成功，六个请求失败，失败状态码：res.text:{'code': 150429, 'message': 'function yyrk7e1a-0-base-testyrconcurrent1to10001reach max instance num 1

根因分析：

   1. 请求首先发给proxyA，proxyA扩容实例，到proxyB上，转发队列中的4个请求给proxyB。
      
   2. frontend还未感知到实例的创建，仍然发送请求给proxyA，proxyA不再转发，请求扩容失败，最终失败。

         

#### 场景二：

​    DTS2022062915135 【长稳-SDV-FunctionBus】python 函数A调用java函数B，函数B sleep 10ms，发送1000个函数A调用函数B的请求，并发度设置为600，预期A和B扩出2个实例，实际函数A扩出3个实例，函数B扩出4个实例，存在过度扩容的问题，请定位具体原因并修改

根因分析：

   1. 请求发给proxyA， proxyA在收到第一个请求时触发扩容，扩容出一个proxyB上的worker。
      
   2. 此时将队列里全部 896 个转发给 proxyB，于是proxyB再次扩容1个实例。

   3. 而proxyA又收到新请求也再次扩容。
      

#### 场景三：

​    DTS2022062511191 【压测长稳-SDV-functionbus】云鲸智能的5000万日活模拟 管理侧 最大瞬时 600并发 当前观察到函数实例在 600个实例到700多个实例波动 ，有多扩容现象， 函数隔 几分钟会有几十个函数实例Terminating

根因分析：

​    在单实例并发度小，请求并发量大的场景下，以及流量本身存在一定波动，扩容导致的请求负载不均衡且无法纠正，会影响实例出现反复扩缩的现象。同时需要明确该场景的流量模型，提升定位定界能力。

### 方案详细设计

#### 现状分析
同上

#### 方案设计
*详细的方案设计，尽量适用时序图、类图等描述*
实例数 0 -> 1 : busProxy 扩容到远端，将当前的请求和 3s [1] 内收到的请求均携带转发地址返回给 frontend ， 供 frontend 重调度。
实例数 1 -> n : busProxy 将队列中 max(1/n, maxConcurrencyPerInstance) 个请求反压给 frontend。

[1]: 稳态下， frontend 通过路由 etcd 订阅事件感知实例新增的时延规格 

frontend 将 busProxy 扩容反压的实例加入路由缓存，后续可以直接决策请求位置，无需多一跳。
    风险： 路由缓存通过 ① 监听路由 etcd ② busProxy 返回的结果 双线决策，可能有时序问题导致的缓存残留，引入问题。
    解决办法： 
        frontend 增加反压调度列表：
            增： proxy 返回的反压实例，且实例未在当前路由信息中。
            删： 监听到路由 etcd 的实例增加事件，如其在反压调度列表中，则删除
            比对：间隔 1min 触发检查，连续两个周期，实例均在反压调度列表中，认为该实例为残留实例，删除，并将残留的调度实例信息删除。



函数互调同理。

#### 开源软件选型
NA

#### 外部交互分析&&上下游依赖需求
NA

### 对外接口
NA

### 约束
NA


## 可信软件

#### 安全性 Security
NA

#### 韧性 Resilience
NA 

#### 隐私性 Privacy
NA

#### 可靠性 Reliability
- 当出现 busProxy / frontend 异常时，通信链路发生错误，请求由管理面进行重试。
- 当出现扩容的 worker 异常时， busProxy 也可以重新扩容调度实例。

#### 可用性 Availability
- 当一致性 hash 指定的调度节点 busProxy 没有资源池资源，请求因为反压重调度，相比之前直接转发多一跳，可能会有 1-2 ms 的延迟。
- 函数互调也是同样的道理。

#### 安全 Safety
NA

## LLT 用例
*说清楚预置条件、输入（构造步骤、构造内容）、输出（预期结果）*


| 序号 | 用例名称 | 测试步骤 | 预期结果 |
| --- | --- | --- | --- |
| 1 | 异地扩容反压 | 调用请求（前端调用/函数互调），异地扩容，请求能够反压 | 请求在异地调用成功 |
| 2 | 异地扩容反压后缓冲时间内继续反压 | 调用请求（前端调用/函数互调），异地扩容，请求能够反压，再次调用，请求仍能反压 | 多次请求在异地调用成功 |
| 3 | 扩容实例即刻退出 | 调用请求，异地扩容，worker退出，worker-manager将其从路由etcd中清理。 | 查看frontend/bus能清理该实例的调度信息。 |