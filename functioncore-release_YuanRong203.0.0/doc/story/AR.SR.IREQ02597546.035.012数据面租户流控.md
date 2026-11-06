[TOC]
# AR.SR.IREQ02597546.035.012 【可过载控制】数据面租户流控

## Story 整体设计

### **功能描述**

**背景说明**

为了对齐管理面的租户流量控制，数据面的流控需跟管理面一致。

**功能描述**

需要保证方法自身的安全、内存完整。复用令牌桶的流控算法，通过在etcd中提取的租户属性的流量阈值对租户可以通过的最大流量进行控制。

**场景分析**

1.租户信息存放在管理面的etcd中，需要通过对其建立的连接获取到租户流量的配额数据。

2.在数据面的流量控制逻辑中需要添加流控方法对租户的流量进行控制，使用令牌桶算法， 令牌桶存有一定量的token做为缓冲，并配置冗余，如果流量超出配额，则报错异常。

3.多实例场景下，按FunctionAccessor的初始个数去除etcd中读取的流量阈值，在阈值基础上设一定量的冗余

**数据流图**

[AR.SR.IREQ02597546.035.012-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=a2c3ab0953d0417585bbc90ab1ca4ce4&versionUUID=)![AR.SR.IREQ02597546.035.012](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/16Ey?t=1634884908292)

### 方案详细设计
#### 方案设计
1. 租户的数据流量阈值需要通过管理面的etcd取出，先在watcher里添加processWatcher协程，对管理面的etcd进行监听，其中etcd数据的key-value格式为：

   > /sn/qos/business/yrk/tenant/<tenantid>
   >
   > {
   > 	“quota”:1000
   > }
   
    如果监听下的etcd的流量配额数据发生了变化，则更新tenant容器保存的流控数据，对限流策略进行调整更新。

   PUT事件更新配额数据，DELETE事件则更新配额数据为默认值。
2. 添加TrafficParams结构体，其中包括TenantLimitRate租户的限流速度，与TenantBucketSize令牌桶的大小，用于生成该租户匹配的RateLimiter。
   添加Tenents结构体，包括一个Map：TenantInfo映射为tenantId与其quota流量配额，用于保存租户的ID及其流控配额。本地的TenantBuf使用其结构来缓存租户信息。
   添加TenantContainer，为tenantId与Limiter，根据这个租户的TrafficParams和quota生成的，用于执行流控策略。
   

3. FunctionAccessor中对租户的请求添加TenentTraffic函数，该函数需要传入租户id的TenantID,对Tenents对象进行流量控制检测，其中包括tenentTakeToken方法、tenentLimiterController方法。tenentTakeToken函数为TenentContainer对象是否可以获取到令牌桶的token，从而决定是否可以接受请求；tenentLimiterController函数是根据流控的阈值参数生成
Limiter，从而对请求的流量进行控制

4. 通过对TenentContainer的参数对象调用tenentTakeToken方法试图调取token，tenentTakeToken函数则调用对象的tenentLimiterController函数与得到限制信息状态，如果对象可以获取到token，则可以允许请求；如若不能则返回报错。从而达到流量控制的效果。

时序图设计如下：

[AR.SR.IREQ02597546.035.012 【可过载控制】数据面租户流控-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=e1f97d7a7f2949f9917877bf28e8ef98&versionUUID=)![AR.SR.IREQ02597546.035.012 【可过载控制】数据面租户流控](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/17bz?t=1634281636216)
#### 外部交互分析&&上下游依赖需求
需要连接管理面的etcd获取租户流量阈值数据
### 对外接口
无
## 可信软件
#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

不涉及

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*
不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*
不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

不涉及


#### 可用性 Availability
*处于按要求执行状态的能力*
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*
不涉及
## LLT 用例
|  序号 |   用例名  | 测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 | 租户流量未超阈值 | 设置流量阈值大小为5，触发5个请求，五个请求都可以通过       | 正常获取token，请求通过 |
| 2 | 租户流量超过阈值 | 设置流量阈值大小为5，触发20个请求，有15个请求无法获取token，报错 | 无法获取token，请求拒绝 |
| 3 | 更新租户阈值信息 | 设置流量阈值大小为5，改为6，同时触发20个请求 | 6个通过，14个无法通过 |
| 4 | 删除租户阈值信息 | 设置流量阈值大小为5，删除event，同时触发20个请求 | 4个通过，16个无法通过  |