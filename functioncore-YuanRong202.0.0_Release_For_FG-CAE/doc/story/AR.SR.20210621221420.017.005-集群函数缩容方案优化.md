# 集群函数缩容方案优化，支持从总集群角度评估缩容
        
## 1. Story 整体设计
### 1.1 功能描述
   目前函数实例扩缩容均由 bus proxy 负责，每个 bus proxy 管理自己名下的函数实例并根据流量评估扩缩容（即多活），因 bus proxy 在请求链路上，因此能得到最快的响应来触发扩容，但缩容却面临问题：
    1. 当单 functiontask 异常，导致了请求重试，即引入了一个新的 bus proxy 管理同一个函数，如果请求持续调用，funcAccessor 的均衡调度会使多个实例均长时间运行，导致实例浪费。
    2. 当某 funcAccessor 因为之前的请求系统错误，出现了 hash 移动，因各个 funcAccessor 间独立工作，无法校准，当批量请求到来时，也会路由分散。
   [busproxy缩容缺陷](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=d37639e437fb4d4c9965d50ebe69ada9)![busproxy缩容缺陷](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1J0M?t=1639379204260)

   增加总集群角度评估缩容，有以下作用：
    1. 根治以上问题。
    2. 为后续扩展提供方便：集群角度函数负载评估/预留实例的弹性等
   
### 1.2 场景分析
1. busProxy 每隔 5min ( 5 * 1min采样间隔 时间窗 ) 将管理的所有 worker 运行数据上报给 FuncInsMgr ，数据结构如下：
````
POST /worker-manager/v1/functions/worker/metrics

// ReportMetrics reported worker metrics
type ReportMetrics struct {
	MaxCurrentRequestNums int `json:"max_current_request_num"`
	UsedTimeSlice int `json:"used_time_slice"`
	// Represents seconds of UTC time since Unix epoch
	// 1970-01-01T00:00:00Z
	SecondsTimestamp int64 `json:"seconds_timestamp"`
}

// WorkerView worker view in time window
type WorkerView struct {
	InstanceID  string          `json:"instance_id"`
	FuncName    string          `json:"function_name"`
	FuncVersion string          `json:"function_version"`
	TenantID    string          `json:"tenant_id"`
	BusinessID  string          `json:"business_id"`
	Metrics     []ReportMetrics `json:"metrics"`
}
````
其中
- metrics： 目前上报包含 5 份数据， 1min 一份；
- maxCurrentRequestNum：时间窗内的最大并发度；
- UsedTimeSlice：时间窗内使用的时间片（单位：秒）；
  
2. funcInsMgr 通过滑动时间窗口查看 15min （误差上下 1min ）内所需 worker 数量(考虑预留实例)，并决策缩容。
    其中： 并发函数使用策略 maxCurrentRequestNum 计算所需 worker 数量
          非并发函数使用策略 UsedTimeSlice 计算所需 worker 数量

   特殊场景：
    + 决定缩容的 worker 有请求（同步/异步）正在执行， 缩容时会优雅退出。同时对 busProxy 下发的新请求返回`ClientExitErrCode = 211503`，将该 worker 剔除调度列表，并返回 funcAccessor 重试其他节点的调度。
    + 缩容 worker， 但此时路由信息仍在， 请求可能路由到该 worker 所属 busProxy ， 此时 busProxy 发现本节点无可用实例，且其他节点有，即反压该请求。暂未识别 worker 缩容期间其所属 busProxy 的扩容反复。
    + worker优雅退出时间满足当前支持的异步函数超时时间 12h。

       
### 1.3 方案详细设计
  
## 2. 可信软件
### 2.1 安全性（Security）
*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*

### 2.2 韧性（Resilience）
*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*  
不涉及
### 2.3 隐私性（Privacy）
*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*  
不涉及
### 2.4 可靠性（Reliability）
*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*  
不涉及
### 2.5 可用性（Availability）
*【处于按要求执行状态的能力】*  
不涉及
### 2.6 安全（Safety）
*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及
## 3. HLT 用例

| 序号  |  用例名|  测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  普通实例缩容 | 制造请求调用过程中 functiontask 异常的故障，拉起多个 worker ，持续调用 | 观察约 15min 后多余 worker 可以被回收|
| 2 |  预留实例不缩容 | 部署包含预留实例的函数，不发起调用 | 预留实例不会因为低负载被缩容   |
