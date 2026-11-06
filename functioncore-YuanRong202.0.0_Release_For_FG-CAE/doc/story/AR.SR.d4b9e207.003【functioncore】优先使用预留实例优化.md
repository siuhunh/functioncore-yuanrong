# AR.SR.d4b9e207.003【functioncore】优先使用预留实例优化

AR： AR-20210630-011-001

## 1. Story 整体设计

### 1.1 功能描述

- **现象**：调用函数后 60s 超时；

- **存在的问题**：
  - 预留实例拉起成功，但proxy未感知到，向workermanager发送扩容请求，workermanager发现预留实例全部拉起，则会选择扩容新实例。如果预留实例部分拉起，则 proxy 的扩容请求会等待，直到超时返回；
  - 等到了预留实例拉起后，返回这个预留实例，也可能存在问题，拉起的预留实例不属于请求的 proxy

- **原因**：
  - workermanager 和 proxy 对预留实例的感知有时延，workermanager 无法判断 proxy 是需要扩容新实例，还是需要调度预留实例



### 1.2 方案详细设计

**1. 正常场景：**

前提是：设置了预留实例

- proxy 发送扩容请求时，自身管理的 worker 数量大于 0，说明需要扩容一个普通实例；

- proxy 发送扩容请求时，自身管理的 worker 数量等于 0：
  - 其他 proxy 是否有实例可调度，有的话，则随机选择一个实例返回；
  - 其他 proxy 没有实例可调度，则等待预留实例拉起，并返回；



**2. 异常场景考虑：**

- proxyA 转发请求给 proxyB，但此时 proxyB 异常了，如何保证请求成功执行？
  - 方案：发送扩容请求给 workermanager，workermanager 扩容一个新的普通实例去处理请求；



**proxy 数据结构：**

```go
// DeployParam Deploy function param
type DeployParam struct {
	FuncName        string      // deployed function name
	Applier         string      // applier instance id
	DeployNode      string      // deploy node id
	Business        string      // business
	TenantID        string      // tenant id
	Version         string      // version
	OwnerIP         string      // owner IP
	TraceID         string      // trace id
	TriggerFlag     string      // This is a trigger request flag.
	OwnedWorkerView WorkersView // proxy owned worker num（新增）
	CPU             int
	Memory          int
}
```



### 1.6 上下游依赖需求

无

### 2.1 安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  
 不涉及

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

| 序号 | 用例名   | 测试步骤                                                     | 预期结果                   |
| ---- | -------- | ------------------------------------------------------------ | -------------------------- |
| 1    | 正常场景 | 设置预留实例马上调用                                         | 调用成功                   |
| 2    | 异常场景 | 1. 设置一个预留实例，等待拉起后，注入节点网络故障<br />2. 调用函数 | 扩容新的普通实例，调用成功 |