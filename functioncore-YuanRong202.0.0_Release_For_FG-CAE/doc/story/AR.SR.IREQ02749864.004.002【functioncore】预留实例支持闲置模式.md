# AR.SR.d4b9e207.003【functioncore】预留实例支持闲置模式

AR.SR.IREQ02749864.004.002 【functioncore】workmanager感知预留实例闲置模式，不进行计费

AR.SR.IREQ02749864.004.003 【functioncore】worker支持闲置模式下pause cpu

## 1. Story 整体设计

### 1.1 功能描述

**背景**

- 为了降低客户的成本，支持预启动实例应对用户并发请求
- 用户勾选预留实例闲置模式后，预留实例的计费方式和 cpu 使用方式会发生变化：
  - 空闲时收取之前预留实例 20% 的费用
  - 工作时，按量收费
  - 预留实例闲置时，冻结 cpu，防止后台偷 cpu



**对齐函数元信息中标识用户是否开启预留实例闲置模式的配置：**

```go
/instances/business/yrk/cluster/fgs-yuanrong-dev02/tenant/fb6a00ff7ae54a5fbb8ff855d0841d00/function/0@default@test-sgs/version/latest
{"instanceMetaData":{"maxInstance":5000000,"minInstance":0,"idleMode": true}}
```

### 1.2 详细设计

- 用户勾选预留实例闲置模式
- workermanager 通过 idleMode 字段，感知到用户勾选了预留实例闲置模式
- workermanager 本地对这个函数的预留实例不计费（预留实例的计费由 proxy 单独上报）
- 当预留实例空闲时，暂停 cpu，防偷跑

[支持预留实例闲置模式-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=e42ea77aa9ed450bb831be109a834946&versionUUID=)![支持预留实例闲置模式](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1qUr?t=1652323770271)

### 1.3 上下游依赖需求

李沛含 AR.SR.IREQ02749864.004.001 proxy 【functioncore】BusProxy支持对闲置模式的预留实例进行计费

夏嘉诚 前端支持用户勾选预留实例闲置模式



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
workermanager 发生给 worker 闲置模式的开关信息时，如果发送失败，则重试 3 次，每次间隔 1s

### 2.5 可用性（Availability）

*【处于按要求执行状态的能力】*  
不涉及

### 2.6 安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及

## 3. HLT 用例

| 序号 | 用例名                               | 测试步骤                                                     | 预期结果                                       |
| ---- | ------------------------------------ | ------------------------------------------------------------ | ---------------------------------------------- |
| 1    | 闲置模式预留实例单独计费             | 1. 用户勾选预留实例闲置模式后，workermanager 预期不对预留实例计费<br />2. 查看 redis 中预留实例 fss-reserve-instance-billing-data 字段中是否有该预留实例，在勾选了闲置模式之后的计费信息 | redis 中没有预留实例勾选闲置模式之后的计费信息 |
| 2    | 闲置模式的预留实例支持空闲是暂停 cpu | 1. 用户勾选预留实例闲置模式后，和普通实例一样，在空闲时 cpu 挂起，防后台偷跑 | 符合预期                                       |
| 3    | 取消闲置模式预留实例单独计费         | 1. 用户取消勾选预留实例闲置模式后，workermanager 恢复对预留实例的计费 | 符合预期                                       |
| 4    | 取消闲置模式预留实例空闲时暂停 cpu   | 1. 用户取消勾选预留实例闲置模式后，不对预留实例的 cpu 执行空闲挂起操作 | 符合预期                                       |