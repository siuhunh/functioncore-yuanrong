# 【functioncore】支持函数资源池动态弹性 AR.SR.20210621221420.030.007

AR： 【functioncore】支持函数资源池动态弹性 AR.SR.20210621221420.030.007

## 1. Story 整体设计

### 1.1 功能描述

功能：支持资源池动态弹性扩缩容

- 池子扩容触发条件：池子使用超过 50%，扩容1/4

- 池子缩容触发条件：池子使用小于 10%，一分钟缩容一次，每次缩当前总大小的 1/8
- 限制条件：pool池初始大小需要记住
  - 扩容时限制总的空闲池子个数不能超过 600 个（600 可配，需根据环境资源计算最大可以启动的池子数量）
  - 缩容时限制不能缩到初始大小以下

- 上述条件可配置

### 1.2 场景分析

**正常场景：**

1. 触发扩容，扩容成功：扩容一次，扩容后空闲 pod的数量是原来的 5/4
2. 触发缩容，缩容成功：缩容一次，缩容后空闲 pod 的数量是原来的 7/8

**异常场景：**

1. 触发扩容，扩容失败：重试三次，每次间隔 1 s
2. 触发缩容，缩容失败：重试三次，每次间隔 1 s

### 1.3 方案详细设计

[AR.SR.20210621221420.030.007-支持函数资源池动态弹性-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=bcc1b0537780447dbe19fcf092951551&versionUUID=)![AR.SR.20210621221420.030.007-支持函数资源池动态弹性](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/12w3?t=1631172737451)

一、弹性扩容

被特化的 pod 增加一个，则触发检测一次是否达到扩容条件。达到，则扩容；扩容当前总空闲池子数量的 1/4。

二、弹性缩容

被特化的 pod 减少一个，则触发检测一次是否达到缩容条件。达到，则启动一个定时器，定时器过程中，如果发现不满足缩容条件，则停止定时器；定时器结束时，再触发检测一次是否达到缩容条件，达到，则缩容。

### 1.5 增加的配置项

配置项对应：

> 池子使用超过 50%：UsedPodNumsExceedPercentage
> 扩容 1/4： ScaleUpPodNumsPercentage
> 池子使用小于 10%：UsedPodNumsBelowPercentage
> 一分钟缩容一次：ScaleDownInterval
> 每次缩容当前总大小的 1/8：ScaleDownPodNumsPercentage
>
> 扩容和缩容池子的数量须是整数（小数使用四舍五入的方式取整）

配置项数据结构：

```go
// PoolMgrConfig defines pool manager config
type PoolMgrConfig struct {
	...// 其他配置项
	...// 其他配置项
	ElasticScalingCfg ElasticScalingConfig     `json:"ElasticScalingCfg,omitempty" valid:",optional"`
}
// ElasticScalingConfig elastic scaling config of pool pod
type ElasticScalingConfig struct {
	UsedPodNumsExceedPercentage float64 // 例如：0.5 代表池子使用超过 50% 的时候，触发扩容
	ScaleUpPodNumsPercentage    float64 // 例如：0.25 扩容当前空闲池子的 1/4
	UsedPodNumsBelowPercentage  float64 // 例如：0.1 代表池子使用小于 10% 的时候，触发缩容
	ScaleDownPodNumsPercentage  float64 // 例如：0.125 代表每次缩容当前总空闲池子数量的 1/8
	ScaleDownInterval           int     // 单位：秒 例如：60 代表触发缩容条件的统计时间窗口为 1 分钟
    MaxPoolSize                 int     // 池子 pod 扩容的上限，超过此上限则停止扩容，此上限需根据环境资源来计算得出
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

| 序号 | 用例名                     | 测试步骤                                                     | 预期结果                                         |
| ---- | -------------------------- | ------------------------------------------------------------ | ------------------------------------------------ |
| 1    | 正常场景触发扩容           | 1. 使用pod数量超过当前空闲池子数量的 50%<br />2. 查看是否有新的pod被拉起，且总的空闲池子的数量是原来池子数量的 5/4 | 新的pod成功被拉起，且处于ready状态，数量符合预期 |
| 2    | 正常场景触发缩容           | 1. 超过一分钟池子的使用量小于 10%<br />2. 查看是否有老的pod被缩容，且缩容后空闲池子的数量是原来池子数量的7/8 | 老的pod被成功缩容，数量符合预期                  |
| 3    | k8s api 不可用时，触发扩容 | 1. 使用pod数量超过当前空闲池子数量的 50%<br />2. 构造资源不足的场景，查看新的pod是否被成功拉起 | 新的pod没有被拉起                                |
| 4    | k8s api 不可用时，触发缩容 | 1. 使用pod数量超过当前空闲池子数量的 50%<br />2. 构造 k8s api 不可用的场景，查看老的pod是否被缩容 | 老的pod没有被缩容                                |