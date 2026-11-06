[TOC]

# AR.SR-20210630-008.002【functionCore】支持1ms 函数计费

关联AR:
+ AR.SR.20210522184626.002.009:【FunctionCore】支持对初始化异常的函数进行计费

## Story 整体设计

### 功能描述
支持 1ms 函数计费， 将日志，计费信息精确到 1ms，不足 1ms 按 1ms统计。  
例如执行0.9, 计费1ms。执行1.1ms , 计费2ms。执行100.1 ms, 计费101ms


### 场景分析
1. 初始化函数日志，计费信息精确到 1ms  
- 初始化日志 billing duration 字段精确到 1ms  
    {"Level":"INFO","log":"2021-08-31 19:01:29.915+08:00 Finish initialize request 'xxx', duration: 1009.722ms, **billing duration: 1010ms**, memory used: 68.668MB.", ...}

- 初始化函数执行异常上报到redis的计费信息精确到 1ms  
上报到 redis value 数据结构，其中 BillingDuration 精确到 1ms
````
type InvokeSummary struct {
	FuncDigest         string        `json:"funcDigest"`      // Function Summary
	Duration           float64       `json:"duration"`        // Running time (unit: ms)
	BillingDuration    int           `json:"billingDuration"` // Charging time (unit: ms)
	MemorySize         int           `json:"memorySize"`      // Memory configuration (unit: MB)
	MemoryUsed         float64       `json:"memoryUsed"`      // Memory usage (unit: MB)
    ErrorType          String        `json:"errorType"`      
}
````

- 初始化函数的执行时长，和第一次函数调用时长相加，统计计费信息精确到1ms

2. 普通函数执行计费信息精确到 1ms  
- 函数执行日志 billing duration 字段精确到 1ms  
    {"Level":"INFO","log":"2021-08-31 19:01:29.927+08:00 Finish invoke request 'xxx', duration: 3.447ms, **billing duration: 4ms**, memory used: 68.668MB.", ...}  

- 普通函数的执行时长，统计计费信息精确到 1ms

3. 预留实例计费另外统计，当前不做修改  
![](http://image.huawei.com/tiny-lts/v1/images/d972c5eb79afe6ecd4289b4963f16ee5_624x264.png@900-0-90-f.png)

	预留实例生命周期如图所示，其中计费时长为T1 ~ T4。

	预留实例计量粒度为秒，不足一分钟，按照一分钟计费。超过一分钟，按照实际执行时长计费。

	例如预留实例执行时长为51秒，按照1分钟计费。执行时长为61秒，则计费时长仍然为61秒。

### 方案详细设计

#### 方案设计
1. 修改初始化计费统计信息精确到 1ms，不足 1ms 按 1ms 计算  
包括日志和执行异常返回到 redis 的计费字段，精度为 1ms  

2. 修改普通函数计费统计信息精确到 1ms，不足 1ms 按 1ms 计算  
包括日志和上报给管理面的计费字段，精度为 1ms  

#### 外部交互分析&&上下游依赖需求
上游需求：支持对初始化异常的函数进行计费。  对接人：殷瑞栋  

### 对外接口
NA

### 约束
NA

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
| 1 | 普通函数计费信息精度为 1ms |  普通函数执行时长1.1ms, 预期计费时长2ms | 函数调用成功，页面返回计费信息为 2ms，查看日志字段计费信息为2ms |
| 2 | 初始化函数计费信息精度为 1ms| 编写函数，配置初始化入口，第一次调用初始化+调用时长精度1ms， 例如初始化 1.2ms, 执行3ms, 预期计费 5ms | 函数调用成功，页面返回计费信息为 5ms，查看日志字段计费信息为 5s  |
| 3 | 初始化函数异常计费信息精度为 1ms| 编写函数，配置初始化入口，进行异常调用 | 函数调用失败，redis 中计费信息精度为 1ms，查看日志字段精度为1ms  |