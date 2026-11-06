# 函数执行详情

关联AR:
+ AR.20210522184837.007： 新增 runtime 管理进程，支持 worker 和 runtime 管理进程间 socket 通信

## Story 整体设计

### 功能描述
用户日志支持标准输出日志采集，包括 自定义运行时， go， 及其他。


### 场景分析
worker 需要采集到各种 runtime 运行时的日志， python \ nodejs \ java 用户日志通过各自的运行时发回给 worker ，自定义运行时日志需要 runtime manager 采集标准输出标准错误日志并发回给 worker ， worker 补充请求的头尾 'Start request' 和 'Finish request' 的记录信息写入 user-function.log （后续将该目录挂载到主机上，供日志采集组件收集，规划在另一 AR ）以及返回前端。


### 方案详细设计

#### 现状分析
当前 python \ nodejs \ java 满足需求 ，用户函数与runtime共进程，runtime将标准输出标准错误截获，类似用户使用提供的logger方式，实时将日志发回给worker。
自定义运行时要求用户自行写入 user-function.log，前端无法采集日志。

#### 方案设计
自定义运行时当前流程：
   [custom runtime](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=3fb89622614f4b03972515c495fef6cf)![custom runtime](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/pF5?t=1623210670416)

新增日志采集流程：   
   由 runtime manager 采集子进程的标准输出日志， 在有日志打印时，实时发送给worker。worker 在请求完成后向 runtime manager 索要日志（影响性能，但确保日志收集完整）。worker将日志写入 user-function.log ，并返回。
   [custom runtime log](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=bcd80b89ed644f21959d153cf59bdc8d)![custom runtime log](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/pK2?t=1623225986128)
   

#### 开源软件选型
无

#### 外部交互分析&&上下游依赖需求
无

### 对外接口
无

### 约束
无


## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*  

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*  

#### 可用性 Availability
在请求多并发的场景下，因标准输出标准错误为系统唯一，因此组件无法识别每条标准日志对应的请求。因此单并发时，请求返回 requestID ，并与请求相关联；多并发时，请求不携带 requestID 。

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  

## LLT 用例
*说清楚预置条件、输入（构造步骤、构造内容）、输出（预期结果）*

| 序号  |  用例名|  测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  自定义运行时用户日志长度超过 2k | 用户日志打印日志长度大于 2k | 正常返回截断后的保留最新的日志且包含头尾信息， user-function.log 中包含完整日志  |
| 2 |  自定义运行时用户日志长度为 2k | 用户日志打印日志长度为 2k | 正常返回全部日志且包含头尾信息， user-function.log 中包含日志  |
| 3 |  自定义运行时用户打印爆盘 | 用户日志打印 100M | 正常返回全部日志且包含头尾信息， user-function.log 中包含 50M 日志  |
| 4 |  trigger中的标准日志打印 | 使用trigger触发函数调用并打印日志 | 正常返回全部日志且包含头尾信息， user-function.log 中包含日志  |
