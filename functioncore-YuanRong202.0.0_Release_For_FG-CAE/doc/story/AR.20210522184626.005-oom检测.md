[TOC]

# oom检测

## Story 整体设计

### 功能描述
runtime-manager在runtime内存不足时，记录oom错误，worker收到runtime的invoke失败时查询runtime manager，runtime manager返回oom错误。worker将oom错误信息作为函数调用结果返回上层。

### 场景分析
+ 该需求是为了让函数使用超过限制的内存后，上层调用者获得oom的错误信息。

### 方案详细设计

#### 现状分析
+ 当前没有检测oom的情况下，调用者在oom时只会收到runtime异常的错误信息，无法得知是oom错误还是其他错误。

#### 方案设计
+ 新增错误码 4018 RuntimeMemoryExceedLimit

###### 被动监测
1. runtime-manager监控runtime内存不足：
    1. 在内存上报时检查当前内存是否已经超过设置的内存大小-5M（例如128-300的池子，内存上限为123M）

2. runtime-manger在内存超出限制后杀掉runtime进程，并且标记runtimeError为RuntimeMemoryExceedLimit
3. worker与runtime连接中断后，worker向runtime-manager发送请求，获取到runtime错误状态为RuntimeMemoryExceedLimit
4. 将worker中未收到response的请求返回，错误为RuntimeMemoryExeedLimit，错误码4018，错误信息runtime memory limit exceeded
5. worker进程退出
6. runtime-manager进程退出

注：2-5 的流程与 AR.20210522184837.004 函数默认磁盘空间限制几乎相同

###### 主动查询
1. worker与runtime连接异常中断后，worker向runtime-manager发送请求
2. runtime-manager主动查询
    1. /sys/fs/cgroup/memory/memory.failcnt不为0
    2. 系统日志最后100条（使用dmesg查询）中有Memory cgroup out of memory: Kill process的日志信息
3. runtime-manager标记runtimeError为RuntimeMemoryExceedLimit
4. worker获取到runtime错误状态为RuntimeMemoryExceedLimit
5. 后同被动监测

[AR.20210522184626.005-oom检测-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=cf982d0bdb8a4935a925a4aed6eff180&versionUUID=)![AR.20210522184626.005-oom检测](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/11Gd?t=1630461949052)

# 开源软件选型
无

#### 外部交互分析&&上下游依赖需求
*描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系*

### 对外接口
*描述对外新增的接口*
+ 无
### 约束
对函数的oom检测无法保证100%可靠，因为runtime manager有可能被linux系统直接杀死导致错误来不及向worker返回

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
*处于按要求执行状态的能力*

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*

## LLT 用例
| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | ------------------------------------ | ---------------------------------------- |
|  1 | runtime 使用内存，超过限制内存 | runtime pod 重启 |
|  2 | runtime 使用内存，不超过限制内存 | runtime pod 正常运行 |
