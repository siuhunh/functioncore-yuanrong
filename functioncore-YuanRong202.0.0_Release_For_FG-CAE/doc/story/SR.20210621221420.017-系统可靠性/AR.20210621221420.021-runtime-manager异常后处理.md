[TOC]

# AR.20210621221420.021 runtime-manager异常后处理

## Story 整体设计

### 功能描述
+ 使runtime-manager在收到缩容请求或者异常信号时能够有优雅退出的能力
+ 使worker能够区分出平台错误和用户错误

### 场景分析
+ runtime-manager收到退出信号（如：k8s缩容），优雅退出并返回平台错误。
+ runtime-manager收到异常退出信号（如：panic），优雅退出并返回平台错误。

### 方案详细设计
#### 方案设计
+ runtime-manager异常后处理

![AR.20210621221420.021-runtime-manager异常后处理](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/zXm?t=1628820975174)

1. runtime-manager收到退出信号，向worker发送request，查看runtime中是否有未完成的请求。
   
   直至runtime中执行的请求数量为0，返回信息至runtime-manager，超时时间为30秒。
2. worker感知与runtime-manager链路断开，置连接状态为false。worker与runtime的连接也断开。
3. worker向runtime-manager请求异常原因，请求时感知连接状态为false
4. 返回平台错误信息

+ 平台错误码：161906 错误信息：runtime-manager process exited

#### 外部交互分析&&上下游依赖需求
无

### 对外接口
无

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  
不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  
不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

#### 可用性 Availability
*处于按要求执行状态的能力*  
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  
不涉及

## LLT 用例
|  序号 | 预置条件 | 预期结果 |
| ------------ | ------------ | ------------ |
|  1 | 调用函数过程中 kill掉runtime-manager进程 未超过30秒| 正常返回执行结果 pod 重启 |
|  2 | 调用函数过程中 kill掉runtime-manager进程 超过30秒| 返回runtime-manager异常信息（平台错误） pod 重启 |
|  3 | 调用函数过程中 kill掉runtime进程 | 返回runtime异常信息 pod 重启 |
