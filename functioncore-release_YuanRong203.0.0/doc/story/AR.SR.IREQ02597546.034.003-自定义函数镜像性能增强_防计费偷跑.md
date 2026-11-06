[TOC]

# 自定义函数镜像性能增强之防计费偷跑


## Story 整体设计

### 功能描述

用户编写异步线程的函数时会出现函数调用已经结束但是仍旧占用计算资源的情况，而现有的计费方式只对用户发起调用请求到请求返回这段时间内消耗的资源进行计费。
因此提出防计费偷跑，在函数调用结束后，后台停止runtime用户进程的运行。

### 场景分析

自定义函数镜像场景中， runtime 由用户的镜像启动，没有 runtime-manager 可操作 runtime 进程状态，因此通过 volume 文件挂载的方式把相应的控制文件挂载在worker下来操作 runtime 容器。

通过 CGroup 中的的 freezer 子系统来挂起和恢复 cgroup 中的进程，来实现资源的调度。 freezer 有一个控制文件 freezer.state，将 FROZEN 写入此文件，可以将 runtime 进程挂起，将 THAWED 写入此文件，可以将已挂起的进程恢复。

worker 监听请求数，当请求数为0时把 FROZEN 写入；当 worker 有新请求且当前 runtime 进程为挂起状态，将 THAWED 写入恢复进程。

### 方案详细设计

#### 方案设计

freezer.state 文件挂载方式：
1. runtime 的 freezer.state 文件在 /sys/fs/cgroup/freezer/kubepods/burstable/pod{POD_UID}/{runtime_containerID} 路径下，
把/sys/fs/cgroup/freezer/kubepods/burstable/pod{POD_UID}文件夹挂载在worker的 /runtime/freezer 路径下
2. {POD_UID}环境变量由worker的Downward API可得；runtime_containerID由workermanager特化时传递给worker

挂起、恢复 runtime 进程的处理方式：

worker 监听 runtime 当前的调用请求数，设置进程挂起等待时间，支持可配置，默认 3s。3s 后请求数仍为0挂起 runtime 进程；当新的请求进来时如果 runtime 进程为挂起状态需要先恢复进程。

另：针对最小函数实例的场景，最小函数实例即预留实例不需要防计费偷跑。通过路由etcd中的applier字段是否为"worker-manager“判断是否为预留实例。

#### 开源软件选型
*新引入的开源软件：原因、选型调研情况*

#### 外部交互分析&&上下游依赖需求
*描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系*

### 对外接口
*描述对外新增的接口*

### 约束
*特性约束描述*


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
|  序号 |   用例名  | 测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 | 防用户进程后台运行 |  编写函数，异步运行，在函数执行返回后进入 runtime 容器里面查看进程状态 | runtime 进程为中断停止状态，用户异步线程无法运行 |
| 2 | runtime 进程恢复运行 | 编写函数，在函数执行返回后 runtime 进程中断停止，重新调用该函数 | 函数正常调用，runtime 进程恢复运行状态 |
