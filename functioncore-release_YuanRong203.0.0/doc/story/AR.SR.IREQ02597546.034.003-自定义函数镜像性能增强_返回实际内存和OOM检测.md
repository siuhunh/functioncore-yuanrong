[TOC]

# 自定义函数镜像性能增强之返回实际内存和OOM检测

## Story 整体设计

### 功能描述
自定义函数镜像场景中，每次函数调用都会返回此时 pod 的实际内存使用情况。当 runtime 容器出现 OOM 时，函数调用请求返回 OOM 相关的错误提示。

### 场景分析
该需求是为了让用户知晓当前的内存使用情况，并且函数使用超过限制的内存后，上层调用者获得oom的错误信息。

### 方案详细设计

#### 方案设计
自定义函数镜像场景中， runtime 由用户的镜像启动，没有 runtime-manager 可监听 runtime 进程，因此通过 volume 文件挂载的方式把内存文件挂载在 worker下，由 worker 查看 runtime 的内存使用情况。

文件挂载：
1. runtime 的内存记录文件 memory.stat 在 /sys/fs/cgroup/memory/kubepods/burstable/pod{POD_UID}/{runtime_containerID} 路径下，
把/sys/fs/cgroup/memory/kubepods/burstable/pod{POD_UID}文件夹挂载在worker的 /runtime/memory 路径下。memory.stat 中的 rss 字段为当前内存实际使用量。
2. {POD_UID}环境变量由worker的Downward API可得；runtime_containerID由workermanager特化时传递给worker

内存查询：

worker 每 2ms 查询一次内存使用情况，缓存在变量 runtimeMemoryUsage 中， 每次请求调用时访问这个值。如果函数调用时间比较长，返回这个过程中内存使用最大值。

OOM检测：

当 runtime 发生 OOM 异常后，通过主动查询的方式感知异常返回给用户。
 
1. worker 与 runtime 连接异常中断后， worker 向 runtime 发送请求失败。
2. worker 主动查询宿主机的系统日志最后100条（使用dmesg查询）中有对应 runtime 的 Memory cgroup out of memory: Kill process的日志信息
3. worker标记runtimeError为RuntimeMemoryExceedLimit，错误码4018，错误信息runtime memory limit exceeded 
4. worker进程退出

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
*说清楚预置条件、输入（构造步骤、构造内容）、输出（预期结果）*

