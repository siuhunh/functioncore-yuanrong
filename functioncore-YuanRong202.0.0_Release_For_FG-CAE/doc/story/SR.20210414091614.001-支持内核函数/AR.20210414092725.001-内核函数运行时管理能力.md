[TOC]

# 内核函数运行时管理能力

## Story 整体设计

### 功能描述
一句话描述：Kernel-function-manager提供管理kernel runtime管理能力，预部署每个节点n个kernel runtime，提供新增和删除运行时的接口（扩缩容功能），由kernelinvoker去触发调用

kernel-function-manager有部分功能与node-manager类似，它的目的是为了内核函数提供worker/runtime的生产者，初始化启动的时候就按照配置，在每个节点上预先启动n个kernel worker/runtime

kernel-function-manager作为kernel worker/runtime的生产者，由kernel-invoker来使用。

Kernel-invoker会订阅kernel-worker信息，当kernel-invoker发现kernel-worker不够用的，可以向kernel-manager发起扩容命令（本轮AR不做）

 



### 场景分析
*描述用户使用场景，尽量使用用例图、交互图等描述*


### 方案详细设计

#### 现状分析
*分析业界类似需求的实现方案及其优缺点，或者当前发布包现状。如没有可不补充。*

#### 方案设计
*详细的方案设计，尽量适用时序图、类图等描述*

##### 示意图

[示意图](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=0c887f8e8c8345bc89b5d61364d24bb3&versionUUID=）

#####  Kernel-init 容器

Kernel-init容器与worker-init的区别是权限，worker-init会限制worker/runtime的网络访问权限，但在kernel-init中不做此限制。

##### 节点信息来源

订阅k8s的node信息变更，通过create/delete/update事件来驱动kernel-worker的创建。

##### Kernel-woreker 初始化启动

配置信息示例，当前kernel-worker的runtime只支持cpp11，配置保留。上一轮AR实现的由deployment拉起kernel-worker的方式就取消了，通过k8s的deployment以及pod的API来代替。

 Kernel-manager与node-manager的区别为：node-manager指定每个池子的大小，实例的分布由k8s调度，而kernel-manager则定义的是每个node节点起多少实例。
```
"name": "kernel-pool1",
"cpu": 500,
"mem": 500,
"runtimename": "cpp11",
"kernelsizepernode": 1,   // 表示每个节点上起1个实例
"annotations": {
    "app": "kernel-pool1"
    },
    "label": {
        "app": "kernel-pool1"
    }},
```
#### 扩缩容

 扩缩容由kernel-invoke来触发，当kernel-invoker发现现有的实例数不够时触发kernel-function-manager的扩容

 本轮AR不实现

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
