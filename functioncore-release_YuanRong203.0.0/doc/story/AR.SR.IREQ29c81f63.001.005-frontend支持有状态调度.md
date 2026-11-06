[TOC]

# frontend支持有状态调度

AR：AR.SR.IREQ29c81f63.001.005 

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

### 1.2特性场景分析

当前有状态调度支持函数间调度，具体场景如下：

函数间调用链路：

场景一：通过Create Function时直接绑定状态ID进行调用。

[有状态函数间调用模型-create-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=8ba80f5a6b094771b87251c4173ecf3d&versionUUID=)![有状态函数间调用模型-create](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3Bpp?t=1671449548023)

场景二：Create Function对象时，不绑定状态ID，通过getInstance去绑定状态ID进行调度。

[有状态函数间调用-getInstance-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=855e016764e54c72aeb47f09892408c3&versionUUID=)![有状态函数间调用-getInstance](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3Bpz?t=1671449941424)

基于当前有状态架构特性，实现frontend通过指定 instanceID，instanceName(stateID，stateKey) 进行有状态调度。

## 2. 特性/功能实现原理

### 2.1总体方案

函数间调用使用new Function接口创建有状态函数的状态实例路由。而后可通过两种方式进行有状态函数直接调度。

1. 指定instanceName调度。

2. 指定instanceID调度。

   instanceID获取需要通过Function对象，使用getInstanceID查询返回得到结果。

分别通过header进行指定：

```go
X-Instance-Name: instanceName
X-Instance-ID: instanceID
```

[frontend支持有状态调度-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=291cd1bb8dc24d9c8c699b7b33b328b3&versionUUID=)![frontend支持有状态调度](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3BrM?t=1671452513383)

**特殊情况**

**同时携带instanceName和instanceID进行调度，以instanceID为准，instanceName不感知。**

**错误信息修改**

当前：

```go
4026:instanceName is not exist
```

修改后：

```go
4026: the instance name (%s) or instance id (%s) does not exist
```

### 2.2模块详细设计

#### 2.2.1XXX子模块设计

##### 2.2.1.1模块实现

##### 2.2.1.2模块对外接口定义

##### 2.2.1.3模块类设计

### 2.3 约束

#### 2.3.1上下游依赖需求

1. 网络打通。

   由于frontend查询状态路由信息，调度到状态机时，可能跨集群。因此集群机器需要将通信端口**22423**对vpc内全放通。

2. header白名单开放。

   rdispatcher会过滤header下放请求，非白名单内的header会被过滤丢弃。

   因此，rdispatcher需要对上述两个header进行放通。

   ```go
   X-Instance-Name: instanceName
   X-Instance-ID: instanceID
   ```

   

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

不涉及。

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

不涉及

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及用户敏感数据。

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

1. 状态路由信息涉及到etcd查询，需要引用singleflight，防止并发请求下缓存击穿问题。
2. 与原有函数间调用相比，新增功能支持通过frontend直接路由到状态机，减少了请求链路，对可靠性有一定增强。

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

不涉及。

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

不涉及。

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

不涉及

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

LLT：单一场景下均可mock场景进行测试。

功能场景较为简单，可测试性较高。

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

新增能力属于基本调度流程上的能力，对性能本身影响可控。

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

不涉及新增资源占用。

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

1. 新增端到端调度有状态，可以指定header传输，需要作资料补充。
2. 同时指定instanceName和instanceID进行调度，只以instanceID为准，需要进行说明。

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

兼容。

新增功能，与老版本无耦合。

## 4. 用例

| **序号** | **用例名**                                               | 用例类型 | **测试步骤**                                                 | **预期结果**                                                 |
| -------- | -------------------------------------------------------- | -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1        | singleflight缓存击穿防护                                 | LLT      | 1. 无缓存下构造多次并发调度。<br>2. mock缓存刷新时间1s。<br>3. 记录缓存刷新次数。 | 缓存只刷新一次                                               |
| 2        | 通过instanceID进行调用，调用成功                         | 功能用例 | 1. 指定instanceName1创建状态实例，通过getInstanceID接口获取到状态ID。<br>2. 通过获取到的instanceID进行调度。 | 调用成功                                                     |
| 3        | 通过instanceName进行调用，调用成功                       | 功能用例 | 1. 指定instanceName1创建状态实例。<br>2. 通过instanceName1进行调度。 | 调用成功                                                     |
| 4        | 同时指定instanceID和instanceName进行调度，使用instanceID | 功能用例 | 1. 指定instanceName1创建状态实例，通过getInstanceID接口获取到状态ID，调用有状态函数返回实例名。<br>2.端到端通过指定instanceID和instanceName2调用有状态函数，观察返回。 | 返回实例名一致                                               |
| 5        | 使用未创建的instanceName/instanceID进行调度，返回错误    | 异常用例 | 1. instanceName/instanceID未创建下，使用instanceName/instanceID调度有状态函数 | 返回报错： the instanceName %s or instanceID %s does not exist |

