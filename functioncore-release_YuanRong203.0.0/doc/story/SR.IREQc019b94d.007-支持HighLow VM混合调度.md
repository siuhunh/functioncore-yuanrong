[TOC]

# SR.IREQc019b94d.007-支持High/Low VM混合调度

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

需求来源于降成本专项。通过Profiler进行观测，识别到现网集群的CPU分配率高，但使用率极低。同时，大部分用户函数并非CPU密集型，可以通过适当的超分策略来提升集群CPU使用率，降低平台侧成本。

主要包含3个AR：

- AR.SR.IREQc019b94d.007.001  支持异构资源池的生命周期管理
- AR.SR.IREQc019b94d.007.002  支持区分函数进行异构调度
- AR.SR.IREQc019b94d.007.003  混合调度能力增强

### 1.2 特性场景分析

(1）场景触发条件及对象：当打开异构调度开关后，worker-manager会创建对应的异构资源池，并在响应实例扩容请求时，根据函数画像信息，进行差异化调度；

(2）使用时间及频度：worker-manager进行函数实例调度时触发；

## 2. 特性/功能实现原理

### 2.1 总体方案

#### 2.1.1 设计目标和原理

##### 2.1.1.1 设计目标

- 降成本：集群CPU使用率提升1.5倍，平台成本再降低20%
- 可靠性：在用户可接受的时间范围内，函数实例调度均可正常执行完成；在异常场景下，支持兜底方案
- 性能：用户函数执行时延不会出现较大的抖动
- 平滑升级：异构调度能力支持独立、平滑上线

##### 2.1.1.2 设计原理

- 引入不同CPU、内存配比的node节点，例如1:2的通用计算增强型（high-end）、1:4的通用计算增强型（low-end）等异构资源；
- 通过对低端机器上函数Pod的CPU request进行压缩砍半，提升低端机器上函数的部署密度，并保证CPU、内存同步分配；
- 将**CPU密集型函数**调度到高端机器上，将**其他类型函数**优先调度到低端机器上，通过这种异构资源池组合，降低平台侧总成本；
- **注：**当前方案中，高低端机器的区别在于CPU、内存配比不同；节点类型均为通用计算增强型，主要是担心其余廉价机型的性能不稳定；



**异构资源的选取策略建议：（c6s、c7系列可以候补）**

- ***high-end机器：***通用计算增强型c6.4xlarge.2（16U32G，1628/月），或者c6.8xlarge.2（32U64G，3152/月，减少daemonset业务组件消耗）
- ***low-end机器：***通用计算增强型c6.4xlarge.4（16U64G，2104/月），或者c6.8xlarge.4（32U128G，4103/月，减少daemonset业务组件消耗）



#### 2.1.2 方案概览

![end2end](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2zoL?t=1668411041553)

1. **函数级观测能力：**Profiler、worker配合，采集函数在请求调用期间的CPU、内存（已采集）、I/O并上报；
2. **函数模型识别能力：**Smart Warmer提供函数画像能力，标识该函数为CPU密集型、内存占用型、I/O密集型，通知rdispatcher将函数资源模型标注到etcd函数instance元数据中；
3. **异构资源池管理能力：**high-end机器承载的pool池规格不变；low-end机器承载的pool池规格的CPU request减半，理论上其部署密度会翻倍；
4. **异构调度能力：**将高需求函数（CPU密集型）优先调度到high-end机器；其他类型函数（内存占用型、I/O密集型等）优先调度到low-end机器；



#### 2.1.3 落地节奏

**2022.12：**异构资源池管理、异构调度能力Ready，支持独立、平滑上线，在乌兰环境进行小范围试水；

**2022.12~2023Q1：**函数级观测能力、函数模型识别能力、CA插件配合、调度调节增强能力逐步Ready，端到端打通；



### 2.2 模块详细设计

#### 2.2.1 异构资源池管理子模块设计

##### 2.2.1.1 异构资源池初始化

[异构资源池管理](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=5830377c2b0d494db6176255e9f58544)![异构资源池管理](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3AGy?t=1670917912885)

1. 已有的16U32G节点打上highend标签，同时在CCE购买16U64G节点并打上lowend标签（后续CA插件支持异构VM后，扩容节点由其自动打标签）

手工打标签的命令：

```
kubectl label nodes worker-8-0-211-104 node-type=lowend

kubectl label nodes worker-8-0-211-137 node-type=highend
```

效果如图所示：

[异构资源池示意图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=923320a4dbb94cb6a9bd80d592da1e65)![异构资源池示意图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3AHB?t=1670918304116)

2. helm包values.yaml中新增了开关，用于控制是否要开启异构调度功能

```
heterogeneousSchedulingConfig:
  enable: false
  strategy: force
```

- 其中enable代表是否要开启异构调度功能，若开启，会生成异构资源池；
- strategy代表开启异构调度功能后，是否要采取默认强制调度策略，万一遇到异常情况，通过将其设置为force来将所有新实例默认调度至highend机器，此项配置支持热加载；

3. worker-manager启动时，会根据helm包中对应的配置，来决定是否创建异构池子；出于平滑升级的考虑，会将原先的池子自动清理

##### 2.2.1.2 异构资源池伸缩

1. **异构资源池基准值设定&调整：**前期采取人工估算、定期check的方式，后续对接smart warmer的资源池级别预测能力进行自动化调节；
2. **异构资源池弹性伸缩：**两个池子在物理/逻辑上均区分管理，复用已有的pool池auto scaling能力即可，但是在资源池扩容时需差异化感知异构节点的资源分配率；
3. **资源池支持从0=>非0跳变：**当前大规格池子需预留的1个实例，为了避免异构池子的引入导致大规格池子的资源碎片翻倍，需支持这种跳变的管理；当前0代表资源池禁用，无法伸缩；



#### 2.2.2 异构调度子模块设计

[异构调度流程图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=f8b2c5cfec614acaafdb85b8ebeef216)![异构调度流程图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3AJt?t=1670921545945)

##### 2.2.2.1 函数模型识别&标注

目标方案是Smart Warmer根据Profiler的函数实例级观测数据进行模型学习，并将得到的画像信息通过rdispatcher写入etcd函数instance元数据中；当前先通过人工观测、标注的方式。

扩展后的instance key/value如下所示：

```
key

/instances/business/yrk/cluster/cluster001/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@panchang_mem/version/latest

value

{"instanceMetaData": {"minInstance": 20, "maxInstance": 100, "instanceType": "", "idleMode": false}, "profileMetaData":{"resourceModel":{"isCPUBound":false,"isMemOccupied":true,"isIOBound":false}, "isInit":true}}
```

##### 2.2.2.2 异构调度策略

- worker-manager收到scale up请求时，通过函数元信息中的函数模型字段，来决策将函数实例调度到high-end或low-end机器上，当前决策依据是isCPUBound是否为true；
- 非CPU密集型函数优先调度至low-end机器，在短时间内无法获取到可用Pod资源时，也允许兜底调度至high-end机器；但CPU密集型函数不允许兜底调度至low-end机器；
- worker-manager监听到函数元信息中的函数模型字段发生改变，只更新本地缓存，不改变当前已有函数实例的位置，避免频繁迁移对系统造成负担，后续扩容请求使用最新的模型数据作为调度依据；
- 未被预测的函数（isInit为false）、非池起的函数Pod（自定义容器镜像、checkpoint Pod）暂时缺省调度至high-end机器；
- 一旦出现问题，通过热加载开关的方式（上文提到的strategy: force）将所有扩容请求均调度至高端机器，同时恢复高端池子配比，即可回退至当前方案；



### 2.3 约束

#### 2.3.1 上下游依赖需求

1230 异构调度能力支持独立上线，整体方案中的其他依赖项在2023Q1逐步落地，主要有：

- 函数级观测&采集能力：worker、Profiler
- 函数模型识别能力：Smart Warmer（支持batch、优先级能力，控制同一时段的模型变化频率）
- 异构场景下的节点级伸缩能力：CA插件
- 异构资源池大小预测能力：Smart Warmer

#### 2.3.2 其他约束

- 非池起的函数Pod（自定义容器镜像、checkpoint Pod）暂时不支持异构调度，默认调度至high-end机器；
- 函数模型变化后，暂不支持对已存在的函数实例进行干预、迁移；



## 3. 可信软件

### 3.1安全性（Security） 

不涉及

### 3.2韧性（Resilience）

worker-manager故障重启后，会根据etcd数据重新恢复本地缓存的函数模型数据，作为调度依据；

### 3.3隐私性（Privacy）

配置项不涉及用户相关敏感数据；

异构调度时不打印租户ID等用户敏感信息；

### 3.4可靠性（Reliability）

低端机器资源不足时，支持非CPU函数兜底调度至高端机器；

遇到预期外故障时，可通过热加载配置项，将后续所有请求默认调度至高端机器；

### 3.5可用性（Availability）

不涉及

### 3.6安全（Safety）

不涉及

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

异构调度支持热加载配置项，当现网出问题时，可以关闭，快速恢复业务；

### 3.8可测试性

黑盒测试场景有关键的维测日志；

LLT覆盖率达到85%；

### 3.9性能

待测试过程中进一步摸底，主要涉及：

- 低端机器上的Pod弹性能力；
- 低端机器上用户函数的执行时延；

### 3.10资源

复用etcd key，未加重etcd负载；

资源池数目翻倍，新增约15个资源池监控协程；

### 3.11资料

内部成本优化需求，不对用户呈现；

### 3.12升级兼容性

支持独立、平滑上线，通过开关控制；

## 4. 用例

| **序号** | **用例名**                                                   | 用例类型 | **测试步骤** | **预期结果** |
| -------- | ------------------------------------------------------------ | -------- | ------------ | ------------ |
| 1        | 异构资源池初始化（开关打开/关闭）                            | 手工黑盒 |              |              |
| 2        | 异构调度（正常/兜底/开关关闭后默认）                         | 手工黑盒 |              |              |
| 3        | 异构资源池独立伸缩（0=>1跳变/感知异构节点分配率决定是否扩容） | 手工黑盒 |              |              |
| 4        | 平滑上线方案（开关打开/关闭，资源池自动清理等）              | 手工黑盒 |              |              |
| 5        | 交叉点测试（smart warmer预测池中的实例和异构调度联动）       | 手工黑盒 |              |              |
| 6        | 性能摸底（弹性速度/调度速度/用户函数执行速度）               | 性能专项 |              |              |
|          |                                                              |          |              |              |

