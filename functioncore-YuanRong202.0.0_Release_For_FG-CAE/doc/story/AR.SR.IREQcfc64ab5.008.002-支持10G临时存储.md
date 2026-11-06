[TOC]

# AR.SR.IREQcfc64ab5.008.002-支持10G临时存储

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

**客户需求：**

部分用户的业务场景，对其可写的磁盘空间有诉求。当前所有函数实例都限制了用户可写空间为512M，不能满足用户的差异化诉求，需支持临时存储空间为10G的实例类型。

### 1.2 特性场景分析

(1）场景触发条件及对象：当用户在函数设置页面打开“临时存储扩容”开关后，会将该函数对应的实例都更新，新实例的/tmp目录可写上限为10G；

(2）使用时间及频度：用户配置其高级功能，并触发新实例调度时；

## 2. 特性/功能实现原理

### 2.1 总体方案

#### 2.1.1 设计目标和原理

##### 2.1.1.1 设计目标

- **功能性：**能够根据用户的配置，动态为其调整函数实例的/tmp目录的可用上限大小
- **可靠性：**在满足用户诉求的同时，保证宿主机的磁盘不被打爆
- **性能：**建议沿用pool池起的机制，避免现拉实例对弹性能力的影响
- **成本：**在性能、成本之间做平衡，避免成本飙升
- **平滑升级改造：**现网的Default节点、CA扩容节点有平滑改造的方案

##### 2.1.1.2 设计原理

[/tmp现状](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=562eeb64d27e4b0792e1355b7f3d81d8)![/tmp现状](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3GHL?t=1673059360882)

- 当前EmptyDir没有指定Medium，即默认存储到宿主机kubelet对应的磁盘中（现网为/dev/mapper/vgpaas-kubernetes，一般为50G），SizeLimit不设置（其实这种模式下设置也无效）；同时worker/runtime容器配置的ephemeral-storage上限为2G，故在我们不控制的情况下，/tmp的使用上限为2G；
- worker当前有磁盘监控机制，进一步限制/tmp的使用上限为512M；
- EmptyDir Volume的生命周期跟Pod是完全一致的，若要对其进行修改、新增等，Pod必定重建；



***核心原理：***

**在函数特化时，挑选合适的pool pod，并动态调整磁盘检测上限。**



#### 2.1.2 方案概览

[总体方案](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=af14fd430c02419da2fd6606396e1bf7)![总体方案](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3GHk?t=1673062061102)

1. 管理面通过函数元信息，标注用户是否要开启“10G临时存储”高级特性；
2. 数据面worker-manager监听函数元信息，特化这类函数实例时，通知worker将磁盘检测上限值调整为10G；
3. 引入保护机制，避免把宿主机目录打爆；
4. 提供现网节点改造指导；



### 2.2 模块详细设计

#### 2.2.1 管理面子模块设计

此章节为初步设计，具体待管理面细化。

##### 2.2.1.1 UI交互设计

[UI示例](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=cc38ab218a7e4d51aac9ac44b89f9261)![UI示例](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3GI9?t=1673072684603)

在“高级设置”中新增一个配置项：临时磁盘扩容；并新增对应描述，若打开，/tmp目录可用空间扩展为10G；

**关键点：**

1、配置项名称、描述、UI展示的位置；

2、10G大小是否固定，还是支持可配置；——建议第一阶段先固定，避免下游系统调度等复杂性

3、此配置开关修改后，是否允许实例重建；——需要重建，避免缩小场景丢磁盘数据

##### 2.2.1.2 函数元信息适配

FuncMgmt根据用户函数的配置，将对应的字段新增注入到元ETCD的函数元信息中，在resourceMetaData结构中新增配置项enable_tmp_expansion，具体样式如下：

```
key

/sn/functions/business/yrk/tenant/7e1ad6a6-cc5c-44fa-bd54-25873f72a86a/function/0@base@panchang_mem/version/latest

value

{"funcMetaData":{"name":"0@base@panchang_mem","handler":"testyrstreserved001_mem.handler","runtime":"python3.6","functionUrn":"sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@panchang_mem","description":"","tenantId":"7e1ad6a6-cc5c-44fa-bd54-25873f72a86a","businessId":"yrk","reversedConcurrency":0,"tags":null,"functionVersionUrn":"sn:cn:yrk:7e1ad6a6-cc5c-44fa-bd54-25873f72a86a:function:0@base@panchang_mem:latest","codeSize":387,"codeSha512":"cf2fea4711b9f933e7499819c03098a43bc35de90d6a0d729602a3fba90b5295edae7f897e54ca41336a77eff823b7dc250721be7f04d74bbaf6fcb36397698b","timeout":900,"version":"latest","versionDescription":"latest","deadLetterConfig":"","functionType":"","created":"20221207141010375","revisionId":"20221207141010375","layers":[],"functionUpdateTime":"20221207141010375","latestVersionUpdateTime":"20221207141010375","publishTime":"","func_name":"panchang_mem","domain_id":"7e1ad6a6-cc5c-44fa-bd54-25873f72a86a","project_name":"cn-north-x","service":"base","isStatefulFunction":false,"isBridgeFunction":false,"enable_auth_in_header":false},"s3MetaData":{"appId":"","bucketId":"bucket-test-log1","objectId":"basepanchang_mem-927d38b70a26f1402912d2fd2909aedca969aeba9cb3d7c39610131d915f5b47.zip","bucketUrl":"http://bucket-test-log1.hwcloudtest.cn:18085"},"envMetaData":{"environment":"","envKey":""},"resourceMetaData":{"cpu":400,"memory":256,"enable_dynamic_memory":false,"customResources":"{}","enable_tmp_expansion":true},"instanceMetaData":{"concurrentNum":1,"minInstance":1,"maxInstance":100,"idleMode":false},"extendedMetaData":{"image_name":"latest-210626210608@stxso","role":{"xrole":"","app_xrole":""},"func_vpc":null,"mount_config":null,"strategy_config":{"concurrency":-1,"concurrentNum":1},"extend_config":"","initializer":{"initializer_handler":"","initializer_timeout":0},"enterprise_project_id":"0","log_tank_service":{"logGroupId":"bbc7cffd-4a00-4764-9b38-bb9f1250844f","logStreamId":"7b0fd65a-b0b0-4e1a-af66-579c031d1847"},"tracing_config":{"tracing_ak":"","tracing_sk":""},"custom_container_config":{"control_path":"","image":"","command":null,"args":null,"working_dir":"","uid":0,"gid":0}}}
```

**关键点：**

1、管理面是否只对新修改、新增的key，才增加这个字段；而对存量key不会字段的补充处理？——如此，需要数据面做缺省值填充，默认为false

##### 2.2.1.3 现网平滑改造

此方案上线后，对于Node节点的数据盘、数据盘的子分区/dev/mapper/vgpaas-kubernetes有扩容诉求，需给出现网改造方案指导；

**关键点：**

1、针对存量节点，如Default节点、CA扩容节点，先对其数据盘进行扩容，再通过LVM扩容/dev/mapper/vgpaas-kubernetes分区；——需提供脚本

2、针对新节点，如Default节点、CA扩容节点，需制定数据盘的新规格，并调整当前初始化脚本，使得/dev/mapper/vgpaas-kubernetes分区大小满足期望；——需适配脚本/CA插件

3、数据盘多大、/dev/mapper/vgpaas-kubernetes分区多大，本文2.2.2.1章节会讨论；

4、特别的，异构规格VM，新的数据盘需单独计算；



#### 2.2.2 数据面子模块设计

##### 2.2.2.1 宿主机磁盘保护机制

先展开此部分，一是在当前方案下，保护机制是关键；二是为了业务流程做铺垫；

**保护机制简述：**

- worker-manager维护Node节点的缓存，动态统计节点上的/dev/mapper/vgpaas-kubernetes分区的使用情况，并考虑定时校准；
- 当特化请求到达时时，若enable_tmp_expansion为true，则调用此缓存提供的**过滤查询接口**，得到无磁盘风险的节点列表，再从这批目标节点中挑选Pod进行特化，若指定时间内等不到，则多集群重试兜底；——其实在当前设定下，此保护机制本身被触发的概率很小，再加上多集群兜底，总体可控；
- 实例特化后、消亡时，需要调用此缓存提供的**增减接口**，来刷新对应Node节点的缓存；
- 特别的，当特化请求到达时，若enable_tmp_expansion为false（大部分场景），则不进行上述的调度选择、缓存更新动作，跟之前保持一致；——下面会讲原因，考虑系统复杂性、性能和成本的平衡

*TBD：两个接口的定义*

**保护机制详细设计：**

当前Node节点的数据盘为200G，分配给/dev/mapper/vgpaas-kubernetes分区50G；每个函数Pod  ephemeral-storage request为1G，故每个Node最多容纳50个Pod；

当前Node节点扣除固定3U占用，剩余13U，按128 Pod算，极限情况最多容纳13/0.15=86个；只要有中大规格Pod在，抢占了CPU，也达不到这么多；

综上，可以认定当前的设定也是合理的，即每个16U32G的节点最多容纳**50个**函数Pod。



如此，可以将数据盘分成如下几个部分：

[数据盘变化](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=182443b44f71432385711c18c6a56a8a)![数据盘变化](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3GL9?t=1673079299757)

**关键点：**

- 普通函数（临时存储512M）在特化时，所有节点都满足，因为灰色部分已经提前按照最大值预留了；——牺牲了<50G的成本，换取了系统复杂度、性能
- 特殊函数（临时存储10G）在特化时，需要进行判断，因为这部分只按30个预留，可能有风险；——这部分没有按最大值预留，考虑到节省成本
- 最终单节点（16U32G，高IO，包月）的数据盘从200G=>500G，成本1621=>1726，若10G也按最大值50个预留，数据盘700G，成本1796；**——再决策下，10G预留多少，甚至保护机制先不做了？工作量vs性价比**



**核心流程：**

- worker-manager启动时，构建Node节点缓存，记录节点kubernetes分区的可用容量；特别的，只需要考虑绿色部分，即300G，此上限值可以通过K8S Node事件感知——**Node缓存生成晚于特化请求，需考虑在启动时提前准备好此缓存**；
- 同时，在启动阶段纳管已有函数实例时，根据标签识别出10G函数实例，并更新好缓存；
- 当特殊函数（临时存储10G）在特化时，返回无磁盘风险的节点列表，并在特化选中Pod后，更新其所在节点的缓存；
- 当特殊函数实例释放时，更新其所在节点的缓存；
- 定时校准并更新缓存，避免误差越来越大；



##### 2.2.2.2 特化时动态调整检测阈值

[特化时动态调整检测阈值](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=3e97a221de684db599dfc360226f90ad)![特化时动态调整检测阈值](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3GMl?t=1673081858332)



### 2.3 约束

- 10G大小固定，暂不支持用户自定义；
- 配置开关改变时，已有的函数实例要重建；
- 计费暂不进行设计；
- 本特性先不考虑与异构规格VM的交叉；
- 非pool池起的函数实例暂不支持；



### 2.4 工作量评估

[工作量](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=0015e264627e43a5854fb1915d80bebd)![工作量](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/3GNC?t=1673083083070)



## 3. 可信软件

### 3.1安全性（Security） 

不涉及

### 3.2韧性（Resilience）

worker-manager故障重启后，会根据K8S事件重建Node缓存，作为特化时的选择依据；

### 3.3隐私性（Privacy）

配置项不涉及用户相关敏感数据；

临时存储检测时不打印租户ID等用户敏感信息；

### 3.4可靠性（Reliability）

若本集群的所有节点都有临时存储被打爆的风险，则多集群重试兜底；

worker-manager故障重启，支持缓存重建；

支持缓存定时校准；

### 3.5可用性（Availability）

不涉及

### 3.6安全（Safety）

不涉及

### 3.7可维护性

不涉及

### 3.8可测试性

黑盒测试场景有关键的维测日志；

LLT覆盖率达到85%；

### 3.9性能

10G函数在特化选择前多一步过滤动作，通过缓存实现，相对于其他备选方案，对性能影响最小；

### 3.10资源

复用etcd key，未加重etcd负载；

扩展Node缓存，新增字段即可；

定时校准Node缓存，多一个定时协程，遍历更新时通过Node名称、Pod标签做过滤，避免短时间消耗大量内存；

### 3.11资料

涉及，管理面统一对外提供；

### 3.12升级兼容性

## 4. 用例

| **序号** | **用例名**       | 用例类型 | **测试步骤**                                                 | **预期结果**                                       |
| -------- | ---------------- | -------- | ------------------------------------------------------------ | -------------------------------------------------- |
| 1        | 磁盘上限调整验证 | 手工黑盒 | 函数页面打开“临时磁盘扩容”开关，触发实例生成，手动往/tmp目录写文件 | 其中/tmp目录写10G以上文件，runtime才会被杀掉重启   |
| 2        | 实例重建验证     | 手工黑盒 | 函数页面修改“临时磁盘扩容”开关，观察实例位置                 | 老实例删除，新实例创建，且磁盘上限按最新的配置调整 |
| 3        | 保护机制验证     | 可靠性   | 构造某个Node节点上有大量10G函数，且磁盘分配到达临界的场景，继续下发特化请求 | 实例不在此Node节点上选中                           |
| 4        | 兜底机制验证     | 可靠性   | 构造某个集群中所有Node节点达到临界的场景，继续下发特化请求   | 本集群失败，重试到其他集群                         |
| 5        | 可靠性验证       | 可靠性   | worker-manager重启后，继续下发特化请求                       | 能够恢复缓存，正确的进行保护机制、调度             |
| 6        | 现网改造验证     | 平滑升级 | 根据新的数据盘大小定义，新的现网节点改造步骤、脚本，进行现网改造 | 改造步骤可行、平滑                                 |
|          |                  |          |                                                              |                                                    |

