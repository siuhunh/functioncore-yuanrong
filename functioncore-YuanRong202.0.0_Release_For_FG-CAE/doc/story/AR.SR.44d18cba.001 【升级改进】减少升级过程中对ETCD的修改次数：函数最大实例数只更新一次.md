[TOC]
# AR.SR.44d18cba.001 【升级改进】减少升级过程中对ETCD的修改次数：函数最大实例数只更新一次

## Story 整体设计

### 功能描述
**背景说明**  
该需求用于切流升级以及集群扩缩容场景。
当前1个函数，会有4个实例信息的key，函数增加，key增加，后期key数量会很多；切流、集群扩缩容时，需要对最大实例信息进行reBalance。集群切出，单个函数涉及reBalance 4个key，切入单个函数涉及reBalance 4个key。集群重均衡时，对etcd操作量巨大；经过本需求改动之后，最大实例信息的控制，Rdispatcher不在每个集群重复写入。修改为数据面workmanage分配实例时，大幅降低etcd中key的数量，以及降低切流时间
附：管理面写入etcd流程以及后续演进
![](./images/1702298161700_image.png)

**功能描述**  
1、数据面监听etcd，获取当前可用集群数量，如果数量变更，刷新缓存，调整每个集群的最大实例数
2、根据集群数量，通过hash算法，算出每个集群分配的最大实例数
3、涉及VPC冲突，需要获得
4、如果管理面的maxInstance为-1时，数据面单个集群限制实例的默认值为100；如果数据面查到的集群为空时，数据面单个集群限制实例的默认值为100
5、数据面最大实例获取失败时，默认可用default是1000，保持以前的现状
6、数据面获取不到可用实例数时，使用初始状态的全部实例数

**用户体验**  
内部架构优化需求，暂不涉及，切流速度会得到显著增加

### 场景分析

### 方案详细设计
1、通过监听etcd的fss_cluster_flow_state字段，获取当前全部集群的状态，获取当前可用的集群数量。当集群发生切流，集群扩缩容的时候，etcd该字段的值发生变化
```go
fss_cluster_flow_state
 
"{\"fgs-yuanrong-az1-01\":{\"state\":\"flowout\",\"state_time\":1689339185},\"fgs-yuanrong-az1-02\":{\"state\":\"running\",\"state_time\":1689340295},\"fgs-yuanrong-az2-01\":{\"state\":\"running\",\"state_time\":1689341525},\"fgs-yuanrong-az2-02\":{\"state\":\"running\",\"state_time\":1689342722}}"
```
状态为running时，代表该集群可调度。 其他状态均不可用。  （特殊备注：grayFlowin时，需要结合灰度策略，判断,当前设置为不可用）
```go
StateRunning            = "running"
StateFlowout            = "flowout"
StateError              = "error"
StateFlowOuting         = "flowouting"     // serviceCm切流进行中
StateFlowOutFinish      = "flowoutFinish"  // serviceCm切流完成
StateFlowIn             = "flowin"
StateFlowIning          = "flowining"      // serviceCm切入进行中
StateFlowInFinish       = "flowinFinish"  // serviceCm切入完成，但是集群功能可能还不可用
StateHealthCheckFlowOuting = "healthcheckflowouting" // 健康检查切流中
StateGrayFlowIn         = "grayFlowin"  // 灰度切流状态
```
2、缓存workerCache中加字段，可用集群数与可用集群列表
```go
functionCache{
    ...
    maxScale int//最大实例数
    availableClusterMount int //函数可用集群数（避免VPC网段冲突）
    availableCluster []string //函数可用集群
    ...
}
```
其中availableClusterMount是availableCluster的长度，作为可用实例数的输入，maxScale作为处理过的单集群最大实例数
3.针对VPC网段冲突问题，rdispatcher会区分VPC函数可以调用的集群，通过etcd传入的/sn/function/{funcUrn}/available/clusters字段，看到函数可用的集群，获取该函数的可用实例数
```go
/sn/function/{funcUrn}/available/clusters
fgs-yuanrong-az1-01,fgs-yuanrong-az1-02,fgs-yuanrong-az2-01,fgs-yuanrong-az2-01
```
4、针对单个函数的实例，更新缓存，使用Rdispatcher的hash策略
当fss_cluster_flow_state字段变化时，更新缓存最大实例数，从元数据/sn/functions中获取总最大实例数n，从fss_cluster_flow_state获取可用集群数量m如果n/m无法整除，基于hash(funcUrn)判断结果是否和当前集群一致，不一致的话取整，一致的话，除不尽的加到该集群上
#### 方案设计


#### 外部交互分析&&上下游依赖需求
不涉及


### 对外接口
无

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*
对原有配置的修改，不涉及安全

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*
对原有配置的修改，不涉及韧性

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*
对原有配置的修改，不涉及隐私


#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*
涉及与etcd连接不通，获取不到预期数据；

#### 可用性 Availability
*处于按要求执行状态的能力*
可用性加强需求，确保Worker-manager可用

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*
不涉及

## LLT 用例
|  序号 | 用例名        | 测试步骤 | 预期结果      |
| ------------ |------------| ------------ |-----------|
| 1 | 验证切流改变单集群最大实例数 | 函数最大实例数默认值为1000，切流查看其前后缓存 | 2个集群切流前500，切流后1000 |
| 2 | 验证切流改变单集群最大实例数 | 函数最大实例数默认值为1000，切流查看其前后缓存 | 2个集群切流后1000，切流完成后500 |
| 3|etcd发生断链，或查到空值 | 函数最大实例数查到空或-1 | 函数最大实例默认值1000 |
| 4|4个集群1000函数实例缩容一个集群 | 函数最大实例数默认值为1000，缩容一个集群 | 2个集群333，1个集群334 |
| 5|4个集群1000函数实例扩一个集群 | 函数最大实例数默认值为1000，扩容一个集群 | 5个集群200 |