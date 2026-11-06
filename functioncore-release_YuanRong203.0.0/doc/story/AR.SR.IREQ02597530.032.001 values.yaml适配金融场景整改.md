[TOC]

# values.yaml适配金融场景整改

关联AR:

+ ARID1: AR.SR.IREQ02597530.032.001

## Story 整体设计

### 功能描述
1.命名格式统一 

2.obs bucketid当前是写死的默认值，要在values.yaml中集成配置 

3.redis、etcd密码以及配置模板化，读取value.yaml填值

4.helm etcd数据自动刷新，增加到helm部署包中

### 场景分析
金融风控实时管控服务，为简化用户配置，提升用户体验，对values.yaml进行整改。


### 方案详细设计

#### 现状分析
1. 命名格式包含小驼峰、大驼峰、下划线、全大写、全小写多种格式。
2. obs bucketid是写死的默认值，不支持在values.yaml里统一配置。
3. redis是写死的默认值，etcd的用户与密码是调用脚本传入的明文，不支持在values.yaml里统一配置。
4. etcd配置数据刷新依赖脚本部署，需要额外的手动操作。

#### 方案设计

##### 命名格式统一 

整改金融对外暴露的配置项，统一为小驼峰格式。

##### obs bucketid当前是写死的默认值，要在values.yaml中集成配置

在values.yaml中集成配置obs bucketid，增加配置项bucketId。

##### redis、etcd密码以及配置模板化，读取value.yaml填值

在values.yaml中集成配置redis密码、etcd用户与密码，增加三个配置项。

##### helm etcd数据自动刷新，增加到helm部署包中

1. 整体流程

   + iam增加init-container容器，用于部署时自动刷新etcd配置数据。
   + 新增镜像iam-init，从公司开源仓引入可信第三方工具etcdctl。
   + iam-init拉起时从configmap读出etcd用户与密码，刷新etcd数据。

2. 镜像制作

   类似worker的，将镜像制作分为两步，将第三方组件与自有组件分开：

   + 增加基础镜像iam-adaptor-init:1.0， 基于ubuntu/euleros打包公司可信版本etcdctl
   + functioncore整体镜像制作流程中，在基础镜像基础上打包iam-init.sh、crypto_tool工具制作出iam-init镜像

3. iam-init工作流程图

![iam-init工作流程](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/xy2?t=1627719009589)

#### 开源软件选型
NA

#### 外部交互分析&&上下游依赖需求
NA

### 对外接口
NA

### 约束
NA


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
| 预置条件           | 测试步骤                                                     | 预期结果                                           |
| ------------------ | ------------------------------------------------------------ | -------------------------------------------------- |
| 第三方组件部署完成 | 不执行init-iam-data.sh，直接helm部署functioncore             | 部署成功                                           |
| 第三方组件部署完成 | 1. 修改values.yaml中bucketId字段后部署<br>2. deploy、invoke函数后查询etcd数据 | 1. 部署成功<br>2. etcd数据与第一步修改的字段一致。 |
| 第三方组件部署完成 | 1. value.yaml配置错误的redis、etcd密码密文，部署functioncore<br>2. value.yaml配置正确的redis、etcd密码密文，部署functioncore | 1. 部署失败<br>2. 部署成功                         |
| 无                 | 查询values.yaml。                                            | 命名符合小驼峰格式                                 |

