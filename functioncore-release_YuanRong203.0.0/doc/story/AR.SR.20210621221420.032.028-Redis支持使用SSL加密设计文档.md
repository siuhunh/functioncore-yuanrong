[TOC]

# Redis 支持使用 SSL 加密设计文档

关联AR:
+ AR.SR.20210621221420.032.028 

## Story 整体设计

### 功能描述

1. 需求目的：建立 Redis 客户端与服务端之间， Redis 集群间的安全通信通道。
2. 实现方式：通过 stunnel 软件实现对通信通道的安全加密。

### 场景分析

当前使用的华为云 Redis，通过华为云官方文档了解，并不支持 SSL ，实现 SSL 加密通信是依赖 stunnel 实现的。

[华为云 Reids3.0 公网连接参考](https://support.huaweicloud.com/usermanual-dcs/dcs-ug-0513002.html)

华为云 DCS 介绍 Redis 版本差异描述：4.0/5.0/6.0 不支持公网访问，所以需要确认这种方式stunnel是否可行。

[Redis 版本差异](https://support.huaweicloud.com/productdesc-dcs/RedisDifference.html)

**Redis建链访问模型**

![Redis建链访问模型](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/zCz?t=1628577574361)


### 方案详细设计

#### 现状分析
参考华为云Redis文档的实现方式，使用 stunnel 开启 SSL。

#### 方案设计
1、当前元戎数据面的 Redis 提供 TLS 加密通信实现，可以实现 HTTPS 的通信。

2、输出 stunnel 搭建的指导文档，不涉及代码层级的适配与修改

#### 开源软件选型

1. 代码层级不引入开源依赖。

2. 部署层级引入 stunnel。

   原因：

   1. stunnel 当前是较为成熟的 TCP 通道加密通信实现的软件，使用简单，搭建快速。
   2. stunnel 不需要对代码做适配，可以直接实现通道的加密。
   3. 当前 FunctionGraph-Yuanrong 融合版本使用的 Redis 为华为云 Redis，根据官网提供指导，可使用 stunnel 进行通道加密。
   4. 版本引入 stunnel4， 参照华为云 Redis 官网指导样例选用。

#### 外部交互分析&&上下游依赖需求
需要协调使用开发环境 Redis，进行 stunnel 搭建测试。

### 对外接口
不涉及

### 约束
不涉及


## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*  

不涉及

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  

不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  

不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*  

不涉及

#### 可用性 Availability
*处于按要求执行状态的能力*  

不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  

不涉及

## LLT 用例
1. 改用 stunnel 方式访问，能成功访问 Reids-server 即可。
2. 环境 搭建 stunnel 方式后，数据能成功写入 Reids。

