[TOC]

# 函数支持公网访问控制Story设计说明书

关联AR:

+ ARID: AR.20210522184837.005

## 1. Story 整体设计

### 1.1 功能描述

**需求**：在配置了VPC场景下屏蔽公网只访问VPC网络

**作用**：在配置了VPC场景下屏蔽公网只访问VPC网络

**约束**：

### 1.2  场景分析

当前未配置VPC的函数默认可以直接访问公网，配置了VPC的函数默认只能访问当前VPC网络和公有云租户区网络。函数实例可通过对接patService来加入一个VPC网络，在该场景下应重新配置router和iptables使worker的流量通往VPC。

### 1.3方案详细设计

当前worker Pod防火墙规则如下：

```shell
-A OUTPUT -d 10.43.0.0/16 -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW -j DROP
-A OUTPUT -d 10.42.0.0/16 -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW -j DROP
-A OUTPUT -d 10.243.0.0/16 -p tcp -m tcp --tcp-flags SYN SYN -m state --state NEW -j DROP
-A OUTPUT -d 10.43.0.0/16 -j DROP
-A OUTPUT -d 10.42.0.0/16 -j DROP
-A OUTPUT -d 10.243.0.0/16 -j DROP
```

总共屏蔽了kubernetes pod网段，service网段，节点网段三块地址。

在vpc场景worker默认路由会设置为vpc网关，同时kubernetes pod及service网段会配置kubernetes网关，如此服务相关流量会走kubernetes路由，其余所有流量（包括访问外网的流量）会走vpc路由受vpc网络管理

```shell
Destination     Gateway         Genmask         Flags Metric Ref    Use Iface
0.0.0.0         172.16.0.1      0.0.0.0         UG    0      0        0 eth0
10.43.0.0       10.42.0.1       255.255.0.0     UG    0      0        0 eth0
10.42.0.0       10.42.0.1       255.255.0.0     UG    0      0        0 eth0
```
同时在防火墙规则上对VPC内的敏感网段进行限制
```shell
-A OUTPUT -d 169.254.0.0/16 -j DROP
```

## 2.可信软件

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

## 3.HLT 用例
|序号|测试用例|测试步骤|预期结果|
| :---: | :---: | :---: | :---: |
|1| 不配置VPC网络测试外网访问 |worker特化时不指定使用patservice，在特化之后进行外网访问|可以访问外网|
|2| 配置VPC网络后测试外网访问 |worker特化时指定使用patservice，在特化之后进行外网访问|外网访问能力与VPC本身的外网访问能力一致|
