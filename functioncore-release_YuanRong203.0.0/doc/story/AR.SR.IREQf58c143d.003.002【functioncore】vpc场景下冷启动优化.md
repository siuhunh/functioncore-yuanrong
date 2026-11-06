[TOC]

# 【functioncore】vpc场景下冷启动优化 

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

worker冷启动优化，加快用户vpc场景下worker的函数部署、VPC配置速度

### 1.2特性场景分析

#### 1.2.1 场景

worker的 函数加载 和 VPC初始化，在loadFunction()内串行执行

1. 函数加载：由deploy模块走s3协议，向obs和cache-manager加载目标函数

2. VPC初始化：通过changeNetWork() 修改网络，配置VPC相关内容

   ![VPC冷启动--原逻辑](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2g6X?t=1663298482511)

#### 1.2.2 优化点

1. 并发 代码加载、VPC配置；

2. 将route del相关操作放在worker优雅退出时执行。VPC配置的route add前，都需要route del来避免上一次VPC的影响；

   ```go
   func buildIPTunnelCmd(commandList []*command, tunnelName string,
   	funcContainerIP string, patContainerIP string) []*command {
   	commandList = append("ip tunnel del %s", ...) // 优雅退出时del，启动过程可减免该操作
   	commandList = append("ip tunnel add %s mode ipip local %s remote %s", ...)
   	commandList = append("ip link set %s up", ...)
   	return commandList
   }
   ```

#### 1.2.3 影响分析

对于 *1.2.2 优化点*  的 *1.* ，需重点考虑VPC配置对网络路由的修改后对代码加载的影响：

1. worker <-> cache-manager ：可能有影响
   - VPC之前：worker与cache-manager的通信经默认路由完成
   - VPC之后：默认路由变更，可能无法连接到cache-manager；
2. worker <-> obs ：无影响
   - VPC之前：走默认路由访问，obs的地址为100.79.80.29:80 (开发01)
   - VPC之后：走新增路由访问，VPC操作管理面将100.64.0.0/10网段(包含obs地址)信息传给worker-manager的vpcmanager添加至NatSubnetList["exclude"] 传给worker，VPC配置后新增包含obs通路的路由信息，访问obs由默认路由改为新增的100.64.0.0/10路由。  (注：已验证，且和管理面确认所有VPC操作均能保证包含obs通路的信息通知)

```shell
# VPC之前
Destination     Gateway         Genmask         Flags Metric Ref    Use Iface
0.0.0.0         10.0.0.17       0.0.0.0         UG    0      0        0 eth0    # 默认路由
10.0.0.0        0.0.0.0         255.255.0.0     U     0      0        0 eth0
169.254.169.254 10.0.0.17       255.255.255.255 UGH   0      0        0 eth0
192.168.0.0     10.0.0.17       255.255.0.0     UG    0      0        0 eth0

# VPC之后
Destination     Gateway         Genmask         Flags Metric Ref    Use Iface
0.0.0.0         10.0.0.202      0.0.0.0         UG    0      0        0 tunl_fgs_vpc # 默认路由
10.0.0.0        0.0.0.0         255.255.0.0     U     0      0        0 eth0
10.247.3.10     10.0.0.17       255.255.255.255 UGH   0      0        0 eth0
100.64.0.0      10.0.0.17       255.192.0.0     UG    0      0        0 eth0 # 包含obs通路的路由
169.254.169.254 10.0.0.17       255.255.255.255 UGH   0      0        0 eth0
192.168.0.0     10.0.0.17       255.255.0.0     UG    0      0        0 eth0
```

VPC配置存在操作对默认路由的变更，若在deploy 执行过程中修改VPC则有可能导致cache-manager访问超时：

1. 不修改默认路由  --- 无影响，情况不存在
2. 修改默认路由      --- 有影响
3. 删除默认路由      --- 有影响

```go
// 当前VPC配置对默认路由的操作
commandList = append("ip route del default")
switch tunnelName {
case funcTunnelName:
	if needEndpoint {
		commandList = append("ip route add %s via %s dev %s onlink", ...)
	} else {
		commandList = append("ip route add default via %s dev %s onlink", ...)
	}
case endpointTunnelName:
	commandList = append("ip route add default via %s dev %s onlink", ...)
default:
	// nothing to do
}
```



## 2. 特性/功能实现原理

### 2.1总体方案

若有VPC配置 则 规避cache-manager<->worker 走默认路由，增加集群的服务网段svcCidr路由配置。

1. worker-manager将集群的服务网段信息传给worker

2. worker判断若需要VPC，新增集群的服务网段路由信息，替代cache-manager原有的默认路由

   ```shell
   ip route add {svcCidr} dev eth0
   ```

3. **将原有 函数加载 和 VPC初始化 并发执行**

4. **worker优雅退出时，新增非默认路由的VPC配置进行删除**

   

![VPC冷启动--现有逻辑](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2g6g?t=1663298696990)



### 2.2模块详细设计

*各模块的具体实现，包括模块的模块实现、对外接口定义、模块类设计*

#### 2.2.1 子模块设计

##### 2.2.1.1模块实现

**网段信息获取**

1. worker-manager可以获取到SVC_CIDR字段信息
2. worker-manager将服务网段信息传给worker，特化过程中增加该信息传输保存至worker

**deploy并发变更**

旧逻辑：

```go
func (s *Server) deploy(ctx context.Context, net network.NetWork, deployMessageChan chan *deploy.Message) {
    // 从obs和cache-manager获取代码
	d, err := s.deployer.Deploy(s.conf)
	...
    
	// VPC操作
	if err = changeNetWork(ctx, net); err != nil {
    ...
```

新逻辑：

```go
func (s *Server) deploy(ctx context.Context, net network.NetWork, deployMessageChan chan *deploy.Message) {
    // 若需要VPC操作，新增集群的容器网段、服务网段路由信息
    if isNeedVPC() {
        addSvcCidrRoute()
    	addPodCidrRoute()
    }
    // go协程，从obs和cache-manager获取代码
    go func() {
		d, err := s.deployer.Deploy(s.conf)
		...
    }()
    // 并发VPC操作
	if err = changeNetWork(ctx, net); err != nil {
    ...
```

#####

### 2.3 约束

#### 2.3.1上下游依赖需求

不涉及

#### 2.3.2其他约束

不涉及

## 3. 可信软件

### 3.1安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

原有安全机制可以覆盖，特性不涉及安全性变动

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

不涉及

### 3.3隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
VPC配置和deploy 操作并行改动，不涉及客户信息变更。

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

1. VPC配置网段，管理面会判断且避开集群的容器、服务网段
2. 增加集群的容器、服务网段路由，避免VPC对默认路由更改后影响集群内的链接

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

1. LLT：模拟VPC操作导致加载代码失败，测试用例可具有稳定性。
2. 测试：比对优化前后VPC场景下，冷启动速度提升。

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

耗时：在VPC场景下，对函数加载前到VPC完成后打点，观测耗时减少量：

1. 从cache-manager成功获取
2. 从obs成功获取

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

1. 新增1个协程
2. cpu同3.9耗时性能

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

客户不感知

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

兼容之前的版本，支持组件独立升级。内部特性，组件外不感知。

## 4. 用例

| **序号** | **用例名**        | 用例类型 | **测试步骤**                          | **预期结果**                                       |
| -------- | ----------------- | -------- | ------------------------------------- | -------------------------------------------------- |
| 1        | Test_deploy       | LLT      | 在加载代码时，完成VPC对默认网卡的修改 | 代码加载成功；默认路由修改成功；新增了服务网段路由 |
| 2        | VPC场景冷启动测试 | ST       | 新增函数，测试VPC场景下冷启动时间     | 启动速度提升XX%                                    |