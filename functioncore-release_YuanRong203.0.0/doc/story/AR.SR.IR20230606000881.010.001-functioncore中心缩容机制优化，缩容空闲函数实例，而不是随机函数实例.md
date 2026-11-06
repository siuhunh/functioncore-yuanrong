[TOC]

# AR.SR.IR20230606000881.010.001-functioncore中心缩容机制优化，缩容空闲函数实例，而不是随机函数实例

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

**背景**：

1、中心缩容机制太随机，可能导致正在工作的worker退出 

2、中心缩容机制存在问题，可能存在过度缩容、误缩容

![](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/svg/81c6a3694d3e4d68bcf1e083db7fde25)

1、由于原先的缩容机制会强删worker，bus从路由etcd感知到有延迟，仍有请求会发到待删worker，会导致请求失败。

2、由于worker-manager做出判断是结合历史数据看，可能会遇到bus中队列还比较长，worker-manager又结合历史数据判断缩容了。

### 1.2 特性场景分析

(1）延迟删除缩容worker；

(2）使用时间及频度：用户配置其高级功能，并触发新实例调度时；

## 2. 特性/功能实现原理

### 2.1 总体方案

**缩容判断**：

![](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/svg/587d11f203594bfbb7b50a15bead6829)

**缩容数据**

在路由etcd中的数据如下，value值新增`"willDelete":"true"`

```
/sn/workers/business/yrk/tenant/12345678901234561234567890123456/function/0@default@hellonj/version/latest/defaultaz/defaultaz-#-pool22-300-128-fusion-7d8fd76cd9-q64q2
{"ip":"10.42.1.90","port":"8080","status":"ready","p2pPort":"22668","nodeIP":"7.218.84.121","nodePort":"22423","applier":"functiontask-zg4dz","ownerIP":"7.218.84.121","cpu":300,"memory":128,"willDelete":"true"}
```

**各组件处理策略**

Frontend：筛选实例的时候，排查路由etcd带待删标签的worker，然后再随机；

Bus：

1、将带待删标签的worker优先级降低，优先打满未带待删标签的本地worker，利用bus自身60s删除无
流量的worker；

2、上报的metric数据带目前请求队列待处理个数

Worker-manager：

1、触发全局扩容时，给待删worker的路由etcd上打上待删标签，然后放置队列中

2、触发扩容：更新队列中取出函数worker更新其标签，再扩容

3、worker-manager重启时需要再加载一下这个list

4、删除worker时再删除一下这个队列

### 2.2 模块详细设计

#### 2.2.1 数据面子模块设计

### 2.3 约束

### 2.4 工作量评估

## 3. 可信软件

### 3.1安全性（Security） 

不涉及

### 3.2韧性（Resilience）

不涉及

### 3.3隐私性（Privacy）

不涉及

### 3.4可靠性（Reliability）

worker-manager重启需要再更新一下list

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

不涉及

### 3.10资源

不涉及

### 3.11资料

涉及，管理面统一对外提供；

### 3.12升级兼容性

## 4. 用例

| **序号** | **用例名** | 用例类型 | **测试步骤**                                                 | **预期结果**               |
| -------- | ---------- | -------- | ------------------------------------------------------------ | -------------------------- |
| 1        | 中心缩容   | 手动     | 1.创建函数；2.以一定并发访问这个函数；3.触发扩容后，持续访问一段时间；4.并发访问减少至1/3;5.观察worker有无减少；6.观察worker-manager和bus的缩容日志 | worker-manager触发全局缩容 |

