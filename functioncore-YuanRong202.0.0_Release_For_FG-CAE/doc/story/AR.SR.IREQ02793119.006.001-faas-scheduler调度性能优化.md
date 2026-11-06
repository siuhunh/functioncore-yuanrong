# AR.SR.IREQ02793119.008.002-支持scheduler统一调度性能

# 场景

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

在1:1并发场景下，请求由faas-scheduler统一调度，当前faas-scheduler的调度规格是：请求持续稳定3分钟后，实例数不超过预计的10%+5

faas-scheduler的调度性能还需要优化，规格是20个函数20实例，函数直接返回、共10000QPS，新增时延不超过2ms

### 1.2特性场景分析

[向scheduler申请实例调度](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=cd08ca48a3d34a45b4300c88e3a7770b)![向scheduler申请实例调度](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2mAm?t=1667545314735)

1.当函数并发度为1时，才会走scheduler调度，否则直接采用本地全量调度

2.当函数等待调度的时延大于250ms时，直接走本地全量调度

3.当条件1、2都满足时，frontend先判断自己的缓存中是否有可用的Processor，如果有，随机选择一个进行调度

4.如果没有可用的Processor，向scheduler请求Processor，scheduler随机选择一个空闲的Processor，将Processor返回给frontend

5.frontend将收到Processor响应，将请求调度到Processor所在的实例上

## 2. 特性/功能实现原理

### 2.1总体方案

#### 2.1.1 性能现状分析

测试基准：java函数，1:1并发，返回hello，规格300-128，20个实例，20并发压测。压测200000请求，单frontend

**兜底调度方案性能**

```
Summary:
  Total:	14.3810 secs
  Slowest:	0.0433 secs
  Fastest:	0.0006 secs
  Average:	0.0014 secs
  Requests/sec:	13907.1967
  
  Total data:	1600000 bytes
  Size/request:	8 bytes

Response time histogram:
  0.001 [1]	|
  0.005 [198130]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.009 [1761]	|
  0.013 [98]	|
  0.018 [5]	|
  0.022 [2]	|
  0.026 [1]	|
  0.030 [0]	|
  0.035 [0]	|
  0.039 [0]	|
  0.043 [2]	|


Latency distribution:
  10% in 0.0008 secs
  25% in 0.0009 secs
  50% in 0.0013 secs
  75% in 0.0017 secs
  90% in 0.0022 secs
  95% in 0.0025 secs
  99% in 0.0047 secs

```

frontend cpu 230%，scheduler cpu 0%，平均时延1.4ms

**性能优化前性能**

```

Summary:
  Total:	71.3532 secs
  Slowest:	0.0363 secs
  Fastest:	0.0010 secs
  Average:	0.0071 secs
  Requests/sec:	2802.9563
  
  Total data:	1600000 bytes
  Size/request:	8 bytes

Response time histogram:
  0.001 [1]	|
  0.004 [3542]	|■
  0.008 [164672]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.012 [29765]	|■■■■■■■
  0.015 [1669]	|
  0.019 [226]	|
  0.022 [73]	|
  0.026 [35]	|
  0.029 [10]	|
  0.033 [3]	|
  0.036 [4]	|


Latency distribution:
  10% in 0.0061 secs
  25% in 0.0064 secs
  50% in 0.0069 secs
  75% in 0.0076 secs
  90% in 0.0085 secs
  95% in 0.0093 secs
  99% in 0.0116 secs

```

frontend cpu 160%，scheduler cpu 80%，平均时延7.1ms，相比兜底调度，新增时延5.7ms

#### 2.1.2性能优化一：实例并行申请

当前最大的性能瓶颈在frontend向scheduler请求实例是单线程的，一个请求必须等待前面的请求向scheduler申请到实例之后，才能被调度。

```
1.Lock
2.本地有可用Processor，直接调度，否则走3
3.向scheduler请求实例
4.scheduler返回实例，直接调度到实例，否则走5
5.兜底调度
6.UnLock
```

这里的问题是锁的粒度太大，这个锁是LoadBalancer外部的一个锁，需要将锁拿到LoadBalancer里面来，由LoadBalancer控制锁，这样多个请求就可以并行向scheduler获取实例，无需等待

将锁拿到LoadBalancer内部之后，调度流程就变成：

```
1.Lock
2.本地有可用Processor，直接调度，UnLock，否则走3
3.UnLock，向scheduler请求实例
4.Lock 
5.scheduler返回实例，直接调度到实例，UnLock，否则走5
6.兜底调度
7.UnLock
```

这样做了之后，就无需请求超过250ms，直接走兜底的处理了，因为请求本身最多就等250ms

#### 2.1.3性能优化二：实例回归和续租协程池化

1.当前一个函数的实例归由一个协程统一处理，效率比较差，优化成协程池，并行归还实例

2.一个函数的续租也是由一个协程进行处理，优化成协程池，并行续租

3.scheduler使用autogc

```
Summary:
  Total:	17.7920 secs
  Slowest:	0.0286 secs
  Fastest:	0.0009 secs
  Average:	0.0018 secs
  Requests/sec:	11241.0322
  
  Total data:	1600000 bytes
  Size/request:	8 bytes

Response time histogram:
  0.001 [1]	|
  0.004 [196615]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.006 [2768]	|■
  0.009 [560]	|
  0.012 [53]	|
  0.015 [2]	|
  0.017 [0]	|
  0.020 [0]	|
  0.023 [0]	|
  0.026 [0]	|
  0.029 [1]	|


Latency distribution:
  10% in 0.0012 secs
  25% in 0.0014 secs
  50% in 0.0016 secs
  75% in 0.0020 secs
  90% in 0.0024 secs
  95% in 0.0027 secs
  99% in 0.0046 secs

```

frontend cpu 290%，scheduler cpu 190%，平均时延1.8ms，相比兜底调度，新增时延0.4ms

#### 2.1.4性能优化三：fasthttp替换grpc

1.当前frontend与scheduler之间采用grpc通信，grpc是基于http2，cpu消耗会比较大

2.尝试用fasthttp+protobuf协议替换grpc

```
Summary:
  Total:	16.1337 secs
  Slowest:	0.0165 secs
  Fastest:	0.0008 secs
  Average:	0.0016 secs
  Requests/sec:	12396.4276
  
  Total data:	1600000 bytes
  Size/request:	8 bytes

Response time histogram:
  0.001 [1]	|
  0.002 [184377]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.004 [13036]	|■■■
  0.005 [1406]	|
  0.007 [907]	|
  0.009 [215]	|
  0.010 [26]	|
  0.012 [18]	|
  0.013 [1]	|
  0.015 [1]	|
  0.016 [12]	|


Latency distribution:
  10% in 0.0011 secs
  25% in 0.0012 secs
  50% in 0.0015 secs
  75% in 0.0018 secs
  90% in 0.0022 secs
  95% in 0.0026 secs
  99% in 0.0044 secs

```

frontend cpu消耗280%，scheduler cpu 150%，平均时延1.6ms，相比兜底增加0.2ms

### 2.4 约束

#### 2.4.1相关场景约束

1.并行租借后，并发度大于10的函数无法使用faas-scheduler，否则会导致frontend多申请

2.在现网来说，并发度10已经是比较大并发了，例如云短信业务就是函数1:10并发，4万QPS。所以scheduler

只开1:1并发。

3.当前每个集群只部署一个faas-scheduler，规格是2U2G，按目前看，1万QPS需要1.5核。这块需要看一下，后续单集群是否需要部署多个scheduler，做负载均衡。

## 3. 可信软件

### 3.1安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*

frontend与scheduler的协议采用fasthttp+tls

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*

与之前保持一致，只有通信协议换成fasthttp

### 3.3隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

与之前保持一致，只有通信协议换成fasthttp

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

与之前保持一致，只有通信协议换成fasthttp

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

内部优化，无影响

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

内部优化，无影响

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

规格是20个函数共10000QPS，新增时延不超过2ms，性能达标

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

frontend新增协程池，池大小10000

frontend去掉每个函数的归还协程

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

内部优化，无影响

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

frontend与scheduler需要一起升级，否则frontend会上报scheduler失联的紧急告警，scheduler调度能力无法使用

## 4. 用例

20个函数，1:1并发，函数内部直接返回，10000QPS压测，相比兜底调度算法，增加的时延不大于2ms

## 5遗留问题

