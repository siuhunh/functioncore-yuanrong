[TOC]

# 支持2M以上大包请求的可靠性及性能

## 1. 需求场景分析

### 1.1特性需求来源与价值概述

需求来源于内部优化需求，当前YuanRong宣称最大支持请求体为6MB的函数请求，初步测试发现这块性能差，可靠性不足，存在隐患，一旦客户使用，容易出现性能以及可靠性问题，需要优化

需要分析性能差以及可靠性不足的原因，针对分析出的原因进行优化，提升性能和可靠性，确定规格，消除潜在风险

### 1.2 场景分析

1.同步无状态函数调用由RDispatcher发给Frontend进入数据面，经过的链路如下

[函数调用](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=4c171cc9f792464db90a8b0856256b48)![函数调用](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Fp3?t=1657593587049)

**函数请求由**RDispatcher发到Frontend进入数据面

## 2. 特性/功能实现原理

### 2.1总体方案

#### 2.1.1极限分析

在函数请求体大于2M场景下，由于节点的网络带宽只有10Gbits，带宽很容易成为性能瓶颈

对于大部分的函数调用，RDispatcher在节点A上，Frontend在节点B上，FunctionTask、worker、Runtime在节点C上。

在单个集群中，Frontend是3-5个，Task数量是每节点1个

以乌兰布察202为例，frontend3个，FunctionTask是30个。

由于Frontend数量远小于FunctionTask数量，那么在函数请求体大于2M场景下， 数据面的性能瓶颈在frontend所在节点的网络带宽上

使用iperf3工具测试10Gbits网络带宽

**节点A和节点B之前的网络**

[ ID] Interval           Transfer     Bandwidth       Retr
[  5]   0.00-10.00  sec  7.87 GBytes  6.76 Gbits/sec  51153             sender
[  5]   0.00-10.00  sec  7.86 GBytes  6.75 Gbits/sec                  receiver

**平均每秒能传输865M，在请求体大小为2M的情况下，单个frontend极限QPS就是430**

**节点A和节点A内部的网络**

[ ID] Interval           Transfer     Bandwidth       Retr
[  5]   0.00-10.00  sec  48.0 GBytes  41.3 Gbits/sec    0             sender
[  5]   0.00-10.00  sec  48.0 GBytes  41.3 Gbits/sec                  receiver

#### 2.1.2性能实测

版本:101.0.0.B001.20220701094748

函数：java函数，请求体2M，直接返回，6U8G，100并发

frontend：1个

实例：1预留实例，最大实例1

压测：100线程并发，10000个请求

**测试工具在节点A，frontend在节点B，实例在节点C**

```
Summary:
  Total:	28.3577 secs
  Slowest:	1.7541 secs
  Fastest:	0.0155 secs
  Average:	0.2602 secs
  Requests/sec:	352.6385
  
  Total data:	120000 bytes
  Size/request:	12 bytes

Response time histogram:
  0.016 [1]	|
  0.189 [3763]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.363 [3567]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.537 [2231]	|■■■■■■■■■■■■■■■■■■■■■■■■
  0.711 [362]	|■■■■
  0.885 [60]	|■
  1.059 [10]	|
  1.233 [4]	|
  1.406 [0]	|
  1.580 [1]	|
  1.754 [1]	|


Latency distribution:
  10% in 0.0596 secs
  25% in 0.1101 secs
  50% in 0.2673 secs
  75% in 0.3699 secs
  90% in 0.4547 secs
  95% in 0.5217 secs
  99% in 0.6738 secs

Details (average, fastest, slowest):
  DNS+dialup:	0.0000 secs, 0.0155 secs, 1.7541 secs
  DNS-lookup:	0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:	0.1082 secs, 0.0010 secs, 1.1194 secs
  resp wait:	0.1519 secs, 0.0116 secs, 1.6488 secs
  resp read:	0.0000 secs, 0.0000 secs, 0.0012 secs

Status code distribution:
  [200]	10000 responses
```

**测试工具在节点A，frontend在节点A，实例在节点A**

```
Summary:
  Total:	22.4667 secs
  Slowest:	0.4962 secs
  Fastest:	0.0184 secs
  Average:	0.2220 secs
  Requests/sec:	445.1039
  
  Total data:	120000 bytes
  Size/request:	12 bytes

Response time histogram:
  0.018 [1]	|
  0.066 [58]	|
  0.114 [99]	|■
  0.162 [113]	|■
  0.210 [2657]	|■■■■■■■■■■■■■■■■■
  0.257 [6325]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.305 [561]	|■■■■
  0.353 [69]	|
  0.401 [64]	|
  0.448 [38]	|
  0.496 [15]	|


Latency distribution:
  10% in 0.1944 secs
  25% in 0.2071 secs
  50% in 0.2210 secs
  75% in 0.2353 secs
  90% in 0.2521 secs
  95% in 0.2650 secs
  99% in 0.3632 secs

Details (average, fastest, slowest):
  DNS+dialup:	0.0001 secs, 0.0184 secs, 0.4962 secs
  DNS-lookup:	0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:	0.0081 secs, 0.0011 secs, 0.2735 secs
  resp wait:	0.2134 secs, 0.0155 secs, 0.4841 secs
  resp read:	0.0003 secs, 0.0000 secs, 0.0308 secs

Status code distribution:
  [200]	10000 responses

```



### 2.2模块详细设计

#### 2.2.1frontend性能优化详细设计

##### 2.2.1.2模块实现

[frontend内部顺序图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=0a1f1ae0a88f4809b6765f2ad2a8f753)![frontend内部顺序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2JkR?t=1658801852511)

1.frontend收到rdispatcher的http请求

2.frontend将Http请求转换成fasthttp请求，这个过程中会将请求body读出，再设置成Fasthttp的body

3.frontend将FastHttp请求发送给Functiontask

从火焰图分析来看，第二步请求body的拷贝非常耗时，可以优化。

**优化方案**

http请求不再从reader里读出，而是直接把reader给Fasthttp请求，减少中间的性能损耗

**测试工具在节点A，frontend在节点B，实例在节点C**

```
Wed Jul 27 11:04:12 CST 2022

Summary:
  Total:	28.2702 secs
  Slowest:	1.5952 secs
  Fastest:	0.0145 secs
  Average:	0.2679 secs
  Requests/sec:	353.7289
  
  Total data:	120000 bytes
  Size/request:	12 bytes

Response time histogram:
  0.015 [1]	|
  0.173 [2934]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.331 [4028]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.489 [2337]	|■■■■■■■■■■■■■■■■■■■■■■■
  0.647 [559]	|■■■■■■
  0.805 [104]	|■
  0.963 [23]	|
  1.121 [7]	|
  1.279 [2]	|
  1.437 [3]	|
  1.595 [2]	|


Latency distribution:
  10% in 0.0900 secs
  25% in 0.1506 secs
  50% in 0.2620 secs
  75% in 0.3525 secs
  90% in 0.4506 secs
  95% in 0.5221 secs
  99% in 0.6880 secs

Details (average, fastest, slowest):
  DNS+dialup:	0.0000 secs, 0.0145 secs, 1.5952 secs
  DNS-lookup:	0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:	0.1103 secs, 0.0008 secs, 1.5144 secs
  resp wait:	0.1575 secs, 0.0113 secs, 1.1749 secs
  resp read:	0.0000 secs, 0.0000 secs, 0.0009 secs

Status code distribution:
  [200]	10000 responses

```

**测试工具在节点A，frontend在节点A，实例在节点A**

```
Summary:
  Total:	19.4095 secs
  Slowest:	0.3926 secs
  Fastest:	0.0173 secs
  Average:	0.1921 secs
  Requests/sec:	515.2128

  Total data:	120000 bytes
  Size/request:	12 bytes

Response time histogram:
  0.017 [1]	|
  0.055 [55]	|
  0.092 [45]	|
  0.130 [87]	|
  0.167 [654]	|■■■■
  0.205 [7005]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.242 [1957]	|■■■■■■■■■■■
  0.280 [74]	|
  0.318 [46]	|
  0.355 [54]	|
  0.393 [22]	|

Latency distribution:
  10% in 0.1696 secs
  25% in 0.1809 secs
  50% in 0.1921 secs
  75% in 0.2031 secs
  90% in 0.2145 secs
  95% in 0.2237 secs
  99% in 0.3029 secs

Details (average, fastest, slowest):
  DNS+dialup:	0.0000 secs, 0.0173 secs, 0.3926 secs
  DNS-lookup:	0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:	0.0055 secs, 0.0010 secs, 0.0670 secs
  resp wait:	0.1861 secs, 0.0143 secs, 0.3874 secs
  resp read:	0.0003 secs, 0.0000 secs, 0.0287 secs

Status code distribution:
  [200]	10000 responses
```

可以看到，在10Gbytes带宽下，性能无变化；在50Gbytes带宽下，qps提升16%，平均时延降低16%

##### 2.2.1.2模块对外接口定义

无

##### 2.2.1.3模块类设计

无

#### 2.2.2 functiontask、worker性能优化详细设计

##### 2.2.2.1模块实现

[functiontask、worker顺序图](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=70aae47a954d4ccfae068dd844035c80)![functiontask、worker顺序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Jlo?t=1658802228463)

[Functiontask、worker协议](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=e4be8a5983de4ff89f561465b310fed8)![Functiontask、worker协议](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2JsV?t=1658816025377)

1.Functiontask收到Frontend的2M Fasthttp请求

2.Functiontask读取2M请求body，并将请求体写入protobuf协议，并序列化成字节流

3.Functiontask拷贝2M的protobuf协议数据，转换成自定义协议TCP协议，将请求发给worker

4.worker解析自定义TCP协议，读取2M的protobuf协议数据，反序列化成请求

从这里可以看到一个2M的请求，在functiontask要经过两层协议的封装，才能发送到worker，实际上只需要1层协议就够了。

**优化方案**

1.请求body不再放入protobuf协议，而是直接以byte数组，作为tcp协议的一部分，这样可以减少一次内存拷贝，减少一份中间数据

**测试工具在节点A，frontend在节点B，实例在节点C**

```
Summary:
  Total:	28.1669 secs
  Slowest:	1.4400 secs
  Fastest:	0.0130 secs
  Average:	0.2633 secs
  Requests/sec:	355.0261
  
  Total data:	120000 bytes
  Size/request:	12 bytes

Response time histogram:
  0.013 [1]	|
  0.156 [2960]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.298 [2887]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.441 [3082]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.584 [816]	|■■■■■■■■■■■
  0.726 [197]	|■■■
  0.869 [46]	|■
  1.012 [9]	|
  1.155 [0]	|
  1.297 [1]	|
  1.440 [1]	|


Latency distribution:
  10% in 0.0729 secs
  25% in 0.1304 secs
  50% in 0.2658 secs
  75% in 0.3600 secs
  90% in 0.4500 secs
  95% in 0.5166 secs
  99% in 0.6744 secs

Details (average, fastest, slowest):
  DNS+dialup:	0.0000 secs, 0.0130 secs, 1.4400 secs
  DNS-lookup:	0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:	0.1085 secs, 0.0008 secs, 1.0337 secs
  resp wait:	0.1547 secs, 0.0101 secs, 1.1452 secs
  resp read:	0.0000 secs, 0.0000 secs, 0.0011 secs

Status code distribution:
  [200]	10000 responses
```

**测试工具在节点A，frontend在节点A，实例在节点A**

```
Summary:
  Total:	18.0939 secs
  Slowest:	0.4317 secs
  Fastest:	0.0206 secs
  Average:	0.1792 secs
  Requests/sec:	552.6717
  
  Total data:	120000 bytes
  Size/request:	12 bytes

Response time histogram:
  0.021 [1]	|
  0.062 [72]	|
  0.103 [71]	|
  0.144 [282]	|■■
  0.185 [6052]	|■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■
  0.226 [3179]	|■■■■■■■■■■■■■■■■■■■■■
  0.267 [233]	|■■
  0.308 [51]	|
  0.350 [35]	|
  0.391 [21]	|
  0.432 [3]	|


Latency distribution:
  10% in 0.1546 secs
  25% in 0.1663 secs
  50% in 0.1785 secs
  75% in 0.1907 secs
  90% in 0.2042 secs
  95% in 0.2163 secs
  99% in 0.2752 secs

Details (average, fastest, slowest):
  DNS+dialup:	0.0000 secs, 0.0206 secs, 0.4317 secs
  DNS-lookup:	0.0000 secs, 0.0000 secs, 0.0000 secs
  req write:	0.0060 secs, 0.0011 secs, 0.0688 secs
  resp wait:	0.1728 secs, 0.0140 secs, 0.4300 secs
  resp read:	0.0003 secs, 0.0000 secs, 0.0451 secs
```

可以看到，在10Gbyte带宽下，性能无变化；在50Gbytes带宽下，在frontend优化之后的基础，qps提升7%，平均时延降低5%

##### 2.2.2.2模块对外接口定义

优化后协议](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=dfcddfc9e835458788f6e2b714c1bce5)![优化后协议](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2JsW?t=1658816191124)

##### 2.2.2.3模块类设计

无

#### 2.2.3 frontend在大包请求场景下的可靠性设计

##### 2.2.3.1模块实现

大包请求场景下，由于内存使用量很高，需要考虑frontend OOM风险，当前frontend限流方案

[frontend内存限流](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=48f72980892b4047aa893ba12d43da39)![frontend内存限流](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2LxK?t=1659060168877)

将测试工具和frontend部署在同一个节点上，测试请求体为5M，10000并发执行，发现frontend存在oom现象

OOM原因：

1.当前内存使用量是估计值，按照请求体大小进行估计，实测发现实际内存使用量大于请求体大小，实测发现，并发100请求执行，新增内存1个G，远大于500M，预估值远远偏小

2.内存估计有偏差，当前系统内存是从/proc/pid/statm文件读取，压测场景下，这里读取的内存比/sys/fs/cgroup/memory/memory.stat要小100M-300M左右，docker stats显示的值和/sys/fs/cgroup/memory/memory.stat里一致，即OOM检测也是和/sys/fs/cgroup/memory/memory.stat这里一致，导致内存估计值偏低。

3.内存水位值90%过高，frontend内存最大4G，只预留400M的空间，由于内存是估计的，在协程调度抖动场景下，很容易oom

优化方案：

1.内存估计值按照请求体大小的1.5倍进行估计

2.系统内存从/sys/fs/cgroup/memory/memory.stat读取，和runtime-manager的oom检测同样的方案

3.内存上水位设置80%，并且采用functiontask的下水位限流方案，对请求体小于等于20000Bytes的请求不进行下水位限流。针对请求体大于20000Bytes的请求，如果内存超过60%的下水位线，对占用内存超过平均值的的函数进行限流，确保大部分场景下，单个函数过载不阻塞其他用户函数

4.内存估计值和上下水位配置可热加载

##### 2.2.3.2模块对外接口定义

无

##### 2.2.3.3模块类设计

无

#### 2.2.4 functiontask在大包请求场景下的可靠性设计

functiontask限流机制与frontend类似

将测试工具和frontend、functiontask部署在同一个节点上，测试请求体为5M，10000并发执行，functiontask也存在oom现象

OOM原因：

1.当前内存使用量是估计值，按照请求体大小进行估计，但由于functiontask对请求进行了多次的协议转换，导致一个5M的请求，实际上会占用3倍的内存， 实测发现，并发100请求执行，内存占用2个G，远大于估计的500M，预估值远远偏小

优化方案：

1.内存估计值按照请求体的2倍进行估计

2.内存下水位调整为0.6

### 2.3 约束

#### 2.3.1上下游依赖需求

无

#### 2.3.2其他约束

当frontend或functiontask内存过载时，请求会失败，返回管理面重试

worker的内存上限和runtime一样。当前worker和runtime目前没有内存水位防护。如果用户需要有处理大包的请求，请让用户使用大规格池子

## 3. 可信软件

### 3.1安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*

内部优化，不涉及

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*

内部优化，不涉及

### 3.3隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

内部优化，不涉及

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

内部优化，不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

无

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

无

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

在大包场景下，性能提升

一般场景下，无影响

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

无新增资源消耗

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

内部优化，客户不感知

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

tcp协议目前用在functiontask与worker通信、functiontask与functiontask通信 ，functiontask和worker协议修改是无法向前兼容的，functiontask和worker必须一起升级，否则会出现请求无法下发

### 3.13后续演进

1.worker和runtime之间也是两层协议，考虑重构优化

2.functiontask和worker的tcp协议可扩展性很差，没有兼容性，考虑重构优化

## 4. 用例

| **序号** | **用例名**         | 用例类型 | **测试步骤**                                                 | **预期结果**                  |
| -------- | ------------------ | -------- | ------------------------------------------------------------ | ----------------------------- |
| 1        | 2M大包请求性能测试 | 性能测试 | 设置函数并发度100，请求体2M，100并发测试，查看QPS和时延      |                               |
| 2        | 6M大包请求性能测试 | 性能测试 | 设置函数并发度100，请求体6M，100并发测试，查看QPS和时延      |                               |
| 3        | 6M大包极限测试     | 极限测试 | 设置函数并发度100，请求体6M，10000并发测试，查看functiontask和frontend是否oom | frontend和functiontask均无oom |