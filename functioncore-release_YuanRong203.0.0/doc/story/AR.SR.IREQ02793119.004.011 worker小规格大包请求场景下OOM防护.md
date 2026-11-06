[TOC]

# AR.SR.IREQ02793119.004.011 worker小规格大包请求场景下OOM防护

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

该需求来自问题单DTS2022080304745转来的，128M规格的runtime对应的worker容器的内存也是128M，在100并发，1M请求压测下，由于worker处在请求链路上，且worker进行了协议转换，使用的内存超过128M，产生了OOM。需要针对这种小规格大包场景进行worker的oom防护

### 1.2特性场景分析

[函数调用]![函数调用](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Fp3?t=1657593587049)



## 2. 特性/功能实现原理

### 2.1总体方案

该问题本质上是一个系统架构问题，在请求链路上的每一个服务，都需要有足够的内存和cpu资源，才能保证性能和可靠性。当前worker在请求链路上，但是内存资源不足，在runtime规格较小，请求和并发比较大，函数调用就会失败。（在wise function，请求是由frontend直接发到runtime）

1）Worker内存优化， Worker增加内存强制回收机制，内存变量解引用之后强制回收



### 2.2模块详细设计

#### 2.2.1worker大包请求内存优化设计

##### 2.2.1.1模块实现

worker中请求流程：

1）Worker 注册回调函数 pkg/worker/invoke/server.go:140 s.bus.RegisterCallback(s.Adaptor)

2）worker接收请求往msgChan里传pkg/functionbus/bus/tcpbus/server.go:147 ConnHandler:  handleConn,

3）worker从msgChan里读取请求msg信息 pkg/functionbus/bus/tcpbus/server.go:193  func (c Connection) serverListenMsg() 

4）每个请求起一个go协程处理 pkg/functionbus/bus/tcpbus/server.go:223 go c.processCallReqMsg(msg)

5）go协程处理实际会调HandleRequest（pkg/functionbus/bus/tcpbus/tcpbus.go:330） pkg/functionbus/bus/tcpbus/connection.go:217 result = c.reqHandler(msg.Payload, c.PeerIP)

6）HandleRequest中会实际调到Adaptor pkg/functionbus/bus/tcpbus/tcpbus.go:218 t.asyncRequest   |pkg/functionbus/bus/tcpbus/tcpbus.go:188 err = t.callback(ctx, dst, req)

7）Adaptor 执行完后 asyncRequest里从**ch**里读取resp pkg/functionbus/bus/tcpbus/tcpbus.go:194  case resp, ok := <-ch

8）ch是AddResponse里塞入数据的，然后AddResponse（ pkg/functionbus/bus/calls/calls.go:76）是由SetResult（pkg/functionbus/bus/tcpbus/tcpbus.go:535）调用的 

9）Adaptor中用户的payload主要是存在req的RawData里 pkg/worker/invoke/server.go:396   Event:      req.RawData



内存控制优化点：

1）msgChan 长度过大，导致请求并发高时，如果serverListenMsg晓飞过慢，请求会堆积在msgChan里内存升高存在潜在oom风险 pkg/functionbus/bus/tcpbus/server.go:143 （**msgChan的大小改成**0**，注意下性能是否会下降**）

2）流程4中每一个请求都起一个go协程处理，也会导致内存堆积引发oom，需要进行优化（改为2个go协程，1个负责接收来自proxy的请求，并发送给runtime，另一个负责接收来自runtime的返回，并返回到proxy）

3）pkg/worker/invoke/server.go:515 **request \*runtime.InvokeRequest** **在构造完请求获取返回后及时释放**Event**（包含用户的**6M**数据）**

4）函数请求和返回都需要做oom防护，因为返回也有可能并发返回导致oom(再测试下runtime是否会oom，或者runtime不oom的情况下会不会引起worker的oom)

5）pkg/worker/runtime/rpc/protocol.go:296 BuildInvokeRequest可能会瞬间变成12M的内存， **可以释放**message







##### 2.2.1.2模块对外接口定义

*无*

##### 2.2.1.3模块类设计

*无*

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*

### 2.3 约束

#### 2.3.1上下游依赖需求

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

| **序号** | **用例名**           | 用例类型             | **测试步骤**                                                 | **预期结果**                                                 |
| -------- | -------------------- | -------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1        | 1M大包请求可靠性测试 | ST（可靠性测试）     | 设置128规格函数并发度100，请求体1M，100并发测试，查看worker是否会oom，与请求返回情况 | worker正常运行不oom，请求正常返回                            |
| 2        | 6M大包请求可靠性测试 | ST（极限可靠性测试） | 设置128规格函数并发度100，请求体6M，100并发测试，查看worker是否会oom，与请求返回情况 | worker正常运行不oom，请求正常返回，或者runtime oom错误码4205 |
| 3        | 1M大包返回可靠性测试 | ST（可靠性测试）     | 设置128规格函数并发度100，返回体1M，100并发测试，查看worker是否会oom，与请求返回情况 | worker正常运行不oom，请求正常返回，或者runtime oom错误码4205 |
| 4        | 6M大包返回可靠性测试 | ST（极限可靠性测试） | 设置128规格函数并发度100，返回体6M，100并发测试，查看worker是否会oom，与请求返回情况 | worker正常运行不oom，请求正常返回，或者runtime oom错误码4205 |