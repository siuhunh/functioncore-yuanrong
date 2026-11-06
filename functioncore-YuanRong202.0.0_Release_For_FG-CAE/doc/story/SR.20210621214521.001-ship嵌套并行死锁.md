[TOC]

# 支持cancel函数请求

关联AR:
+ AR.20210622083934.001: 多层ship，无法扩容情况下，不死锁

## Story 整体设计

### 功能描述


### 场景分析
+ 递归调用：
金融计算场景中，往往将所有的用户函数依赖封装成一个函数包，该函数实例作为一个 executor 来执行用户的函数片段。
例如：金融部门将所有的函数片段 code1, code2 的依赖打包进函数 refer中，外部请求 request1 中携带用户函数 code1 和参数 args1 到 refer 
的 executor 中进行执行，发现 code1 中又触发了函数间调用 request2。 并且因为 code2 的 executor 也是 refer，所以该函数间调用就转变
成了 refer 的递归调用。

[var场景死锁请求模型图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=0fdee85db2194f66be6db2dccff25f1c&versionUUID=)![var场景死锁请求模型图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/vJB?t=1626404230101)

+ 死锁：
如上面所讲的案例中，对于首次外部触发的请求 request1【从 frontend 过来的请求】
我们称为第一层请求，第一层请求 request1 触发的请求 request2 称为第二层请求，第三层，第四层请求类推。

在资源不充沛的场景下，一批请求的第一层请求占据所有的实例，且得不到空闲的实例去调度第二层请求，但是资源要想被释放第一层资源就需要完成。
即第n层请求依赖第n+1层请求的执行结果，第n+1层请求的调度又依赖第n层请求资源的释放，就导致了死锁问题。

[VaR场景死锁请求流程图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=a130f6e98e3b41efa40130d0919fa379&versionUUID=)![VaR场景死锁请求流程图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/vJI?t=1626404304836)

### 方案详细设计
+ 方案：基于层深的优先级调度
当发生递归调用触发invoke， 调用阻塞函数get之后，runtime处于空闲状态，所以可以在触发get的同时，如果当前runtime中的线程池仍有剩余, 
通知proxy该runtime的一个线程占用的CPU资源释放， 该runtime的处理能力+1。后续proxy 会调度一个新的请求到该runtime中进行处理。
如果递归调用触发了一组n个invoke， 用wait 接口阻塞等待n次调用结果，这时候 runtime 的处理能力应该+n。
这样一来在发生递归调用的时候，就最大程度的利用了runtime的处理能力，能够处理后续递归请求。

+ 具体需求：
1. 分析调用层数，pattarn 调用 invoke 的时候讲请求的层深度传递过来， functioncore 后续传递请求时作为请求调度的属性传递给proxy中
```
    fdk ---将requestLayer封装为 invokeRequest pb--- functiontask 解析requestLayer 通过coresdk 传给proxy---
```
2. 新增runtime-worker-proxy之间的新的消息传递，触发get的时候， 当此时的线程数量没有超过100的时候，通知proxy同步修改proxy metrics 
中 worker 的处理能力+1，当 runtime 中调用一组invoke 请求，使用 wait接口的时候处理能 +n 
3. Proxy中按照层深的优先级排序， 请求的结构体中加上请求的层深度，且采用优先级队列

[VaR场景解决死锁流程图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=e74f0c8715384729a16d475adcfc478b&versionUUID=)![VaR场景解决死锁流程图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/vJJ?t=1626404377645)

### 其他情况约束说明
1. 如果线程1 在get 到结果之后没有立即结束，而是做了其他消耗CPU 资源的事情，会导致线程1和线程2 之间CPU 资源的抢占
2. 当发生递归调用的时候， 资源极端的情况下会发生一个runtime中同时计算100个请求，100个请求中为各个层深的组合， runtime中可能会发生OOM。
3. 并发度 * 一次请求触发的递归调用次数  < 线程池最大线程数,  （当前python线程池最大为100 ）
4. 该递归调用仅在ship调用中开发， 普通场景下的函数调用约束仍旧不支持递归调用，且ship调用的层深度约束在4层
5. 当前触发一次函数请求完成的超时时间仍然为900s, 所以如果有递归调用的时候， 资源有限的情况下，调度递归请求可能会存在超时的可能
6. 层深大的请求先执行完成
7. 资源充足的情况下， 会按照函数的默认并发度进行扩容， 用来分担递归请求的压力, 但是没有说是一定调度到新的实例还是旧的实例中


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
*说清楚预置条件、输入（构造步骤、构造内容）、输出（预期结果）*

1. 部署test函数, 最大实例为1，函数并发量为1。编写ship函数， 嵌套4层ship调用，每层触发1次调用， 调用成功
2. 部署test函数，最大实例为1，函数并发量为1。编写ship函数， 嵌套2层ship调用，第二层触发99次调用，遍历get_snfuture 调用成功
3. 部署test函数，最大实例为1，函数并发量为1。编写ship函数，嵌套2层ship调用，第二层触发99次调用，使用wait调用成功

pattarn 接口举列子：

函数yaml
```
    service: cancel
    description: this is a hello world service
    default:
      runtime: python3.8
      memorySize: 500
      timeout: 900
    functions:
      helloworld:
        runtime: python3.8
        memorySize: 500
        timeout: 900
        concurrentNum: 1
        minInstance: 1
        maxInstance: 1
```

```
import time
import sys
import yuanrong
func = 'sn:cn:yrk:45c1af1c00c4425cbf5575d729879947:function:0-cancel-helloworld:$latest'
yuanrong.init(package_ref=func,logging_level="INFO", cluster_server_addr="100.107.74.168")
a = 55555

@yuanrong.ship(log_to_driver=True)
def funcC():
    return 22

@yuanrong.ship(log_to_driver=True)
def getnum():
    print('this is funcA')
    @yuanrong.ship(log_to_driver=True)
    def funcB():
        futures = []
        for i in range(20):
            future0 = funcC.ship()
            futures.append(future0)
        rest0 = yuanrong.get(futures)
        return rest0
    time.sleep(1)
    future = funcB.ship()
    rest = yuanrong.get(future)
    return rest

time_s = time.time()
idlist = [getnum.ship() for i in range(1)]
ret = yuanrong.get(idlist)
print("result is", ret)
```
