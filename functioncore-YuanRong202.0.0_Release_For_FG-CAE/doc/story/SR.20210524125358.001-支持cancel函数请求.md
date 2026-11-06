[TOC]

# 支持cancel函数请求

关联AR:
+ AR.20210526105559.001: 支持cancel元戎内部函数调用请求
+ AR.20210526104921.001: 支持cancel云下函数调用请求

## Story 整体设计

### 功能描述
1. Python SDK中提供cancel接口，用户在使用invoke调用一个函数后，可以使用cancel接口取消该次函数调用。
2. Frontend对外提供cancel等相关接口，以支持cancel来自云下的函数调用请求。

### 场景分析
1. 在SAT的算法并行场景中，只要有一个计算实例计算出结果，就无需其他计算实例的结果，如果能cancel这些计算实例，可以节约不必要的计算资源浪费；
2. 在金融VaR计算中，除了节省计算资源外，还会涉及多层计算的场景，需要提供cancel函数调用链的能力；
3. VaR计算场景中，用户在云下通过ship调用提交计算任务，需要提供云下的接口方便取消函数调用。


### 方案详细设计

#### 现状分析
调研了Ray的cancel实现。Ray在去年4月提交了一个[MR](https://github.com/ray-project/ray/pull/7669/files) ，支持了cancel调用。
Ray的cancel支持force和non-force两种模式：
- Force模式，简单粗暴，直接调用exit()退出进程
- Non-force模式，会调用_thread.interrupt_main()向主线程抛出KeyboardInterrupt异常，主线程中捕获到异常之后，再将异常封装成RayCancellationError向外抛出，走原有的异常处理流程并退出进程。

之后，Ray又提交了两个MR（地址：https://github.com/ray-project/ray/pull/8225/files 、https://github.com/ray-project/ray/pull/11923/files ），分别支持了remote cancel和recursive cancel。
核心实现为，Ray本身为点对点的结构，worker会有个task manager来维护子任务，worker可以直接获取子任务列表并向其发送rpc cancel请求。

Functioncore现状：
1. runtime内采用1：N的模型，每个请求跑在一个线程上，如果直接采用进程退出的形式，会导致runtime内其他正在执行的请求丢失。
2. 没有直接的点对点通信，需要由busproxy负责请求的转发。

#### 方案设计

##### Python Runtime内实现取消函数运行
Python的ctypes库提供了一个较为底层的调用，可以向指定的线程发送指定的异常，如果用户函数内没有处理该异常，便可以实现线程执行的退出。

```python
def stop_thread(thread):
    tid = ctypes.c_long(thread.ident)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(
        KeyboardInterrupt))
    if res == 0:
        log.get_logger().error("failed to stop thread, thread not exist")
    elif res != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")
```
Python Runtime在捕获到该异常之后，可以做进一步封装，作为函数调用结果返回。
```python
except KeyboardInterrupt:
    errcode = error.USER_CANCEL_ERROR
    errmsg = "function invocation canceled"
    return util.handle_exception(errcode, errmsg)
```

Runtime内部需要维护一个map，用来标记每次调用请求对应的thread线程；调用请求结束之后需要清理map中对应的请求。

**限制**：如果线程被一些I/O操作或者sleep函数阻塞，那么需要等到阻塞结束之后，线程才能触发异常。

##### Python SDK提供cancel接口
接口原型：
```python
def cancel(self, snfuture, force=False, recursive=True):
    """Cancel SNFuture"""
    pass
```
snfuture即为原有的invoke接口的返回值，内部包含了由functiontask生成的futureID，Functioncore根据此futureID来执行cancel调用。

force和recursive均为可选的默认参数

force代表是否强退，默认为false非强退模式，即向任务线程发送异常，最后返回一个USER_CANCEL_ERROR；强退模式会直接结束runtime进程，返回function instance exception的错误。

recursive代表是否递归cancel，默认为true，即在函数调用链上，a->b->c->d，如果cancel了b，则自动cancel c和d的调用。

##### FunctionCore实现cancel调用
整体的cancel流程如下：

[cancel整体流程](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=99f0957acb024507baff4651c8e741c2&versionUUID=)![支持cancel元戎内部函数调用请求](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qgS?t=1623997943783)

需要在函数调用执行的过程中，维护以下信息：
1. Proxy中需要维护futureID对应的下一跳信息（本节点的worker，或其他proxy）
2. Worker中需要维护runtime发出的invoke请求对应的futureID信息（对应函数调用链）
3. Runtime中需要维护futureID对应的执行线程信息。

##### Frontend提供接口支持cancel调用
由于frontend是多实例负载均衡的，invoke请求和cancel请求可能会被发送到不同的frontend实例上，在不依赖外部数据库保存调用状况的情况下，可以借助负载均衡的session亲和能力，保证同一个客户端发送的请求被调度到同一个frontend实例上。
- K8s的Service提供了SessionAffinity功能，当该项被配置为ClientIP时，kube-proxy会根据客户端的ip来进行请求的亲和，来自相同ip的请求会被调度到同一个实例上。
- 对于部署在CCE集群上的场景，云下通过CCE的ELB访问frontend，可在ELB中配置会话保持功能。

[frontend-cancel](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=c00be5f438974fd09c477b3013b2eeb2&versionUUID=)![支持cancel云下函数调用请求](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qgT?t=1623997975203)

Frontend原先提供的函数调用接口是同步的，可在接口的header里增加一个标识字段，用来标识该请求是否为异步调用（默认为同步调用）。同步调用与原有的流程保持完全一致。

异步调用的情况下，frontend将生成一个future_id并将其作为结果返回，同时将future_id向下传递到proxy执行函数调用流程。

客户端拿到future_id之后，可以根据future_id查询函数调用的结果，或者根据future_id取消该次函数调用。


#### 开源软件选型
不涉及

#### 外部交互分析&&上下游依赖需求
不涉及

### 对外接口
1. frontend原有的函数调用接口增加headers字段X-ASYNC用来标识是否为异步请求：
- 如果该字段未设置或者为false，则保持原有的函数调用流程。
- 若该字段为true，函数调用立即返回一个futureID

2. 新增调用结果查询接口：

`GET http://{ip}:{port}/serverless/v1/invocation/{id}`

| 参数 | 类型 | 可选/必选 | 描述/举例 |
| --- | --- | --- | --- |
| id | string | 必选 | 请求的futureID |

3. 新增Cancel调用请求接口：

`DELETE http://{ip}:{port}/serverless/v1/invocation/{id}?force={false}&recursive={true}`

| 参数 | 类型 | 可选/必选 | 描述/举例 |
| --- | --- | --- | --- |
| id | string | 必选 | 请求的futureID |
| force | bool | 可选/默认false | 是否强制cancel |
| recursive | bool | 可选/默认true | 是否递归cancel |



### 约束
由于调用链的信息均维护在每个组件（frontend、proxy、worker、runtime）的内存中，若组件异常重启，则不保证cancel调用能够成功。


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

| 序号 | 用例名称 | 测试步骤 | 预期结果 |
| --- | --- | --- | --- |
| 1 | cancel正在执行的请求 | invoke一个耗时函数，在函数执行完成之前立即调用cancel，并获取调用结果 | cancel成功，调用结果为 函数调用被cancel |
| 2 | cancel已经执行完的请求 | invoke一个函数，待该函数执行完成后，cancel该次调用，并获取调用结果 | cancel提示请求不存在或已经被cancel，函数调用结果能正确返回 |