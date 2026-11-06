# 异步长时间函数可靠性提升
        
## 1. Story 整体设计
### 1.1 功能描述
   元戎数据面支持函数异步调用，包括：异步调用的结果写入redis，异步函数计费，异步函数异常场景下的可靠性。
   
### 1.2 场景分析
   同步调用： fnAccessor -> proxy -> worker 长连接同步等待返回
   异步调用： fnAccessor -> proxy -> worker 返回202 accepted，请求结果后续由 proxy 写入 redis

   什么场景下会触发异步调用：
      请求执行时间超过 900s ， 数据面（ worker ）会在执行 900s 时返回请求接收，长连接即同步收到了返回结果。

   特殊场景：
   1. proxy 异常： 恢复后， worker 仍在运行，可以同步到断链期间的 worker 运行数据和请求（包含 worker 缓存的请求和正在执行的请求），其中对于已异步接收的请求， proxy 会将该请求结果上报。
   2. worker 异常： 优雅退出前，上报异步调用的请求结果

   
        【注】：极端场景 proxy / worker 同时故障或者 worker 在执行 900s 后被强制退出，则异步调用记录丢失，无法执行并写入结果（也不计费），需要管理面的超时保障，使该请求结束。
       
### 1.3 方案详细设计
   相关交互如下：
   [proxybus-workerbus](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=072c9700ca9f4eb2b99fcf31933edb7e)![proxybus-workerbus](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/17Cd?t=1634197665835)

#### 1.3.1 fnAccessor 函数调用的接口变化
request ：
    header中新增 x-invoke-source=obs , 后续在返回结果中携带，供管理面上报统计使用。

response： 异步返回结果的请求，返回 statusCode ： 202 。

#### 1.3.2 fnAccessor 和 proxy 内部接口变化
request ： 透传新增headers。
response： 异步返回结果的请求，返回 statusCode ： 202 。（同步返回结果， statusCode ： 200）

#### 1.3.3 proxy 和 worker 内部接口变化（ bus ）
新增异步接收返回错误码 AcceptedCode: 2
新增异步执行上报消息

#### 1.3.4 proxy 写入 redis 异步调用结果的格式
key ： fss-async-invoke-result-{requestId}
value： 
````
{ 
   "headers": "{$map}"
   "statusCode": 200,
   "body": "{}"
}   
其中 headers 需要包含
    "x-invoke-source" : "obs"
    "x-log-result" : "MjAyMS0xMC0xNCAwOTozODo0OC4zMDErMDg6MDAgU3RhcnQgaW52b2tlIHJlcXVlc3QgJzJjYjAwNGY0LTI0NWYtNDkzOS1hNTcyLWFjYjZmNDE3NGNmNScsIHZlcnNpb246IGxhdGVzdAphYmNkZQoKMjAyMS0xMC0xNCAwOTozODo0OC4zMDcrMDg6MDAgRmluaXNoIGludm9rZSByZXF1ZXN0ICcyY2IwMDRmNC0yNDVmLTQ5MzktYTU3Mi1hY2I2ZjQxNzRjZjUnLCBkdXJhdGlvbjogNS42NjBtcywgYmlsbGluZyBkdXJhdGlvbjogNm1zLCBtZW1vcnkgdXNlZDogMTM1LjY0MU1CLCBiaWxsaW5nIG1lbW9yeTogNTEyTUI="
    "x-instance-id" : "pool23-400-256-fusion-59bd9fb656-vfcl9"
    "x-invoke-summary" : '{"funcDigest":"1fb349758836c11e3de94577a0caf11","duration":5.66,"billingDuration":6,"memorySize":512,"memoryUsed":135.640625}'
    "x-billing-duration" : 0
    "x-function-urn" : "projectID@package@functionName@version@memsize"
````

        【注】：请求是否计费（单并发非预留实例运行，需要计费）根据 "x-billing-duration":0 判断

### 1.5 上下游依赖需求
functionGraph 管理面 支持异步长时间函数可靠性提升（梁新宜）。 --------- 10.20/21联调
有交集： AR.SR.IREQ02597546.033.001-Busproxy升级兼容（函数处理） （胡旭）
  
## 2. 可信软件
### 2.1 安全性（Security）
*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  
写入 redis 的内容为异步函数用户返回数据（建议用户不要返回敏感数据），和一些调用的摘要（不敏感）。安全性可控。    

### 2.2 韧性（Resilience）
*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*  
不涉及
### 2.3 隐私性（Privacy）
*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*  
不涉及
### 2.4 可靠性（Reliability）
*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*  
不涉及
### 2.5 可用性（Availability）
*【处于按要求执行状态的能力】*  
不涉及
### 2.6 安全（Safety）
*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及
## 3. HLT 用例

| 序号  |  用例名|  测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  短时间函数调用 | 调用 helloworld 函数 | funcAccessor 返回调用成功和结果， redis 无数据|
| 2 |  长时间函数调用 | 调用执行时间 1000s 的函数 | 约 900s 后，funcAccessor 返回调用接收，约 1000s 后从 redis 查看到返回结果   |
| 3 |  长时间函数调用proxy异常 | 调用执行时间 1000s 的函数 | proxy异常发生在未异步接收前，请求会重试到其他节点执行。发生在异步接收后，约 1000s 后从 redis 查看到返回结果   |
| 4 |  长时间函数调用worker异常 | 调用执行时间 1000s 的函数 | proxy异常发生在未异步接收前，请求会重试到其他节点执行。发生在异步接收后，worker优雅退出（执行完异步请求才退出），约 1000s 后从 redis 查看到返回结果   |