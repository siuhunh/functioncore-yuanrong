# FunctionBus通信路径不使用主机网络

关联AR： 

        AR.SR.IREQ02597546.028.001: worker更改为通信server端，接收bus proxy的建链请求
        AR.SR.IREQ02597546.028.002: BusProxy 作为通信客户端主动与 worker 建链
        AR.SR.IREQ02597546.028.003: Busproxy 从etcd中获取需要连接的worker信息
        
## 1. Story 整体设计
### 1.1 功能描述
   FunctionBus Proxy 和 worker 的通信链路改为： FunctionBus Proxy 作为客户端， worker 作为服务端。
   
### 1.2 场景分析
   目前 FunctionBus Proxy 使用主机网络提供服务， worker 在启动后向本节点的 FunctionBus Proxy 建立连接，后续通过此通信路径进行服务调用。
   基于安全需求（ FunctionBus 通信路径不使用主机网络），本次整改将 FunctionBus Proxy 作为通信的 client 端， 通过扩容\监控 etcd 发现 worker 的通信地址，并建立通信连接。
   Proxy 向 worker 建链流程在worker已经被特化成函数实例时发生。
   
        【注】：建链耗时1ms以内(实测200-400us)，可不考虑对冷启动的影响。减少了预先将池子中worker地址写入etcd等后续的复杂性。
       
### 1.3 方案详细设计
   function instance manager：
   [bus proxy 通过 workermanager 获取 worker 通信地址-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=076df64fa5804f959d21e125b1199607&versionUUID=)![bus proxy 通过 workermanager 获取 worker 通信地址](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/uYV?t=1626061305616)
   将 worker 的 ip 和 port 写入 k8s cr status 内容（当前使用 bus tcp 链路， 使用 p2pPort 字段填写）：
```
    Instance Id:  defaultaz-#-pool24-600-512-fusion-7b4b4d5fc4-z6h7x
    Applier:      dggphisprg01151
    Ip:           10.42.0.26
    Node IP:      10.244.196.133
    Node Name:    master
    Node Port:    22423
    Owner IP:     10.244.196.133
    p2pPort:      22668
    Port:         8080
    Status:       ready
```
   后续会根据 cr 的内容更新 etcd 中的数据：
```
/sn/workers/business/yrk/tenant/c53626012ba84727b938ca8bf03108ef/function/0@default@zttest/version/latest/defaultaz/defaultaz-#-pool23-400-256-fusion-7559dc86cd-tn88v
{"ip":"172.16.0.63","port":"8080","status":"ready","p2pPort":"22668","nodeIP":"192.168.246.172","nodePort":"22423","nodeName":"192.168.246.172"}
```

   proxy：
   自身信息写入 etcd ， 供 function accessor 和其他 proxy 发现并通信：
```
/sn/workers/business/yrk/tenant/0/function/function-task/version/$latest/defaultaz/192.168.175.131
{"ip":"","port":"8080","status":"ready","p2pPort":"32568","nodeIP":"192.168.175.131","nodePort":"22423"}
```

   [proxy作为通信客户端向worker建链](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=d884a24d73924ebab3f58f1d39f49c92)![proxy作为通信客户端向worker建链](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/uAZ?t=1625713261568)
   异常场景：
   1. 建链失败(对端退出\未启动， ip 不存在)，归还 worker （标记其为问题 worker ） ，向 function instance manager 请求缩容， function node manager 会立即删掉它。 proxy 尝试重新扩容一次。
   2. 建链成功，将worker纳入调度列表。后续链接断开，则将 worker 踢出调度列表，等待 etcd 信息更新，再去重新连接。同时请求会发送失败，重新选择 worker 执行。

   worker：
   1. 初始化流程启动通信的 server （端口通过配置文件配置），等待连接。
   2. 监听连接上的消息（即调用请求）。
   3. 连接异常，等待被重连。

    
### 1.5 上下游依赖需求
- AR.20210623113005.001: 【functioncore】busproxy worker 请求分发和Eviction策略
- AR.20210623113213.001: Frontend和Busproxy 负载均衡策略需要匹配
- AR.20210623113821.001: Busproxy 对远端worker的转发策略
- AR.20210623114222.001: workermanger 在纳管资源性能优化
以上需求7.16 日交付。
  
## 2. 可信软件
### 2.1 安全性（Security）
*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  
    
不涉及
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
| 1 |  基础测试 | 函数调用0-1 | 调用成功，并检查 etcd ，cr 数据均符合预期|
| 2 |  worker异常退出 | 函数调用0-1后，删除worker，再次调用函数 | 申请新的worker调用成功  |