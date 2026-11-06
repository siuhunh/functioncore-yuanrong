# CLI工具适配var计算

关联AR： AR.20210518124351.001：CLI工具适配var计算

## 1. Story 整体设计
### 1.1 功能描述
    CLI 工具对外名称从 SN 改为 yrctl
    对于 Python函数，service.yaml 中 handler 为可选项配置【可配可不配】
    对于 Python 函数，允许函数代码包为空函数
    函数部署成功，返回函数的 URN 信息
    
我们的平台产品名称已经正式从 Serverless Native 更名为 YuanRong， 
所以命令行工具 "sn" 更名为 "yrctl"， 命令行中的回显 "sn" 改为 "YuanRong" 或者 "yrctl"  

而且由于当前 Python runtime 支持 var 计算， 且 var 计算的函数部署形态不太一致。 
创建 Python 函数的时候， 如果 service.yaml 中定义了 handler， 那么用户函数 *.py 以及其函数代码目录都必须提供（当前创建函数的约束一致）
但是允许 service.yaml 中不定义 handler, 当 handler 没有配置的时候, runtime 中的 executor 进行执行。 
此时如果用户函数 *.py 以及其函数代码目录存在， 在 runtime 中作为 executor 的依赖函数， 同时也允许其不存在

   
### 1.2 场景分析
在 var 计算场景下， 用户函数并不是在创建函数的时候进行部署的， 相反创建的该函数更像是一个满足用户函数执行的条件的容器
用户函数会调用 pattern 的 ship 函数， 通过序列化代码片段的方式 将用户函数传到 runtime 的 executor 进行执行 

cli 部署流程图
[python函数发布-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=8024212728654b928936b40aeadefd52&versionUUID=)![python函数发布](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qLh?t=1623848032434)

runtime 函数加载流程图
[函数加载流程图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=6b2768b65c4442af85af4e6bec956dc2&versionUUID=)![函数加载流程图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qLL?t=1623848080874)

### 1.3 方案详细设计
1. 在 CLI 里面， 所有的 Serverless Native , 和 sn 都会抽出来， 定义为常量
2. 在 CLI 创建函数时， python 函数，不指定 handler 的函数为有效函数
3. 在 CLI 创建函数时， python 函数，不指定 handler 的函数， 且不提供用户函数 *.py 为有效函数
4. 在 CLI 创建函数时， python 函数，指定 handler 的函数， 且不提供用户函数 *.py 为无效函数

【注：支持var计算的函数属于比较特殊的函数， 所以命令行并不提供默认的创建模板】


### 1.4 上下游依赖需求
无
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
| 1 |  CLI 工具名测试 | 运行 yrctl --[command] | 使用正常且回显没有sn  |
| 2 |  python 函数部署 | service.yaml 中没有指定handler | yrctl deploy 成功， 如果正确使用pattern客户端进行调用， 调用成功  |
| 3 |  python 函数部署 | service.yaml 中正常指定了handler， 但是没有提供用户函数*.py| yrctl deploy 失败|
| 4 |  其他语言函数部署 | service.yaml 中没有指定handler| yrctl deploy 失败|
| 5 |  其他语言函数部署 | 没有提供用户函数*.py| yrctl deploy 失败|
| 6 |  函数部署 | 按照约束进行用户函数进行创建| yrctl deploy 成功， 且返回信息中显示function的urn|
example1: 当前目录下只有：service.yaml, 没有配置了handler

```
tree
.
└── service.yaml

service.yaml:

     service: ptest
     description: this is a hello world service
     default:
       runtime: python3.7
       memorySize: 500
       timeout: 900
     functions:
       helloworld:
         runtime: python3.7
         memorySize: 500
         timeout: 900

yrctl deploy
Started to deploy the service ptest
--------------------------------
Started to deploy the function helloworld
Function code package costs time 0ms
Function package upload costs time 833ms
functionID: sn:cn:yrk:12345678901234561234567890123456:function:0-ptest-helloworld:$latest
Succeeded to deploy the function helloworld!
--------------------------------
Succeeded to deploy the service ptest!
  
```

example2: 当前目录下只有：service.yaml, 配置了handler
```
tree
.
└── service.yaml

service: ptest
description: this is a hello world service
default:
  runtime: python3.7
  memorySize: 500
  timeout: 900
functions:
  helloworld:
    handler: handler.my_handler
    runtime: python3.7
    memorySize: 500
    timeout: 900

yrctl deploy
Failed to validate service.yaml, function helloworld directory not exists.
```

