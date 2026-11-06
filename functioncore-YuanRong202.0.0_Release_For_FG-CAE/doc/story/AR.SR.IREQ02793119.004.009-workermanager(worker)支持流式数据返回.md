[TOC]

# YuanRong设计文档模板

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

https://clouddevops.huawei.com/workitem/US2022070701122

当前DWR服务使用函数流进行视频转码，图片压缩等服务，由于当前函数流架构不支持进行流式数据处理，当前采用OBS作为文件缓存中转，存在时延较大等问题，无法适应后续业务发展。本需求目前只实现go runtime

![img](https://clouddevops.huawei.com/vision-file-storage/api/file/download/upload-v2/2022/6/7/d00500859/7e78998b4f1e4260844daf5d64c750bd/image.png)

为了支持这个需求落地实现，worker-manager和worker需要做如下几件事情

1. workermanager、worker支持创建函数时选择"返回数据流"
2. workermanager deployment增加streamhandler 网段配置的环境变量、端口配置的环境变量, worker manager在创建worker deployment的时候将这两个变量放到deployment的pod template的env中
3. 当识别到函数开启了流式数据时，worker通过iptables规则打通和streamHandler之间特定端口特定网络网段的网络，保障runtime和streamHandler之间能够通信
4. funcitonbus支持传递X-Stream-Enable、X-Stream-Addr、X-Stream-Run-Id给runtime



### 1.2特性场景分析

执行的工作流需要返回流式数据时，最后一个函数节点的执行流程如下图所示

![img](https://clouddevops.huawei.com/vision-file-storage/api/file/download/upload-v2/2022/6/8/d00500859/c42090c090ff45eb8712ae81e1170847/image.png)

1.新增流处理服务StreamHandler，用于从runtime接收流数据，并返回给workflow

2.streamHandler 启动时注册ip信息到workflow etcd路径上/sn/workflow/streamhandler/

3.worker识别到函数开启了支持流式数据返回功能时，worker在特化阶段通过iptables规则打通和streamHandler之间特定端口特定网络网段的网络，保障runtime和streamHandler之间能够通信

4.go-runtime新增SDK用于写入流数据，go-runtime异步将流数据分块发送到StreamHandler，json按照原路直接返回（图中蓝线）

5.workflow接收到json后，从StreamHandler分块获取流数据，并进行组装，当所有的数据块组装成数据流返回invoker(不再返回json)

6.invoker将流式数据返回client(不再返回json)

## 2. 特性/功能实现原理

### 2.1总体方案

![img](https://clouddevops.huawei.com/vision-file-storage/api/file/download/upload-v2/2022/6/16/d00500859/5706d9f6a86d450f8632952e0e13ff18/image.png)

### 2.2模块详细设计

1. workermanager、worker支持创建函数时选择"返回数据流"
2. workermanager deployment增加streamhandler 网段配置的环境变量、端口配置的环境变量, worker manager在创建worker deployment的时候将这两个变量放到deployment的pod template的env中
3. 当识别到函数开启了流式数据时，worker通过iptables规则打通和streamHandler之间网络，保障runtime和streamHandler之间能够通信
4. funcitonbus支持传递X-Stream-Enable、X-Stream-Addr、X-Stream-Run-Id给runtime

#### 2.2.1 functionbus子模块设计

##### 2.2.1.1模块实现

*以时序图的形式描述模块的功能需求和非功能需求的实现过程*

funcitonbus支持传递X-Stream-Enable、X-Stream-Addr、X-Stream-Run-Id给runtime

[运行模型-顺序图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/a5e8b08a8af049c89e43371b8d07db8b?projectDiagramUUID=67a42d992f91493ca02b576392e6f911&versionUUID=)![运行模型-顺序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2PBs?t=1659867880807)

##### 2.2.1.2模块对外接口定义

*描述模块的对外接口（如果有），所有外部模块只关注模块接口，不关注模块实现*

不涉及

##### 2.2.1.3模块类设计

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*



#### 2.2.2 worker-manager/worker子模块设计

##### 2.2.1.1模块实现

*以时序图的形式描述模块的功能需求和非功能需求的实现过程*

[运行模型-顺序图-worker特化流程-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/a5e8b08a8af049c89e43371b8d07db8b?projectDiagramUUID=3b8cd9eb27944b22a333f37427645a96&versionUUID=)![运行模型-顺序图-worker特化流程](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2PHY?t=1659930297126)

[运行模型-顺序图-特化流程2-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/a5e8b08a8af049c89e43371b8d07db8b?projectDiagramUUID=37c884907a914e19a78c3017647de3a6&versionUUID=)![运行模型-顺序图-特化流程2](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2PHr?t=1660373656534)



##### 2.2.1.2模块对外接口定义

*描述模块的对外接口（如果有），所有外部模块只关注模块接口，不关注模块实现*

不涉及新增接口

##### 2.2.1.3模块类设计

*以类图的形式描述模块的功能需求和非功能需求的静态结构，类设计重点考虑可扩展性、可测试性、可重用性，类的设计要符合SOLID原则*



1. 数据结构

   在函数元数据中，增加是否支持流式数据返回开关配置 isStreamEnable, 由管理面写入

   ```go
   "funcMetaData": {
           "layers": null,
           "name": "0@default@gotest",
           "description": "",
           "functionUrn": "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@default@gotest",
           "tenantId": "c53626012ba84727b938ca8bf03108ef",
           "reversedConcurrency": 0,
           "tags": null,
           "created": "2022-07-27 08:21:58.234 UTC",
           "functionUpdateTime": "2022-07-27T16:21:57+08:00",
           "functionVersionUrn": "sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@default@gotest:latest",
           "revisionId": "20220727082158234",
           "codeSize": 7428935,
           "codeSha512": "a9c95a5cd97ec2d325d967b48dd1c6a0c05a2822ef6b3b1f238d3bf303ddce320711108a35ef21054a53bbb74e8b807e3935409454d94183770432ed39dae7f0",
           "handler": "handler",
           "runtime": "go1.x",
           "timeout": 3,
           "version": "latest",
           "versionDescription": "",
           "deadLetterConfig": "",
           "latestVersionUpdateTime": "2022-07-27T16:21:57+08:00",
           "publishTime": "",
           "businessId": "yrk",
           "functionType": "",
           "id": "3cfa69c2-3d3e-47ec-9f81-3a7106cf8e6c",
           "func_id": "afb70bb8-5207-4dfb-a880-5eeff455012c",
           "func_name": "gotest",
           "domain_id": "bfa257032eeb4c6f8d3f8c3b50c3b896",
           "project_name": "cn-north-7",
           "service": "default",
           "dependencies": "",
           "enable_cloud_debug": "DISABLED",
           "isStatefulFunction": false,
           "isStreamEnable": true,
           "type": "v2",
           "enable_auth_in_header": false,
           "dns_domain_cfg": [],
           "isStreamEnable": true
       },
   ```

2. 配置参数传递

   [领域模型-test-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/a5e8b08a8af049c89e43371b8d07db8b?projectDiagramUUID=e05b99db82ce4aa6a146dc32767b7912&versionUUID=)![领域模型-test](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2Run?t=1660552854026)

### 2.3 约束

#### 2.3.1上下游依赖需求

workflow支持流式数据

【管理面】invoker支持同步工作流返回流式数据

#### 2.3.2其他约束

目前只支持go语言的函数， 其他语言暂不支持

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

1. worker挂了重新拉起的情况，worker挂掉会重新拉起一个新的pod，识别到函数开启了支持流式数据返回功能，worker会重新打通网络，如果网络打通失败，说明特化请求失败。所以新拉起来的worker。

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

streamhandler先升级，worker-manager、worker、runtime后升级

## 4. 用例

本次数据面单独转测，因此用例主要针对如何单独测试数据面

| **序号** | **用例名**                       | 用例类型 | **测试步骤**                                                 | **预期结果**                                                 |
| -------- | -------------------------------- | -------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| 1        | go语言函数开启支持流式数据返回   | ST       | 1.创建go语言函数, 将etcd源数据信息meta中的isStreamEnable修改为true | 进worker所在的container，通过iptables -L -n 可以查看到包含streamhandler ip的iptables规则 |
| 2        | go语言函数未开启支持流式数据返回 | ST       | 1.创建go语言函数, 将etcd源数据信息meta中的isStreamEnable修改为false | 进worker所在的container，通过iptables -L -n 查看不到包含streamhandler ip的iptables规则 |

