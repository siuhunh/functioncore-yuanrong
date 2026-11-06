[TOC]

# AR.SR.IR20230824003700.021.001 【用户VOC】【智利Zapping】函数支持gRPC调用

## 1. 需求场景分析

场景：当前元戎支持处理 http 请求，但无法处理用户发过来的简单 gRPC 请求。 本需求是为了满足智利客户对元戎支持 gRPC 的诉求。

### 1.1 特性需求来源与价值概述

需求来源：智利zapping客户，智利 top1 足球直播客户
1、Zaping要求将其微服务迁移到华为云平台。目前微服务内部通信协议为gRPC。
2、目前有三种迁移选项：CCE、CCI和FunctionGraph。Zaping更喜欢使用FunctionGraph，因为它更简单，更便宜。
3、Zaping的接口功能是HTTP，如Web前端功能和访问功能。这些函数通过grpc调用其他业务函数，如电子邮件函数。在gRPC中，Web前端函数调用不同的业务函数，就像调用本机函数一样。

价值概述：API流量峰值在夜间和凌晨，每分钟最高120k请求，每小时超过5M请求。 智利zapping客户将在第2季度至第3季度将服务扩展到秘鲁。

### 1.2特性场景分析

1、普通 gRPC 函数
2、自定义镜像 gRPC 函数

## 2. 特性/功能实现原理

[【需求】支持简单gRPC请求调用-Open Model](https://clouddragon.huawei.com/cloudmodeling/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=f2e34c7768dc4a55add564228479023c&versionUUID=)
![【需求】支持简单gRPC请求调用](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/faf179d585fb4219a24aa6d6d6679c07?t=1697114879198)

1. APIG 负责将用户 gRPC 请求作为 http2 请求发送给 rdispatcher;
2. rdispatcher 接受到 http2 请求后，解析 headers 和 body 字段，并封装到 apigEvent 中，以 http 请求发送给数据面 frontend;
3. 数据面 frontend 将请求一路透传到 worker，worker识别到 gRPC 字段后，将请求以 http2 的形式发送给 runtime(gRPC server);
4. worker 收到 runtime(gRPC server) 返回的 response，解析 body 请求体并封装到 http response 中，一路返回到 frontend 和 rdispatcher;

### 2.3 约束

1、当前只支持简单 gRPC 请求，不支持流式 gRPC 请求

#### 2.3.1上下游依赖需求

【编程模型】【智利zapping客户】函数支持gRPC调用 ：https://clouddevops.huawei.com/workitem/FE2023032900425
- 前端：增加 grpc 函数创建的选择
- APIG：透传用户的请求到 rdispatcher
- 管理面：rdispatcher获取用户请求中的headers和body，并构造apigEvents，发送http请求到数据面

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

不涉及目录权限、进程权限等相关安全内容修改，安全性影响可控

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

复用 http 函数可靠性设计

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及用户相关敏感数据

复用 http 函数可靠性设计

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

复用 http 函数可靠性设计

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

无

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

不涉及

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

无

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

新增 gRPC 请求相关用例

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

无

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

未增加资源消耗；

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

无

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

不涉及

## 4. 用例

| **序号** | **用例名**           | 用例类型 | **测试步骤**                         | **预期结果** |
| -------- |-------------------| -------- |----------------------------------|----------|
| 1        | 普通 gRPC 函数调用      | UT      | 创建普通 gRPC 函数，调用函数返回正常            | 函数调用正常   |
| 2        | 自定义镜像 gRPC 函数调用   | UT      | 创建自定义镜像 gRPC 函数，调用函数返回正常         | 函数调用正常   |
| 3        | 普通 gRPC 函数调用失败    | UT      | 创建普通 gRPC 函数，传入错误的apigEvent，调用失败 | 函数调用失败   |
| 4        | 自定义镜像 gRPC 函数调用失败 | UT      | 创建自定义镜像 gRPC 函数，传入错误的apigEvent，调用失败      | 函数调用失败   |

*注：属于内部可靠性优化，涉及增加用例，同时需要保证已有功能用例正常执行*