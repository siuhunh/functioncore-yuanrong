# 【functioncore】支持C++ graph兼容 AR.SR-20210630-001.006

## 1. Story 整体设计

### 1.1 功能描述

功能：支持 c++ runtime 兼容 graph

1）支持layer
2）支持界面定义入口函数的方式
3）支持多并发
4）支持context完整接口
5）支持提供日志接口和支持标准输出日志
6）支持环境变量设置，支持环境变量加载到context
7）支持graph ak，sk传递。

### 1.2 场景分析

支持通过二进制的方式，调用 c++ 函数

### 1.3 方案详细设计

[cpp11-bin runtime 调用流程-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=5ff5bd47088749eda62943c0eeb7d145&versionUUID=)![cpp11-bin runtime 调用流程](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/1Prj?t=1642579151459)

- instance manager 发送 cpp11-bin 语言的特化请求

- worker 收到请求后，向 runtime manager 发送请求，停止其他语言的 runtime 进程，同时拉起 cpp11-bin 的 runtime 进程。
- worker 与 cpp11-bin runtime 进程建立 tcp 连接
- worker 启动定时任务，定时去做健康检查，检查 cpp11-bin runtime 是否正常

新增的变化：

- runtime-cpp 当前只编译出了 .so 文件，没有提供静态链接库，本次需提供静态链接库
- 用户文档中要重新说明 CMakeFiles.txt 的写法，增加 target_link_libraries(handler，/path/libglog.a，lpthread -lm）以支持静态链接库

约束：

- cpp 函数目前只支持 cpp bin 二进制的方式调用函数

### 1.5 增加的配置项

```yaml
"cpp11-bin": {
      "port": 31542,
      "enable": false
 }
```



### 1.6 上下游依赖需求

上游需求：AR.SR-20210630-001.005【functionCore】支持C++编译环境 丁震

下游需求：AR.SR-20210630-001.004【functionCore】支持C++函数，支持函数间invoke 丁震

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

| 序号 | 用例名                                                       | 测试步骤                                                     | 预期结果 |
| ---- | ------------------------------------------------------------ | ------------------------------------------------------------ | -------- |
| 1    | hello world 基础用例                                         | 1. 按照文档中的 CMakeFiles.txt 指导，编写本地 CMakeFiles.txt 文件<br />2. 本地编译 c++ 二进制<br />3. 上传二进制压缩包<br />4. 调用函数 | 调用成功 |
| 2    | 1）支持layer<br/>2）支持界面定义入口函数的方式<br/>3）支持多并发<br/>4）支持context完整接口<br/>5）支持提供日志接口和支持标准输出日志<br/>6）支持环境变量设置，支持环境变量加载到context<br/>7）支持graph ak，sk传递。 | 分别测试这些特性是否支持                                     | 支持     |