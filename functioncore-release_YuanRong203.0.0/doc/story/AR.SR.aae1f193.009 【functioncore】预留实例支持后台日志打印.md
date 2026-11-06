[TOC]

# 【functioncore】预留实例支持后台日志打印

AR.SR.aae1f193.009

## Story 整体设计

### 功能描述
用户可以查看预留实例在非调用状态下的日志

### 场景分析
预留实例因为一直收费，需要支持用户启动后台线程时，打印的日志也能在lts显示

### 方案详细设计
worker在收到从

1. rpc类函数（nodejs、java、python等）通过rpc接口传回的用户日志时
2. 其他类函数（直接启动用户二进制，http、custom-runtime，go）通过runtime-manager传回的用户日志时
3. 自定义镜像通过收集docker日志时

如果当前有没有请求，就额外往用户日志文件写入一条记录，使用默认的traceID：“default”

示例：

{"Level":"INFO","log":"xxxx","podName":"pool24-600-512-fusion-77b954f6b5-2qblx","package":"default","function":"zcy_python_long","version":"latest","stream":"stdout","instanceId":"633b4587-cd04-4ec0-baaf-24341a734aaa","newLogGroupId":"","newLogStreamId":"","requestId":"default","time":"2022-07-10T10:08:36Z","errorType":"","status":"success","stage":"load","finishLog":"true","livedataTraceId":""}

用户可以在lts中通过instanceId过滤得到某实例全部日志


#### 外部交互分析&&上下游依赖需求
前端函数日志界面增加以instanceId过滤的功能，已与郑亚林对齐，前端不影响功能转测，日志都会上报在lts中

### 对外接口
无

### 约束
无

## 可信软件

#### 安全性 Security
不涉及

#### 韧性 Resilience
不涉及

#### 隐私性 Privacy
不涉及

#### 可靠性 Reliability
不涉及

#### 可用性 Availability
不涉及

#### 安全 Safety
不涉及

## LLT 用例
| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | ------------------------------------ | ---------------------------------------- |
| 1 | 用户后台日志打印正常 | 1.预留实例执行一个函数调用，开启后台进程输出日志 | 1.LTS日志展示正常 |
