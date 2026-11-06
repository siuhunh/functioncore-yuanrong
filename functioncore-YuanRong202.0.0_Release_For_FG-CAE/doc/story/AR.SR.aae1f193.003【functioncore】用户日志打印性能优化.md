[TOC]

# 【数据面】**【functioncore】用户日志打印性能优化**  

## Story 整体设计

### 功能描述



针对language为custom-runtime或者go1.x或者http或者custom-container类型语言的函数，当调用这些函数时，worker针对每个函数调用请求所携带的traceID创建一个logRecorder， logRecorder中有一个长度大小为64的LogChan用于存放用户日志消息，当worker收到runtime发送过来的写日志请求时，日志信息先被放入LogChan队列中，然后才从LogChan队列中取出写入文件中。当写日志量比较大且从LogChan取出日志消息的速度小于往LogChan存日志的速度时，当前worker代码逻辑不是阻塞等待LogChan有空间存储日志消息，而是直接丢弃此条日志消息，在这种情况下用户日志不会被写入文件中，就会存在用户日志丢失的情况。

针对language不在上述四个范围内的其他语言类型函数调用，worker是针对每一个请求所携带的invokeID创建一个logRecorder，  logRecorder中同样有一个长度大小为64的LogChan用于存放用户日志消息，当worker收到runtime发送过来的写日志请求时，日志信息先被放入LogChan队列中，然后才从LogChan队列中取出写入文件中。当写日志量比较大且从LogChan取出日志消息的速度小于往LogChan存日志的速度时，当前worker代码逻辑不是阻塞，而是直接丢弃此条用户日志消息， 因此此条用户日志不会被写入文件中。

因为用户日志对用户问题定位至关重要，因此需要对当前写入阻塞就丢弃的逻辑进行优化。

优化的目标有两个：

1. 用户日志不能丢弃
2. 用户日志不丢弃情况下函数调用性能不能变差



### 场景分析

现状分析

当前现状是: 

[用户日志打印性能优化](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=289c1b053b67447ab8175524edfe8674)![用户日志打印性能优化](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/24xk?t=1655285873058)

1.  worker 针对每次请求调用的traceID或者requestID存有如下结构的一个recorder， 其中LogChan 大小为64， 用来存放runtime发送给worker的日志消息

   ```
   // LogRecorder record user log of function
   type LogRecorder struct {
      f      *FunctionLogger
      writer *zap.Logger
      // LogChan store Log of local function
      LogChan   chan *FunctionLog
      localLogs []*FunctionLog
      // LogHeader system log header
      logHeader *FunctionLog
      // logFooter system log footer
      logFooter *FunctionLog
   
      stopCh chan struct{}
      // Log of remote function, which local function called
      remoteLog string
      // livedataID user defined requestID
      livedataID string
      // requestID identifies request
      requestID string
      // LogOption means whether to bring back logs
      LogOption string
      // log level of a function, not log level of a invocation
      logLevel string
   
      // stage initialize/invoke
      stage string
      // withLineBreak
      separatedWithLineBreak bool
      // Count increase when LogRecorder is created and reduce when logChan is to be close
      count int32 // atomic
   
      sync.RWMutex
      wg sync.WaitGroup
   }
   ```

2. worker在处理每个请求时，会启动一个go 协程用于专门处理和当前请求的日志消息的写文件操作

3.  用户函数在函数中通过context.getLogger()方式获取runtime的logger给worker发送写日志的rpc请求，worker接收到请求后，先将日志消息存于LogChan中， 日志消息结构体是FunctionLog

   ```
   // FunctionLog is format of function logs which are flushed
   type FunctionLog struct {
      Time         string `json:"Time"`
      Level        string `json:"Level"`
      Message      string `json:"MSG"`
      FunctionName string `json:"FunctionName"`
      // Not json field
      TraceID       string
      ErrorType     string
      IsFinishedLog bool
      IsStdLog      bool
      // LivedataID user defined requestID
      LivedataID string
      // Stage initialize/invoke
      Stage string
      // index
      Index int
   }
   ```

4. 当LogChan有容量时，日志消息直接放到LogChan中，当LogChan满时，当前逻辑是直接丢弃此条日志消息，并且同时打印一行
5. worker在处理请求时，在协程中进行从LogChan取日志消息，然后通过zap日志框架进行写文件的操作

### 方案详细设计

#### 优化后的方案

[用户日志打印性能优化最终方案](https://cloudmodeling.tools.huawei.com/myspace/diagram?diagramId=5759283574af4dcda331da012eaa2794)![用户日志打印性能优化最终方案](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/25Fd?t=1655300592461)

1. 针对每个requestID或者traceID创建的Recorder的LogChan大小由64调整为1000
2. worker启动时启动一个协程， 协程拥有一个全局队列userFuncLogChan， worker处理请求时，不直接对从LogChan取出的日志消息进行写文件操作，而是将日志消息从LogChan取出放入userFuncLogChan队列，worker启动的时候启动的那个协程中每次遍历userFuncLogChan时，取len(userFuncLogChan)条日志消息进行一次io批量写入文件的操作
3. 原来在worker处理请求的时候启动一个go协程用于从LogChan取出日志消息进行写文件操作优化为从LogChan取出日志消息进行放入userFuncLogChan队列
4. 一个日志消息进行一次写文件操作优化为每次遍历userFuncLogChan时，取len(userFuncLogChan)条日志消息进行一次io批量写入文件的操作



# 开源软件选型

无

#### 外部交互分析&&上下游依赖需求
无

### 对外接口
*描述对外新增的接口*
+ 无
### 约束
无

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

#### 可用性 Availability
*处于按要求执行状态的能力*

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*

## LLT 用例
| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | ------------------------------------ | ---------------------------------------- |
| 1 | 用户日志打印正常 | 1.执行一个函数调用<br /> |1.LTS日志展示正常<br/>2. 函数日志页面监控->日志->函数日志页面展示日志正常|
| 2 | 用户日志不会丢弃 | 1.使用压测脚本执行大批量函数调用 |1.观察用户日志打印条数是否和预期一致|
| 3 | 观察用户日志性能 | 1.做性能压测 | 1.日志不会成为函数调用耗时瓶颈                               |

