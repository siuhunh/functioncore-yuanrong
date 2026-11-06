[TOC]

# 【functionCore】支持单实例多并发函数计费


## 1. Story整体设计
### 1. 功能描述
1）元戎数据面统计单实例多并发函数被调用时“资源占用时长”数据并上报到redis, FunctionGraph管理面从redis采集该数据进行计费。

### 2. 场景分析
当一个函数的配置项“单实例并发数” > 1 时，worker 收到并发的调用请求，存在多个函数调用请求到响应的时间窗口重合的情况。如果在这种场景还是按每次调用响应时携带的计费时长进行计费，相比单实例单并发计费无任何优势。
所以单实例多并发函数，按资源占用时长进行计费。例如当一个函数执行需要花费5秒，三次函数调用请求分别在三个函数实例执行，三个实例总的执行时长为15秒，如果让三个请求在一个实例并发处理，这样实例占用时长只有5秒，大大节省了用户实例资源费用。

分析资源占用时长可能存在场景如下：
[单实例多并发计费-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=410e6d03612040508a8c123f36bf82d0&versionUUID=)
![单实例多并发计费](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/16qN?t=1633950865469)

资源占用时长在worker进行统计，只有在函数配置项“单实例并发数” > 1 时，才开始统计“资源占用时长”；
资源占用时长统计周期为一分钟；资源占用时长统计成功后上报到functiontask, functiontask收到上报的资源占用时长后写入到redis；
具体统计上报流程如下：
[单实例多并发计费上报流程-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=87975b4e1abd4d439b59d1fc29431608&versionUUID=)
![单实例多并发计费上报流程](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/16qh?t=1633952730556)

### 3. 方案详细设计

#### 3.1 周期统计资源占用时长
worker新增一个结构体用于统计并发调用的资源占用时长：
````
type ConcurrentResource struct {
	requestCount     int32
	startTime        time.Time
	startTimeMutex   sync.Mutex
	timeWindows      []TimeWindows
	timeWindowsMutex sync.Mutex
	cleanup          sync.Once
}
````
1. 调用请求新增属性 Concurrency：为当前函数的并发度，用于判断此次请求是否为并发调用。如果是则要统计其资源占用时长。
2. requestCount 字段用于统计并发资源占用时长：每当有调用请求（Concurrency>1）进入，requestCount 递增，每当有调用响应（Concurrency>1）返回，requestCount 递减，
   所以资源占用时长就是 requestCount 从 0->1->2->n->2->1->0 值变化过程的时间窗口。
3. startTime 字段为并发资源占用时长的开始时间（首个请求的计费开始时间，即 requestCount 递增为1时的 summary.StartTime）。
4. timeWindows 字段，用于存储已统计的并发资源占用时间窗口。 每当 requestCount 递减为0时，向 timeWindows 数组 append 一个元素，元素结构为 {startTime, endTime, billingDuration} ,
   endTime 为并发资源占用时长的结束时间（最后一个请求的计费结束时间，当 requestCount 递减为0时的 summary.EndTime）；并将 startTime 置为ZeroTime；
5 因为对 startTime 的读取写入涉及并发数据竞争，所以添加了互斥锁 startTimeMutex。数据竞争场景如下：
   （1）requestCount 递减为0要读取 startTime ，如果同时又请求进来 requestCount 递增为1要写入 startTime。
   （2）获取已统计的并发资源占用时间窗口时，如果当前正在并发调用，会生成一个资源占用时间窗口，涉及读取更新 startTime，如果此时 requestCount 递减为0，也会读取更新 startTime。
6. 因为对 timeWindows 的读取写入涉及并发数据竞争，所以添加了互斥锁 timeWindowsMutex。数据竞争场景如下：
   （1）获取已统计的资源占用窗口，如果当前正在并发调用，会生成一个资源占用时间窗口并append到timeWindows，如果此时 requestCount 递减为0，会生成一个时间窗口并append到 timeWindows。
   （2）上报成功后清理timeWindows内已上报成功的资源占用窗口，如果此时 requestCount 递减为0，会生成一个时间窗口并append到 timeWindows。 
7. cleanup 用于首次调用清理初始化或启动的时间窗口，因为初始化或启动会向 timeWindows 添加一个时间窗口，如果函数一开始并发度为 1不应该上报这个窗口导致重复计费，
   所以首次调用的Concurrency == 1时，如果有初始化和启动，需清理初始化或启动的时间窗口。

资源占用时长窗口结构体如下：
````
type TimeWindows struct {
	StartTime       int64 `json:"startTime"`       // unit: ms
	EndTime         int64 `json:"endTime"`         // unit: ms
	BillingDuration int   `json:"billingDuration"` // Charging time (unit: ms)
}
````
StartTime为并发资源占用开始时间
EndTime为并发资源占用的结束时间
BillingDuration为计费时长，即EndTime - StartTime的时长，不足一毫秒按一毫秒进行计费。

注：
1. 资源占用时长计费在管理面按秒级计费，数据面上报时长单位为毫秒。
2. 性能实例：计量粒度为1毫秒，总计费时长小于1分钟按1分钟计算。例如一个性能实例按量模式下运行时长为51毫秒，将按照1分钟计量；运行时长为60001毫秒，则按照实际时长60001毫秒计量。
3. 初始化或启动成功会向timeWindows添加一个初始化或启动的时长窗口用于计费，失败场景，会将失败计费上报redis，所以不要重复上报。

#### 3.2 worker定时上报并发资源占用时长窗口
worker定时上报已统计的并发资源占用时间窗口数组；上报时间间隔可配置，默认5分钟；
每次上报先获取 periodStartTime 和 periodEndTime ，periodStartTime 为上一个统计周期的 periodEndTime，periodEndTime 为当前时间;
获取已统计的并发资源占用时间窗口数组方式如下：
1. 如果 ConcurrentResource.startTime == 0 说明该函数实例当前没有被并发调用
   （1）如果 len(usedResourceTimeWindows) == 0 说明没有需上报的资源已占用时长数据
   （2）如果 len(usedResourceTimeWindows) > 0 说明有需上报的资源已占用时长数据。 所以将 timeWindows 内的元素全部上报.
2. 如果 ConcurrentResource.startTime > 0 说明该函数实例当前正在被调用
   （1）如果 ConcurrentResource.startTime before periodStartTime ，说明并发资源占用的开始时间在periodStartTime之前，所以timeWindows新增资源占用时长窗口为 [periodStartTime, periodEndTime]。
   上报成功后更新 ConcurrentResource.startTime = periodEndTime。
   （2）如果 ConcurrentResource.startTime after or equal periodStartTime 说明并发资源占用的开始时间在periodStartTime之后，所以timeWindows新增资源占用时长窗口为 [startTime, periodEndTime]。
   上报成功后更新 ConcurrentResource.startTime = periodEndTime。
   
在得到已统计的并发资源占用时间窗口数组timeWindows后, 如果时间窗口个数大于1个，需对其进行二次聚拢合并成一个时间窗口。
合一的时间窗口:
StartTime = timeWindows[0].StartTime
EndTime = timeWindows[num-1].EndTime
BillingDuration = timeWindows 内所有时间窗口的 BillingDuration 的总和。

注：
1. StartTime 和 EndTime 只做参考，不能用于计费计算。
2. 二次聚拢合前的数据需落盘，用于后续审计，落盘路径/home/sn/log/service/billing.log。
3. 二次聚拢后的 BillingDuration 不得超过 统计周期 * 60000ms , 超过则重置为 统计周期 * 60000ms。
4. 函数实例退出前进行最后一个上报，避免并发资源占用时长数据丢失。

异常场景：
1. runtime 异常重启场景，资源占用时长如何统计？
当 runtime 异常重启时，worker 继续接收调用请求并缓存到一个队列。当 runtime 重启成功，将队列中的请求出队处理，出队处理去请求走原先逻辑，不影响资源占用时长的计算。同时缓存队列不进行计费。

2. worker 和 runtime-manager 异常
当 worker 和 runtime-manager 异常函数实例会退出，在优雅退出阶段进行最后一次并发资源占用时长数据上报。 如果异常退出时 ConcurrentResource.startTime > 0 ，补充上报资源占用时长[startTime, nowTime]。
因为资源占用时长不考虑因系统异常导致请求失败的场景，资源被占用即计费。

3. functionbus 异常重启
functionbus 异常重启，worker上报并发资源占用时长请求会失败，上报失败不丢失数据，保留到下一次一并上报。

4. redis 一段时间不可用
当 redis 一段时间不可用，functionbus 响应返回失败错误码，worker 收到响应错误码为失败时，不丢失上报数据，保留到下一次一并上报。

5. 持续未上报成功
   因为并发资源占用时长数据会进行二次聚拢，所以每次上报只有一条时长窗口数据，持续一小时未上报成功也就12条数据，一条数据占用24byte数据
   所以worker200M内存，分100M内存用于存储时间窗口数据，支持连续15170天数据持续未上报成功。
   
6. 极端场景无法上报
   （1）redis持续不可用; 一个worker连续15170天数据持续未上报成功，此类场景基本不会出现。
   （2）functionBus持续不可用；functionBus不可用，也就没有调用请求到worker，所以也就没有需要上报的资源占用时长。
   （3）worker实例直接退出，例如节点机器直接下线场景，导致worker直接退出，未走优雅退出逻辑，无法进行最后一次资源占用时长上报；
    如果之前的周期上报成功，则最多丢失默认五分钟的数据，如果之前周期都为上报失败，例如（1），则会丢失大量数据。
   但是真实资源占用窗口数据已落盘，可以进行恢复。
   

#### 3.2 functiontask接收上报资源占用时长接口设计
FunctionTask 内的 FunctionBus 新增一个 reportRuntimeMetrics 接口，接收来自 worker 上报的资源占用时长请求。
新增消息类型 pb.Message_REPORT_RUNTIME_METRICS_REQUEST
bus.proto 新增消息结构如下：
````
message ReportRuntimeMetricsRequest {
    string FunctionUrn = 1;
    string PodName = 2;
    // ConcurrentInstance; ReservedInstance
    string MetricsName = 3;
    string MetricsValue = 4;
}

message ReportRuntimeMetricsResponse {
        // Success: 0, Failed for others
        uint32 ErrorCode = 1;
        // Message for error
        string ErrorMessage = 2;
}
````
失败场景：FunctionBus连接的redis不可用，需返回错误信息给worker。

#### 3.3 上报到redis的计费信息
functionbus收到上报的资源占用时长指标后，将其转换成json字符串，插入到 redis 的列表 fss-multiple-concurrent-billing-data
````
{ 
   "functionURN" ： "projectID@package@functionName@version@memsize" , 
   "podName" : "pool23-400-256-fusion-59bd9fb656-vfcl9"
   "timeWindows" : [{"startTime": xxxx, "endTime": xxxx, "billingDuration": xxxx}]
}
````
functionURN: 函数URN
podName: 函数实例名称
timeWindows: 资源已占用时长窗口 
startTime: 时长窗口开始时间戳，单位毫秒
endTime: 时长窗口结束时间戳，单位毫秒
billingDuration: 计费时长（endTime - startTime; 不足一毫秒向上取整按一毫秒计费）单位毫秒

#### 3.4 新增计费请求头
新增计费请求头：x-billing-duration，只有调用请求并发为1时，其值计费时长。预留实例或并发度>1的请求，值为0

### 4. 上下游依赖需求
functionGraph 管理面 funcmonitor 组件采集上报到redis的资源占用时长数据（梁新宜）。

## 2. 可信软件

1. 安全性（Security） 
   *【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 
   不涉及，计费信息无敏感信息
2. 韧性（Resilience）
   *【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 
   不涉及
3. 隐私性（Privacy） 
   *【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
   不涉及
4. 可靠性（Reliability）
   *【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
   1. 当functionbus不可用，worker无法上报资源占用时长时，未上报成功的数据不清除，直到下一次上报成功为止。
   2. 当worker实例退出时，需将资源已占用时长数据上报成功后，才可继续优雅退出。
5. 可用性（Availability）
   不涉及
6. 安全（Safety）
   *【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】
   不涉及
## 3. 用例

| **序号**  | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ----------------------------------------------------------- | ----------------------------------- | -------------------------------------- |
| 1        | 函数配置单实例并发度为1，调用该函数一次                            | 管理面创建函数，设置单实例并发度为1，调用该函数后，查询redis的ConcurrentResourceFeeList列表 | 列表内无该函数的数据 |
| 2        | 函数配置单实例并发度为2，调用该函数一次                            | 管理面创建函数，设置单实例并发度为2，调用该函数后，查询redis的ConcurrentResourceFeeList列表 | 列表内有该函数的数据 |
| 3        | 函数配置单实例并发度为5，非并发调用5次                             | 管理面创建函数，设置单实例并发度为5，非并发调用5次，查询redis的ConcurrentResourceFeeList列表 | 列表内有该函数的数据，且上报的资源占用时长元素为5个 |
| 4        | 函数配置单实例并发度为10，并发调用10次                             | 管理面创建函数，设置单实例并发度为10，并发调用10次，查询redis的ConcurrentResourceFeeList列表 | 列表内有该函数的数据，且上报资源占用时长窗口为1个 |
| 6        | 函数配置单实例并发度为100，不间断的并发调用                         | 管理面创建函数，设置单实例并发度为100，不间断的并发调用，查询redis的ConcurrentResourceFeeList列表 | 列表有该函数的数据，且上报的资源占用时长窗口连续 |
| 7        | 函数配置单实例并发度为100，并发调用10次，全部响应后再并发调用10次      | 管理面创建函数，设置单实例并发度为100，并发调用10次，全部响应后再并发调用10次，查询redis的ConcurrentResourceFeeList列表 | 列表有该函数的数据，且上报资源占用时长窗口为2个 |
| 8        | runtime-manager/worker异常时资源占用时长的统计                   | 管理面创建函数，设置单实例并发度为100，并发调用，模拟runtime-manager/worker异常，查询redis的ConcurrentResourceFeeList列表 | 列表有该函数的数据，时间窗口连续且在实例退出后终止 |
| 9        | runtime异常时资源占用时长的统计                                  | 管理面创建函数，设置单实例并发度为100，并发调用，模拟runtime异常，查询redis的ConcurrentResourceFeeList列表 | 列表有该函数的数据，时间窗口列表中间有一段间隔未占用资源空隙（runtime异常重启时间窗口） |
