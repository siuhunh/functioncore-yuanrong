[TOC]

# AR.20210522184626.007 防计费偷跑

## Story 整体设计

### 功能描述
**背景说明**  
用户函数调用返回后会统计函数计费信息：包括函数调用时长，CPU、 内存资源消耗等。但是目前不限制用户编写异步线程的函数，在函数调用返回后还可能继续占用 CPU、内存资源。  
**功能描述**  
需要限制用户进程在后台调度，绕过计费统计继续运行。  
**用户体验**  
用户无法绕过计费统计偷跑进程，在函数调用返回后用户进程停止运行。

### 场景分析
1. 进程状态分析
- D：uninterruptible sleep (usually IO)
- R：running or runnable (on run queue)
- S：interruptible sleep (waiting for an event to complete)
- T：stopped by job control signal
- t：stopped by debugger during the tracing
- W：paging (not valid since the 2.6.xx kernel)
- X：dead (should never be seen)
- Z：defunct ("zombie") process, terminated but not - reaped by its parent

    其中 T (TASK_STOPPED) 状态：向进程发送一个 SIGSTOP 信号，它就会因响应该信号而进入 TASK_STOPPED 状态（除非该进程本身处于 TASK_UNINTERRUPTIBLE 状态而不响应信号）。SIGSTOP 与 SIGKILL 信号一样，是非常强制的。不允许用户进程通过 signal 系列的系统调用重新设置对应的信号处理函数。  
   
   向进程发送一个 SIGCONT 信号（kill -18），可以让其从 TASK_STOPPED 状态恢复到TASK_RUNNING状态；或者 kill -9 直接尝试杀死。  
    
	可以通过给 runtime process 发送 SIGSTOP 信号让 runtime 进程挂起，通过 SIGCONT 信号恢复继续运行。

2. worker 支持并发操作，在 runtime 请求全部返回后挂起 runtime 进程及其所有子进程
- 单次调用的异步操作不做单独限制，但是保证所有的异步操作消耗的资源被统计到计费信息中。即单次函数请求返回后，只要有请求继续调用不会限制之前请求的异步操作，所有的资源消耗会被统计在新的请求调用计费信息中。
- 监听 runtime 当前的调用请求数，当请求数为 0 时发送 SIGSTOP 信号挂起 runtime 进程
- 记录 runtime 状态，当新的请求进来时如果 runtime 进程为挂起状态需要先发送 SIGCONT 信号恢复 ruuntime 进程状态为 running  
[防计费偷跑-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=bb444789be24489b8817704a9146c011&versionUUID=)![防计费偷跑](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/10eq?t=1629699393742)

3. 函数最小实例场景不挂起 runtime 线程  
使用预留实例的情况下，资源消耗计算稍有不同，FunctionGraph根据预留实例设置在实例创建成功后开始计时。如果预留实例存活时间不足1分钟，将按照 1 分钟计算，超过一分钟的部分，按照秒的粒度向上取证计算。例如一个512M的函数预留实例运行时间为 51 秒，将按照1分钟计算，计算资源消耗为 0.5G * 60s = 30GB-秒。


### 方案详细设计

#### 方案设计
1. runtimeManager 提供 runtime 进程及其所有子进程挂起和恢复接口  
进程挂起：syscall.Kill(process.Pid,syscall.SIGSTOP)  
进程恢复：syscall.Kill(process.Pid,syscall.SIGCONT)

2. worker 监听 runtime 当前的调用请求数，当调用请求数为 0 时调用 runtimeManager 的进程挂起接口  
设置进程挂起等待时间，支持可配置，默认 3s。3s 后请求数仍为 0 则调用 runtimeManager 接口挂起 runtime 进程  

3. worker 记录 runtime 进程状态。
- 当新的请求进来时如果 runtime 进程为挂起状态需要先调用 runtimeManager 的进程恢复接口
- 监听 runtime 当前的调用请求数不为 0 ，判断 runtime 进程状态是否为 running,不为 running 则调用 runtimeManager 进程恢复接口

4. 异常场景处理  
- 调用进程挂起接口失败。 ———— 设置重试，重试 3 次。重试重新判断请求数是否为 0，不为 0 取消继续请求。重试后仍然失败则 worker 容器优雅退出。
- 调用进程恢复接口失败。———— 设置重试，重试 3 次。重试后仍然失败则 worker 容器优雅退出。
- 调用进程挂起接口时有新的请求调用进入，如何保证多个信号的执行顺序 ———— 对 runtime 进程的最终状态保证准确性。除了在函数请求时调用时进程恢复恢复接口，另外还监听进程状态和当前请求数是否正常。  

5. worker 增加标签标记是否为最小函数实例 pod  
针对最小函数实例的场景,workermanager 在拉起 worker 和 runtime pod 时传递一个标签来标记是否为最小函数实例 pod。worker和runtime 如果为最小函数实例 pod，则跳过以上防计费操作，即不挂起 runtime 进程。

6. 增加开关控制是否开启防计费偷跑功能，默认为 false
| 配置项  | 说明  | 默认值  |
| ------------ | ------------ | ------------ |
| backgroundProcessLimit.enable  | 是否开启防计费偷跑功能 |  false |
| backgroundProcessLimit.idleTime | worker请求数为 0 后多久挂起 runtime 进程，单位秒 | 3  |
| backgroundProcessLimit.retryTimes  | 发送挂起/恢复信号失败重试次数  | 3  |

#### 外部交互分析&&上下游依赖需求
无

### 对外接口
无

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
|  序号 |   用例名  | 测试步骤 | 预期结果  |
| ------------ | ------------ | ------------ | ------------ |
| 1 | 防用户进程后台运行 |  编写函数，异步运行，在函数执行返回后进入 runtime 容器里面查看进程状态 | runtime 进程为中断停止状态，用户异步线程无法运行 |
| 2 | runtime 进程恢复运行 | 编写函数，在函数执行返回后 runtime 进程中断停止，重新调用该函数 | 函数正常调用，runtime 进程恢复运行状态 |