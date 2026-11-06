[TOC]

# AR.SR.IR20230331002271.010.001 init阶段函数互调后关闭worker与bus-proxy的链接防止冲突，同时保证互调功能不受runtime异常的影响


## Story 整体设计

### 功能描述
**背景说明**  
ERP场景下，通过ServiceBridge接入的微服务，需要在worker 的 init阶段支持函数互调，即在init阶段与bus建链，作为client端发请求，调用别的函数；bus启动一个server端，开启32569端口，处理worker发送过来的请求。在此背景下，存在可靠性场景的风险
![](./images/1684142994971_image.png)

**功能描述**  

init阶段函数互调后关闭worker与bus-proxy的链接防止冲突，同时保证互调功能不受runtime异常的影响

**用户体验**  
可靠性场景，属于内部优化需求，外部用户不感知

### 场景分析
为了提高该场景的可靠性，面对现网的复杂情况，需要做以下三点的可靠性加强
1、init阶段结束后，worker需要主动断开与functionbus的链接，并通过原先具备的链接对worker进行纳管
2、Java函数能够继续正常支持函数互调的能力,并采集业务相关的日志，需要确认其他runtime是否具备了这个能力，并且能够精确采集日志
3、Runtime异常退出，走bus正常纳管的链路通信，重新走HotLoad逻辑拉起Runtime，然此时load过程中存在MessageQueue阻塞Bus发来的请求，而MessageQueue又需要等待Hotload执行完成方可处理请求，形成死锁，导致函数超时

### 方案详细设计

#### 方案设计

1、场景一：Worker完成init之后，达到了ready的状态，worker与bus建立了新的链接之后（32568端口），主动断开init阶段与bus建立的的链接（32569端口），在确认worker被bus纳管之后，worker清理之前init阶段与bus建立的链接（TCPbus），并drop这个端口
![](./images/1684163965079_image.png)


2、场景二：通过Python、Node.js的函数互调的用例，采集init阶段函数调用的日志传到worker，确保这Runtime在init阶段的日志能够被worker与LTS服务采集（函数互调只涉及Runtime的验证，不涉及Custom 与 HTTP函数）

3、场景三：worker已经到了ready状态，成功的被bus纳管，但是之后runtime发生了异常退出，worker通知runtime-manager重新拉起这个Runtime，runtime重启之后worker会走hotload的逻辑来拉起Runtime。但是这种情况下messagequeue会阻塞住，要等Hotload执行完毕才能继续执行请求
为了解决这种问题，针对worker的messagequeue对象要做处理，新增一个Error方法以及error字段，来存储当前队列里的错误信息，Runtime发生重启、Hotload出现异常状况加载失败，或者worker与runtime需要重连等异常情况，在messagequeue队列里添加错误信息；并且队列在执行WaitUnblock方法时（仅在bus回调adaptor时使用），若判断队列中存在错误信息，不执行等待逻辑，直接处理请求
![](./images/1684248617137_image.png)
#### 外部交互分析&&上下游依赖需求
暂无

> **丽萍测的可靠性问题**
>init 阶段调函数B（不存在的规格），函数B特化失败，导致函数B调用失败，可以失败不能阻塞
>

### 对外接口
内部可靠性优化，不涉及外部接口
## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*
drop掉worker的32569端口

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*
内部优化，不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*
内部优化，不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*
本需求为可靠性遗留问题

#### 可用性 Availability
*处于按要求执行状态的能力*
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*
不涉及

## LLT 用例
|  序号 | 用例名        | 测试步骤 | 预期结果      |
| ------------ |------------| ------------ |-----------|
| 1 | worker ready 之后清理TCP链接成功| worker init结束释放client，bus关端口  | 正确释放网络资源 |
| 2 | worker ready 之后清理TCP链接失败|  构造错误导致无法释放client与port | 清理链接失败 |
| 3 | Python3.9函数完成在init阶段互调并成功采集日志 |  主被调函数执行成功，采集日志  | 执行成功  |
| 4 | node.js14函数完成在init阶段互调并成功采集日志 |  主被调函数执行成功，采集日志  | 执行成功 |
| 5 | worker在ready，runtime重启后，正常hotload |  执行命令杀runtime，重新拉起后再执行互调函数  | 执行成功 |
| 6 | worker在ready，runtime重启后，hotload失败 | 报告响应错误，请求失败   | invoke 失败 |