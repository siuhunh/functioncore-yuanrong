# 函数POD 配置的capbility，使用完毕后释放


## 1. Story 整体设计
### 1.1 功能描述
- runtime manager 在启动函数后，释放capbility能力
- worker 在启动函数后，释放capbility能力
- runtime worker对外接口，安全加固
### 1.2 场景分析
#### runtime-capbility
当前runtime容器中中，因为runtime-manager需要拉起新的runtime进程，挂盘，杀进程等操作，runtime-manager被赋予了一些特殊的能力，详细见下表，这部分功能被赋予后，未及时删除，会存在被再次使用的风险
```yaml
# 忽略文件的DAC访问限制 用在GID UID需求
- DAC_OVERRIDE
# 忽略文件属主ID必须和进程用户ID相匹配的限制，用在GID UID需求
- FOWNER
# 停掉进程
- KILL
# 设置进程GID
- SETGID
# 设置进程UID
- SETUID
# 磁盘挂载
- SYS_ADMIN
```
#### worker-capbility
当前worker容器中，因为worker有修改网络信息的功能，被赋予了```NET_ADMIN```的能力，需要在使用后释放

#### 接口安全
当前接口分析结果

||接口|接口类型|处理分支|执行次数|详细内容||
|:--|:--|:--|:--|:--|:--|:--|
|runtime-manager|requestHandler|socket server 无鉴权|StopProcess|1|停掉进程 挂挂载 换UIDGID|1.接口只处理一次请求<br/>2.处理完成后去掉特殊的能力|
||||RequestFinished||worker client 发送的 可能存在多次 ，runtimeManager 收集的custom的日志，worker需要通过此来保证日志都收集到，一次，此消息是返回给worker的一个空的信息，此处理是为了保证时序||
||||GotRuntimeException|n|从异常的缓存map中获取异常信息||
||||||||
|worker|custom-readyPath|POST||1|当前逻辑内只执行一次，往clientReadySignal.c放了一条数据|函数初始化完成阶段，调用该接口，表示自定义函数进入ready阶段|
||custom-nextPath|GET||1|有校验request.InvokeID|runtime获取数据，runtime能拿到调用的数据|
||custom-responsePath|POST||n|runtime告诉custom函数执行情况，会存在异常调用|当前的安全机制<br/>1.大小的校验<br/>2.invokeID的校验，如果找不到会报错<br/>3.返回值最终的链路只是返回到前段，没有根据返回值做其他的处理|
||custom-errorPath|POST||1|解析runtime返回的错误|返回错误信息|
||||||||
||load|POST||1|有鉴权|内部会开启bus的TCP接口，见后续分析|
||healthz|GET||n|无鉴权|只获取健康信息|
||stop|POST||1|无鉴权|调用后worker会停掉，删掉|
||worker 最为server|socket server||n|有鉴权|建立连接的时候有鉴权，不带鉴权的不会接受他的消息，但是没有踢掉连接的处理|

### 1.3 方案详细设计
**runtime-capbility**
- 启动子进程前，去掉所有传递给子进程的capbility，防止能力扩散
- 做完重启子进程，挂载等操作之后，去掉当前进程capbility的功能

**worker-capbility**
- 修改完容器网络后，去掉NET_ADMIN```的功能

```go
pid2, err := capability.NewPid2(0)
err = pid2.Load()
pid2.Unset(capability.CAPS|capability.BOUNDS,capability.CAP_SYS_ADMIN)
pid2.Clear(capability.CAPS|capability.BOUNDS)
err = pid2.Apply(capability.CAPS|capability.BOUNDS)
```
**接口安全**

以上有梳理的接口有两个接口存在风险
- StopProcess接口只处理一次请求，后续请求类型均不作处理
- worker stop接口暂时没发现使用的地方，删掉


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
| 1 |  功能完整性测试 | 调用普通函数，挂载磁盘函数，修改用户ID函数  |函数功能正常|
| 2 |  功能完整性测试 | 调用挂载磁盘函数 |函数功能正常|
| 3 |  功能完整性测试 | 修改用户ID函数 |函数功能正常|
| 4 |  安全渗透测试 |  接口测试工具，测试到runtime或者worker的接口,需要测试辅助 | 无法攻击当前容器或平台 |
| 5 |  功能完整性测试 |  设置vpc测试worker修改net| 调用正常 |
| 6 |  功能完整性测试 |  日志写入检测| 日志写入正常 |
