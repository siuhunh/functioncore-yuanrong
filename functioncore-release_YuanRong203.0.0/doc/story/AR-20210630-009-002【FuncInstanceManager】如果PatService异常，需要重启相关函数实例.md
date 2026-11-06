# 如果PatService异常，需要重启相关函数实例


## 1. Story 整体设计
### 1.1 功能描述
1.runtime与patService之间网络能够互通

2.runtime与worker可以感知patService的异常自动退出

AR 名称与实际开发对齐开发需求有差异，以开发功能描述为准
### 1.2 场景分析
用户从函数中访问其他云服务时，需要通过patService来打通网络

### 1.3 方案详细设计
[runtime与patService模块交互-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=4b2b1c53d55b474380bc2c7c4bbfb230&versionUUID=)![runtime与patService模块交互](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/qNr?t=1623894639584)

**流程描述**
- .workermanager在特化函数时（load接口），将patService相关信息给worker
- .worker收到请求后
    - 通过iptables打通网络配置
    - 通过ip命令修改路由信息，对于外部访问，需要配置网关为patService的ip
    - 异步ping vpc网关（间隔5秒，等待1秒，不重试），如果出现ping不通的场景，则认为是有异常，退出worker进程
- worker容器当前是sn用户，需要对应调整第三步的处理，因为iptables ,ip 这些命令需要有执行权限，优先当前版本功能调通，权限在后续调整

接口新增信息
```go
type PatServiceConfigure struct {
    HostVmCidr     string              `json:"hostVmCidr"`     //VM虚机网段
    PatContainerIP string              `json:"patContainerIP"` //容器IP
    PatGateway     string              `json:"patGateway"`     //绑定租户网关
    PatPodName     string              `json:"patPodName"`
    TenantCidr     string              `json:"tenantCidr"`
    NatSubnetList  map[string][]string `json:"natSubnetList"`
}
```


### 联调时间

6-21 联调（优先保证能够调通）


### 1.4 上下游依赖需求

1.workermanager接口传参正常

2.patService部署功能正常

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
| 1 |  创建带有vpc的函数调用 | 调用正常，网络通畅  |调用正常，网络通畅|
| 2 |  删掉patService |  对应的worker正常退出 | worker感知退出 |
