# Worker增加配置项配置 cpu 和 memory 资源

关联AR： 

        AR.20210602125401.001: Worker 增加配置项配置 cpu 和 memory 资源
        
## 1. Story 整体设计
### 1.1 功能描述
   runtime 的定义的几种资源规格， 只是定义了 limits 规格及资源上限， 但是对于 request 资源下限，是写在代码里面的，并没有提供可配置的文件进行修改
   
### 1.2 场景分析
   金融Var计算场景下， 函数多为CPU密集型任务， 
   但是当前 core 中， 不管是何种规格大小的 runtime， 分配的 request 资源大小写死在代码里面 cpu:200m, memory：300Mi，
   这就导致当整个集群资源不足的情况下， 拉起的实例仅仅满足 request 所定义的下限资源， 而达不到 limits 定义的资源上限。
   所以当 CPU 密集型任务计算的时候， runtime 所使用的资源达不到定义的规格
   
        【注】：Request: 容器使用的最小资源需求，作为容器调度时资源分配的判断依赖。 
        只有当节点上可分配资源量>=容器资源请求数时才允许将容器调度到该节点。 但Request参数不限制容器的最大可使用资源。 Limit: 容器能使用资源的资源的最大值，设置为0表示使用资源无上限。
       
### 1.3 方案详细设计
   在 nodemanager 中的配置文件中， 定义池子信息的时候， request cpu 和 request memory 都体现在配置文件中， 默认值依旧按照 cpu:200m, memory：300Mi
   当用到不同场景的时候， 能够给运维人员部署 core 的时候提供一个修改的入口
   requestcpu 和 requestmemory 约束： 小于等于limitcpu 和 limitmemory
   
例如：node-manager 的 node-manager.conf 配置
```
{
"name": "pool1",
"limitcpu": 500,
"limitmem": 500,
"requestcpu": 200,
"requestmem": 300,
"poolsize": 1,
"runtimename": "nodejs10.15.2",
"annotations": {
"app": "pool1"
},
"label": {
"app": "pool1"
}},
```
pool1的资源分配如下
```
 resources:
          limits:
            cpu: 500m
            ephemeral-storage: 500Mi
            memory: 500Mi
          requests:
            cpu: 200m
            ephemeral-storage: 500Mi
            memory: 300Mi
```
    
### 1.5 上下游依赖需求
不涉及
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
| 1 |  部署测试 | 部署 functioncore 的时候， 修改发布包chars里 nodemanager 中node-manager-config.yaml 的 requestcpu 和 requestmemory| 相应pod的资源得到改变|
| 2 |  部署测试 | 部署完 functioncore, 修改 nodemanager configmap 中的 requestcpu 和 requestmemory | 相应pod的资源得到改变  |

