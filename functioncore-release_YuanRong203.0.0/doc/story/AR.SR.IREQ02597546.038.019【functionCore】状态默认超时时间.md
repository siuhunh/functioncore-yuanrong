【functionCore】状态Redis支持单独集群部署，多实例部署.md

## 1. Story整体设计

### 1. 功能描述
状态默认超时时间可配置，默认1小时

### 2. 场景分析
当前状态以stateID：state形式存储在redis中，采用命令Setex设置key，但参数TIMEOUT写死，需要改为从配置中读取。

### 3. 方案详细设计

-  在functiontask的config结构体中加入StateIDTTL字段，默认值为3600S。
- 在statequeue的初始化时将ttl值设置为StateIDTTL，后续传入redis时键值存活时间将由ttl决定。
- 在functiontask的yaml配置文件中加入stateid_ttl，默认值为3600。


### 4. 上下游依赖需求
无

## 2. 可信软件

1. 安全性（Security） 
   *【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 
   不涉及
2. 韧性（Resilience）
   *【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 
   不涉及
3. 隐私性（Privacy） 
   *【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
   不涉及
4. 可靠性（Reliability）
   *【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
   不涉及
5. 可用性（Availability）
   不涉及
6. 安全（Safety）
   *【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】
   不涉及

## 3. 用例

| 序号 | 用例名               | 测试步骤                                                     | 预期结果         |
| ---- | -------------------- | ------------------------------------------------------------ | ---------------- |
| 1    | 测试状态默认超时时间 | 1.进入functiontask容器，确认conf.json中stateid_ttl值为3600<br>2. 通过命令调用函数 curl -ki -H "Content-Type: application/json" -X POST -d '{"name":"zt"}' "http://7.185.19.150:31220/serverless/v1/functions/sn:cn:yrk:c53626012ba84727b938ca8bf03108ef:function:0@zt@invoke_js_addone:latest/invocations" -H "X-State-Key: {\"cmd\":\"invoke\",\"source\":\"rdispatcher\",\"statekey\":\"testaddone\"}"<br>1.在redis中用ttl指令查询stateID剩余存活时间 | 剩余时间接近3600 |

