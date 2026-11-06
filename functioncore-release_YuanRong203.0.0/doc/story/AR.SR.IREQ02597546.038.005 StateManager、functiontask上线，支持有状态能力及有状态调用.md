[TOC]

# 【functionCore】functionTask 上线，支持有状态能力及函数调用 AR.SR.IREQ02597546.038.004
# 【functionCore】StateManager上线，支持有状态能力及有状态调用 AR.SR.IREQ02597546.038.005
# 【functioncore】functiontask与state-manager之间的安全加密 AR.SR.IREQ02597546.038.006

## Story 整体设计

### 功能描述
**背景说明**
**功能描述**  
**用户体验**
在FunctionGraph上线元戎有状态函数功能特性，支持用户创建并执行有状态函数，进行状态的创建、修改和删除，函数界面前端能够正确返回状态结果

### 场景分析
functiontask开启有状态功能开关，支持有状态函数调用请求
新增部署statemanager组件
functiontask与statemanager之间改为https通信，增加ak、sk验签

#### 方案设计
functiontask 配置变化 
  
  填写已有state_server_addr字段为状态持久化的dcs服务地址
  state_server_enable_tls
  state_server_mode
  state_server_password
  
  将状态持久化开关置为true（当前默认为关闭状态）
  persistent_state
  persistent_future
  init_state_client

  
新增部署state-manager组件
  
  1、state-manager采用主备方式部署，replicas默认为3
  2、state-manager组件功能需要依赖k8s的metrics服务，需要在构建cce服务时进行相关配置部署
  3、state-manager对接的etcd为元数据etcd（路由etcd）待确认

functiontask与statemanager之间的安全加密
  
  1、functiontask与statemanager之间的http通信变更为https
  2、组件之间的鉴权逻辑适配为融合版本的ak、sk鉴权方式

#### 外部交互分析&&上下游依赖需求

【functionCore】BusProxy 支持函数间调用和有状态调用  张同

### 对外接口
无新增对外接口，state-manager为系统内部组件，只有core-sdk进行调用

### 约束
state-manager组件当前默认资源规划为：
limits:
  cpu: "2"
  memory: 2Gi
requests:
  cpu: "1"
  memory: 2Gi

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
| **序号** | **用例名**                 | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------- | ------------------------------------ | ---------------------------------------- |
| 1        | 组件正常部署   | 配置正确适配                |functiontask组件、state-manager组件能够正常部署启动                     |


