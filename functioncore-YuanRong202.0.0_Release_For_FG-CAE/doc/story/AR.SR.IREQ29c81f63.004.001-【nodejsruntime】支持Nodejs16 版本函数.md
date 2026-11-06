[TOC]

# 【nodejsruntime】支持Nodejs16 版本函数

## 1. Story整体设计

### 1. 功能描述
需求： runtime 新增nodejs 16 runtime

### 2. 场景分析
用户创建函数能够选择nodejs 16 runtime，创建成功后能够测试函数并返回执行结果。
		
### 3. 方案详细设计
nodejs 16 调研：
nodejs 16 新特性：https://weekly.shanyue.tech/release/node-16/
nodejs 16.17 相关信息：https://github.com/nodejs/node/releases/tag/v16.17.0 和 https://github.com/nodejs/node/releases/tag/v16.17.1

1. fusion镜像安装nodejs 16.17.1 LTS版本（满足客户诉求），安装目录/opt/function/runtime/nodejs16.17，并安装默认集成的三方库：https://support.huaweicloud.com/usermanual-functiongraph/functiongraph_01_0391.html
2. worker模块新增注册 nodejs16.17 runtime
3. nodejs runtime 模块新增nodejs 16执行分支

nodejs16 runtime提前启动调研
128规格池子：默认启动runtime为nodejs(10.16、12.13)、python3.6和Java8，加上 runtime-manager ，已占用内存为120MB左右，nodejs16在该规格不提前启动
256规格池子：默认启动runtime为nodejs(10.16、12.13、14.18)、python(3.6和3.9)、java(8和11)、php7.3，已占用内存为235MB左右，nodejs16本身占用内存16MB，可提前启动

4. worker-manager deployment 环境变量 RUNTIME_MANAGER_CONFIG 新增 nodejs 16 配置如下：
```
   "nodejs16.17": {
        "port": 31548,
        "enable": true,
        "excludeSpecifications": [128],
        "workerDir": "/opt/function"
   }
```
### 4. 上下游依赖需求
管理面： 管理面支持nodejs 16.17 runtime选择并成功创建 ------ 王能元

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

| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ---------------------------------------------------------- | ------------------------------------ | ------------------------------------- |
| 1        | 测试默认函数                                                  | 创建nodejs16.17函数，发起调用请求       | 成功返回函数执行结果                       |
| 2        | 测试函数日志（console.log和context.getLogger）                 | 创建nodejs16.17函数，发起调用请求       | 成功返回函数执行结果和日志                  |
| 3        | 测试函数初始化入口                                             | 创建nodejs16.17函数，配置初始化入口，发起调用请求 | 首次执行初始化成功，并只初始化一次     |
| 4        | 测试nodejs 16 runtime默认集成三方库                            | 创建nodejs16.17函数，函数内加载默认三方库，发起调用请求 | 成功返回函数执行结果           |
| 5        | 测试nodejs 16 runtime日志是否正常                              | 创建nodejs16.17函数，发起调用请求        | runtime日志正常       |
| 6        | 全量冒烟用例                                                  | 启动全量冒烟                           | 全量冒烟用例通过       |