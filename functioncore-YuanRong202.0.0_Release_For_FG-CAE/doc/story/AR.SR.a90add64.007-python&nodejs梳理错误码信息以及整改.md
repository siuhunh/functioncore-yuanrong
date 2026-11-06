[TOC]

# AR.SR.a90add64.007-python&nodejs梳理错误码信息以及整改

## Story 整体设计

### 功能描述

**背景说明**

梳理错误码主要发现以下问题：

（1）一个错误码对应多个错误信息

（2）各个runtime的错误码的错误信息不统一（测试提到的此问题比较多）

（3）错误信息不完整，错误信息描述不能够给用户足够提示

（4）错误信息与错误码不符问题

（5) 代码里面有一些错误码并没有使用到

因此需要对错误码整体做个梳理以及整改， 本文档主要涉及 python 和 nodejs runtime模块的错误码梳理以及整改


### **场景分析**

### 方案详细设计

#### 方案设计

##### 问题1- 一个错误码对应多个错误信息统一目标如下：

**4001：function entry is not found, error: + 对应错误信息（python和 nodejs runtime需修改）**

python runtime  的错误信息根据不同分支有多种：

（1）failed to find module '{}' 

（2）Handler '{}' not found in module '{}' 

（3） Failed to import module '{}'  

（4）Syntax error in module '{}' 

（5） Module initialization error

nodejs runtime  的错误信息根据不同分支有多种：

（1）Cannot find module '${handlerPath}'  

（2） Not found module '${handlerPath}'` 

（3） Handler '${handlerName}' not found in module '${handlerPath}'

**4002：function invocation exception, error: + 对应错误信息（python和 nodejs runtime需修改）**

python runtime  的错误信息根据不同分支有多种：

（1）errmsg = str(exp) 即 exception的信息 

（2）Function invocation catch a '{}' exception

（3）function invocation exception: traceback: {}

nodejs runtime  的错误信息根据不同分支有多种：用户函数异常，不同异常返回错误信息内容不同

**4003：state content is too large （无需修改）**

**4004：response body size %d larger than max value %d （无需修改）**

**4005：init state function invocation exception, error: + 对应错误信息（python和 nodejs runtime需修改）**

**4007：function result is invalid, error: + 对应错误信息（python和 nodejs runtime需修改）**

python runtime  的错误信息：json string is invalid:%s   或者  failed to convert the invoking result to " "a JSON string: " + str(exp)

nodejs runtime  的错误信息： 根据实际异常返回错误信息

**4009：function initialization exception, error: + 对应错误信息（python和 nodejs runtime需修改）**

python runtime  的错误信息根据不同分支有多种：

（1）invalid initializer handler '{}' 

（2）failed to find module '{}' 

（3） Handler '{}' not found in module '{}'nodejs

nodejs runtime  的错误信息根据不同分支有多种：初始化函数执行异常或初始化函数存在问题（不存在等）

**4040：invalid input parameter, error: + 对应错误信息**

4040 错误码对应的错误参数比较多，比如函数名，版本和函数调用payload等等，不建议加 invalid input parameter, error：前缀加了之后效果如下：

invalid input parameter, error: invalid func_name, not match regular expression，重复出现两次invalid。

##### 问题2-各个runtime的错误码的错误信息不统一：

问题1已统一错误信息前缀，具体报错信息看是否有语言特性，如不是则统一。

##### 问题3-错误信息不完整，错误信息描述不能够给用户足够提示：

python & nodejs runtime不存在该问题

##### 问题4-错误信息与错误码不符问题：

python runtime 初始化状态返回错误码为 4002 需改为 4005

nodejs runtime 解析请求json失败错误码为 4007 需改为 4002

##### 问题5-代码里面有一些错误码并没有使用到：

python2.7 runtime 有状态错误码没有用到需删除，如下：

USER_STATE_TOO_LARGE = 4003

USER_STATE_UNDEFINED = 4005


#### 外部交互分析&&上下游依赖需求
修改错误信息后，测试用例需适配

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

| 序号 | 用例名                                              | 测试步骤                     | 预期结果                                         |
| ---- | ------------------------------------------------- | --------------------------- | ------------------------------------------------ |
| 1    | 找不到函数入口                                       | 构造函数入口不存在错误          | 返回错误信息前缀：function entry is not found, error: |
| 2    | 函数返回值内容超平台限制                               | 构造函数返回超过6M内容          | 返回错误信息前缀：function invocation exception, error: |
| 3    | 状态未定义                                          | 构造init_state函数不存在       | 返回错误信息前缀：init state function invocation exception, error: |
| 4    | 函数调用结果非法                                     | 构造返回返回结果异常             | 返回错误信息前缀：function result is invalid, error: |
| 5    | 初始化函数异常                                       | 构造初始化函数异常              | 返回错误信息前缀：function initialization exception, error: |
