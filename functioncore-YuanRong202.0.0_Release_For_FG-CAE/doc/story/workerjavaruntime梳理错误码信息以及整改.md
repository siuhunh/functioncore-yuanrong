[TOC]

# worker以及runtime-java梳理错误码信息以及整改

## Story 整体设计

### 功能描述

**背景说明**

https://wiki.huawei.com/domains/867/wiki/5357/WIKI20220622113199

梳理错误码主要发现以下五大类问题

（1）一个错误码对应多个错误信息

（2）各个runtime的错误码的错误信息不统一（测试提到的此问题比较多）

（3）错误信息不完整，错误信息描述不能够给用户足够提示。

（4）错误信息与错误码不符问题

 （5)  代码里面有一些错误码并没有使用到，

因此需要对错误码整体做个梳理以及整改， 本文档主要涉及worker以及runtime-java模块的错误码梳理以及整改

**功能描述**

针对上述五个场景镜像错误码整改

（1）一个错误码对应多个错误信息

（2）各个runtime的错误码的错误信息不统一（测试提到的此问题比较多）

（3）错误信息不完整，错误信息描述不能够给用户足够提示。

（4）错误信息与错误码不符问题

（5)  代码里面有一些错误码并没有使用到

**用户体验**

出现错误，用户感知，得到错误信息和错误码通知，错误信息和错误码描述信息准备

### **场景分析**

1. 一个错误码对应多个错误信息

   **4002 （function invocation exception） 错误信息有：**

   - web runtime  对应信息  "function invocation exception + 错误信息"
   - go runtime    对应信息   返回runtime的err信息
   - 自定义镜像     对应信息   "%s is not a valid swr image name"
   - 自定义镜像     对应信息   " image %s does not exist "
   - 自定义镜像     对应信息   " image %s is too large. %d > %d bytes"
   - 自定义镜像     对应信息  " %s : %s"(reason:message)
   - 其他各语言     对应信息  对应的runtime梳理到各个语言的表格后整理
   - python runtime  的错误信息根据不同分支有多种：（1）errmsg = str(exp) 即 exception的信息 （2）Function invocation catch a '{}' exception （3） function invocation exception: traceback: {}
   - nodejs runtime  的错误信息根据不同分支有多种：用户函数异常，不同异常返回错误信息内容不同
   - java runtime 的错误信息：Exception: %s
   - php runtime 的错误信息：（1）request timeout  （2）input data is empty. （3）runtime_handler error
   - **建议统一  对齐各个runtime 输出"function invocation exception, error : + 自己错误信息"******（runtime修改涉及）****

    

   **4010 （invoke timed out after %s） 错误信息有：**

   - 对应信息  "invoke timed out after %s"

   - 对应信息  "failed to send cancel request for %s time out" 

   - 对应信息  "failed to check health in server after %s"

   - 对应信息  "call invoke timeout"   

     **建议统一为 "call invoke timeout %s"** **(worker修改涉及)**

   

   

   **4012 （load function timed out after %s） 错误信息有：**

   - 对应信息  "failed to load function in server after %s"

   - 对应信息  "load function timed out after %s" 

     ********建议统一为"load function timed out after %s"************（worker修改涉及）****

   

    

   **4201 （function initialization failed） 错误信息有：（其他runtime 调用init函数失败也是返回这个错误码）**

   - 其他各语言     对应信息  对应的runtime梳理到各个语言的表格后整理 主要是对应各个runtime的4009的错误
   - python runtime  的错误信息根据不同分支有多种：（1）invalid initializer handler '{}' （2）failed to find module '{}' （3） Handler '{}' not found in module '{}'nodejs
   - nodejs runtime  的错误信息根据不同分支有多种：初始化函数执行异常或初始化函数存在问题（不存在等）
   - java runtime 的错误信息：Exception: %s
   - php runtime 的错误信息：（1）request timeout  （2）runtime_initializer_handler error （3）runtime_handler error
   - **建议统一  "function initialization failed, error :  + 自己错误信息"******（runtime修改涉及）****

    

   **4204 （runtime process is exited） 错误信息有：**

   - web runtime          对应信息  "runtime process is exited"
- go runtime            对应信息  "runtime process is exited"
   - go runtime            对应信息  cmd.Start() 返回的err错误
- cunstom runtime  对应信息  "runtime process is exited"
   - cunstom runtime   对应信息  cmd.Start() 返回的err错误
- 其他语言的runtime 对应信息  "runtime process is exited"
   
  - **建议统一 "runtime process is exited" 或者 "runtime process is exited, error: + 自己错误信息"**
   
 
   

   
2.  各个runtime的错误码的错误信息不统一（测试提到的此问题比较多）

   

   **4001 （找不到函数入口） 错误信息有：**

   - python runtime  的错误信息根据不同分支有多种：（1）failed to find module '{}' （2）Handler '{}' not found in module '{}' （3） Failed to import module '{}'  (4) Syntax error in module '{}' （5） Module initialization error

   - nodejs runtime  的错误信息根据不同分支有多种：（1）Cannot find module '${handlerPath}'  （2） Not found module '${handlerPath}'` （3） Handler '${handlerName}' not found in module '${handlerPath}'

   - java runtime 的错误信息：Specified method not found:%s
   - php runtime 的错误信息：（1）Handler %s not found in module  （2）runtime_initializer_handler can't be empty. （3）runtime_handler cant't be empty

    

   **4002 （function invocation exception） 错误信息有：**

   - python runtime  的错误信息根据不同分支有多种：（1）errmsg = str(exp) 即 exception的信息 （2）Function invocation catch a '{}' exception （3） function invocation exception: traceback: {}
   - nodejs runtime  的错误信息根据不同分支有多种：用户函数异常，不同异常返回错误信息内容不同
   - java runtime 的错误信息：Exception: %s
   - php runtime 的错误信息：（1）request timeout  （2）input data is empty. （3）runtime_handler error

    

   **4003 （状态信息太大） 错误信息有：**

   - 各语言runtime 错误信息一致：state content is too large

    

   **4004 （函数返回值内容超平台限制） 错误信息有：**

   - python runtime  的错误信息：response body size {} exceeds the limit of {}
   - nodejs runtime  的错误信息： response body size %d exceeds the limit of %d
   - java runtime 的错误信息：the return value of the function exceeds the maximum allowed limit
   - php runtime 的错误信息：response body size %d larger than max value %d

    

   **4005 （函数返回值内容超平台限制） 错误信息有：**

   - 各语言runtime 错误信息一致：state is undefined

    

   **4006（心跳） 错误信息有：（仅java-runtime）**

   - Specified method or clazz is empty, it can not be empty
   - Specified heartbeat handler format error, it must contain colon character: %s

    

   **4007 （函数请求或返回结果json解析非法） 错误信息有：（仅python-runtime、nodejs-runtime、php-runtime）**

   - python runtime  的错误信息：json string is invalid:%s   或者  failed to convert the invoking result to " "a JSON string: " + str(exp)
   - nodejs runtime  的错误信息： 根据实际异常返回错误信息
   - php runtime 的错误信息：exception信息

    

   **4009 （init 函数异常） 错误信息有：**

   - python runtime  的错误信息根据不同分支有多种：（1）invalid initializer handler '{}' （2）failed to find module '{}' （3） Handler '{}' not found in module '{}'nodejs
   - nodejs runtime  的错误信息根据不同分支有多种：初始化函数执行异常或初始化函数存在问题（不存在等）
   - java runtime 的错误信息：Exception: %s
   - php runtime 的错误信息：（1）request timeout  （2）runtime_initializer_handler error （3）runtime_handler error

    

   **4010 （心跳函数调用异常） 错误信息有：（仅java-runtime）**

   - Exception:%s

    

   **4040（使用有状态SDK，对被调函数名，instanceName等参数的检查，检查不通过返回对应的错误信息） 错误信息有：**

   - 不同场景返回错误信息不同，例如函数名错误，返回invalid func_name, expect string  或者 invalid func_name, not match regular expression

     

     

     **建议统一成******runtime中公共的错误码****, **runtime-java修改涉及**

     **4001 ：******function entry exception******，error： +  对应错误信息**

     **4002：function invocation exception ，error：+ 对应错误信息**

     **4003：******state content is too large****

     **4004：response body size %d larger than max value %d**

     **4005：******init state function invocation exception, error: + 对应错误信息********

     **4007：function result is invalid， error： +  对应错误信息** 

     **4009： function initialization exception，error： + 对应错误信息**

3. 错误信息不完整，错误信息描述不能够给用户足够提示。

   **初始化报空指针异常时，runtime-java返回报错为Exception: null**

   **runtime-java中初始化、加载、调用空指针异常类的报错均存在此问题**

4. 错误信息与错误码不符问题

   **java11函数，构造返回响应过大，返回报错code为4002， 报错信息为response body size 6291458 exceeds the limit of 6291456， 实际错误状态码应该是4004**。

   

5. 代码里面有一些错误码并没有使用到

worker代码中存在一些未使用的错误状态码，针对错误码在代码中定义但是未被使用，删除这些未使用的错误码，测试下兼容性

| 错误码 | 返回错误信息                                                 | 场景描述                  | 解决方案                                           | 备注   |
| ------ | ------------------------------------------------------------ | ------------------------- | -------------------------------------------------- | ------ |
| 4001   | function entry is not found                                  | 找不到函数入口            | 请检查函数的执行入口和上传代码的函数入口是否一致。 | 未使用 |
| 4003   | state content is too large                                   | 状态信息太大              | 检查状态信息                                       | 未使用 |
| 4004   | return value of the function exceeds the maximum allowed limit | 函数返回值内容超平台限制  | 请检查函数返回值内容的大小是否超过平台限制（6M）。 | 未使用 |
| 4005   | state is undefined                                           | 状态未定义                | 检查状态信息                                       | 未使用 |
| 4007   | function result is invalid                                   | 函数调用结果非法          | 根据错误提示检查函数代码                           | 未使用 |
| 4009   | function initialization exception                            | init 函数异常             | 请检查 init 函数代码逻辑                           | 未使用 |
| 4203   | runtime bootstrap process is exited                          | 用户bootstrap未执行完退出 | 请检查用户bootstrap执行是否有问题                  | 未使用 |
|        |                                                              |                           |                                                    |        |



### 方案详细设计

#### 方案设计


#### 外部交互分析&&上下游依赖需求

NA

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

| 序号 | 用例名                                                       | 测试步骤                                                     | 预期结果                                         |
| ---- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------ |
| 1    | 一个错误码对应多个错误信息，返回报错信息前缀是否统一， 主要涉及错误码4002, 4010, 4012, 4201, 4204 | 编写函数构造错误，观察一个错误码对应多个错误信息时，返回报错信息是否统一 | 一个错误码对应多个错误信息，返回报错信息前缀统一 |
| 2    | runtime-java报错信息前缀是否按照统一标准修改                 | 编写函数构造错误，观察runtime-java报错信息前缀是否统一       | runtime-java报错信息前缀统一                     |
| 3    | runtime-java、worker报错信息与报错code一致                   | 编写函数构造错误，观察runtime-java和worker报错信息是否与报错code一致 | 报错信息与报错code一致                           |
| 4    | runtime-java、worker报错信息是否足够清晰                     | 编写函数构造错误，观察runtime-java、worker报错信息是否足够清晰 | 观察报错信息足够清晰                             |
| 5    |                                                              |                                                              |                                                  |

