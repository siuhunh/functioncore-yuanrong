[TOC]

# 【functionCore】【可靠性】函数代码入参规范检查

## 1. Story整体设计

### 1. 功能描述
需求：
1. 对函数入参规范检查，返回明确的错误信息和错误码

### 2. 场景分析
nodejs、python 和 java 三个 runtime 在加载函数或初始化函数时，需要校验其入参个数，如果入参个数不符合规范则返回明确的错误信息和错误码。

函数入参个数为2个，分别为event和context；(注nodejs6有3个入参：event, context, callback)
初始化函数入参个数为1个，为context；
		
### 3. 方案详细设计
nodejs通过如下方式获取函数入参个数：
````
const functioninstance = module[handlerName]
functioninstance.length
````

python通过如下方式获取函数入参个数：
````
Python2.7写法：
def abc(a,b,c):
    print a,b
    
count=abc.func_code.co_argcount

python3.6写法
def abc(a,b,c):
    print a,b
count=abc.__code__.co_argcount
````

java通过如下方式获取函数入参个数：
````
handlerMethod.getParameterCount();
````
​java增加函数Context入参参数类型是否正确

参数检查返回错误信息模板：
函数：
````
"Handler expect to have " + PARAMETER_COUNT_REQUIRED + " parameters, but received " + count
````
初始化函数：
````
"Initializer expect to have " + PARAMETER_COUNT_INITIALIZER_REQUIRED + " parameters, but received " + count
````
java context 参数类型错误：
````
"The last parameter should be an instance of com.huawei.services.runtime.Context, not "
                                        + pType[PARAMETER_COUNT_INITIALIZER_REQUIRED - 1].getName();
````

返回错误码，使用现有错误码4004：
````
// IllegalArgumentErrorCode code of illegal argument
IllegalArgumentErrorCode = 4004
````

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

| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | ------------------------------------ | ---------------------------------------- |
| 1        | nodejs10函数错误入参个数                                   | 创建函数，修改函数，入参个数错误，点击测试    | 成功返回对应错误信息                 |
| 2        | nodejs10初始化函数错误入参个数                                    | 创建函数，修改初始化函数，入参个数错误，点击测试    | 成功返回对应错误信息          |
| 3        | nodejs6函数错误入参个数                                    | 创建函数，修改函数，入参个数错误，点击测试    | 成功返回对应错误信息          |
| 4        | nodejs6初始化函数错误入参个数                                    | 创建函数，修改初始化函数，入参个数错误，点击测试    | 成功返回对应错误信息          |
| 5        | python2.7函数错误入参个数                                    | 创建函数，修改函数，入参个数错误，点击测试     | 成功返回对应错误信息  |
| 6        | python2.7初始化函数错误入参个数                                    | 创建函数，修改初始化函数，入参个数错误，点击测试     | 成功返回对应错误信息  |
| 7        | python3.6函数错误入参个数                                    | 创建函数，修改函数，入参个数错误，点击测试     | 成功返回对应错误信息  |
| 8        | python3.6初始化函数错误入参个数                                    | 创建函数，修改初始化函数，入参个数错误，点击测试     | 成功返回对应错误信息  |
| 9        | java8函数错误入参个数                                    | 创建函数，修改函数，入参个数错误，点击测试     | 成功返回对应错误信息  |
| 10        | java8函数context入参类型错误哦                              | 创建函数，修改函数，context入参类型错误，点击测试    | 成功返回对应错误信息  |
| 11        | java8初始化函数错误入参个数                                    | 创建函数，修改初始化函数，入参个数错误，点击测试     | 成功返回对应错误信息  |
| 12       | java8初始化函数context入参类型错误哦                              | 创建函数，修改初始化函数，context入参类型错误，点击测试    | 成功返回对应错误信息  |