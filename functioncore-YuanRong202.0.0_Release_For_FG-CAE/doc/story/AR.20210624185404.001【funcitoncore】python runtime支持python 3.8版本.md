# python runtime支持python 3.8版本


## 1. Story 整体设计
### 1.1 功能描述
+ python runtime支持python 3.8版本
+ 根据用户函数指定的python版本拉起对应版本runtime，各版本兼容
### 1.2 场景分析
1. 调用python3.7版本用户函数
2. 调用python3.8版本用户函数
### 1.3 方案详细设计
**限制**
+ 不支持python子版本兼容，如python3.8.1、python3.8.2
+ CLI本地调试功能不新增版本python3.8
+ 基础镜像python:2.0中包含多版本python，不同python版本使用同一镜像

**cli新增语言python3.8**
+ 命令窗口语言选项新增python3.8
+ cli及functionrepo多处语言校验等新增python3.8
+ cli新增python3.8函数模板，类似python3.7

**nodemanager新增python3.8各类规格pod池**
+ node-manager-config.yaml中新增各类规格的python3.8 pod池
+ node-manager中构造runtime deployment时，新增`RUNTIME_LANGUAGE`环境变量，如python3.8的runtime则有``RUNTIME_LANGUAGE=python3.8``

**支持使用指定版本python解释器拉起runtime进程**
+ Dockerfile-runtime-python中新增环境变量，并作为参数传入ENTRYPOINT
```shell
ENV RUNTIME_LANGUAGE=python3.7
ENTRYPOINT /bin/bash /home/snuser/bin/entrypoint ${RUNTIME_LANGUAGE}
```
+ entrypoint_runtime_python中根据环境变量调用指定版本python拉起runtime
```shell
exec -c ${PythonWithVersion} ${WRAPPER} ${ADDRESS} ${HandlerFilePath} ${HandlerName} ${CLASSPATH}
```

### 1.4 上下游依赖需求
无
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
| 1 |  ubuntu python3.7用户函数 | 调用用户函数 | 调用成功  |
| 2 |  ubuntu python3.8用户函数 |  调用用户函数 | 调用成功  |
| 3 |  EulerOS  python3.7用户函数 | 调用用户函数 | 调用成功  |
| 4 |  EulerOS  python3.8用户函数 |  调用用户函数 | 调用成功  |
| 5 |  ARM python3.7用户函数 | 调用用户函数 | 调用成功  |
| 6 |  ARM python3.8用户函数 |  调用用户函数 | 调用成功  |
