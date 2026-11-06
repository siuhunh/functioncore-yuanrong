[TOC]

# AR.SR.IREQ02597546.040.001 支持Java11

## Story 整体设计

### 功能描述
+ java runtime支持java 11版本
+ 根据用户函数指定的java版本拉起对应版本runtime，各版本兼容

### 场景分析
+ 调用java8版本函数
+ 调用java11版本函数

### 方案详细设计

**java11和java8之间不完美兼容**
+ API废除或替换 【已适配】
+ ClassLoader变化带来的URLClassLoader的变化 【已适配】
+ JVM过期启动参数修改 【不涉及】

**300-128规格pod无法特化前启动java11**

【目前300-128规格中加载nodejs(6.10、8.10、10.16、12.13)、python3.6、java8共占用约120M】
+ java11通过配置的方式不在300-128规格中提前启动，在收到特化请求之后仍可以启动java11
+ 其余规格均提前启动java11
  
```text
  "java11": {
      "port": 31540,
      "enable": true,
      "traceMasterAddress": "",
      "excludeSpecifications": [128]
  }
```

**HTTP函数**
+ 与java8一致，在bootstrap中使用jre启动用户jar包
```text
java8
/opt/function/runtime/java8/rtsp/jre/bin/java -jar -Dfile.encoding=utf-8 /opt/function/code/gsondemo-0.0.1-SNAPSHOT.jar

java11
/opt/function/runtime/java11/rtsp/jre/bin/java -jar -Dfile.encoding=utf-8 /opt/function/code/gsondemo-0.0.1-SNAPSHOT.jar
```



#### 外部交互分析&&上下游依赖需求
无

### 对外接口
无

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*  
不涉及

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*  
不涉及

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

#### 可用性 Availability
*处于按要求执行状态的能力*  
不涉及

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*  
不涉及

## LLT 用例
|  序号 | 预置条件 | 预期结果 |
| ------------ | ------------ | ------------ |
|  1 | 特化之前查看300-128规格pod，不包含java11进程，特化后包含| 符合预期 |
|  2 | 特化之前查看其余规格pod，包含java11进程| 符合预期 |
|  3 | 上传查看jdk版本代码的jar包，分别使用java8和java11运行 | 分别返回1.8.x和11.0.x |
|  4 | java11日志打印（CONSOLE） | 符合预期 |
