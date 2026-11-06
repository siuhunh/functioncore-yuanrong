[TOC]

# AR.SR-20210630-001.002 支持Python3.9版本
# AR.SR-20210630-001.002 支持Node.js14版本

## Story 整体设计

### 功能描述
+ 支持Python3.9, Node.js14版本
+ 根据用户函数指定的版本拉起对应版本runtime，各版本兼容

### 场景分析
+ 调用Python3.9版本函数
+ 调用Node.js14.18版本函数

### 方案详细设计

选用LTS（Long Term Support）的版本。
python3.9新特性： https://docs.python.org/zh-cn/3/whatsnew/3.9.html
nodejs14.18新特性：https://jishuin.proginn.com/p/763bfbd49056

**300-128规格pod无法特化前启动**

【目前300-128规格中加载nodejs(6.10、8.10、10.16、12.13)、python3.6、java8共占用约120M】
+ 通过配置的方式使python3.9，nodejs14.18不在300-128规格中提前启动，在收到特化请求之后仍可以启动
  
```text
                "python3.9": {
                    "port": 31541,
                    "enable": true,
                    "excludeSpecifications": [128]
                },
                "nodejs14.18": {
                    "port": 31542,
                    "enable": true,
                    "excludeSpecifications": [128]
```

**HTTP函数**
+ 与其他语言一致，在bootstrap中使用语言二进制启动用户服务
```text
nodejs14.18
/opt/function/runtime/nodejs14.18/rtsp/nodejs/bin/node xxx.js

python3.9
/opt/function/runtime/python3.9/rtsp/python/bin/python3.9 xxx.py
```



#### 外部交互分析&&上下游依赖需求
前端： FG v2支持新语言 ------ 沈宵宵
管理面： 管理面支持新语言 ------ 夏嘉诚
现网部署： 因镜像超过swr手动上传镜像限制，需暂时绕过，通过伏羲流水线 / obs桶上传镜像 ------ 施广帅
（nodejs14 客户石墨文档 北京四
  python3.9 客户在新加坡）

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
|  1 | 使用python3.9函数代码调用| 调用成功 |
|  2 | 使用nodejs14.18函数代码调用| 调用成功 |
