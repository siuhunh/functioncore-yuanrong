[TOC]  

# 数据面组件安全加固

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

需求来源为安全送检

### 1.2特性场景分析

- 数据面组件（包含workflow）Role权限定义存在冲突，且存在权限过大的风险
  
  例如functiontask虽然不涉及任何k8s的操作，在k8s Role中却定义了大量操作权限；函数的CRD特性虽然早就已经删除，但workermanager却依然保留了对CRD的操作权限；这些均不符合权限最小化原则。

- 数据面组件（包含workflow）的deployment定义中需要补充安全加固项

## 2. 特性/功能实现原理

### 2.1总体方案

针对Role权限，初步识别仅worker-manager和state-manager需要k8s Role权限，其余组件的Role定义均删除；worker-manager和state-manager需根据实际需要，保留最小权限，删除不必要的权限；



deployment的安全加固项，在ICSL_YuanRong-101.0.0分支中已经完成，本次仅涉及将修改同步到FunctionGraph_master分支，并对faas-scheduler也做相应修改；主要涉及三项：

1. 增加enableServiceLinks: false

2. 除worker-manager、state-manager外，增加automountServiceAccountToken: false配置（包括vpcpulltrigger）

3. 除worker-manager外，启动前增加source /etc/profile（runtime容器需要额外添加chmod 750 /home/sn，执行之后再修改chmod 550 /home/sn）



### 2.3 约束

#### 2.3.1上下游依赖需求

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security）

不涉及

### 3.2韧性（Resilience）

不涉及

### 3.3隐私性（Privacy）

不涉及

### 3.4可靠性（Reliability）

不涉及

### 3.5可用性（Availability）

不涉及

### 3.6安全（Safety）

不涉及

### 3.7可维护性

不涉及

### 3.8可测试性

不涉及

### 3.9性能

不涉及

### 3.10资源

不涉及

### 3.11资料

不涉及 

### 3.12升级兼容性

采用helm升级，role等定义均在helm纳管范围，不影响升级兼容性。

## 4. 用例

安全加固需求，保证修改后组件功能不受影响，冒烟全量通过即可。
