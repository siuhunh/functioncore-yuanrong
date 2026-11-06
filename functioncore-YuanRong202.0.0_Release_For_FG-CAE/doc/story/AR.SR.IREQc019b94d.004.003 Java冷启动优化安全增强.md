[TOC]

# AR.SR.IREQc019b94d.004.003 Java冷启动优化安全增强

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

Java快照模式下安全增强

### 1.2特性场景分析

1）worker解压内存快照（之前是runtime manager，约束：解压到根目录），并更新目录权限最小化
2）worker快照包加解密（gzip，同时通过status判断快照包是否ready）
3）worker/runtime manager安全评审和其他安全未尽事宜

## 2. 特性/功能实现原理

注：fastfreeze会修改/var/tmp/fastfreeze下的文件，且该路径不可配置

### 原流程：

#### 制作快照
1. init
	1. runtime-manager杀死所有runtime
	2. 不设置根目录只读，设置文件权限 `/opt/function/code`, `/var/tmp/fastfreeze`
	3. runtime-manager用fastfreeze启动java runtime fastfreeze_run.sh
2. 不启动磁盘监控
3. hotload
4. 特化完成
5. worker收到快照请求后，通知runtime-manager
	1. runtime-manager fastfreeze_checkpoint.sh 打包java runtime打开的文件到 /opt/snapshot/snapshot.zip 
	3. worker上传obs


#### 正常特化：
1. worker下载zip /opt/snapshot
2. init
	1. runtime-manager杀死所有runtime
	2. 不设置根目录只读，设置文件权限 `/opt/function/code`
	3. runtime-manager用fastfreeze恢复java runtime fastfreeze_restore.sh /opt/snapshot /opt/huawei/image
3. 不启动磁盘监控

### 安全增强后：

#### 前置条件
- *对/var/tmp做磁盘监控，大小和/tmp加起来不能新增超过512M*
- *快照生成目录放到/var/tmp/snapshot*
#### 制作快照
1. init
	1. runtime-manager杀死所有runtime
	2. ~~不设置根目录只读，设置文件权限 `/opt/function/code`, `/var/tmp/fastfreeze`~~
	3. runtime-manager用fastfreeze启动java runtime fastfreeze_run.sh
2. 不启动磁盘监控 -> *启动磁盘检测*
3. hotload
4. 特化完成
5. worker收到快照请求
	1. *关闭 /var/tmp 目录的大小限制*
	2. runtime-manager fastfreeze_checkpoint.sh 打包java runtime打开的文件到 /opt/snapshot/snapshot.zip  -> *使用fastfreeze的`--preserve-path /tmp /opt/function/code /home/snuser/log` 将preserve-path中的文件也复制进快照到runtime的/var/tmp/snapshot目录*
	3. *worker 7zip加密打包/var/tmp/snapshot*
	4. worker上传obs


#### 正常特化：
1. worker下载zip /opt/snapshot -> *worker解密解压到/var/tmp/snapshot*
2. init
	1. runtime-manager杀死所有runtime
	2. 不设置根目录只读，设置文件权限 `/opt/function/code`, `/var/tmp/fastfreeze` -> *设置根目录只读，并暂时将preserve-path中的目录设置为snuser可写*
	3. runtime-manager用fastfreeze恢复java runtime fastfreeze_restore.sh /opt/snapshot /opt/huawei/image -> *从/var/tmp/snapshot恢复，fastfreeze内部会将preserve-path的文件untar到相应路径*
	4. *restore完成时，fastfreeze朝/var/tmp/notify目录中写入一个文件，runtime-manager检测到后认为restore完成*
	5. *修复preserve-path的文件权限*
3. 不启动磁盘监控 -> *启动磁盘检测*

## 3. 可信软件

### 3.1安全性（Security） 

#### 制作快照

hotload时会执行用户函数的初始化入口，用户可以使用snuser权限随意对/var/tmp目录修改：

1. 无法超过/var/tmp/磁盘大小限制，对宿主机磁盘影响可控
2. 修改/var/tmp/fastfreeze内的文件，让fastfreeze异常：fastfreeze进程使用snuser执行，无越权风险
3. 修改/var/tmp/snapshot快照目录：worker和runtime-manager都不会执行快照中的文件，正常特化下载快照后，使用snuser执行fastfreeze_restore.sh时才可能执行快照目录中的文件，无越权风险

#### 正常特化

restore完成时，用户函数也会同步开始执行，在修复preserve-path文件权限和启动磁盘监控前，有一个极短的时间窗口，可以执行用户代码

1. preserve-path的几个路径
    - `/opt/function/code` 储存用户代码包，临时允许修改也无所谓
    - `/tmp` 一直可以修改，超过512M报错
    - `/home/snuser/log` runtime日志文件（不包括worker，runtime-manager），临时允许修改也无所谓

#### 7Zip 加密压缩、解压

- 加密 `/opt/huawei/7z_linux_x86/7z a -mmt6 -tzip -p$1 -mem=AES256`
- 解密 `/opt/huawei/7z_linux_x86/7z x -mmt6 -p$1`

加密使用AES256算法，$1是密钥，在k8s worker-key secret中增加密钥，为128位的安全随机数。加密解密在worker中，用户无法拿到密钥，无安全风险

### 3.2韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

*【客户可感知的需求，需要输出相应资料】*

### 3.12升级兼容性

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

| **序号** | **用例名** | 用例类型 | **测试步骤** | **预期结果** |
| -------- | ---------- | -------- | ------------ | ------------ |
| 1        | 打快照、特化快照    | LLT      | 1. 打快照 2. 特化快照           | 正常执行，安全加强后不影响功能           |
