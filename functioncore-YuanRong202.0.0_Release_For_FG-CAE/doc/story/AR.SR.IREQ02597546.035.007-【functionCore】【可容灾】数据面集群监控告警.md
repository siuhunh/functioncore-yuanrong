[TOC]

【functionCore】【可容灾】数据面集群监控告警


## 1. Story整体设计

### 1. 功能描述
对数据面集群添加告警配置，在出现监控指标超过阈值情况时及时通知相关人员，相关人员登录集群查看数据面运行情况，如果已出现数据面服务异常情况，优先恢复数据面服务，
然后分析导致指标超过阈值原因并输出解决方案。

### 2. 场景分析
分析数据面集群、节点和常驻服务所需的指标监控，制定告警规则，添加告警配置。

### 3. 方案详细设计

#### 如何添加监控告警
1. 进入云容器引擎cce。
2. 单击“监控中心”。
3. 选择“告警 > 告警规则”。
4. 单击“创建阈值”。
5. 在“添加阈值”界面，根据界面提示配置参数。
5.1. 输入阈值名称。
5.2. 选择资源类型：可选择容器组件、虚机组件、主机、主机磁盘、主机网络等等，我们选择主机或容器组件。
5.3. 选择资源后点击下一步；当我们选择资源类型为主机时，选择资源展开项就是集群下所有主机，我们可以选择单个或多个主机；当我们选择资源类型为容器组件时，选择资源展开项就是集群下的组件。
5.4. 然后开始定义阈值，首先选择指标名称，可选值有CPU内核总量、CPU内核占用、物理内存使用率等等。
5.5. 输入阈值条件，例如CPU使用率，大于等于80%，连续周期3，阈值统计方式“平均值”，统计周期“一分钟”。
5.6. 输入告警级别，告警级别有提示、次要、重要、紧急。
5.7. 选择是否发送通知，如果要发送通知，需选择主题，如果没有主题则在SMN界面新建主题。注：发送通知会产生费用。
5.8. 如果发送通知配置为是，需要配置触发场景，有"超限阈值"和"恢复正常"。

#### 整理数据面需添加阈值配置
监控阈值分为三大类：集群所有节点、集群单个节点、集群内数据面常驻服务
阈值命名规则也分为三大类：
1. regionID-yuanrongClusterID-metricsName
2. regionID-yuanrongClusterID-nodeName-metricsName
3. regionID-yuanrongClusterID-serviceName-metricsName

regionID为区域ID，例如北京四：cn-north-4;
yuanrongClusterID为元戎数据面集群ID，例如functiongraph-yuanrong01;
nodeName为节点名称，例如functiongraph-yuanrong01-38991-ceaiq，可去除functiongraph-yuanrong01；
serviceName为常驻服务名称，例如FunctionTask；
metricsName为阈值条件，例如CPU使用率

数据面常驻服务：cache-manager、FunctionAccessor(frontend)、FunctionTask、FunctionInstanceManger(WorkerManager)、workflow

节点可添加的阈值条件：物理磁盘总量、物理磁盘已使用总量、CPU内核总量、CPU内核占用、CPU使用率，可用物理内存、可用虚拟内存、物理内存使用率、主机状态、NTP偏移量、NTP服务器状态、NTP同步状态、进程数量、物理内存容量、虚拟内容容量、虚拟内存使用率
常驻服务可添加的阈值条件：CPU内核总量、CPU内核占用、CPU使用率、磁盘读取速率、磁盘写入速率、文件系统可用、文件系统容量、文件系统使用率、物理内存总量、物理内存使用率、物理内存使用量、下行Bps、下行错包率、下行Pps、容器错包个数、上行Bps、上行错包率、上行Pps、组件状态
[指标详细说明](https://support.huaweicloud.com/productdesc-aom/aom_06_0009.html)

输出数据面阈值规则表：

| 序号 | 阈值名称                                                     | 资源类型 | 指标名称       | 阈值条件 | 统计方式 | 统计周期 | 连续周期 | 告警级别 | 是否通知 |
| ---- | ------------------------------------------------------------ | -------- | -------------- | -------- | -------- | -------- | -------- | -------- | -------- |
| 1    | cn-north-7-cce-cluster-yuanrong01-cache-manager-cpuUsage     | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 2    | cn-north-7-cce-cluster-yuanrong01-cache-manager-memUsage     | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 3    | cn-north-7-cce-cluster-yuanrong01-cache-manager-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 4    | cn-north-7-cce-cluster-yuanrong01-cache-manager-status       | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 5    | cn-north-7-cce-cluster-yuanrong02-cache-manager-cpuUsage     | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 6    | cn-north-7-cce-cluster-yuanrong02-cache-manager-memUsage     | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 7    | cn-north-7-cce-cluster-yuanrong02-cache-manager-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 8    | cn-north-7-cce-cluster-yuanrong02-cache-manager-status       | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 9    | cn-north-7-cce-cluster-yuanrong03-cache-manager-cpuUsage     | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 10   | cn-north-7-cce-cluster-yuanrong03-cache-manager-memUsage     | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 11   | cn-north-7-cce-cluster-yuanrong03-cache-manager-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 12   | cn-north-7-cce-cluster-yuanrong03-cache-manager-status       | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 13   | cn-north-7-cce-cluster-yuanrong01-FunctionAccessor-cpuUsage  | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 14   | cn-north-7-cce-cluster-yuanrong01-FunctionAccessor-memUsage  | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 15   | cn-north-7-cce-cluster-yuanrong01-FunctionAccessor-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 16   | cn-north-7-cce-cluster-yuanrong01-FunctionAccessor-status    | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 17   | cn-north-7-cce-cluster-yuanrong02-FunctionAccessor-cpuUsage  | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 18   | cn-north-7-cce-cluster-yuanrong02-FunctionAccessor-memUsage  | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 19   | cn-north-7-cce-cluster-yuanrong02-FunctionAccessor-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 20   | cn-north-7-cce-cluster-yuanrong02-FunctionAccessor-status    | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 21   | cn-north-7-cce-cluster-yuanrong03-FunctionAccessor-cpuUsage  | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 22   | cn-north-7-cce-cluster-yuanrong03-FunctionAccessor-memUsage  | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 23   | cn-north-7-cce-cluster-yuanrong03-FunctionAccessor-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 24   | cn-north-7-cce-cluster-yuanrong03-FunctionAccessor-status    | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 25   | cn-north-7-cce-cluster-yuanrong01-funcmonitor-cpuUsage       | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 26   | cn-north-7-cce-cluster-yuanrong01-funcmonitor-memUsage       | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 27   | cn-north-7-cce-cluster-yuanrong01-funcmonitor-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 28   | cn-north-7-cce-cluster-yuanrong01-funcmonitor-status         | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 29   | cn-north-7-cce-cluster-yuanrong02-funcmonitor-cpuUsage       | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 30   | cn-north-7-cce-cluster-yuanrong02-funcmonitor-memUsage       | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 31   | cn-north-7-cce-cluster-yuanrong02-funcmonitor-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 32   | cn-north-7-cce-cluster-yuanrong02-funcmonitor-status         | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 33   | cn-north-7-cce-cluster-yuanrong03-funcmonitor-cpuUsage       | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 34   | cn-north-7-cce-cluster-yuanrong03-funcmonitor-memUsage       | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 35   | cn-north-7-cce-cluster-yuanrong03-funcmonitor-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 36   | cn-north-7-cce-cluster-yuanrong03-funcmonitor-status         | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 37   | cn-north-7-cce-cluster-yuanrong01-functiontask-cpuUsage      | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 38   | cn-north-7-cce-cluster-yuanrong01-functiontask-memUsage      | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 39   | cn-north-7-cce-cluster-yuanrong01-functiontask-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 40   | cn-north-7-cce-cluster-yuanrong01-functiontask-status        | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 41   | cn-north-7-cce-cluster-yuanrong02-functiontask-cpuUsage      | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 42   | cn-north-7-cce-cluster-yuanrong02-functiontask-memUsage      | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 43   | cn-north-7-cce-cluster-yuanrong02-functiontask-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 44   | cn-north-7-cce-cluster-yuanrong02-functiontask-status        | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 45   | cn-north-7-cce-cluster-yuanrong03-functiontask-cpuUsage      | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 46   | cn-north-7-cce-cluster-yuanrong03-functiontask-memUsage      | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 47   | cn-north-7-cce-cluster-yuanrong03-functiontask-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 48   | cn-north-7-cce-cluster-yuanrong03-functiontask-status        | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 49   | cn-north-7-cce-cluster-yuanrong01-FunctionInstanceManager-cpuUsage | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 50   | cn-north-7-cce-cluster-yuanrong01-FunctionInstanceManager-memUsage | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 51   | cn-north-7-cce-cluster-yuanrong01-FunctionInstanceManager-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 52   | cn-north-7-cce-cluster-yuanrong01-FunctionInstanceManager-status | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 53   | cn-north-7-cce-cluster-yuanrong02-FunctionInstanceManager-cpuUsage | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 54   | cn-north-7-cce-cluster-yuanrong02-FunctionInstanceManager-memUsage | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 55   | cn-north-7-cce-cluster-yuanrong02-FunctionInstanceManager-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 56   | cn-north-7-cce-cluster-yuanrong02-FunctionInstanceManager-status | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 57   | cn-north-7-cce-cluster-yuanrong03-FunctionInstanceManager-cpuUsage | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 58   | cn-north-7-cce-cluster-yuanrong03-FunctionInstanceManager-memUsage | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 59   | cn-north-7-cce-cluster-yuanrong03-FunctionInstanceManager-filesystemUsage | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 60   | cn-north-7-cce-cluster-yuanrong03-FunctionInstanceManager-status | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 61   | cn-north-7-cce-cluster-yuanrong01-workflow-cpuUsage          | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 62   | cn-north-7-cce-cluster-yuanrong01-workflow-memUsage          | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 63   | cn-north-7-cce-cluster-yuanrong01-workflow-filesystemUsage   | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 64   | cn-north-7-cce-cluster-yuanrong01-workflow-status            | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 65   | cn-north-7-cce-cluster-yuanrong02-workflow-cpuUsage          | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 66   | cn-north-7-cce-cluster-yuanrong02-workflow-memUsage          | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 67   | cn-north-7-cce-cluster-yuanrong02-workflow-filesystemUsage   | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 68   | cn-north-7-cce-cluster-yuanrong02-workflow-status            | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 69   | cn-north-7-cce-cluster-yuanrong03-workflow-cpuUsage          | 容器组件 | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 70   | cn-north-7-cce-cluster-yuanrong03-workflow-memUsage          | 容器组件 | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 71   | cn-north-7-cce-cluster-yuanrong03-workflow-filesystemUsage   | 容器组件 | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 72   | cn-north-7-cce-cluster-yuanrong03-workflow-status            | 容器组件 | 组件状态       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
| 73   | cn-north-7-cce-cluster-yuanrong01-cpuUsage                   | 主机     | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 74   | cn-north-7-cce-cluster-yuanrong01-memUsage                   | 主机     | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 75   | cn-north-7-cce-cluster-yuanrong01-filesystemUsage            | 主机     | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 76   | cn-north-7-cce-cluster-yuanrong01-data_percent               | 主机     | 磁盘空间使用率 | >=80%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 77   | cn-north-7-cce-cluster-yuanrong02-cpuUsage                   | 主机     | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 78   | cn-north-7-cce-cluster-yuanrong02-memUsage                   | 主机     | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 79   | cn-north-7-cce-cluster-yuanrong02-filesystemUsage            | 主机     | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 80   | cn-north-7-cce-cluster-yuanrong02-data_percent               | 主机     | 磁盘空间使用率 | >=80%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 81   | cn-north-7-cce-cluster-yuanrong03-cpuUsage                   | 主机     | CPU使用率      | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 82   | cn-north-7-cce-cluster-yuanrong03-memUsage                   | 主机     | 物理内存使用率 | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
| 83   | cn-north-7-cce-cluster-yuanrong03-filesystemUsage            | 主机     | 文件系统使用率 | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
| 84   | cn-north-7-cce-cluster-yuanrong03-data_percent               | 主机     | 磁盘空间使用率 | >=80%    | 平均值   | 1min     | 1        | 重要     | 是       |

### 4. 上下游依赖需求
与施广帅对接在数据面添加告警规则。

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
