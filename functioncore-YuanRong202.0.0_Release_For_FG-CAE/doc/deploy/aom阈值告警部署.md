[TOC]

【functionCore】AOM阈值告警部署


### 1. 描述

对数据面集群添加告警配置，在出现监控指标超过阈值情况时及时通知相关人员，相关人员登录集群查看数据面运行情况，如果已出现数据面服务异常情况，优先恢复数据面服务，然后分析导致指标超过阈值原因并输出解决方案。

### 2. 操作说明

#### 如何添加监控告警
1. 进入华为云界面的云容器引擎CCE。
2. 单击“监控中心”。
3. 选择“告警 > 告警规则”。
4. 单击“创建阈值”。
5. 在“添加阈值”界面，根据界面提示配置参数。
5.1. 输入阈值名称。
5.2. 选择资源类型：可选择容器组件、虚机组件、主机、主机磁盘、主机网络等等，选择主机或容器组件。
5.3. 选择资源后点击下一步；当选择资源类型为主机时，选择资源展开项就是集群下所有主机；当选择资源类型为容器组件时，选择资源展开项就是集群下的组件。
5.4.开始定义阈值，首先选择指标名称。
5.5. 输入阈值条件，例如CPU使用率，大于等于95%，连续周期5，阈值统计方式“平均值”，统计周期“一分钟”。
5.6. 输入告警级别，告警级别有提示、次要、重要、紧急。
5.7. 选择是否发送通知，如果要发送通知，需选择主题。
5.8. 如果发送通知配置为是，需要配置触发场景，有"超限阈值"和"恢复正常"。

### 3.数据面添加阈值配置
#### 阈值命名规则：

**主机**： regionID-yuanrongClusterID-metricsName
**组件**： regionID-yuanrongClusterID-serviceName-metricsName

- regionID为区域ID，例如乌兰察布202：cn-north-6;
- yuanrongClusterID为元戎数据面集群ID，例如fgs-yuanrong01-az1;
- serviceName为常驻服务名称。（数据面常驻服务：cache-manager、FunctionAccessor(frontend)、FunctionTask、FunctionInstanceManger(WorkerManager)）
- metricsName为阈值指标，例如CPU使用率。[指标详细说明](https://support.huaweicloud.com/productdesc-aom/aom_06_0009.html)



####  数据面阈值规则：

- 已部署区域：乌兰202，广州

- 以乌兰202的fgs-yuanrong01-az1集群为例，规则见下表

- 

- | 序号 | 阈值名称                                                     | 资源类型 | 指标名称                                                     | 阈值条件 | 统计方式 | 统计周期 | 连续周期 | 告警级别 | 是否通知 |
  | ---- | ------------------------------------------------------------ | -------- | ------------------------------------------------------------ | -------- | -------- | -------- | -------- | -------- | -------- |
  | 1    | cn-north-6-fgs-yuanrong01-az1-cache-manager-cpuUsage         | 容器组件 | CPU使用率                                                    | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 2    | cn-north-6-fgs-yuanrong01-az1-cache-manager-memUsage         | 容器组件 | 物理内存使用率                                               | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 3    | cn-north-6-fgs-yuanrong01-az1-cache-manager-filesystemUsage  | 容器组件 | 文件系统使用率                                               | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
  | 4    | cn-north-6-fgs-yuanrong01-az1-cache-manager-status           | 容器组件 | 组件状态                                                     | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 5    | cn-north-6-fgs-yuanrong01-az1-FunctionAccessor-cpuUsage      | 容器组件 | CPU使用率                                                    | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 6    | cn-north-6-fgs-yuanrong01-az1-FunctionAccessor-memUsage      | 容器组件 | 物理内存使用率                                               | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 7    | cn-north-6-fgs-yuanrong01-az1-FunctionAccessor-filesystemUsage | 容器组件 | 文件系统使用率                                               | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
  | 8    | cn-north-6-fgs-yuanrong01-az1-FunctionAccessor-status        | 容器组件 | 组件状态                                                     | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 9    | cn-north-6-fgs-yuanrong01-az1-funcmonitor-cpuUsage           | 容器组件 | CPU使用率                                                    | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 10   | cn-north-6-fgs-yuanrong01-az1-funcmonitor-memUsage           | 容器组件 | 物理内存使用率                                               | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 11   | cn-north-6-fgs-yuanrong01-az1-funcmonitor-filesystemUsage    | 容器组件 | 文件系统使用率                                               | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
  | 12   | cn-north-6-fgs-yuanrong01-az1-funcmonitor-status             | 容器组件 | 组件状态                                                     | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 13   | cn-north-6-fgs-yuanrong01-az1-functiontask-cpuUsage          | 容器组件 | CPU使用率                                                    | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 14   | cn-north-6-fgs-yuanrong01-az1-functiontask-memUsage          | 容器组件 | 物理内存使用率                                               | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 15   | cn-north-6-fgs-yuanrong01-az1-functiontask-filesystemUsage   | 容器组件 | 文件系统使用率                                               | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
  | 16   | cn-north-6-fgs-yuanrong01-az1-functiontask-status            | 容器组件 | 组件状态                                                     | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 17   | cn-north-6-fgs-yuanrong01-az1-FunctionInstanceManager-cpuUsage | 容器组件 | CPU使用率                                                    | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 18   | cn-north-6-fgs-yuanrong01-az1-FunctionInstanceManager-memUsage | 容器组件 | 物理内存使用率                                               | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 19   | cn-north-6-fgs-yuanrong01-az1-FunctionInstanceManager-filesystemUsage | 容器组件 | 文件系统使用率                                               | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
  | 20   | cn-north-6-fgs-yuanrong01-az1-FunctionInstanceManager-status | 容器组件 | 组件状态                                                     | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 21   | cn-north-6-fgs-yuanrong01-az1-cpuUsage                       | 主机     | CPU使用率                                                    | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 22   | cn-north-6-fgs-yuanrong01-az1-memUsage                       | 主机     | 物理内存使用率                                               | >=95%    | 平均值   | 1min     | 5        | 重要     | 是       |
  | 23   | cn-north-6-fgs-yuanrong01-az1-data_percent                   | 主机     | 磁盘空间使用率                                               | >=95%    | 平均值   | 1min     | 1        | 重要     | 是       |
  | 24   | cn-north-6-fgs-yuanrong01-az1-ntpServerStatus                | 主机     | NTP服务器状态(ntpServerStatus)                               | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 25   | cn-north-6-fgs-yuanrong01-az1-nodeStatus                     | 主机     | 主机状态(nodeStatus)                                         | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 26   | cn-north-6-fgs-yuanrong01-az1-diskRWStatus                   | 主机     | 磁盘读写状态(diskRWStatus)（/var/paas,/var/lib/docker,  /mnt） | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
  | 27   | cn-north-6-fgs-yuanrong01-az1-ntpStatus                      | 主机     | NTP同步状态(ntpStatus)                                       | >0       | 平均值   | 1min     | 5        | 重要     | 是       |
