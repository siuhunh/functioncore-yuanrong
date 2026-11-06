[TOC]

# ETCD 虚机快速部署

## 1. 目录树

默认部署在 /home/etcd 目录下，基本目录树结构如下：

```shell
tree -L 2 /home/etcd/
/home/etcd/ # etcd 部署目录
├── alarm # etcd 告警目录 不开启告警的话可以不配置该目录
│   ├── alarm.log # 告警日志，有告警产生后生成
│   ├── config.json # 告警相关配置
│   ├── etcdAlarm # etcd告警二进制，functioncore/tools/etcd-deploy/etcdalarm/cmd/build.sh 编译产生
├── bin # bin 目录，来源：functioncore/tools/etcd-deploy/bin
│   ├── cluster.cfg # 基础配置
│   ├── crontab.sh # 定时任务脚本
│   ├── log-div.sh # 日志压缩脚本
│   ├── monitor.sh # etcd进程监控脚本
│   ├── start.sh # etcd启动脚本
│   └── status.sh # etcd状态查询脚本
├── cipher # cipher 数据面集群环境加解密算法密钥
│   ├── common_shared.key
│   └── root.key
├── data  # etcd 元数据目录，执行启动脚本后生成
│   └── member
├── etcdlogs # etcd 日志目录，执行启动脚本后生成
│   ├── bak # etcd 日志压缩备份目录
│   └── etcd.log # etcd 日志文件
├── etcd-v3.4.10-linux-amd64 # etcd服务包，来源：etcd官网，v3.4.10 release
│   ├── etcd # etcd服务启动命令
│   ├── etcdctl # etcd服务交互命令
├── https # 数据面集群环境 tls通信证书和密钥
│   ├── cert_pwd
│   ├── server.cer
│   ├── server_key.pem
│   └── trust.cer
├── ntpcheck # ntp检测配置目录
│   ├── ntp_check # ntp检测二进制，functioncore/cmd/ntp-check/build.sh
│   ├── ntp_check_config.json # ntp检测配置
    └── ntp_check.log # ntp检测日志
└── ssl # etcd ssl目录：etcd双向鉴权密钥，不开启双向鉴权可以不配置该目录
    ├── ca.pem # 根证书文件
    ├── cert.pem # 证书链文件
    └── key.pem # 证书链私钥
```

## 2. 部署

1 准备三台虚拟机，分别在三台虚机的/home/etcd目录下依照目录树结构搭建好部署目录。

2 填写部署配置：

node1:
```shell
vim bin/cluster.cfg

# 当前节点是node1，INDEX=0
INDEX=0
# 指定节点列表
NODES_LIST=node_ip1,node_ip2,node_ip3
```
node2:
```shell
vim bin/cluster.cfg

# 当前节点是node2，INDEX=1
INDEX=1
# 指定节点列表
NODES_LIST=node_ip1,node_ip2,node_ip3
```
node3:
```shell
vim bin/cluster.cfg

# 当前节点是node3，INDEX=2
INDEX=2
# 指定节点列表
NODES_LIST=node_ip1,node_ip2,node_ip3
```
  其他配置均可使用默认配置。

  * 默认占用32379,32380端口 
  * 默认开启ssl鉴权 
  * 默认开启etcd告警。
  * 默认开启NTP检查和告警。

3 部署

三台虚机分别执行如下启动命令：
```shell
# 执行启动脚本
bash bin/start.sh

# 执行定时任务脚本
bash bin/crontab.sh
```

4 检查集群运行状态
```shell
# 检查集群运行状态
bash bin/status.sh
```

