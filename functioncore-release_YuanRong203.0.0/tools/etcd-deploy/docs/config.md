[TOC]

# 部署脚本配置详细说明

## 1. ETCD部署配置

**目录树：** bin/cluster.cfg
```shell
tree -L 1 /home/etcd/bin/

/home/etcd/bin/
├── cluster.cfg # ETCD启动 配置
├── crontab.sh
├── log-div.sh
├── monitor.sh
├── start.sh
└── status.sh
```

**配置内容详解：**
```cfg
# INDEX 即NODES_LIST索引，用于标记当前机器是哪一个node
INDEX=0
# NODES_LIST 即 node列表，记录ETCD集群内三个节点的IP，当前节点IP与INDEX需要对应
NODES_LIST=ip_1,ip_2,ip_3

# ETCD_BASE_PATH：基础目录
ETCD_BASE_PATH=/home/etcd
# ETCD_BIN_PATH ETCD启动脚本目录：用于存放启动脚本，定时任务脚本等
ETCD_BIN_PATH=${ETCD_BASE_PATH}/bin
# ETCD_STORE_BASE_DIR etcd版本包路径
ETCD_STORE_BASE_DIR=${ETCD_BASE_PATH}/etcd-v3.4.10-linux-amd64
# ETCD_APP_PATH etcd boot command
ETCD_APP_PATH="${ETCD_STORE_BASE_DIR}"/etcd

# ETCD_CTL_PATH ETCD 交互命令
ETCD_CTL_PATH="${ETCD_STORE_BASE_DIR}"/etcdctl
# ETCD_DATA_DIR etcd 数据目录
ETCD_DATA_DIR=${ETCD_BASE_PATH}/data
# LOG_DIR etcd 日志目录
LOG_DIR=${ETCD_BASE_PATH}/etcdlogs
# ETCD_BACK_DIR etcd 数据备份目录
ETCD_BACK_DIR=${ETCD_BASE_PATH}/backup
# ETCD_SSL_PATH etcd 双向认证证书目录
ETCD_SSL_PATH=${ETCD_BASE_PATH}/ssl
# ETCD_ALARM_PATH etcd 告警配置目录
ETCD_ALARM_PATH=${ETCD_BASE_PATH}/alarm

# CLIENT_PORT 指定etcd客户端监听端口
CLIENT_PORT=32379

# PEER_PORT 指定etcd服务端间监听端口
PEER_PORT=32380

# CLUSTER_STATE 用于标记集群存在状态，默认可以不修改
CLUSTER_STATE=existing

# 用于开启ETCD双向认证，指定证书名和路径
SSL_ENABLE=true
CA_FILE=${ETCD_SSL_PATH}/ca.pem
CERT_FILE=${ETCD_SSL_PATH}/cert.pem
KEY_FILE=${ETCD_SSL_PATH}/key.pem

PEER_CA_FILE=${ETCD_SSL_PATH}/ca.pem
PEER_CERT_FILE=${ETCD_SSL_PATH}/cert.pem
PEER_KEY_FILE=${ETCD_SSL_PATH}/key.pem

# BACKEND_BYTES 配置ETCD后端存储最大值 8Gb
BACKEND_BYTES=8589934592

# AUTO_COMPACTION 配置ETCD自动压缩，1表示1小时
AUTO_COMPACTION=1

# ETCD_ALARM_ENABLE 用于开启ETCD告警
ETCD_ALARM_ENABLE=true

# CLUSTER_NAME 用于etcd集群命名，三个节点配置一致即可
CLUSTER_NAME=etcd-cluster
```

## 2. 告警配置项说明

**目录树：** alarm/config.json
```shell
tree -L 2 /home/etcd/alarm/

/home/etcd/alarm/
├── alarm.log
├── config.json # ETCD告警 配置
├── etcdAlarm
```

**配置内容详解：**

```json
{

  "address": "指定告警地址",
  "authConfig": {
    "tlsProtocol": "默认即可",
    "tlsCiphers": "默认即可",
    "AKey": "k8s环境组件AKey",
    "SKey": "k8s环境组件SKey",
    "sslPath": "默认即可",
    "cipherPath": "默认即可"
  },
  "locationInfo": {
    "region": "指定环境所属region", 
    "cluster": "指定环境所属集群",
    "NodeIP": "指定当前节点的IP",
    "service": "指定服务名，默认即可 routerETCD or metadataETCD"
  },
  "alarmInfo": {
    "importantLevel": "指定告警级别 1：紧急 2：重要",
    "content": "指定告警内容：router etcd has been restarted",
    "resource": "指定资源名 routerETCD or metadataETCD"
  }
}

```

## 3. NTP检查和告警配置项说明
**目录树：**/home/etcd/ntpcheck/
```shell
tree -L 1 /home/etcd/ntpcheck/
/home/etcd/ntpcheck/
├── ntp_check 
├── ntp_check_config.json # ntp检测配置文件
└── ntp_check.log 
```

**配置内容详解：**
```conf
[omsvc]
# 指定告警地址
address = ip:port

[ntp_server]
# support address0 address1 ... address10 默认不修改
address0 = ntp01-in.huawei.com:123
address1 = ntp02-in.huawei.com:123
address2 = ntp03-in.huawei.com:123

[check]
# 指定检测间隔 单位：s
interval   = 5
# 指定连续失败次数后告警
alarm_cnt  = 1
# 指定NTP检测时间差值，超过范围后恢复
max_offset = 150

[alarm]
# 指定告警内容
content = "The NTP offset is greater than 150 seconds. hostname.ip"

[tls]
# 默认tls协议，无需修改
tls_protocol = TLSv1.2
# 默认tls算法套，无需修改
tls_ciphers  = TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
# 数据面集群accessKey
AKey        = ${aKey}
# 数据面集群secretKey
SKey        = ${sKey}
# 数据面集群TLS通信证书密钥路径
ssl_path    = /home/etcd/https
# 数据面集群加解密密钥路径
cipher_path = /home/etcd/cipher
```


