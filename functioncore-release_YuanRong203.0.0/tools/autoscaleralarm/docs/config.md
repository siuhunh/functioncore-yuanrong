## 告警配置项说明

**目录树：** alarm/config.json
```shell
tree -L 2 /home/etcd/autoscaleralarm/

/home/autoscaleralarm/alarm/
├── alarm.log
├── config.json # ETCD告警 配置
├── autoscaleralarm
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
  "caeRouteEtcd": {
    "servers":  "cae router etcd 地址",
    "user": "账号",
    "password": "密码",
    "cafile": "ca 证书路径,自定义位置",
    "certfile": "cert 证书配置, 自定义位置",
    "keyfile": "etcd key file,自定义位置"
  }
}
```


