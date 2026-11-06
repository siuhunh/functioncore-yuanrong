[TOC]

# 告警二进制编译和使用

## 1. 编译

```shell
# 编译
bash functioncore/tools/autoscaleralarm/alarm/cmd/build.sh

# 查看生成
ls -l functioncore/tools/autoscaleralarm/alarm/cmd/output
total 13816
-rwx------ 1 root root 14146656 Aug 15 19:17 etcdAlarm
```

## 2. 使用

检查二进制是否有执行权限，若没有，增加。

```shell
chmod +x autoscaleralarm
```

[配置config文件](./config.md)

```shell
./autoscaleralarm --config=${配置文件路径}  
[root@dggphis36582 bin]# ./autoscaleralarm   --config=./config.json
2024-02-26 09:10:22.406 | INFO | localauth/authcheck.go:309 | dggphis36582 | current timestamp                                  DiffLimit is 5.000000
2024-02-26 09:10:22.436 | INFO | etcd3/client.go:259 | dggphis36582 | etcd cluster address is                                   [http://10.243.32.188:32379]
2024-02-26 09:10:22.741 | INFO | alarm/alarm.go:271 | dggphis36582 | register master node circle service start!
```