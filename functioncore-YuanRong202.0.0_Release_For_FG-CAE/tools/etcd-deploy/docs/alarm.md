[TOC]

# 告警二进制编译和使用

## 1. 编译

```shell
# 编译
bash functioncore/tools/etcd-deploy/etcdalarm/cmd/build.sh

# 查看生成
ls -l functioncore/tools/etcd-deploy/etcdalarm/cmd/output
total 13816
-rwx------ 1 root root 14146656 Aug 15 19:17 etcdAlarm
```

## 2. 使用

检查二进制是否有执行权限，若没有，增加。

```shell
chmod +x etcdAlarm
```

[配置config文件](./config.md)

```shell
./etcdAlarm
[2022-08-15 11:58:09.020 INFO localauth/authcheck.go:309] [dggphis36370] current timestampDiffLimit is 5.000000
Usage of ./etcdAlarm:
  -alarmType string
        specify whether to report or clear alarms. Options: report or clear
  -config string
        specify the path of the alarm binary configuration file (default "/home/etcd/alarm/config.json")
```

```shell
# 告警
./etcdAlarm --alarmType report [--config /home/etcd/alarm/config.json]
[2022-08-15 12:08:17.089 INFO localauth/authcheck.go:309] [fgs-yuanrong-az1-01-70762-btixk] current timestampDiffLimit is 5.000000
[2022-08-15 12:08:17.103 INFO alarm/alarm.go:175] [fgs-yuanrong-az1-01-70762-btixk] reported alarm successfully, alarm type:1, resource: testZtRouterEtcd
# 消警
./etcdAlarm --alarmType clear [--config /home/etcd/alarm/config.json]
[2022-08-15 12:09:15.419 INFO localauth/authcheck.go:309] [fgs-yuanrong-az1-01-70762-btixk] current timestampDiffLimit is 5.000000
[2022-08-15 12:09:15.452 INFO alarm/alarm.go:175] [fgs-yuanrong-az1-01-70762-btixk] reported alarm successfully, alarm type:2, resource: testZtRouterEtcd
```