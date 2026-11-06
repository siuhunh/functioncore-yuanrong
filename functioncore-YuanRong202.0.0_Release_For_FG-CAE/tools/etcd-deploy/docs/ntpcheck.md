[TOC]

# NTP 检测

## 1. 编译

```shell
# 编译
bash functioncore/cmd/ntp-check/build.sh

# 查看生成
ls -l functioncore/cmd/ntp-check/output/
total 14048
-rwx------ 1 root root 14382112 Aug 16 09:32 ntp_check
```

## 2. 使用

查看命令是否有执行权限，若没有，增加执行权限：

```shell
chmod +x ntp_check
```

[配置config文件](./config.md)

准备好配置文件，直接启动即可，默认读取命令执行目录下的config

```shell
# 启动
nohup ntp_check >> ntpcheck.log &

tail -f ntpcheck.log
[2022-08-16 06:02:34.547 INFO localauth/authcheck.go:309] [dggphis36370] current timestampDiffLimit is 5.000000
[2022-08-16 06:08:08.334 INFO localauth/authcheck.go:309] [dggphis36370] current timestampDiffLimit is 5.000000
[2022-08-16 06:08:08.335 INFO ntpcheck/ntp_client.go:49] [dggphis36370] hostName: test, localIP: 0.0.0.0
[2022-08-16 06:08:13.347 INFO ntpcheck/ntp_check.go:203] [dggphis36370] Ntp Server time : 2022-08-16 14:08:13

[2022-08-16 06:08:18.347 INFO ntpcheck/ntp_check.go:203] [dggphis36370] Ntp Server time : 2022-08-16 06:08:18
[2022-08-16 06:08:23.347 INFO ntpcheck/ntp_check.go:203] [dggphis36370] Ntp Server time : 2022-08-16 06:08:23
[2022-08-16 06:08:28.347 INFO ntpcheck/ntp_check.go:203] [dggphis36370] Ntp Server time : 2022-08-16 06:08:28
```

可以自己指定config路径
```shell
./ntp_check --help
[2022-08-16 06:06:43.688 INFO localauth/authcheck.go:309] [dggphis36370] current timestampDiffLimit is 5.000000
Usage of ./ntp_check:
  -config string
        specify the configuration directory (default "ntp_check_config.json")
        
# 例：
./ntp_check --config /home/etcd/ntpcheck/ntp_check_config.json
[2022-08-16 06:08:08.334 INFO localauth/authcheck.go:309] [dggphis36370] current timestampDiffLimit is 5.000000
[2022-08-16 06:08:08.335 INFO ntpcheck/ntp_client.go:49] [dggphis36370] hostName: test, localIP: 0.0.0.0
[2022-08-16 06:08:13.347 INFO ntpcheck/ntp_check.go:203] [dggphis36370] Ntp Server time : 2022-08-16 14:08:13
```



