# 编译方式

```shell
cd aksk-tool 
go bulid -o auth .
```
会生成 auth 的可执行文件

# 使用方式
```shell
./auth -h
Usage:
  auth [flags]

Flags:
  -h, --help       help for auth
  -p, --port int   HTTP port (default 8000)
  -s, --server     Enabling the HTTP Service

```

1.单词执行，获取AKSK鉴权信息
```shell
./auth
authorization:SDK-HMAC-SHA256 Credential=R0SOASQS5M3V4CCZ2BEC/20210804///sdk_request, SignedHeaders=content-type;x-sdk-date, Signature=08b5ba1fc77af13421fd0c9eae8a62197a70f5ba15c75898be51608b9ce1325b, appid=auth-cli
X-Timestamp-Auth:1628060579
```
2.开启http服务,-s 用来开启http服务，此时需要调用http接口来获取鉴权数据
```shell
./auth -s
# 使用curl 获取数据
curl 127.0.0.1:8000
{"authorization":"SDK-HMAC-SHA256 Credential=R0SOASQS5M3V4CCZ2BEC/20210804///sdk_request, SignedHeaders=content-type;x-sdk-date, Signature=f6bc69198c6d4779a00c0d6e0d9d95408780e1abaeceaa69203ff5a75856c02f, appid=auth-cli","x-timestamp-auth":"1628060813"}[
```

# 配置文件说明

config.yaml 用来配置AKSK信息

工具还需要有加解密配置文件root.key commom_shared.key，因为仓库限制，无法上传，可取测试或者开发环境的配置文件使用