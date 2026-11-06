[TOC]

# 1. 使用背景

当前存在场景：生产环境使用 Redis5.0，不支持 TLS 通信。

stunnel：用于本身不支持安全通信的服务，增加安全通信功能，不涉及工具源码的修改。参考[公司内部技术标准规范库/Redis安全加固指南](https://w3.huawei.com/ipd/tsl/#!tsl_new/standard/standard.html?standardId=43978)3.5章节，使用 stunnel 搭建 SSL 安全通信通道。

stunnel 原理:

* tcp 通信: 通信双方直接进行信息交流。

> client --------> server

* 使用 stunnel: 通过 stunnel 转发 client 和 server 之间的消息。

> client ---> stunnel-client  ------------>  stunnel-server ---> server

# 2. stunnel 搭建安全通信通道

## 1. stunnel 安装

**1. yum 安装**

```sh
yum install stunnel
```

**2. 源码安装**

推荐安装最新版本：当前最新版5.60。

[stunnel官网最新版下载页面](http://www.stunnel.org/downloads.html )

```sh
# 源码安装
wget https://www.stunnel.org/downloads/stunnel-5.60.tar.gz
# 解压
tar -xvzf stunnel-5.60.tar.gz
cd stunnel-5.60
./configure
make && make install
```

如果安装遇到问题，可以参照官方文档:

[FAQ](https://www.stunnel.org/faq.html)

## 2. 配置文件编辑

编辑配置文件

```sh
vim stunnel.conf
```

*说明：*

```sh
# 配置文件是自定义的，可以自己创建一个配置文件。比如:
touch test.conf
vim test.conf
# 如果环境没有vi/vim命令，从其他环境导入也可，或者创建文件后通过 echo 写入
touch test.conf
echo "***" >> test.conf
```

```conf
client=yes/no

verify=2
cert=${cert_path}/cert.pem
key=${key_path}/key.pem
CAfile=${ca_path}/ca.pem

debug=info
output=${log_path}

foreground=no/yes

# 服务名
[Redis]
accept=[IP]:port
connect=IP:port
```

配置详解：

|   配置项   |       值        |                             含义                             |
| :--------: | :-------------: | :----------------------------------------------------------: |
|   client   |     yes/no      |                yes 表示客户端；no 表示服务端                 |
|   verify   |      1/2/3      |                         证书校验等级                         |
|    cert    |  ${cert_path}   |                        指定证书链文件                        |
|    key     |   ${key_path}   |                     cert 指定证书的私钥                      |
|   CAfile   |   ${ca_path}    |                          ca 文件名                           |
|   debug    | 0/1/2/3/4/5/6/7 | emerg (0)，alert (1)， crit (2)，err (3)，warning (4)，notice (5)，info (6)或debug (7) 。所有数值小于等于该级别的日志将被显示，不指定的话默认是5。 |
|   output   |   ${log_path}   |                       指定日志输出路径                       |
| foreground |     yes/no      |              yes 表示前台运行；no 表示后台运行               |
|   accept   |    [IP]:port    |  指定 stunnel 的监听地址：服务端指定端口，客户端指定IP:port  |
|  connect   |     IP:port     |                    指定 stunnel 连接地址                     |

1. verify：TLS 校验等级。

  > 1	如果对端提供证书，才做校验
  >
  > 2	对端必须提供安全证书，并校验安全证书，双向认证
  >
  > 3	必须提供安全证书，并根据CA校验证书是否合法，校验证书链，推荐
  >
  > 如果不开启，默认不验证

  自签证书，双向校验对端权限，开启等级2即可。

2. debug：日志等级。

  > emerg (0)，alert (1)， crit (2)，err (3)，warning (4)，notice (5)，info (6)或debug (7) 。
  >
  > 所有数值小于等于该级别的日志将被显示，不指定的话默认是5。

3. 服务配置：

    1. server端：

        ```conf
        [服务名]
        # 服务端 stunnel 监听客户端 stunnel 的端口
        accept=port
        # 服务端 stunnel 连接服务端的组件
        connect=IP:port
        ```

    2. client端：

        ```conf
        [服务名]
        # 客户端 stunnel 监听客户端组件的访问
        accept=IP:port
        # 客户端 stunnel 连接服务端 stunnel
        connect=IP:port
        ```

    accept：表示 stunnel 监听访问的地址/端口。

    connect：表示 stunnel 主动去连接的地址/端口。

    *注：*

    stunnel 服务端和客户端 各自使用自己的配置去启动， TLS 相关证书使用同一套，进行双向认证。

## 3. 启动

```sh
stunnel stunnel.conf
```

# 3. Example

### Redis 通过 stunnel 搭建安全通信通道

[证书生成参考](./encryption_authentication.md)

```sh
# 准备好证书
ls -al functioncore/tools/basic/libs/ssl-tool/certs/redis-cluster/
-rw-r--r-- 1 root root 2884 Aug 19 15:39 ca.pem
-rw-r--r-- 1 root root 4531 Aug 19 15:39 cert.pem
-rw------- 1 root root 1704 Aug 19 15:39 key.pem
```

*注意：*

> 生产环境使用这套密钥时，注意控制密钥权限。
>
> 以下测试，将生成的密钥拷贝到客户端和服务端机器的指定目录：/home/redis_ssl，根据实际场景指定。

1. 启动 Redis 服务。

   假设当前 Redis 服务部署在 10.10.10.10 节点上，开放端口 6379。

   访问地址限定：127.0.0.1:6379，限制本机访问。

2. 服务端 stunnel 配置和启动：

   *配置文件默认指定：/etc/stunnel/stunnel.conf，根据实际场景指定。*

   ```sh
   vim /etc/stunnel/stunnel.conf
   ```

   ```conf
   client=no
   
   verify=2
   cert=/home/redis_ssl/cert.pem
   key=/home/redis_ssl/key.pem
   CAfile=/home/redis_ssl/ca.pem
   
   debug=notice
   output=stunnel.log
   
   foreground=no
   
   # 服务名
   [Redis]
   # 开放16379端口，供客户端 stunnel 访问
   accept=16379
   # stunnel 与 Redis 同节点部署，访问 Redis服务
   connect=127.0.0.1:6379
   ```

   ```sh
   # 启动
   stunnel /etc/stunnel/stunnel.conf
   ```

3. 客户端 stunnel 配置和启动：

   ```sh
   vim /etc/stunnel/stunnel.conf
   ```

   ```conf
   # 注意 client 变成了 yes
   client=yes
   
   verify=2
   cert=/home/redis_ssl/cert.pem
   key=/home/redis_ssl/key.pem
   CAfile=/home/redis_ssl/ca.pem
   
   debug=notice
   output=stunnel.log
   
   foreground=no
   
   # 服务名
   [Redis]
   # 客户端 stunnel 接收本机客户端服务的访问
   accept=127.0.0.1:6379
   # 远程访问服务端的 stunnel
   connect=10.10.10.10:16379
   ```

   ```sh
   # 启动
   stunnel /etc/stunnel/stunnel.conf
   ```

4. 验证：

   ```sh
   # 客户端使用 Redis-cli 去访问客户端的stunnel
   Redis-cli -h 127.0.0.1 -p 6379
   # 校验 Redis 密码
   auth password
   # set get 验证
   set "aaa" "bbb"
   get "aaa"
   	"bbb" # 显示bbb
   ```

# 4. FunctionGraph-YuanRong 融合版本搭建 

## 1. 使用场景

当前 YuanRong 数据面组件 instanceManager 会连接 Redis 服务。由于使用的 Redis 服务版本是 5.0，不支持TLS，故需要使用 stunnel 建立安全通道。

## 2. 搭建方案

**客户端：即 instanceManager**

1. 挂载证书相关文件。

   1. 准备 ssl 通信依赖的证书：FunctionGraph-YuanRong 融合版本由 FunctionGraph 提供。

   如：

   ```sh
   # 已在 /root/stunnel_ssl/redis_ssl 准备证书 ca.pem,cert.pem,key.pem
   pwd
   # /root/stunnel_ssl/redis_ssl
   ls 
   # ca.pem  cert.pem  key.pem
   ```

   2. 将准备好的证书作 secrets 挂载到集群内。

      ```sh
      kubectl create secret generic redis-secret --from-file=ca.pem=/root/stunnel_ssl/redis_ssl/ca.pem --from-file=cert.pem=/root/stunnel_ssl/redis_ssl/cert.pem --from-file=key.pem=/root/stunnel_ssl/redis_ssl/key.pem
      ```

2. 修改配置文件: values.yaml。

   |                    配置项                     |        默认值        |                    含义                     |
   | :-------------------------------------------: | :------------------: | :-----------------------------------------: |
   |     .Values.global.clusterRedis.sslEnable     |         true         |      表示 redis 通过 stunnel 开启 ssl       |
   | .Values.global.clusterRedis.stunnelServerAddr | StunnelServerIP:port | 需要自行配置，填写 stunnel 服务端的访问地址 |

3. instanceManager 镜像需要集成 stunnel

   1. 单独进入执行 instanceManager 的镜像编译

      ```sh
      # 增加 -e 参数，表示集成 stunnel 镜像，支持开启 ssl
      cd /functioncore/build/workermanager/docker_build.sh -v ${image_id} -s
      ```

**服务端：即 Redis**

送检：Redis 服务自行搭建，使用 Redis5.0 版本，模拟现网环境 Redis。

推荐使用 Redis6.0 版本，已支持 TLS 通信实现。