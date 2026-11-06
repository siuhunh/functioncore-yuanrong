[TOC]

# 【元戎】安全需求合集

## 功能描述
安全分析属于安全验证及确认措施的一种，目的在于确保用户信息安全以及降低系统受到攻击的风险。

## 场景分析
+ ### 场景一：


TLS服务在v1.2版本默认启用重协商服务，重协商会引入不安全的场景，导致服务方受恶意攻击。因此加入TLS服务端防止SSL重协商攻击的措施。

目前支持HTTPS的组件如下

| **序号** | **组件名**   |
| -------- | ---------- |
| 1        | FunctionAccessor  |
| 2        | etcd  |
| 3        | BusProxy  |
| 4        | 华为OBS  |
| 5        | redis-cluster  |

+ ### 场景二：

日志中包含用户名、用户ID、租户ID等敏感信息可能会造成用户信息泄露风险，不符合公司安全要求。因此，需要排查当前的所有日志中是否包含用户名、用户ID、租户ID等敏感信息。

涉及组件如下

| **序号** | **组件名**                |
| -------- | ------------------------- |
| 1        | FunctionAccessor          |
| 2        | functionbus               |
| 3        | function instance manager |
| 4        | worker                    |
| 5        | workflow                  |
| 6        | functiontask              |
| 7        | runtimemanager            |
| 8        | nodejs-runtime            |
| 9        | python-runtime            |
| 10       | java-runtime              |
| 11       | custom-runtime            |

+ ### 场景三：

包含敏感数据的文件（例如，包含敏感数据的配置文件、日志文件、个人敏感数据文件、用户口令文件、密钥文件、证书文件）和其目录的权限设置应只允许文件的属主或需要访问该文件/目录的用户拥有相应的权限。

涉及组件如下：

| **序号** | **组件名**   |
| -------- | ---------- |
| 1        | FunctionAccessor  |
| 2        | function instance manager  |
| 3        | worker  |
| 4        | runtimemanager  |
| 5        | nodejs-runtime  |
| 6        | python-runtime  |
| 7        | java-runtime  |
| 8        | custom-runtime  |
| 9        | function-task  |
| 10       | workflow  |


### 场景四：
 AR.20210621221420.046   统一加解密机制

functionGraph与functionCore原有使用的加密方式是不同的，在交付给functionGraph的组件需要统一使用functionGraph的加密方式。以便对秘钥的统一管理和维护
涉及组件包括

| **序号** | **组件名**                |
| -------- | ------------------------- |
| 1        | function accessor         |
| 2        | functionbus               |
| 3        | function instance manager |
| 4        | worker                    |

### 场景五：

系统中的各类帐号的权限需要满足“权限最小化”原则

涉及组件如下

  | **序号** | **组件名**   |
  | -------- | ---------- |
  | 1        | function accessor  |
  | 2        | functionbus/functiontask  |
  | 3        | function instance manager   |
  | 4        | worker-init     |
  | 5        | worker          |
  | 6        | runtime-manager  |
  | 7        | python-runtime  |
  | 8        | java-runtime   |
  | 9        | nodejs-runtime  |
  | 10       | custom-runtime  |
  | 11       | workerflow      |

### 场景八：

minio client端属于Apache License V2.0协议；minio server端属于AGPLv3协议，不符合开源要求。
经CCB决定使用华为云obs client替换minio client。

涉及组件如下:
+ Graph

| **序号** | **组件名**   |
| -------- | ---------- |
| 1        | cache-manager  |
| 2        | worker          |

+ Core

| **序号** | **组件名**   |
| -------- | ---------- |
| 1        | function-repo  |

## 方案详细设计

+ ### 场景一：
#### 现状分析
+ 支持HTTPS组件现阶段未关闭重协商功能，由于重协商机制并不安全，主要针对重协商的攻击如下:

> ### 1. Dos攻击
> &emsp;&emsp;TLS的handshake过程需要使用非对称算法进行身份认证和密钥协商，这个过程需要很多计算资源。Handshake本来只在TLS连接开始建立时执行一次，但由于重协商机制的引入，使得client被允许不断发起新的handshake。由于client可以使用较少的资源来执行handshake（比如：不检查server的证书，这样可以避免校验签名的开销），这样攻击者就可以更容易地耗尽server的资源导致其拒绝为其它用户的请求提供服务。
>
> &emsp;&emsp;这种攻击与分布式拒绝服务攻击(DDoS)的不同之处在于，它不需要大量的攻击来消耗网络带宽，而仅仅通过一台主机的一个TCP/IP socket来耗尽server的资源（这样就会导致当前的DoS和DDoS防御策略无效）。例如，一台server通常能执行150-300次/s握手，而一个client可以发起多达1000次/s握手请求。

> ### 2. 中间人攻击
> &emsp;&emsp;由于TLS的重协商前后的两条TLS连接之间没有关联（即使它们发生在同一条TCP连接上），而且应用层（如HTTP）与加密层很少交互，导致TLS层面发生的事情与上层应用了解到的信息不匹配。
> 因此，一个中间人攻击者就可以通过如下步骤来利用这个漏洞：
>
> 1) 拦截一个client到server的TCP连接，截住其TLS handshake请求；
>
> 2) 新建一个到server的TLS连接，在handshake之后发送攻击负载；
>
> 3) 将1中拦截的handshake请求通过与server的TLS连接发送过去，这样在server看来是重协商，而在client看来是一条全新的TLS连接。一旦重协商完成，client与server开始交换应用层数据，攻击者的攻击负载和client的正常数据就会被server合并处理，从而使得攻击成功。


#### 方案设计
+ 目前TLS v1.2版本默认打开重协商功能，TLS v1.3版本默认不支持重协商。因此目前采用的策略是关闭重协商功能。
```go
tls.Config{
    Renegotiation: tls.RenegotiateNever,
}
```
* ### 场景二：

#### 现状分析

目前代码中存在些许打印租户ID、用户ID的日志。

#### 方案设计

+ 使用**grep -irn "log" | grep -i tenantID** 和 **grep -irn "log" | grep -i userId** 等命令在代码中全局搜索。
+ 使用**grep -irn "print" | grep -i tenantID** 和 **grep -irn "print" | grep -i userId** 等命令搜索打印在终端的日志。
+ 排查有打印用户名，用户ID，租户ID等敏感信息的日志，有则删除或匿名化处理。

+ ### 场景三：
#### 现状分析
+ 目前容器中的文件权限如下(以frontend为例):
```text
.
|-- [drwxr-xr-x sn      ]  bin
|   |-- [-rwx------ sn      ]  entrypoint
|   `-- [-rwx------ sn      ]  frontend
|-- [drwxrwsrwx root    ]  config
|   |-- [drwxr-sr-x root    ]  ..2021_07_13_07_53_17.571536779
|   |   `-- [-r--r----- root    ]  log.json
|   |-- [lrwxrwxrwx root    ]  ..data -> ..2021_07_13_07_53_17.571536779
|   `-- [lrwxrwxrwx root    ]  log.json -> ..data/log.json
|-- [drwxrwsrwx root    ]  log
|   `-- [-rw------- sn      ]  frontend-run.log
|-- [drwxr-xr-x root    ]  resource
|   |-- [drwxr-xr-x root    ]  rdo
|   |   `-- [drwxr-xr-x root    ]  v1
|   |       |-- [drwxrwsrwx root    ]  apple
|   |       |   |-- [drwxr-sr-x root    ]  ..2021_07_13_07_53_17.956753733
|   |       |   |   `-- [-r--r----- root    ]  a.txt
|   |       |   |-- [lrwxrwxrwx root    ]  ..data -> ..2021_07_13_07_53_17.956753733
|   |       |   `-- [lrwxrwxrwx root    ]  a.txt -> ..data/a.txt
|   |       |-- [drwxrwsrwx root    ]  boy
|   |       |   |-- [drwxr-sr-x root    ]  ..2021_07_13_07_53_17.939900004
|   |       |   |   `-- [-r--r----- root    ]  b.txt
|   |       |   |-- [lrwxrwxrwx root    ]  ..data -> ..2021_07_13_07_53_17.939900004
|   |       |   `-- [lrwxrwxrwx root    ]  b.txt -> ..data/b.txt
|   |       |-- [drwxrwsrwx root    ]  cat
|   |       |   |-- [drwxr-sr-x root    ]  ..2021_07_13_07_53_17.285678435
|   |       |   |   `-- [-r--r----- root    ]  c.txt
|   |       |   |-- [lrwxrwxrwx root    ]  ..data -> ..2021_07_13_07_53_17.285678435
|   |       |   `-- [lrwxrwxrwx root    ]  c.txt -> ..data/c.txt
|   |       `-- [drwxrwsrwx root    ]  dog
|   |           |-- [drwxr-sr-x root    ]  ..2021_07_13_07_53_17.431457320
|   |           |   `-- [-r--r----- root    ]  d.txt
|   |           |-- [lrwxrwxrwx root    ]  ..data -> ..2021_07_13_07_53_17.431457320
|   |           `-- [lrwxrwxrwx root    ]  d.txt -> ..data/d.txt
|   `-- [drwxrwsrwx root    ]  wdo
|       |-- [drwxr-sr-x root    ]  ..2021_07_13_07_53_17.860577694
|       |   `-- [-r--r----- root    ]  wdo.json
|       |-- [lrwxrwxrwx root    ]  ..data -> ..2021_07_13_07_53_17.860577694
|       `-- [lrwxrwxrwx root    ]  wdo.json -> ..data/wdo.json
`-- [-rwxr-x--- sn      ]  tree

26 directories, 18 files
```
#### 存在问题
+ 目前deployment挂载的文件默认使用readOnly，权限暂不考虑。
+ ~~log文件夹由第三方日志工具（zap）管理生成，属主为root，权限777，是否需要修改权限。~~
+ log文件夹也是由deployment挂载进去，权限暂不考虑。
+ 排查是否生成中间文件。

#### 方案设计
+ 配置权限如下：

  | **文件类型** | **权限**   |
    | -------- | ---------- |
  | 日志文件   | 600  |
  | 配置文件   | 400  |
  | 秘钥证书文件 | 400  |
  | 文件夹    | 700  |


* ### 场景四：

#### 现状分析

数据加密

|                           | 组件初始化 | etcd密码                                                     | redis密码                         | 华为OBS | 环境变量 | RuntimeUserData custom runtime               | FG管理面写入etcd数据 |
| :------------------------ | :--------- | :----------------------------------------------------------- | :-------------------------------- | :------ | :------- | :------------------------------------------- | -------------------- |
| Function Accessor         | √          | √                                                            | × （restful接口获取redis信息）    | √       | NA       | NA                                           | NA                   |
| functiontask              | √          | √                                                            | NA                                | NA      | NA       | NA                                           | NA                   |
| Function Instance manager | √          | √                                                            | NA                                | NA      | NA       | NA                                           | √                    |
| worker                    | √          | × （仅kernel-worker使用）**<font color="#dd0000">（整改）</font>** | × （var计算runtime获取redis信息） | √       | √        | × **<font color="#dd0000">（整改 ）</font>** | NA                   |

通信鉴权

| 发送方                    | 接收方                    | 功能                            | 加密方式                                                     |
| :------------------------ | :------------------------ | :------------------------------ | :----------------------------------------------------------- |
| worker                    | functiontask              | functiontask和worker之间tcp通信 | 简易字符串（黄区）暂无（绿区）**<font color="#dd0000">（整改）</font>** |
| Function Instance manager | worker                    | load Function                   | 简易字符串（黄区） 秘钥方式（绿区）                          |
| functiontask              | Function Instance manager | 扩缩容                          | 秘钥方式                                                     |
| Function Accessor         | busProxy                  | invoke、cancel                  | 秘钥方式                                                     |
| 外部                      | Function Accessor         | invoke、cancel、query           | TLS、秘钥方式                                                |
| worker                    | Function Instance manager | 获取实例信息（内核函数）        | 秘钥方式（黄区）                                             |

秘钥方式：

即会通过IAM（黄区）、functionGraph加密方式（绿区）进行加解密

```
SignWithIAM
localauth.SignLocally
```

简易字符串：

简易字符为黄区方式，即发送如下所示字符串，在接收方去查看字符串是否为该格式即可，不涉及经过iam鉴权中心

```
signKey := "appid=" + "worker" + "=timestamp=" + curTimeTamp
```

#### 方案设计

1、worker中kernel-worker使用etcd密码

方案：整改与其它组件etcd密码一致，即通过分支判断使用不同的加解密方式

```go
switch utils.GetFuncBranch() {
case utils.DefaultBranch:
	etcdPwd, err = crypto.Decrypt(plainPwd, workKey)
case utils.FusionBranch:
	etcdPwd, err = localauth.Decrypt(plainPwd)
}
```

2、custom runtime中RuntimeUserData

冗余代码进行删除

3、functiontask和worker间通信鉴权，对鉴权数据进行加密

方案与1相似，通过分支判断使用不同的加解密方式，新增localauth.Encrypt，在worker对鉴权数据进行加密，在functiontask对鉴权数据进行解密。

```go
switch commonUtils.GetFuncBranch() {
case commonUtils.DefaultBranch:
   	key, err = crypto.Encrypt(signKey, crypto.GetWorkKey(constants.WorkKeyTask))
case commonUtils.FusionBranch:
   	key, err = localauth.Encrypt(signKey)
}
```

* ### 场景五：

#### 现状分析

+ 除各语言runtime（包括custom-runtime）和runtime-manager外，其余组件已在Dockerfile中声明使用非root的用户，已满足要求。

+ runtime-manger会设置各语言runtime（包括custom-runtime）的uid和gid，默认为1003snuser。如果用户配置了自己的uid、gid，则前端会做限制，functiongraph进行处理，数据面不做合法性校验，也满足要求。
    ```go
    // psudo code
    func SetCmdCredByID(cmd *exec.Cmd, uid, gid int) {
      cred := &syscall.Credential{
        Uid:    uid,
        Gid:    gid,
      }
      cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
    }
    ```

+ runtime-manager自身使用root权限启动，不满足要求
    ```Dockerfile
    # use root user to start runtime manager
    USER 0
    ```

#### 方案设计

+ 难点：runtime-manager需要设置各语言runtime（包括custom-runtime）的uid和gid，且该uid和gid是用户可配置的（见AR.20210522184837.003）。因为只有root用户才有权限使用setuid等的系统调用设置uid和gid，所以runtime-manager需要以root启动。同时，runtime manager还需要挂载文件系统、杀进程等的其他权限，也需要以root启动。

+ 解决方案：使用linux的capability，对runtime-manager可执行程序添加setuid和setgid等的权限，以sn用户启动runtime-manager。

  1. 修改runtime-manager的Dockerfile创建sn和snuser两个用户以及用户组

    ```Dockerfile
    # psudo code
    RUN groupadd -g 1002 sn && \
        useradd -u 1002 -g 1002 sn && \
        groupadd -g 1003 snuser && \
        useradd -u 1003 -g 1003 snuser
    ```

    将runtime相关内容设给snuser，runtime-manager相关设给sn。

    使用setcap命令设置runtime-manager进程的setuid和setgid等的其他权限

    ```Dockerfile
    # psudo code
    RUN setcap 'cap_setuid,cap_setgid=ep ...' runtime-manager
    ```

    使用sn用户启动runtime-manager。

  2. 同时修改function instance manager创建runtime容器时设置的corev1.Capabilities，使其与setcap中的相同

    ```go
    // psudo code
    corev1.Capabilities{
		  Add: []corev1.Capability{"SET_UID", "SET_GID", ...},
	  }
    ```

  3. 在Dockerfile中将sn用户加入snuser用户组

    ```Dockerfile
    RUN usermod -aG snuser sn
    ```

    使runtime-manager可以监控为snuser权限的/tmp目录

  4. 修改runtime-manager中mount相关的代码，使用系统调用“mount”而不是mount程序，因为mount程序会判断用户是否为root，非root直接报错。

    ```go
    // psudo code

    // 将这类代码改为
    cmd := exec.Command("sh", "-c", "mount -t tmpfs -o size=2m tmpfs ./abc")
    cmd.Run()

    // 系统调用的mount
    syscall.Mount("tmpfs", "./abc", "tmpfs", 0, "size=2m")
    ```

    需要注意的是，nfs类型的mount需要添加addr=ip在最后一个data参数中，例如

    ```go
    // 10.0.0.1是通过dns解析my-nfs-server得到的
    syscall.Mount("my-nfs-server:/abc", "/abc", "nfs", 0, "addr=10.0.0.1,...")
    ```

  5. 修改runtime-manager中useradd、groupadd、usermod相关代码，直接修改/etc/passwd、/etc/group创建新用户和用户组，因为这些程序同样会判断用户是否为root，非root直接报错。

  6. 增加的权限有

    ```go
    cap_setuid
    cap_setgid
    cap_dac_override // 创建mount的本地路径，有权限修改/etc/passwd，/etc/group文件
    cap_fowner // 创建mount的本地路径
    cap_kill // 杀进程权限
    cap_sys_admin // mount权限
    ```

* ### 场景六：

需求：
使用HTTPS下载函数代码，保障包括机密性，真实性以及完整性。
机密性是指，传输的内容不被除通信的双方外的第三方获取；
真实性是指，通信的对端正是期待的对端，而不是其它第三方冒充的；
完整性则是指，传输的数据是完整的，数据没有被篡改或丢失。

#### 1. 场景分析
[AR.20210621221420.043-HTTPS下载函数代码-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/2928afd8ae624e609af7b0148bf05fad?projectDiagramUUID=5d95a47309594dd3beeece98d2ad986c&versionUUID=)

![AR.20210621221420.043-HTTPS下载函数代码](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/uwj?t=1626227930247)

#### 2. 方案详细设计
1. s3开启https, helm部署前修改配置项`HTTPS.Enable.S3` 为true
2. 客户端验证服务端证书
（1） 华为云obs配置的证书是受信的，无需配置ca证书

验证：修改`HTTPS.Enable.S3` 为true，创建并调用函数成功，日志如下：
````
[2021-07-14 07:04:15.910 INFO app/server.go:93] [pool22-300-128-fusion-69d89bc7fb-bgccq] starting to create the app server
[2021-07-14 07:04:15.910 INFO tcpbus/client.go:79] [pool22-300-128-fusion-69d89bc7fb-bgccq] client succeed to create connection to server 7.185.19.125:32568
[2021-07-14 07:04:16.013 WARN tls/option.go:75] [pool22-300-128-fusion-69d89bc7fb-bgccq] failed to load root ca, err: open /home/sn/resource/ca/ca.pem: no such file or directory
[2021-07-14 07:04:16.013 INFO deploy/deployer.go:106] [pool22-300-128-fusion-69d89bc7fb-bgccq] s3 EndpointURL: https://obs.cn-north-7.ulanqab.huawei.com
[2021-07-14 07:04:16.013 INFO app/server.go:84] [pool22-300-128-fusion-69d89bc7fb-bgccq] starting to init the router
..........
[2021-07-14 07:04:19.802 INFO app/server.go:402] [pool22-300-128-fusion-69d89bc7fb-bgccq] warm starting the server
[2021-07-14 07:04:19.802 INFO utils/utils.go:167] [pool22-300-128-fusion-69d89bc7fb-bgccq] succeed to set the ResourceLimitsMemory: 209715200
[2021-07-14 07:08:24.412 INFO app/server.go:185] [pool22-300-128-fusion-69d89bc7fb-bgccq] load function, traceID: a92992afb2635fb7257451df441256e1
[2021-07-14 07:08:24.413 INFO deploy/deployer.go:312] [pool22-300-128-fusion-69d89bc7fb-bgccq] deploy the function: BucketID=functionstorage-cn-north-7, ObjectID=functions/c53626012ba84727b938ca8bf03108ef/default/py_log/latest/index.zip
[2021-07-14 07:08:24.714 INFO deploy/deployer.go:252] [pool22-300-128-fusion-69d89bc7fb-bgccq] deploy function successfully
[2021-07-14 07:08:24.714 INFO deploy/deployer.go:266] [pool22-300-128-fusion-69d89bc7fb-bgccq] deploy layers successfully
[2021-07-14 07:08:24.714 INFO app/server.go:259] [pool22-300-128-fusion-69d89bc7fb-bgccq] finished deploying the function
````
问题：打印warn日志，因为开启了ssl，如果证书路径为空，则加载默认的/home/sn/resource/ca/ca.pem，但是该文件不存在。

新增配置项`HTTPS.Trusted.s3`，当为true时，不加载默认的/home/sn/resource/ca/ca.pem，避免打印warn日志。

* ### 场景七：

需求：
Unix和类Unix系统用户的umask须被设置成027或更大

#### 1. 场景分析
容器内runc默认会将umask设置为022，不满足安全规范

#### 2. 方案详细设计
因为runc会在容器启动时将umask设置为022，所以在Dockerfile中设置不生效，需要在各组件的entrypoint脚本中加入

```bash
umask 0027
```

涉及的组件有：所有元戎组件

注意：docker exec 看到的umask的置为 docker exec启动的/bin/bash 进程的umask值，不是容器进程实际使用的umask值。

* ### 场景八：

#### 现状分析

目前对接对象存储的客户端是minio client

#### 方案设计
使用华为云obs client替换minio client，涉及接口如下：
+ minio
```go
func New(endpoint string, opts *Options) (*Client, error)
func (c Client) BucketExists(ctx context.Context, bucketName string) (bool, error)
func (c Client) MakeBucket(ctx context.Context, bucketName string, opts MakeBucketOptions) (err error)
func (c Client) FPutObject(ctx context.Context, bucketName, objectName, filePath string, opts PutObjectOptions) (info UploadInfo, err error)
func (c Client) PresignedGetObject(ctx context.Context, bucketName string, objectName string, expires time.Duration, reqParams url.Values) (u *url.URL, err error)
func (c Client) FGetObject(ctx context.Context, bucketName, objectName, filePath string, opts GetObjectOptions) error
func (c Client) GetObject(ctx context.Context, bucketName, objectName string, opts GetObjectOptions) (*Object, error)
func (c Client) StatObject(ctx context.Context, bucketName, objectName string, opts StatObjectOptions) (ObjectInfo, error)
```
+ obs
```go
func New(ak, sk, endpoint string, configurers ...configurer) (*ObsClient, error)
func (obsClient ObsClient) HeadBucket(bucketName string, extensions ...extensionOptions) (output *BaseModel, err error)
func (obsClient ObsClient) CreateBucket(input *CreateBucketInput, extensions ...extensionOptions) (output *BaseModel, err error)
func (obsClient ObsClient) DeleteObject(input *DeleteObjectInput, extensions ...extensionOptions) (output *DeleteObjectOutput, err error)
func (obsClient ObsClient) PutFile(input *PutFileInput, extensions ...extensionOptions) (output *PutObjectOutput, err error)
func (obsClient ObsClient) CreateSignedUrl(input *CreateSignedUrlInput) (output *CreateSignedUrlOutput, err error)
func (obsClient ObsClient) GetObject(input *GetObjectInput, extensions ...extensionOptions) (output *GetObjectOutput, err error)
func (obsClient ObsClient) GetObjectMetadata(input *GetObjectMetadataInput, extensions ...extensionOptions) (output *GetObjectMetadataOutput, err error)
```

# 开源软件选型

*新引入的开源软件：原因、选型调研情况*

#### 外部交互分析&&上下游依赖需求
*描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系*

### 对外接口
*描述对外新增的接口*

+ 无
### 约束
*特性约束描述*

## 可信软件

#### 安全性 Security
*系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性*

#### 韧性 Resilience
*系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态*

#### 隐私性 Privacy
*系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务*

#### 可靠性 Reliability
*在给定的条件、给定的时间区间能无失效地执行要求的能力*

#### 可用性 Availability
*处于按要求执行状态的能力*

#### 安全 Safety
*系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的*

## LLT 用例
| **场景** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ---------------------------------------------------------- | ----------------------------------- | -------------------------------------- |
| 场景一        | 关闭重协商功能                                               | 使用openssl连接服务器443端口            | 触发重协商，服务器报错并断开连接             |
| 场景二 | 从测试用例日志中分析 | 跑部分测试用例，从日志中全局搜索，排查是否有相关敏感信息 | 无敏感信息 |
| 场景三 | 查看文件权限 | 使用tree等命令验证 | 文件权限合规 |
| 场景四 | 排除common crypto 的依赖 | common decrypt 直接return 空 | 调用正常 |
| 场景四 | 统一加解密 | 查看统一加解密修改之后调用流程是否正常 | 调用正常 |
| 场景五 | 进入任意fusion类型的runtime pod                  | 查看容器内进程            | runtime manager运行用户为sn，runtime的运行用户为snuser                         |
| 场景六 | https下载用户代码                           | 1. helm部署设置HTTPS.Enable.S3为true; 2. 创建并调用函数 | 函数调用返回成功           |
| 场景七 | umask配置生效                         | 查看任意元戎组件进程的umask值（cat /proc/1/status | grep Umask）        | 返回0027           |
| 场景八 | obs client替换minio client                         | 1.部署并配置对象存储server端为obs；2. 创建并调用函数 | 	函数调用返回成功          |