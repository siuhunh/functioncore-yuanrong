# 通信加密鉴权部署

元戎支持使用 HTTPS 加密通信的方式部署元戎和第三方组件以增强系统安全性，当前支持加密通信的组件包括 minio/keycloak/etcd 等第三方组件，以及 iam/admin-service 等元戎运行态关键组件。

为了支持加密通信，除了环境要求和准备章节，[安装部署](../deploy.md)各步骤需要做如下修改:

+ 开启 HTTPS 功能需要提供相应的证书，所有部署流程开始前需要先准备自签证书: 可以到 tools/basic/libs/ssl-tool 目录下执行
  * `sh gen-ca.sh`，根据提示输入密码用于保护证书，生成 CA(Certification Authority) 证书
  * `sh gen-minio.sh` 生成 MinIO 证书，根据提示，输入上一步的密码，再输入 K8S 集群的 IP 地址，即可在 tools/basic/libs/ssl-tool/certs/minio 目录下生成相应的证书
  * `sh gen-keycloak.sh` 生成 keycloak 证书，根据提示，输入上一步的密码，再输入 K8S 集群的 IP 地址，即可在 tools/basic/libs/ssl-tool/certs/keycloak 目录下生成相应的证书
  * `sh gen-iam.sh` 生成 IAM-access 证书，根据提示，输入上一步的密码，再输入 K8S 集群的 IP 地址，即可在 tools/basic/libs/ssl-tool/certs/iam-adaptor 目录下生成相应的证书
  * `sh gen-etcd.sh` 生成 etcd 证书，根据提示，输入上一步的密码，再输入 K8S 集群的 IP 地址，即可在 tools/basic/libs/ssl-tool/ssl 目录下生成相应的证书
  ```sh
  bash gen-etcd.sh
  please input ca key password
  ******
  please input all etcd nodes
  127.0.0.1
  ```
  * `sh gen-redis.sh` 生成 redis 证书，根据提示，输入上一步的密码，再输入 K8S 集群的 IP 地址，即可在 tools/basic/libs/ssl-tool/certs/redis-cluster 目录下生成相应的证书
  
+ 第三方组件部署前做如下修改: 
  * libs/minio/values.yaml 中的 tls.enabled 设置为 true
+ 安装好 K8S 集群（rancher 界面可以正常登陆）后，再将证书依次先写入 K8S 集群: 
  * `bash enable-minio-https.sh` 执行后需要重新安装一下 MinIO 组件，参考 process.sh 脚本帮助
  * `bash enable-keycloak-https.sh`
  * `bash enable-iam-https.sh`
  * `bash enable-admin-service-https.sh`
  * `bash enable-etcd-https.sh`
  ```sh
  bash enable-etcd-https.sh
  please enter the path to ca cert
  ******
  please enter the path to ds-core-etcd cert:
  ******
  please enter the path to ds-core-etcd key:
  ******
  ```
  * `bash enable-redis-cluster-https.sh`
+ 若为开启 keycloak 的通信加密/鉴权，还需要重新部署 keycloak 组件:
  * 修改版本包 basic/libs/keycloak 目录下的 value.yaml 文件中的两处:
    * 开启 tls.enable 为true
    * extraVolumes 和 extraVolumeMounts下进行 cert 文件的挂载
  * 利用 basic/process.sh 脚本卸载，重新安装 keycloak 组件：
    * 卸载： bash process.sh -o uninstall keycloak
    * 重新安装： bash process.sh -o install keycloak
    * 重新在 keycloak 上注册用户

+ 若为开启 etcd 的通信加密/鉴权，还需要重新部署 etcd 组件: 
  * 修改版本包 basic/libs/ds-care-etcd 目录下的 values.yaml 文件中的两处：
    * 关闭 rbac 鉴权：修改 auth.rbac.enable 为 false
    * 开启 client 鉴权：修改 clent.secureTransport 为 true
  * 执行 `helm uninstall` 命令卸载 ds-core-etcd，执行 `kubectl delete` 卸载 ds-core-etcd 的 pvc
  * 执行 `helm install` 命令安装运行态 etcd，此时的 etcd 即为开启证书校验模式，通过原 http 方式已无法正常访问
    * 创建 root 用户：执行命令会提示设置密码，配置再次输入确认

      IP 为 etcd 的 NodeIP， 对于 K8S 集群内 etcd 默认 Port 为 32379， 对于自行安装的 K8S 外的 etcd 集群，默认端口为 2379
    ```sh
    ${appPath}/etcdctl --cacert=/path/to/ca.pem --endpoints=https://IP:Port user add root
    ```
    * 开启鉴权：返回 Authentication Enabled 即更新成功
    ```sh
    ${appPath}/etcdctl --cacert=/path/to/ca.pem --endpoints=https://IP:Port --user root:{密码} auth enable
    ```
    * 验证：初始化环境返回 0 即表示可以访问
    ```sh
    ${appPath}/etcdctl --cacert=/path/to/ca.pem --endpoints=https://IP:Port --user root:{密码} get / --prefix |wc -l
    ```
    > 注: IP即为环境部署所在的节点IP。

| Parameter                                       | Description                                                                                                                                               | Default                                                     |
|-------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------|
| `auth.rbac.enabled`                             | Switch to enable the etcd authentication.                                                                                                                 | `true`                                                      |
| `auth.client.secureTransport`                   | Switch to encrypt client communication using TLS certificates                                                                                             | `false`                                                     |

+ 若为开启 redis 的通信加密/鉴权，还需要重新部署 redis 集群: 
  * x86 环境：修改 tools/basic/libs/redis/x86_64/values-production.yaml 文件
    * 修改 tls.enabled 为 true
  * 重新部署 redis 集群：
    * 卸载：bash process.sh -o uninstall -m redis-cluster
    * 安装：bash process.sh -o install -m redis-cluster
  > 注: arm 环境：修改 tools/basic/libs/redis/aarch64/values-production.yaml 文件
    
| Parameter                               | Description                                                                         | Default                                                 |
|-----------------------------------------|-------------------------------------------------------------------------------------|---------------------------------------------------------|
| `tls.enabled`                           | Enable TLS support for replication traffic                                          | `false`                                                 |
    
+ 部署 FunctionCore 组件时，执行 `bash config.sh` 和 `helm install functioncore ./` 之间新增如下修改:
  * values.yaml 文件中修改 **global.HTTPS** 参数，将 IAM/MinIO/keycloak/AdminService/CustomCA 均设置为 **true**
  * 注: CustomCA 为 false 的情况暂时比较麻烦，因此暂时不建议配置为 false

> 注: 线上环境为保证通讯安全，建议都打开加密鉴权通信。
