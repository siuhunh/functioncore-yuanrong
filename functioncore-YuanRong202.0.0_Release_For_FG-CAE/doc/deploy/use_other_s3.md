### 支持其他兼容S3(Simple Storage Service)的对象存储服务

本项目函数包默认保存在 MinIO 对象存储服务，支持配置使用其他 S3 兼容的对象存储服务（eg: AWS S3 服务，华为云 OBS 服务），部署流程需要做如下修改:
+ 当使用场景为 worker 直接从存储服务下载代码包时，整体流程不变，部署 functioncore 组件时，执行 `bash config.sh` 和 `helm install functioncore ./` 之前做如下修改:
    * 设置 values.yaml 文件中的 `global.S3_AK` , `global.S3_SK` 和 `global.S3_ENDPOINT` 字段

      | 参数          | 描述                                                         | 示例             |
      | ------------- | ------------------------------------------------------------ | ---------------- |
      | **S3_AK**     | S3 服务的 access key                              | / |
      | **S3_SK** | 加密后的 S3 服务的 secret key     | /       |
      | **S3_ENDPOINT** | 兼容 S3 协议的对象存储服务的访问点 | obs.cn-north-4.myhuaweicloud.com        |
      | **S3_MODE** | S3 服务的部署模式（集群内/集群外）     | 当选择为 OBS 时 S3_MODE 配置改为 outCluster，使用 local-cache-service 设置为 inCluster |
    * 如果要 worker 使用https下载代码包（需对象存储服务已支持），设置 values.yaml 文件中的 `global.HTTPS.Enable.S3` 字段，如果对象存储服务服务端的证书是受信的，设置 values.yaml 文件中的`global.HTTPS.TrustedCA.S3` 字段
      
      | 参数          | 描述                                                         | 示例             |
      | ------------- | ------------------------------------------------------------ | ---------------- |
      | **global.HTTPS.Enable.S3**     |  开启与S3的HTTPS加密通信。true(开启) or false(不开启)。default false。 | / |
      | **global.HTTPS.TrustedCA.S3** | S3服务端证书是否受信。true(受信) or false(不受信)。default false。    | /       |


> 注:需要注意 OBS 创建桶时桶名全局（华为云服务）唯一，桶名重复会导致创建桶失败。
+ 当使用场景为 worker 使用节点代理从存储服务 local-cache-service 下载代码包时（当前只支持集群内 MinIO），基础组件部署前做如下修改:
    * values.yaml 配置修改，具体参考上文 S3 相关配置描述
    * libs/local-cache-service/values.yaml 中的 params.ADDR 设置为对应 MinIO 的 endpoint

      | 参数          | 描述                                                         | 示例             |
      | ------------- | ------------------------------------------------------------ | ---------------- |
      | **params.ADDR** | MinIO 访问地址 | / |

> 注: 节点代理可以将代码包缓存至本地，以便下次下载时使用。