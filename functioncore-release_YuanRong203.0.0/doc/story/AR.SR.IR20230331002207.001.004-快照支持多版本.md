# 【functioncore】快照支持多版本

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

AR.SR.IR20230331002207.001.004

兼容适配快照无损升级

### 1.2特性场景分析

平台版本升级，快照兼容，需要快照支持多版本

## 2. 特性/功能实现原理

### 2.1总体方案

上线升级方案分为三阶段

#### 阶段一 数据面升级

1. worker-manager对etcd里函数元数据拓展`snapshotMetaData`和`snapshot`元数据做序列化兼容处理，若不是新数据结构则走原有逻辑
2. 新增worker-manager的configMap中的热加载开关`multiSnapshotConfig`,值为string类型，内容为javaruntime生成jar包的size大小，如下所示

   ```
   "multiSnapshotConfig": {
        "enable": true,
        "snapshotCheckInfo": {
          "javaRuntimeSize": 231231,
          "javaRuntimeHash": "testhash"
        }
      }
   ```

   若配置该项，则worker-manager补充存量etcd中的快照版本和javaruntime size信息，如下格式：

   ```
   "snapshotVersions": [
        {
            "objectId": "khj.zip.k0059126429287",
            "sha256": "4b4ce7f45768f19b08506e5f31c1a80a5d49a00d673d972a2966f6f4b3a95dca",
            "size": 18001131,
            "imageTag": "k0059126429287",
            "timestamp": "2023-03-28 19:41:23.902648881 +0800 CST m=+639.463577290",
            "snapshotCheckInfo": {
                "javaRuntimeSize": 231231,
                "javaRuntimeHash": "xxxx"
             },
        }
    ]
   ```
3. worker-manager回写snapshotVersions的objectId时补充imageTag作为后缀，保持objectId唯一
4. checkpoint完成后，runtime-manager在与worker的共享目录中写入snapshotCheckInfo信息
5. checkpoint后上传完成后，worker读取与runtime-manager的共享目录下snapshotCheckInfo信息，回传给worker-manager的结构体中新增字段`snapshotCheckInfo` (worker-manager侧保证数据结构兼容)

#### 阶段二 管理面升级

1. 管理面支持写入新数据结构的snapshot元数据。
2. 管理面监听snapshot元数据 update 事件，并更新函数元数据snapshot拓展`snapshot/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}/id/{snapshotID}`
3. 管理面写入objectId逻辑不变，由worker-manager回写objectId为"原有前缀-imageTag"区分多版本快照的名字为唯一。

#### 阶段三 打开热加载开关`multiSnapshotConfig`

**worker-manager，管理面升级，修改热加载开关`multiSnapshotConfig`**，触发回写更新当前快照版本和javaRuntimeSize，当检测到开关值改变时，worker-manage遍历所有snapshot元数据，获取当前runtime-manager的ImageTag

1. 若snapshotVersions为空，则补充`imageTag`, `snapshotCheckInfo`, `timeStamp`等字段至snapshotVersions
2. 升级后的管理面监听到snapshot元数据更新事件后，回写函数元数据snapshot拓展

### 2.2模块详细设计

#### 2.2.1 管理面

1. 管理面需要在函数元数据snapshotMetaData数组中保留两个最新的版本，objectId保持原有逻辑固定前缀，由worker-manager回写objectId为"前缀-imageTag"

函数元数据snapshotMetaData拓展新增字段`snapshotVersions` ，数据结构，保留多版本的快照信息。

```
type SnapshotDeploymentSpec struct {
	AppID             string             `json:"appId"`
	BucketID          string             `json:"bucketId"`
	ObjectID          string             `json:"objectId"`
	BucketURL         string             `json:"bucketUrl"`
	SnapshotSha256    string             `json:"sha256,omitempty"`
	SnapshotSize      int64              `json:"size,omitempty"`
	SnapshotCheckInfo *SnapshotCheckInfo `json:"snapshotCheckInfo,omitempty"`
	SnapshotVersions  []SnapshotVersion  `json:"snapshotVersions,omitempty"`
}
```

```
type SnapshotCheckInfo struct {
	JavaRuntimeSize int64  `json:"javaRuntimeSize,omitempty"`
	JavaRuntimeHash string `json:"javaRuntimeHash,omitempty"`
}
```

```
type SnapshotVersion struct {
	ObjectID          string            `json:"objectId,omitempty"`
	SnapshotSha256    string            `json:"sha256,omitempty"`
	SnapshotSize      int64             `json:"size,omitempty"`
	SnapshotCheckInfo SnapshotCheckInfo `json:"snapshotCheckInfo,omitempty"`
	SnapshotImageTag  string            `json:"imageTag,omitempty"`
	Timestamp         int64             `json:"timestamp,omitempty"`
	ErrCode           int               `json:"code,omitempty"`
	ErrMessage        string            `json:"message,omitempty"`       
}
```

snapshot元数据新增字段`snapshotVersions` ，数据结构，保留多版本的快照信息。

管理面触发快照，在`snapshotVersions`写入sha256，size为空的snapshotVersion的版本

key:

/snapshot/business/yrk/cluster/{clusterID}/tenant/{tenantID}/function/{functionName}/version/{version}/id/{snapshotID}

value:

```
{
"appId": "",
"bucketId": "bucket-test-log1",
"bucketUrl": "",
"objectId": "prefix-xxx",
"sha256": "",
"size": 0,
"snapshotVersions" : [{
   "objectId": "prefix-xxx",
   "sha256": "",
   "size": 0
  }]
}
```

数据面制作快照完成后，管理面回写函数元数据的`snapshotMetaData`，快照制作完成后格式如下：

```
value ----- json:
	SnapshotDeployment     SnapshotDeploymentSpec `json:"snapshotDeployment"`
	// ID 为管理面对该版本函数元数据的唯一标识
	ID      string `json:"ID"`
eg：
"snapshotMetaData": {
        "appId": "",
        "bucketId": "bucket-test-log1",
        "objectId": "khj.zip",
        "bucketUrl": "",
        "sha256": "e828836812e4902a883de2bd877060cb80494e0842ebed005892d4e8118a95f5",
        "size": 17511664,
        "snapshotVersions": [
            {
                "objectId": "khj.zip.k0059126426573",
                "sha256": "e828836812e4902a883de2bd877060cb80494e0842ebed005892d4e8118a95f5",
                "size": 17511664,
                "snapshotCheckInfo": {
                    "javaRuntimeSize": 2312350,
                    "javaRuntimeHash": "8e542011b75d294b8a59a69633dded48d2a1254e3f29a0aad149673b30bf0657"
                },
                "imageTag": "k0059126426573",
                "timestamp": 1650230400
            }
        ]
    }
```

数据面新版本快照制作失败，返回错误信息如下

```
{
        "appId": "",
        "bucketId": "bucket-test-log1",
        "objectId": "khj.zip",
        "bucketUrl": "",
        "sha256": "",
        "size": ,
        "snapshotVersions": [
            {
                "code":161905,
                "message":"download code from obs error: obs: service returned error: Status=404 Not Found, Code=NoSuchKey, Message=The specified key does not exist., RequestId=1754D7E95653B000, bucket: bucket-test-log1, object: khj.zip.101.3.0.B001.20230403134041, layer: "
            }
        ]
    }
```

2. 监听snapshot元数据 update 事件，并将snapshot元数据中的数据更新到函数元数据snapshot拓展中。

#### 2.2.2 worker-manager

1. 对etcd里函数元数据拓展`snapshotMetaData`和`snapshot`元数据做结构体序列化兼容处理，若不是多版本数组则走原有逻辑进行解析。
2. 新增worker-manager的configMap中的热加载开关`multiSnapshotConfig`, 每当`enable`值为true时，worker-manager遍历检查snapshot元数据中snapshotVersions是否为空：

   ```
   worker-manager-config: |-
    {
      "poolSizeConfig": {
        ......
      },
      .......
      "multiSnapshotConfig": {
        "enable": true,
        "snapshotCheckInfo": {
          "javaRuntimeSize": 231231,
          "javaRuntimeHash": "testhash"
        }
      }
    }
   ```

    1. 若snapshotVersions为空，则更新snapshot元数据，写入当前版本至snapshotVersions数组
    2. 若snapshotVersions不为空且imageTag相等，则不做处理
    3. ~~若snapshotVersions不为空且imageTag不相等，开始重制当前imagTag版本的快照，并追加~~ (**本次需求不做，预留快照升级需求**)

#### 2.2.3 runtime-manager & worker

1. runtime-manager完成checkpoint后，获取javaRuntime的信息，将`SnapshotCheckInfo`写入`/var/tmp/java-runtime-info`
2. worker在完成checkpoint后，从`/runtime/var-tmp/java-runtime-info` 读取SnapshotCheckInfo， 作为快照信息，返回给worker-manager

数据结构如下

```
type SnapshotCheckInfo struct {
	JavaRuntimeSize int64  `json:"javaRuntimeSize,omitempty"`
	JavaRuntimeHash string `json:"javaRuntimeHash,omitempty"`
}
```

![image](https://wiki.huawei.com/vision-file-storage/api/file/download/upload-v2/2023/3/7/20230407T1502Z/4c5913058631437a9a962b1dafb60884/image.png)

### 2.3 约束

#### 2.3.1上下游依赖需求

数据面可单独先升级，不影响正常调用。
管理面必须在数据面之后升级。

#### 2.3.2其他约束

该功能上线前，javaruntime不能升级

## 3. 可信软件

### 3.1安全性（Security）

NA

### 3.2韧性（Resilience）

### 3.3隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】* 配置项不涉及用户相关敏感数据

### 3.4可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*
和之前切流升级保持一致

### 3.6安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7 可维护性

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

管理面和数据面升级走正常升级观测流程

### 3.9性能

*【评估需求本身的性能影响、对其他流程的性能影响】*
NA

### 3.10资源

NA

### 3.11资料

需要新增work-manager的热加载开关描述

### 3.12升级兼容性

按照上文提及的升级顺序，保障兼容和无损。

## 4. 用例

| **序号** | **用例名**       | 用例类型    | **测试步骤**                                                                          | **预期结果**                                        |
| ---------------- | ------------------------ | ------------- | --------------------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| 1              |  升级兼容性黄区       | 自动用例    |   管理面未升级情况下，升级wm版本，开启快速启动，测试checkpoint/restore                 |      冷启动基本流程不失败      |
| 2              |  升级兼容性绿区       | 自动用例    |    管理面未升级情况下，升级wm版本，开启快速启动，测试checkpoint/restore                 |      冷启动基本流程不失败      |
| 3              |  多版本记录1        | 手工用例    |   升级wm版本，配置work-manager的热加载开关multiSnapshotConfig    |    ETCD中的snapshot元数据中有snapshotVersions，且都有snapshotCheckInfo，imageTag，timestamp等字段。snapshotCheckInfo中值为配置的multiSnapshotConfig中的值      |
| 4              |   多版本记录2       | 手工用例    |   升级wm版本，配置work-manager的热加载开关multiSnapshotConfig    |    ETCD中的snapshot元数据中objectId都带有imageTag后缀      |
