[TOC]

# 【FunctionCore】支持函数挂载Layer


## 1. Story整体设计
### 1. 功能描述
需求：  

解耦YuanRong Layer能力，对接FunctionGraph。

### 2. 场景分析
1. ​在funcGraph的函数页面，修改函数来添加或删除依赖代码包，点击保存按钮，需更新函数metadata中的**layers信息**，元戎收到函数调用请求后，加载函数绑定的依赖包(如有)。

​2. 当更新依赖包，如果此依赖包已被函数绑定，需更新函数metadata中的**layers信息**，元戎收到函数调用请求后，加载更新后的依赖包。

3. 当删除依赖包，如果此依赖包已被函数绑定，funcgraph则不允许删除改依赖包。

### 3. 方案详细设计
1. 依赖包的上传、更新、删除管理还是由funcGraph管理，依赖包的信息无需写入到ETCD。  

2. 修改函数添加或删除依赖代码包，点击保存按钮后，需更新存储在ETCD的函数metadata中的**layers信息**，**layers信息**示例如下：  

```
 "layers":[
    {
        "bucketUrl":"http://***:18085",
        "objectId":"layername-1622426672616",
        "bucketId":"bucket-test-log1",
        "appId":"61102"
    }
 ]
```
layers为一个数组，表示函数已绑定的依赖包数组：  
对应结构体如下：
```  
type layerBucket struct {
	BucketURL string `json:"bucketUrl"`
	ObjectID  string `json:"objectId"`
	BucketID  string `json:"bucketId"`
	AppID     string `json:"appId"`
}
```
描述如下：  

|          字段          |                             描述                             |                          备注                         |
| :---------------- | :----------------------------------------------------------: | :----------------------------------------------------------: |
|        bucketUrl         |               存储依赖包的bucketUrl             |                                                       |
| objectId | 存储依赖包的对象id。 | |
|      bucketId      | 存储依赖包的桶id |          |
|     appId    |        经确认此字段无用                                |  |

存储在ETCD的函数metadata示例如下：  

```
key: /sn/functions/business/yrk/tenant/dd6ad92c46164e60bd04488869eeabf5/function/0-hg-helloworld/version/$latest, 
value: {"funcMetaData":{"name":"0-hg-helloworld","description":"",
"functionUrn":"sn:cn:yrk:dd6ad92c46164e60bd04488869eeabf5:function:0-hg-helloworld",
"tenantId":"dd6ad92c46164e60bd04488869eeabf5","businessId":"yrk",
"reversedConcurrency":0,"tags":null,"functionVersionUrn":"sn:cn:yrk:dd6ad92c46164e60bd04488869eeabf5:function:0-hg-helloworld:$latest",
"codeSize":1362,"codeSha256":"bef82ef2cbfc8847b344f1e6112441fd461a4b4b5add168ddc10233e53e0059b","handler":"handler.my_handler",
"runtime":"python3.7","timeout":900,"version":"$latest","versionDescription":"$latest","statefulFlag":false,
"deadLetterConfig":"",
"layers":[{"bucketUrl":"http://bucket-test-log1.hwcloudtest.cn:18085","objectId":"mylayer-1622426672616","bucketId":"bucket-test-log1","appId":"61022"}],
"functionType":"","created":"2021-05-31 03:03:20.728 UTC","revisionId":"20210531030320728"},
"s3MetaData":{"appId":"61022","bucketUrl":.....
```
差异点统计：  

|          差异点          |                             funcGraph                  |                          元戎                       |       元戎开发面涉及组件      |          元戎运行面涉及组件       |  
| :---------------- | :----------------------------------------------------------: | :------------------------------------------------: | :-----------------------: | :----------------------------: |
|        上传依赖包文件格式         |     zip    |           zip       |    CLI、admin-service、func-repo        |    worker配置storageType为local检查     |
|        依赖包大小限制         |     上传的文件大小限制为10M，如超过10M，请通过OBS上传，OBS对ZIP文件限制大小为300M |  50M       |    CLI、admin-service、func-repo        |    worker配置storageType为local检查     |
|        解压后依赖包大小限制         |   1.5G    |           100M      |    CLI、admin-service、func-repo        |    worker配置storageType为local检查     |
|        文件个数限制         |   无    |           30000       |    CLI、admin-service、func-repo        |    worker配置storageType为local检查     |
|        目录深度限制         |   无  |           20       |    CLI、admin-service、func-repo        |    worker配置storageType为local检查     |
|        依赖包名称规范  |   可包含字母、数字、下划线和中划线，以大小写字母开头，以字母或数字结尾，长度不超过60个字符。  |  必须小写字母开头，小写字母或数字结尾，其余支持小写字母、数字和中划线，长度2-32       |    CLI、admin-service、func-repo        |    无    |
|        单个函数绑定依赖包个数限制  |   20  |    5       | func-repo | 无 |
|        依赖包版本         |   funcGraph依赖包不存在版本  |  纯数字，从1开始，单租户最多1000个层，单个层最多1万个版本，版本号最大100万   | func-repo |        无      |
|        运行时语言         |   一个依赖包只能配置一个runtime  |  可配置多个   |    CLI、admin-service、func-repo        |    无    |

因依赖包的上传、更新、删除管理还是由funcGraph管理，依赖包的信息无需写入到ETCD，所以元戎开发面不适配funcgraph依赖包相关规则，
同时worker配置storageType为s3不检查依赖包大小等限制，需funcgraph自行检查依赖包大小等限制（不包括文件个数和目录深度）---已确认。

优化：  

1. 下载超大依赖包：
当前元戎worker加载依赖包，对其大小有限制，如果依赖包大小超过worker内存限制大小则异常，如果上传依赖包大于128M，而worker内存限制为128M，必然失败提示`storeObject size is out of range`。
对此业务逻辑需进行优化，保证内存限制128M的worker，可以正常下载并解压大于128M的依赖包。可参考funcgraph的processrouter设计。

补充：因原先worker对ephemeral-storage本地临时性存储默认为500M, 按funcgraph解压前300M与解压后1.5G限制，当依赖包解压后超过500M，必定导致worker所在pod杀死。
经确认fusion-worker对ephemeral-storage限制改为2G，同时funcgraph管理面对函数包+依赖包总大小也按2G大小进行限制。

2. 本地缓存依赖包管理：  
funcgraph的processrouter将依赖包下载`/fssdata/repository/dependencies/dependency.Owner/dependency.Id/dependency.ETag`目录下，下载依赖包文件名为dependency.Id.zip，
首先检查该文件是否存在，如果存在则直接返回，如果不存在则下载，下载完成后检查父目录下是否有老依赖包，如果有则删除。而函数软件包有filemanager，对函数软件包有最大保存的软件包个数、软件包老化时间和清理周期管理。

补充：当前worker无法重复加载不同函数，本地缓存实现需挂载节点宿主机目录，同时实现一个节点文件缓存组件，实现对函数（单文件nodejs和python、jar、zip）或依赖包的本地缓存管理，本需求实现该功能。


### 4. 上下游依赖需求

​	funcgraph往ETCD写入函数metadata中时需带入layers信息

## 2. 可信软件

1. 安全性（Security） 
   *【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 
   不涉及
2. 韧性（Resilience）
   *【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 
   不涉及
3. 隐私性（Privacy） 
   *【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
   不涉及
4. 可靠性（Reliability）
   *【在给定的条件、给定的时间区间能无失效地执行要求的能力】*
   1、worker下载依赖包一半失败，如何处理？
   下载失败则则返回失败
   2、网络带宽不够，如何处理？
   有超时限制，如果带宽不够下载超时则返回失败
   3、代码包如何保持完整性？
   当前worker未对下载包进行完整性校验
5. 可用性（Availability）
   不涉及
6. 安全（Safety）
   *【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】
   不涉及
## 3. 用例

| **序号** | **用例名**                                                   | **测试步骤**                         | **预期结果**                             |
| -------- | ------------------------------------------------------------ | ------------------------------------ | ---------------------------------------- |
| 1        | 函数绑定新依赖包                                   | 函数工作流console页面，修改函数，添加函数依赖包，保存函数并测试调用 | 函数执行成功，进入函数pod，新增依赖包存在  |
| 2        | 函数去除依赖包                                   | 函数工作流console页面，修改函数，去除部分函数依赖包，保存函数并测试调用 | 函数执行成功，进入函数pod，删除依赖包不存在  |
| 3        | 更新函数已绑定依赖包                                  | 进入依赖包管理页面，更新一个已被函数绑定的依赖包，测试调用对应函数 | 函数执行成功，进入函数pod，查看依赖包为更新后的依赖包  |
| 4        | 删除已绑定函数的依赖包                                  | 进入依赖包管理页面，删除一个已被函数绑定的依赖包 | 删除失败，提示该依赖包已被绑定  |
| 5        | 函数绑定超大依赖包，函数内存限制128M      | 函数工作流console页面，修改函数，添加超大函数依赖包，设置函数内存限制128M，保存函数并测试调用  | 函数执行成功，进入函数pod，超大依赖包存在  |
